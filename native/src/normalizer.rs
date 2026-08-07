use crate::*;

/// Changelog normalization (Flink's `ChangelogNormalize` / `ProcTimeDeduplicateKeepLastRowFunction`,
/// keep-last on a changelog): turns an upsert or duplicate-bearing changelog into a regular
/// INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE changelog with no duplication, keyed by the unique key.
/// It keeps the last full row per key (stored as INSERT) and, on each input row:
///   * a "put" (`+I`/`+U`): first row → emit `+I`; an unchanged row → suppress (no emit); a changed
///     row → emit `-U`(previous) if `generate_update_before`, then `+U`(new).
///   * a "remove" (`-D`/`-U`): emit `-D`(the stored full row, since a tombstone may carry only the
///     key) and clear the key; a remove of an absent key emits nothing.
/// Proctime — it emits synchronously per input row, so there is no watermark buffering.
pub(crate) struct ChangelogNormalizer<S: KeyedStateStore<NormalizedRow> = MemoryNormalizerStore> {
    key_columns: Vec<usize>,
    key_timestamp_precisions: Vec<i32>,
    generate_update_before: bool,
    // Idle-state retention millis (0 = off — Flink's default). With TTL on, a key expires `ttl_ms`
    // after its last write, and the unchanged-row suppression is disabled: Flink always emits
    // -U/+U under TTL to keep refreshing downstream state.
    ttl_ms: i64,
    // When the last full expiry sweep ran; the sweep reclaims keys never touched again, once per
    // TTL period (expiry itself is enforced lazily at each touch).
    last_sweep_ms: i64,
    schema: Option<SchemaRef>,
    payload_converter: Option<RowConverter>,
    rows: S,
    mini_batch: bool,
    staged: MiniBatchChanges<ByteKey, Arc<[u8]>>,
    staged_bytes: usize,
    snapshot_cache: Option<NormalizerSnapshotCache>,
    pub(crate) memory: OperatorMemory,
}

/// The resident default backend for the normalizer store (see `state/` for the seam).
pub(crate) type MemoryNormalizerStore = MemoryStateStore<NormalizedRow>;

pub(crate) struct NormalizedRow {
    payload: Arc<[u8]>,
    staged: bool,
    /// Wall-clock millis of the key's last write (Flink state TTL, `OnCreateAndWrite`); stays 0
    /// while TTL is off.
    last_write_ms: i64,
}

struct NormalizerSnapshotCache {
    max_parallelism: usize,
    timestamp_precisions: Vec<i32>,
    snapshots: BTreeMap<i32, Vec<u8>>,
}

/// Estimated footprint of one stored full row (scalar cells, no entry overhead — the key side
/// carries it via [`group_key_bytes`]).
pub(crate) fn scalar_row_bytes(row: &[ScalarValue]) -> usize {
    row.iter().map(ScalarValue::size).sum()
}

/// The normalizer persistent backend: the generic persistent store under a plain row-payload codec.

/// The normalizer value codec for the persistent store: exactly a row-payload codec (see
/// `RowPayloadCodec`) — the stored last row per unique key, as typed columns.

impl ChangelogNormalizer {
    pub(crate) fn new(key_columns: Vec<usize>, generate_update_before: bool) -> Self {
        let key_arity = key_columns.len();
        ChangelogNormalizer {
            key_columns,
            key_timestamp_precisions: vec![-1; key_arity],
            generate_update_before,
            ttl_ms: 0,
            last_sweep_ms: 0,
            schema: None,
            payload_converter: None,
            rows: MemoryNormalizerStore::default(),
            mini_batch: false,
            staged: MiniBatchChanges::default(),
            staged_bytes: 0,
            snapshot_cache: None,
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds the stored last-row-per-key state by the operator's task off-heap budget (negative =
    /// unaccounted), accounting any restored rows immediately.
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .rows
            .iter()
            .map(|(key, row)| byte_key_bytes(&key.0) + row.payload.len())
            .sum();
        self.memory
            .attach("changelog-normalize", budget_bytes, state)?;
        Ok(self)
    }
}

impl<S: KeyedStateStore<NormalizedRow>> ChangelogNormalizer<S> {
    /// Moves this freshly built (empty, memory-backed) normalizer's configuration onto another
    /// state backend; construction goes through `new` + builders first so backend choice stays
    /// orthogonal to the shape builders.
    pub(crate) fn with_backend<T: KeyedStateStore<NormalizedRow>>(
        self,
        rows: T,
    ) -> ChangelogNormalizer<T> {
        ChangelogNormalizer {
            key_columns: self.key_columns,
            key_timestamp_precisions: self.key_timestamp_precisions,
            generate_update_before: self.generate_update_before,
            ttl_ms: self.ttl_ms,
            last_sweep_ms: self.last_sweep_ms,
            schema: self.schema,
            payload_converter: self.payload_converter,
            rows,
            mini_batch: self.mini_batch,
            staged: self.staged,
            staged_bytes: self.staged_bytes,
            snapshot_cache: None,
            memory: self.memory,
        }
    }

    /// Attaches the task off-heap budget for a backend that starts with nothing resident (a
    /// read-through store hydrates on demand; there is no restored map to pre-account).
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory.attach("changelog-normalize", budget_bytes, 0)?;
        Ok(self)
    }

    /// The backing store, for backend-specific control paths (checkpointing a persistent store).
    pub(crate) fn store_mut(&mut self) -> &mut S {
        &mut self.rows
    }

    pub(crate) fn with_mini_batch(mut self, mini_batch: bool) -> Self {
        self.mini_batch = mini_batch;
        self
    }

    /// Sets the idle-state retention (`table.exec.state.ttl`) in millis; 0 (Flink's default)
    /// disables expiry.
    pub(crate) fn with_state_ttl(mut self, ttl_ms: i64) -> Self {
        self.ttl_ms = ttl_ms.max(0);
        self
    }

    /// Reclaims every key whose TTL elapsed with no further touch — the lazy per-touch expiry
    /// never sees such a key again. Silent, like Flink's background cleanup.
    fn sweep_expired(&mut self, ttl: StateTtl) {
        let track = self.memory.tracking();
        let mut reclaimed = 0isize;
        self.rows.retain_live(&mut |key, row| {
            if ttl.expired(row.last_write_ms) {
                if track {
                    reclaimed += (byte_key_bytes(key) + row.payload.len()) as isize;
                }
                false
            } else {
                true
            }
        });
        if reclaimed != 0 {
            self.memory.record(-reclaimed);
        }
    }

    pub(crate) fn with_key_timestamp_precisions(
        mut self,
        key_timestamp_precisions: Vec<i32>,
    ) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        self
    }

    fn ensure_payload_converter(&mut self, batch: &RecordBatch, arity: usize) {
        if self.payload_converter.is_none() {
            self.payload_converter = Some(
                RowConverter::new(
                    (0..arity)
                        .map(|column| SortField::new(batch.column(column).data_type().clone()))
                        .collect(),
                )
                .expect("normalizer payload converter"),
            );
        }
    }

    /// Folds an input changelog batch into the keep-last state and returns the normalized
    /// changelog. `now_ms` is the host's wall-clock reading for this call (only read when state
    /// TTL is on).
    pub(crate) fn push(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        self.snapshot_cache = None;
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        // The sweep reclaims keys no later row ever touches. Once per TTL period bounds its
        // amortized cost at one map walk per period; it must not run mid-bundle, where removing a
        // staged key's state would turn silent expiry into a spurious -D at the flush.
        if ttl.enabled()
            && self.staged.touched_keys() == 0
            && now_ms >= self.last_sweep_ms + self.ttl_ms
        {
            self.sweep_expired(ttl);
            self.last_sweep_ms = now_ms;
        }
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        self.ensure_payload_converter(batch, arity);
        self.rows
            .begin_batch(batch, &self.key_columns, &self.key_timestamp_precisions)?;
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let payloads = self
            .payload_converter
            .as_ref()
            .unwrap()
            .convert_columns(&data_arrays)
            .expect("encode normalizer payload");
        let row_kinds = row_kind_column(batch);

        let mut out_rows: Vec<Arc<[u8]>> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();

        // Keys are encoded into the encoder's reused buffer: probes and removes borrow the bytes,
        // and a key is copied into an owned `ByteKey` only when it first enters the map.
        let mut key_encoder =
            BinaryRowBatchEncoder::new(batch, &self.key_columns, &self.key_timestamp_precisions);
        for row in 0..batch.num_rows() {
            let kind = row_kinds.map(|k| k.value(row)).unwrap_or(0);
            let key = key_encoder.encode(row);
            let current = payloads.row(row).data();
            // An expired key is deleted on read and treated as never seen (Flink's
            // NeverReturnExpired): a put re-enters through the fresh +I path below, and a remove
            // falls into the absent-key skip. Nothing is emitted for the expiry, and the
            // first-touch preimage staged after it is None, so a mini-batch flush emits +I too.
            let on_expired = |row: &NormalizedRow| {
                if track {
                    delta -= (byte_key_bytes(key) + row.payload.len()) as isize;
                }
            };
            // INSERT(0)/UPDATE_AFTER(2) put; UPDATE_BEFORE(1)/DELETE(3) remove.
            if kind == 0 || kind == 2 {
                match ttl_get_mut(
                    &mut self.rows,
                    key,
                    ttl,
                    |row| row.last_write_ms,
                    on_expired,
                ) {
                    None => {
                        let current: Arc<[u8]> = Arc::from(current);
                        if track {
                            delta += (byte_key_bytes(key) + current.len()) as isize;
                        }
                        let staged = self.mini_batch;
                        if staged {
                            self.staged.touch(ByteKey::from(key), None);
                        } else {
                            out_rows.push(current.clone());
                            out_kinds.push(0); // +I
                        }
                        // Flink's OnCreateAndWrite: creation stamps the TTL clock.
                        let last_write_ms = if ttl.enabled() { ttl.now() } else { 0 };
                        self.rows.insert(
                            ByteKey::from(key),
                            NormalizedRow {
                                payload: current,
                                staged,
                                last_write_ms,
                            },
                        );
                    }
                    // With TTL on the unchanged-row suppression is disabled: Flink always emits
                    // -U/+U so downstream state keeps refreshing instead of expiring too early.
                    Some(prev) if prev.payload.as_ref() == current && !ttl.enabled() => {
                        continue; // unchanged — emit nothing (no state TTL)
                    }
                    Some(prev) => {
                        let current: Arc<[u8]> = Arc::from(current);
                        if track {
                            // Same key: only the stored row is replaced.
                            delta += current.len() as isize - prev.payload.len() as isize;
                        }
                        if self.mini_batch {
                            if !prev.staged {
                                self.staged
                                    .touch(ByteKey::from(key), Some(prev.payload.clone()));
                                prev.staged = true;
                            }
                        } else {
                            if self.generate_update_before {
                                out_rows.push(prev.payload.clone());
                                out_kinds.push(1); // -U the previous row
                            }
                            out_rows.push(current.clone());
                            out_kinds.push(2); // +U the new row
                        }
                        prev.payload = current;
                        if ttl.enabled() {
                            // Every put is a state write, so it refreshes the key's TTL.
                            prev.last_write_ms = ttl.now();
                        }
                    }
                }
            } else {
                let removed = ttl_get_mut(
                    &mut self.rows,
                    key,
                    ttl,
                    |row| row.last_write_ms,
                    on_expired,
                )
                .map(|prev| (prev.payload.clone(), prev.staged));
                if let Some((payload, staged)) = removed {
                    self.rows.remove(key);
                    if track {
                        delta -= (byte_key_bytes(key) + payload.len()) as isize;
                    }
                    if self.mini_batch {
                        if !staged {
                            self.staged.touch(ByteKey::from(key), Some(payload));
                        }
                    } else {
                        out_rows.push(payload); // emit the stored full row, not the (maybe key-only) tombstone
                        out_kinds.push(3); // -D
                    }
                }
            }
        }
        if self.mini_batch {
            let retained = self
                .staged
                .retained_bytes(|key| byte_key_bytes(&key.0), |row| row.len());
            delta += retained as isize - self.staged_bytes as isize;
            self.staged_bytes = retained;
        }
        // A mini-batch bundle spans pushes: hydrated keys stay resident until the flush ends the
        // bundle, so the staged re-probes there stay truthful.
        if !self.mini_batch {
            self.rows.end_bundle()?;
        }
        self.memory.record(delta + self.rows.footprint_delta());
        self.memory.account()?;
        Ok(self.emit(out_rows, out_kinds))
    }

    pub(crate) fn flush_mini_batch(&mut self) -> Result<RecordBatch, DataFusionError> {
        if !self.mini_batch {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        // The same TTL rule as immediate mode: a bundle whose net transition leaves the row
        // unchanged is suppressed only with retention off (staged keys were all written this
        // bundle, so none can be expired here).
        let changes = self.staged.drain_final(self.ttl_ms == 0, |key| {
            self.rows.get_mut(&key.0).map(|row| {
                row.staged = false;
                row.payload.clone()
            })
        });
        let mut out_rows = Vec::with_capacity(changes.len() * 2);
        let mut out_kinds = Vec::with_capacity(changes.len() * 2);
        for (_, change) in changes {
            match change {
                MiniBatchChange::Insert(after) => {
                    out_rows.push(after);
                    out_kinds.push(0);
                }
                MiniBatchChange::Delete(before) => {
                    out_rows.push(before);
                    out_kinds.push(3);
                }
                MiniBatchChange::Update { before, after } => {
                    if self.generate_update_before {
                        out_rows.push(before);
                        out_kinds.push(1);
                    }
                    out_rows.push(after);
                    out_kinds.push(2);
                }
            }
        }
        self.rows.end_bundle()?;
        self.memory
            .record(self.rows.footprint_delta() - self.staged_bytes as isize);
        self.staged_bytes = 0;
        self.memory.account()?;
        Ok(self.emit(out_rows, out_kinds))
    }

    pub(crate) fn staged_keys(&self) -> usize {
        self.staged.touched_keys()
    }

    pub(crate) fn staging_bytes(&self) -> usize {
        self.staged_bytes
    }

    fn emit(&self, out_rows: Vec<Arc<[u8]>>, out_kinds: Vec<i8>) -> RecordBatch {
        if out_rows.is_empty() {
            return RecordBatch::new_empty(Arc::new(Schema::empty()));
        }
        let schema = self
            .schema
            .as_ref()
            .expect("schema set once a row was processed");
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let converter = self
            .payload_converter
            .as_ref()
            .expect("payload converter set");
        let parser = converter.parser();
        let mut columns = converter
            .convert_rows(out_rows.iter().map(|row| parser.parse(row)))
            .expect("decode normalizer payloads");
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build changelog-normalize batch")
    }
}

/// The raw keyed-state snapshot/restore surface exists only on the memory backend — a persistent
/// store checkpoints through its own commit path instead of materializing the key space.
impl ChangelogNormalizer {
    /// Serializes the stored last-row-per-key set with its already canonical BinaryRow key.
    /// One IPC blob per key group of raw state bytes: the stored Flink-BinaryRow key and
    /// arrow-row payload, verbatim — no decode, and the group is one hash of the stored key's
    /// bytes per entry. The schema's metadata carries the typed payload schema so the converter
    /// can be rebuilt before any input arrives.
    fn raw_snapshot_groups(&self, max_parallelism: usize) -> BTreeMap<i32, Vec<u8>> {
        let Some(schema) = &self.schema else {
            return BTreeMap::new();
        };
        // The TTL timestamps ride a trailing column only while TTL is on, so a TTL-off snapshot
        // stays byte-identical to the pre-TTL format (and disabling TTL sheds the timestamps).
        let ttl_on = self.ttl_ms > 0;
        let mut builders: BTreeMap<i32, (BinaryBuilder, BinaryBuilder, Int64Builder)> =
            BTreeMap::new();
        for (key, row) in self.rows.iter() {
            let group = flink_key_group(hash_bytes_by_words(&key.0), max_parallelism) as i32;
            let (keys, payloads, write_timestamps) = builders.entry(group).or_default();
            keys.append_value(&key.0);
            payloads.append_value(&row.payload);
            write_timestamps.append_value(row.last_write_ms);
        }
        let mut fields = vec![
            Field::new(RAW_SNAPSHOT_KEY, DataType::Binary, false),
            Field::new(RAW_SNAPSHOT_ROW, DataType::Binary, false),
        ];
        if ttl_on {
            fields.push(Field::new(TTL_TS_COLUMN, DataType::Int64, false));
        }
        let raw_schema = Arc::new(Schema::new_with_metadata(
            fields,
            std::collections::HashMap::from([(
                RAW_SNAPSHOT_PAYLOAD_SCHEMA.to_string(),
                encode_schema_metadata(schema),
            )]),
        ));
        builders
            .into_iter()
            .map(|(group, (mut keys, mut payloads, mut write_timestamps))| {
                let mut columns: Vec<ArrayRef> =
                    vec![Arc::new(keys.finish()), Arc::new(payloads.finish())];
                if ttl_on {
                    columns.push(Arc::new(write_timestamps.finish()));
                }
                let batch = RecordBatch::try_new(raw_schema.clone(), columns)
                    .expect("raw normalizer snapshot batch");
                (group, write_ipc(&batch))
            })
            .collect()
    }

    #[cfg(test)]
    fn snapshot(&self) -> Vec<u8> {
        self.raw_snapshot_groups(1).remove(&0).unwrap_or_default()
    }

    #[cfg(test)]
    fn restore(
        key_columns: Vec<usize>,
        generate_update_before: bool,
        bytes: &[u8],
        restored_at_ms: i64,
    ) -> Self {
        Self::restore_partitions(
            key_columns,
            generate_update_before,
            &[bytes.to_vec()],
            restored_at_ms,
        )
    }

    /// Raw-format rows carry the stored key and payload bytes verbatim — restoring is a straight
    /// map rebuild with no decode or re-encode. The trailing TTL timestamps ride along when the
    /// writer had TTL on; a pre-TTL snapshot restored into a TTL'd normalizer stamps every key
    /// with the restore time — a full retention from now, Flink's enable-TTL migration — instead
    /// of 0, which would expire everything on first touch.
    fn load_batch_raw(&mut self, batch: &RecordBatch, restored_at_ms: i64) {
        if self.schema.is_none() {
            let payload_schema =
                decode_schema_metadata(batch).expect("raw normalizer snapshot payload schema");
            self.payload_converter = Some(
                RowConverter::new(
                    payload_schema
                        .fields()
                        .iter()
                        .map(|field| SortField::new(field.data_type().clone()))
                        .collect(),
                )
                .expect("restore normalizer payload converter"),
            );
            self.schema = Some(payload_schema);
        }
        let keys = column_binary(batch, RAW_SNAPSHOT_KEY);
        let payloads = column_binary(batch, RAW_SNAPSHOT_ROW);
        let write_timestamps = (batch.num_columns() > 2).then(|| {
            assert_eq!(
                batch.schema().field(2).name(),
                TTL_TS_COLUMN,
                "normalizer snapshot schema"
            );
            column_i64(batch, TTL_TS_COLUMN)
        });
        for row in 0..batch.num_rows() {
            self.rows.insert(
                ByteKey::from(keys.value(row)),
                NormalizedRow {
                    payload: Arc::from(payloads.value(row)),
                    staged: false,
                    last_write_ms: write_timestamps
                        .as_ref()
                        .map_or(restored_at_ms, |ts| ts.value(row)),
                },
            );
        }
    }

    /// Snapshots written before the raw format decoded the rows to typed columns
    /// (`[binary_key, data cols..]`); kept so existing savepoints keep restoring. The format
    /// predates TTL, so every key is stamped with the restore time (the enable-TTL migration).
    fn load_batch_decoded(&mut self, batch: &RecordBatch, restored_at_ms: i64) {
        let schema = Arc::new(Schema::new(
            batch.schema().fields()[1..]
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>(),
        ));
        self.schema = Some(schema.clone());
        let converter = RowConverter::new(
            schema
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )
        .expect("restore normalizer payload converter");
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .expect("normalizer snapshot binary keys");
        let data_arrays: Vec<ArrayRef> = (1..batch.num_columns())
            .map(|column| batch.column(column).clone())
            .collect();
        let payloads = converter
            .convert_columns(&data_arrays)
            .expect("encode restored normalizer payloads");
        for row in 0..batch.num_rows() {
            let key = ByteKey::from(keys.value(row));
            self.rows.insert(
                key,
                NormalizedRow {
                    payload: Arc::from(payloads.row(row).data()),
                    staged: false,
                    last_write_ms: restored_at_ms,
                },
            );
        }
        self.payload_converter = Some(converter);
    }

    fn snapshot_partitions(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        self.materialize_raw_keyed_snapshots(max_parallelism, timestamp_precisions);
        self.snapshot_cache
            .take()
            .expect("normalizer raw snapshot cache")
            .snapshots
    }

    fn materialize_raw_keyed_snapshots(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) {
        assert_eq!(self.key_timestamp_precisions, timestamp_precisions);
        if self.snapshot_cache.as_ref().is_some_and(|cache| {
            cache.max_parallelism == max_parallelism
                && cache.timestamp_precisions.as_slice() == timestamp_precisions
        }) {
            return;
        }
        self.snapshot_cache = Some(NormalizerSnapshotCache {
            max_parallelism,
            timestamp_precisions: timestamp_precisions.to_vec(),
            snapshots: self.raw_snapshot_groups(max_parallelism),
        });
    }

    fn restore_partitions(
        key_columns: Vec<usize>,
        generate_update_before: bool,
        snapshots: &[Vec<u8>],
        restored_at_ms: i64,
    ) -> Self {
        let mut normalizer = ChangelogNormalizer::new(key_columns, generate_update_before);
        for bytes in snapshots {
            for batch in read_ipc_if_present(bytes) {
                if batch.schema_ref().field(0).name() == RAW_SNAPSHOT_KEY {
                    normalizer.load_batch_raw(&batch, restored_at_ms);
                } else {
                    normalizer.load_batch_decoded(&batch, restored_at_ms);
                }
            }
        }
        normalizer
    }
}

state_bytes_getter!(
    Java_tech_streamfusion_Native_changelogNormalizerStateBytes,
    ChangelogNormalizer
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_changelogNormalizerStagingBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let normalizer = unsafe { &*(handle as *const ChangelogNormalizer) };
        normalizer.staging_bytes() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_changelogNormalizerStagedKeys<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let normalizer = unsafe { &*(handle as *const ChangelogNormalizer) };
        normalizer.staged_keys() as jlong
    })
}

/// Creates a changelog normalizer (keep-last per unique key) and returns an opaque handle.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createChangelogNormalizer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    key_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    generate_update_before: jboolean,
    mini_batch: jboolean,
    state_ttl_millis: jlong,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let keys = read_columns(&env, &key_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let normalizer = ChangelogNormalizer::new(keys, generate_update_before != 0)
            .with_mini_batch(mini_batch != 0)
            .with_key_timestamp_precisions(timestamp_precisions)
            .with_state_ttl(state_ttl_millis)
            .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, normalizer)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushChangelogNormalizer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let normalizer = unsafe { &mut *(handle as *mut ChangelogNormalizer) };
        match normalizer.flush_mini_batch() {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Folds an input changelog batch into the keep-last state and exports the normalized changelog.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushChangelogNormalizer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let normalizer = unsafe { &mut *(handle as *mut ChangelogNormalizer) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            normalizer.push(&batch, now_millis)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotChangelogNormalizerPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let normalizer = unsafe { &mut *(handle as *mut ChangelogNormalizer) };
        let precisions = read_i32_array(&env, &timestamp_precisions);
        keyed_state_partition_array(
            &mut env,
            normalizer.snapshot_partitions(max_parallelism as usize, &precisions),
            "changelog-normalizer",
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreChangelogNormalizerPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    key_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    generate_update_before: jboolean,
    mini_batch: jboolean,
    state_ttl_millis: jlong,
    now_millis: jlong,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let keys = read_columns(&env, &key_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let count = env
            .get_array_length(&snapshots)
            .expect("read normalizer raw partition count");
        let mut restored = Vec::with_capacity(count as usize);
        for index in 0..count {
            let bytes = JByteArray::from(
                env.get_object_array_element(&snapshots, index)
                    .expect("read normalizer raw partition"),
            );
            restored.push(
                env.convert_byte_array(&bytes)
                    .expect("read normalizer raw partition bytes"),
            );
        }
        let normalizer = ChangelogNormalizer::restore_partitions(
            keys,
            generate_update_before != 0,
            &restored,
            now_millis,
        )
        .with_mini_batch(mini_batch != 0)
        .with_key_timestamp_precisions(timestamp_precisions)
        .with_state_ttl(state_ttl_millis)
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, normalizer)
    })
}

/// Releases a changelog normalizer handle.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeChangelogNormalizer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<ChangelogNormalizer>(handle));
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(keys: Vec<i64>, values: Vec<i64>, kinds: Vec<i8>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
                Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(values)),
                Arc::new(Int8Array::from(kinds)),
            ],
        )
        .unwrap()
    }

    fn rows(batch: &RecordBatch) -> Vec<(i64, i64, i8)> {
        if batch.num_rows() == 0 {
            return Vec::new();
        }
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|row| (keys.value(row), values.value(row), kinds.value(row)))
            .collect()
    }

    #[test]
    fn mini_batch_emits_first_preimage_and_final_postimage() {
        let mut normalizer = ChangelogNormalizer::new(vec![0], true).with_mini_batch(true);
        assert_eq!(
            normalizer
                .push(&batch(vec![1, 2], vec![10, 5], vec![0, 0]), 0)
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(
            rows(&normalizer.flush_mini_batch().unwrap()),
            vec![(1, 10, 0), (2, 5, 0)]
        );

        normalizer
            .push(&batch(vec![1, 1], vec![20, 30], vec![2, 2]), 0)
            .unwrap();
        normalizer
            .push(&batch(vec![2, 3, 3], vec![5, 7, 7], vec![3, 0, 3]), 0)
            .unwrap();
        assert_eq!(
            rows(&normalizer.flush_mini_batch().unwrap()),
            vec![(1, 10, 1), (1, 30, 2), (2, 5, 3)]
        );
        assert_eq!(normalizer.staged_keys(), 0);
        assert_eq!(normalizer.staging_bytes(), 0);
    }

    #[test]
    fn mini_batch_without_update_before_only_emits_final_update() {
        let mut normalizer = ChangelogNormalizer::new(vec![0], false).with_mini_batch(true);
        normalizer
            .push(&batch(vec![1], vec![10], vec![0]), 0)
            .unwrap();
        normalizer.flush_mini_batch().unwrap();
        normalizer
            .push(&batch(vec![1, 1], vec![20, 30], vec![2, 2]), 0)
            .unwrap();
        assert_eq!(
            rows(&normalizer.flush_mini_batch().unwrap()),
            vec![(1, 30, 2)]
        );
    }

    // State TTL: an idle key expires ttl millis after its last write; the next put is a fresh +I
    // (Flink's NeverReturnExpired: expired reads as absent).
    #[test]
    fn ttl_expires_an_idle_key_into_a_fresh_insert() {
        let mut normalizer = ChangelogNormalizer::new(vec![0], true).with_state_ttl(1000);
        let out = normalizer
            .push(&batch(vec![1], vec![10], vec![0]), 5000)
            .unwrap();
        assert_eq!(rows(&out), vec![(1, 10, 0)]);
        // ts 5000 + ttl 1000 <= 6000: expired exactly at the boundary — a fresh +I, not -U/+U.
        let out = normalizer
            .push(&batch(vec![1], vec![5], vec![2]), 6000)
            .unwrap();
        assert_eq!(rows(&out), vec![(1, 5, 0)]);
    }

    // A write refreshes the TTL (OnCreateAndWrite): steadily-touched keys never expire, and expiry
    // is timed from the LAST write.
    #[test]
    fn ttl_refreshes_on_every_write() {
        let mut normalizer = ChangelogNormalizer::new(vec![0], true).with_state_ttl(1000);
        normalizer
            .push(&batch(vec![1], vec![10], vec![0]), 5000)
            .unwrap();
        let out = normalizer
            .push(&batch(vec![1], vec![20], vec![2]), 5900)
            .unwrap();
        assert_eq!(rows(&out), vec![(1, 10, 1), (1, 20, 2)]); // alive: -U/+U
                                                              // 900ms later the original write is long past ttl, but the refresh at 5900 keeps it alive.
        let out = normalizer
            .push(&batch(vec![1], vec![30], vec![2]), 6800)
            .unwrap();
        assert_eq!(rows(&out), vec![(1, 20, 1), (1, 30, 2)]);
    }

    // A remove reaching an expired (absent) key deletes the corpse silently — Flink emits a -D
    // only for a stored row it can still read.
    #[test]
    fn ttl_drops_a_tombstone_against_an_expired_key() {
        let mut normalizer = ChangelogNormalizer::new(vec![0], true).with_state_ttl(1000);
        normalizer
            .push(&batch(vec![1], vec![10], vec![0]), 5000)
            .unwrap();
        let out = normalizer
            .push(&batch(vec![1], vec![10], vec![3]), 7000)
            .unwrap();
        assert_eq!(out.num_rows(), 0);
        // The corpse is gone: the next put for the key is a fresh insert.
        let out = normalizer
            .push(&batch(vec![1], vec![5], vec![0]), 7000)
            .unwrap();
        assert_eq!(rows(&out), vec![(1, 5, 0)]);
    }

    // With TTL on, the unchanged-row suppression is disabled: Flink always emits -U/+U so
    // downstream TTL state keeps refreshing (the deterministic, parity-testable TTL behavior).
    #[test]
    fn ttl_emits_the_unchanged_row_it_would_otherwise_suppress() {
        let mut normalizer = ChangelogNormalizer::new(vec![0], true).with_state_ttl(3_600_000);
        normalizer
            .push(&batch(vec![1], vec![10], vec![0]), 5000)
            .unwrap();
        let out = normalizer
            .push(&batch(vec![1], vec![10], vec![2]), 5001)
            .unwrap();
        assert_eq!(rows(&out), vec![(1, 10, 1), (1, 10, 2)]); // -U/+U, not suppressed

        // The -U half still honors generate_update_before.
        let mut no_before = ChangelogNormalizer::new(vec![0], false).with_state_ttl(3_600_000);
        no_before
            .push(&batch(vec![1], vec![10], vec![0]), 5000)
            .unwrap();
        let out = no_before
            .push(&batch(vec![1], vec![10], vec![2]), 5001)
            .unwrap();
        assert_eq!(rows(&out), vec![(1, 10, 2)]);
    }

    // TTL timestamps ride the snapshot as absolute millis: expiry after a restore is timed from
    // the original write, not from the restore.
    #[test]
    fn ttl_timestamps_survive_snapshot_restore() {
        let mut normalizer = ChangelogNormalizer::new(vec![0], true).with_state_ttl(1000);
        normalizer
            .push(&batch(vec![1], vec![10], vec![0]), 5000)
            .unwrap();
        let snapshot = normalizer.snapshot();
        let mut alive =
            ChangelogNormalizer::restore(vec![0], true, &snapshot, 5500).with_state_ttl(1000);
        let out = alive
            .push(&batch(vec![1], vec![20], vec![2]), 5999)
            .unwrap();
        assert_eq!(rows(&out), vec![(1, 10, 1), (1, 20, 2)]); // one ms inside the window
        let mut expired =
            ChangelogNormalizer::restore(vec![0], true, &snapshot, 5500).with_state_ttl(1000);
        let out = expired
            .push(&batch(vec![1], vec![20], vec![2]), 6000)
            .unwrap();
        assert_eq!(rows(&out), vec![(1, 20, 0)]); // ts 5000 + 1000 <= 6000 — fresh insert
    }

    // A pre-TTL snapshot (no timestamp column) restored into a TTL'd normalizer stamps every key
    // with the restore time — a full retention from now, Flink's enable-TTL migration — instead of
    // expiring everything on first touch.
    #[test]
    fn ttl_enable_migration_stamps_restore_time() {
        let mut normalizer = ChangelogNormalizer::new(vec![0], true);
        normalizer
            .push(&batch(vec![1], vec![10], vec![0]), 0)
            .unwrap();
        let snapshot = normalizer.snapshot(); // TTL off: no timestamp column
        let mut restored =
            ChangelogNormalizer::restore(vec![0], true, &snapshot, 5000).with_state_ttl(1000);
        let out = restored
            .push(&batch(vec![1], vec![20], vec![2]), 5999)
            .unwrap();
        assert_eq!(rows(&out), vec![(1, 10, 1), (1, 20, 2)]); // alive until restore + ttl
        let mut expired =
            ChangelogNormalizer::restore(vec![0], true, &snapshot, 5000).with_state_ttl(1000);
        let out = expired
            .push(&batch(vec![1], vec![20], vec![2]), 6000)
            .unwrap();
        assert_eq!(rows(&out), vec![(1, 20, 0)]);
    }

    // The periodic sweep reclaims keys that are never touched again, silently (expiry emits
    // nothing).
    #[test]
    fn ttl_sweep_reclaims_idle_keys_silently() {
        let mut normalizer = ChangelogNormalizer::new(vec![0], true).with_state_ttl(1000);
        normalizer
            .push(&batch(vec![1], vec![10], vec![0]), 5000)
            .unwrap();
        normalizer
            .push(&batch(vec![2], vec![20], vec![0]), 5000)
            .unwrap();
        // Touching only key 2 well past key 1's expiry triggers the once-per-period sweep; key 1's
        // row is gone from the snapshot without any -D having been emitted.
        let out = normalizer
            .push(&batch(vec![2], vec![1], vec![2]), 7000)
            .unwrap();
        assert_eq!(rows(&out), vec![(2, 1, 0)]); // key 2 itself had expired too — fresh +I
        let snapshot = normalizer.snapshot();
        let mut probe =
            ChangelogNormalizer::restore(vec![0], true, &snapshot, 7000).with_state_ttl(1000);
        // Key 1 was swept: a delete for it finds nothing and emits nothing.
        let out = probe
            .push(&batch(vec![1], vec![10], vec![3]), 7100)
            .unwrap();
        assert_eq!(out.num_rows(), 0);
    }

    // The mini-batch flush applies the same TTL rule: a bundle whose net transition is a no-op
    // still emits -U/+U with retention on (an unchanged row must stage instead of being swallowed).
    #[test]
    fn ttl_mini_batch_flush_emits_unchanged_transitions() {
        let mut normalizer = ChangelogNormalizer::new(vec![0], true)
            .with_state_ttl(3_600_000)
            .with_mini_batch(true);
        normalizer
            .push(&batch(vec![1], vec![10], vec![0]), 5000)
            .unwrap();
        normalizer.flush_mini_batch().unwrap();
        normalizer
            .push(&batch(vec![1], vec![10], vec![2]), 5001)
            .unwrap(); // net no-op bundle
        assert_eq!(
            rows(&normalizer.flush_mini_batch().unwrap()),
            vec![(1, 10, 1), (1, 10, 2)]
        );
    }

    // A key that expires between the pushes of one bundle stages old=None after the delete-on-read,
    // so the flush emits the fresh +I Flink would.
    #[test]
    fn ttl_mini_batch_stages_no_preimage_for_an_expired_key() {
        let mut normalizer = ChangelogNormalizer::new(vec![0], true)
            .with_state_ttl(1000)
            .with_mini_batch(true);
        normalizer
            .push(&batch(vec![1], vec![10], vec![0]), 5000)
            .unwrap();
        normalizer.flush_mini_batch().unwrap();
        // Key 9 opens the next bundle, so the sweep (skipped mid-bundle) cannot reclaim key 1;
        // its expiry is enforced by the delete-on-read probe, staging a None preimage.
        normalizer
            .push(&batch(vec![9], vec![90], vec![0]), 5500)
            .unwrap();
        normalizer
            .push(&batch(vec![1], vec![20], vec![2]), 7000)
            .unwrap();
        assert_eq!(
            rows(&normalizer.flush_mini_batch().unwrap()),
            vec![(9, 90, 0), (1, 20, 0)]
        );
    }
}
