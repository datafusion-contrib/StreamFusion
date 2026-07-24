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

/// The normalizer persistent backend: the generic Paimon store under a plain row-payload codec.
#[cfg(feature = "paimon-state")]
pub(crate) type PaimonNormalizerStore = crate::state::PaimonStore<NormalizerStateCodec>;

/// The normalizer value codec for the Paimon store: exactly a row-payload codec (see
/// `RowPayloadCodec`) — the stored last row per unique key, as typed columns.
#[cfg(feature = "paimon-state")]
pub(crate) struct NormalizerStateCodec {
    row: crate::state::RowPayloadCodec,
}

#[cfg(feature = "paimon-state")]
impl NormalizerStateCodec {
    pub(crate) fn new(row_types: Vec<DataType>) -> Self {
        NormalizerStateCodec { row: crate::state::RowPayloadCodec::new(row_types) }
    }
}

#[cfg(feature = "paimon-state")]
impl crate::state::PaimonStateCodec for NormalizerStateCodec {
    type Value = NormalizedRow;

    fn supported(&self) -> bool {
        self.row.supported()
    }

    fn value_fields(&self) -> Vec<(String, DataType)> {
        self.row.fields()
    }

    fn encode(&self, row: &NormalizedRow) -> Vec<ScalarValue> {
        self.row.encode_payload(&row.payload)
    }

    fn decode(&self, scalars: &[ScalarValue]) -> NormalizedRow {
        let (payload, _) = self.row.decode_payload(scalars);
        NormalizedRow { payload, staged: false }
    }

    fn value_bytes(&self, row: &NormalizedRow) -> usize {
        row.payload.len()
    }
}

impl ChangelogNormalizer {
    pub(crate) fn new(key_columns: Vec<usize>, generate_update_before: bool) -> Self {
        let key_arity = key_columns.len();
        ChangelogNormalizer {
            key_columns,
            key_timestamp_precisions: vec![-1; key_arity],
            generate_update_before,
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

    /// Bounds the stored last-row-per-key state by the operator's managed-memory budget (negative =
    /// unaccounted), accounting any restored rows immediately.
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .rows
            .iter()
            .map(|(key, row)| byte_key_bytes(&key.0) + row.payload.len())
            .sum();
        self.memory.attach("changelog-normalize", budget_bytes, state)?;
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

    /// Attaches the managed-memory budget for a backend that starts with nothing resident (a
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

    pub(crate) fn push(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        self.snapshot_cache = None;
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
            // INSERT(0)/UPDATE_AFTER(2) put; UPDATE_BEFORE(1)/DELETE(3) remove.
            if kind == 0 || kind == 2 {
                match self.rows.get_mut(key) {
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
                        self.rows.insert(ByteKey::from(key), NormalizedRow { payload: current, staged });
                    }
                    Some(prev) if prev.payload.as_ref() == current => {
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
                                self.staged.touch(ByteKey::from(key), Some(prev.payload.clone()));
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
                    }
                }
            } else if let Some(prev) = self.rows.get(key) {
                let (payload, staged) = (prev.payload.clone(), prev.staged);
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
        if self.mini_batch {
            let retained = self.staged.retained_bytes(
                |key| byte_key_bytes(&key.0),
                |row| row.len(),
            );
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
        let changes = self.staged.drain_final(|key| {
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
        let schema = self.schema.as_ref().expect("schema set once a row was processed");
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let converter = self.payload_converter.as_ref().expect("payload converter set");
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
    fn snapshot(&self) -> Vec<u8> {
        let selected: Vec<ByteKey> = self.rows.keys().cloned().collect();
        self.snapshot_keys(&selected)
    }

    fn snapshot_keys(&self, selected: &[ByteKey]) -> Vec<u8> {
        let Some(schema) = &self.schema else { return Vec::new() };
        if selected.is_empty() {
            return Vec::new();
        }
        let mut fields: Vec<Field> = vec![Field::new("binary_key", DataType::Binary, false)];
        fields.extend(schema.fields().iter().map(|f| f.as_ref().clone()));
        let mut columns: Vec<ArrayRef> = vec![Arc::new(
            arrow::array::BinaryArray::from_iter_values(selected.iter().map(|key| key.0.as_ref())),
        )];
        let converter = self.payload_converter.as_ref().expect("payload converter set");
        let parser = converter.parser();
        columns.extend(
            converter
                .convert_rows(selected.iter().map(|key| {
                    parser.parse(&self.rows.get(&key.0).expect("selected key present").payload)
                }))
                .expect("decode normalizer snapshot payloads"),
        );
        write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("changelog-normalize snapshot"),
        )
    }

    fn restore(key_columns: Vec<usize>, generate_update_before: bool, bytes: &[u8]) -> Self {
        let mut normalizer = ChangelogNormalizer::new(key_columns, generate_update_before);
        for batch in read_ipc_if_present(bytes) {
            let schema = Arc::new(Schema::new(
                batch.schema().fields()[1..].iter().map(|field| field.as_ref().clone()).collect::<Vec<_>>(),
            ));
            normalizer.schema = Some(schema.clone());
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
            let data_arrays: Vec<ArrayRef> =
                (1..batch.num_columns()).map(|column| batch.column(column).clone()).collect();
            let payloads = converter
                .convert_columns(&data_arrays)
                .expect("encode restored normalizer payloads");
            for row in 0..batch.num_rows() {
                let key = ByteKey::from(keys.value(row));
                normalizer.rows.insert(
                    key,
                    NormalizedRow { payload: Arc::from(payloads.row(row).data()), staged: false },
                );
            }
            normalizer.payload_converter = Some(converter);
        }
        normalizer
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
        let mut keys_by_group: BTreeMap<i32, Vec<ByteKey>> = BTreeMap::new();
        for key in self.rows.keys().cloned() {
            let group = flink_key_group(hash_bytes_by_words(&key.0), max_parallelism) as i32;
            keys_by_group.entry(group).or_default().push(key);
        }
        let snapshots = keys_by_group
            .iter()
            .map(|(&group, keys)| (group, self.snapshot_keys(keys)))
            .collect();
        self.snapshot_cache = Some(NormalizerSnapshotCache {
            max_parallelism,
            timestamp_precisions: timestamp_precisions.to_vec(),
            snapshots,
        });
    }

    fn restore_partitions(
        key_columns: Vec<usize>,
        generate_update_before: bool,
        snapshots: &[Vec<u8>],
    ) -> Self {
        let mut merged = ChangelogNormalizer::new(key_columns.clone(), generate_update_before);
        for bytes in snapshots {
            let restored = ChangelogNormalizer::restore(
                key_columns.clone(),
                generate_update_before,
                bytes,
            );
            if merged.schema.is_none() {
                merged.schema = restored.schema.clone();
                merged.payload_converter = restored.payload_converter;
            }
            merged.rows.absorb(restored.rows);
        }
        merged
    }
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_changelogNormalizerStateBytes, ChangelogNormalizer);

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_changelogNormalizerStagingBytes<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let normalizer = unsafe { &*(handle as *const ChangelogNormalizer) };
    normalizer.staging_bytes() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_changelogNormalizerStagedKeys<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let normalizer = unsafe { &*(handle as *const ChangelogNormalizer) };
    normalizer.staged_keys() as jlong
}

/// Creates a changelog normalizer (keep-last per unique key) and returns an opaque handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createChangelogNormalizer<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    key_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    generate_update_before: jboolean,
    mini_batch: jboolean,
    memory_budget_bytes: jlong,
) -> jlong {
    let keys = read_columns(&env, &key_columns);
    let timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let normalizer = ChangelogNormalizer::new(keys, generate_update_before != 0)
        .with_mini_batch(mini_batch != 0)
        .with_key_timestamp_precisions(timestamp_precisions)
        .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, normalizer)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushChangelogNormalizer<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let normalizer = unsafe { &mut *(handle as *mut ChangelogNormalizer) };
    match normalizer.flush_mini_batch() {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Folds an input changelog batch into the keep-last state and exports the normalized changelog.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushChangelogNormalizer<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let normalizer = unsafe { &mut *(handle as *mut ChangelogNormalizer) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        normalizer.push(&batch)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Serializes the normalizer's per-key last rows for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotChangelogNormalizer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let normalizer = unsafe { &mut *(handle as *mut ChangelogNormalizer) };
    env.byte_array_from_slice(&normalizer.snapshot())
        .expect("failed to allocate changelog-normalize snapshot array")
        .into_raw()
}

/// Rebuilds a changelog normalizer from a snapshot and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreChangelogNormalizer<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    key_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    generate_update_before: jboolean,
    mini_batch: jboolean,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let keys = read_columns(&env, &key_columns);
    let timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read changelog-normalize snapshot");
    let normalizer = ChangelogNormalizer::restore(keys, generate_update_before != 0, &bytes)
        .with_mini_batch(mini_batch != 0)
        .with_key_timestamp_precisions(timestamp_precisions)
        .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, normalizer)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotChangelogNormalizerPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    let normalizer = unsafe { &mut *(handle as *mut ChangelogNormalizer) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    keyed_state_partition_array(
        &mut env,
        normalizer.snapshot_partitions(max_parallelism as usize, &precisions),
        "changelog-normalizer",
    )
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreChangelogNormalizerPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    key_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    generate_update_before: jboolean,
    mini_batch: jboolean,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let keys = read_columns(&env, &key_columns);
    let timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
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
    let normalizer = ChangelogNormalizer::restore_partitions(keys, generate_update_before != 0, &restored)
        .with_mini_batch(mini_batch != 0)
        .with_key_timestamp_precisions(timestamp_precisions)
        .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, normalizer)
}

/// Releases a changelog normalizer handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeChangelogNormalizer<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<ChangelogNormalizer>(handle));
    }
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
        let keys = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let values = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let kinds = batch.column(2).as_any().downcast_ref::<Int8Array>().unwrap();
        (0..batch.num_rows())
            .map(|row| (keys.value(row), values.value(row), kinds.value(row)))
            .collect()
    }

    #[test]
    fn mini_batch_emits_first_preimage_and_final_postimage() {
        let mut normalizer = ChangelogNormalizer::new(vec![0], true).with_mini_batch(true);
        assert_eq!(
            normalizer
                .push(&batch(vec![1, 2], vec![10, 5], vec![0, 0]))
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(
            rows(&normalizer.flush_mini_batch().unwrap()),
            vec![(1, 10, 0), (2, 5, 0)]
        );

        normalizer
            .push(&batch(vec![1, 1], vec![20, 30], vec![2, 2]))
            .unwrap();
        normalizer
            .push(&batch(vec![2, 3, 3], vec![5, 7, 7], vec![3, 0, 3]))
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
        normalizer.push(&batch(vec![1], vec![10], vec![0])).unwrap();
        normalizer.flush_mini_batch().unwrap();
        normalizer
            .push(&batch(vec![1, 1], vec![20, 30], vec![2, 2]))
            .unwrap();
        assert_eq!(rows(&normalizer.flush_mini_batch().unwrap()), vec![(1, 30, 2)]);
    }
}
