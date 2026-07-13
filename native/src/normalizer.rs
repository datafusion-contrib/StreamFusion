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
pub(crate) struct ChangelogNormalizer {
    key_columns: Vec<usize>,
    key_timestamp_precisions: Vec<i32>,
    generate_update_before: bool,
    schema: Option<SchemaRef>,
    rows: HashMap<ByteKey, JoinRow>,
    mini_batch: bool,
    staged: MiniBatchChanges<ByteKey, JoinRow>,
    staged_bytes: usize,
    snapshot_cache: Option<NormalizerSnapshotCache>,
    memory: OperatorMemory,
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

impl ChangelogNormalizer {
    pub(crate) fn new(key_columns: Vec<usize>, generate_update_before: bool) -> Self {
        let key_arity = key_columns.len();
        ChangelogNormalizer {
            key_columns,
            key_timestamp_precisions: vec![-1; key_arity],
            generate_update_before,
            schema: None,
            rows: HashMap::default(),
            mini_batch: false,
            staged: MiniBatchChanges::default(),
            staged_bytes: 0,
            snapshot_cache: None,
            memory: OperatorMemory::unaccounted(),
        }
    }

    pub(crate) fn with_mini_batch(mut self, mini_batch: bool) -> Self {
        self.mini_batch = mini_batch;
        self
    }

    /// Bounds the stored last-row-per-key state by the operator's managed-memory budget (negative =
    /// unaccounted), accounting any restored rows immediately.
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .rows
            .iter()
            .map(|(key, row)| byte_key_bytes(&key.0) + scalar_row_bytes(row))
            .sum();
        self.memory.attach("changelog-normalize", budget_bytes, state)?;
        Ok(self)
    }

    fn with_key_timestamp_precisions(mut self, key_timestamp_precisions: Vec<i32>) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        self
    }

    pub(crate) fn push(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        self.snapshot_cache = None;
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let data_arrays: Vec<&ArrayRef> = (0..arity).map(|i| batch.column(i)).collect();
        let row_kinds = row_kind_column(batch);

        let mut out_rows: Vec<JoinRow> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();

        // Keys are encoded into the encoder's reused buffer: probes and removes borrow the bytes,
        // and a key is copied into an owned `ByteKey` only when it first enters the map.
        let mut key_encoder =
            BinaryRowBatchEncoder::new(batch, &self.key_columns, &self.key_timestamp_precisions);
        for row in 0..batch.num_rows() {
            let kind = row_kinds.map(|k| k.value(row)).unwrap_or(0);
            let key = key_encoder.encode(row);
            let current: JoinRow = data_arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a, row).expect("changelog-normalize row scalar"))
                .collect();
            // INSERT(0)/UPDATE_AFTER(2) put; UPDATE_BEFORE(1)/DELETE(3) remove.
            if kind == 0 || kind == 2 {
                match self.rows.get_mut(key) {
                    None => {
                        if track {
                            delta += (byte_key_bytes(key) + scalar_row_bytes(&current)) as isize;
                        }
                        if self.mini_batch {
                            self.staged.push(ByteKey::from(key), MiniBatchChange::Insert(current.clone()));
                        } else {
                            out_rows.push(current.clone());
                            out_kinds.push(0); // +I
                        }
                        self.rows.insert(ByteKey::from(key), current);
                    }
                    Some(prev) if *prev == current => {
                        continue; // unchanged — emit nothing (no state TTL)
                    }
                    Some(prev) => {
                        if track {
                            // Same key: only the stored row is replaced.
                            delta += scalar_row_bytes(&current) as isize
                                - scalar_row_bytes(prev) as isize;
                        }
                        if self.mini_batch {
                            self.staged.push(
                                ByteKey::from(key),
                                MiniBatchChange::Update { before: prev.clone(), after: current.clone() },
                            );
                        } else {
                            if self.generate_update_before {
                                out_rows.push(prev.clone());
                                out_kinds.push(1); // -U the previous row
                            }
                            out_rows.push(current.clone());
                            out_kinds.push(2); // +U the new row
                        }
                        *prev = current;
                    }
                }
            } else if let Some(prev) = self.rows.remove(key) {
                if track {
                    delta -= (byte_key_bytes(key) + scalar_row_bytes(&prev)) as isize;
                }
                if self.mini_batch {
                    self.staged.push(ByteKey::from(key), MiniBatchChange::Delete(prev));
                } else {
                    out_rows.push(prev); // emit the stored full row, not the (maybe key-only) tombstone
                    out_kinds.push(3); // -D
                }
            }
        }
        if self.mini_batch {
            let retained = self.staged.retained_bytes(
                |key| byte_key_bytes(&key.0),
                |row| scalar_row_bytes(row),
            );
            delta += retained as isize - self.staged_bytes as isize;
            self.staged_bytes = retained;
        }
        self.memory.record(delta);
        self.memory.account()?;
        Ok(self.emit(out_rows, out_kinds))
    }

    pub(crate) fn flush_mini_batch(&mut self) -> Result<RecordBatch, DataFusionError> {
        if !self.mini_batch {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        let changes = self.staged.drain();
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
        self.memory.record(-(self.staged_bytes as isize));
        self.staged_bytes = 0;
        self.memory.account()?;
        Ok(self.emit(out_rows, out_kinds))
    }

    fn staged_keys(&self) -> usize {
        self.staged.touched_keys()
    }

    fn staging_bytes(&self) -> usize {
        self.staged_bytes
    }

    fn emit(&self, out_rows: Vec<JoinRow>, out_kinds: Vec<i8>) -> RecordBatch {
        if out_rows.is_empty() {
            return RecordBatch::new_empty(Arc::new(Schema::empty()));
        }
        let schema = self.schema.as_ref().expect("schema set once a row was processed");
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| {
                scalars_to_array(out_rows.iter().map(|r| r[j].clone()).collect(), fields[j].data_type())
            })
            .collect();
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build changelog-normalize batch")
    }

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
        columns.extend((0..schema.fields().len())
            .map(|j| {
                scalars_to_array(
                    selected
                        .iter()
                        .map(|key| self.rows[key][j].clone())
                        .collect(),
                    schema.field(j).data_type(),
                )
            })
            .collect::<Vec<_>>());
        write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("changelog-normalize snapshot"),
        )
    }

    fn restore(key_columns: Vec<usize>, generate_update_before: bool, bytes: &[u8]) -> Self {
        let mut normalizer = ChangelogNormalizer::new(key_columns, generate_update_before);
        for batch in read_ipc_if_present(bytes) {
            normalizer.schema = Some(Arc::new(Schema::new(
                batch.schema().fields()[1..].iter().map(|field| field.as_ref().clone()).collect::<Vec<_>>(),
            )));
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .expect("normalizer snapshot binary keys");
            let arity = batch.num_columns() - 1;
            for row in 0..batch.num_rows() {
                let key = ByteKey::from(keys.value(row));
                let stored: JoinRow = (0..arity)
                    .map(|i| {
                        ScalarValue::try_from_array(batch.column(i + 1), row)
                            .expect("restore scalar")
                    })
                    .collect();
                normalizer.rows.insert(key, stored);
            }
        }
        normalizer
    }

    fn snapshot_key_groups(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> Vec<i32> {
        self.materialize_raw_keyed_snapshots(max_parallelism, timestamp_precisions);
        self.snapshot_cache
            .as_ref()
            .expect("normalizer raw snapshot cache")
            .snapshots
            .keys()
            .copied()
            .collect()
    }

    fn snapshot_key_group(
        &mut self,
        key_group: i32,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> Vec<u8> {
        self.materialize_raw_keyed_snapshots(max_parallelism, timestamp_precisions);
        self.snapshot_cache
            .as_ref()
            .expect("normalizer raw snapshot cache")
            .snapshots
            .get(&key_group)
            .cloned()
            .expect("requested non-empty normalizer raw key group")
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
            }
            merged.rows.extend(restored.rows);
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
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_changelogNormalizerSnapshotKeyGroups<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jintArray {
    let normalizer = unsafe { &mut *(handle as *mut ChangelogNormalizer) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let groups = normalizer.snapshot_key_groups(max_parallelism as usize, &precisions);
    let output = env
        .new_int_array(groups.len() as i32)
        .expect("allocate normalizer raw key groups");
    env.set_int_array_region(&output, 0, &groups)
        .expect("write normalizer raw key groups");
    output.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotChangelogNormalizerKeyGroup<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key_group: jint,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jbyteArray {
    let normalizer = unsafe { &mut *(handle as *mut ChangelogNormalizer) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let snapshot = normalizer.snapshot_key_group(key_group, max_parallelism as usize, &precisions);
    env.byte_array_from_slice(&snapshot)
        .expect("allocate normalizer raw key-group snapshot")
        .into_raw()
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
