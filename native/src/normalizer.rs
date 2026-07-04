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
    generate_update_before: bool,
    schema: Option<SchemaRef>,
    rows: HashMap<GroupKey, JoinRow>,
    memory: OperatorMemory,
}

/// Estimated footprint of one stored full row (scalar cells, no entry overhead — the key side
/// carries it via [`group_key_bytes`]).
pub(crate) fn scalar_row_bytes(row: &[ScalarValue]) -> usize {
    row.iter().map(ScalarValue::size).sum()
}

impl ChangelogNormalizer {
    fn new(key_columns: Vec<usize>, generate_update_before: bool) -> Self {
        ChangelogNormalizer {
            key_columns,
            generate_update_before,
            schema: None,
            rows: HashMap::default(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds the stored last-row-per-key state by the operator's managed-memory budget (negative =
    /// unaccounted), accounting any restored rows immediately.
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .rows
            .iter()
            .map(|(key, row)| group_key_bytes(key) + scalar_row_bytes(row))
            .sum();
        self.memory.attach("changelog-normalize", budget_bytes, state)?;
        Ok(self)
    }

    fn push(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let key_arrays: Vec<&ArrayRef> =
            self.key_columns.iter().map(|&i| batch.column(i)).collect();
        let data_arrays: Vec<&ArrayRef> = (0..arity).map(|i| batch.column(i)).collect();
        let row_kinds = row_kind_column(batch);

        let mut out_rows: Vec<JoinRow> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();

        for row in 0..batch.num_rows() {
            let kind = row_kinds.map(|k| k.value(row)).unwrap_or(0);
            let key = read_key(&key_arrays, row);
            let current: JoinRow = data_arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a, row).expect("changelog-normalize row scalar"))
                .collect();
            // INSERT(0)/UPDATE_AFTER(2) put; UPDATE_BEFORE(1)/DELETE(3) remove.
            if kind == 0 || kind == 2 {
                match self.rows.get(&key) {
                    None => {
                        if track {
                            delta += (group_key_bytes(&key) + scalar_row_bytes(&current)) as isize;
                        }
                        out_rows.push(current.clone());
                        out_kinds.push(0); // +I
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
                        if self.generate_update_before {
                            out_rows.push(prev.clone());
                            out_kinds.push(1); // -U the previous row
                        }
                        out_rows.push(current.clone());
                        out_kinds.push(2); // +U the new row
                    }
                }
                self.rows.insert(key, current);
            } else if let Some(prev) = self.rows.remove(&key) {
                if track {
                    delta -= (group_key_bytes(&key) + scalar_row_bytes(&prev)) as isize;
                }
                out_rows.push(prev); // emit the stored full row, not the (maybe key-only) tombstone
                out_kinds.push(3); // -D
            }
        }
        self.memory.record(delta);
        self.memory.account()?;
        Ok(self.emit(out_rows, out_kinds))
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

    /// Serializes the stored last-row-per-key set (keys are re-derived from each row on restore).
    fn snapshot(&self) -> Vec<u8> {
        let Some(schema) = &self.schema else { return Vec::new() };
        let rows: Vec<&JoinRow> = self.rows.values().collect();
        if rows.is_empty() {
            return Vec::new();
        }
        let fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| {
                scalars_to_array(rows.iter().map(|r| r[j].clone()).collect(), fields[j].data_type())
            })
            .collect();
        write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("changelog-normalize snapshot"),
        )
    }

    fn restore(key_columns: Vec<usize>, generate_update_before: bool, bytes: &[u8]) -> Self {
        let mut normalizer = ChangelogNormalizer::new(key_columns, generate_update_before);
        for batch in read_ipc_if_present(bytes) {
            normalizer.schema = Some(batch.schema());
            let key_arrays: Vec<&ArrayRef> =
                normalizer.key_columns.iter().map(|&i| batch.column(i)).collect();
            let arity = batch.num_columns();
            for row in 0..batch.num_rows() {
                let key = read_key(&key_arrays, row);
                let stored: JoinRow = (0..arity)
                    .map(|i| ScalarValue::try_from_array(batch.column(i), row).expect("restore scalar"))
                    .collect();
                normalizer.rows.insert(key, stored);
            }
        }
        normalizer
    }
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_changelogNormalizerStateBytes, ChangelogNormalizer);

/// Creates a changelog normalizer (keep-last per unique key) and returns an opaque handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createChangelogNormalizer<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    key_columns: JIntArray<'local>,
    generate_update_before: jboolean,
    memory_budget_bytes: jlong,
) -> jlong {
    let keys = read_columns(&env, &key_columns);
    let normalizer = ChangelogNormalizer::new(keys, generate_update_before != 0)
        .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, normalizer)
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
    generate_update_before: jboolean,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let keys = read_columns(&env, &key_columns);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read changelog-normalize snapshot");
    let normalizer = ChangelogNormalizer::restore(keys, generate_update_before != 0, &bytes)
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
