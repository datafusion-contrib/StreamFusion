use crate::*;

/// One buffered probe-side (left) row of a temporal join: its data values, its event time, and its
/// changelog `RowKind` (forwarded onto the emitted joined row, as Flink does).
pub(crate) struct LeftEntry {
    row: JoinRow,
    time: i64,
    kind: i8,
}

/// Event-time temporal table join (`... JOIN versioned FOR SYSTEM_TIME AS OF probe.rowtime`): a
/// faithful port of Flink's `TemporalRowTimeJoinOperator`. The build (right) side is a *versioned*
/// table — a changelog keyed by the equi-join key, each version timestamped by its right rowtime; the
/// probe (left) side is buffered until the watermark passes its time, then joined against the version
/// of the build row valid at the probe row's time.
///
/// State is partitioned by the equi-join key (the operator is keyed in Flink). Per key:
/// - `right_state`: `rightTime -> (row, RowKind)`, last-write-wins per timestamp (Flink's
///   `rightState.put(rowTime, row)`), every RowKind retained — a `-D`/`-U` marks that the version
///   starting at that time has no row.
/// - `left_state`: rows buffered in arrival order with their event time.
///
/// On a watermark, every buffered left row whose time the watermark has passed is emitted (in arrival
/// order): the latest right version with `rightTime <= leftTime` is found by an ordered lookup; if it
/// exists, is an accumulate message, and satisfies the residual non-equi predicate, the pair is
/// emitted carrying the left row's RowKind, otherwise (LEFT join) a null-padded row is emitted. Old
/// versions behind the watermark are then dropped, always keeping at least the latest valid one.
///
/// Emission is gated on the watermark, so the result is independent of arrival interleaving and of
/// cross-key emission order — deterministic, and value-comparable to the host. Only INNER and LEFT are
/// possible (Flink rejects RIGHT/FULL for temporal join), so only the build side can be absent.
pub(crate) struct TemporalJoiner {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    left_time: usize,
    right_time: usize,
    join_type: JoinKind,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    predicate: Option<JoinPredicate>,
    left_state: HashMap<GroupKey, Vec<LeftEntry>>,
    right_state: HashMap<GroupKey, BTreeMap<i64, (JoinRow, i8)>>,
    memory: OperatorMemory,
}

/// Estimated footprint of one buffered probe row (its scalars, time, kind, and container entry).
pub(crate) fn left_entry_bytes(entry: &LeftEntry) -> usize {
    scalar_row_bytes(&entry.row) + GROUP_ENTRY_OVERHEAD
}

/// Estimated footprint of one build-side version (its scalars, kind, and tree entry).
pub(crate) fn right_version_bytes(row: &JoinRow) -> usize {
    scalar_row_bytes(row) + GROUP_ENTRY_OVERHEAD
}

impl TemporalJoiner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_time: usize,
        right_time: usize,
        join_type: JoinKind,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        predicate: Option<JoinPredicate>,
    ) -> Self {
        TemporalJoiner {
            left_keys,
            right_keys,
            left_time,
            right_time,
            join_type,
            left_schema,
            right_schema,
            predicate,
            left_state: HashMap::default(),
            right_state: HashMap::default(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds both sides' state by the operator's managed-memory budget (negative = unaccounted),
    /// accounting any restored state immediately.
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let left: usize = self
            .left_state
            .iter()
            .map(|(key, entries)| {
                group_key_bytes(key) + entries.iter().map(left_entry_bytes).sum::<usize>()
            })
            .sum();
        let right: usize = self
            .right_state
            .iter()
            .map(|(key, versions)| {
                group_key_bytes(key)
                    + versions.values().map(|(row, _)| right_version_bytes(row)).sum::<usize>()
            })
            .sum();
        self.memory.attach("temporal-join", budget_bytes, left + right)?;
        Ok(self)
    }

    fn left_types(&self) -> Vec<DataType> {
        self.left_schema.fields().iter().map(|f| f.data_type().clone()).collect()
    }

    fn right_types(&self) -> Vec<DataType> {
        self.right_schema.fields().iter().map(|f| f.data_type().clone()).collect()
    }

    /// Buffers a probe-side batch (no output until a watermark). Each row is stored under its
    /// equi-join key with its event time and changelog kind, in arrival order within the key.
    pub(crate) fn push_left(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let arity = data_arity(batch);
        let key_arrays: Vec<&ArrayRef> = self.left_keys.iter().map(|&i| batch.column(i)).collect();
        let times = rt_to_millis(batch.column(self.left_time));
        let kinds = row_kind_column(batch);
        let track = self.memory.tracking();
        let mut delta = 0isize;
        for row in 0..batch.num_rows() {
            let key = read_key(&key_arrays, row);
            let jrow: JoinRow = (0..arity)
                .map(|i| ScalarValue::try_from_array(batch.column(i), row).expect("temporal left scalar"))
                .collect();
            if track && !self.left_state.contains_key(&key) {
                delta += group_key_bytes(&key) as isize;
            }
            let entry = LeftEntry {
                row: jrow,
                time: times.value(row),
                kind: kinds.map_or(0, |k| k.value(row)),
            };
            if track {
                delta += left_entry_bytes(&entry) as isize;
            }
            self.left_state.entry(key).or_default().push(entry);
        }
        self.memory.record(delta);
        self.memory.account()
    }

    /// Folds a build-side changelog batch into the versioned state, keyed by equi-join key and indexed
    /// by right rowtime (last-write-wins per timestamp, every RowKind kept — Flink's `rightState.put`).
    pub(crate) fn push_right(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let arity = data_arity(batch);
        let key_arrays: Vec<&ArrayRef> = self.right_keys.iter().map(|&i| batch.column(i)).collect();
        let times = rt_to_millis(batch.column(self.right_time));
        let kinds = row_kind_column(batch);
        let track = self.memory.tracking();
        let mut delta = 0isize;
        for row in 0..batch.num_rows() {
            let key = read_key(&key_arrays, row);
            let jrow: JoinRow = (0..arity)
                .map(|i| ScalarValue::try_from_array(batch.column(i), row).expect("temporal right scalar"))
                .collect();
            if track && !self.right_state.contains_key(&key) {
                delta += group_key_bytes(&key) as isize;
            }
            if track {
                delta += right_version_bytes(&jrow) as isize;
            }
            let replaced = self
                .right_state
                .entry(key)
                .or_default()
                .insert(times.value(row), (jrow, kinds.map_or(0, |k| k.value(row))));
            if track {
                if let Some((old, _)) = replaced {
                    delta -= right_version_bytes(&old) as isize; // last-write-wins per timestamp
                }
            }
        }
        self.memory.record(delta);
        self.memory.account()
    }

    /// Emits the joined rows for every buffered left row the watermark has passed and drops the build
    /// versions the watermark has made obsolete. Output is `[left data.., right data..]` + `$row_kind$`.
    pub(crate) fn advance(&mut self, watermark: i64) -> RecordBatch {
        let left_outer = self.join_type == JoinKind::LeftOuter;
        let has_pred = self.predicate.is_some();

        // Resolve each triggered left row to the build version valid at its time (an accumulate
        // version with the largest rightTime <= leftTime), collecting candidate pairs for one batched
        // predicate evaluation. Triggered rows are removed; later rows stay buffered.
        let mut decisions: Vec<(JoinRow, i8, Option<JoinRow>)> = Vec::new();
        let mut pred_pairs: Vec<JoinRow> = Vec::new();
        let mut pred_idx: Vec<usize> = Vec::new();
        let track = self.memory.tracking();
        let mut freed = 0usize;
        let keys: Vec<GroupKey> = self.left_state.keys().cloned().collect();
        for key in &keys {
            let entries = self.left_state.remove(key).expect("left key present");
            let versions = self.right_state.get(key);
            let mut remaining: Vec<LeftEntry> = Vec::new();
            for e in entries {
                if e.time > watermark {
                    remaining.push(e);
                    continue;
                }
                if track {
                    freed += left_entry_bytes(&e);
                }
                let valid = versions
                    .and_then(|m| m.range(..=e.time).next_back())
                    .and_then(|(_, (row, kind))| {
                        // Only an accumulate version (+I/+U) is a row; a -D/-U marks "no row here".
                        (*kind == 0 || *kind == 2).then(|| row.clone())
                    });
                let idx = decisions.len();
                if has_pred {
                    if let Some(row) = &valid {
                        pred_pairs.push(e.row.iter().chain(row).cloned().collect());
                        pred_idx.push(idx);
                    }
                }
                decisions.push((e.row, e.kind, valid));
            }
            if remaining.is_empty() {
                if track {
                    freed += group_key_bytes(key);
                }
            } else {
                self.left_state.insert(key.clone(), remaining);
            }
        }

        // A candidate that fails the residual non-equi predicate is not a match (Flink's
        // `joinCondition.apply`), so it falls back to a null-pad (LEFT) or is dropped (INNER).
        if has_pred && !pred_pairs.is_empty() {
            let joined = UpdatingJoiner::joined_schema(&self.left_schema, &self.right_schema);
            let mask = self.predicate.as_mut().expect("predicate present").evaluate(&joined, &pred_pairs);
            for (k, &idx) in pred_idx.iter().enumerate() {
                if !mask.get(k).copied().unwrap_or(false) {
                    decisions[idx].2 = None;
                }
            }
        }

        let right_nulls: JoinRow = self.right_types().iter().map(null_scalar).collect();
        let mut out_rows: Vec<JoinRow> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        for (left, kind, valid) in decisions {
            match valid {
                Some(right) => {
                    out_rows.push(left.into_iter().chain(right).collect());
                    out_kinds.push(kind);
                }
                None if left_outer => {
                    out_rows.push(left.into_iter().chain(right_nulls.iter().cloned()).collect());
                    out_kinds.push(kind);
                }
                None => {}
            }
        }

        // Drop versions older than the latest one still valid at the watermark; keep that one and all
        // newer (Flink always keeps at least the latest version).
        for versions in self.right_state.values_mut() {
            if let Some((&keep_from, _)) = versions.range(..=watermark).next_back() {
                let stale: Vec<i64> = versions.range(..keep_from).map(|(&t, _)| t).collect();
                for t in stale {
                    if let Some((old, _)) = versions.remove(&t) {
                        if track {
                            freed += right_version_bytes(&old);
                        }
                    }
                }
            }
        }
        self.memory.forget(freed);
        self.memory.account_shrink();

        if out_rows.is_empty() {
            return empty_batch();
        }
        let types: Vec<DataType> = self.left_types().into_iter().chain(self.right_types()).collect();
        let mut fields: Vec<Field> =
            (0..types.len()).map(|j| Field::new(format!("c{j}"), types[j].clone(), true)).collect();
        let mut columns: Vec<ArrayRef> = (0..types.len())
            .map(|j| scalars_to_array(out_rows.iter().map(|r| r[j].clone()).collect(), &types[j]))
            .collect();
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build temporal-join output batch")
    }

    /// Serializes one side's buffered rows as `[data cols.., __time__, __kind__]` (empty when none).
    fn serialize_left(&self) -> Vec<u8> {
        let mut rows: Vec<&JoinRow> = Vec::new();
        let mut times: Vec<i64> = Vec::new();
        let mut kinds: Vec<i8> = Vec::new();
        for entries in self.left_state.values() {
            for e in entries {
                rows.push(&e.row);
                times.push(e.time);
                kinds.push(e.kind);
            }
        }
        Self::write_side(&self.left_schema, &rows, &times, &kinds)
    }

    fn serialize_right(&self) -> Vec<u8> {
        let mut rows: Vec<&JoinRow> = Vec::new();
        let mut times: Vec<i64> = Vec::new();
        let mut kinds: Vec<i8> = Vec::new();
        for versions in self.right_state.values() {
            for (&t, (row, kind)) in versions {
                rows.push(row);
                times.push(t);
                kinds.push(*kind);
            }
        }
        Self::write_side(&self.right_schema, &rows, &times, &kinds)
    }

    fn write_side(schema: &SchemaRef, rows: &[&JoinRow], times: &[i64], kinds: &[i8]) -> Vec<u8> {
        if rows.is_empty() {
            return Vec::new();
        }
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| scalars_to_array(rows.iter().map(|r| r[j].clone()).collect(), fields[j].data_type()))
            .collect();
        fields.push(Field::new("__time__", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(times.to_vec())));
        fields.push(Field::new("__kind__", DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(kinds.to_vec())));
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("temporal side"))
    }

    pub(crate) fn snapshot(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for section in [self.serialize_left(), self.serialize_right()] {
            out.extend_from_slice(&(section.len() as u32).to_le_bytes());
            out.extend_from_slice(&section);
        }
        out
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_time: usize,
        right_time: usize,
        join_type: JoinKind,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        predicate: Option<JoinPredicate>,
        bytes: &[u8],
    ) -> Self {
        let mut joiner = TemporalJoiner::new(
            left_keys, right_keys, left_time, right_time, join_type, left_schema, right_schema,
            predicate,
        );
        let sections = read_framed_sections(bytes);
        for batch in read_ipc_if_present(&sections[0]) {
            let arity = batch.num_columns() - 2;
            let times = column_i64(&batch, "__time__");
            let kinds = batch
                .column_by_name("__kind__")
                .expect("__kind__")
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("__kind__ i8");
            let key_arrays: Vec<&ArrayRef> = joiner.left_keys.iter().map(|&i| batch.column(i)).collect();
            for row in 0..batch.num_rows() {
                let key = read_key(&key_arrays, row);
                let jrow: JoinRow = (0..arity)
                    .map(|i| ScalarValue::try_from_array(batch.column(i), row).expect("temporal left scalar"))
                    .collect();
                joiner.left_state.entry(key).or_default().push(LeftEntry {
                    row: jrow,
                    time: times.value(row),
                    kind: kinds.value(row),
                });
            }
        }
        for batch in read_ipc_if_present(&sections[1]) {
            let arity = batch.num_columns() - 2;
            let times = column_i64(&batch, "__time__");
            let kinds = batch
                .column_by_name("__kind__")
                .expect("__kind__")
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("__kind__ i8");
            let key_arrays: Vec<&ArrayRef> = joiner.right_keys.iter().map(|&i| batch.column(i)).collect();
            for row in 0..batch.num_rows() {
                let key = read_key(&key_arrays, row);
                let jrow: JoinRow = (0..arity)
                    .map(|i| ScalarValue::try_from_array(batch.column(i), row).expect("temporal right scalar"))
                    .collect();
                joiner
                    .right_state
                    .entry(key)
                    .or_default()
                    .insert(times.value(row), (jrow, kinds.value(row)));
            }
        }
        joiner
    }
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_temporalJoinerStateBytes, TemporalJoiner);

/// Creates an event-time temporal-table joiner (`FOR SYSTEM_TIME AS OF probe.rowtime`) and returns an
/// opaque handle. `left_time`/`right_time` locate the rowtime column on each side; the two schema
/// addresses seed the per-side data schemas (so a LEFT join can type the null-padding); the encoded
/// arrays carry the optional residual non-equi predicate. The JVM owns the handle and must release it
/// with the matching close.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createTemporalJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_time: jint,
    right_time: jint,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let left_schema = import_schema(left_schema_address);
    let right_schema = import_schema(right_schema_address);
    let predicate = read_join_predicate(
        &mut env, &pred_kinds, &pred_payload, &pred_child_counts, &pred_longs, &pred_doubles,
        &pred_strings,
    );
    let joiner = TemporalJoiner::new(
        left,
        right,
        left_time as usize,
        right_time as usize,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
        predicate,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, joiner)
}

/// Buffers a probe-side (left) batch (no output until a watermark).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushLeftTemporalJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        joiner.push_left(&batch)
    };
    if let Err(e) = result {
        throw_memory_limit(&mut env, &e.to_string());
    }
}

/// Folds a build-side (right) changelog batch into the versioned state (no output until a watermark).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushRightTemporalJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        joiner.push_right(&batch)
    };
    if let Err(e) = result {
        throw_memory_limit(&mut env, &e.to_string());
    }
}

/// Advances the watermark, emitting the joined rows for buffered probe rows it has passed and dropping
/// obsolete build versions.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_advanceTemporalJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
    let result = joiner.advance(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases the temporal joiner and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeTemporalJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<TemporalJoiner>(handle));
    }
}

/// Serializes the joiner's buffered probe rows and versioned build state for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotTemporalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let joiner = unsafe { &*(handle as *mut TemporalJoiner) };
    env.byte_array_from_slice(&joiner.snapshot())
        .expect("failed to allocate temporal-join snapshot array")
        .into_raw()
}

/// Rebuilds a temporal joiner from a snapshot taken by a prior run and returns a fresh handle.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreTemporalJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_time: jint,
    right_time: jint,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let left_schema = import_schema(left_schema_address);
    let right_schema = import_schema(right_schema_address);
    let predicate = read_join_predicate(
        &mut env, &pred_kinds, &pred_payload, &pred_child_counts, &pred_longs, &pred_doubles,
        &pred_strings,
    );
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read temporal-join snapshot");
    let joiner = TemporalJoiner::restore(
        left,
        right,
        left_time as usize,
        right_time as usize,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
        predicate,
        &bytes,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, joiner)
}
