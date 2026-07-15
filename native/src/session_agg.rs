use crate::*;

/// One open session for a key: its end (the latest element's timestamp plus the gap) and the
/// incremental accumulators folding in its rows. The start is the map key that holds it.
pub(crate) struct Session {
    end: i64,
    accumulators: Vec<Box<dyn Accumulator>>,
}

/// Folds the state of one accumulator set into another, used when two sessions merge into one.
pub(crate) fn merge_into(into: &mut [Box<dyn Accumulator>], mut from: Vec<Box<dyn Accumulator>>) {
    for (target, source) in into.iter_mut().zip(from.iter_mut()) {
        let state: Vec<ArrayRef> =
            source.state().expect("state").into_iter().map(|s| s.to_array().expect("scalar")).collect();
        target.merge_batch(&state).expect("failed to merge session");
    }
}

/// Event-time session-window aggregation. Unlike the fixed-bin tumbling/hopping windows, sessions
/// are dynamic and per key: each element opens a `[ts, ts + gap)` window that merges with any
/// existing session it intersects, so a single element can bridge two sessions into one. A session
/// is finalized once a watermark passes its end. The connected-components result this produces is
/// order-independent, matching the host's merging window assigner.
pub(crate) struct SessionAggregator {
    gap_millis: i64,
    aggregates: Vec<WindowAggregate>,
    // Keyed by the arrow-row memcomparable key encoding, like the other aggregators (see
    // `TumblingAggregator::key_converter`).
    sessions: HashMap<OwnedRow, BTreeMap<i64, Session>>,
    key_converter: Option<RowConverter>,
    key_types: Vec<DataType>,
    memory: OperatorMemory,
}

/// Estimated heap footprint of one open session (its accumulators plus the map entry).
pub(crate) fn session_bytes(session: &Session) -> usize {
    accumulators_bytes(&session.accumulators) + GROUP_ENTRY_OVERHEAD
}

impl SessionAggregator {
    pub(crate) fn new(gap_millis: i64, value_types: Vec<i64>, kinds: Vec<i64>) -> Self {
        SessionAggregator {
            gap_millis,
            aggregates: build_aggregates(&kinds, &value_types),
            sessions: HashMap::default(),
            key_converter: None,
            key_types: Vec::new(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds this aggregator's state by the operator's managed-memory budget (negative =
    /// unaccounted), accounting any restored sessions immediately.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .sessions
            .iter()
            .map(|(key, map)| {
                owned_row_bytes(key) + map.values().map(session_bytes).sum::<usize>()
            })
            .sum();
        self.memory.attach("session-aggregate", budget_bytes, state)?;
        Ok(self)
    }

    pub(crate) fn update(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let ts = column_i64(batch, "ts");
        // One value column per aggregate (value0, value1, …); each accumulator reads its own.
        let values: Vec<&ArrayRef> = (0..self.aggregates.len())
            .map(|i| batch.column_by_name(&format!("value{i}")).expect("missing value column"))
            .collect();
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());

        // Group row positions per key, then segment each key's rows (in timestamp order) into
        // gap-connected runs — within a run every row is within `gap` of the next, so the run forms
        // a single candidate session and its value slice + accumulator update happen once, not once
        // per row (Arroyo's session operator likewise partitions the batch per key and feeds
        // sessions batch slices). The runs are exactly the connected components the row-at-a-time
        // walk would build, so merging each run against the stored sessions gives the same result.
        let mut by_key: ahash::HashMap<Row<'_>, Vec<u32>> = ahash::HashMap::default();
        for row in 0..batch.num_rows() {
            by_key.entry(keys_encoded.row(row)).or_default().push(row as u32);
        }
        let track = self.memory.tracking();
        for (key, mut rows) in by_key {
            rows.sort_by_key(|&row| ts.value(row as usize));
            let key = key.owned();
            let mut delta = 0isize;
            if track && !self.sessions.contains_key(&key) {
                delta += owned_row_bytes(&key) as isize;
            }
            let map = self.sessions.entry(key).or_default();
            let mut run_start = 0;
            while run_start < rows.len() {
                let mut run_end = run_start + 1;
                let mut last_ts = ts.value(rows[run_start] as usize);
                while run_end < rows.len()
                    && ts.value(rows[run_end] as usize) <= last_ts + self.gap_millis
                {
                    last_ts = ts.value(rows[run_end] as usize);
                    run_end += 1;
                }
                let candidate_start = ts.value(rows[run_start] as usize);
                let candidate_end = last_ts + self.gap_millis;
                // Restore arrival order within the run so accumulators fold rows in the same order
                // as the input batch (float sums are order-sensitive bitwise).
                let mut run_rows = rows[run_start..run_end].to_vec();
                run_rows.sort_unstable();
                let indices = UInt32Array::from(run_rows);
                let run_values: Vec<ArrayRef> =
                    values.iter().map(|v| take(v, &indices, None).expect("take value")).collect();

                // Existing sessions are maximal and pairwise separated, but a run's candidate window
                // can still straddle more than one, so absorb every session it intersects.
                // Intersection is inclusive at the bounds (a gap of exactly `gap` still merges),
                // matching the host's `TimeWindow.intersects`. Separation means starts and ends are
                // sorted together, so the intersecting sessions are a contiguous tail of the starts
                // at or before `candidate_end`: walk it backwards and stop at the first session that
                // ends before the candidate — a bounded probe instead of a scan of every open
                // session, which dominates when a key holds many not-yet-closed sessions.
                let mut overlapping: Vec<i64> = map
                    .range(..=candidate_end)
                    .rev()
                    .take_while(|(_, session)| session.end >= candidate_start)
                    .map(|(start, _)| *start)
                    .collect();
                overlapping.reverse();

                let mut start = candidate_start;
                let mut end = candidate_end;
                let mut accumulators: Vec<Box<dyn Accumulator>> =
                    self.aggregates.iter().map(WindowAggregate::create_accumulator).collect();
                for overlap in overlapping {
                    let session = map.remove(&overlap).expect("session present");
                    if track {
                        delta -= session_bytes(&session) as isize;
                    }
                    start = start.min(overlap);
                    end = end.max(session.end);
                    merge_into(&mut accumulators, session.accumulators);
                }
                for (i, accumulator) in accumulators.iter_mut().enumerate() {
                    accumulator.update_batch(std::slice::from_ref(&run_values[i])).expect("update");
                }
                let session = Session { end, accumulators };
                if track {
                    delta += session_bytes(&session) as isize;
                }
                map.insert(start, session);
                run_start = run_end;
            }
            if track {
                self.memory.record(delta);
            }
        }
        self.memory.account()
    }

    /// Finalizes and removes sessions the watermark has closed, emitting
    /// `[key, window_start, window_end, result0..resultN-1]`. The end is the session's own bound,
    /// not a fixed offset, so it travels as its own column.
    pub(crate) fn flush(&mut self, watermark: i64) -> RecordBatch {
        let n = self.aggregates.len();
        let mut rows: Vec<(OwnedRow, i64, i64, Vec<ScalarValue>)> = Vec::new();
        let track = self.memory.tracking();
        let mut freed = 0usize;
        for (key, map) in self.sessions.iter_mut() {
            let closed: Vec<i64> =
                map.iter().filter(|(_, s)| s.end <= watermark).map(|(start, _)| *start).collect();
            for start in closed {
                let mut session = map.remove(&start).expect("session present");
                if track {
                    freed += session_bytes(&session);
                }
                let results = session
                    .accumulators
                    .iter_mut()
                    .map(|a| a.evaluate().expect("failed to finalize"))
                    .collect();
                rows.push((key.clone(), start, session.end, results));
            }
        }
        self.sessions.retain(|key, map| {
            if map.is_empty() {
                if track {
                    freed += owned_row_bytes(key);
                }
                return false;
            }
            true
        });
        if track {
            self.memory.forget(freed);
            self.memory.account_shrink();
        }
        rows.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));

        let keys: Vec<OwnedRow> = rows.iter().map(|(key, ..)| key.clone()).collect();
        let starts: Vec<i64> = rows.iter().map(|(_, start, ..)| *start).collect();
        let ends: Vec<i64> = rows.iter().map(|(_, _, end, _)| *end).collect();
        let mut fields = key_fields(&self.key_types);
        let mut columns = decode_keys(self.key_converter.as_ref(), &keys, &self.key_types);
        fields.push(Field::new("window_start", DataType::Int64, false));
        fields.push(Field::new("window_end", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(starts)));
        columns.push(Arc::new(Int64Array::from(ends)));
        for i in 0..n {
            let scalars: Vec<ScalarValue> = rows.iter().map(|(_, _, _, r)| r[i].clone()).collect();
            fields.push(Field::new(format!("result{i}"), self.aggregates[i].result_type(), false));
            columns.push(scalars_to_array(scalars, &self.aggregates[i].result_type()));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build result batch")
    }

    /// Serializes every open session (one row per (key, session): key, start, end, then each
    /// accumulator's state fields) with Arrow IPC, mirroring the tumbling checkpoint path.
    fn snapshot(&mut self) -> Vec<u8> {
        write_ipc(&self.snapshot_batch())
    }

    fn snapshot_batch(&mut self) -> RecordBatch {
        let state_fields: Vec<Field> =
            self.aggregates.iter().flat_map(WindowAggregate::state_fields).collect();

        let mut keys: Vec<OwnedRow> = Vec::new();
        let mut starts: Vec<i64> = Vec::new();
        let mut ends: Vec<i64> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); state_fields.len()];
        for (key, map) in self.sessions.iter_mut() {
            for (start, session) in map.iter_mut() {
                keys.push(key.clone());
                starts.push(*start);
                ends.push(session.end);
                let mut column = 0;
                for accumulator in session.accumulators.iter_mut() {
                    for scalar in accumulator.state().expect("state") {
                        state_columns[column].push(scalar);
                        column += 1;
                    }
                }
            }
        }

        let mut fields = key_fields(&self.key_types);
        let mut columns = decode_keys(self.key_converter.as_ref(), &keys, &self.key_types);
        fields.push(Field::new("window_start", DataType::Int64, false));
        fields.push(Field::new("window_end", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(starts)));
        columns.push(Arc::new(Int64Array::from(ends)));
        fields.extend(state_fields.iter().cloned());
        for (index, scalars) in state_columns.into_iter().enumerate() {
            columns.push(if scalars.is_empty() {
                new_empty_array(state_fields[index].data_type())
            } else {
                ScalarValue::iter_to_array(scalars).expect("state array")
            });
        }

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build snapshot batch")
    }

    pub(crate) fn snapshot_partitions(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        self.raw_snapshot_partitions(max_parallelism, timestamp_precisions)
    }

    fn raw_snapshot_partitions(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        let batch = self.snapshot_batch();
        let state_field_count: usize = self
            .aggregates
            .iter()
            .map(|aggregate| aggregate.state_fields().len())
            .sum();
        let key_count = batch.num_columns() - 2 - state_field_count;
        let key_columns: Vec<usize> = (0..key_count).collect();
        let mut rows_by_group: BTreeMap<i32, Vec<u32>> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            let key_group = flink_key_group(
                binary_row_hash(&batch, &key_columns, row, timestamp_precisions),
                max_parallelism,
            ) as i32;
            rows_by_group.entry(key_group).or_default().push(row as u32);
        }
        let mut snapshots = BTreeMap::new();
        for (key_group, rows) in rows_by_group {
            let indices = UInt32Array::from(rows);
            let columns = batch
                .columns()
                .iter()
                .map(|column| take(column, &indices, None).expect("partition session snapshot"))
                .collect();
            let partition = RecordBatch::try_new(batch.schema(), columns)
                .expect("partitioned session snapshot");
            snapshots.insert(key_group, write_ipc(&partition));
        }
        snapshots
    }

    fn restore(gap_millis: i64, value_types: Vec<i64>, kinds: Vec<i64>, bytes: &[u8]) -> Self {
        let mut aggregator = SessionAggregator::new(gap_millis, value_types, kinds);
        if bytes.is_empty() {
            return aggregator;
        }
        let field_counts: Vec<usize> =
            aggregator.aggregates.iter().map(|a| a.state_fields().len()).collect();
        let state_field_total: usize = field_counts.iter().sum();
        let reader = arrow::ipc::reader::StreamReader::try_new(bytes, None)
            .expect("failed to open snapshot reader");
        for batch in reader {
            let batch = batch.expect("failed to read snapshot");
            // Columns are [key0..key{arity-1}, window_start, window_end, state fields...].
            let arity = batch.num_columns() - 2 - state_field_total;
            let key_arrays: Vec<&ArrayRef> = (0..arity).map(|j| batch.column(j)).collect();
            aggregator.key_types = key_types(&key_arrays);
            let keys_encoded =
                encode_keys(&mut aggregator.key_converter, &key_arrays, batch.num_rows());
            let starts = batch
                .column(arity)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("window_start int64");
            let ends = batch
                .column(arity + 1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("window_end int64");
            for row in 0..batch.num_rows() {
                let mut accumulators: Vec<Box<dyn Accumulator>> =
                    aggregator.aggregates.iter().map(WindowAggregate::create_accumulator).collect();
                let mut column = arity + 2;
                for (i, accumulator) in accumulators.iter_mut().enumerate() {
                    let count = field_counts[i];
                    let state: Vec<ArrayRef> =
                        (column..column + count).map(|c| batch.column(c).slice(row, 1)).collect();
                    accumulator.merge_batch(&state).expect("failed to restore session");
                    column += count;
                }
                aggregator
                    .sessions
                    .entry(keys_encoded.row(row).owned())
                    .or_default()
                    .insert(starts.value(row), Session { end: ends.value(row), accumulators });
            }
        }
        aggregator
    }

    pub(crate) fn restore_partitions(
        gap_millis: i64,
        value_types: Vec<i64>,
        kinds: Vec<i64>,
        snapshots: &[Vec<u8>],
    ) -> Self {
        let batches: Vec<RecordBatch> = snapshots
            .iter()
            .flat_map(|bytes| read_ipc_if_present(bytes))
            .collect();
        let snapshot = batches.first().map(|first| {
            write_ipc(
                &concat_batches(&first.schema(), batches.iter())
                    .expect("merge session raw partitions"),
            )
        });
        SessionAggregator::restore(
            gap_millis,
            value_types,
            kinds,
            snapshot.as_deref().unwrap_or_default(),
        )
    }
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_sessionAggregatorStateBytes, SessionAggregator);

/// Creates a stateful session-window aggregator and returns an opaque handle. As with the tumbling
/// handle, the JVM owns the native state across calls and must release it with the matching close.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createSessionAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    gap_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let aggregator = SessionAggregator::new(gap_millis, value_types, kinds)
        .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}

/// Folds a batch from the JVM into the session aggregator, merging sessions as elements bridge them.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updateSessionAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        aggregator.update(&batch)
    };
    if let Err(e) = result {
        throw_memory_limit(&mut env, &e.to_string());
    }
}

/// Emits the sessions the given watermark has closed as a batch and drops them from state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushSessionAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
    let result = aggregator.flush(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases the session aggregator and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeSessionAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<SessionAggregator>(handle));
    }
}

/// Serializes the aggregator's open sessions so the JVM can store them in a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
    env.byte_array_from_slice(&aggregator.snapshot())
        .expect("failed to allocate snapshot array")
        .into_raw()
}

/// Rebuilds a session aggregator from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreSessionAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    gap_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read snapshot");
    let aggregator = SessionAggregator::restore(gap_millis, value_types, kinds, &bytes)
        .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotSessionAggregatorPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    keyed_state_partition_array(
        &mut env,
        aggregator.snapshot_partitions(max_parallelism as usize, &precisions),
        "session-window",
    )
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreSessionAggregatorPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    gap_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let count = env
        .get_array_length(&snapshots)
        .expect("read session raw partition count");
    let mut restored = Vec::with_capacity(count as usize);
    for index in 0..count {
        let bytes = JByteArray::from(
            env.get_object_array_element(&snapshots, index)
                .expect("read session raw partition"),
        );
        restored.push(
            env.convert_byte_array(&bytes)
                .expect("read session raw partition bytes"),
        );
    }
    let aggregator = SessionAggregator::restore_partitions(gap_millis, value_types, kinds, &restored)
        .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}
