use crate::*;

/// Buffered rows of one side of a window join, grouped by window then by equi-join key.
/// Event-time INNER window join: the join of two windowing-TVF inputs on their equi-join key within
/// the same window — `a JOIN b ON a.k = b.k` where both sides carry matching `window_start` /
/// `window_end` columns (assigned upstream by identical `TUMBLE`/`HOP`/`CUMULATE` windows).
///
/// Input batches are buffered per side; on a watermark, the rows whose window has closed (its end at
/// or before the watermark) are joined and evicted. The window equality is folded into the equi-keys
/// — `window_start` and `window_end` join alongside the user key — so a single hash join over the
/// closed rows matches only within a window. Late rows for an already-closed window produce no
/// further output, matching Flink's watermark semantics.
pub(crate) struct WindowJoiner {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    left_wstart: usize,
    left_wend: usize,
    right_wstart: usize,
    right_wend: usize,
    predicate: Option<JoinPredicate>,
    join_type: JoinKind,
    // Eager data schemas, seeded at construction so an outer join can type the null-padding for a side
    // that never saw a row.
    left_data_schema: SchemaRef,
    right_data_schema: SchemaRef,
    left_schema: Option<SchemaRef>,
    right_schema: Option<SchemaRef>,
    left_buffered: Vec<RecordBatch>,
    right_buffered: Vec<RecordBatch>,
    memory: OperatorMemory,
}

impl WindowJoiner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_wstart: usize,
        left_wend: usize,
        right_wstart: usize,
        right_wend: usize,
        predicate: Option<JoinPredicate>,
        join_type: JoinKind,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
    ) -> Self {
        WindowJoiner {
            left_keys,
            right_keys,
            left_wstart,
            left_wend,
            right_wstart,
            right_wend,
            predicate,
            join_type,
            left_data_schema,
            right_data_schema,
            left_schema: None,
            right_schema: None,
            left_buffered: Vec::new(),
            right_buffered: Vec::new(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds the buffered rows by the operator's managed-memory budget (negative = unaccounted),
    /// accounting any restored buffers immediately.
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        if budget_bytes >= 0 {
            self.memory.attach("window-join", budget_bytes, 0)?;
            self.account()?;
        }
        Ok(self)
    }

    /// Re-accounts the buffered batches (recounted per batch, not per row).
    fn account(&mut self) -> Result<(), DataFusionError> {
        if self.memory.tracking() {
            self.memory.set(
                buffered_batches_bytes(&self.left_buffered)
                    + buffered_batches_bytes(&self.right_buffered),
            );
            self.memory.account()?;
        }
        Ok(())
    }

    pub(crate) fn push_left(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        self.left_schema = Some(batch.schema());
        self.left_buffered.push(batch);
        self.account()
    }

    pub(crate) fn push_right(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        self.right_schema = Some(batch.schema());
        self.right_buffered.push(batch);
        self.account()
    }

    /// Splits a side's buffer into the rows whose window has closed (`window_end <= watermark`,
    /// returned) and the rest (kept buffered). `None` if the side has not seen any rows.
    fn split_closed(
        buffered: &mut Vec<RecordBatch>,
        schema: &Option<SchemaRef>,
        wend: usize,
        watermark: i64,
    ) -> Option<RecordBatch> {
        let schema = schema.as_ref()?;
        if buffered.is_empty() {
            return None;
        }
        let all = concat_batches(schema, buffered.iter()).expect("concat window-join buffer");
        let ends = rt_to_millis(all.column(wend));
        let closed_mask: BooleanArray = ends.iter().map(|v| Some(v.unwrap() <= watermark)).collect();
        let closed = filter_record_batch(&all, &closed_mask).expect("filter closed windows");
        let pending_mask = arrow::compute::not(&closed_mask).expect("negate window mask");
        let pending = filter_record_batch(&all, &pending_mask).expect("filter pending windows");
        *buffered = if pending.num_rows() > 0 { vec![pending] } else { Vec::new() };
        Some(closed)
    }

    /// Joins and evicts the windows the watermark has closed. For an outer join the unmatched rows of
    /// the closed windows are null-padded here too: because a window's rows on both sides close in the
    /// same flush, the INNER join over the closed rows sees every potential match, so a closed row that
    /// does not appear in it never matched. Empty batch when nothing is emitted. Fallible because
    /// the join's working memory draws on the operator's budget.
    pub(crate) fn flush(&mut self, watermark: i64) -> Result<RecordBatch, DataFusionError> {
        let left = Self::split_closed(&mut self.left_buffered, &self.left_schema, self.left_wend, watermark);
        let right =
            Self::split_closed(&mut self.right_buffered, &self.right_schema, self.right_wend, watermark);
        self.account().expect("closing windows only shrinks the buffers");
        // Join on the user keys plus the window bounds, so only rows of the same window match.
        let mut on: Vec<(usize, usize)> =
            self.left_keys.iter().zip(&self.right_keys).map(|(&l, &r)| (l, r)).collect();
        on.push((self.left_wstart, self.right_wstart));
        on.push((self.left_wend, self.right_wend));
        let filter =
            residual_filter(&self.left_data_schema, &self.right_data_schema, None, self.predicate.as_mut());

        if self.join_type == JoinKind::Inner {
            return match (left, right) {
                (Some(left), Some(right)) if left.num_rows() > 0 && right.num_rows() > 0 => {
                    hash_join_inner(left, right, &on, filter, self.memory.task_ctx())
                }
                _ => Ok(empty_batch()),
            };
        }

        // Outer: tag the closed rows with transient row-ids (== row index), join, and from the matched
        // row-ids null-pad the closed rows of each outer side that never appeared in a pair.
        let left_closed = left.filter(|b| b.num_rows() > 0);
        let right_closed = right.filter(|b| b.num_rows() > 0);
        let left_types: Vec<DataType> =
            self.left_data_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let right_types: Vec<DataType> =
            self.right_data_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let mut outputs: Vec<RecordBatch> = Vec::new();
        let mut matched_left: HashSet<i64> = HashSet::default();
        let mut matched_right: HashSet<i64> = HashSet::default();
        if let (Some(left), Some(right)) = (&left_closed, &right_closed) {
            let (mut lc, mut rc) = (0i64, 0i64);
            let joined = hash_join_inner(
                append_rowids(left, &mut lc),
                append_rowids(right, &mut rc),
                &on,
                filter,
                self.memory.task_ctx(),
            )?;
            if joined.num_rows() > 0 {
                let total = joined.num_columns();
                let lrid = joined.column(left_types.len()).as_any().downcast_ref::<Int64Array>().expect("lrid");
                let rrid = joined.column(total - 1).as_any().downcast_ref::<Int64Array>().expect("rrid");
                for i in 0..joined.num_rows() {
                    matched_left.insert(lrid.value(i));
                    matched_right.insert(rrid.value(i));
                }
                let keep: Vec<usize> =
                    (0..left_types.len()).chain(left_types.len() + 1..total - 1).collect();
                let fields: Vec<Field> = keep
                    .iter()
                    .enumerate()
                    .map(|(j, &i)| Field::new(format!("c{j}"), joined.schema().field(i).data_type().clone(), true))
                    .collect();
                let columns: Vec<ArrayRef> = keep.iter().map(|&i| joined.column(i).clone()).collect();
                outputs.push(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("window pairs"));
            }
        }
        if self.join_type.left_is_outer() {
            if let Some(left) = &left_closed {
                if let Some(pad) = unmatched_null_pad(left, &matched_left, &left_types, &right_types, true) {
                    outputs.push(pad);
                }
            }
        }
        if self.join_type.right_is_outer() {
            if let Some(right) = &right_closed {
                if let Some(pad) = unmatched_null_pad(right, &matched_right, &left_types, &right_types, false) {
                    outputs.push(pad);
                }
            }
        }
        Ok(match outputs.len() {
            0 => empty_batch(),
            1 => outputs.pop().expect("one output"),
            _ => concat_batches(&outputs[0].schema(), outputs.iter()).expect("concat window outputs"),
        })
    }

    /// Serializes both buffers (`[u32 left_len][left ipc][right ipc]`) for a checkpoint.
    pub(crate) fn snapshot(&self) -> Vec<u8> {
        let serialize = |schema: &Option<SchemaRef>, buffered: &[RecordBatch]| match schema {
            Some(schema) if !buffered.is_empty() => {
                write_ipc(&concat_batches(schema, buffered.iter()).expect("concat window-join buffer"))
            }
            _ => Vec::new(),
        };
        let left = serialize(&self.left_schema, &self.left_buffered);
        let right = serialize(&self.right_schema, &self.right_buffered);
        let mut out = (left.len() as u32).to_le_bytes().to_vec();
        out.extend_from_slice(&left);
        out.extend_from_slice(&right);
        out
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_wstart: usize,
        left_wend: usize,
        right_wstart: usize,
        right_wend: usize,
        predicate: Option<JoinPredicate>,
        join_type: JoinKind,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
        bytes: &[u8],
    ) -> Self {
        let mut joiner = WindowJoiner::new(
            left_keys,
            right_keys,
            left_wstart,
            left_wend,
            right_wstart,
            right_wend,
            predicate,
            join_type,
            left_data_schema,
            right_data_schema,
        );
        let left_len = u32::from_le_bytes(bytes[0..4].try_into().expect("snapshot len")) as usize;
        for batch in read_ipc_if_present(&bytes[4..4 + left_len]) {
            joiner.left_schema = Some(batch.schema());
            joiner.left_buffered.push(batch);
        }
        for batch in read_ipc_if_present(&bytes[4 + left_len..]) {
            joiner.right_schema = Some(batch.schema());
            joiner.right_buffered.push(batch);
        }
        joiner
    }
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_windowJoinerStateBytes, WindowJoiner);

/// Creates an event-time INNER window joiner and returns an opaque handle. The key/window column
/// indices locate the equi-join key and the `window_start`/`window_end` columns within each side's
/// input batch. The JVM owns the handle across calls and must release it with the matching close.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createWindowJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_window_start: jint,
    left_window_end: jint,
    right_window_start: jint,
    right_window_end: jint,
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
        &mut env,
        &pred_kinds,
        &pred_payload,
        &pred_child_counts,
        &pred_longs,
        &pred_doubles,
        &pred_strings,
    );
    let joiner = WindowJoiner::new(
        left,
        right,
        left_window_start as usize,
        left_window_end as usize,
        right_window_start as usize,
        right_window_end as usize,
        predicate,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, joiner)
}

/// Buffers a left batch (no output); its rows are joined later when the watermark closes their window.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushLeftWindowJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
    // The pushed batch is retained in the buffer (not dropped), so no JVM release upcall runs
    // between a failed account and the throw (see updateTumblingAggregator).
    let result = joiner.push_left(import_record_batch(in_array_address, in_schema_address));
    if let Err(e) = result {
        throw_memory_limit(&mut env, &e.to_string());
    }
}

/// Buffers a right batch (no output).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushRightWindowJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
    // The pushed batch is retained in the buffer (not dropped), so no JVM release upcall runs
    // between a failed account and the throw (see updateTumblingAggregator).
    let result = joiner.push_right(import_record_batch(in_array_address, in_schema_address));
    if let Err(e) = result {
        throw_memory_limit(&mut env, &e.to_string());
    }
}

/// Exports the INNER matches of every window the watermark has closed (then evicts those windows).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushWindowJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
    match joiner.flush(watermark_millis) {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Releases the window joiner and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeWindowJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<WindowJoiner>(handle));
    }
}

/// Serializes the window joiner's buffered rows for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let joiner = unsafe { &*(handle as *mut WindowJoiner) };
    env.byte_array_from_slice(&joiner.snapshot())
        .expect("failed to allocate window-join snapshot array")
        .into_raw()
}

/// Rebuilds a window joiner from a snapshot taken by a prior run and returns a fresh handle.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreWindowJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_window_start: jint,
    left_window_end: jint,
    right_window_start: jint,
    right_window_end: jint,
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
        &mut env,
        &pred_kinds,
        &pred_payload,
        &pred_child_counts,
        &pred_longs,
        &pred_doubles,
        &pred_strings,
    );
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read window-join snapshot");
    let joiner = WindowJoiner::restore(
        left,
        right,
        left_window_start as usize,
        left_window_end as usize,
        right_window_start as usize,
        right_window_end as usize,
        predicate,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
        &bytes,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, joiner)
}
