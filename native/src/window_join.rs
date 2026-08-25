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
    pub(crate) current_watermark: i64,
    pub(crate) left_late_drops: u64,
    pub(crate) right_late_drops: u64,
    pub(crate) memory: OperatorMemory,
    /// Persistent-state mode: both sides' buffered rows live in the persistent store (write buffers
    /// + disk tables); the in-memory `*_buffered` vectors stay empty between calls.
    #[cfg(feature = "rocksdb-state")]
    store: Option<crate::state::RocksWindowBuffer>,
    key_timestamp_precisions: Vec<i32>,
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
        let key_arity = left_keys.len();
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
            current_watermark: i64::MIN,
            left_late_drops: 0,
            right_late_drops: 0,
            memory: OperatorMemory::unaccounted(),
            #[cfg(feature = "rocksdb-state")]
            store: None,
            // Equi-join key columns have matching types on both sides, so one precision stream
            // serves both (the raw snapshot partitioner already relies on this).
            key_timestamp_precisions: vec![-1; key_arity],
        }
    }

    pub(crate) fn with_key_timestamp_precisions(
        mut self,
        key_timestamp_precisions: Vec<i32>,
    ) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        self
    }

    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_store(mut self, store: crate::state::RocksWindowBuffer) -> Self {
        self.store = Some(store);
        self
    }

    /// Attaches the task off-heap budget for a backend that starts with nothing resident.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory.attach("window-join", budget_bytes, 0)?;
        Ok(self)
    }

    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn store_mut(&mut self) -> &mut crate::state::RocksWindowBuffer {
        self.store.as_mut().expect("window-join rocksdb store")
    }

    /// Bounds the buffered rows by the operator's task off-heap budget (negative = unaccounted),
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
        let (batch, dropped) = Self::filter_late(batch, self.left_wend, self.current_watermark)?;
        self.left_late_drops += dropped as u64;
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return self.push_store(batch, true);
        }
        self.left_buffered.push(batch);
        self.account()
    }

    pub(crate) fn push_right(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        self.right_schema = Some(batch.schema());
        let (batch, dropped) = Self::filter_late(batch, self.right_wend, self.current_watermark)?;
        self.right_late_drops += dropped as u64;
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return self.push_store(batch, false);
        }
        self.right_buffered.push(batch);
        self.account()
    }

    fn filter_late(
        batch: RecordBatch,
        window_end_column: usize,
        watermark: i64,
    ) -> Result<(RecordBatch, usize), DataFusionError> {
        let input_rows = batch.num_rows();
        let ends = rt_to_millis(batch.column(window_end_column));
        let live: BooleanArray = ends
            .iter()
            .map(|end| Some(end.is_some_and(|end| end > watermark)))
            .collect();
        let batch = filter_record_batch(&batch, &live)?;
        let late_rows = input_rows - batch.num_rows();
        Ok((batch, late_rows))
    }

    /// Persistent-state arrival path: every input row appends to its side's store table under
    /// a fresh arrival sequence, valued by its window end (the fire column) and routed by the
    /// equi-join key's group. Nothing joins here — emission is watermark-driven (`flush`).
    #[cfg(feature = "rocksdb-state")]
    fn push_store(&mut self, batch: RecordBatch, left: bool) -> Result<(), DataFusionError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let (keys, wend) = if left {
            (&self.left_keys, self.left_wend)
        } else {
            (&self.right_keys, self.right_wend)
        };
        let ends = rt_to_millis(batch.column(wend));
        self.store
            .as_mut()
            .expect("window-join rocksdb store")
            .push(left, &batch, keys, &self.key_timestamp_precisions, &ends)
    }

    /// Persistent-state firing path: each side's table scan removes and returns the rows of every
    /// closed window, reassembled in arrival order, and the memory path's own join runs over them.
    #[cfg(feature = "rocksdb-state")]
    fn flush_store(&mut self, watermark: i64) -> Result<RecordBatch, DataFusionError> {
        let (left_schema, right_schema) = self.side_schemas();
        let store = self.store.as_mut().expect("window-join rocksdb store");
        let left = store.take_closed(true, watermark, &left_schema)?;
        let right = store.take_closed(false, watermark, &right_schema)?;
        self.join_closed(left, right)
    }

    /// Each side's learned input schema (the declared data schema until its first batch), which
    /// store-reconstructed batches carry so they match what the memory path would have buffered.
    #[cfg(feature = "rocksdb-state")]
    fn side_schemas(&self) -> (SchemaRef, SchemaRef) {
        (
            self.left_schema
                .clone()
                .unwrap_or_else(|| self.left_data_schema.clone()),
            self.right_schema
                .clone()
                .unwrap_or_else(|| self.right_data_schema.clone()),
        )
    }

    /// Decodes restored blob key groups once at open and appends them through the typed store, so
    /// a canonical or raw restore continues on the direct persistent path. Blob order is the
    /// memory restore's order, so the appended sequences reproduce its arrival order per side;
    /// the processing-time deadline arrives from the host's restored timer frame.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn import_partitions(
        &mut self,
        snapshots: &[Vec<u8>],
        timer_deadline: i64,
    ) -> Result<(), DataFusionError> {
        for bytes in snapshots {
            if bytes.len() < 4 {
                continue;
            }
            let left_len =
                u32::from_le_bytes(bytes[0..4].try_into().expect("snapshot len")) as usize;
            assert!(
                4 + left_len <= bytes.len(),
                "truncated window-join raw key-group snapshot"
            );
            for (left, section) in [
                (true, &bytes[4..4 + left_len]),
                (false, &bytes[4 + left_len..]),
            ] {
                for batch in read_ipc_if_present(section) {
                    if left {
                        self.left_schema = Some(batch.schema());
                    } else {
                        self.right_schema = Some(batch.schema());
                    }
                    self.push_store(batch, left)?;
                }
            }
        }
        self.store_mut().adopt_restored(timer_deadline);
        Ok(())
    }

    /// The complete buffered state in the memory snapshot's per-key-group encoding, for
    /// backend-independent canonical savepoints (see `snapshot_partitions`).
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn canonical_partitions(
        &mut self,
    ) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError> {
        let (left_schema, right_schema) = self.side_schemas();
        let store = self.store.as_ref().expect("window-join rocksdb store");
        let left = store.rows_by_group(true, &left_schema)?;
        let right = store.rows_by_group(false, &right_schema)?;
        let mut groups: Vec<i32> = left.keys().chain(right.keys()).copied().collect();
        groups.sort_unstable();
        groups.dedup();
        let mut snapshots = BTreeMap::new();
        for key_group in groups {
            snapshots.insert(
                key_group,
                Self::snapshot_parts(
                    left.get(&key_group).map(write_ipc).unwrap_or_default(),
                    right.get(&key_group).map(write_ipc).unwrap_or_default(),
                ),
            );
        }
        Ok(snapshots)
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
        let closed_mask: BooleanArray =
            ends.iter().map(|v| Some(v.unwrap() <= watermark)).collect();
        let closed = filter_record_batch(&all, &closed_mask).expect("filter closed windows");
        let pending_mask = arrow::compute::not(&closed_mask).expect("negate window mask");
        let pending = filter_record_batch(&all, &pending_mask).expect("filter pending windows");
        *buffered = if pending.num_rows() > 0 {
            vec![pending]
        } else {
            Vec::new()
        };
        Some(closed)
    }

    /// Joins and evicts the windows the watermark has closed. For an outer join the unmatched rows of
    /// the closed windows are null-padded here too: because a window's rows on both sides close in the
    /// same flush, the INNER join over the closed rows sees every potential match, so a closed row that
    /// does not appear in it never matched. Empty batch when nothing is emitted. Fallible because
    /// the join's working memory draws on the operator's budget.
    pub(crate) fn flush(&mut self, watermark: i64) -> Result<RecordBatch, DataFusionError> {
        self.current_watermark = self.current_watermark.max(watermark);
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return self.flush_store(watermark);
        }
        let left = Self::split_closed(
            &mut self.left_buffered,
            &self.left_schema,
            self.left_wend,
            watermark,
        );
        let right = Self::split_closed(
            &mut self.right_buffered,
            &self.right_schema,
            self.right_wend,
            watermark,
        );
        self.account()
            .expect("closing windows only shrinks the buffers");
        self.join_closed(left, right)
    }

    /// Joins the closed rows of both sides — the tail every flush shares, memory- or
    /// persistent. See {@link flush} for the outer-join null-padding rationale.
    fn join_closed(
        &mut self,
        left: Option<RecordBatch>,
        right: Option<RecordBatch>,
    ) -> Result<RecordBatch, DataFusionError> {
        // Join on the user keys plus the window bounds, so only rows of the same window match.
        let mut on: Vec<(usize, usize)> = self
            .left_keys
            .iter()
            .zip(&self.right_keys)
            .map(|(&l, &r)| (l, r))
            .collect();
        on.push((self.left_wstart, self.right_wstart));
        on.push((self.left_wend, self.right_wend));
        let filter = residual_filter(
            &self.left_data_schema,
            &self.right_data_schema,
            None,
            self.predicate.as_mut(),
        );

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
        let left_types: Vec<DataType> = self
            .left_data_schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let right_types: Vec<DataType> = self
            .right_data_schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
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
                let lrid = joined
                    .column(left_types.len())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("lrid");
                let rrid = joined
                    .column(total - 1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("rrid");
                for i in 0..joined.num_rows() {
                    matched_left.insert(lrid.value(i));
                    matched_right.insert(rrid.value(i));
                }
                let keep: Vec<usize> = (0..left_types.len())
                    .chain(left_types.len() + 1..total - 1)
                    .collect();
                let fields: Vec<Field> = keep
                    .iter()
                    .enumerate()
                    .map(|(j, &i)| {
                        Field::new(
                            format!("c{j}"),
                            joined.schema().field(i).data_type().clone(),
                            true,
                        )
                    })
                    .collect();
                let columns: Vec<ArrayRef> =
                    keep.iter().map(|&i| joined.column(i).clone()).collect();
                outputs.push(
                    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                        .expect("window pairs"),
                );
            }
        }
        if self.join_type.left_is_outer() {
            if let Some(left) = &left_closed {
                if let Some(pad) =
                    unmatched_null_pad(left, &matched_left, &left_types, &right_types, true)
                {
                    outputs.push(pad);
                }
            }
        }
        if self.join_type.right_is_outer() {
            if let Some(right) = &right_closed {
                if let Some(pad) =
                    unmatched_null_pad(right, &matched_right, &left_types, &right_types, false)
                {
                    outputs.push(pad);
                }
            }
        }
        Ok(match outputs.len() {
            0 => empty_batch(),
            1 => outputs.pop().expect("one output"),
            _ => {
                concat_batches(&outputs[0].schema(), outputs.iter()).expect("concat window outputs")
            }
        })
    }

    /// Serializes both buffers (`[u32 left_len][left ipc][right ipc]`) for a checkpoint.
    pub(crate) fn snapshot(&self) -> Vec<u8> {
        let serialize = |schema: &Option<SchemaRef>, buffered: &[RecordBatch]| match schema {
            Some(schema) if !buffered.is_empty() => write_ipc(
                &concat_batches(schema, buffered.iter()).expect("concat window-join buffer"),
            ),
            _ => Vec::new(),
        };
        let left = serialize(&self.left_schema, &self.left_buffered);
        let right = serialize(&self.right_schema, &self.right_buffered);
        Self::snapshot_parts(left, right)
    }

    fn snapshot_parts(left: Vec<u8>, right: Vec<u8>) -> Vec<u8> {
        let mut out = (left.len() as u32).to_le_bytes().to_vec();
        out.extend_from_slice(&left);
        out.extend_from_slice(&right);
        out
    }

    pub(crate) fn snapshot_partitions(
        &self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        self.raw_snapshot_partitions(max_parallelism, timestamp_precisions)
    }

    fn raw_snapshot_partitions(
        &self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        let snapshot = self.snapshot();
        let left_len =
            u32::from_le_bytes(snapshot[0..4].try_into().expect("snapshot len")) as usize;
        let left = Self::side_raw_partitions(
            &snapshot[4..4 + left_len],
            &self.left_keys,
            max_parallelism,
            timestamp_precisions,
        );
        let right = Self::side_raw_partitions(
            &snapshot[4 + left_len..],
            &self.right_keys,
            max_parallelism,
            timestamp_precisions,
        );
        let mut groups: Vec<i32> = left.keys().chain(right.keys()).copied().collect();
        groups.sort_unstable();
        groups.dedup();
        let mut snapshots = BTreeMap::new();
        for key_group in groups {
            snapshots.insert(
                key_group,
                Self::snapshot_parts(
                    left.get(&key_group)
                        .map(Self::merge_snapshot_batches)
                        .unwrap_or_default(),
                    right
                        .get(&key_group)
                        .map(Self::merge_snapshot_batches)
                        .unwrap_or_default(),
                ),
            );
        }
        snapshots
    }

    fn side_raw_partitions(
        bytes: &[u8],
        key_columns: &[usize],
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<RecordBatch>> {
        let mut partitions = BTreeMap::new();
        for batch in read_ipc_if_present(bytes) {
            let mut rows_by_group: BTreeMap<i32, Vec<u32>> = BTreeMap::new();
            for row in 0..batch.num_rows() {
                let key_group = flink_key_group(
                    binary_row_hash(&batch, key_columns, row, timestamp_precisions),
                    max_parallelism,
                ) as i32;
                rows_by_group.entry(key_group).or_default().push(row as u32);
            }
            for (key_group, rows) in rows_by_group {
                let indices = UInt32Array::from(rows);
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| {
                        take(column, &indices, None).expect("partition window-join snapshot")
                    })
                    .collect();
                partitions.entry(key_group).or_insert_with(Vec::new).push(
                    RecordBatch::try_new(batch.schema(), columns)
                        .expect("partitioned window-join snapshot"),
                );
            }
        }
        partitions
    }

    fn merge_snapshot_batches(batches: &Vec<RecordBatch>) -> Vec<u8> {
        write_ipc(
            &concat_batches(&batches[0].schema(), batches.iter())
                .expect("merge window-join raw partitions"),
        )
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
        if bytes.is_empty() {
            return joiner;
        }
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore_partitions(
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
        snapshots: &[Vec<u8>],
    ) -> Self {
        let mut left_batches = Vec::new();
        let mut right_batches = Vec::new();
        for bytes in snapshots {
            if bytes.len() < 4 {
                continue;
            }
            let left_len =
                u32::from_le_bytes(bytes[0..4].try_into().expect("snapshot len")) as usize;
            assert!(
                4 + left_len <= bytes.len(),
                "truncated window-join raw key-group snapshot"
            );
            left_batches.extend(read_ipc_if_present(&bytes[4..4 + left_len]));
            right_batches.extend(read_ipc_if_present(&bytes[4 + left_len..]));
        }
        let left = (!left_batches.is_empty())
            .then(|| Self::merge_snapshot_batches(&left_batches))
            .unwrap_or_default();
        let right = (!right_batches.is_empty())
            .then(|| Self::merge_snapshot_batches(&right_batches))
            .unwrap_or_default();
        WindowJoiner::restore(
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
            &Self::snapshot_parts(left, right),
        )
    }
}

state_bytes_getter!(
    Java_tech_streamfusion_Native_windowJoinerStateBytes,
    WindowJoiner
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_windowJoinerLateDrops<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    left: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let joiner = unsafe { &*(handle as *const WindowJoiner) };
        (if left != 0 {
            joiner.left_late_drops
        } else {
            joiner.right_late_drops
        }) as jlong
    })
}

/// Creates an event-time INNER window joiner and returns an opaque handle. The key/window column
/// indices locate the equi-join key and the `window_start`/`window_end` columns within each side's
/// input batch. The JVM owns the handle across calls and must release it with the matching close.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createWindowJoiner<'local>(
    env: JNIEnv<'local>,
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
    crate::bridge::jni_guard(env, move |mut env| {
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
    })
}

/// Buffers a left batch (no output); its rows are joined later when the watermark closes their window.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushLeftWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
        // The pushed batch is retained in the buffer (not dropped), so no JVM release upcall runs
        // between a failed account and the throw (see updateTumblingAggregator).
        let result = joiner.push_left(import_record_batch(in_array_address, in_schema_address));
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Buffers a right batch (no output).
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRightWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
        // The pushed batch is retained in the buffer (not dropped), so no JVM release upcall runs
        // between a failed account and the throw (see updateTumblingAggregator).
        let result = joiner.push_right(import_record_batch(in_array_address, in_schema_address));
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Exports the INNER matches of every window the watermark has closed (then evicts those windows).
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
        match joiner.flush(watermark_millis) {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Releases the window joiner and its native state.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<WindowJoiner>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotWindowJoinerPartitions<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &*(handle as *const WindowJoiner) };
        let precisions = read_i32_array(&env, &timestamp_precisions);
        keyed_state_partition_array(
            &mut env,
            joiner.snapshot_partitions(max_parallelism as usize, &precisions),
            "window-join",
        )
    })
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreWindowJoinerPartitions<'local>(
    env: JNIEnv<'local>,
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
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
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
        let count = env
            .get_array_length(&snapshots)
            .expect("read window-join raw partition count");
        let mut restored = Vec::with_capacity(count as usize);
        for index in 0..count {
            let bytes = JByteArray::from(
                env.get_object_array_element(&snapshots, index)
                    .expect("read window-join raw partition"),
            );
            restored.push(
                env.convert_byte_array(&bytes)
                    .expect("read window-join raw partition bytes"),
            );
        }
        let joiner = WindowJoiner::restore_partitions(
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
            &restored,
        )
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, joiner)
    })
}
