use crate::*;

/// Event-time INNER interval join, Flink's
/// `a JOIN b ON a.k = b.k AND a.rt BETWEEN b.rt + lower AND b.rt + upper`.
///
/// Buffers both inputs as batches. When a batch arrives on one side it is joined against the other
/// side's buffered rows — an INNER hash join on the equi-keys with the interval as a residual filter
/// (`lower <= left.rt - right.rt <= upper`) — so each matched pair is emitted exactly once, when the
/// second of its two rows arrives. This is the insert-then-join-the-other-side structure of Arroyo's
/// `JoinWithExpiration`, which likewise runs a DataFusion join over the batches it has buffered. A
/// row is evicted once the watermark passes the point beyond which no future row of the other side
/// could match it. Output columns are the left input columns followed by the right input columns.
pub(crate) struct IntervalJoiner {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    left_time: usize,
    right_time: usize,
    lower: i64,
    upper: i64,
    predicate: Option<JoinPredicate>,
    join_type: JoinKind,
    // Eager data schemas (no `$rowid$`), seeded at construction so an outer join can type the
    // null-padding for a side before that side's first batch arrives.
    left_data_schema: SchemaRef,
    right_data_schema: SchemaRef,
    // Buffered rows: data-only for an INNER join, data + a trailing `__rowid__` for an outer join (so
    // a matched buffered row can be identified to set its match flag, and an unmatched one null-padded
    // at eviction).
    left_buffered: Vec<RecordBatch>,
    right_buffered: Vec<RecordBatch>,
    // Outer only: the row-ids that have matched at least once, and the per-side id counters.
    left_matched: HashSet<i64>,
    right_matched: HashSet<i64>,
    left_next_id: i64,
    right_next_id: i64,
    memory: OperatorMemory,
}

/// Estimated footprint of one matched-row-id set entry (an i64 plus the hash-set slot).
pub(crate) const MATCHED_ID_BYTES: usize = 48;

impl IntervalJoiner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_time: usize,
        right_time: usize,
        lower: i64,
        upper: i64,
        predicate: Option<JoinPredicate>,
        join_type: JoinKind,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
    ) -> Self {
        IntervalJoiner {
            left_keys,
            right_keys,
            left_time,
            right_time,
            lower,
            upper,
            predicate,
            join_type,
            left_data_schema,
            right_data_schema,
            left_buffered: Vec::new(),
            right_buffered: Vec::new(),
            left_matched: HashSet::default(),
            right_matched: HashSet::default(),
            left_next_id: 0,
            right_next_id: 0,
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds the buffered rows (plus the outer-join match flags) by the operator's managed-memory
    /// budget (negative = unaccounted), accounting any restored state immediately.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        if budget_bytes >= 0 {
            self.memory.attach("interval-join", budget_bytes, 0)?;
            self.account()?;
        }
        Ok(self)
    }

    /// Re-accounts after a state change: the buffered batches are recounted per batch (far cheaper
    /// than the per-row concat/join work of the same call), the match-flag sets by `len`.
    fn account(&mut self) -> Result<(), DataFusionError> {
        if self.memory.tracking() {
            self.memory.set(
                buffered_batches_bytes(&self.left_buffered)
                    + buffered_batches_bytes(&self.right_buffered)
                    + (self.left_matched.len() + self.right_matched.len()) * MATCHED_ID_BYTES,
            );
            self.memory.account()?;
        }
        Ok(())
    }

    fn key_pairs(&self) -> Vec<(usize, usize)> {
        self.left_keys.iter().zip(&self.right_keys).map(|(&l, &r)| (l, r)).collect()
    }

    /// The schema of one side's buffered batches: data, plus a trailing `__rowid__` for an outer join.
    fn buf_schema(&self, is_left: bool) -> SchemaRef {
        let data = if is_left { &self.left_data_schema } else { &self.right_data_schema };
        if self.join_type == JoinKind::Inner {
            data.clone()
        } else {
            with_rowid_schema(data)
        }
    }

    /// Joins an incoming left batch against the buffered right rows (equi-key + interval bounds and
    /// the residual non-equi predicate), then buffers it. Empty until the right side has rows. A
    /// proctime join stamps every row's time with the operator's clock (passed in) before joining, so
    /// the interval is measured in processing time rather than read from a rowtime column.
    pub(crate) fn push_left(&mut self, batch: RecordBatch, proctime_now: Option<i64>) -> Result<RecordBatch, DataFusionError> {
        let batch = match proctime_now {
            Some(now) => {
                let target = self.left_data_schema.field(self.left_time).data_type().clone();
                stamp_time_column(&batch, self.left_time, now, &target)
            }
            None => batch,
        };
        let interval = Some((self.left_time, self.right_time, self.lower, self.upper));
        let filter = residual_filter(
            &self.left_data_schema,
            &self.right_data_schema,
            interval,
            self.predicate.as_mut(),
        );
        if self.join_type == JoinKind::Inner {
            let result = if self.right_buffered.is_empty() {
                empty_batch()
            } else {
                let right = concat_batches(&self.buf_schema(false), self.right_buffered.iter())
                    .expect("concat right interval buffer");
                hash_join_inner(batch.clone(), right, &self.key_pairs(), filter, self.memory.task_ctx())?
            };
            self.left_buffered.push(batch);
            self.account()?;
            return Ok(result);
        }
        // Outer: tag with row-ids, join, record matches on both sides, then buffer the tagged batch.
        let tagged = append_rowids(&batch, &mut self.left_next_id);
        let result = if self.right_buffered.is_empty() {
            empty_batch()
        } else {
            let right = concat_batches(&self.buf_schema(false), self.right_buffered.iter())
                .expect("concat right interval buffer");
            self.join_tagged(tagged.clone(), right, filter)?
        };
        self.left_buffered.push(tagged);
        self.account()?;
        Ok(result)
    }

    /// Joins an incoming right batch against the buffered left rows, then buffers it. As with {@link
    /// push_left}, a proctime join stamps the row time with the clock before joining.
    pub(crate) fn push_right(&mut self, batch: RecordBatch, proctime_now: Option<i64>) -> Result<RecordBatch, DataFusionError> {
        let batch = match proctime_now {
            Some(now) => {
                let target = self.right_data_schema.field(self.right_time).data_type().clone();
                stamp_time_column(&batch, self.right_time, now, &target)
            }
            None => batch,
        };
        let interval = Some((self.left_time, self.right_time, self.lower, self.upper));
        let filter = residual_filter(
            &self.left_data_schema,
            &self.right_data_schema,
            interval,
            self.predicate.as_mut(),
        );
        if self.join_type == JoinKind::Inner {
            let result = if self.left_buffered.is_empty() {
                empty_batch()
            } else {
                let left = concat_batches(&self.buf_schema(true), self.left_buffered.iter())
                    .expect("concat left interval buffer");
                hash_join_inner(left, batch.clone(), &self.key_pairs(), filter, self.memory.task_ctx())?
            };
            self.right_buffered.push(batch);
            self.account()?;
            return Ok(result);
        }
        let tagged = append_rowids(&batch, &mut self.right_next_id);
        let result = if self.left_buffered.is_empty() {
            empty_batch()
        } else {
            let left = concat_batches(&self.buf_schema(true), self.left_buffered.iter())
                .expect("concat left interval buffer");
            self.join_tagged(left, tagged.clone(), filter)?
        };
        self.right_buffered.push(tagged);
        self.account()?;
        Ok(result)
    }

    /// Runs the (always INNER) hash join of two row-id-tagged operands `[left data.., left __rowid__]`
    /// and `[right data.., right __rowid__]`, records the matched row-ids on both sides, and returns
    /// the matched pairs projected back to `[left data.., right data..]` (the row-ids dropped).
    fn join_tagged(
        &mut self,
        left_tagged: RecordBatch,
        right_tagged: RecordBatch,
        filter: Option<JoinFilter>,
    ) -> Result<RecordBatch, DataFusionError> {
        let left_arity = self.left_data_schema.fields().len();
        let joined =
            hash_join_inner(left_tagged, right_tagged, &self.key_pairs(), filter, self.memory.task_ctx())?;
        if joined.num_rows() == 0 {
            return Ok(empty_batch());
        }
        // Layout after the join (renamed c0..): [left data.., left rid, right data.., right rid].
        let total = joined.num_columns();
        let left_rid = joined.column(left_arity).as_any().downcast_ref::<Int64Array>().expect("left rid");
        let right_rid = joined.column(total - 1).as_any().downcast_ref::<Int64Array>().expect("right rid");
        for i in 0..joined.num_rows() {
            self.left_matched.insert(left_rid.value(i));
            self.right_matched.insert(right_rid.value(i));
        }
        // Project out the two row-id columns, leaving [left data.., right data..].
        let keep: Vec<usize> =
            (0..left_arity).chain(left_arity + 1..total - 1).collect();
        let fields: Vec<Field> = keep
            .iter()
            .enumerate()
            .map(|(j, &i)| Field::new(format!("c{j}"), joined.schema().field(i).data_type().clone(), true))
            .collect();
        let columns: Vec<ArrayRef> = keep.iter().map(|&i| joined.column(i).clone()).collect();
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("project interval pairs"))
    }

    /// Drops the rows the watermark has made dead and, for an outer join, returns the null-padded
    /// rows for the evicted outer-side rows that never matched (append-only — emitted once). A left
    /// row can no longer match once `left.rt - lower <= watermark` (a future right row has rt >
    /// watermark, but matching it needs `right.rt <= left.rt - lower`); a right row once
    /// `right.rt + upper <= watermark`. Because an outer row is evicted only once no future other-side
    /// row could match it, all its potential matches have been seen, so its match flag is final.
    pub(crate) fn advance(&mut self, watermark: i64) -> RecordBatch {
        let (lower, upper) = (self.lower, self.upper);
        if self.join_type == JoinKind::Inner {
            Self::evict_inner(&mut self.left_buffered, &self.left_data_schema, self.left_time, |rt| {
                rt - lower > watermark
            });
            Self::evict_inner(&mut self.right_buffered, &self.right_data_schema, self.right_time, |rt| {
                rt + upper > watermark
            });
            self.account().expect("eviction only shrinks state");
            return empty_batch();
        }
        let left_pads = self.evict_outer(true, |rt| rt - lower > watermark);
        let right_pads = self.evict_outer(false, |rt| rt + upper > watermark);
        self.account().expect("eviction only shrinks state");
        match (left_pads, right_pads) {
            (None, None) => empty_batch(),
            (Some(b), None) | (None, Some(b)) => b,
            (Some(l), Some(r)) => {
                concat_batches(&l.schema(), [l, r].iter()).expect("concat interval null-pads")
            }
        }
    }

    /// INNER eviction: keeps only the buffered rows whose rowtime (column `time`) satisfies `keep`.
    fn evict_inner(
        buffered: &mut Vec<RecordBatch>,
        schema: &SchemaRef,
        time: usize,
        keep: impl Fn(i64) -> bool,
    ) {
        if buffered.is_empty() {
            return;
        }
        let all = concat_batches(schema, buffered.iter()).expect("concat interval buffer");
        let rt = rt_to_millis(all.column(time));
        let mask: BooleanArray = rt.iter().map(|v| Some(keep(v.unwrap()))).collect();
        let kept = filter_record_batch(&all, &mask).expect("filter interval buffer");
        *buffered = if kept.num_rows() > 0 { vec![kept] } else { Vec::new() };
    }

    /// Outer eviction for one side: keeps the live rows, drops the dead ones' match flags, and returns
    /// the null-padded rows for evicted rows that never matched (only when this side is outer).
    fn evict_outer(&mut self, is_left: bool, keep: impl Fn(i64) -> bool) -> Option<RecordBatch> {
        let buffered = std::mem::take(if is_left { &mut self.left_buffered } else { &mut self.right_buffered });
        if buffered.is_empty() {
            return None;
        }
        let time = if is_left { self.left_time } else { self.right_time };
        let buf_schema = self.buf_schema(is_left);
        let all = concat_batches(&buf_schema, buffered.iter()).expect("concat interval buffer");
        let rt = rt_to_millis(all.column(time));
        let keep_mask: BooleanArray = rt.iter().map(|v| Some(keep(v.unwrap()))).collect();
        let kept = filter_record_batch(&all, &keep_mask).expect("filter interval kept");
        let evicted =
            filter_record_batch(&all, &arrow::compute::not(&keep_mask).expect("negate keep mask"))
                .expect("filter interval evicted");
        let target = if is_left { &mut self.left_buffered } else { &mut self.right_buffered };
        *target = if kept.num_rows() > 0 { vec![kept] } else { Vec::new() };

        let rid_index = all.num_columns() - 1;
        let evicted_rids = evicted.column(rid_index).as_any().downcast_ref::<Int64Array>().expect("rid");
        let this_outer =
            if is_left { self.join_type.left_is_outer() } else { self.join_type.right_is_outer() };
        let matched = if is_left { &mut self.left_matched } else { &mut self.right_matched };
        // Read match flags before dropping them.
        let unmatched_mask: BooleanArray = (0..evicted.num_rows())
            .map(|i| Some(this_outer && !matched.contains(&evicted_rids.value(i))))
            .collect();
        for i in 0..evicted.num_rows() {
            matched.remove(&evicted_rids.value(i));
        }
        if !this_outer {
            return None;
        }
        let unmatched = filter_record_batch(&evicted, &unmatched_mask).expect("filter unmatched evicted");
        if unmatched.num_rows() == 0 {
            return None;
        }
        Some(self.null_pad(&unmatched, is_left))
    }

    /// Builds the null-padded output `[left data.., right data..]` (columns `c0..`) for unmatched rows
    /// of one side: the side's data columns (the trailing `__rowid__` dropped) beside all-null columns
    /// for the other side.
    fn null_pad(&self, rows: &RecordBatch, is_left: bool) -> RecordBatch {
        let left_types: Vec<DataType> =
            self.left_data_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let right_types: Vec<DataType> =
            self.right_data_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        build_null_pad(rows, &left_types, &right_types, is_left)
    }

    /// Serializes both buffers and (for an outer join) the per-side matched row-id sets, length-framed.
    pub(crate) fn snapshot(&self) -> Vec<u8> {
        let buf = |is_left: bool, buffered: &[RecordBatch]| -> Vec<u8> {
            if buffered.is_empty() {
                Vec::new()
            } else {
                write_ipc(&concat_batches(&self.buf_schema(is_left), buffered.iter()).expect("concat buf"))
            }
        };
        Self::snapshot_parts([
            buf(true, &self.left_buffered),
            buf(false, &self.right_buffered),
            serialize_id_set(&self.left_matched),
            serialize_id_set(&self.right_matched),
        ])
    }

    fn snapshot_parts(sections: [Vec<u8>; 4]) -> Vec<u8> {
        let mut out = Vec::new();
        for section in sections {
            out.extend_from_slice(&(section.len() as u32).to_le_bytes());
            out.extend_from_slice(&section);
        }
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
        let sections = read_framed_sections(&self.snapshot());
        let left = Self::side_raw_partitions(
            &sections[0],
            &self.left_keys,
            max_parallelism,
            timestamp_precisions,
        );
        let right = Self::side_raw_partitions(
            &sections[1],
            &self.right_keys,
            max_parallelism,
            timestamp_precisions,
        );
        let left_matched = Self::matched_raw_partitions(&left, &self.left_matched);
        let right_matched = Self::matched_raw_partitions(&right, &self.right_matched);
        let mut groups: Vec<i32> = left
            .keys()
            .chain(right.keys())
            .chain(left_matched.keys())
            .chain(right_matched.keys())
            .copied()
            .collect();
        groups.sort_unstable();
        groups.dedup();
        let mut snapshots = BTreeMap::new();
        for key_group in groups {
            snapshots.insert(
                key_group,
                Self::snapshot_parts([
                    left.get(&key_group).map(Self::merge_snapshot_batches).unwrap_or_default(),
                    right.get(&key_group).map(Self::merge_snapshot_batches).unwrap_or_default(),
                    left_matched.get(&key_group).cloned().unwrap_or_default(),
                    right_matched.get(&key_group).cloned().unwrap_or_default(),
                ]),
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
                    .map(|column| take(column, &indices, None).expect("partition interval snapshot"))
                    .collect();
                partitions
                    .entry(key_group)
                    .or_insert_with(Vec::new)
                    .push(
                        RecordBatch::try_new(batch.schema(), columns)
                            .expect("partitioned interval snapshot"),
                    );
            }
        }
        partitions
    }

    fn matched_raw_partitions(
        partitions: &BTreeMap<i32, Vec<RecordBatch>>,
        matched: &HashSet<i64>,
    ) -> BTreeMap<i32, Vec<u8>> {
        let mut by_group = BTreeMap::new();
        if matched.is_empty() {
            return by_group;
        }
        for (key_group, batches) in partitions {
            let mut ids = HashSet::default();
            for batch in batches {
                let rowids = batch
                    .column(batch.num_columns() - 1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("interval row id");
                for row in 0..rowids.len() {
                    let id = rowids.value(row);
                    if matched.contains(&id) {
                        ids.insert(id);
                    }
                }
            }
            let bytes = serialize_id_set(&ids);
            if !bytes.is_empty() {
                by_group.insert(*key_group, bytes);
            }
        }
        by_group
    }

    fn merge_snapshot_batches(batches: &Vec<RecordBatch>) -> Vec<u8> {
        write_ipc(
            &concat_batches(&batches[0].schema(), batches.iter())
                .expect("merge interval raw partitions"),
        )
    }

    /// Gives each outer-join row a handle-local id again while combining raw key groups.
    ///
    /// Row ids identify whether a buffered outer row has already produced a match. They are
    /// allocated independently by every subtask, so two raw key groups from different subtasks
    /// may both contain (for example) row id zero after a scale-down. Remap one raw partition at a
    /// time before combining it with the others, keeping its matched-id set aligned with its rows.
    fn remap_outer_rowids(
        batches: Vec<RecordBatch>,
        matched: HashSet<i64>,
        next_id: &mut i64,
    ) -> (Vec<RecordBatch>, HashSet<i64>) {
        let mut remapped_ids = HashMap::default();
        let mut remapped_batches = Vec::with_capacity(batches.len());
        for batch in batches {
            let rowid_column = batch.num_columns() - 1;
            let rowids = batch
                .column(rowid_column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("interval row id");
            let mut new_ids = Vec::with_capacity(rowids.len());
            for row in 0..rowids.len() {
                let new_id = *next_id;
                *next_id = next_id.checked_add(1).expect("interval row id overflow");
                remapped_ids.insert(rowids.value(row), new_id);
                new_ids.push(new_id);
            }
            let mut columns = batch.columns().to_vec();
            columns[rowid_column] = Arc::new(Int64Array::from(new_ids));
            remapped_batches.push(
                RecordBatch::try_new(batch.schema(), columns).expect("remap interval row ids"),
            );
        }
        let remapped_matched = matched
            .into_iter()
            .map(|id| {
                *remapped_ids
                    .get(&id)
                    .expect("matched interval row missing from its buffer")
            })
            .collect();
        (remapped_batches, remapped_matched)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_time: usize,
        right_time: usize,
        lower: i64,
        upper: i64,
        predicate: Option<JoinPredicate>,
        join_type: JoinKind,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
        bytes: &[u8],
    ) -> Self {
        let mut joiner = IntervalJoiner::new(
            left_keys,
            right_keys,
            left_time,
            right_time,
            lower,
            upper,
            predicate,
            join_type,
            left_data_schema,
            right_data_schema,
        );
        if bytes.is_empty() {
            return joiner;
        }
        let sections = read_framed_sections(bytes);
        joiner.left_buffered = read_ipc_if_present(&sections[0]);
        joiner.right_buffered = read_ipc_if_present(&sections[1]);
        joiner.left_matched = deserialize_id_set(&sections[2]);
        joiner.right_matched = deserialize_id_set(&sections[3]);
        // Resume the id counters past any live buffered row (evicted ids are gone, so reuse is safe).
        joiner.left_next_id = max_rowid(&joiner.left_buffered) + 1;
        joiner.right_next_id = max_rowid(&joiner.right_buffered) + 1;
        joiner
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore_partitions(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_time: usize,
        right_time: usize,
        lower: i64,
        upper: i64,
        predicate: Option<JoinPredicate>,
        join_type: JoinKind,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
        snapshots: &[Vec<u8>],
    ) -> Self {
        let mut left_batches = Vec::new();
        let mut right_batches = Vec::new();
        let mut left_matched = HashSet::default();
        let mut right_matched = HashSet::default();
        let mut left_next_id = 0;
        let mut right_next_id = 0;
        for bytes in snapshots {
            let sections = read_framed_sections(bytes);
            if sections.len() == 4 {
                let left = read_ipc_if_present(&sections[0]);
                let right = read_ipc_if_present(&sections[1]);
                if join_type == JoinKind::Inner {
                    left_batches.extend(left);
                    right_batches.extend(right);
                } else {
                    let (left, matched) = Self::remap_outer_rowids(
                        left,
                        deserialize_id_set(&sections[2]),
                        &mut left_next_id,
                    );
                    let (right, matched_right) = Self::remap_outer_rowids(
                        right,
                        deserialize_id_set(&sections[3]),
                        &mut right_next_id,
                    );
                    left_batches.extend(left);
                    right_batches.extend(right);
                    left_matched.extend(matched);
                    right_matched.extend(matched_right);
                }
            }
        }
        let left = (!left_batches.is_empty())
            .then(|| Self::merge_snapshot_batches(&left_batches))
            .unwrap_or_default();
        let right = (!right_batches.is_empty())
            .then(|| Self::merge_snapshot_batches(&right_batches))
            .unwrap_or_default();
        IntervalJoiner::restore(
            left_keys,
            right_keys,
            left_time,
            right_time,
            lower,
            upper,
            predicate,
            join_type,
            left_data_schema,
            right_data_schema,
            &Self::snapshot_parts([
                left,
                right,
                serialize_id_set(&left_matched),
                serialize_id_set(&right_matched),
            ]),
        )
    }
}

/// The interval-bounds conjunct `joined[left_rt] BETWEEN joined[right_rt] + lower AND + upper`, built
/// against the joined intermediate schema. `right_type` is the right rowtime's type, which the bound
/// offset must match (arrow rejects a timestamp plus a duration of a different unit).
pub(crate) fn interval_bounds_expr(
    intermediate: &SchemaRef,
    left_rt: usize,
    right_rt: usize,
    right_type: &DataType,
    lower: i64,
    upper: i64,
) -> Arc<dyn PhysicalExpr> {
    use arrow::datatypes::TimeUnit;
    let offset = |millis: i64| -> ScalarValue {
        match right_type {
            DataType::Int64 => ScalarValue::Int64(Some(millis)),
            DataType::Timestamp(TimeUnit::Second, _) => ScalarValue::DurationSecond(Some(millis / 1_000)),
            DataType::Timestamp(TimeUnit::Millisecond, _) => ScalarValue::DurationMillisecond(Some(millis)),
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                ScalarValue::DurationMicrosecond(Some(millis * 1_000))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                ScalarValue::DurationNanosecond(Some(millis * 1_000_000))
            }
            other => panic!("unsupported interval-join rowtime type: {other:?}"),
        }
    };
    let left_col: Arc<dyn PhysicalExpr> =
        Arc::new(Column::new(intermediate.field(left_rt).name(), left_rt));
    let right_col: Arc<dyn PhysicalExpr> =
        Arc::new(Column::new(intermediate.field(right_rt).name(), right_rt));
    let bound = |millis: i64| -> Arc<dyn PhysicalExpr> {
        binary(right_col.clone(), Operator::Plus, lit(offset(millis)), intermediate)
            .expect("failed to build interval bound")
    };
    let ge = binary(left_col.clone(), Operator::GtEq, bound(lower), intermediate)
        .expect("failed to build lower bound");
    let le = binary(left_col.clone(), Operator::LtEq, bound(upper), intermediate)
        .expect("failed to build upper bound");
    binary(ge, Operator::And, le, intermediate).expect("failed to build interval and")
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_intervalJoinerStateBytes, IntervalJoiner);

/// Creates an event-time INNER interval joiner and returns an opaque handle. The key/time column
/// indices locate the equi-join key and rowtime within each side's input batch; `lower`/`upper` are
/// the inclusive bounds (millis) on `left.rt - right.rt`. The JVM owns the handle across calls.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createIntervalJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_time: jint,
    right_time: jint,
    lower: jlong,
    upper: jlong,
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
    let joiner = IntervalJoiner::new(
        left,
        right,
        left_time as usize,
        right_time as usize,
        lower,
        upper,
        predicate,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, joiner)
}

/// Pushes a left batch, probing the buffered right rows and exporting the matched pairs.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushLeftIntervalJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    proctime: jboolean,
    proctime_now_millis: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
    // The pushed batch is retained in the buffer (not dropped), so no JVM release upcall runs
    // between a failed account and the throw (see updateTumblingAggregator).
    let batch = import_record_batch(in_array_address, in_schema_address);
    let result = joiner.push_left(batch, (proctime != 0).then_some(proctime_now_millis));
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Pushes a right batch, probing the buffered left rows and exporting the matched pairs.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushRightIntervalJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    proctime: jboolean,
    proctime_now_millis: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
    // The pushed batch is retained in the buffer (not dropped), so no JVM release upcall runs
    // between a failed account and the throw (see updateTumblingAggregator).
    let batch = import_record_batch(in_array_address, in_schema_address);
    let result = joiner.push_right(batch, (proctime != 0).then_some(proctime_now_millis));
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Advances the combined watermark, evicting rows no future arrival can match, and exporting the
/// null-padded rows for evicted outer rows that never matched (empty for an INNER join).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_advanceIntervalJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
    let result = joiner.advance(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases the interval joiner and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeIntervalJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<IntervalJoiner>(handle));
    }
}

/// Serializes the joiner's buffered rows for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let joiner = unsafe { &*(handle as *mut IntervalJoiner) };
    env.byte_array_from_slice(&joiner.snapshot())
        .expect("failed to allocate join snapshot array")
        .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotIntervalJoinerPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    let joiner = unsafe { &*(handle as *const IntervalJoiner) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    keyed_state_partition_array(
        &mut env,
        joiner.snapshot_partitions(max_parallelism as usize, &precisions),
        "interval-join",
    )
}

/// Rebuilds an interval joiner from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreIntervalJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_time: jint,
    right_time: jint,
    lower: jlong,
    upper: jlong,
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
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read join snapshot");
    let joiner = IntervalJoiner::restore(
        left,
        right,
        left_time as usize,
        right_time as usize,
        lower,
        upper,
        predicate,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
        &bytes,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, joiner)
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreIntervalJoinerPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_time: jint,
    right_time: jint,
    lower_bound: jlong,
    upper_bound: jlong,
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
        .expect("read interval raw partition count");
    let mut restored = Vec::with_capacity(count as usize);
    for index in 0..count {
        let bytes = JByteArray::from(
            env.get_object_array_element(&snapshots, index)
                .expect("read interval raw partition"),
        );
        restored.push(
            env.convert_byte_array(&bytes)
                .expect("read interval raw partition bytes"),
        );
    }
    let joiner = IntervalJoiner::restore_partitions(
        left,
        right,
        left_time as usize,
        right_time as usize,
        lower_bound,
        upper_bound,
        predicate,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
        &restored,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, joiner)
}
