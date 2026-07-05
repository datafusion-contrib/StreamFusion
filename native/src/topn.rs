use crate::*;

/// One ORDER BY column for the Top-N comparator: which column, ascending vs descending, and whether
/// nulls sort first (independent of direction, as in SQL `NULLS FIRST`/`LAST`).
pub(crate) struct SortColumn {
    pub(crate) index: usize,
    pub(crate) ascending: bool,
    pub(crate) nulls_first: bool,
}

/// Orders two rows by the sort columns, returning the first column's decision. Null placement
/// follows `nulls_first` and is not flipped by `ascending`; the value comparison is.
pub(crate) fn compare_rows(a: &[ScalarValue], b: &[ScalarValue], sort: &[SortColumn]) -> std::cmp::Ordering {
    use std::cmp::Ordering::Equal;
    for s in sort {
        let (x, y) = (&a[s.index], &b[s.index]);
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Equal,
            (true, false) => {
                if s.nulls_first {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (false, true) => {
                if s.nulls_first {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            (false, false) => {
                let c = x.partial_cmp(y).unwrap_or(Equal);
                if s.ascending {
                    c
                } else {
                    c.reverse()
                }
            }
        };
        if ord != Equal {
            return ord;
        }
    }
    Equal
}

/// Append-only streaming Top-N (`ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) <= N`). Per partition
/// it keeps the top `limit` rows sorted by the order keys (ties in arrival order), exactly the host's
/// append-only bounded buffer.
///
/// With the rank number **not** projected (`output_rank_number = false`): on each input row it
/// inserts into the buffer; if that overflows the limit it drops the last (rank N+1) — emitting
/// nothing if the new row is the one dropped, else a DELETE of the displaced row — and otherwise
/// emits the new row as an INSERT. Output is the input columns plus the `$row_kind$` byte.
///
/// With the rank number projected (`output_rank_number = true`): a row entering at rank `r` shifts
/// everyone below it down by one, so the operator emits the cascade Flink's `AppendOnlyTopNFunction`
/// does — for each rank from `r` to the buffer end, UPDATE_BEFORE(old occupant)/UPDATE_AFTER(new
/// occupant), and an INSERT for the row taking a brand-new rank; a row pushed past `limit` is
/// retracted by the UPDATE_BEFORE at the last rank (no separate delete). Output appends the rank
/// (a bigint) before the `$row_kind$` byte.
/// arrow-row encoders for a Top-N, built once from the first batch's column types and reused: the
/// partition key, the memcomparable sort key (per-column ASC/DESC + null placement), and the
/// value-encoded full row.
pub(crate) struct TopNConverters {
    partition: RowConverter,
    sort: RowConverter,
    payload: RowConverter,
}

/// A buffered Top-N row as compact arrow-row bytes: its memcomparable sort key and the value-encoded
/// full row. No per-cell `ScalarValue`, so a buffer insert and the rank cascade move/clone a single
/// byte buffer rather than deep-cloning every column (notably the heap strings that dominated the
/// `ScalarValue` path's malloc/clone churn). `OwnedRow` is `Ord`/`Eq` by those bytes, so ordering and
/// the full-row equality the eviction needs are byte compares.
/// A buffered Top-N row: the memcomparable sort key and the value-encoded full row. The payload is an
/// `Arc` because the with-rank cascade emits the same buffered row multiple times (as a `-U` at one
/// rank and a `+U` at the next); sharing it makes those emits refcount bumps rather than a byte-buffer
/// clone each — the allocator churn a differential profile pinned as the Top-N's cost over Flink (which
/// reuses `BinaryRowData`). The decode back to Arrow still happens once per emitted row, on flush.
pub(crate) type TopNRow = (OwnedRow, Arc<OwnedRow>);

pub(crate) struct TopNRanker {
    partition_columns: Vec<usize>,
    sort_columns: Vec<SortColumn>,
    limit: i64,
    output_rank_number: bool,
    schema: Option<SchemaRef>,
    converters: Option<TopNConverters>,
    // Keyed by the partition key's encoded bytes (`ByteKey`): the per-row probe hashes the BORROWED
    // bytes, so a row for an existing partition — or one dropped at rank > N — allocates nothing.
    groups: HashMap<ByteKey, Vec<TopNRow>>,
    pub(crate) memory: OperatorMemory,
}

/// Estimated footprint of one buffered Top-N entry (sort key + payload row + container overhead).
pub(crate) fn topn_entry_bytes(entry: &TopNRow) -> usize {
    entry.0.row().as_ref().len() + entry.1.row().as_ref().len() + GROUP_ENTRY_OVERHEAD
}

impl TopNRanker {
    pub(crate) fn new(
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        output_rank_number: bool,
    ) -> Self {
        TopNRanker {
            partition_columns,
            sort_columns,
            limit,
            output_rank_number,
            schema: None,
            converters: None,
            groups: HashMap::default(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds the per-partition buffers by the operator's managed-memory budget (negative =
    /// unaccounted), accounting any restored buffers immediately.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .groups
            .iter()
            .map(|(key, buffer)| {
                byte_key_bytes(&key.0) + buffer.iter().map(topn_entry_bytes).sum::<usize>()
            })
            .sum();
        self.memory.attach("top-n", budget_bytes, state)?;
        Ok(self)
    }

    /// Builds the three arrow-row converters from a batch's column types, once.
    fn ensure_converters(&mut self, batch: &RecordBatch, arity: usize) {
        if self.converters.is_some() {
            return;
        }
        let payload = RowConverter::new(
            (0..arity).map(|i| SortField::new(batch.column(i).data_type().clone())).collect(),
        )
        .expect("top-n payload converter");
        // A plain LIMIT (no ORDER BY) has zero sort columns; like the empty partition key, encode a
        // constant dummy so all rows compare equal and the buffer preserves arrival order (Flink's
        // first-n by arrival). With sort columns present, encode them memcomparable with their options.
        let sort = if self.sort_columns.is_empty() {
            RowConverter::new(vec![SortField::new(DataType::Boolean)]).expect("top-n empty sort converter")
        } else {
            RowConverter::new(
                self.sort_columns
                    .iter()
                    .map(|s| {
                        SortField::new_with_options(
                            batch.column(s.index).data_type().clone(),
                            SortOptions { descending: !s.ascending, nulls_first: s.nulls_first },
                        )
                    })
                    .collect(),
            )
            .expect("top-n sort converter")
        };
        // A global Top-N (LIMIT / SortLimit with no PARTITION BY) has zero partition columns; arrow-row
        // can't encode N rows of no columns, so key on a constant dummy column (all rows → one group),
        // exactly as the group-aggregate keying does.
        let partition_refs: Vec<&ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i)).collect();
        let partition = key_row_converter(&partition_refs);
        self.converters = Some(TopNConverters { partition, sort, payload });
    }

    pub(crate) fn push(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        self.ensure_converters(batch, arity);
        let conv = self.converters.as_ref().expect("converters set");
        // Encode the whole batch columnar->row in three vectorized passes (partition key, sort key,
        // full-row payload), instead of materializing a `ScalarValue` per cell.
        let partition_arrays: Vec<ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i).clone()).collect();
        let sort_arrays: Vec<ArrayRef> =
            self.sort_columns.iter().map(|s| batch.column(s.index).clone()).collect();
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let parts = encode_group_keys(&conv.partition, &partition_arrays, batch.num_rows());
        let keys = encode_group_keys(&conv.sort, &sort_arrays, batch.num_rows());
        let payloads = conv.payload.convert_columns(&data_arrays).expect("encode payload");

        let limit = self.limit as usize;
        let output_rank = self.output_rank_number;
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let groups = &mut self.groups;
        let mut out_rows: Vec<Arc<OwnedRow>> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        let mut out_ranks: Vec<i64> = Vec::new();

        for row in 0..batch.num_rows() {
            // Compare the memcomparable sort key by borrow — no per-row `owned()` alloc until the row
            // is known to enter (the common case for a bounded Top-N is a row that does not).
            let key_row = keys.row(row);
            // Borrowed partition-key probe; the key bytes are copied only when a partition first
            // appears (buffers never empty out).
            let part = parts.row(row).data();
            let buffer = match groups.get_mut(part) {
                Some(buffer) => buffer,
                None => {
                    if track {
                        delta += (part.len() + GROUP_ENTRY_OVERHEAD) as isize;
                    }
                    groups.entry(ByteKey::from(part)).or_default()
                }
            };
            // Insert after any rows that order equal-or-before, preserving arrival order for ties
            // (byte compare of the memcomparable sort key).
            let pos = buffer.partition_point(|(k, _)| k.row() <= key_row);

            if output_rank {
                if pos >= limit {
                    continue; // beyond rank N — the new row never enters the top-N (nothing allocated)
                }
                let old_len = buffer.len();
                buffer.insert(pos, (key_row.owned(), Arc::new(payloads.row(row).owned())));
                if track {
                    delta += topn_entry_bytes(&buffer[pos]) as isize;
                }
                // Cascade from the new row's rank to the buffer end (capped at the limit): each rank's
                // occupant changes, so retract the old and append the new; a brand-new rank inserts.
                let upper = (old_len + 1).min(limit); // highest 1-based rank to emit
                for rank in (pos + 1)..=upper {
                    let new_occupant = buffer[rank - 1].1.clone();
                    if rank <= old_len {
                        out_rows.push(buffer[rank].1.clone()); // old occupant (shifted down by one)
                        out_kinds.push(1); // -U
                        out_ranks.push(rank as i64);
                        out_rows.push(new_occupant);
                        out_kinds.push(2); // +U
                        out_ranks.push(rank as i64);
                    } else {
                        out_rows.push(new_occupant);
                        out_kinds.push(0); // +I a brand-new rank
                        out_ranks.push(rank as i64);
                    }
                }
                if buffer.len() > limit {
                    if track {
                        delta -= buffer[limit..].iter().map(topn_entry_bytes).sum::<usize>() as isize;
                    }
                    buffer.truncate(limit); // the row past N was retracted by the -U at rank=limit
                }
            } else {
                let payload = Arc::new(payloads.row(row).owned());
                buffer.insert(pos, (key_row.owned(), Arc::clone(&payload)));
                if track {
                    delta += topn_entry_bytes(&buffer[pos]) as isize;
                }
                if buffer.len() > limit {
                    let evicted = buffer.pop().expect("buffer over limit is non-empty");
                    if track {
                        delta -= topn_entry_bytes(&evicted) as isize;
                    }
                    if *evicted.1 == *payload {
                        continue; // the new row was itself rank N+1 — it never entered the top-N
                    }
                    out_rows.push(evicted.1);
                    out_kinds.push(3); // -D the displaced row
                }
                out_rows.push(payload);
                out_kinds.push(0); // +I the new row
            }
        }
        self.memory.record(delta);
        self.memory.account()?;
        Ok(self.emit(out_rows, out_kinds, out_ranks))
    }

    fn emit(&self, out_rows: Vec<Arc<OwnedRow>>, out_kinds: Vec<i8>, out_ranks: Vec<i64>) -> RecordBatch {
        if out_rows.is_empty() {
            return RecordBatch::new_empty(Arc::new(Schema::empty()));
        }
        let schema = self.schema.as_ref().expect("schema set once a row was processed");
        let conv = self.converters.as_ref().expect("converters set");
        // The rank cascade emits the same buffered (Arc-shared) row at many positions — often the
        // same hot top-N rows across every cascade in the batch — so decode each distinct row once
        // and rebuild the emitted positions with a take. The output batch is unchanged; only the
        // row->columnar decode (the operator's dominant cost in the q19 profile) shrinks from
        // O(emitted) to O(distinct).
        let mut index_of: HashMap<*const OwnedRow, u32> = HashMap::default();
        let mut distinct: Vec<&Arc<OwnedRow>> = Vec::new();
        let mut positions: Vec<u32> = Vec::with_capacity(out_rows.len());
        for row in &out_rows {
            let idx = *index_of.entry(Arc::as_ptr(row)).or_insert_with(|| {
                distinct.push(row);
                (distinct.len() - 1) as u32
            });
            positions.push(idx);
        }
        let decoded: Vec<ArrayRef> = conv
            .payload
            .convert_rows(distinct.iter().map(|r| r.row()))
            .expect("decode top-n payloads");
        let indices = UInt32Array::from(positions);
        let mut columns: Vec<ArrayRef> = decoded
            .iter()
            .map(|c| take(c, &indices, None).expect("gather top-n payloads"))
            .collect();
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        if self.output_rank_number {
            fields.push(Field::new("w0$o0", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(out_ranks)));
        }
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build top-n changelog batch")
    }

    /// Serializes the buffered rows in per-partition buffer order (partition derivable from the row).
    pub(crate) fn snapshot(&self) -> Vec<u8> {
        let Some(schema) = &self.schema else { return Vec::new() };
        let Some(conv) = &self.converters else { return Vec::new() };
        let rows: Vec<Row> = self.groups.values().flatten().map(|(_, p)| p.row()).collect();
        if rows.is_empty() {
            return Vec::new();
        }
        let columns = conv.payload.convert_rows(rows).expect("decode top-n snapshot payloads");
        write_ipc(&RecordBatch::try_new(schema.clone(), columns).expect("top-n snapshot"))
    }

    pub(crate) fn restore(
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        output_rank_number: bool,
        bytes: &[u8],
    ) -> Self {
        let mut ranker = TopNRanker::new(partition_columns, sort_columns, limit, output_rank_number);
        for batch in read_ipc_if_present(bytes) {
            let arity = batch.num_columns();
            ranker.schema = Some(batch.schema());
            ranker.ensure_converters(&batch, arity);
            let conv = ranker.converters.as_ref().expect("converters set");
            let partition_arrays: Vec<ArrayRef> =
                ranker.partition_columns.iter().map(|&i| batch.column(i).clone()).collect();
            let sort_arrays: Vec<ArrayRef> =
                ranker.sort_columns.iter().map(|s| batch.column(s.index).clone()).collect();
            let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
            let parts = encode_group_keys(&conv.partition, &partition_arrays, batch.num_rows());
            let keys = encode_group_keys(&conv.sort, &sort_arrays, batch.num_rows());
            let payloads = conv.payload.convert_columns(&data_arrays).expect("encode payload");
            let groups = &mut ranker.groups;
            for row in 0..batch.num_rows() {
                groups
                    .entry(ByteKey::from(parts.row(row).data()))
                    .or_default()
                    .push((keys.row(row).owned(), Arc::new(payloads.row(row).owned())));
            }
        }
        ranker
    }
}

/// Retracting streaming Top-N — Flink's `RetractableTopNFunction`: a `ROW_NUMBER() OVER (PARTITION BY
/// … ORDER BY …) <= N` over a **changelog** input (e.g. a Top-N of a GROUP BY result). Unlike the
/// append-only ranker it keeps the **full** sorted buffer per key (never truncated to N), so when a
/// top-N row is retracted the row that was at rank N+1 can be promoted into the top-N.
///
/// Each input row accumulates (`+I`/`+U`) by inserting into the sorted buffer or retracts (`-U`/`-D`)
/// by removing the first full-row-equal match. The emitted changelog is then the **diff of the top-N
/// before vs after** the mutation: with the rank number projected, compared by rank position (a
/// changed occupant → `-U`(old)/`+U`(new), a newly-occupied rank → `+I`, a vacated rank → `-D`);
/// without it, compared as a row multiset (rows that left → `-D`, rows that entered → `+I`). This
/// single diff covers insert and retract and collapses to the same materialized result as Flink's
/// per-case cascade.
pub(crate) struct RetractableTopNRanker {
    partition_columns: Vec<usize>,
    sort_columns: Vec<SortColumn>,
    /// Rank window: output ranks `[offset+1, limit]` (1-based), i.e. buffer indices `[offset, limit)`.
    /// `offset = rankStart - 1` (0 for the common no-`OFFSET` case); `limit = rankEnd`.
    offset: i64,
    limit: i64,
    output_rank_number: bool,
    schema: Option<SchemaRef>,
    groups: HashMap<GroupKey, Vec<JoinRow>>,
    memory: OperatorMemory,
}

impl RetractableTopNRanker {
    fn new(
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        offset: i64,
        limit: i64,
        output_rank_number: bool,
    ) -> Self {
        RetractableTopNRanker {
            partition_columns,
            sort_columns,
            offset,
            limit,
            output_rank_number,
            schema: None,
            groups: HashMap::default(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds the full per-partition buffers by the operator's managed-memory budget (negative =
    /// unaccounted), accounting any restored buffers immediately.
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .groups
            .iter()
            .map(|(key, buffer)| {
                group_key_bytes(key)
                    + buffer.iter().map(|r| scalar_row_bytes(r) + GROUP_ENTRY_OVERHEAD).sum::<usize>()
            })
            .sum();
        self.memory.attach("retracting-top-n", budget_bytes, state)?;
        Ok(self)
    }

    fn push(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        let partition_arrays: Vec<&ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i)).collect();
        let data_arrays: Vec<&ArrayRef> = (0..arity).map(|i| batch.column(i)).collect();
        let row_kinds = row_kind_column(batch);
        // Output window: buffer indices [offset, limit) = ranks [offset+1, limit], clamped to len.
        let (offset, limit) = (self.offset as usize, self.limit as usize);
        let track = self.memory.tracking();
        let mut delta = 0isize;

        let mut out_rows: Vec<JoinRow> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        let mut out_ranks: Vec<i64> = Vec::new();

        for row in 0..batch.num_rows() {
            let key = read_key(&partition_arrays, row);
            if track && !self.groups.contains_key(&key) {
                delta += group_key_bytes(&key) as isize;
            }
            let full: JoinRow = data_arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a, row).expect("retract top-n row scalar"))
                .collect();
            // +I(0)/+U(2) accumulate; -U(1)/-D(3) retract.
            let retract = matches!(row_kinds.map(|k| k.value(row)).unwrap_or(0), 1 | 3);
            let sort = &self.sort_columns;
            let buffer = self.groups.entry(key).or_default();
            let old_top: Vec<JoinRow> =
                buffer[offset.min(buffer.len())..limit.min(buffer.len())].to_vec();
            if retract {
                if let Some(pos) = buffer.iter().position(|r| *r == full) {
                    if track {
                        delta -= (scalar_row_bytes(&buffer[pos]) + GROUP_ENTRY_OVERHEAD) as isize;
                    }
                    buffer.remove(pos);
                }
            } else {
                let pos = buffer
                    .partition_point(|r| compare_rows(r, &full, sort) != std::cmp::Ordering::Greater);
                if track {
                    delta += (scalar_row_bytes(&full) + GROUP_ENTRY_OVERHEAD) as isize;
                }
                buffer.insert(pos, full.clone());
            }
            let new_top: Vec<JoinRow> =
                buffer[offset.min(buffer.len())..limit.min(buffer.len())].to_vec();
            self.diff(&old_top, &new_top, &mut out_rows, &mut out_kinds, &mut out_ranks);
        }
        self.memory.record(delta);
        self.memory.account()?;
        Ok(self.emit(out_rows, out_kinds, out_ranks))
    }

    /// Appends the changelog transitioning the top-N from `old_top` to `new_top` (see the struct doc).
    fn diff(
        &self,
        old_top: &[JoinRow],
        new_top: &[JoinRow],
        out_rows: &mut Vec<JoinRow>,
        out_kinds: &mut Vec<i8>,
        out_ranks: &mut Vec<i64>,
    ) {
        if self.output_rank_number {
            for i in 0..old_top.len().max(new_top.len()) {
                let rank = self.offset + i as i64 + 1; // window position i is rank offset+i+1
                match (old_top.get(i), new_top.get(i)) {
                    (Some(o), Some(n)) if o != n => {
                        out_rows.push(o.clone());
                        out_kinds.push(1); // -U the old occupant of this rank
                        out_ranks.push(rank);
                        out_rows.push(n.clone());
                        out_kinds.push(2); // +U the new occupant
                        out_ranks.push(rank);
                    }
                    (Some(_), Some(_)) => {} // rank unchanged
                    (Some(o), None) => {
                        out_rows.push(o.clone());
                        out_kinds.push(3); // -D a rank that lost its occupant
                        out_ranks.push(rank);
                    }
                    (None, Some(n)) => {
                        out_rows.push(n.clone());
                        out_kinds.push(0); // +I a newly-occupied rank
                        out_ranks.push(rank);
                    }
                    (None, None) => {}
                }
            }
        } else {
            // No rank column — only membership matters; diff the two row multisets.
            let mut old_counts: HashMap<&JoinRow, i32> = HashMap::default();
            for r in old_top {
                *old_counts.entry(r).or_insert(0) += 1;
            }
            for r in new_top {
                match old_counts.get_mut(r) {
                    Some(c) if *c > 0 => *c -= 1, // still present — no change
                    _ => {
                        out_rows.push(r.clone());
                        out_kinds.push(0); // +I a row that entered the top-N
                    }
                }
            }
            for (r, count) in old_counts {
                for _ in 0..count {
                    out_rows.push(r.clone());
                    out_kinds.push(3); // -D a row that left the top-N
                }
            }
        }
    }

    fn emit(&self, out_rows: Vec<JoinRow>, out_kinds: Vec<i8>, out_ranks: Vec<i64>) -> RecordBatch {
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
        if self.output_rank_number {
            fields.push(Field::new("w0$o0", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(out_ranks)));
        }
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build retract top-n changelog batch")
    }

    fn snapshot(&self) -> Vec<u8> {
        let Some(schema) = &self.schema else { return Vec::new() };
        let rows: Vec<&JoinRow> = self.groups.values().flatten().collect();
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
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("retract top-n snapshot"),
        )
    }

    fn restore(
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        offset: i64,
        limit: i64,
        output_rank_number: bool,
        bytes: &[u8],
    ) -> Self {
        let mut ranker =
            RetractableTopNRanker::new(partition_columns, sort_columns, offset, limit, output_rank_number);
        for batch in read_ipc_if_present(bytes) {
            ranker.schema = Some(batch.schema());
            let partition_arrays: Vec<&ArrayRef> =
                ranker.partition_columns.iter().map(|&i| batch.column(i)).collect();
            for row in 0..batch.num_rows() {
                let key = read_key(&partition_arrays, row);
                let full: JoinRow = (0..batch.num_columns())
                    .map(|i| ScalarValue::try_from_array(batch.column(i), row).expect("retract top-n scalar"))
                    .collect();
                ranker.groups.entry(key).or_default().push(full); // stored in buffer (sorted) order
            }
        }
        ranker
    }
}

/// The Top-N handle the JVM holds: append-only (insert-only input, bounded buffer) or retracting
/// (changelog input, full buffer). Both push a batch and return a changelog, snapshot, and restore.
pub(crate) enum TopNHandle {
    Append(TopNRanker),
    Retract(RetractableTopNRanker),
}

impl TopNHandle {
    fn push(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        match self {
            TopNHandle::Append(r) => r.push(batch),
            TopNHandle::Retract(r) => r.push(batch),
        }
    }

    /// Bounds the ranker's buffers by the operator's managed-memory budget (negative = unaccounted).
    fn with_memory_budget(self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        Ok(match self {
            TopNHandle::Append(r) => TopNHandle::Append(r.with_memory_budget(budget_bytes)?),
            TopNHandle::Retract(r) => TopNHandle::Retract(r.with_memory_budget(budget_bytes)?),
        })
    }

    fn snapshot(&self) -> Vec<u8> {
        match self {
            TopNHandle::Append(r) => r.snapshot(),
            TopNHandle::Retract(r) => r.snapshot(),
        }
    }
}

/// Window Top-N / window deduplication over a windowing-TVF input (Flink's `WindowRank` /
/// `WindowDeduplicate`): within each window (the attached `window_start`/`window_end` columns) and
/// partition key, rank rows by the sort key and keep the top N, emitting them once the watermark
/// closes the window. Append-only — a closed window's rows are emitted exactly once. Window
/// deduplication is the `limit = 1` case (keep-first = sort by rowtime ascending, keep-last =
/// descending). Late rows (whose window already closed) are dropped, matching the host.
pub(crate) struct WindowRanker {
    window_start_col: usize,
    window_end_col: usize,
    partition_columns: Vec<usize>,
    sort_columns: Vec<SortColumn>,
    limit: i64,
    output_rank_number: bool,
    current_watermark: i64,
    /// Bounded, sorted top-N buffer per (window_end, window_start, partition key).
    groups: HashMap<(i64, i64, GroupKey), Vec<JoinRow>>,
    schema: Option<SchemaRef>,
    memory: OperatorMemory,
}

impl WindowRanker {
    fn new(
        window_start_col: usize,
        window_end_col: usize,
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        output_rank_number: bool,
    ) -> Self {
        WindowRanker {
            window_start_col,
            window_end_col,
            partition_columns,
            sort_columns,
            limit,
            output_rank_number,
            current_watermark: i64::MIN,
            groups: HashMap::default(),
            schema: None,
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds the per-window buffers by the operator's managed-memory budget (negative =
    /// unaccounted), accounting any restored buffers immediately.
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .groups
            .iter()
            .map(|((_, _, key), buffer)| {
                group_key_bytes(key)
                    + buffer.iter().map(|r| scalar_row_bytes(r) + GROUP_ENTRY_OVERHEAD).sum::<usize>()
            })
            .sum();
        self.memory.attach("window-rank", budget_bytes, state)?;
        Ok(self)
    }

    fn push(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        let ws = rt_to_millis(batch.column(self.window_start_col));
        let we = rt_to_millis(batch.column(self.window_end_col));
        let partition_arrays: Vec<&ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i)).collect();
        let data_arrays: Vec<&ArrayRef> = (0..arity).map(|i| batch.column(i)).collect();
        let track = self.memory.tracking();
        let mut delta = 0isize;
        for row in 0..batch.num_rows() {
            let window_end = we.value(row);
            if window_end <= self.current_watermark {
                continue; // late: the window already closed and emitted
            }
            let window_start = ws.value(row);
            let key = read_key(&partition_arrays, row);
            let full: JoinRow = data_arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a, row).expect("window-rank row scalar"))
                .collect();
            if track {
                delta += (scalar_row_bytes(&full) + GROUP_ENTRY_OVERHEAD) as isize;
            }
            let key_bytes = if track { group_key_bytes(&key) } else { 0 };
            let buffer = self.groups.entry((window_end, window_start, key)).or_default();
            // An empty buffer means the (window, key) entry was just created (never emptied by push).
            if track && buffer.is_empty() {
                delta += key_bytes as isize;
            }
            // Insert after rows ordering equal-or-before, preserving arrival order for ties (the
            // ROW_NUMBER tie-break), then drop anything past rank N.
            let pos = buffer
                .partition_point(|r| compare_rows(r, &full, &self.sort_columns) != std::cmp::Ordering::Greater);
            buffer.insert(pos, full);
            if buffer.len() as i64 > self.limit {
                if track {
                    delta -= buffer[self.limit as usize..]
                        .iter()
                        .map(|r| scalar_row_bytes(r) + GROUP_ENTRY_OVERHEAD)
                        .sum::<usize>() as isize;
                }
                buffer.truncate(self.limit as usize);
            }
        }
        self.memory.record(delta);
        self.memory.account()
    }

    /// Emits the top-N rows of every window the watermark has closed, in rank order (with the rank
    /// number appended when the host projects it), and evicts those windows.
    fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.current_watermark = watermark;
        let mut ready: Vec<(i64, i64, GroupKey)> =
            self.groups.keys().filter(|(we, _, _)| *we <= watermark).cloned().collect();
        // Evict in (window_end, window_start) order for a deterministic emission sequence.
        ready.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        let mut rows: Vec<JoinRow> = Vec::new();
        let mut ranks: Vec<i64> = Vec::new();
        let track = self.memory.tracking();
        let mut freed = 0usize;
        for group in ready {
            let buffer = self.groups.remove(&group).expect("ready group present");
            if track {
                freed += GROUP_ENTRY_OVERHEAD;
                freed += buffer.iter().map(|r| scalar_row_bytes(r) + GROUP_ENTRY_OVERHEAD).sum::<usize>();
            }
            for (rank, row) in buffer.into_iter().enumerate() {
                rows.push(row);
                ranks.push(rank as i64 + 1);
            }
        }
        self.memory.forget(freed);
        self.memory.account_shrink();
        self.emit(rows, ranks)
    }

    fn emit(&self, rows: Vec<JoinRow>, ranks: Vec<i64>) -> RecordBatch {
        let schema = match &self.schema {
            Some(schema) => schema.clone(),
            None => return RecordBatch::new_empty(Arc::new(Schema::empty())),
        };
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| scalars_to_array(rows.iter().map(|r| r[j].clone()).collect(), fields[j].data_type()))
            .collect();
        if self.output_rank_number {
            fields.push(Field::new("w0$o0", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(ranks)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("window-rank output batch")
    }

    fn snapshot(&self) -> Vec<u8> {
        let mut out = self.current_watermark.to_le_bytes().to_vec();
        let Some(schema) = &self.schema else { return out };
        let rows: Vec<&JoinRow> = self.groups.values().flatten().collect();
        if rows.is_empty() {
            return out;
        }
        let fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| scalars_to_array(rows.iter().map(|r| r[j].clone()).collect(), fields[j].data_type()))
            .collect();
        out.extend_from_slice(&write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("window-rank snapshot"),
        ));
        out
    }

    fn restore(
        window_start_col: usize,
        window_end_col: usize,
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        output_rank_number: bool,
        bytes: &[u8],
    ) -> Self {
        let mut ranker = WindowRanker::new(
            window_start_col,
            window_end_col,
            partition_columns,
            sort_columns,
            limit,
            output_rank_number,
        );
        if bytes.len() < 8 {
            return ranker;
        }
        ranker.current_watermark = i64::from_le_bytes(bytes[0..8].try_into().expect("watermark"));
        // Re-inserting through push reproduces each group's sorted, truncated buffer; buffered rows
        // have window_end > the watermark, so none are dropped as late.
        for batch in read_ipc_if_present(&bytes[8..]) {
            ranker.push(&batch);
        }
        ranker
    }
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_windowRankerStateBytes, WindowRanker);

/// [`state_bytes_getter`] for the Top-N handle, which wraps its two ranker variants in an enum.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_topNRankerStateBytes<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let ranker = unsafe { &*(handle as *const TopNHandle) };
    (match ranker {
        TopNHandle::Append(r) => r.memory.state_bytes,
        TopNHandle::Retract(r) => r.memory.state_bytes,
    }) as jlong
}

/// Creates a window-rank ranker (window Top-N / window deduplication) over the attached
/// window_start/window_end columns and returns an opaque handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createWindowRanker<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    window_start_col: jint,
    window_end_col: jint,
    partition_columns: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    limit: jlong,
    output_rank_number: jboolean,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
    let ranker = WindowRanker::new(
        window_start_col as usize,
        window_end_col as usize,
        partitions,
        sort,
        limit,
        output_rank_number != 0,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, ranker)
}

/// Buffers an input batch (no output); each window's top-N rows are emitted when the watermark
/// closes the window.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushWindowRanker<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let ranker = unsafe { &mut *(handle as *mut WindowRanker) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        ranker.push(&batch)
    };
    if let Err(e) = result {
        throw_memory_limit(&mut env, &e.to_string());
    }
}

/// Exports the top-N rows of every window the watermark has closed (with the rank number appended
/// when the host projects it).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushWindowRanker<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let ranker = unsafe { &mut *(handle as *mut WindowRanker) };
    let result = ranker.flush(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases the window-rank ranker and its per-window state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeWindowRanker<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<WindowRanker>(handle));
    }
}

/// Serializes the ranker's per-window buffers and watermark for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotWindowRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let ranker = unsafe { &mut *(handle as *mut WindowRanker) };
    env.byte_array_from_slice(&ranker.snapshot())
        .expect("failed to allocate window-rank snapshot array")
        .into_raw()
}

/// Rebuilds a window-rank ranker from a snapshot and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreWindowRanker<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    window_start_col: jint,
    window_end_col: jint,
    partition_columns: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    limit: jlong,
    output_rank_number: jboolean,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read window-rank snapshot");
    let ranker = WindowRanker::restore(
        window_start_col as usize,
        window_end_col as usize,
        partitions,
        sort,
        limit,
        output_rank_number != 0,
        &bytes,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, ranker)
}

/// Builds the sort-column comparator config from three parallel arrays (column index, ascending,
/// nulls-first), as the JVM passes the resolved ORDER BY spec.
pub(crate) fn read_sort_columns(
    env: &JNIEnv,
    indices: &JIntArray,
    ascending: &JIntArray,
    nulls_first: &JIntArray,
) -> Vec<SortColumn> {
    let indices = read_columns(env, indices);
    let ascending = read_int_array(env, ascending);
    let nulls_first = read_int_array(env, nulls_first);
    indices
        .into_iter()
        .enumerate()
        .map(|(i, index)| SortColumn {
            index,
            ascending: ascending[i] != 0,
            nulls_first: nulls_first[i] != 0,
        })
        .collect()
}

/// Creates an append-only streaming Top-N ranker (`ROW_NUMBER ... <= limit`, no rank-number output)
/// and returns an opaque handle. The JVM owns it and must release it with the matching close.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createTopNRanker<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    offset: jlong,
    limit: jlong,
    output_rank_number: jboolean,
    retracting: jboolean,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
    let handle = if retracting != 0 {
        TopNHandle::Retract(RetractableTopNRanker::new(
            partitions,
            sort,
            offset,
            limit,
            output_rank_number != 0,
        ))
    } else {
        // The append-only ranker is the no-OFFSET path (offset always 0).
        TopNHandle::Append(TopNRanker::new(partitions, sort, limit, output_rank_number != 0))
    };
    boxed_or_throw(&mut env, handle.with_memory_budget(memory_budget_bytes))
}

/// Folds an input batch into the per-partition top-N and exports the changelog it produces (the
/// input columns plus `$row_kind$`).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushTopNRanker<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let ranker = unsafe { &mut *(handle as *mut TopNHandle) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        ranker.push(&batch)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Serializes the ranker's per-partition buffers for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let ranker = unsafe { &mut *(handle as *mut TopNHandle) };
    env.byte_array_from_slice(&ranker.snapshot())
        .expect("failed to allocate top-n snapshot array")
        .into_raw()
}

/// Rebuilds a Top-N ranker from a snapshot and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreTopNRanker<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    offset: jlong,
    limit: jlong,
    output_rank_number: jboolean,
    retracting: jboolean,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read top-n snapshot");
    let handle = if retracting != 0 {
        TopNHandle::Retract(RetractableTopNRanker::restore(
            partitions,
            sort,
            offset,
            limit,
            output_rank_number != 0,
            &bytes,
        ))
    } else {
        TopNHandle::Append(TopNRanker::restore(partitions, sort, limit, output_rank_number != 0, &bytes))
    };
    boxed_or_throw(&mut env, handle.with_memory_budget(memory_budget_bytes))
}

/// Releases a Top-N ranker handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeTopNRanker<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<TopNHandle>(handle));
    }
}
