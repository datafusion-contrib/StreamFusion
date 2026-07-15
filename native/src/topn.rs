use crate::*;

/// One ORDER BY column for the Top-N comparator: which column, ascending vs descending, and whether
/// nulls sort first (independent of direction, as in SQL `NULLS FIRST`/`LAST`).
#[derive(Clone)]
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

impl TopNConverters {
    /// Builds the three arrow-row converters from a batch's column types.
    fn build(
        batch: &RecordBatch,
        arity: usize,
        partition_columns: &[usize],
        sort_columns: &[SortColumn],
    ) -> Self {
        let payload = RowConverter::new(
            (0..arity).map(|i| SortField::new(batch.column(i).data_type().clone())).collect(),
        )
        .expect("top-n payload converter");
        // A plain LIMIT (no ORDER BY) has zero sort columns; like the empty partition key, encode a
        // constant dummy so all rows compare equal and the buffer preserves arrival order (Flink's
        // first-n by arrival). With sort columns present, encode them memcomparable with their options.
        let sort = if sort_columns.is_empty() {
            RowConverter::new(vec![SortField::new(DataType::Boolean)]).expect("top-n empty sort converter")
        } else {
            RowConverter::new(
                sort_columns
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
            partition_columns.iter().map(|&i| batch.column(i)).collect();
        let partition = key_row_converter(&partition_refs);
        TopNConverters { partition, sort, payload }
    }
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

fn topn_staged_entry_bytes(key: &ByteKey, old: &Vec<Arc<OwnedRow>>) -> usize {
    // The key is owned once by the lookup map and once by the deterministic first-touch vector.
    byte_key_bytes(&key.0)
        + key.len()
        + std::mem::size_of::<ByteKey>()
        + std::mem::size_of::<Vec<Arc<OwnedRow>>>()
        + old.capacity() * std::mem::size_of::<Arc<OwnedRow>>()
}

pub(crate) struct TopNRanker {
    partition_columns: Vec<usize>,
    sort_columns: Vec<SortColumn>,
    limit: i64,
    output_rank_number: bool,
    // Mini-batch mode: emit the NET rank diff per logical bundle (old top-N vs new top-N for each
    // touched partition) instead of the host's per-record -U/+U cascade. Gated on the host plan
    // running mini-batch, whose parity contract is the collapsed changelog — which the diff
    // preserves exactly — rather than the per-record byte sequence (see divergences/20).
    net_diff: bool,
    schema: Option<SchemaRef>,
    converters: Option<TopNConverters>,
    // Keyed by the partition key's encoded bytes (`ByteKey`): the per-row probe hashes the BORROWED
    // bytes, so a row for an existing partition — or one dropped at rank > N — allocates nothing.
    groups: HashMap<ByteKey, Vec<TopNRow>>,
    // Transient mini-batch preimages, captured once per partition in deterministic first-touch
    // order and drained at the next logical boundary.
    staged_order: Vec<ByteKey>,
    staged_old_tops: HashMap<ByteKey, Vec<Arc<OwnedRow>>>,
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
        net_diff: bool,
    ) -> Self {
        TopNRanker {
            partition_columns,
            sort_columns,
            limit,
            output_rank_number,
            net_diff,
            schema: None,
            converters: None,
            groups: HashMap::default(),
            staged_order: Vec::new(),
            staged_old_tops: HashMap::default(),
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
        if self.converters.is_none() {
            self.converters =
                Some(TopNConverters::build(batch, arity, &self.partition_columns, &self.sort_columns));
        }
    }

    pub(crate) fn push(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        if self.net_diff {
            return self.push_net_diff(batch);
        }
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

    pub(crate) fn staged_partitions(&self) -> usize {
        self.staged_order.len()
    }

    pub(crate) fn staging_bytes(&self) -> usize {
        self.staged_old_tops
            .iter()
            .map(|(key, old)| topn_staged_entry_bytes(key, old))
            .sum()
    }

    /// Folds one physical batch into a logical mini-batch. The first touch of a partition captures
    /// its preimage; output is deferred until `flush_net_diff`, so Arrow batch boundaries are not
    /// observable in the collapsed changelog.
    fn push_net_diff(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        self.ensure_converters(batch, arity);
        let conv = self.converters.as_ref().expect("converters set");
        let partition_arrays: Vec<ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i).clone()).collect();
        let sort_arrays: Vec<ArrayRef> =
            self.sort_columns.iter().map(|s| batch.column(s.index).clone()).collect();
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let parts = encode_group_keys(&conv.partition, &partition_arrays, batch.num_rows());
        let keys = encode_group_keys(&conv.sort, &sort_arrays, batch.num_rows());
        let payloads = conv.payload.convert_columns(&data_arrays).expect("encode payload");

        let limit = self.limit as usize;
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let groups = &mut self.groups;

        let staged_order = &mut self.staged_order;
        let staged_old_tops = &mut self.staged_old_tops;
        for row in 0..batch.num_rows() {
            let key_row = keys.row(row);
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
            if !staged_old_tops.contains_key(part) {
                let key = ByteKey::from(part);
                let old: Vec<Arc<OwnedRow>> = buffer.iter().map(|(_, p)| p.clone()).collect();
                if track {
                    delta += topn_staged_entry_bytes(&key, &old) as isize;
                }
                staged_order.push(key.clone());
                staged_old_tops.insert(key, old);
            }
            let pos = buffer.partition_point(|(k, _)| k.row() <= key_row);
            if pos >= limit {
                continue; // beyond rank N — never enters (a buffer never exceeds the limit)
            }
            buffer.insert(pos, (key_row.owned(), Arc::new(payloads.row(row).owned())));
            if track {
                delta += topn_entry_bytes(&buffer[pos]) as isize;
            }
            if buffer.len() > limit {
                let evicted = buffer.pop().expect("buffer over limit is non-empty");
                if track {
                    delta -= topn_entry_bytes(&evicted) as isize;
                }
            }
        }
        self.memory.record(delta);
        self.memory.account()?;

        Ok(self.emit(Vec::new(), Vec::new(), Vec::new()))
    }

    /// Emits one final diff per partition touched since the preceding logical boundary.
    pub(crate) fn flush_net_diff(&mut self) -> RecordBatch {
        if !self.net_diff {
            return self.emit(Vec::new(), Vec::new(), Vec::new());
        }
        let staged_bytes = if self.memory.tracking() { self.staging_bytes() } else { 0 };
        let touched = std::mem::take(&mut self.staged_order);
        let old_tops = std::mem::take(&mut self.staged_old_tops);
        let mut out_rows: Vec<Arc<OwnedRow>> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        let mut out_ranks: Vec<i64> = Vec::new();
        for part in touched {
            let old = &old_tops[&part];
            let new: Vec<Arc<OwnedRow>> = self.groups[&part]
                .iter()
                .map(|(_, payload)| Arc::clone(payload))
                .collect();
            if self.output_rank_number {
                diff_top(
                    true,
                    0,
                    old,
                    &new,
                    &mut out_rows,
                    &mut out_kinds,
                    &mut out_ranks,
                );
            } else {
                // Preserve append-only Top-N's delete-before-insert ordering at a bundle boundary.
                let mut counts: HashMap<&[u8], i64> = HashMap::default();
                for row in old {
                    *counts.entry(row.row().data()).or_insert(0) += 1;
                }
                for row in &new {
                    *counts.entry(row.row().data()).or_insert(0) -= 1;
                }
                for row in old {
                    let count = counts.get_mut(row.row().data()).expect("counted");
                    if *count > 0 {
                        *count -= 1;
                        out_rows.push(Arc::clone(row));
                        out_kinds.push(3);
                    }
                }
                for row in &new {
                    let count = counts.get_mut(row.row().data()).expect("counted");
                    if *count < 0 {
                        *count += 1;
                        out_rows.push(Arc::clone(row));
                        out_kinds.push(0);
                    }
                }
            }
        }
        self.memory.forget(staged_bytes);
        self.memory.account_shrink();
        self.emit(out_rows, out_kinds, out_ranks)
    }

    fn emit(&self, out_rows: Vec<Arc<OwnedRow>>, out_kinds: Vec<i8>, out_ranks: Vec<i64>) -> RecordBatch {
        emit_changelog(
            self.schema.as_ref(),
            self.converters.as_ref(),
            self.output_rank_number,
            out_rows,
            out_kinds,
            out_ranks,
        )
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
        net_diff: bool,
        bytes: &[u8],
    ) -> Self {
        let mut ranker =
            TopNRanker::new(partition_columns, sort_columns, limit, output_rank_number, net_diff);
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

/// Decodes an emitted Top-N changelog — rows as Arc-shared payload byte rows — into the output
/// batch. A cascade/diff emits the same buffered row at many positions (often the same hot top-N
/// rows across every mutation in the batch), so each distinct row decodes once and the emitted
/// positions are rebuilt with a take: the row->columnar decode (the dominant cost in the q19
/// profile) shrinks from O(emitted) to O(distinct).
fn emit_changelog(
    schema: Option<&SchemaRef>,
    converters: Option<&TopNConverters>,
    output_rank_number: bool,
    out_rows: Vec<Arc<OwnedRow>>,
    out_kinds: Vec<i8>,
    out_ranks: Vec<i64>,
) -> RecordBatch {
    if out_rows.is_empty() {
        return RecordBatch::new_empty(Arc::new(Schema::empty()));
    }
    let schema = schema.expect("schema set once a row was processed");
    let conv = converters.expect("converters set");
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
    if output_rank_number {
        fields.push(Field::new("w0$o0", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(out_ranks)));
    }
    fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
    columns.push(Arc::new(Int8Array::from(out_kinds)));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("failed to build top-n changelog batch")
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
    net_diff: bool,
    schema: Option<SchemaRef>,
    converters: Option<TopNConverters>,
    // The append-only ranker's byte-row state (see TopNRow): partition probes by borrowed bytes,
    // rows as (memcomparable sort key, Arc-shared payload) — no per-cell `ScalarValue`, and the
    // before/after top-N snapshots the diff reads are refcount bumps, not row deep-clones.
    groups: HashMap<ByteKey, Vec<TopNRow>>,
    staged_order: Vec<ByteKey>,
    staged_old_tops: HashMap<ByteKey, Vec<Arc<OwnedRow>>>,
    memory: OperatorMemory,
}

impl RetractableTopNRanker {
    pub(crate) fn new(
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
            net_diff: false,
            schema: None,
            converters: None,
            groups: HashMap::default(),
            staged_order: Vec::new(),
            staged_old_tops: HashMap::default(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    pub(crate) fn with_net_diff(mut self, net_diff: bool) -> Self {
        self.net_diff = net_diff;
        self
    }

    /// Bounds the full per-partition buffers by the operator's managed-memory budget (negative =
    /// unaccounted), accounting any restored buffers immediately.
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .groups
            .iter()
            .map(|(key, buffer)| {
                byte_key_bytes(&key.0) + buffer.iter().map(topn_entry_bytes).sum::<usize>()
            })
            .sum();
        self.memory.attach("retracting-top-n", budget_bytes, state)?;
        Ok(self)
    }

    pub(crate) fn push(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        if self.net_diff {
            return self.push_net_diff(batch);
        }
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        if self.converters.is_none() {
            self.converters =
                Some(TopNConverters::build(batch, arity, &self.partition_columns, &self.sort_columns));
        }
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

        let row_kinds = row_kind_column(batch);
        // Output window: buffer indices [offset, limit) = ranks [offset+1, limit], clamped to len.
        let (offset, limit) = (self.offset as usize, self.limit as usize);
        let (rank_output, rank_base) = (self.output_rank_number, self.offset);
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let groups = &mut self.groups;

        let mut out_rows: Vec<Arc<OwnedRow>> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        let mut out_ranks: Vec<i64> = Vec::new();

        for row in 0..batch.num_rows() {
            // Borrowed partition-key probe; the key bytes are copied only when a partition first
            // appears (a full retracting buffer never removes its partition entry).
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
            // The top-N window before the mutation: Arc bumps of the payloads, not row clones.
            let old_top: Vec<Arc<OwnedRow>> = buffer
                [offset.min(buffer.len())..limit.min(buffer.len())]
                .iter()
                .map(|(_, p)| Arc::clone(p))
                .collect();
            // +I(0)/+U(2) accumulate; -U(1)/-D(3) retract.
            let retract = matches!(row_kinds.map(|k| k.value(row)).unwrap_or(0), 1 | 3);
            if retract {
                // Remove the first full-row-equal match — a byte compare of the value-encoded
                // payload (the append-only ranker's equality trade).
                let full = payloads.row(row);
                if let Some(pos) = buffer.iter().position(|(_, p)| p.row() == full) {
                    if track {
                        delta -= topn_entry_bytes(&buffer[pos]) as isize;
                    }
                    buffer.remove(pos);
                }
            } else {
                // Insert after any rows that order equal-or-before, preserving arrival order for
                // ties (byte compare of the memcomparable sort key).
                let key_row = keys.row(row);
                let pos = buffer.partition_point(|(k, _)| k.row() <= key_row);
                buffer.insert(pos, (key_row.owned(), Arc::new(payloads.row(row).owned())));
                if track {
                    delta += topn_entry_bytes(&buffer[pos]) as isize;
                }
            }
            let new_top: Vec<Arc<OwnedRow>> = buffer
                [offset.min(buffer.len())..limit.min(buffer.len())]
                .iter()
                .map(|(_, p)| Arc::clone(p))
                .collect();
            diff_top(rank_output, rank_base, &old_top, &new_top, &mut out_rows, &mut out_kinds, &mut out_ranks);
        }
        self.memory.record(delta);
        self.memory.account()?;
        Ok(emit_changelog(
            self.schema.as_ref(),
            self.converters.as_ref(),
            self.output_rank_number,
            out_rows,
            out_kinds,
            out_ranks,
        ))
    }

    pub(crate) fn staged_partitions(&self) -> usize {
        self.staged_order.len()
    }

    pub(crate) fn staging_bytes(&self) -> usize {
        self.staged_old_tops
            .iter()
            .map(|(key, old)| topn_staged_entry_bytes(key, old))
            .sum()
    }

    /// Applies a changelog batch to the full retracting buffers while retaining only the first
    /// visible Top-N preimage for each partition touched in the current logical mini-batch.
    fn push_net_diff(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        if self.converters.is_none() {
            self.converters =
                Some(TopNConverters::build(batch, arity, &self.partition_columns, &self.sort_columns));
        }
        let conv = self.converters.as_ref().expect("converters set");
        let partition_arrays: Vec<ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i).clone()).collect();
        let sort_arrays: Vec<ArrayRef> =
            self.sort_columns.iter().map(|s| batch.column(s.index).clone()).collect();
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let parts = encode_group_keys(&conv.partition, &partition_arrays, batch.num_rows());
        let keys = encode_group_keys(&conv.sort, &sort_arrays, batch.num_rows());
        let payloads = conv.payload.convert_columns(&data_arrays).expect("encode payload");
        let row_kinds = row_kind_column(batch);
        let (offset, limit) = (self.offset as usize, self.limit as usize);
        let track = self.memory.tracking();
        let mut delta = 0isize;

        for row in 0..batch.num_rows() {
            let part = parts.row(row).data();
            let buffer = match self.groups.get_mut(part) {
                Some(buffer) => buffer,
                None => {
                    if track {
                        delta += (part.len() + GROUP_ENTRY_OVERHEAD) as isize;
                    }
                    self.groups.entry(ByteKey::from(part)).or_default()
                }
            };
            if !self.staged_old_tops.contains_key(part) {
                let key = ByteKey::from(part);
                let old = buffer[offset.min(buffer.len())..limit.min(buffer.len())]
                    .iter()
                    .map(|(_, payload)| Arc::clone(payload))
                    .collect();
                if track {
                    delta += topn_staged_entry_bytes(&key, &old) as isize;
                }
                self.staged_order.push(key.clone());
                self.staged_old_tops.insert(key, old);
            }

            let retract = matches!(row_kinds.map(|k| k.value(row)).unwrap_or(0), 1 | 3);
            if retract {
                let full = payloads.row(row);
                if let Some(pos) = buffer.iter().position(|(_, payload)| payload.row() == full) {
                    if track {
                        delta -= topn_entry_bytes(&buffer[pos]) as isize;
                    }
                    buffer.remove(pos);
                }
            } else {
                let key_row = keys.row(row);
                let pos = buffer.partition_point(|(key, _)| key.row() <= key_row);
                buffer.insert(pos, (key_row.owned(), Arc::new(payloads.row(row).owned())));
                if track {
                    delta += topn_entry_bytes(&buffer[pos]) as isize;
                }
            }
        }
        self.memory.record(delta);
        self.memory.account()?;
        Ok(emit_changelog(
            self.schema.as_ref(),
            self.converters.as_ref(),
            self.output_rank_number,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ))
    }

    pub(crate) fn flush_net_diff(&mut self) -> RecordBatch {
        if !self.net_diff {
            return emit_changelog(
                self.schema.as_ref(),
                self.converters.as_ref(),
                self.output_rank_number,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            );
        }
        let staged_bytes = if self.memory.tracking() { self.staging_bytes() } else { 0 };
        let touched = std::mem::take(&mut self.staged_order);
        let old_tops = std::mem::take(&mut self.staged_old_tops);
        let (offset, limit) = (self.offset as usize, self.limit as usize);
        let mut out_rows = Vec::new();
        let mut out_kinds = Vec::new();
        let mut out_ranks = Vec::new();
        for part in touched {
            let buffer = &self.groups[&part];
            let new_top: Vec<Arc<OwnedRow>> = buffer
                [offset.min(buffer.len())..limit.min(buffer.len())]
                .iter()
                .map(|(_, payload)| Arc::clone(payload))
                .collect();
            diff_top(
                self.output_rank_number,
                self.offset,
                &old_tops[&part],
                &new_top,
                &mut out_rows,
                &mut out_kinds,
                &mut out_ranks,
            );
        }
        self.memory.forget(staged_bytes);
        self.memory.account_shrink();
        emit_changelog(
            self.schema.as_ref(),
            self.converters.as_ref(),
            self.output_rank_number,
            out_rows,
            out_kinds,
            out_ranks,
        )
    }

    /// Serializes the buffered rows in per-partition buffer order (partition derivable from the row).
    fn snapshot(&self) -> Vec<u8> {
        let Some(schema) = &self.schema else { return Vec::new() };
        let Some(conv) = &self.converters else { return Vec::new() };
        let rows: Vec<Row> = self.groups.values().flatten().map(|(_, p)| p.row()).collect();
        if rows.is_empty() {
            return Vec::new();
        }
        let columns = conv.payload.convert_rows(rows).expect("decode retract top-n snapshot");
        write_ipc(&RecordBatch::try_new(schema.clone(), columns).expect("retract top-n snapshot"))
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
            let arity = batch.num_columns();
            ranker.schema = Some(batch.schema());
            if ranker.converters.is_none() {
                ranker.converters = Some(TopNConverters::build(
                    &batch,
                    arity,
                    &ranker.partition_columns,
                    &ranker.sort_columns,
                ));
            }
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
                    .push((keys.row(row).owned(), Arc::new(payloads.row(row).owned()))); // buffer order
            }
        }
        ranker
    }
}

/// Appends the changelog transitioning a partition's top-N from `old_top` to `new_top` (see the
/// retracting ranker's doc). Row identity is payload-byte equality, with an `Arc` pointer check as
/// the fast path (an unchanged rank is usually the same buffered row).
fn diff_top(
    output_rank_number: bool,
    rank_base: i64,
    old_top: &[Arc<OwnedRow>],
    new_top: &[Arc<OwnedRow>],
    out_rows: &mut Vec<Arc<OwnedRow>>,
    out_kinds: &mut Vec<i8>,
    out_ranks: &mut Vec<i64>,
) {
    if output_rank_number {
        for i in 0..old_top.len().max(new_top.len()) {
            let rank = rank_base + i as i64 + 1; // window position i is rank offset+i+1
            match (old_top.get(i), new_top.get(i)) {
                (Some(o), Some(n)) if !Arc::ptr_eq(o, n) && o.row() != n.row() => {
                    out_rows.push(Arc::clone(o));
                    out_kinds.push(1); // -U the old occupant of this rank
                    out_ranks.push(rank);
                    out_rows.push(Arc::clone(n));
                    out_kinds.push(2); // +U the new occupant
                    out_ranks.push(rank);
                }
                (Some(_), Some(_)) => {} // rank unchanged
                (Some(o), None) => {
                    out_rows.push(Arc::clone(o));
                    out_kinds.push(3); // -D a rank that lost its occupant
                    out_ranks.push(rank);
                }
                (None, Some(n)) => {
                    out_rows.push(Arc::clone(n));
                    out_kinds.push(0); // +I a newly-occupied rank
                    out_ranks.push(rank);
                }
                (None, None) => {}
            }
        }
    } else {
        // No rank column — only membership matters; diff the two row multisets by payload bytes.
        let mut old_counts: HashMap<&[u8], i32> = HashMap::default();
        for r in old_top {
            *old_counts.entry(r.row().data()).or_insert(0) += 1;
        }
        for r in new_top {
            match old_counts.get_mut(r.row().data()) {
                Some(c) if *c > 0 => *c -= 1, // still present — no change
                _ => {
                    out_rows.push(Arc::clone(r));
                    out_kinds.push(0); // +I a row that entered the top-N
                }
            }
        }
        for r in old_top {
            let count = old_counts.get_mut(r.row().data()).expect("counted");
            if *count > 0 {
                *count -= 1;
                out_rows.push(Arc::clone(r));
                out_kinds.push(3); // -D a row that left the top-N
            }
        }
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

    fn flush(&mut self) -> RecordBatch {
        match self {
            TopNHandle::Append(r) => r.flush_net_diff(),
            TopNHandle::Retract(r) => r.flush_net_diff(),
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

    fn snapshot_partitions(
        &self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        topn_snapshot_partitions(
            &self.snapshot(),
            self.partition_columns(),
            max_parallelism,
            timestamp_precisions,
        )
    }

    fn partition_columns(&self) -> &[usize] {
        match self {
            TopNHandle::Append(r) => &r.partition_columns,
            TopNHandle::Retract(r) => &r.partition_columns,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn restore_partitions(
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        offset: i64,
        limit: i64,
        output_rank_number: bool,
        retracting: bool,
        net_diff: bool,
        snapshots: &[Vec<u8>],
    ) -> Self {
        // `OwnedRow` values retain the RowConverter that created them.  Restoring every raw key
        // group separately and then extending the maps would therefore mix rows produced by
        // different converters, which Arrow rejects when we later decode the state.  Reassemble
        // the independent IPC payloads first, then run the normal restore once.
        let batches: Vec<RecordBatch> = snapshots
            .iter()
            .flat_map(|bytes| read_ipc_if_present(bytes))
            .collect();
        let snapshot = batches.first().map(|first| {
            let combined = concat_batches(&first.schema(), batches.iter())
                .expect("merge top-n raw partitions");
            write_ipc(&combined)
        });
        if retracting {
            TopNHandle::Retract(RetractableTopNRanker::restore(
                partition_columns,
                sort_columns,
                offset,
                limit,
                output_rank_number,
                snapshot.as_deref().unwrap_or_default(),
            ).with_net_diff(net_diff))
        } else {
            TopNHandle::Append(TopNRanker::restore(
                partition_columns,
                sort_columns,
                limit,
                output_rank_number,
                net_diff,
                snapshot.as_deref().unwrap_or_default(),
            ))
        }
    }
}

fn topn_snapshot_partitions(
    bytes: &[u8],
    partition_columns: &[usize],
    max_parallelism: usize,
    timestamp_precisions: &[i32],
) -> BTreeMap<i32, Vec<u8>> {
    let mut partitions = BTreeMap::new();
    for batch in read_ipc_if_present(bytes) {
        let mut rows_by_group: BTreeMap<i32, Vec<u32>> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            let key_group = flink_key_group(
                binary_row_hash(&batch, partition_columns, row, timestamp_precisions),
                max_parallelism,
            ) as i32;
            rows_by_group.entry(key_group).or_default().push(row as u32);
        }
        for (key_group, rows) in rows_by_group {
            let indices = UInt32Array::from(rows);
            let columns = batch
                .columns()
                .iter()
                .map(|column| take(column, &indices, None).expect("partition top-n snapshot"))
                .collect();
            let partition = RecordBatch::try_new(batch.schema(), columns)
                .expect("partitioned top-n snapshot");
            partitions.insert(key_group, write_ipc(&partition));
        }
    }
    partitions
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
        self.snapshot_parts(self.snapshot_batch())
    }

    fn snapshot_parts(&self, batch: Option<RecordBatch>) -> Vec<u8> {
        let mut out = self.current_watermark.to_le_bytes().to_vec();
        let Some(batch) = batch else { return out };
        out.extend_from_slice(&write_ipc(&batch));
        out
    }

    fn snapshot_batch(&self) -> Option<RecordBatch> {
        let Some(schema) = &self.schema else { return None };
        let rows: Vec<&JoinRow> = self.groups.values().flatten().collect();
        if rows.is_empty() {
            return None;
        }
        let fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| scalars_to_array(rows.iter().map(|r| r[j].clone()).collect(), fields[j].data_type()))
            .collect();
        Some(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("window-rank snapshot"))
    }

    fn snapshot_key_groups(
        &self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> Vec<i32> {
        self.raw_snapshot_partitions(max_parallelism, timestamp_precisions)
            .keys()
            .copied()
            .collect()
    }

    fn snapshot_key_group(
        &self,
        key_group: i32,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> Vec<u8> {
        self.raw_snapshot_partitions(max_parallelism, timestamp_precisions)
            .remove(&key_group)
            .expect("requested non-empty window-rank raw key group")
    }

    fn raw_snapshot_partitions(
        &self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        let mut snapshots = BTreeMap::new();
        let Some(batch) = self.snapshot_batch() else { return snapshots };
        let mut rows_by_group: BTreeMap<i32, Vec<u32>> = BTreeMap::new();
        for row in 0..batch.num_rows() {
            let key_group = flink_key_group(
                binary_row_hash(&batch, &self.partition_columns, row, timestamp_precisions),
                max_parallelism,
            ) as i32;
            rows_by_group.entry(key_group).or_default().push(row as u32);
        }
        for (key_group, rows) in rows_by_group {
            let indices = UInt32Array::from(rows);
            let columns = batch
                .columns()
                .iter()
                .map(|column| take(column, &indices, None).expect("partition window-rank snapshot"))
                .collect();
            let partition = RecordBatch::try_new(batch.schema(), columns)
                .expect("partitioned window-rank snapshot");
            snapshots.insert(key_group, self.snapshot_parts(Some(partition)));
        }
        snapshots
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

    fn restore_partitions(
        window_start_col: usize,
        window_end_col: usize,
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        output_rank_number: bool,
        snapshots: &[Vec<u8>],
    ) -> Self {
        let mut watermark = i64::MIN;
        let mut batches = Vec::new();
        for bytes in snapshots {
            if bytes.len() >= 8 {
                watermark = watermark.max(i64::from_le_bytes(
                    bytes[0..8].try_into().expect("window-rank watermark"),
                ));
                batches.extend(read_ipc_if_present(&bytes[8..]));
            }
        }
        if batches.is_empty() {
            let mut empty = WindowRanker::new(
                window_start_col,
                window_end_col,
                partition_columns,
                sort_columns,
                limit,
                output_rank_number,
            );
            empty.current_watermark = watermark;
            return empty;
        }
        // See TopNHandle::restore_partitions: `GroupKey` owns Arrow row bytes and must be made by
        // one RowConverter.  Concatenating raw key-group payloads before restore preserves that.
        let combined = concat_batches(&batches[0].schema(), batches.iter())
            .expect("merge window-rank raw partitions");
        let mut bytes = watermark.to_le_bytes().to_vec();
        bytes.extend_from_slice(&write_ipc(&combined));
        WindowRanker::restore(
            window_start_col,
            window_end_col,
            partition_columns,
            sort_columns,
            limit,
            output_rank_number,
            &bytes,
        )
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

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_topNRankerStagingBytes<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let ranker = unsafe { &*(handle as *const TopNHandle) };
    match ranker {
        TopNHandle::Append(r) => r.staging_bytes() as jlong,
        TopNHandle::Retract(r) => r.staging_bytes() as jlong,
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_topNRankerStagedPartitions<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let ranker = unsafe { &*(handle as *const TopNHandle) };
    match ranker {
        TopNHandle::Append(r) => r.staged_partitions() as jlong,
        TopNHandle::Retract(r) => r.staged_partitions() as jlong,
    }
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

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_windowRankerSnapshotKeyGroups<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jintArray {
    let ranker = unsafe { &*(handle as *const WindowRanker) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let groups = ranker.snapshot_key_groups(max_parallelism as usize, &precisions);
    let output = env
        .new_int_array(groups.len() as i32)
        .expect("allocate window-rank raw key groups");
    env.set_int_array_region(&output, 0, &groups)
        .expect("write window-rank raw key groups");
    output.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotWindowRankerKeyGroup<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key_group: jint,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jbyteArray {
    let ranker = unsafe { &*(handle as *const WindowRanker) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let snapshot = ranker.snapshot_key_group(key_group, max_parallelism as usize, &precisions);
    env.byte_array_from_slice(&snapshot)
        .expect("allocate window-rank raw key-group snapshot")
        .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreWindowRankerPartitions<
    'local,
>(
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
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
    let count = env
        .get_array_length(&snapshots)
        .expect("read window-rank raw partition count");
    let mut restored = Vec::with_capacity(count as usize);
    for index in 0..count {
        let bytes = JByteArray::from(
            env.get_object_array_element(&snapshots, index)
                .expect("read window-rank raw partition"),
        );
        restored.push(
            env.convert_byte_array(&bytes)
                .expect("read window-rank raw partition bytes"),
        );
    }
    let ranker = WindowRanker::restore_partitions(
        window_start_col as usize,
        window_end_col as usize,
        partitions,
        sort,
        limit,
        output_rank_number != 0,
        &restored,
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
    net_diff: jboolean,
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
        ).with_net_diff(net_diff != 0))
    } else {
        // The append-only ranker is the no-OFFSET path (offset always 0).
        TopNHandle::Append(TopNRanker::new(
            partitions,
            sort,
            limit,
            output_rank_number != 0,
            net_diff != 0,
        ))
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

/// Emits the append-only ranker's net changes at a Flink logical mini-batch boundary.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushTopNRanker<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let ranker = unsafe { &mut *(handle as *mut TopNHandle) };
    export_record_batch(ranker.flush(), out_array_address, out_schema_address);
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
    net_diff: jboolean,
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
        ).with_net_diff(net_diff != 0))
    } else {
        TopNHandle::Append(TopNRanker::restore(
            partitions,
            sort,
            limit,
            output_rank_number != 0,
            net_diff != 0,
            &bytes,
        ))
    };
    boxed_or_throw(&mut env, handle.with_memory_budget(memory_budget_bytes))
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotTopNRankerPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    let ranker = unsafe { &*(handle as *const TopNHandle) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let partitions = ranker.snapshot_partitions(max_parallelism as usize, &precisions);
    let output = env
        .new_object_array(
            partitions.len() as i32,
            "[B",
            jni::objects::JObject::null(),
        )
        .expect("allocate top-n raw keyed-state partitions");
    for (index, (key_group, payload)) in partitions.into_iter().enumerate() {
        let mut framed = Vec::with_capacity(std::mem::size_of::<i32>() + payload.len());
        framed.extend_from_slice(&key_group.to_be_bytes());
        framed.extend_from_slice(&payload);
        let framed = env
            .byte_array_from_slice(&framed)
            .expect("allocate top-n raw keyed-state partition");
        env.set_object_array_element(&output, index as i32, framed)
            .expect("store top-n raw keyed-state partition");
    }
    output.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreTopNRankerPartitions<
    'local,
>(
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
    net_diff: jboolean,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
    let count = env
        .get_array_length(&snapshots)
        .expect("read top-n raw partition count");
    let mut restored = Vec::with_capacity(count as usize);
    for index in 0..count {
        let bytes = JByteArray::from(
            env.get_object_array_element(&snapshots, index)
                .expect("read top-n raw partition"),
        );
        restored.push(
            env.convert_byte_array(&bytes)
                .expect("read top-n raw partition bytes"),
        );
    }
    let ranker = TopNHandle::restore_partitions(
        partitions,
        sort,
        offset,
        limit,
        output_rank_number != 0,
        retracting != 0,
        net_diff != 0,
        &restored,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, ranker)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn changelog(values: &[i64], kinds: &[i8]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
                Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1; values.len()])),
                Arc::new(Int64Array::from(values.to_vec())),
                Arc::new(Int8Array::from(kinds.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn retracting_top_n_emits_one_final_diff_per_logical_bundle() {
        let mut ranker = RetractableTopNRanker::new(
            vec![0],
            vec![SortColumn { index: 1, ascending: true, nulls_first: false }],
            0,
            2,
            false,
        )
        .with_net_diff(true);

        assert_eq!(ranker.push(&changelog(&[10, 20, 30], &[0, 0, 0])).unwrap().num_rows(), 0);
        assert_eq!(ranker.flush_net_diff().num_rows(), 2);

        assert_eq!(ranker.push(&changelog(&[10], &[3])).unwrap().num_rows(), 0);
        assert_eq!(ranker.push(&changelog(&[5], &[0])).unwrap().num_rows(), 0);
        assert_eq!(ranker.staged_partitions(), 1);
        let out = ranker.flush_net_diff();
        let values = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let kinds = out.column(2).as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(values.values(), &[5, 10]);
        assert_eq!(kinds.values(), &[0, 3]);
        assert_eq!(ranker.staged_partitions(), 0);
    }
}
