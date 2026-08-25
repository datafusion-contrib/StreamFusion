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
pub(crate) fn compare_rows(
    a: &[ScalarValue],
    b: &[ScalarValue],
    sort: &[SortColumn],
) -> std::cmp::Ordering {
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
    // Arc-shared: the state codec re-derives sort keys and payload rows on hydration, and
    // arrow-row rejects rows decoded by a different converter INSTANCE — the codec and the
    // operator must literally share these two.
    sort: Arc<RowConverter>,
    payload: Arc<RowConverter>,
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
            (0..arity)
                .map(|i| SortField::new(batch.column(i).data_type().clone()))
                .collect(),
        )
        .expect("top-n payload converter");
        // A plain LIMIT (no ORDER BY) has zero sort columns; like the empty partition key, encode a
        // constant dummy so all rows compare equal and the buffer preserves arrival order (Flink's
        // first-n by arrival). With sort columns present, encode them memcomparable with their options.
        let sort = if sort_columns.is_empty() {
            RowConverter::new(vec![SortField::new(DataType::Boolean)])
                .expect("top-n empty sort converter")
        } else {
            RowConverter::new(
                sort_columns
                    .iter()
                    .map(|s| {
                        SortField::new_with_options(
                            batch.column(s.index).data_type().clone(),
                            SortOptions {
                                descending: !s.ascending,
                                nulls_first: s.nulls_first,
                            },
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
        TopNConverters {
            partition,
            sort: Arc::new(sort),
            payload: Arc::new(payload),
        }
    }

    /// Builds the converter set from the operator's declared input type — the persistent path,
    /// which must share these instances with its state codec before any batch arrives.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn from_declared(
        schema: &SchemaRef,
        partition_columns: &[usize],
        sort_columns: &[SortColumn],
    ) -> Self {
        let empty = RecordBatch::new_empty(schema.clone());
        Self::build(&empty, empty.num_columns(), partition_columns, sort_columns)
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
#[derive(Clone)]
pub(crate) struct TopNRow {
    pub(crate) sort: OwnedRow,
    pub(crate) payload: Arc<OwnedRow>,
    /// Wall-clock millis of the entry's last write (Flink state-TTL); stays 0 while TTL is off.
    /// Append-only granularity is the sort-key list (Flink's `MapState<sortKey, List<row>>`): every
    /// write to a list refreshes all its rows, so byte-equal sort keys always share one timestamp.
    /// The retracting ranker instead expires the WHOLE buffer on the head entry's clock (see the
    /// divergence note on its push).
    pub(crate) ts_ms: i64,
}

fn topn_staged_entry_bytes(key: &ByteKey, old: &Vec<Arc<OwnedRow>>) -> usize {
    // The key is owned once by the lookup map and once by the deterministic first-touch vector.
    byte_key_bytes(&key.0)
        + key.len()
        + std::mem::size_of::<ByteKey>()
        + std::mem::size_of::<Vec<Arc<OwnedRow>>>()
        + old.capacity() * std::mem::size_of::<Arc<OwnedRow>>()
}

pub(crate) struct TopNRanker<S: KeyedStateStore<Vec<TopNRow>> = MemoryTopNStore> {
    partition_columns: Vec<usize>,
    key_timestamp_precisions: Vec<i32>,
    sort_columns: Vec<SortColumn>,
    limit: i64,
    output_rank_number: bool,
    // Mini-batch mode: emit the NET rank diff per logical bundle (old top-N vs new top-N for each
    // touched partition) instead of the host's per-record -U/+U cascade. Gated on the host plan
    // running mini-batch, whose parity contract is the collapsed changelog — which the diff
    // preserves exactly — rather than the per-record byte sequence (see divergences/20).
    net_diff: bool,
    // Idle-state retention millis (0 = off — Flink's default). Expiry is silent: downstream only
    // ever saw +I's from the append-only ranker, and Flink emits nothing when rank state expires.
    ttl_ms: i64,
    // When the last full expiry sweep ran; the sweep reclaims partitions never touched again, at
    // most once per TTL period (expiry itself is enforced lazily at each partition touch).
    last_sweep_ms: i64,
    schema: Option<SchemaRef>,
    converters: Option<TopNConverters>,
    // Keyed by the partition key's Flink BinaryRow bytes — the encoding every keyed store speaks
    // (its hash IS the Flink key group) — probed borrowed, so a row for an existing partition (or
    // one dropped at rank > N) allocates nothing.
    groups: S,
    // Transient mini-batch preimages, captured once per partition in deterministic first-touch
    // order and drained at the next logical boundary.
    staged_order: Vec<ByteKey>,
    staged_old_tops: HashMap<ByteKey, Vec<Arc<OwnedRow>>>,
    pub(crate) memory: OperatorMemory,
}

/// The resident default backend for the Top-N buffer store (see `state/` for the seam).
pub(crate) type MemoryTopNStore = MemoryStateStore<Vec<TopNRow>>;

/// Estimated footprint of one buffered Top-N entry (sort key + payload row + container overhead).
pub(crate) fn topn_entry_bytes(entry: &TopNRow) -> usize {
    entry.sort.row().as_ref().len() + entry.payload.row().as_ref().len() + GROUP_ENTRY_OVERHEAD
}

/// The Top-N buffer's persistent backend: the generic persistent store under the raw whole-list
/// codec — one RocksDB value per partition key holding the ordered buffer.
#[cfg(feature = "rocksdb-state")]
pub(crate) type RocksTopNStore = crate::state::RocksStore<TopNStateCodec>;

/// The Top-N value codec for the persistent store: raw — `[n_rows: u32 LE]`, then per buffered row
/// `[sort_len: u32 LE][sort][payload_len: u32 LE][payload][ts_ms: i64 LE]`, in buffer order. The
/// element order IS the ranker's sort-plus-arrival invariant, so the round trip must not reorder;
/// the sort and payload bytes are the ranker's own arrow-row encodings, parsed back through the
/// SAME converter instances the operator ranks with (arrow-row rejects rows from another
/// instance). The store-level TTL prefix carries the buffer's NEWEST row clock, so compaction only
/// drops a value once every row expired; per-row expiry stays the ranker's lazy prune, exactly as
/// on the memory backend.
#[cfg(feature = "rocksdb-state")]
pub(crate) struct TopNStateCodec {
    sort: Arc<RowConverter>,
    payload: Arc<RowConverter>,
}

#[cfg(feature = "rocksdb-state")]
impl TopNStateCodec {
    pub(crate) fn new(converters: &TopNConverters) -> Self {
        TopNStateCodec {
            sort: Arc::clone(&converters.sort),
            payload: Arc::clone(&converters.payload),
        }
    }
}

#[cfg(feature = "rocksdb-state")]
impl crate::state::RocksStateCodec for TopNStateCodec {
    type Value = Vec<TopNRow>;
    fn supported(&self) -> bool {
        true
    }
    fn value_fields(&self) -> Vec<(String, DataType)> {
        vec![("rows".to_string(), DataType::Binary)]
    }
    fn encode(&self, _value: &Vec<TopNRow>) -> Vec<ScalarValue> {
        unreachable!("raw codec")
    }
    fn decode(&self, _scalars: &[ScalarValue]) -> Vec<TopNRow> {
        unreachable!("raw codec")
    }
    fn value_bytes(&self, value: &Vec<TopNRow>) -> usize {
        4 + value
            .iter()
            .map(|entry| 16 + entry.sort.row().data().len() + entry.payload.row().data().len())
            .sum::<usize>()
    }
    fn write_ms(&self, value: &Vec<TopNRow>) -> i64 {
        value.iter().map(|entry| entry.ts_ms).max().unwrap_or(0)
    }
    fn raw(&self) -> bool {
        true
    }
    fn raw_write(&self, value: &Vec<TopNRow>, out: &mut Vec<u8>) {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        for entry in value {
            write_length_prefixed(out, entry.sort.row().data());
            write_length_prefixed(out, entry.payload.row().data());
            out.extend_from_slice(&entry.ts_ms.to_le_bytes());
        }
    }
    fn from_raw(&self, bytes: &[u8]) -> Vec<TopNRow> {
        let mut cursor = RawListCursor::new(bytes);
        let sort_parser = self.sort.parser();
        let payload_parser = self.payload.parser();
        (0..cursor.u32())
            .map(|_| TopNRow {
                sort: sort_parser.parse(cursor.bytes()).owned(),
                payload: Arc::new(payload_parser.parse(cursor.bytes()).owned()),
                ts_ms: cursor.i64(),
            })
            .collect()
    }
}

#[cfg(feature = "rocksdb-state")]
fn write_length_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// Reads a raw list layout back: length-prefixed byte slices and fixed-width integers, in exactly
/// the order the writer appended them.
#[cfg(feature = "rocksdb-state")]
struct RawListCursor<'a> {
    bytes: &'a [u8],
}

#[cfg(feature = "rocksdb-state")]
impl<'a> RawListCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        RawListCursor { bytes }
    }

    fn u32(&mut self) -> usize {
        let (head, rest) = self.bytes.split_at(4);
        self.bytes = rest;
        u32::from_le_bytes(head.try_into().expect("u32 field")) as usize
    }

    fn i64(&mut self) -> i64 {
        let (head, rest) = self.bytes.split_at(8);
        self.bytes = rest;
        i64::from_le_bytes(head.try_into().expect("i64 field"))
    }

    fn bytes(&mut self) -> &'a [u8] {
        let len = self.u32();
        let (head, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        head
    }
}

/// Refreshes the whole tie group of the row just inserted at `pos`. Flink's append-only Top-N
/// state is `MapState<sortKey, List<row>>` and every insert writes the ENTIRE sort-key list back
/// (`dataState.put(sortKey, inputs)` in `AppendOnlyTopNFunction.processElement`), so all buffered
/// rows with a byte-equal sort key take this write's timestamp together. Ties sit contiguously
/// below the insertion point (`partition_point` places a new row after its equals), so the
/// downward walk covers the list.
fn refresh_sort_key_ties(buffer: &mut [TopNRow], pos: usize, now_ms: i64) {
    buffer[pos].ts_ms = now_ms;
    let mut i = pos;
    while i > 0 && buffer[i - 1].sort == buffer[pos].sort {
        i -= 1;
        buffer[i].ts_ms = now_ms;
    }
}

/// Refreshes the rows still sharing the evicted row's sort key: when eviction trims (rather than
/// empties) the last sort-key list, Flink writes the trimmed list back (`updateState` in
/// `AppendOnlyTopNHelper.processElementWithoutRowNumber`), refreshing it; a sole-member list is
/// removed with no write. Only the without-rank-number algorithm rewrites on eviction — the
/// with-rank path (`processElementWithRowNumber`) only removes sort keys wholly past the rank end.
fn refresh_evicted_ties(buffer: &mut [TopNRow], evicted: &TopNRow, now_ms: i64) {
    let mut i = buffer.len();
    while i > 0 && buffer[i - 1].sort == evicted.sort {
        i -= 1;
        buffer[i].ts_ms = now_ms;
    }
}

/// Drops a buffer's expired rows (Flink's expired map-state entries read as absent), returning the
/// reclaimed bytes when `track`. Silent by contract: expiry emits nothing downstream.
fn prune_expired_topn_rows(buffer: &mut Vec<TopNRow>, ttl: StateTtl, track: bool) -> isize {
    let mut reclaimed = 0isize;
    buffer.retain(|entry| {
        if ttl.expired(entry.ts_ms) {
            if track {
                reclaimed += topn_entry_bytes(entry) as isize;
            }
            false
        } else {
            true
        }
    });
    reclaimed
}

impl TopNRanker {
    pub(crate) fn new(
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        output_rank_number: bool,
        net_diff: bool,
    ) -> Self {
        let key_arity = partition_columns.len();
        TopNRanker {
            partition_columns,
            key_timestamp_precisions: vec![-1; key_arity],
            sort_columns,
            limit,
            output_rank_number,
            net_diff,
            ttl_ms: 0,
            last_sweep_ms: 0,
            schema: None,
            converters: None,
            groups: MemoryTopNStore::default(),
            staged_order: Vec::new(),
            staged_old_tops: HashMap::default(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds the per-partition buffers by the operator's task off-heap budget (negative =
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
}

impl<S: KeyedStateStore<Vec<TopNRow>>> TopNRanker<S> {
    /// Moves this freshly built (empty, memory-backed) ranker's configuration onto another state
    /// backend; construction goes through `new` + builders first so backend choice stays
    /// orthogonal to the shape builders.
    pub(crate) fn with_backend<T: KeyedStateStore<Vec<TopNRow>>>(self, groups: T) -> TopNRanker<T> {
        TopNRanker {
            partition_columns: self.partition_columns,
            key_timestamp_precisions: self.key_timestamp_precisions,
            sort_columns: self.sort_columns,
            limit: self.limit,
            output_rank_number: self.output_rank_number,
            net_diff: self.net_diff,
            ttl_ms: self.ttl_ms,
            last_sweep_ms: self.last_sweep_ms,
            schema: self.schema,
            converters: self.converters,
            groups,
            staged_order: self.staged_order,
            staged_old_tops: self.staged_old_tops,
            memory: self.memory,
        }
    }

    /// Attaches the task off-heap budget for a backend that starts with nothing resident (a
    /// read-through store hydrates on demand; there is no restored map to pre-account).
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory.attach("top-n", budget_bytes, 0)?;
        Ok(self)
    }

    /// The backing store, for backend-specific control paths (checkpointing a persistent store).
    pub(crate) fn store_mut(&mut self) -> &mut S {
        &mut self.groups
    }

    pub(crate) fn with_key_timestamp_precisions(
        mut self,
        key_timestamp_precisions: Vec<i32>,
    ) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        self
    }

    /// Pre-installs a converter set built from declared types (the persistent path, which must share
    /// the codec's converters); the lazy first-batch build then never runs.
    pub(crate) fn with_converters(mut self, converters: TopNConverters) -> Self {
        self.converters = Some(converters);
        self
    }

    /// Pre-installs the payload schema alongside `with_converters` on the persistent path, so
    /// canonical snapshots work before the first input batch arrives.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_payload_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Builds the three arrow-row converters from a batch's column types, once.
    fn ensure_converters(&mut self, batch: &RecordBatch, arity: usize) {
        if self.converters.is_none() {
            self.converters = Some(TopNConverters::build(
                batch,
                arity,
                &self.partition_columns,
                &self.sort_columns,
            ));
        }
    }

    /// Sets the idle-state retention (`table.exec.state.ttl`) in millis; 0 (Flink's default)
    /// disables expiry.
    pub(crate) fn with_state_ttl(mut self, ttl_ms: i64) -> Self {
        self.ttl_ms = ttl_ms.max(0);
        self
    }

    /// Reclaims every buffered row whose TTL elapsed with no further touch of its partition — the
    /// lazy first-touch prune never sees such a partition again. Silent, like Flink's background
    /// cleanup.
    fn sweep_expired(&mut self, ttl: StateTtl) {
        let track = self.memory.tracking();
        let mut reclaimed = 0isize;
        self.groups.retain_live(&mut |key, buffer| {
            reclaimed += prune_expired_topn_rows(buffer, ttl, track);
            if buffer.is_empty() {
                if track {
                    reclaimed += (key.len() + GROUP_ENTRY_OVERHEAD) as isize;
                }
                false
            } else {
                true
            }
        });
        if reclaimed != 0 {
            self.memory.record(-reclaimed);
        }
    }

    /// `now_ms` is the host's wall-clock reading for this call (only read when state TTL is on).
    pub(crate) fn push(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        if self.net_diff {
            return self.push_net_diff(batch, now_ms);
        }
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        self.ensure_converters(batch, arity);
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        if ttl.enabled() && now_ms >= self.last_sweep_ms + self.ttl_ms {
            self.sweep_expired(ttl);
            self.last_sweep_ms = now_ms;
        }
        self.groups.begin_batch(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        )?;
        let conv = self.converters.as_ref().expect("converters set");
        // Encode the sort key and full-row payload columnar->row in two vectorized passes; the
        // partition key encodes per row into the BinaryRow encoder's reused buffer.
        let mut parts = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let sort_arrays: Vec<ArrayRef> = self
            .sort_columns
            .iter()
            .map(|s| batch.column(s.index).clone())
            .collect();
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let keys = encode_group_keys(&conv.sort, &sort_arrays, batch.num_rows());
        let payloads = conv
            .payload
            .convert_columns(&data_arrays)
            .expect("encode payload");

        let limit = self.limit as usize;
        let output_rank = self.output_rank_number;
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let groups = &mut self.groups;
        // Partitions already pruned by this call: expiry is enforced once per partition per push
        // (before any preimage or rank read), not re-walked for every row.
        let mut pruned: HashSet<ByteKey> = HashSet::default();
        let mut out_rows: Vec<Arc<OwnedRow>> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        let mut out_ranks: Vec<i64> = Vec::new();

        for row in 0..batch.num_rows() {
            // Compare the memcomparable sort key by borrow — no per-row `owned()` alloc until the row
            // is known to enter (the common case for a bounded Top-N is a row that does not).
            let key_row = keys.row(row);
            // Borrowed partition-key probe; the key bytes are copied only when a partition first
            // appears (buffers never empty out).
            let part = parts.encode(row);
            let buffer = match groups.get_mut(part) {
                Some(buffer) => buffer,
                None => {
                    if track {
                        delta += (part.len() + GROUP_ENTRY_OVERHEAD) as isize;
                    }
                    groups.insert(ByteKey::from(part), Vec::new())
                }
            };
            // Expired rows vanish silently — no -D (downstream only ever saw +I's, and Flink emits
            // nothing when rank state expires); the rest of the row ranks against the survivors.
            if ttl.enabled() && !pruned.contains(part) {
                delta -= prune_expired_topn_rows(buffer, ttl, track);
                pruned.insert(ByteKey::from(part));
            }
            // Insert after any rows that order equal-or-before, preserving arrival order for ties
            // (byte compare of the memcomparable sort key).
            let pos = buffer.partition_point(|e| e.sort.row() <= key_row);

            if output_rank {
                if pos >= limit {
                    continue; // beyond rank N — the new row never enters the top-N (nothing allocated)
                }
                let old_len = buffer.len();
                buffer.insert(
                    pos,
                    TopNRow {
                        sort: key_row.owned(),
                        payload: Arc::new(payloads.row(row).owned()),
                        ts_ms: 0,
                    },
                );
                if track {
                    delta += topn_entry_bytes(&buffer[pos]) as isize;
                }
                if ttl.enabled() {
                    refresh_sort_key_ties(buffer, pos, ttl.now());
                }
                // Cascade from the new row's rank to the buffer end (capped at the limit): each rank's
                // occupant changes, so retract the old and append the new; a brand-new rank inserts.
                let upper = (old_len + 1).min(limit); // highest 1-based rank to emit
                for rank in (pos + 1)..=upper {
                    let new_occupant = buffer[rank - 1].payload.clone();
                    if rank <= old_len {
                        out_rows.push(buffer[rank].payload.clone()); // old occupant (shifted down by one)
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
                        delta -=
                            buffer[limit..].iter().map(topn_entry_bytes).sum::<usize>() as isize;
                    }
                    buffer.truncate(limit); // the row past N was retracted by the -U at rank=limit
                }
            } else {
                let payload = Arc::new(payloads.row(row).owned());
                buffer.insert(
                    pos,
                    TopNRow {
                        sort: key_row.owned(),
                        payload: Arc::clone(&payload),
                        ts_ms: 0,
                    },
                );
                if track {
                    delta += topn_entry_bytes(&buffer[pos]) as isize;
                }
                if ttl.enabled() {
                    refresh_sort_key_ties(buffer, pos, ttl.now());
                }
                if buffer.len() > limit {
                    let evicted = buffer.pop().expect("buffer over limit is non-empty");
                    if track {
                        delta -= topn_entry_bytes(&evicted) as isize;
                    }
                    if ttl.enabled() {
                        refresh_evicted_ties(buffer, &evicted, ttl.now());
                    }
                    if *evicted.payload == *payload {
                        continue; // the new row was itself rank N+1 — it never entered the top-N
                    }
                    out_rows.push(evicted.payload);
                    out_kinds.push(3); // -D the displaced row
                }
                out_rows.push(payload);
                out_kinds.push(0); // +I the new row
            }
        }
        self.groups.end_bundle()?;
        self.memory.record(delta + self.groups.footprint_delta());
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
    fn push_net_diff(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        self.ensure_converters(batch, arity);
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        // The sweep must not run mid-bundle: removing a staged partition's rows would surface at
        // the flush as a spurious diff instead of silent expiry.
        if ttl.enabled()
            && self.staged_old_tops.is_empty()
            && now_ms >= self.last_sweep_ms + self.ttl_ms
        {
            self.sweep_expired(ttl);
            self.last_sweep_ms = now_ms;
        }
        // The mini-batch bundle spans pushes: hydrated partitions stay resident until the flush
        // ends the bundle, so the staged preimages' re-probes there stay truthful.
        self.groups.begin_batch(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        )?;
        let conv = self.converters.as_ref().expect("converters set");
        let mut parts = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let sort_arrays: Vec<ArrayRef> = self
            .sort_columns
            .iter()
            .map(|s| batch.column(s.index).clone())
            .collect();
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let keys = encode_group_keys(&conv.sort, &sort_arrays, batch.num_rows());
        let payloads = conv
            .payload
            .convert_columns(&data_arrays)
            .expect("encode payload");

        let limit = self.limit as usize;
        let output_rank = self.output_rank_number;
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let groups = &mut self.groups;

        let staged_order = &mut self.staged_order;
        let staged_old_tops = &mut self.staged_old_tops;
        for row in 0..batch.num_rows() {
            let key_row = keys.row(row);
            let part = parts.encode(row);
            let buffer = match groups.get_mut(part) {
                Some(buffer) => buffer,
                None => {
                    if track {
                        delta += (part.len() + GROUP_ENTRY_OVERHEAD) as isize;
                    }
                    groups.insert(ByteKey::from(part), Vec::new())
                }
            };
            if !staged_old_tops.contains_key(part) {
                // Expiry is enforced only on the bundle's first touch, BEFORE the preimage
                // capture: pruning silently here keeps the expired rows out of the flush diff,
                // while pruning mid-bundle would surface them as spurious deletes.
                if ttl.enabled() {
                    delta -= prune_expired_topn_rows(buffer, ttl, track);
                }
                let key = ByteKey::from(part);
                let old: Vec<Arc<OwnedRow>> = buffer.iter().map(|e| e.payload.clone()).collect();
                if track {
                    delta += topn_staged_entry_bytes(&key, &old) as isize;
                }
                staged_order.push(key.clone());
                staged_old_tops.insert(key, old);
            }
            let pos = buffer.partition_point(|e| e.sort.row() <= key_row);
            if pos >= limit {
                continue; // beyond rank N — never enters (a buffer never exceeds the limit)
            }
            buffer.insert(
                pos,
                TopNRow {
                    sort: key_row.owned(),
                    payload: Arc::new(payloads.row(row).owned()),
                    ts_ms: 0,
                },
            );
            if track {
                delta += topn_entry_bytes(&buffer[pos]) as isize;
            }
            if ttl.enabled() {
                refresh_sort_key_ties(buffer, pos, ttl.now());
            }
            if buffer.len() > limit {
                let evicted = buffer.pop().expect("buffer over limit is non-empty");
                if track {
                    delta -= topn_entry_bytes(&evicted) as isize;
                }
                if ttl.enabled() && !output_rank {
                    refresh_evicted_ties(buffer, &evicted, ttl.now());
                }
            }
        }
        self.memory.record(delta + self.groups.footprint_delta());
        self.memory.account()?;

        Ok(self.emit(Vec::new(), Vec::new(), Vec::new()))
    }

    /// Emits one final diff per partition touched since the preceding logical boundary.
    pub(crate) fn flush_net_diff(&mut self) -> RecordBatch {
        if !self.net_diff {
            return self.emit(Vec::new(), Vec::new(), Vec::new());
        }
        let staged_bytes = if self.memory.tracking() {
            self.staging_bytes()
        } else {
            0
        };
        let touched = std::mem::take(&mut self.staged_order);
        let old_tops = std::mem::take(&mut self.staged_old_tops);
        let mut out_rows: Vec<Arc<OwnedRow>> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        let mut out_ranks: Vec<i64> = Vec::new();
        for part in touched {
            let old = &old_tops[&part];
            let new: Vec<Arc<OwnedRow>> = self
                .groups
                .get(&part.0)
                .expect("staged partition resident")
                .iter()
                .map(|e| Arc::clone(&e.payload))
                .collect();
            if self.output_rank_number {
                diff_top(
                    true,
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
        self.groups.end_bundle().expect("end top-n bundle");
        self.memory.record(self.groups.footprint_delta());
        self.memory.forget(staged_bytes);
        self.memory.account_shrink();
        self.emit(out_rows, out_kinds, out_ranks)
    }

    fn emit(
        &self,
        out_rows: Vec<Arc<OwnedRow>>,
        out_kinds: Vec<i8>,
        out_ranks: Vec<i64>,
    ) -> RecordBatch {
        emit_changelog(
            self.schema.as_ref(),
            self.converters.as_ref(),
            self.output_rank_number,
            out_rows,
            out_kinds,
            out_ranks,
        )
    }
}

/// The Top-N raw snapshot adds the memcomparable sort key between the shared key/row columns;
/// the schema's metadata carries the typed payload schema so converters can be rebuilt before
/// any input arrives.
const RAW_SNAPSHOT_SORT: &str = "__sort__";

/// One side's buffers as raw state bytes, one IPC blob per key group, buffer order preserved.
/// Snapshotting decodes nothing: the group is one hash of the stored partition key's bytes per
/// bucket (that encoding's hash IS Flink's key-group input). The TTL timestamps ride a trailing
/// column only while TTL is on, so a TTL-off snapshot stays byte-identical to the pre-TTL format;
/// buffer order is preserved, so the retracting ranker's head clock round-trips on the head row.
fn raw_topn_snapshot_groups(
    groups: &MemoryTopNStore,
    schema: Option<&SchemaRef>,
    max_parallelism: usize,
    ttl_on: bool,
) -> BTreeMap<i32, Vec<u8>> {
    let Some(schema) = schema else {
        return BTreeMap::new();
    };
    let mut partitions: BTreeMap<i32, Vec<(&ByteKey, &Vec<TopNRow>)>> = BTreeMap::new();
    for (key, buffer) in groups.iter() {
        if buffer.is_empty() {
            continue;
        }
        let group = flink_key_group(hash_bytes_by_words(&key.0), max_parallelism) as i32;
        partitions.entry(group).or_default().push((key, buffer));
    }
    partitions
        .into_iter()
        .map(|(group, entries)| {
            (
                group,
                write_raw_topn_snapshot_partition(entries.into_iter(), schema, ttl_on),
            )
        })
        .collect()
}

fn write_raw_topn_snapshot_partition<'a>(
    entries: impl Iterator<Item = (&'a ByteKey, &'a Vec<TopNRow>)>,
    schema: &SchemaRef,
    ttl_on: bool,
) -> Vec<u8> {
    let mut keys = BinaryBuilder::new();
    let mut sorts = BinaryBuilder::new();
    let mut rows = BinaryBuilder::new();
    let mut write_timestamps = Int64Builder::new();
    for (key, buffer) in entries {
        for entry in buffer {
            keys.append_value(&key.0);
            sorts.append_value(entry.sort.row().data());
            rows.append_value(entry.payload.row().data());
            write_timestamps.append_value(entry.ts_ms);
        }
    }
    let mut fields = vec![
        Field::new(RAW_SNAPSHOT_KEY, DataType::Binary, false),
        Field::new(RAW_SNAPSHOT_SORT, DataType::Binary, false),
        Field::new(RAW_SNAPSHOT_ROW, DataType::Binary, false),
    ];
    if ttl_on {
        fields.push(Field::new(TTL_TS_COLUMN, DataType::Int64, false));
    }
    let raw_schema = Arc::new(Schema::new_with_metadata(
        fields,
        std::collections::HashMap::from([(
            RAW_SNAPSHOT_PAYLOAD_SCHEMA.to_string(),
            encode_schema_metadata(schema),
        )]),
    ));
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(keys.finish()),
        Arc::new(sorts.finish()),
        Arc::new(rows.finish()),
    ];
    if ttl_on {
        columns.push(Arc::new(write_timestamps.finish()));
    }
    let batch = RecordBatch::try_new(raw_schema, columns).expect("raw top-n snapshot batch");
    write_ipc(&batch)
}

/// Commits a persistent Top-N store and exports every non-empty buffer in the raw key-group
/// encoding the memory snapshots write, for backend-independent canonical savepoints. Empty
/// buffers are skipped exactly as the memory snapshots skip them.
#[cfg(feature = "rocksdb-state")]
fn rocks_canonical_partitions<C, R>(
    groups: &mut crate::state::RocksStore<C>,
    write_partition: impl Fn(&[(&ByteKey, &Vec<R>)]) -> Vec<u8>,
) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError>
where
    C: crate::state::RocksStateCodec<Value = Vec<R>>,
{
    let keys = groups.canonical_keys_by_group()?;
    let mut partitions = BTreeMap::new();
    for (&group, selected) in &keys {
        let entries: Vec<(&ByteKey, &Vec<R>)> = selected
            .iter()
            .map(|key| {
                (
                    key,
                    groups.get(&key.0).expect("canonical key remains resident"),
                )
            })
            .filter(|(_, buffer)| !buffer.is_empty())
            .collect();
        if !entries.is_empty() {
            partitions.insert(group, write_partition(&entries));
        }
    }
    groups.finish_canonical_scan();
    Ok(partitions)
}

/// Commits the persistent store and exports the complete logical table in the same raw key-group
/// encoding the memory snapshot writes, for backend-independent canonical savepoints.
#[cfg(feature = "rocksdb-state")]
impl TopNRanker<RocksTopNStore> {
    pub(crate) fn canonical_partitions(
        &mut self,
    ) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError> {
        let schema = self
            .schema
            .clone()
            .expect("declared schema installed on the persistent path");
        let ttl_on = self.ttl_ms > 0;
        rocks_canonical_partitions(&mut self.groups, |entries| {
            write_raw_topn_snapshot_partition(
                entries.iter().map(|&(key, buffer)| (key, buffer)),
                &schema,
                ttl_on,
            )
        })
    }
}

/// See [`TopNRanker::canonical_partitions`]; the retracting buffers share the raw encoding.
#[cfg(feature = "rocksdb-state")]
impl RetractableTopNRanker<RocksTopNStore> {
    pub(crate) fn canonical_partitions(
        &mut self,
    ) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError> {
        let schema = self
            .schema
            .clone()
            .expect("declared schema installed on the persistent path");
        let ttl_on = self.ttl_ms > 0;
        rocks_canonical_partitions(&mut self.groups, |entries| {
            write_raw_topn_snapshot_partition(
                entries.iter().map(|&(key, buffer)| (key, buffer)),
                &schema,
                ttl_on,
            )
        })
    }
}

/// Immutable append-only Top-N view captured at an aligned checkpoint barrier. Payload rows are
/// shared through their existing Arcs; only the small partition and sort keys are copied. IPC is
/// deliberately deferred until Flink's heap-state serializer runs on the async checkpoint thread.
struct AppendTopNSnapshot {
    schema: SchemaRef,
    ttl_on: bool,
    partitions: BTreeMap<i32, Vec<(ByteKey, Vec<TopNRow>)>>,
}

impl AppendTopNSnapshot {
    fn capture(ranker: &TopNRanker, max_parallelism: usize) -> Option<Self> {
        let schema = ranker.schema.clone()?;
        let mut partitions: BTreeMap<i32, Vec<(ByteKey, Vec<TopNRow>)>> = BTreeMap::new();
        for (key, buffer) in ranker.groups.iter() {
            if buffer.is_empty() {
                continue;
            }
            let group = flink_key_group(hash_bytes_by_words(&key.0), max_parallelism) as i32;
            partitions
                .entry(group)
                .or_default()
                .push((key.clone(), buffer.clone()));
        }
        Some(Self {
            schema,
            ttl_on: ranker.ttl_ms > 0,
            partitions,
        })
    }

    fn encode(&self, key_group: i32) -> Option<Vec<u8>> {
        let entries = self.partitions.get(&key_group)?;
        Some(write_raw_topn_snapshot_partition(
            entries.iter().map(|(key, rows)| (key, rows)),
            &self.schema,
            self.ttl_on,
        ))
    }
}

/// The raw keyed-state snapshot/restore surface exists only on the memory backend — a persistent
/// store checkpoints through its own commit path instead of materializing the key space.
impl TopNRanker {
    /// Serializes the buffered rows in per-partition buffer order (partition derivable from the row).
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Vec<u8> {
        raw_topn_snapshot_groups(&self.groups, self.schema.as_ref(), 1, self.ttl_ms > 0)
            .remove(&0)
            .unwrap_or_default()
    }

    pub(crate) fn snapshot_partitions(&self, max_parallelism: usize) -> BTreeMap<i32, Vec<u8>> {
        raw_topn_snapshot_groups(
            &self.groups,
            self.schema.as_ref(),
            max_parallelism,
            self.ttl_ms > 0,
        )
    }

    /// `restored_at_ms` stamps rows from a snapshot carrying no TTL timestamps (a pre-TTL or
    /// TTL-off writer) — a full retention from the restore, Flink's enable-TTL migration.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore(
        partition_columns: Vec<usize>,
        key_timestamp_precisions: Vec<i32>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        output_rank_number: bool,
        net_diff: bool,
        bytes: &[u8],
        restored_at_ms: i64,
    ) -> Self {
        Self::restore_partitions(
            partition_columns,
            key_timestamp_precisions,
            sort_columns,
            limit,
            output_rank_number,
            net_diff,
            &[bytes.to_vec()],
            restored_at_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore_partitions(
        partition_columns: Vec<usize>,
        key_timestamp_precisions: Vec<i32>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        output_rank_number: bool,
        net_diff: bool,
        snapshots: &[Vec<u8>],
        restored_at_ms: i64,
    ) -> Self {
        let mut ranker = TopNRanker::new(
            partition_columns,
            sort_columns,
            limit,
            output_rank_number,
            net_diff,
        )
        .with_key_timestamp_precisions(key_timestamp_precisions);
        for bytes in snapshots {
            for batch in read_ipc_if_present(bytes) {
                if batch.schema_ref().field(0).name() == RAW_SNAPSHOT_KEY {
                    load_topn_batch_raw(
                        &mut ranker.schema,
                        &mut ranker.converters,
                        &mut ranker.groups,
                        &ranker.partition_columns,
                        &ranker.sort_columns,
                        &batch,
                        restored_at_ms,
                    );
                } else {
                    ranker.load_batch_decoded(&batch, restored_at_ms);
                }
            }
        }
        ranker
    }

    /// Snapshots written before the raw format decoded the buffers to typed columns; kept so
    /// existing savepoints keep restoring. The format predates TTL, so every row is stamped with
    /// the restore time (the enable-TTL migration).
    fn load_batch_decoded(&mut self, batch: &RecordBatch, restored_at_ms: i64) {
        let arity = batch.num_columns();
        self.schema = Some(batch.schema());
        self.ensure_converters(batch, arity);
        let conv = self.converters.as_ref().expect("converters set");
        let mut parts = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let sort_arrays: Vec<ArrayRef> = self
            .sort_columns
            .iter()
            .map(|s| batch.column(s.index).clone())
            .collect();
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let keys = encode_group_keys(&conv.sort, &sort_arrays, batch.num_rows());
        let payloads = conv
            .payload
            .convert_columns(&data_arrays)
            .expect("encode payload");
        let groups = &mut self.groups;
        for row in 0..batch.num_rows() {
            let part = parts.encode(row);
            let buffer = match groups.get_mut(part) {
                Some(buffer) => buffer,
                None => groups.insert(ByteKey::from(part), Vec::new()),
            };
            buffer.push(TopNRow {
                sort: keys.row(row).owned(),
                payload: Arc::new(payloads.row(row).owned()),
                ts_ms: restored_at_ms,
            });
        }
    }
}

/// Raw-format rows carry the stored partition key, sort key, and payload bytes verbatim —
/// restoring wraps the bytes back into rows with the ranker's own converters (no decode, no
/// re-encode, and no cross-converter mixing since every blob parses through the same instances).
/// The TTL timestamps are read by name when the writer had TTL on; a snapshot without them stamps
/// every row with `restored_at_ms` (Flink's enable-TTL migration).
fn load_topn_batch_raw(
    schema: &mut Option<SchemaRef>,
    converters: &mut Option<TopNConverters>,
    groups: &mut MemoryTopNStore,
    partition_columns: &[usize],
    sort_columns: &[SortColumn],
    batch: &RecordBatch,
    restored_at_ms: i64,
) {
    if schema.is_none() {
        let payload_schema =
            decode_schema_metadata(batch).expect("raw top-n snapshot payload schema");
        let empty = RecordBatch::new_empty(payload_schema.clone());
        *converters = Some(TopNConverters::build(
            &empty,
            empty.num_columns(),
            partition_columns,
            sort_columns,
        ));
        *schema = Some(payload_schema);
    }
    let conv = converters.as_ref().expect("converters set");
    let sort_parser = conv.sort.parser();
    let payload_parser = conv.payload.parser();
    let keys = column_binary(batch, RAW_SNAPSHOT_KEY);
    let sorts = column_binary(batch, RAW_SNAPSHOT_SORT);
    let rows = column_binary(batch, RAW_SNAPSHOT_ROW);
    let write_timestamps = batch
        .column_by_name(TTL_TS_COLUMN)
        .is_some()
        .then(|| column_i64(batch, TTL_TS_COLUMN));
    for row in 0..batch.num_rows() {
        let part = keys.value(row);
        let buffer = match groups.get_mut(part) {
            Some(buffer) => buffer,
            None => groups.insert(ByteKey::from(part), Vec::new()),
        };
        buffer.push(TopNRow {
            sort: sort_parser.parse(sorts.value(row)).owned(),
            payload: Arc::new(payload_parser.parse(rows.value(row)).owned()),
            ts_ms: write_timestamps
                .as_ref()
                .map_or(restored_at_ms, |ts| ts.value(row)),
        });
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
pub(crate) struct RetractableTopNRanker<S: KeyedStateStore<Vec<TopNRow>> = MemoryTopNStore> {
    partition_columns: Vec<usize>,
    key_timestamp_precisions: Vec<i32>,
    sort_columns: Vec<SortColumn>,
    /// Rank window: output ranks `[offset+1, limit]` (1-based), i.e. buffer indices `[offset, limit)`.
    /// `offset = rankStart - 1` (0 for the common no-`OFFSET` case); `limit = rankEnd`.
    offset: i64,
    limit: i64,
    output_rank_number: bool,
    net_diff: bool,
    // Idle-state retention millis (0 = off). Expiry granularity is the WHOLE buffer, clocked on
    // the head entry's `ts_ms` — see the invariant documented on `push`.
    ttl_ms: i64,
    last_sweep_ms: i64,
    schema: Option<SchemaRef>,
    converters: Option<TopNConverters>,
    // The append-only ranker's byte-row state (see TopNRow): partition probes by borrowed bytes,
    // rows as (memcomparable sort key, Arc-shared payload) — no per-cell `ScalarValue`, and the
    // before/after top-N snapshots the diff reads are refcount bumps, not row deep-clones.
    groups: S,
    staged_order: Vec<ByteKey>,
    staged_old_tops: HashMap<ByteKey, Vec<Arc<OwnedRow>>>,
    pub(crate) memory: OperatorMemory,
}

impl RetractableTopNRanker {
    pub(crate) fn new(
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        offset: i64,
        limit: i64,
        output_rank_number: bool,
    ) -> Self {
        let key_arity = partition_columns.len();
        RetractableTopNRanker {
            partition_columns,
            key_timestamp_precisions: vec![-1; key_arity],
            sort_columns,
            offset,
            limit,
            output_rank_number,
            net_diff: false,
            ttl_ms: 0,
            last_sweep_ms: 0,
            schema: None,
            converters: None,
            groups: MemoryTopNStore::default(),
            staged_order: Vec::new(),
            staged_old_tops: HashMap::default(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds the full per-partition buffers by the operator's task off-heap budget (negative =
    /// unaccounted), accounting any restored buffers immediately.
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .groups
            .iter()
            .map(|(key, buffer)| {
                byte_key_bytes(&key.0) + buffer.iter().map(topn_entry_bytes).sum::<usize>()
            })
            .sum();
        self.memory
            .attach("retracting-top-n", budget_bytes, state)?;
        Ok(self)
    }
}

impl<S: KeyedStateStore<Vec<TopNRow>>> RetractableTopNRanker<S> {
    pub(crate) fn with_net_diff(mut self, net_diff: bool) -> Self {
        self.net_diff = net_diff;
        self
    }

    /// Moves this freshly built (empty, memory-backed) ranker's configuration onto another state
    /// backend (see the append-only ranker's `with_backend`).
    pub(crate) fn with_backend<T: KeyedStateStore<Vec<TopNRow>>>(
        self,
        groups: T,
    ) -> RetractableTopNRanker<T> {
        RetractableTopNRanker {
            partition_columns: self.partition_columns,
            key_timestamp_precisions: self.key_timestamp_precisions,
            sort_columns: self.sort_columns,
            offset: self.offset,
            limit: self.limit,
            output_rank_number: self.output_rank_number,
            net_diff: self.net_diff,
            ttl_ms: self.ttl_ms,
            last_sweep_ms: self.last_sweep_ms,
            schema: self.schema,
            converters: self.converters,
            groups,
            staged_order: self.staged_order,
            staged_old_tops: self.staged_old_tops,
            memory: self.memory,
        }
    }

    /// Attaches the task off-heap budget for a backend that starts with nothing resident.
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory.attach("retracting-top-n", budget_bytes, 0)?;
        Ok(self)
    }

    /// The backing store, for backend-specific control paths (checkpointing a persistent store).
    pub(crate) fn store_mut(&mut self) -> &mut S {
        &mut self.groups
    }

    pub(crate) fn with_key_timestamp_precisions(
        mut self,
        key_timestamp_precisions: Vec<i32>,
    ) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        self
    }

    /// Pre-installs a converter set built from declared types (the persistent path, which must share
    /// the codec's converters); the lazy first-batch build then never runs.
    pub(crate) fn with_converters(mut self, converters: TopNConverters) -> Self {
        self.converters = Some(converters);
        self
    }

    /// Pre-installs the payload schema alongside `with_converters` on the persistent path, so
    /// canonical snapshots work before the first input batch arrives.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_payload_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Sets the idle-state retention (`table.exec.state.ttl`) in millis; 0 (Flink's default)
    /// disables expiry.
    pub(crate) fn with_state_ttl(mut self, ttl_ms: i64) -> Self {
        self.ttl_ms = ttl_ms.max(0);
        self
    }

    /// Reclaims every buffer whose head clock expired with no further touch of its partition.
    /// Silent, like Flink's background cleanup.
    fn sweep_expired(&mut self, ttl: StateTtl) {
        let track = self.memory.tracking();
        let mut reclaimed = 0isize;
        self.groups.retain_live(&mut |key, buffer| {
            if buffer.first().is_some_and(|head| ttl.expired(head.ts_ms)) {
                if track {
                    reclaimed += (key.len() + GROUP_ENTRY_OVERHEAD) as isize;
                    reclaimed += buffer.iter().map(topn_entry_bytes).sum::<usize>() as isize;
                }
                false
            } else {
                true
            }
        });
        if reclaimed != 0 {
            self.memory.record(-reclaimed);
        }
    }

    /// Clears the buffer if its head clock expired, before anything reads it. Silent: a stale
    /// retraction then finds nothing and emits nothing (Flink's lenient warn-and-skip), and the
    /// next accumulate seeds a fresh buffer through the normal diff.
    fn expire_whole_buffer(buffer: &mut Vec<TopNRow>, ttl: StateTtl, track: bool) -> isize {
        if !buffer.first().is_some_and(|head| ttl.expired(head.ts_ms)) {
            return 0;
        }
        let reclaimed = if track {
            buffer.iter().map(topn_entry_bytes).sum::<usize>() as isize
        } else {
            0
        };
        buffer.clear();
        reclaimed
    }

    /// `now_ms` is the host's wall-clock reading for this call (only read when state TTL is on).
    ///
    /// TTL model — a DELIBERATE divergence from Flink's mixed granularity: Flink keeps a
    /// `ValueState<SortedMap>` treemap it rewrites on EVERY record for the key plus a per-sort-key
    /// `MapState`, so in practice the treemap's TTL governs the partition. We model exactly that
    /// whole-buffer clock, carried on the head entry: after ANY mutation of a non-empty buffer
    /// (accumulate or retract, anywhere in it), `buffer[0].ts_ms = now`; expiry clears the whole
    /// buffer at once.
    pub(crate) fn push(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        if self.net_diff {
            return self.push_net_diff(batch, now_ms);
        }
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        if self.converters.is_none() {
            self.converters = Some(TopNConverters::build(
                batch,
                arity,
                &self.partition_columns,
                &self.sort_columns,
            ));
        }
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        if ttl.enabled() && now_ms >= self.last_sweep_ms + self.ttl_ms {
            self.sweep_expired(ttl);
            self.last_sweep_ms = now_ms;
        }
        self.groups.begin_batch(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        )?;
        let conv = self.converters.as_ref().expect("converters set");
        // Encode the sort key and full-row payload columnar->row in two vectorized passes; the
        // partition key encodes per row into the BinaryRow encoder's reused buffer.
        let mut parts = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let sort_arrays: Vec<ArrayRef> = self
            .sort_columns
            .iter()
            .map(|s| batch.column(s.index).clone())
            .collect();
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let keys = encode_group_keys(&conv.sort, &sort_arrays, batch.num_rows());
        let payloads = conv
            .payload
            .convert_columns(&data_arrays)
            .expect("encode payload");

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
            let part = parts.encode(row);
            let buffer = match groups.get_mut(part) {
                Some(buffer) => buffer,
                None => {
                    if track {
                        delta += (part.len() + GROUP_ENTRY_OVERHEAD) as isize;
                    }
                    groups.insert(ByteKey::from(part), Vec::new())
                }
            };
            // Whole-buffer expiry precedes the preimage capture, so the diff never surfaces the
            // expired rows. Re-checking per row is a head compare and stays a no-op once any
            // mutation refreshed the head to this call's clock.
            if ttl.enabled() {
                delta -= Self::expire_whole_buffer(buffer, ttl, track);
            }
            // The top-N window before the mutation: Arc bumps of the payloads, not row clones.
            let old_top: Vec<Arc<OwnedRow>> = buffer
                [offset.min(buffer.len())..limit.min(buffer.len())]
                .iter()
                .map(|e| Arc::clone(&e.payload))
                .collect();
            // +I(0)/+U(2) accumulate; -U(1)/-D(3) retract.
            let retract = matches!(row_kinds.map(|k| k.value(row)).unwrap_or(0), 1 | 3);
            if retract {
                // Remove the first full-row-equal match — a byte compare of the value-encoded
                // payload (the append-only ranker's equality trade).
                let full = payloads.row(row);
                if let Some(pos) = buffer.iter().position(|e| e.payload.row() == full) {
                    if track {
                        delta -= topn_entry_bytes(&buffer[pos]) as isize;
                    }
                    buffer.remove(pos);
                }
            } else {
                // Insert after any rows that order equal-or-before, preserving arrival order for
                // ties (byte compare of the memcomparable sort key).
                let key_row = keys.row(row);
                let pos = buffer.partition_point(|e| e.sort.row() <= key_row);
                buffer.insert(
                    pos,
                    TopNRow {
                        sort: key_row.owned(),
                        payload: Arc::new(payloads.row(row).owned()),
                        ts_ms: 0,
                    },
                );
                if track {
                    delta += topn_entry_bytes(&buffer[pos]) as isize;
                }
            }
            // The head-clock invariant: Flink ends EVERY processElement with treeMap.update —
            // even a stale retraction's warn-and-skip — and any ValueState write refreshes its
            // TTL, so every processed record refreshes the partition's whole-buffer clock.
            if ttl.enabled() {
                if let Some(head) = buffer.first_mut() {
                    head.ts_ms = ttl.now();
                }
            }
            let new_top: Vec<Arc<OwnedRow>> = buffer
                [offset.min(buffer.len())..limit.min(buffer.len())]
                .iter()
                .map(|e| Arc::clone(&e.payload))
                .collect();
            diff_top(
                rank_output,
                true,
                rank_base,
                &old_top,
                &new_top,
                &mut out_rows,
                &mut out_kinds,
                &mut out_ranks,
            );
        }
        self.groups.end_bundle()?;
        self.memory.record(delta + self.groups.footprint_delta());
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
    fn push_net_diff(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        if self.converters.is_none() {
            self.converters = Some(TopNConverters::build(
                batch,
                arity,
                &self.partition_columns,
                &self.sort_columns,
            ));
        }
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        // The sweep must not run mid-bundle: dropping a staged partition's buffer would surface
        // at the flush as a spurious diff instead of silent expiry.
        if ttl.enabled()
            && self.staged_old_tops.is_empty()
            && now_ms >= self.last_sweep_ms + self.ttl_ms
        {
            self.sweep_expired(ttl);
            self.last_sweep_ms = now_ms;
        }
        // The mini-batch bundle spans pushes: hydrated partitions stay resident until the flush
        // ends the bundle, so the staged preimages' re-probes there stay truthful.
        self.groups.begin_batch(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        )?;
        let conv = self.converters.as_ref().expect("converters set");
        let mut parts = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let sort_arrays: Vec<ArrayRef> = self
            .sort_columns
            .iter()
            .map(|s| batch.column(s.index).clone())
            .collect();
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let keys = encode_group_keys(&conv.sort, &sort_arrays, batch.num_rows());
        let payloads = conv
            .payload
            .convert_columns(&data_arrays)
            .expect("encode payload");
        let row_kinds = row_kind_column(batch);
        let (offset, limit) = (self.offset as usize, self.limit as usize);
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let groups = &mut self.groups;

        let staged_order = &mut self.staged_order;
        let staged_old_tops = &mut self.staged_old_tops;
        for row in 0..batch.num_rows() {
            let part = parts.encode(row);
            let buffer = match groups.get_mut(part) {
                Some(buffer) => buffer,
                None => {
                    if track {
                        delta += (part.len() + GROUP_ENTRY_OVERHEAD) as isize;
                    }
                    groups.insert(ByteKey::from(part), Vec::new())
                }
            };
            if !staged_old_tops.contains_key(part) {
                // Whole-buffer expiry only on the bundle's first touch, BEFORE the preimage
                // capture — silent in the flush diff (see push_net_diff on the append-only ranker).
                if ttl.enabled() {
                    delta -= Self::expire_whole_buffer(buffer, ttl, track);
                }
                let key = ByteKey::from(part);
                let old = buffer[offset.min(buffer.len())..limit.min(buffer.len())]
                    .iter()
                    .map(|e| Arc::clone(&e.payload))
                    .collect();
                if track {
                    delta += topn_staged_entry_bytes(&key, &old) as isize;
                }
                staged_order.push(key.clone());
                staged_old_tops.insert(key, old);
            }

            let retract = matches!(row_kinds.map(|k| k.value(row)).unwrap_or(0), 1 | 3);
            let mut mutated = false;
            if retract {
                let full = payloads.row(row);
                if let Some(pos) = buffer.iter().position(|e| e.payload.row() == full) {
                    if track {
                        delta -= topn_entry_bytes(&buffer[pos]) as isize;
                    }
                    buffer.remove(pos);
                    mutated = true;
                }
            } else {
                let key_row = keys.row(row);
                let pos = buffer.partition_point(|e| e.sort.row() <= key_row);
                buffer.insert(
                    pos,
                    TopNRow {
                        sort: key_row.owned(),
                        payload: Arc::new(payloads.row(row).owned()),
                        ts_ms: 0,
                    },
                );
                if track {
                    delta += topn_entry_bytes(&buffer[pos]) as isize;
                }
                mutated = true;
            }
            if ttl.enabled() && mutated {
                if let Some(head) = buffer.first_mut() {
                    head.ts_ms = ttl.now();
                }
            }
        }
        self.memory.record(delta + self.groups.footprint_delta());
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
        let staged_bytes = if self.memory.tracking() {
            self.staging_bytes()
        } else {
            0
        };
        let touched = std::mem::take(&mut self.staged_order);
        let old_tops = std::mem::take(&mut self.staged_old_tops);
        let (offset, limit) = (self.offset as usize, self.limit as usize);
        let mut out_rows = Vec::new();
        let mut out_kinds = Vec::new();
        let mut out_ranks = Vec::new();
        for part in touched {
            let buffer = self.groups.get(&part.0).expect("staged partition resident");
            let new_top: Vec<Arc<OwnedRow>> = buffer
                [offset.min(buffer.len())..limit.min(buffer.len())]
                .iter()
                .map(|e| Arc::clone(&e.payload))
                .collect();
            diff_top(
                self.output_rank_number,
                true,
                self.offset,
                &old_tops[&part],
                &new_top,
                &mut out_rows,
                &mut out_kinds,
                &mut out_ranks,
            );
        }
        self.groups
            .end_bundle()
            .expect("end retracting top-n bundle");
        self.memory.record(self.groups.footprint_delta());
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
}

/// The raw keyed-state snapshot/restore surface exists only on the memory backend — a persistent
/// store checkpoints through its own commit path instead of materializing the key space.
impl RetractableTopNRanker {
    /// Serializes the buffered rows in per-partition buffer order (partition derivable from the row).
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Vec<u8> {
        raw_topn_snapshot_groups(&self.groups, self.schema.as_ref(), 1, self.ttl_ms > 0)
            .remove(&0)
            .unwrap_or_default()
    }

    fn snapshot_partitions(&self, max_parallelism: usize) -> BTreeMap<i32, Vec<u8>> {
        raw_topn_snapshot_groups(
            &self.groups,
            self.schema.as_ref(),
            max_parallelism,
            self.ttl_ms > 0,
        )
    }

    /// `restored_at_ms` stamps rows from a snapshot carrying no TTL timestamps (a pre-TTL or
    /// TTL-off writer) — the enable-TTL migration; with timestamps present, buffer order is
    /// preserved so the head clock round-trips.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore(
        partition_columns: Vec<usize>,
        key_timestamp_precisions: Vec<i32>,
        sort_columns: Vec<SortColumn>,
        offset: i64,
        limit: i64,
        output_rank_number: bool,
        bytes: &[u8],
        restored_at_ms: i64,
    ) -> Self {
        Self::restore_partitions(
            partition_columns,
            key_timestamp_precisions,
            sort_columns,
            offset,
            limit,
            output_rank_number,
            &[bytes.to_vec()],
            restored_at_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn restore_partitions(
        partition_columns: Vec<usize>,
        key_timestamp_precisions: Vec<i32>,
        sort_columns: Vec<SortColumn>,
        offset: i64,
        limit: i64,
        output_rank_number: bool,
        snapshots: &[Vec<u8>],
        restored_at_ms: i64,
    ) -> Self {
        let mut ranker = RetractableTopNRanker::new(
            partition_columns,
            sort_columns,
            offset,
            limit,
            output_rank_number,
        )
        .with_key_timestamp_precisions(key_timestamp_precisions);
        for bytes in snapshots {
            for batch in read_ipc_if_present(bytes) {
                if batch.schema_ref().field(0).name() == RAW_SNAPSHOT_KEY {
                    load_topn_batch_raw(
                        &mut ranker.schema,
                        &mut ranker.converters,
                        &mut ranker.groups,
                        &ranker.partition_columns,
                        &ranker.sort_columns,
                        &batch,
                        restored_at_ms,
                    );
                } else {
                    ranker.load_batch_decoded(&batch, restored_at_ms);
                }
            }
        }
        ranker
    }

    /// Snapshots written before the raw format decoded the buffers to typed columns; kept so
    /// existing savepoints keep restoring. The format predates TTL, so every row is stamped with
    /// the restore time (the enable-TTL migration).
    fn load_batch_decoded(&mut self, batch: &RecordBatch, restored_at_ms: i64) {
        let arity = batch.num_columns();
        self.schema = Some(batch.schema());
        if self.converters.is_none() {
            self.converters = Some(TopNConverters::build(
                batch,
                arity,
                &self.partition_columns,
                &self.sort_columns,
            ));
        }
        let conv = self.converters.as_ref().expect("converters set");
        let mut parts = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let sort_arrays: Vec<ArrayRef> = self
            .sort_columns
            .iter()
            .map(|s| batch.column(s.index).clone())
            .collect();
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let keys = encode_group_keys(&conv.sort, &sort_arrays, batch.num_rows());
        let payloads = conv
            .payload
            .convert_columns(&data_arrays)
            .expect("encode payload");
        let groups = &mut self.groups;
        for row in 0..batch.num_rows() {
            let part = parts.encode(row);
            let buffer = match groups.get_mut(part) {
                Some(buffer) => buffer,
                None => groups.insert(ByteKey::from(part), Vec::new()),
            };
            buffer.push(TopNRow {
                sort: keys.row(row).owned(),
                payload: Arc::new(payloads.row(row).owned()),
                ts_ms: restored_at_ms,
            }); // buffer order
        }
    }
}

/// Appends the changelog transitioning a partition's top-N from `old_top` to `new_top` (see the
/// retracting ranker's doc). Row identity is payload-byte equality, with an `Arc` pointer check as
/// the fast path (an unchanged rank is usually the same buffered row).
fn diff_top(
    output_rank_number: bool,
    generate_update_before: bool,
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
                    if generate_update_before {
                        out_rows.push(Arc::clone(o));
                        out_kinds.push(1); // -U the old occupant of this rank
                        out_ranks.push(rank);
                    }
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
        let mut entered = Vec::new();
        for r in new_top {
            match old_counts.get_mut(r.row().data()) {
                Some(c) if *c > 0 => *c -= 1, // still present — no change
                _ => entered.push(Arc::clone(r)),
            }
        }
        // Flink retracts the row leaving the rank window before inserting its replacement. The
        // order matters to upsert sinks when both rows have the same downstream key.
        for r in old_top {
            let count = old_counts.get_mut(r.row().data()).expect("counted");
            if *count > 0 {
                *count -= 1;
                out_rows.push(Arc::clone(r));
                out_kinds.push(3); // -D a row that left the top-N
            }
        }
        for row in entered {
            out_rows.push(row);
            out_kinds.push(0); // +I a row that entered the top-N
        }
    }
}

/// Update-fast streaming Top-N — Flink's `UpdatableTopNFunction` (and `FastTop1Function` for
/// `limit == 1`): a rank over a changelog whose rows are replaced in place by a unique key (which
/// contains the partition key) with a sort key the planner proved monotonic, so no retraction ever
/// arrives — an update is just a new version of its row key. Only the top-N rows are kept per
/// partition, exactly Flink's state shape: a row displaced past rank N is deleted, and a row
/// arriving beyond rank N never enters (its later versions can only improve toward the top). The
/// emitted changelog is the per-input-row diff of the top-N before vs after the mutation — the
/// retracting ranker's contract, materially identical to Flink's cascade. A tracked row whose sort
/// key moves the wrong way (possible when upstream state expired) re-sorts like any change —
/// Flink's lenient path.
///
/// `limit == 1` replicates `FastTop1Function`, which never consults the unique key: a record that
/// does not strictly improve on the current top-1 is dropped without touching state or output, so
/// a same-sort-key update keeps the stale payload. Matching Flink's materialized result means
/// reproducing exactly that.
pub(crate) struct UpdatableRow {
    pub(crate) sort: OwnedRow,
    pub(crate) payload: Arc<OwnedRow>,
    pub(crate) row_key: ByteKey,
    /// Wall-clock millis of the entry's last write (Flink state-TTL); stays 0 while TTL is off.
    /// Granularity is the row key (Flink's `MapState<rowKey, …>` / the top-1 `ValueState`):
    /// refreshed by an in-place replace, a move, or an insert.
    pub(crate) ts_ms: i64,
}

pub(crate) fn updatable_entry_bytes(entry: &UpdatableRow) -> usize {
    entry.sort.row().as_ref().len()
        + entry.payload.row().as_ref().len()
        + entry.row_key.len()
        + GROUP_ENTRY_OVERHEAD
}

/// The update-fast buffer's persistent backend: the generic persistent store under the raw
/// whole-list codec.
#[cfg(feature = "rocksdb-state")]
pub(crate) type RocksUpdatableTopNStore = crate::state::RocksStore<UpdatableTopNStateCodec>;

/// [`TopNStateCodec`] for the update-fast buffer: each row additionally carries its unique-key
/// bytes — `[sort_len: u32 LE][sort][payload_len: u32 LE][payload][row_key_len: u32 LE][row_key]
/// [ts_ms: i64 LE]` per row after the `u32` row count.
#[cfg(feature = "rocksdb-state")]
pub(crate) struct UpdatableTopNStateCodec {
    sort: Arc<RowConverter>,
    payload: Arc<RowConverter>,
}

#[cfg(feature = "rocksdb-state")]
impl UpdatableTopNStateCodec {
    pub(crate) fn new(converters: &TopNConverters) -> Self {
        UpdatableTopNStateCodec {
            sort: Arc::clone(&converters.sort),
            payload: Arc::clone(&converters.payload),
        }
    }
}

#[cfg(feature = "rocksdb-state")]
impl crate::state::RocksStateCodec for UpdatableTopNStateCodec {
    type Value = Vec<UpdatableRow>;
    fn supported(&self) -> bool {
        true
    }
    fn value_fields(&self) -> Vec<(String, DataType)> {
        vec![("rows".to_string(), DataType::Binary)]
    }
    fn encode(&self, _value: &Vec<UpdatableRow>) -> Vec<ScalarValue> {
        unreachable!("raw codec")
    }
    fn decode(&self, _scalars: &[ScalarValue]) -> Vec<UpdatableRow> {
        unreachable!("raw codec")
    }
    fn value_bytes(&self, value: &Vec<UpdatableRow>) -> usize {
        4 + value
            .iter()
            .map(|entry| {
                20 + entry.sort.row().data().len()
                    + entry.payload.row().data().len()
                    + entry.row_key.len()
            })
            .sum::<usize>()
    }
    fn write_ms(&self, value: &Vec<UpdatableRow>) -> i64 {
        value.iter().map(|entry| entry.ts_ms).max().unwrap_or(0)
    }
    fn raw(&self) -> bool {
        true
    }
    fn raw_write(&self, value: &Vec<UpdatableRow>, out: &mut Vec<u8>) {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        for entry in value {
            write_length_prefixed(out, entry.sort.row().data());
            write_length_prefixed(out, entry.payload.row().data());
            write_length_prefixed(out, &entry.row_key.0);
            out.extend_from_slice(&entry.ts_ms.to_le_bytes());
        }
    }
    fn from_raw(&self, bytes: &[u8]) -> Vec<UpdatableRow> {
        let mut cursor = RawListCursor::new(bytes);
        let sort_parser = self.sort.parser();
        let payload_parser = self.payload.parser();
        (0..cursor.u32())
            .map(|_| UpdatableRow {
                sort: sort_parser.parse(cursor.bytes()).owned(),
                payload: Arc::new(payload_parser.parse(cursor.bytes()).owned()),
                row_key: ByteKey::from(cursor.bytes()),
                ts_ms: cursor.i64(),
            })
            .collect()
    }
}

/// [`prune_expired_topn_rows`] for the update-fast buffer: per-row-key entry expiry. Silent — the
/// next record for an expired row key is treated as a fresh insert (for `limit == 1` that means
/// even a strictly worse row becomes the new top-1, exactly Flink's expired `ValueState` read).
fn prune_expired_updatable_rows(
    buffer: &mut Vec<UpdatableRow>,
    ttl: StateTtl,
    track: bool,
) -> isize {
    let mut reclaimed = 0isize;
    buffer.retain(|entry| {
        if ttl.expired(entry.ts_ms) {
            if track {
                reclaimed += updatable_entry_bytes(entry) as isize;
            }
            false
        } else {
            true
        }
    });
    reclaimed
}

/// The raw update-fast snapshot stores the row's unique-key bytes alongside the shared
/// key/sort/row columns, so restore stays a byte wrap like the other rankers'.
const RAW_SNAPSHOT_ROW_KEY: &str = "__row_key__";

pub(crate) struct UpdatableTopNRanker<
    S: KeyedStateStore<Vec<UpdatableRow>> = MemoryUpdatableTopNStore,
> {
    partition_columns: Vec<usize>,
    key_timestamp_precisions: Vec<i32>,
    row_key_columns: Vec<usize>,
    row_key_timestamp_precisions: Vec<i32>,
    sort_columns: Vec<SortColumn>,
    limit: i64,
    output_rank_number: bool,
    generate_update_before: bool,
    // Idle-state retention millis (0 = off); per-row-key entry expiry, like Flink's MapState TTL.
    ttl_ms: i64,
    last_sweep_ms: i64,
    schema: Option<SchemaRef>,
    converters: Option<TopNConverters>,
    // Keyed like the other rankers' stores (see TopNRanker::groups); the buffer stays a Vec
    // sorted by the memcomparable sort key — the binary-searched hot path — on every backend.
    groups: S,
    pub(crate) memory: OperatorMemory,
}

/// The resident default backend for the update-fast buffer store (see `state/` for the seam).
pub(crate) type MemoryUpdatableTopNStore = MemoryStateStore<Vec<UpdatableRow>>;

impl UpdatableTopNRanker {
    pub(crate) fn new(
        partition_columns: Vec<usize>,
        key_timestamp_precisions: Vec<i32>,
        row_key_columns: Vec<usize>,
        row_key_timestamp_precisions: Vec<i32>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        output_rank_number: bool,
        generate_update_before: bool,
    ) -> Self {
        UpdatableTopNRanker {
            partition_columns,
            key_timestamp_precisions,
            row_key_columns,
            row_key_timestamp_precisions,
            sort_columns,
            limit,
            output_rank_number,
            generate_update_before,
            ttl_ms: 0,
            last_sweep_ms: 0,
            schema: None,
            converters: None,
            groups: MemoryStateStore::default(),
            memory: OperatorMemory::unaccounted(),
        }
    }
}

impl<S: KeyedStateStore<Vec<UpdatableRow>>> UpdatableTopNRanker<S> {
    /// Moves this freshly built (empty, memory-backed) ranker's configuration onto another state
    /// backend (see the append-only ranker's `with_backend`).
    pub(crate) fn with_backend<T: KeyedStateStore<Vec<UpdatableRow>>>(
        self,
        groups: T,
    ) -> UpdatableTopNRanker<T> {
        UpdatableTopNRanker {
            partition_columns: self.partition_columns,
            key_timestamp_precisions: self.key_timestamp_precisions,
            row_key_columns: self.row_key_columns,
            row_key_timestamp_precisions: self.row_key_timestamp_precisions,
            sort_columns: self.sort_columns,
            limit: self.limit,
            output_rank_number: self.output_rank_number,
            generate_update_before: self.generate_update_before,
            ttl_ms: self.ttl_ms,
            last_sweep_ms: self.last_sweep_ms,
            schema: self.schema,
            converters: self.converters,
            groups,
            memory: self.memory,
        }
    }

    /// Attaches the task off-heap budget for a backend that starts with nothing resident.
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory.attach("update-fast-top-n", budget_bytes, 0)?;
        Ok(self)
    }

    /// The backing store, for backend-specific control paths (checkpointing a persistent store).
    pub(crate) fn store_mut(&mut self) -> &mut S {
        &mut self.groups
    }

    /// Pre-installs a converter set built from declared types (the persistent path, which must share
    /// the codec's converters); the lazy first-batch build then never runs.
    pub(crate) fn with_converters(mut self, converters: TopNConverters) -> Self {
        self.converters = Some(converters);
        self
    }

    /// Pre-installs the payload schema alongside `with_converters` on the persistent path, so
    /// canonical snapshots work before the first input batch arrives.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_payload_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Sets the idle-state retention (`table.exec.state.ttl`) in millis; 0 (Flink's default)
    /// disables expiry.
    pub(crate) fn with_state_ttl(mut self, ttl_ms: i64) -> Self {
        self.ttl_ms = ttl_ms.max(0);
        self
    }

    /// Reclaims every entry whose TTL elapsed with no further touch of its partition. Silent,
    /// like Flink's background cleanup.
    fn sweep_expired(&mut self, ttl: StateTtl) {
        let track = self.memory.tracking();
        let mut reclaimed = 0isize;
        self.groups.retain_live(&mut |key, buffer| {
            reclaimed += prune_expired_updatable_rows(buffer, ttl, track);
            if buffer.is_empty() {
                if track {
                    reclaimed += (key.len() + GROUP_ENTRY_OVERHEAD) as isize;
                }
                false
            } else {
                true
            }
        });
        if reclaimed != 0 {
            self.memory.record(-reclaimed);
        }
    }

    /// `now_ms` is the host's wall-clock reading for this call (only read when state TTL is on).
    pub(crate) fn push(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        if self.converters.is_none() {
            self.converters = Some(TopNConverters::build(
                batch,
                arity,
                &self.partition_columns,
                &self.sort_columns,
            ));
        }
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        if ttl.enabled() && now_ms >= self.last_sweep_ms + self.ttl_ms {
            self.sweep_expired(ttl);
            self.last_sweep_ms = now_ms;
        }
        self.groups.begin_batch(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        )?;
        let conv = self.converters.as_ref().expect("converters set");
        let mut parts = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let mut row_keys = BinaryRowBatchEncoder::new(
            batch,
            &self.row_key_columns,
            &self.row_key_timestamp_precisions,
        );
        let sort_arrays: Vec<ArrayRef> = self
            .sort_columns
            .iter()
            .map(|s| batch.column(s.index).clone())
            .collect();
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let keys = encode_group_keys(&conv.sort, &sort_arrays, batch.num_rows());
        let payloads = conv
            .payload
            .convert_columns(&data_arrays)
            .expect("encode payload");

        let limit = self.limit as usize;
        let top1 = limit == 1;
        let rank_output = self.output_rank_number;
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let groups = &mut self.groups;
        // Partitions already pruned by this call (see the append-only push).
        let mut pruned: HashSet<ByteKey> = HashSet::default();
        // Every state write (replace, move, insert) stamps the entry's clock; 0 with TTL off so
        // the TTL-off state stays byte-identical.
        let stamp = if ttl.enabled() { ttl.now() } else { 0 };
        let mut out_rows: Vec<Arc<OwnedRow>> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        let mut out_ranks: Vec<i64> = Vec::new();

        for row in 0..batch.num_rows() {
            let key_row = keys.row(row);
            let part = parts.encode(row);
            let buffer = match groups.get_mut(part) {
                Some(buffer) => buffer,
                None => {
                    if track {
                        delta += (part.len() + GROUP_ENTRY_OVERHEAD) as isize;
                    }
                    groups.insert(ByteKey::from(part), Vec::new())
                }
            };
            // Per-row-key expiry, enforced before the preimage capture: an expired entry reads as
            // absent, so its row key's next record takes the fresh-insert path below.
            if ttl.enabled() && !pruned.contains(part) {
                delta -= prune_expired_updatable_rows(buffer, ttl, track);
                pruned.insert(ByteKey::from(part));
            }
            // The bounded buffer IS the top-N window.
            let old_top: Vec<Arc<OwnedRow>> =
                buffer.iter().map(|e| Arc::clone(&e.payload)).collect();
            // UpdatableTopNFunction treats a new version of an already-buffered unique key as an
            // UPDATE, even when its sort position changes. Preserve that changelog identity instead
            // of reducing the transition to an anonymous membership delete/insert pair.
            let mut direct_update: Option<(Arc<OwnedRow>, Arc<OwnedRow>, i64)> = None;
            if top1 {
                // FastTop1Function: only a strict improvement replaces the buffered row.
                match buffer.first() {
                    None => {
                        let entry = UpdatableRow {
                            sort: key_row.owned(),
                            payload: Arc::new(payloads.row(row).owned()),
                            row_key: ByteKey::from(row_keys.encode(row)),
                            ts_ms: stamp,
                        };
                        if track {
                            delta += updatable_entry_bytes(&entry) as isize;
                        }
                        buffer.push(entry);
                    }
                    Some(current) if key_row < current.sort.row() => {
                        let old_payload = Arc::clone(&current.payload);
                        if track {
                            delta -= updatable_entry_bytes(&buffer[0]) as isize;
                        }
                        buffer[0] = UpdatableRow {
                            sort: key_row.owned(),
                            payload: Arc::new(payloads.row(row).owned()),
                            row_key: ByteKey::from(row_keys.encode(row)),
                            ts_ms: stamp,
                        };
                        if track {
                            delta += updatable_entry_bytes(&buffer[0]) as isize;
                        }
                        direct_update = Some((old_payload, Arc::clone(&buffer[0].payload), 1));
                    }
                    _ => continue, // equal or worse — dropped, state and output untouched
                }
            } else {
                let row_key = row_keys.encode(row);
                match buffer.iter().position(|e| &*e.row_key.0 == row_key) {
                    Some(index) => {
                        let old_payload = Arc::clone(&buffer[index].payload);
                        if buffer[index].sort.row() == key_row {
                            // Same sort key: replace the payload in place, preserving the row's
                            // position among sort-key ties (Flink's innerRank).
                            let payload = Arc::new(payloads.row(row).owned());
                            if track {
                                delta += payload.row().as_ref().len() as isize
                                    - buffer[index].payload.row().as_ref().len() as isize;
                            }
                            buffer[index].payload = payload;
                            buffer[index].ts_ms = stamp;
                        } else {
                            let previous = buffer.remove(index);
                            if track {
                                delta -= updatable_entry_bytes(&previous) as isize;
                            }
                            let pos = buffer.partition_point(|e| e.sort.row() <= key_row);
                            buffer.insert(
                                pos,
                                UpdatableRow {
                                    sort: key_row.owned(),
                                    payload: Arc::new(payloads.row(row).owned()),
                                    row_key: previous.row_key,
                                    ts_ms: stamp,
                                },
                            );
                            if track {
                                delta += updatable_entry_bytes(&buffer[pos]) as isize;
                            }
                        }
                        let updated = buffer
                            .iter()
                            .find(|entry| &*entry.row_key.0 == row_key)
                            .expect("updated top-n row remains buffered");
                        direct_update =
                            Some((old_payload, Arc::clone(&updated.payload), index as i64 + 1));
                    }
                    None => {
                        let pos = buffer.partition_point(|e| e.sort.row() <= key_row);
                        if pos >= limit {
                            continue; // beyond rank N — never enters, never tracked
                        }
                        buffer.insert(
                            pos,
                            UpdatableRow {
                                sort: key_row.owned(),
                                payload: Arc::new(payloads.row(row).owned()),
                                row_key: ByteKey::from(row_key),
                                ts_ms: stamp,
                            },
                        );
                        if track {
                            delta += updatable_entry_bytes(&buffer[pos]) as isize;
                        }
                        if buffer.len() > limit {
                            let evicted = buffer.pop().expect("buffer over limit is non-empty");
                            if track {
                                delta -= updatable_entry_bytes(&evicted) as isize;
                            }
                        }
                    }
                }
            }
            let new_top: Vec<Arc<OwnedRow>> =
                buffer.iter().map(|e| Arc::clone(&e.payload)).collect();
            if !rank_output {
                if let Some((old, new, _)) = &direct_update {
                    if self.generate_update_before {
                        out_rows.push(Arc::clone(old));
                        out_kinds.push(1); // -U old version of the same unique key
                    }
                    out_rows.push(Arc::clone(new));
                    out_kinds.push(2); // +U new version
                    continue;
                }
            }
            let output_start = out_rows.len();
            diff_top(
                rank_output,
                self.generate_update_before,
                0,
                &old_top,
                &new_top,
                &mut out_rows,
                &mut out_kinds,
                &mut out_ranks,
            );
            // Flink treats every record for an existing unique key as an update. In particular,
            // recovery may replay an UPDATE_AFTER whose projected values equal the restored row;
            // UpdatableTopNFunction still emits the update pair instead of suppressing it as a
            // value-level no-op.
            if rank_output && out_rows.len() == output_start {
                if let Some((old, new, old_rank)) = &direct_update {
                    if self.generate_update_before {
                        out_rows.push(Arc::clone(old));
                        out_kinds.push(1);
                        out_ranks.push(*old_rank);
                    }
                    out_rows.push(Arc::clone(new));
                    out_kinds.push(2);
                    out_ranks.push(*old_rank);
                }
            }
            // Flink first retracts the row displaced at the new rank, then retracts the updated
            // unique key at its old rank, and only then emits the first UPDATE_AFTER. Move that
            // old-rank preimage immediately behind the first displaced-row preimage.
            if rank_output {
                if let Some((old, _, old_rank)) = &direct_update {
                    if let Some(index) = (output_start..out_rows.len()).find(|&index| {
                        out_kinds[index] == 1
                            && out_ranks[index] == *old_rank
                            && out_rows[index].row() == old.row()
                    }) {
                        if index > output_start {
                            let row = out_rows.remove(index);
                            let kind = out_kinds.remove(index);
                            let rank = out_ranks.remove(index);
                            out_rows.insert(output_start + 1, row);
                            out_kinds.insert(output_start + 1, kind);
                            out_ranks.insert(output_start + 1, rank);
                        }
                    }
                }
            }
        }
        self.groups.end_bundle()?;
        self.memory.record(delta + self.groups.footprint_delta());
        self.memory.account()?;
        Ok(emit_changelog(
            self.schema.as_ref(),
            self.converters.as_ref(),
            rank_output,
            out_rows,
            out_kinds,
            out_ranks,
        ))
    }
}

/// One key group's raw update-fast snapshot blob, buffer order preserved. The TTL timestamps ride
/// a trailing column only while TTL is on, so a TTL-off snapshot stays byte-identical to the
/// pre-TTL format.
fn write_raw_updatable_snapshot_partition<'a>(
    entries: impl Iterator<Item = (&'a ByteKey, &'a Vec<UpdatableRow>)>,
    schema: &SchemaRef,
    ttl_on: bool,
) -> Vec<u8> {
    let mut keys = BinaryBuilder::new();
    let mut sorts = BinaryBuilder::new();
    let mut row_keys = BinaryBuilder::new();
    let mut rows = BinaryBuilder::new();
    let mut write_timestamps = Int64Builder::new();
    for (key, buffer) in entries {
        for entry in buffer {
            keys.append_value(&key.0);
            sorts.append_value(entry.sort.row().data());
            row_keys.append_value(&entry.row_key.0);
            rows.append_value(entry.payload.row().data());
            write_timestamps.append_value(entry.ts_ms);
        }
    }
    let mut fields = vec![
        Field::new(RAW_SNAPSHOT_KEY, DataType::Binary, false),
        Field::new(RAW_SNAPSHOT_SORT, DataType::Binary, false),
        Field::new(RAW_SNAPSHOT_ROW_KEY, DataType::Binary, false),
        Field::new(RAW_SNAPSHOT_ROW, DataType::Binary, false),
    ];
    if ttl_on {
        fields.push(Field::new(TTL_TS_COLUMN, DataType::Int64, false));
    }
    let raw_schema = Arc::new(Schema::new_with_metadata(
        fields,
        std::collections::HashMap::from([(
            RAW_SNAPSHOT_PAYLOAD_SCHEMA.to_string(),
            encode_schema_metadata(schema),
        )]),
    ));
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(keys.finish()),
        Arc::new(sorts.finish()),
        Arc::new(row_keys.finish()),
        Arc::new(rows.finish()),
    ];
    if ttl_on {
        columns.push(Arc::new(write_timestamps.finish()));
    }
    let batch =
        RecordBatch::try_new(raw_schema, columns).expect("raw update-fast top-n snapshot batch");
    write_ipc(&batch)
}

/// See [`TopNRanker::canonical_partitions`]; the update-fast buffers use their own raw encoding.
#[cfg(feature = "rocksdb-state")]
impl UpdatableTopNRanker<RocksUpdatableTopNStore> {
    pub(crate) fn canonical_partitions(
        &mut self,
    ) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError> {
        let schema = self
            .schema
            .clone()
            .expect("declared schema installed on the persistent path");
        let ttl_on = self.ttl_ms > 0;
        rocks_canonical_partitions(&mut self.groups, |entries| {
            write_raw_updatable_snapshot_partition(
                entries.iter().map(|&(key, buffer)| (key, buffer)),
                &schema,
                ttl_on,
            )
        })
    }
}

/// The raw keyed-state snapshot/restore surface exists only on the memory backend — a persistent
/// store checkpoints through its own commit path instead of materializing the key space.
impl UpdatableTopNRanker {
    /// Bounds the per-partition buffers by the operator's task off-heap budget (negative =
    /// unaccounted), accounting any restored buffers immediately.
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .groups
            .iter()
            .map(|(key, buffer)| {
                byte_key_bytes(&key.0) + buffer.iter().map(updatable_entry_bytes).sum::<usize>()
            })
            .sum();
        self.memory
            .attach("update-fast-top-n", budget_bytes, state)?;
        Ok(self)
    }

    pub(crate) fn snapshot_partitions(&self, max_parallelism: usize) -> BTreeMap<i32, Vec<u8>> {
        let Some(schema) = self.schema.as_ref() else {
            return BTreeMap::new();
        };
        let mut partitions: BTreeMap<i32, Vec<(&ByteKey, &Vec<UpdatableRow>)>> = BTreeMap::new();
        for (key, buffer) in self.groups.iter() {
            if buffer.is_empty() {
                continue;
            }
            let group = flink_key_group(hash_bytes_by_words(&key.0), max_parallelism) as i32;
            partitions.entry(group).or_default().push((key, buffer));
        }
        partitions
            .into_iter()
            .map(|(group, entries)| {
                (
                    group,
                    write_raw_updatable_snapshot_partition(
                        entries.into_iter(),
                        schema,
                        self.ttl_ms > 0,
                    ),
                )
            })
            .collect()
    }

    /// `restored_at_ms` stamps rows from a snapshot carrying no TTL timestamps (a pre-TTL or
    /// TTL-off writer) — the enable-TTL migration.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore_partitions(
        partition_columns: Vec<usize>,
        key_timestamp_precisions: Vec<i32>,
        row_key_columns: Vec<usize>,
        row_key_timestamp_precisions: Vec<i32>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        output_rank_number: bool,
        generate_update_before: bool,
        snapshots: &[Vec<u8>],
        restored_at_ms: i64,
    ) -> Self {
        let mut ranker = UpdatableTopNRanker::new(
            partition_columns,
            key_timestamp_precisions,
            row_key_columns,
            row_key_timestamp_precisions,
            sort_columns,
            limit,
            output_rank_number,
            generate_update_before,
        );
        for bytes in snapshots {
            for batch in read_ipc_if_present(bytes) {
                if ranker.schema.is_none() {
                    let payload_schema = decode_schema_metadata(&batch)
                        .expect("raw update-fast snapshot payload schema");
                    let empty = RecordBatch::new_empty(payload_schema.clone());
                    ranker.converters = Some(TopNConverters::build(
                        &empty,
                        empty.num_columns(),
                        &ranker.partition_columns,
                        &ranker.sort_columns,
                    ));
                    ranker.schema = Some(payload_schema);
                }
                let conv = ranker.converters.as_ref().expect("converters set");
                let sort_parser = conv.sort.parser();
                let payload_parser = conv.payload.parser();
                let keys = column_binary(&batch, RAW_SNAPSHOT_KEY);
                let sorts = column_binary(&batch, RAW_SNAPSHOT_SORT);
                let row_keys = column_binary(&batch, RAW_SNAPSHOT_ROW_KEY);
                let rows = column_binary(&batch, RAW_SNAPSHOT_ROW);
                let write_timestamps = batch
                    .column_by_name(TTL_TS_COLUMN)
                    .is_some()
                    .then(|| column_i64(&batch, TTL_TS_COLUMN));
                for row in 0..batch.num_rows() {
                    let part = keys.value(row);
                    let buffer = match ranker.groups.get_mut(part) {
                        Some(buffer) => buffer,
                        None => ranker.groups.insert(ByteKey::from(part), Vec::new()),
                    };
                    buffer.push(UpdatableRow {
                        sort: sort_parser.parse(sorts.value(row)).owned(),
                        payload: Arc::new(payload_parser.parse(rows.value(row)).owned()),
                        row_key: ByteKey::from(row_keys.value(row)),
                        ts_ms: write_timestamps
                            .as_ref()
                            .map_or(restored_at_ms, |ts| ts.value(row)),
                    });
                }
            }
        }
        ranker
    }
}

/// The Top-N handle the JVM holds: append-only (insert-only input, bounded buffer), retracting
/// (changelog input, full buffer), or update-fast (unique-keyed changelog without retractions,
/// bounded buffer). All push a batch and return a changelog, snapshot, and restore.
pub(crate) enum TopNHandle {
    Append(TopNRanker),
    Retract(RetractableTopNRanker),
    UpdateFast(UpdatableTopNRanker),
}

impl TopNHandle {
    fn cache_size(&self) -> usize {
        match self {
            TopNHandle::Append(r) => r.groups.iter().map(|(_, rows)| rows.len()).sum(),
            TopNHandle::Retract(_) => 0,
            TopNHandle::UpdateFast(r) => r.groups.iter().map(|(_, rows)| rows.len()).sum(),
        }
    }

    fn push(&mut self, batch: &RecordBatch, now_ms: i64) -> Result<RecordBatch, DataFusionError> {
        match self {
            TopNHandle::Append(r) => r.push(batch, now_ms),
            TopNHandle::Retract(r) => r.push(batch, now_ms),
            TopNHandle::UpdateFast(r) => r.push(batch, now_ms),
        }
    }

    fn flush(&mut self) -> RecordBatch {
        match self {
            TopNHandle::Append(r) => r.flush_net_diff(),
            TopNHandle::Retract(r) => r.flush_net_diff(),
            // The update-fast ranker has no net-diff staging; every push emitted its diff already.
            TopNHandle::UpdateFast(_) => RecordBatch::new_empty(Arc::new(Schema::empty())),
        }
    }

    /// Bounds the ranker's buffers by the operator's task off-heap budget (negative = unaccounted).
    fn with_memory_budget(self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        Ok(match self {
            TopNHandle::Append(r) => TopNHandle::Append(r.with_memory_budget(budget_bytes)?),
            TopNHandle::Retract(r) => TopNHandle::Retract(r.with_memory_budget(budget_bytes)?),
            TopNHandle::UpdateFast(r) => {
                TopNHandle::UpdateFast(r.with_memory_budget(budget_bytes)?)
            }
        })
    }

    fn snapshot_partitions(&self, max_parallelism: usize) -> BTreeMap<i32, Vec<u8>> {
        match self {
            TopNHandle::Append(r) => r.snapshot_partitions(max_parallelism),
            TopNHandle::Retract(r) => r.snapshot_partitions(max_parallelism),
            TopNHandle::UpdateFast(r) => r.snapshot_partitions(max_parallelism),
        }
    }

    fn capture_append_snapshot(&self, max_parallelism: usize) -> Option<AppendTopNSnapshot> {
        match self {
            TopNHandle::Append(ranker) => AppendTopNSnapshot::capture(ranker, max_parallelism),
            TopNHandle::Retract(_) | TopNHandle::UpdateFast(_) => None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn restore_partitions(
        partition_columns: Vec<usize>,
        key_timestamp_precisions: Vec<i32>,
        sort_columns: Vec<SortColumn>,
        offset: i64,
        limit: i64,
        output_rank_number: bool,
        retracting: bool,
        net_diff: bool,
        state_ttl_ms: i64,
        snapshots: &[Vec<u8>],
        restored_at_ms: i64,
    ) -> Self {
        if retracting {
            TopNHandle::Retract(
                RetractableTopNRanker::restore_partitions(
                    partition_columns,
                    key_timestamp_precisions,
                    sort_columns,
                    offset,
                    limit,
                    output_rank_number,
                    snapshots,
                    restored_at_ms,
                )
                .with_net_diff(net_diff)
                .with_state_ttl(state_ttl_ms),
            )
        } else {
            TopNHandle::Append(
                TopNRanker::restore_partitions(
                    partition_columns,
                    key_timestamp_precisions,
                    sort_columns,
                    limit,
                    output_rank_number,
                    net_diff,
                    snapshots,
                    restored_at_ms,
                )
                .with_state_ttl(state_ttl_ms),
            )
        }
    }
}

/// The persistent Top-N handle the JVM holds under the RocksDB backend: the three ranker variants
/// over their typed read-through stores (the [`TopNHandle`] analog). Each variant owns one
/// single-table store; a checkpoint commits that store, and canonical savepoints re-encode the
/// logical table in the memory snapshots' raw key-group format.
#[cfg(feature = "rocksdb-state")]
pub(crate) enum RocksTopNHandle {
    Append(TopNRanker<RocksTopNStore>),
    Retract(RetractableTopNRanker<RocksTopNStore>),
    UpdateFast(UpdatableTopNRanker<RocksUpdatableTopNStore>),
}

#[cfg(feature = "rocksdb-state")]
impl RocksTopNHandle {
    pub(crate) fn push(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        match self {
            RocksTopNHandle::Append(r) => {
                r.store_mut().set_clock(now_ms);
                r.push(batch, now_ms)
            }
            RocksTopNHandle::Retract(r) => {
                r.store_mut().set_clock(now_ms);
                r.push(batch, now_ms)
            }
            RocksTopNHandle::UpdateFast(r) => {
                r.store_mut().set_clock(now_ms);
                r.push(batch, now_ms)
            }
        }
    }

    pub(crate) fn flush(&mut self) -> RecordBatch {
        match self {
            RocksTopNHandle::Append(r) => r.flush_net_diff(),
            RocksTopNHandle::Retract(r) => r.flush_net_diff(),
            // The update-fast ranker has no net-diff staging; every push emitted its diff already.
            RocksTopNHandle::UpdateFast(_) => RecordBatch::new_empty(Arc::new(Schema::empty())),
        }
    }

    pub(crate) fn checkpoint(
        &mut self,
        snapshot_dir: &str,
    ) -> Result<crate::state::RocksCheckpointManifest, DataFusionError> {
        match self {
            RocksTopNHandle::Append(r) => r.store_mut().checkpoint(snapshot_dir),
            RocksTopNHandle::Retract(r) => r.store_mut().checkpoint(snapshot_dir),
            RocksTopNHandle::UpdateFast(r) => r.store_mut().checkpoint(snapshot_dir),
        }
    }

    pub(crate) fn canonical_partitions(
        &mut self,
    ) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError> {
        match self {
            RocksTopNHandle::Append(r) => r.canonical_partitions(),
            RocksTopNHandle::Retract(r) => r.canonical_partitions(),
            RocksTopNHandle::UpdateFast(r) => r.canonical_partitions(),
        }
    }

    pub(crate) fn state_bytes(&self) -> usize {
        match self {
            RocksTopNHandle::Append(r) => r.memory.state_bytes,
            RocksTopNHandle::Retract(r) => r.memory.state_bytes,
            RocksTopNHandle::UpdateFast(r) => r.memory.state_bytes,
        }
    }

    pub(crate) fn staging_bytes(&self) -> usize {
        match self {
            RocksTopNHandle::Append(r) => r.staging_bytes(),
            RocksTopNHandle::Retract(r) => r.staging_bytes(),
            RocksTopNHandle::UpdateFast(_) => 0,
        }
    }

    pub(crate) fn staged_partitions(&self) -> usize {
        match self {
            RocksTopNHandle::Append(r) => r.staged_partitions(),
            RocksTopNHandle::Retract(r) => r.staged_partitions(),
            RocksTopNHandle::UpdateFast(_) => 0,
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
    pub(crate) current_watermark: i64,
    pub(crate) late_drops: u64,
    /// Bounded, sorted top-N buffer per (window_end, window_start, partition key).
    groups: HashMap<(i64, i64, GroupKey), Vec<JoinRow>>,
    schema: Option<SchemaRef>,
    pub(crate) memory: OperatorMemory,
    /// Persistent-state mode: committed (window, key) buffers live in the persistent store; the
    /// resident `groups` map holds only the current bundle's touched groups (hydrated on first
    /// touch, written through at the bundle boundary, then dropped).
    #[cfg(feature = "rocksdb-state")]
    store: Option<crate::state::RocksWindowRankStore>,
    /// The bundle's touched (window, key) groups and their store keys — exactly the resident set,
    /// so the write-through drains it and the routing needs no second hash pass.
    #[cfg(feature = "rocksdb-state")]
    store_groups: HashMap<(i64, i64, GroupKey), Vec<u8>>,
    #[cfg(feature = "rocksdb-state")]
    store_key_converter: Option<RowConverter>,
    #[cfg(feature = "rocksdb-state")]
    key_timestamp_precisions: Vec<i32>,
}

impl WindowRanker {
    pub(crate) fn new(
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
            late_drops: 0,
            groups: HashMap::default(),
            schema: None,
            memory: OperatorMemory::unaccounted(),
            #[cfg(feature = "rocksdb-state")]
            store: None,
            #[cfg(feature = "rocksdb-state")]
            store_groups: HashMap::default(),
            #[cfg(feature = "rocksdb-state")]
            store_key_converter: None,
            #[cfg(feature = "rocksdb-state")]
            key_timestamp_precisions: Vec::new(),
        }
    }

    /// Attaches the persistent (window, key) store, seeding the row schema (a firing may need to
    /// emit before any batch arrives), the partition-key codec, and the late-data watermark from
    /// the checkpoint the store restored.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_store(
        mut self,
        store: crate::state::RocksWindowRankStore,
        schema: SchemaRef,
    ) -> Self {
        self.current_watermark = store.watermark();
        let key_types: Vec<DataType> = self
            .partition_columns
            .iter()
            .map(|&column| schema.field(column).data_type().clone())
            .collect();
        self.store_key_converter = Some(key_row_converter_from_types(&key_types));
        self.schema = Some(schema);
        self.store = Some(store);
        self
    }

    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_key_timestamp_precisions(
        mut self,
        key_timestamp_precisions: Vec<i32>,
    ) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        self
    }

    /// Persists the late-data watermark and takes the store's native checkpoint.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn checkpoint_store(
        &mut self,
        snapshot_dir: &str,
    ) -> Result<crate::state::RocksCheckpointManifest, DataFusionError> {
        let watermark = self.current_watermark;
        self.store
            .as_mut()
            .expect("window-rank rocksdb store")
            .checkpoint(watermark, snapshot_dir)
    }

    /// Bounds the per-window buffers by the operator's task off-heap budget (negative =
    /// unaccounted), accounting any restored buffers immediately.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .groups
            .iter()
            .map(|((_, _, key), buffer)| {
                group_key_bytes(key)
                    + buffer
                        .iter()
                        .map(|r| scalar_row_bytes(r) + GROUP_ENTRY_OVERHEAD)
                        .sum::<usize>()
            })
            .sum();
        self.memory.attach("window-rank", budget_bytes, state)?;
        Ok(self)
    }

    pub(crate) fn push(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        let ws = rt_to_millis(batch.column(self.window_start_col));
        let we = rt_to_millis(batch.column(self.window_end_col));
        let partition_arrays: Vec<&ArrayRef> = self
            .partition_columns
            .iter()
            .map(|&i| batch.column(i))
            .collect();
        #[cfg(feature = "rocksdb-state")]
        self.hydrate_store_groups(batch, &ws, &we, &partition_arrays)?;
        let data_arrays: Vec<&ArrayRef> = (0..arity).map(|i| batch.column(i)).collect();
        let track = self.memory.tracking();
        let mut delta = 0isize;
        for row in 0..batch.num_rows() {
            let window_end = we.value(row);
            if window_end <= self.current_watermark {
                self.late_drops += 1;
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
            let buffer = self
                .groups
                .entry((window_end, window_start, key))
                .or_default();
            // An empty buffer means the (window, key) entry was just created (never emptied by push).
            if track && buffer.is_empty() {
                delta += key_bytes as isize;
            }
            // Insert after rows ordering equal-or-before, preserving arrival order for ties (the
            // ROW_NUMBER tie-break), then drop anything past rank N.
            let pos = buffer.partition_point(|r| {
                compare_rows(r, &full, &self.sort_columns) != std::cmp::Ordering::Greater
            });
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
        self.memory.account()?;
        #[cfg(feature = "rocksdb-state")]
        self.write_through_store()?;
        Ok(())
    }

    /// Persistent-state hydration: point-reads every touched (window, key) group not yet resident
    /// this bundle — one multi-get, one columnar decode — so the ranking and the write-through see
    /// the committed buffer (committed rows precede this batch's rows, preserving the ROW_NUMBER
    /// arrival tie-break). Every touched group's key-group routing (the blob path's BinaryRow
    /// hash) is recorded for the write-through.
    #[cfg(feature = "rocksdb-state")]
    fn hydrate_store_groups(
        &mut self,
        batch: &RecordBatch,
        ws: &Int64Array,
        we: &Int64Array,
        partition_arrays: &[&ArrayRef],
    ) -> Result<(), DataFusionError> {
        if self.store.is_none() {
            return Ok(());
        }
        let key_columns: Vec<ArrayRef> = partition_arrays.iter().map(|&a| a.clone()).collect();
        let key_rows = if key_columns.is_empty() {
            None
        } else {
            Some(
                self.store_key_converter
                    .as_ref()
                    .expect("window-rank store key converter")
                    .convert_columns(&key_columns)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
            )
        };
        let mut encoder = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let mut probes: Vec<(i64, i64, GroupKey)> = Vec::new();
        let mut db_keys: Vec<Vec<u8>> = Vec::new();
        for row in 0..batch.num_rows() {
            let window_end = we.value(row);
            if window_end <= self.current_watermark {
                continue;
            }
            let group = (window_end, ws.value(row), read_key(partition_arrays, row));
            if self.store_groups.contains_key(&group) {
                continue;
            }
            let store = self.store.as_ref().expect("window-rank rocksdb store");
            let key_group = store.key_group(encoder.hash(row));
            let key_bytes = key_rows
                .as_ref()
                .map(|rows| rows.row(row).data())
                .unwrap_or(&[]);
            let db_key = store.db_key(key_group, group.0, group.1, key_bytes);
            db_keys.push(db_key.clone());
            self.store_groups.insert(group.clone(), db_key);
            probes.push(group);
        }
        if probes.is_empty() {
            return Ok(());
        }
        let fetched = self
            .store
            .as_ref()
            .expect("window-rank rocksdb store")
            .get(&db_keys)?;
        let track = self.memory.tracking();
        for ((end, start, key), stored) in probes.into_iter().zip(fetched) {
            let Some(rows) = stored else {
                continue;
            };
            if track {
                self.memory.record(
                    (group_key_bytes(&key)
                        + rows
                            .iter()
                            .map(|r| scalar_row_bytes(r) + GROUP_ENTRY_OVERHEAD)
                            .sum::<usize>()) as isize,
                );
            }
            self.groups.insert((end, start, key), rows);
        }
        self.memory.account()
    }

    /// Persistent-state bundle boundary: writes every touched (window, key) group's ranked buffer
    /// through in one columnar encode, then drops the resident entries — the map stays empty
    /// between bundles and RocksDB owns durability and memory.
    #[cfg(feature = "rocksdb-state")]
    fn write_through_store(&mut self) -> Result<(), DataFusionError> {
        if self.store.is_none() {
            return Ok(());
        }
        let track = self.memory.tracking();
        let store_groups = std::mem::take(&mut self.store_groups);
        let mut entries: Vec<(Vec<u8>, Vec<JoinRow>)> = Vec::with_capacity(store_groups.len());
        let mut freed = 0usize;
        for ((end, start, key), db_key) in store_groups {
            if track {
                freed += group_key_bytes(&key);
            }
            let buffer = self
                .groups
                .remove(&(end, start, key))
                .expect("touched group resident");
            if track {
                freed += buffer
                    .iter()
                    .map(|r| scalar_row_bytes(r) + GROUP_ENTRY_OVERHEAD)
                    .sum::<usize>();
            }
            entries.push((db_key, buffer));
        }
        self.store
            .as_mut()
            .expect("window-rank rocksdb store")
            .put(entries)?;
        self.memory.forget(freed);
        self.memory.account_shrink();
        Ok(())
    }

    /// Emits the top-N rows of every window the watermark has closed, in rank order (with the rank
    /// number appended when the host projects it), and evicts those windows.
    pub(crate) fn flush(&mut self, watermark: i64) -> Result<RecordBatch, DataFusionError> {
        self.current_watermark = watermark;
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return self.flush_store(watermark);
        }
        let mut ready: Vec<(i64, i64, GroupKey)> = self
            .groups
            .keys()
            .filter(|(we, _, _)| *we <= watermark)
            .cloned()
            .collect();
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
                freed += buffer
                    .iter()
                    .map(|r| scalar_row_bytes(r) + GROUP_ENTRY_OVERHEAD)
                    .sum::<usize>();
            }
            for (rank, row) in buffer.into_iter().enumerate() {
                rows.push(row);
                ranks.push(rank as i64 + 1);
            }
        }
        self.memory.forget(freed);
        self.memory.account_shrink();
        Ok(self.emit(rows, ranks))
    }

    /// Persistent-state firing: the store removes and returns every closed (window, key) group in
    /// (window_end, window_start) order with each buffer already ranked — every fired group leaves
    /// the store, so a closed window can never re-fire after a restore.
    #[cfg(feature = "rocksdb-state")]
    fn flush_store(&mut self, watermark: i64) -> Result<RecordBatch, DataFusionError> {
        let fired = self
            .store
            .as_mut()
            .expect("window-rank rocksdb store")
            .take_closed(watermark)?;
        let mut rows: Vec<JoinRow> = Vec::new();
        let mut ranks: Vec<i64> = Vec::new();
        for group in fired {
            for (rank, row) in group.rows.into_iter().enumerate() {
                rows.push(row);
                ranks.push(rank as i64 + 1);
            }
        }
        Ok(self.emit(rows, ranks))
    }

    fn emit(&self, rows: Vec<JoinRow>, ranks: Vec<i64>) -> RecordBatch {
        let schema = match &self.schema {
            Some(schema) => schema.clone(),
            None => return RecordBatch::new_empty(Arc::new(Schema::empty())),
        };
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| {
                scalars_to_array(
                    rows.iter().map(|r| r[j].clone()).collect(),
                    fields[j].data_type(),
                )
            })
            .collect();
        if self.output_rank_number {
            fields.push(Field::new("w0$o0", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(ranks)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("window-rank output batch")
    }

    fn snapshot_parts(&self, batch: Option<RecordBatch>) -> Vec<u8> {
        let mut out = self.current_watermark.to_le_bytes().to_vec();
        let Some(batch) = batch else { return out };
        out.extend_from_slice(&write_ipc(&batch));
        out
    }

    fn snapshot_batch(&self) -> Option<RecordBatch> {
        if self.schema.is_none() {
            return None;
        }
        let rows: Vec<&JoinRow> = self.groups.values().flatten().collect();
        if rows.is_empty() {
            return None;
        }
        Some(self.rows_batch(&rows))
    }

    fn rows_batch(&self, rows: &[&JoinRow]) -> RecordBatch {
        let schema = self.schema.as_ref().expect("window-rank schema");
        let fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| {
                scalars_to_array(
                    rows.iter().map(|r| r[j].clone()).collect(),
                    fields[j].data_type(),
                )
            })
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("window-rank snapshot")
    }

    /// Canonical savepoint from the persistent store: every committed buffer re-partitioned by its
    /// stored key group under the memory path's raw keyed encoding (watermark plus one IPC batch
    /// per key group), so backend transitions stay byte-compatible.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn canonical_partitions(&self) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError> {
        let stored = self
            .store
            .as_ref()
            .expect("window-rank rocksdb store")
            .scan_all()?;
        let mut rows_by_group: BTreeMap<i32, Vec<JoinRow>> = BTreeMap::new();
        for group in stored {
            rows_by_group
                .entry(group.key_group)
                .or_default()
                .extend(group.rows);
        }
        let mut snapshots = BTreeMap::new();
        for (key_group, rows) in rows_by_group {
            let rows: Vec<&JoinRow> = rows.iter().collect();
            snapshots.insert(key_group, self.snapshot_parts(Some(self.rows_batch(&rows))));
        }
        Ok(snapshots)
    }

    fn snapshot_partitions(
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
        let mut snapshots = BTreeMap::new();
        let Some(batch) = self.snapshot_batch() else {
            return snapshots;
        };
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

    pub(crate) fn restore_partitions(
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

state_bytes_getter!(
    Java_tech_streamfusion_Native_windowRankerStateBytes,
    WindowRanker
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_windowRankerLateDrops<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let ranker = unsafe { &*(handle as *const WindowRanker) };
        ranker.late_drops as jlong
    })
}

/// [`state_bytes_getter`] for the Top-N handle, which wraps its two ranker variants in an enum.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_topNRankerStateBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let ranker = unsafe { &*(handle as *const TopNHandle) };
        (match ranker {
            TopNHandle::Append(r) => r.memory.state_bytes,
            TopNHandle::Retract(r) => r.memory.state_bytes,
            TopNHandle::UpdateFast(r) => r.memory.state_bytes,
        }) as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_topNRankerCacheSize<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let ranker = unsafe { &*(handle as *const TopNHandle) };
        ranker.cache_size() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_topNRankerStagingBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let ranker = unsafe { &*(handle as *const TopNHandle) };
        match ranker {
            TopNHandle::Append(r) => r.staging_bytes() as jlong,
            TopNHandle::Retract(r) => r.staging_bytes() as jlong,
            TopNHandle::UpdateFast(_) => 0, // no net-diff staging
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_topNRankerStagedPartitions<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let ranker = unsafe { &*(handle as *const TopNHandle) };
        match ranker {
            TopNHandle::Append(r) => r.staged_partitions() as jlong,
            TopNHandle::Retract(r) => r.staged_partitions() as jlong,
            TopNHandle::UpdateFast(_) => 0, // no net-diff staging
        }
    })
}

/// Creates a window-rank ranker (window Top-N / window deduplication) over the attached
/// window_start/window_end columns and returns an opaque handle.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createWindowRanker<'local>(
    env: JNIEnv<'local>,
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
    crate::bridge::jni_guard(env, move |mut env| {
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
    })
}

/// Buffers an input batch (no output); each window's top-N rows are emitted when the watermark
/// closes the window.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushWindowRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut WindowRanker) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            ranker.push(&batch)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Exports the top-N rows of every window the watermark has closed (with the rank number appended
/// when the host projects it).
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushWindowRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut WindowRanker) };
        match ranker.flush(watermark_millis) {
            Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Releases the window-rank ranker and its per-window state.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeWindowRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<WindowRanker>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotWindowRankerPartitions<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &*(handle as *const WindowRanker) };
        let precisions = read_i32_array(&env, &timestamp_precisions);
        keyed_state_partition_array(
            &mut env,
            ranker.snapshot_partitions(max_parallelism as usize, &precisions),
            "window-rank",
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreWindowRankerPartitions<'local>(
    env: JNIEnv<'local>,
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
    crate::bridge::jni_guard(env, move |mut env| {
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
    })
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
pub extern "system" fn Java_tech_streamfusion_Native_createTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    offset: jlong,
    limit: jlong,
    output_rank_number: jboolean,
    retracting: jboolean,
    net_diff: jboolean,
    state_ttl_millis: jlong,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
        let handle = if retracting != 0 {
            TopNHandle::Retract(
                RetractableTopNRanker::new(
                    partitions,
                    sort,
                    offset,
                    limit,
                    output_rank_number != 0,
                )
                .with_key_timestamp_precisions(timestamp_precisions)
                .with_net_diff(net_diff != 0)
                .with_state_ttl(state_ttl_millis),
            )
        } else {
            // The append-only ranker is the no-OFFSET path (offset always 0).
            TopNHandle::Append(
                TopNRanker::new(
                    partitions,
                    sort,
                    limit,
                    output_rank_number != 0,
                    net_diff != 0,
                )
                .with_key_timestamp_precisions(timestamp_precisions)
                .with_state_ttl(state_ttl_millis),
            )
        };
        boxed_or_throw(&mut env, handle.with_memory_budget(memory_budget_bytes))
    })
}

/// Folds an input batch into the per-partition top-N and exports the changelog it produces (the
/// input columns plus `$row_kind$`). `now_millis` is the host's processing-time reading — the
/// state-TTL clock.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut TopNHandle) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            ranker.push(&batch, now_millis)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Emits the append-only ranker's net changes at a Flink logical mini-batch boundary.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let ranker = unsafe { &mut *(handle as *mut TopNHandle) };
        export_record_batch(ranker.flush(), out_array_address, out_schema_address);
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotTopNRankerPartitions<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    _timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &*(handle as *const TopNHandle) };
        keyed_state_partition_array(
            &mut env,
            ranker.snapshot_partitions(max_parallelism as usize),
            "top-n",
        )
    })
}

/// Captures an immutable append-only Top-N checkpoint view. A zero return asks Java to use the
/// synchronous compatibility path (empty/uninitialized state or a non-append ranker).
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_captureTopNRankerSnapshot<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let ranker = unsafe { &*(handle as *const TopNHandle) };
        ranker
            .capture_append_snapshot(max_parallelism as usize)
            .map_or(0, |snapshot| Box::into_raw(Box::new(snapshot)) as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_topNRankerSnapshotKeyGroups<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    snapshot_handle: jlong,
) -> jni::sys::jintArray {
    crate::bridge::jni_guard(env, move |env| {
        let snapshot = unsafe { &*(snapshot_handle as *const AppendTopNSnapshot) };
        let groups: Vec<jint> = snapshot.partitions.keys().copied().collect();
        let output = env
            .new_int_array(groups.len() as i32)
            .expect("allocate top-n snapshot key groups");
        env.set_int_array_region(&output, 0, &groups)
            .expect("write top-n snapshot key groups");
        output.into_raw()
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_encodeTopNRankerSnapshotPartition<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    snapshot_handle: jlong,
    key_group: jint,
) -> jbyteArray {
    crate::bridge::jni_guard(env, move |env| {
        let snapshot = unsafe { &*(snapshot_handle as *const AppendTopNSnapshot) };
        let payload = snapshot
            .encode(key_group)
            .unwrap_or_else(|| panic!("top-n snapshot has no key group {key_group}"));
        env.byte_array_from_slice(&payload)
            .expect("allocate encoded top-n snapshot partition")
            .into_raw()
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeTopNRankerSnapshot<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    snapshot_handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<AppendTopNSnapshot>(snapshot_handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreTopNRankerPartitions<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    offset: jlong,
    limit: jlong,
    output_rank_number: jboolean,
    retracting: jboolean,
    net_diff: jboolean,
    state_ttl_millis: jlong,
    now_millis: jlong,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
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
            timestamp_precisions,
            sort,
            offset,
            limit,
            output_rank_number != 0,
            retracting != 0,
            net_diff != 0,
            state_ttl_millis,
            &restored,
            now_millis,
        )
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, ranker)
    })
}

/// Creates an update-fast streaming Top-N ranker (Flink's `UpdatableTopNFunction` /
/// `FastTop1Function` shape: unique-keyed changelog without retractions, monotonic sort key) and
/// returns an opaque handle served by the shared Top-N push/flush/snapshot/close entry points.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createUpdateFastTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    row_key_columns: JIntArray<'local>,
    row_key_timestamp_precisions: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    limit: jlong,
    output_rank_number: jboolean,
    generate_update_before: jboolean,
    state_ttl_millis: jlong,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let partition_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let row_keys = read_columns(&env, &row_key_columns);
        let row_key_precisions = read_i32_array(&env, &row_key_timestamp_precisions);
        let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
        let handle = TopNHandle::UpdateFast(
            UpdatableTopNRanker::new(
                partitions,
                partition_precisions,
                row_keys,
                row_key_precisions,
                sort,
                limit,
                output_rank_number != 0,
                generate_update_before != 0,
            )
            .with_state_ttl(state_ttl_millis),
        );
        boxed_or_throw(&mut env, handle.with_memory_budget(memory_budget_bytes))
    })
}

/// Rebuilds an update-fast Top-N ranker from raw keyed-state partition blobs.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreUpdateFastTopNRankerPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    row_key_columns: JIntArray<'local>,
    row_key_timestamp_precisions: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    limit: jlong,
    output_rank_number: jboolean,
    generate_update_before: jboolean,
    state_ttl_millis: jlong,
    now_millis: jlong,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let partition_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let row_keys = read_columns(&env, &row_key_columns);
        let row_key_precisions = read_i32_array(&env, &row_key_timestamp_precisions);
        let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
        let count = env
            .get_array_length(&snapshots)
            .expect("read update-fast top-n raw partition count");
        let mut restored = Vec::with_capacity(count as usize);
        for index in 0..count {
            let bytes = JByteArray::from(
                env.get_object_array_element(&snapshots, index)
                    .expect("read update-fast top-n raw partition"),
            );
            restored.push(
                env.convert_byte_array(&bytes)
                    .expect("read update-fast top-n raw partition bytes"),
            );
        }
        let ranker = TopNHandle::UpdateFast(
            UpdatableTopNRanker::restore_partitions(
                partitions,
                partition_precisions,
                row_keys,
                row_key_precisions,
                sort,
                limit,
                output_rank_number != 0,
                generate_update_before != 0,
                &restored,
                now_millis,
            )
            .with_state_ttl(state_ttl_millis),
        )
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, ranker)
    })
}

/// Releases a Top-N ranker handle.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<TopNHandle>(handle));
    })
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
            vec![SortColumn {
                index: 1,
                ascending: true,
                nulls_first: false,
            }],
            0,
            2,
            false,
        )
        .with_net_diff(true);

        assert_eq!(
            ranker
                .push(&changelog(&[10, 20, 30], &[0, 0, 0]), 0)
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(ranker.flush_net_diff().num_rows(), 2);

        assert_eq!(
            ranker.push(&changelog(&[10], &[3]), 0).unwrap().num_rows(),
            0
        );
        assert_eq!(
            ranker.push(&changelog(&[5], &[0]), 0).unwrap().num_rows(),
            0
        );
        assert_eq!(ranker.staged_partitions(), 1);
        let out = ranker.flush_net_diff();
        let values = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let kinds = out.column(2).as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(values.values(), &[10, 5]);
        assert_eq!(kinds.values(), &[3, 0]);
        assert_eq!(ranker.staged_partitions(), 0);
    }
}
