use crate::*;

/// Append-only keep-first deduplication on a rowtime order — Flink's
/// `RowTimeDeduplicateKeepFirstRowFunction`. Per partition key it keeps the row with the minimum
/// rowtime and emits it exactly once, when a watermark reaches that rowtime; every later row for the
/// key is then ignored, and a row arriving with a rowtime already below the watermark is dropped as
/// late. Insert-only: once a key's candidate fires, no smaller-rowtime row can still arrive (it would
/// be late), so the emitted row is final and never retracted.
///
/// Columnar: the per-key candidates live as a single Arrow batch — one row per pending key — and row
/// data moves only through `filter`/`take`/`concat` kernels, never materialized into scalars. Each
/// batch is reduced to its per-key minimum-rowtime row and merged with the standing candidates; only
/// the key (for grouping) and the rowtime (i64) are read per row, as any keyed reduction must.
pub(crate) struct KeepFirstDeduplicator {
    partition_columns: Vec<usize>,
    key_timestamp_precisions: Vec<i32>,
    rt_column: usize,
    current_watermark: i64,
    /// One row per pending key — that key's minimum-rowtime candidate — awaiting its release.
    pending: Option<RecordBatch>,
    /// Keys whose first row has already been emitted, as arrow-row bytes probed by borrowed slice
    /// (the steady-state row — a key already emitted — allocates nothing); later rows are ignored.
    emitted: HashSet<ByteKey>,
    key_converter: Option<RowConverter>,
    key_types: Vec<DataType>,
    schema: Option<SchemaRef>,
    snapshot_cache: Option<DedupSnapshotCache>,
    memory: OperatorMemory,
}

impl KeepFirstDeduplicator {
    pub(crate) fn new(partition_columns: Vec<usize>, rt_column: usize) -> Self {
        let key_arity = partition_columns.len();
        KeepFirstDeduplicator {
            partition_columns,
            key_timestamp_precisions: vec![-1; key_arity],
            rt_column,
            current_watermark: i64::MIN,
            pending: None,
            emitted: HashSet::default(),
            key_converter: None,
            key_types: Vec::new(),
            schema: None,
            snapshot_cache: None,
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds this deduplicator's state (the pending batch plus the emitted-key set) by the
    /// operator's managed-memory budget (negative = unaccounted).
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state = self.pending.as_ref().map_or(0, |b| b.get_array_memory_size())
            + self.emitted.iter().map(|k| byte_key_bytes(&k.0)).sum::<usize>();
        self.memory.attach("keep-first-deduplicate", budget_bytes, state)?;
        Ok(self)
    }

    fn with_key_timestamp_precisions(mut self, key_timestamp_precisions: Vec<i32>) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        self
    }

    pub(crate) fn push(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        self.snapshot_cache = None;
        let schema = batch.schema();
        self.schema = Some(schema.clone());
        // Drop late rows (rowtime already below the watermark) with a columnar filter.
        let rt = rt_to_millis(batch.column(self.rt_column));
        let live_mask: BooleanArray =
            rt.iter().map(|v| Some(v.unwrap() >= self.current_watermark)).collect();
        let live = filter_record_batch(batch, &live_mask).expect("dedup late filter");
        // Merge with the standing candidates and reduce to one minimum-rowtime row per pending key.
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let combined = match self.pending.take() {
            Some(prev) => {
                if track {
                    delta -= prev.get_array_memory_size() as isize;
                }
                concat_batches(&schema, [&prev, &live]).expect("dedup concat")
            }
            None => live,
        };
        let reduced = self.min_per_key(&combined);
        if track && reduced.num_rows() > 0 {
            delta += reduced.get_array_memory_size() as isize;
        }
        self.pending = (reduced.num_rows() > 0).then_some(reduced);
        self.memory.record(delta);
        self.memory.account()
    }

    /// Reduces a batch to one row per non-emitted key: the row with the minimum rowtime, ties going to
    /// the earlier position (candidates precede new rows in `combined`, so a tie keeps the incumbent —
    /// Flink's keep-first rule of replacing only on a strictly smaller rowtime). The winning rows are
    /// gathered with `take`; the row data is never materialized into scalars.
    fn min_per_key(&mut self, batch: &RecordBatch) -> RecordBatch {
        let key_arrays: Vec<&ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i)).collect();
        self.key_types = key_types(&key_arrays);
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
        let rt = rt_to_millis(batch.column(self.rt_column));
        // Both maps probe by borrowed key bytes: the per-batch reduction borrows straight from the
        // encoded batch, and the emitted-set probe (the steady-state path — every row of an
        // already-fired key) allocates nothing.
        let mut best: HashMap<&[u8], (i64, u32)> = HashMap::default();
        for row in 0..batch.num_rows() {
            let key = keys_encoded.row(row).data();
            if self.emitted.contains(key) {
                continue; // this key's first row already emitted
            }
            let rowtime = rt.value(row);
            match best.get(key) {
                Some((existing, _)) if *existing <= rowtime => {}
                _ => {
                    best.insert(key, (rowtime, row as u32));
                }
            }
        }
        let mut indices: Vec<u32> = best.into_values().map(|(_, idx)| idx).collect();
        indices.sort_unstable();
        let idx = UInt32Array::from(indices);
        let columns: Vec<ArrayRef> =
            batch.columns().iter().map(|c| take(c, &idx, None).expect("dedup take")).collect();
        RecordBatch::try_new(batch.schema(), columns).expect("dedup compacted batch")
    }

    /// Emits each pending key's candidate whose rowtime the watermark has now reached (insert-only),
    /// records those keys as emitted, and keeps the rest. Both partitions are columnar filters.
    pub(crate) fn flush(&mut self, watermark: i64) -> Result<RecordBatch, DataFusionError> {
        self.snapshot_cache = None;
        self.current_watermark = watermark;
        let Some(pending) = self.pending.take() else {
            return Ok(self.empty());
        };
        let track = self.memory.tracking();
        let mut delta = 0isize;
        if track {
            delta -= pending.get_array_memory_size() as isize;
        }
        let rt = rt_to_millis(pending.column(self.rt_column));
        let ready_mask: BooleanArray = rt.iter().map(|v| Some(v.unwrap() <= watermark)).collect();
        let ready = filter_record_batch(&pending, &ready_mask).expect("dedup ready filter");
        let not_ready =
            filter_record_batch(&pending, &arrow::compute::not(&ready_mask).expect("dedup not"))
                .expect("dedup keep filter");
        if track && not_ready.num_rows() > 0 {
            delta += not_ready.get_array_memory_size() as isize;
        }
        self.pending = (not_ready.num_rows() > 0).then_some(not_ready);
        if ready.num_rows() > 0 {
            let key_arrays: Vec<&ArrayRef> =
                self.partition_columns.iter().map(|&i| ready.column(i)).collect();
            self.key_types = key_types(&key_arrays);
            let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, ready.num_rows());
            for row in 0..ready.num_rows() {
                let key = keys_encoded.row(row).data();
                // The emitted-key set grows for the operator's lifetime, so a flush can grow state.
                if !self.emitted.contains(key) {
                    self.emitted.insert(ByteKey::from(key));
                    if track {
                        delta += byte_key_bytes(key) as isize;
                    }
                }
            }
        }
        self.memory.record(delta);
        self.memory.account()?;
        Ok(ready)
    }

    fn empty(&self) -> RecordBatch {
        match &self.schema {
            Some(schema) => RecordBatch::new_empty(schema.clone()),
            None => RecordBatch::new_empty(Arc::new(Schema::empty())),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        self.snapshot_parts(self.pending.clone(), self.emitted_batch())
    }

    fn snapshot_parts(
        &self,
        pending_batch: Option<RecordBatch>,
        emitted_batch: Option<RecordBatch>,
    ) -> Vec<u8> {
        let mut out = self.current_watermark.to_le_bytes().to_vec();
        let pending = pending_batch.map(|batch| write_ipc(&batch)).unwrap_or_default();
        out.extend_from_slice(&(pending.len() as u32).to_le_bytes());
        out.extend_from_slice(&pending);
        out.extend_from_slice(
            &emitted_batch.map(|batch| write_ipc(&batch)).unwrap_or_default(),
        );
        out
    }

    /// The emitted keys as an IPC batch of just the key columns (decoded from the stored key bytes).
    fn emitted_batch(&self) -> Option<RecordBatch> {
        if self.emitted.is_empty() {
            return None;
        }
        let keys: Vec<&[u8]> = self.emitted.iter().map(|k| k.0.as_ref()).collect();
        let fields = key_fields(&self.key_types);
        let columns = decode_byte_keys(self.key_converter.as_ref(), &keys, &self.key_types);
        Some(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("dedup emitted"))
    }

    fn snapshot_partitions(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        self.materialize_raw_keyed_snapshots(max_parallelism, timestamp_precisions);
        self.snapshot_cache
            .take()
            .expect("dedup raw snapshot cache")
            .snapshots
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
        let pending = self.pending.clone();
        let emitted = self.emitted_batch();
        let mut pending_by_group: BTreeMap<i32, Vec<u32>> = BTreeMap::new();
        let mut emitted_by_group: BTreeMap<i32, Vec<u32>> = BTreeMap::new();
        if let Some(batch) = &pending {
            for row in 0..batch.num_rows() {
                let key_group = flink_key_group(
                    binary_row_hash(
                        batch,
                        &self.partition_columns,
                        row,
                        timestamp_precisions,
                    ),
                    max_parallelism,
                ) as i32;
                pending_by_group.entry(key_group).or_default().push(row as u32);
            }
        }
        if let Some(batch) = &emitted {
            let key_columns: Vec<usize> = (0..batch.num_columns()).collect();
            for row in 0..batch.num_rows() {
                let key_group = flink_key_group(
                    binary_row_hash(batch, &key_columns, row, timestamp_precisions),
                    max_parallelism,
                ) as i32;
                emitted_by_group.entry(key_group).or_default().push(row as u32);
            }
        }
        let mut groups: Vec<i32> = pending_by_group
            .keys()
            .chain(emitted_by_group.keys())
            .copied()
            .collect();
        groups.sort_unstable();
        groups.dedup();
        let mut snapshots = BTreeMap::new();
        for key_group in groups {
            let subset = |batch: &RecordBatch, rows: &[u32]| {
                let indices = UInt32Array::from(rows.to_vec());
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| take(column, &indices, None).expect("partition dedup snapshot"))
                    .collect();
                RecordBatch::try_new(batch.schema(), columns).expect("partitioned dedup snapshot")
            };
            let pending_part = pending_by_group
                .get(&key_group)
                .map(|rows| subset(pending.as_ref().expect("pending rows have a batch"), rows));
            let emitted_part = emitted_by_group
                .get(&key_group)
                .map(|rows| subset(emitted.as_ref().expect("emitted rows have a batch"), rows));
            snapshots.insert(key_group, self.snapshot_parts(pending_part, emitted_part));
        }
        self.snapshot_cache = Some(DedupSnapshotCache {
            max_parallelism,
            timestamp_precisions: timestamp_precisions.to_vec(),
            snapshots,
        });
    }

    fn restore(partition_columns: Vec<usize>, rt_column: usize, bytes: &[u8]) -> Self {
        let mut dedup = KeepFirstDeduplicator::new(partition_columns, rt_column);
        if bytes.len() < 8 {
            return dedup;
        }
        dedup.current_watermark = i64::from_le_bytes(bytes[0..8].try_into().expect("watermark"));
        let pending_len = u32::from_le_bytes(bytes[8..12].try_into().expect("pending len")) as usize;
        for batch in read_ipc_if_present(&bytes[12..12 + pending_len]) {
            dedup.schema = Some(batch.schema());
            dedup.pending = Some(batch);
        }
        for batch in read_ipc_if_present(&bytes[12 + pending_len..]) {
            let key_arrays: Vec<&ArrayRef> = (0..batch.num_columns()).map(|i| batch.column(i)).collect();
            dedup.key_types = key_types(&key_arrays);
            let keys_encoded = encode_keys(&mut dedup.key_converter, &key_arrays, batch.num_rows());
            for row in 0..batch.num_rows() {
                dedup.emitted.insert(ByteKey::from(keys_encoded.row(row).data()));
            }
        }
        dedup
    }

    fn restore_partitions(
        partition_columns: Vec<usize>,
        rt_column: usize,
        snapshots: &[Vec<u8>],
    ) -> Self {
        let mut watermark = i64::MIN;
        let mut pending = Vec::new();
        let mut emitted = Vec::new();
        for bytes in snapshots {
            if bytes.len() < 12 {
                continue;
            }
            watermark = watermark.max(i64::from_le_bytes(bytes[0..8].try_into().expect("dedup watermark")));
            let pending_len =
                u32::from_le_bytes(bytes[8..12].try_into().expect("dedup pending len")) as usize;
            assert!(12 + pending_len <= bytes.len(), "truncated dedup raw key-group snapshot");
            pending.extend(read_ipc_if_present(&bytes[12..12 + pending_len]));
            emitted.extend(read_ipc_if_present(&bytes[12 + pending_len..]));
        }
        // Arrow rows carry the RowConverter that created them.  Coalesce every raw-key-group IPC
        // payload before one normal restore so pending keys and emitted-key bytes share a converter.
        let merge = |batches: Vec<RecordBatch>| {
            batches.first().map(|first| {
                write_ipc(
                    &concat_batches(&first.schema(), batches.iter())
                        .expect("merge dedup raw partitions"),
                )
            })
        };
        let pending = merge(pending).unwrap_or_default();
        let emitted = merge(emitted).unwrap_or_default();
        let mut bytes = watermark.to_le_bytes().to_vec();
        bytes.extend_from_slice(&(pending.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&pending);
        bytes.extend_from_slice(&emitted);
        KeepFirstDeduplicator::restore(partition_columns, rt_column, &bytes)
    }
}

/// Eager (push→emit, no watermark buffering) deduplication keyed by a partition key. Serves three of
/// the four dedup variants — the watermark-buffered event-time keep-first lives in
/// {@link KeepFirstDeduplicator}:
///   * **rowtime keep-last** — Flink's `RowTimeDeduplicateFunction`: keep the **maximum**-rowtime row;
///     the first row emits `+I`, a later row (rowtime `>=` the stored one) emits `-U`(previous, gated
///     on `generate_update_before`)/`+U`(new), and a smaller-rowtime row is ignored.
///   * **proctime keep-last** — Flink's `ProcTimeDeduplicateKeepLastRowFunction`: the same, but in
///     arrival order, so every later row replaces (no rowtime read or comparison).
///   * **proctime keep-first** — Flink's `ProcTimeDeduplicateKeepFirstRowFunction`: the first row per
///     key emits `+I` and every later row is dropped; insert-only output (no `$row_kind$`).
/// Insert-only input. The stored full row per key lives as scalars and is rebuilt with
/// `scalars_to_array` on emit, like the changelog normalizer below.
pub(crate) struct KeepLastDeduplicator<S: KeyedStateStore<DedupRow> = MemoryDedupStore> {
    partition_columns: Vec<usize>,
    key_timestamp_precisions: Vec<i32>,
    rt_column: usize,
    generate_update_before: bool,
    /// Whether the order is a rowtime (read + compared) or proctime (arrival order; rt ignored).
    rowtime_ordered: bool,
    /// Keep-first (insert-only, first row wins) vs keep-last (retract changelog, latest row wins).
    keep_first: bool,
    schema: Option<SchemaRef>,
    /// arrow-row encoder for the value-encoded full row, built once from the first batch.
    payload_converter: Option<RowConverter>,
    /// Per key: the stored row's rowtime (millis, 0 in proctime) and its full row as arrow-row bytes.
    /// The key is the partition key's Flink BinaryRow bytes — the same encoding every keyed store
    /// speaks (its hash IS the Flink key group, which the persistent backend's bucket layout relies
    /// on) — probed borrowed so the steady state (key already stored) allocates nothing. The payload
    /// is an `Arc<[u8]>`: a new row is copied once into state, emitting it (and retracting the
    /// previous row) just bumps the refcount — the `-U` moves the replaced payload out of the map,
    /// never re-copying it.
    rows: S,
    mini_batch: bool,
    staged: Vec<DedupStagedChange>,
    staged_bytes: usize,
    snapshot_cache: Option<DedupSnapshotCache>,
    pub(crate) memory: OperatorMemory,
}

/// The resident default backend for the dedup store (see `state/` for the seam).
pub(crate) type MemoryDedupStore = MemoryStateStore<DedupRow>;

pub(crate) struct DedupRow {
    rowtime: i64,
    payload: Arc<[u8]>,
    staged: bool,
}

struct DedupStagedChange {
    key: ByteKey,
    before: Option<Arc<[u8]>>,
}

struct DedupSnapshotCache {
    max_parallelism: usize,
    timestamp_precisions: Vec<i32>,
    snapshots: BTreeMap<i32, Vec<u8>>,
}

/// Estimated footprint of one stored last-row entry (encoded key + payload + map entry).
pub(crate) fn dedup_entry_bytes(key: &[u8], payload: &[u8]) -> usize {
    key.len() + payload.len() + GROUP_ENTRY_OVERHEAD
}

/// The dedup persistent backend: the generic Paimon store under the dedup row codec.
#[cfg(feature = "paimon-state")]
pub(crate) type PaimonDedupStore = crate::state::PaimonStore<DedupStateCodec>;

/// The dedup value codec for the Paimon store: a row-payload codec (see `RowPayloadCodec`), with
/// the rowtime re-derived from the row's own rowtime column on hydration, exactly as restore does.
#[cfg(feature = "paimon-state")]
pub(crate) struct DedupStateCodec {
    row: crate::state::RowPayloadCodec,
    rt_column: usize,
    rowtime_ordered: bool,
}

#[cfg(feature = "paimon-state")]
impl DedupStateCodec {
    pub(crate) fn new(row_types: Vec<DataType>, rt_column: usize, rowtime_ordered: bool) -> Self {
        DedupStateCodec {
            row: crate::state::RowPayloadCodec::new(row_types),
            rt_column,
            rowtime_ordered,
        }
    }
}

#[cfg(feature = "paimon-state")]
impl crate::state::PaimonStateCodec for DedupStateCodec {
    type Value = DedupRow;

    fn supported(&self) -> bool {
        self.row.supported()
    }

    fn value_fields(&self) -> Vec<(String, DataType)> {
        self.row.fields()
    }

    fn encode(&self, row: &DedupRow) -> Vec<ScalarValue> {
        self.row.encode_payload(&row.payload)
    }

    fn decode(&self, scalars: &[ScalarValue]) -> DedupRow {
        let (payload, columns) = self.row.decode_payload(scalars);
        let rowtime = if self.rowtime_ordered {
            rt_to_millis(&columns[self.rt_column]).value(0)
        } else {
            0
        };
        DedupRow { rowtime, payload, staged: false }
    }

    fn value_bytes(&self, row: &DedupRow) -> usize {
        row.payload.len()
    }
}

impl KeepLastDeduplicator {
    pub(crate) fn new(
        partition_columns: Vec<usize>,
        rt_column: usize,
        generate_update_before: bool,
        rowtime_ordered: bool,
        keep_first: bool,
    ) -> Self {
        let key_arity = partition_columns.len();
        KeepLastDeduplicator {
            partition_columns,
            key_timestamp_precisions: vec![-1; key_arity],
            rt_column,
            generate_update_before,
            rowtime_ordered,
            keep_first,
            schema: None,
            payload_converter: None,
            rows: MemoryDedupStore::default(),
            mini_batch: false,
            staged: Vec::new(),
            staged_bytes: 0,
            snapshot_cache: None,
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds this deduplicator's stored rows by the operator's managed-memory budget (negative =
    /// unaccounted), accounting any restored rows immediately.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize =
            self.rows.iter().map(|(key, row)| dedup_entry_bytes(&key.0, &row.payload)).sum();
        self.memory.attach("deduplicate", budget_bytes, state)?;
        Ok(self)
    }
}

impl<S: KeyedStateStore<DedupRow>> KeepLastDeduplicator<S> {
    /// Moves this freshly built (empty, memory-backed) deduplicator's configuration onto another
    /// state backend; construction goes through `new` + builders first so backend choice stays
    /// orthogonal to the shape builders.
    pub(crate) fn with_backend<T: KeyedStateStore<DedupRow>>(
        self,
        rows: T,
    ) -> KeepLastDeduplicator<T> {
        KeepLastDeduplicator {
            partition_columns: self.partition_columns,
            key_timestamp_precisions: self.key_timestamp_precisions,
            rt_column: self.rt_column,
            generate_update_before: self.generate_update_before,
            rowtime_ordered: self.rowtime_ordered,
            keep_first: self.keep_first,
            schema: self.schema,
            payload_converter: self.payload_converter,
            rows,
            mini_batch: self.mini_batch,
            staged: self.staged,
            staged_bytes: self.staged_bytes,
            snapshot_cache: None,
            memory: self.memory,
        }
    }

    /// Attaches the managed-memory budget for a backend that starts with nothing resident (a
    /// read-through store hydrates on demand; there is no restored map to pre-account).
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory.attach("deduplicate", budget_bytes, 0)?;
        Ok(self)
    }

    /// The backing store, for backend-specific control paths (checkpointing a persistent store).
    pub(crate) fn store_mut(&mut self) -> &mut S {
        &mut self.rows
    }

    pub(crate) fn with_mini_batch(mut self, mini_batch: bool) -> Self {
        self.mini_batch = mini_batch && !self.keep_first;
        self
    }

    pub(crate) fn with_key_timestamp_precisions(
        mut self,
        key_timestamp_precisions: Vec<i32>,
    ) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        self
    }

    pub(crate) fn staging_bytes(&self) -> usize {
        self.staged_bytes
    }

    pub(crate) fn staged_keys(&self) -> usize {
        self.staged.len()
    }

    /// Builds the full-row arrow-row converter from a batch's column types, once.
    fn ensure_converters(&mut self, batch: &RecordBatch, arity: usize) {
        if self.payload_converter.is_some() {
            return;
        }
        self.payload_converter = Some(
            RowConverter::new(
                (0..arity).map(|i| SortField::new(batch.column(i).data_type().clone())).collect(),
            )
            .expect("dedup payload converter"),
        );
    }

    pub(crate) fn push(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        self.snapshot_cache = None;
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        self.ensure_converters(batch, arity);
        self.rows
            .begin_batch(batch, &self.partition_columns, &self.key_timestamp_precisions)?;
        let mut parts =
            BinaryRowBatchEncoder::new(batch, &self.partition_columns, &self.key_timestamp_precisions);
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let payloads =
            self.payload_converter.as_ref().unwrap().convert_columns(&data_arrays).expect("encode dedup payload");
        // The rowtime is read only for a rowtime order; proctime dedup uses arrival order.
        let rt = self.rowtime_ordered.then(|| rt_to_millis(batch.column(self.rt_column)));

        let keep_first = self.keep_first;
        let rowtime_ordered = self.rowtime_ordered;
        let generate_update_before = self.generate_update_before;
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let rows = &mut self.rows;
        let mut out_rows: Vec<Arc<[u8]>> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        for row in 0..batch.num_rows() {
            // Borrowed probe: the key bytes are copied into the map only when a key first appears,
            // and a dropped/ignored row allocates nothing at all.
            let key = parts.encode(row);
            // keep-first: the first row per key wins, later rows are dropped (insert-only).
            if keep_first {
                if rows.contains(key) {
                    continue;
                }
                let payload: Arc<[u8]> = Arc::from(payloads.row(row).data());
                if track {
                    delta += dedup_entry_bytes(key, &payload) as isize;
                }
                out_rows.push(payload.clone());
                out_kinds.push(0); // +I — first row for the key
                rows.insert(ByteKey::from(key), DedupRow { rowtime: 0, payload, staged: false });
                continue;
            }
            let rowtime = rt.as_ref().map_or(0, |rt| rt.value(row));
            match rows.get_mut(key) {
                None => {
                    let payload: Arc<[u8]> = Arc::from(payloads.row(row).data());
                    if track {
                        delta += dedup_entry_bytes(key, &payload) as isize;
                    }
                    let staged = self.mini_batch;
                    let owned = ByteKey::from(key);
                    if staged {
                        let retained = byte_key_bytes(key);
                        self.staged.push(DedupStagedChange { key: owned.clone(), before: None });
                        self.staged_bytes += retained;
                        delta += retained as isize;
                    } else {
                        out_rows.push(payload.clone());
                        out_kinds.push(0); // +I — first row for the key
                    }
                    rows.insert(owned, DedupRow { rowtime, payload, staged });
                }
                // A rowtime order ignores an older (smaller-rowtime) row; proctime always replaces.
                Some(stored) if rowtime_ordered && rowtime < stored.rowtime => {
                    continue;
                }
                Some(stored) => {
                    let payload: Arc<[u8]> = Arc::from(payloads.row(row).data());
                    if track {
                        // Same key: only the payload is replaced.
                        delta += payload.len() as isize - stored.payload.len() as isize;
                    }
                    if self.mini_batch {
                        if !stored.staged {
                            let before = stored.payload.clone();
                            let retained = byte_key_bytes(key) + before.len();
                            self.staged.push(DedupStagedChange {
                                key: ByteKey::from(key),
                                before: Some(before),
                            });
                            self.staged_bytes += retained;
                            delta += retained as isize;
                            stored.staged = true;
                        }
                    } else {
                        if generate_update_before {
                            out_rows.push(stored.payload.clone());
                            out_kinds.push(1); // -U the previous row
                        }
                        out_rows.push(payload.clone());
                        out_kinds.push(2); // +U the new (later) row
                    }
                    stored.rowtime = rowtime;
                    stored.payload = payload;
                }
            }
        }
        // A mini-batch bundle spans pushes: hydrated keys stay resident until the flush ends the
        // bundle, so the staged re-probes there stay truthful.
        if !self.mini_batch {
            self.rows.end_bundle()?;
        }
        self.memory.record(delta + self.rows.footprint_delta());
        self.memory.account()?;
        Ok(self.emit(out_rows, out_kinds))
    }

    pub(crate) fn flush_mini_batch(&mut self) -> Result<RecordBatch, DataFusionError> {
        if !self.mini_batch {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        let changes = std::mem::take(&mut self.staged);
        let mut out_rows = Vec::with_capacity(changes.len() * 2);
        let mut out_kinds = Vec::with_capacity(changes.len() * 2);
        for DedupStagedChange { key, before } in changes {
            let row = self.rows.get_mut(&key.0).expect("staged dedup key remains in state");
            row.staged = false;
            let after = row.payload.clone();
            if before.as_ref() == Some(&after) {
                continue;
            }
            if let Some(before) = before {
                if self.generate_update_before {
                    out_rows.push(before);
                    out_kinds.push(1);
                }
                out_rows.push(after);
                out_kinds.push(2);
            } else {
                out_rows.push(after);
                out_kinds.push(0);
            }
        }
        self.rows.end_bundle()?;
        self.memory
            .record(self.rows.footprint_delta() - self.staged_bytes as isize);
        self.staged_bytes = 0;
        self.memory.account()?;
        Ok(self.emit(out_rows, out_kinds))
    }

    fn emit(&self, out_rows: Vec<Arc<[u8]>>, out_kinds: Vec<i8>) -> RecordBatch {
        if out_rows.is_empty() {
            return RecordBatch::new_empty(Arc::new(Schema::empty()));
        }
        let schema = self.schema.as_ref().expect("schema set once a row was processed");
        let conv = self.payload_converter.as_ref().expect("converter set");
        // One vectorized row->columnar pass rebuilds every data column (cf. the per-cell scalar build).
        let parser = conv.parser();
        let mut columns: Vec<ArrayRef> =
            conv.convert_rows(out_rows.iter().map(|r| parser.parse(r))).expect("decode dedup payloads");
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        // Keep-first is insert-only (every emitted row is a +I), so it carries no $row_kind$ column;
        // keep-last emits a changelog and tags each row's kind.
        if !self.keep_first {
            fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
            columns.push(Arc::new(Int8Array::from(out_kinds)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build keep-last dedup batch")
    }
}

/// The raw keyed-state snapshot/restore surface exists only on the memory backend — a persistent
/// store checkpoints through its own commit path instead of materializing the key space.
impl KeepLastDeduplicator {
    /// Serializes the stored last-row-per-key set; the rowtime is re-derived from each row on restore.
    fn snapshot(&self) -> Vec<u8> {
        self.snapshot_batch()
            .map(|batch| write_ipc(&batch))
            .unwrap_or_default()
    }

    fn snapshot_batch(&self) -> Option<RecordBatch> {
        let Some(schema) = &self.schema else { return None };
        let Some(conv) = &self.payload_converter else { return None };
        if self.rows.iter().next().is_none() {
            return None;
        }
        let parser = conv.parser();
        let columns = conv
            .convert_rows(self.rows.iter().map(|(_, row)| parser.parse(&row.payload)))
            .expect("decode dedup snapshot payloads");
        Some(RecordBatch::try_new(schema.clone(), columns).expect("keep-last snapshot"))
    }

    fn snapshot_partitions(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        self.materialize_raw_keyed_snapshots(max_parallelism, timestamp_precisions);
        self.snapshot_cache
            .take()
            .expect("dedup raw snapshot cache")
            .snapshots
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
        let mut snapshots = BTreeMap::new();
        if let Some(batch) = self.snapshot_batch() {
            let mut rows_by_group: BTreeMap<i32, Vec<u32>> = BTreeMap::new();
            for row in 0..batch.num_rows() {
                let key_group = flink_key_group(
                    binary_row_hash(
                        &batch,
                        &self.partition_columns,
                        row,
                        timestamp_precisions,
                    ),
                    max_parallelism,
                ) as i32;
                rows_by_group.entry(key_group).or_default().push(row as u32);
            }
            for (key_group, rows) in rows_by_group {
                let indices = UInt32Array::from(rows);
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| take(column, &indices, None).expect("partition dedup snapshot"))
                    .collect();
                let partition = RecordBatch::try_new(batch.schema(), columns)
                    .expect("partitioned dedup snapshot");
                snapshots.insert(key_group, write_ipc(&partition));
            }
        }
        self.snapshot_cache = Some(DedupSnapshotCache {
            max_parallelism,
            timestamp_precisions: timestamp_precisions.to_vec(),
            snapshots,
        });
    }

    fn restore(
        partition_columns: Vec<usize>,
        key_timestamp_precisions: Vec<i32>,
        rt_column: usize,
        generate_update_before: bool,
        rowtime_ordered: bool,
        keep_first: bool,
        bytes: &[u8],
    ) -> Self {
        let mut dedup = KeepLastDeduplicator::new(
            partition_columns,
            rt_column,
            generate_update_before,
            rowtime_ordered,
            keep_first,
        )
        .with_key_timestamp_precisions(key_timestamp_precisions);
        for batch in read_ipc_if_present(bytes) {
            let arity = batch.num_columns();
            dedup.schema = Some(batch.schema());
            dedup.ensure_converters(&batch, arity);
            // The stored rowtime matters only to the rowtime keep-last comparison; proctime stores 0.
            let rt = rowtime_ordered.then(|| rt_to_millis(batch.column(dedup.rt_column)));
            let mut parts = BinaryRowBatchEncoder::new(
                &batch,
                &dedup.partition_columns,
                &dedup.key_timestamp_precisions,
            );
            let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
            let payloads =
                dedup.payload_converter.as_ref().unwrap().convert_columns(&data_arrays).expect("encode payload");
            let rows = &mut dedup.rows;
            for row in 0..batch.num_rows() {
                rows.insert(
                    ByteKey::from(parts.encode(row)),
                    DedupRow {
                        rowtime: rt.as_ref().map_or(0, |rt| rt.value(row)),
                        payload: Arc::from(payloads.row(row).data()),
                        staged: false,
                    },
                );
            }
        }
        dedup
    }

    fn restore_partitions(
        partition_columns: Vec<usize>,
        key_timestamp_precisions: Vec<i32>,
        rt_column: usize,
        generate_update_before: bool,
        rowtime_ordered: bool,
        keep_first: bool,
        snapshots: &[Vec<u8>],
    ) -> Self {
        let batches: Vec<RecordBatch> = snapshots
            .iter()
            .flat_map(|bytes| read_ipc_if_present(bytes))
            .collect();
        let snapshot = batches.first().map(|first| {
            write_ipc(
                &concat_batches(&first.schema(), batches.iter())
                    .expect("merge dedup raw partitions"),
            )
        });
        KeepLastDeduplicator::restore(
            partition_columns,
            key_timestamp_precisions,
            rt_column,
            generate_update_before,
            rowtime_ordered,
            keep_first,
            snapshot.as_deref().unwrap_or_default(),
        )
    }
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_keepFirstDeduplicatorStateBytes, KeepFirstDeduplicator);

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_keepLastDeduplicatorStateBytes, KeepLastDeduplicator);

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_keepLastDeduplicatorStagingBytes<'local>(
    _env: JNIEnv<'local>, _class: JClass<'local>, handle: jlong,
) -> jlong {
    let dedup = unsafe { &*(handle as *const KeepLastDeduplicator) };
    dedup.staged_bytes as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_keepLastDeduplicatorStagedKeys<'local>(
    _env: JNIEnv<'local>, _class: JClass<'local>, handle: jlong,
) -> jlong {
    let dedup = unsafe { &*(handle as *const KeepLastDeduplicator) };
    dedup.staged.len() as jlong
}

/// Creates a keep-first deduplicator over the given partition-key columns and rowtime column.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createKeepFirstDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let dedup = KeepFirstDeduplicator::new(partitions, rt_column as usize)
        .with_key_timestamp_precisions(timestamp_precisions)
        .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, dedup)
}

/// Buffers an input batch (no output); each key's minimum-rowtime row is emitted later, on the
/// watermark that reaches its rowtime.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushKeepFirstDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        dedup.push(&batch)
    };
    if let Err(e) = result {
        throw_memory_limit(&mut env, &e.to_string());
    }
}

/// Exports each key's first (minimum-rowtime) row whose rowtime the watermark has reached.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushKeepFirstDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
    // The emitted-key set grows here, so even a flush can exceed the budget.
    match dedup.flush(watermark_millis) {
        Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Releases the deduplicator and its per-key state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeKeepFirstDeduplicator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<KeepFirstDeduplicator>(handle));
    }
}

/// Serializes the deduplicator's pending candidates, emitted keys, and watermark for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotKeepFirstDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
    env.byte_array_from_slice(&dedup.snapshot())
        .expect("failed to allocate dedup snapshot array")
        .into_raw()
}

/// Rebuilds a keep-first deduplicator from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreKeepFirstDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    rt_column: jint,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read dedup snapshot");
    let dedup = KeepFirstDeduplicator::restore(partitions, rt_column as usize, &bytes)
        .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, dedup)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotKeepFirstDeduplicatorPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    keyed_state_partition_array(
        &mut env,
        dedup.snapshot_partitions(max_parallelism as usize, &precisions),
        "keep-first-dedup",
    )
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreKeepFirstDeduplicatorPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let count = env
        .get_array_length(&snapshots)
        .expect("read keep-first dedup raw partition count");
    let mut restored = Vec::with_capacity(count as usize);
    for index in 0..count {
        let bytes = JByteArray::from(
            env.get_object_array_element(&snapshots, index)
                .expect("read keep-first dedup raw partition"),
        );
        restored.push(
            env.convert_byte_array(&bytes)
                .expect("read keep-first dedup raw partition bytes"),
        );
    }
    let dedup = KeepFirstDeduplicator::restore_partitions(
        partitions,
        rt_column as usize,
        &restored,
    )
    .with_key_timestamp_precisions(timestamp_precisions)
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, dedup)
}

/// Creates an eager deduplicator (rowtime/proctime keep-last, or proctime keep-first) and returns an
/// opaque handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createKeepLastDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
    generate_update_before: jboolean,
    rowtime_ordered: jboolean,
    keep_first: jboolean,
    mini_batch: jboolean,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let dedup = KeepLastDeduplicator::new(
        partitions,
        rt_column as usize,
        generate_update_before != 0,
        rowtime_ordered != 0,
        keep_first != 0,
    )
    .with_mini_batch(mini_batch != 0)
    .with_key_timestamp_precisions(timestamp_precisions)
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, dedup)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushKeepLastDeduplicator<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, handle: jlong,
    out_array_address: jlong, out_schema_address: jlong,
) {
    let dedup = unsafe { &mut *(handle as *mut KeepLastDeduplicator) };
    match dedup.flush_mini_batch() {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Folds an input batch and returns the retract changelog it produces (emitted eagerly per row).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushKeepLastDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let dedup = unsafe { &mut *(handle as *mut KeepLastDeduplicator) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        dedup.push(&batch)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeKeepLastDeduplicator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<KeepLastDeduplicator>(handle));
    }
}

/// Serializes the keep-last deduplicator's per-key stored rows for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotKeepLastDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let dedup = unsafe { &mut *(handle as *mut KeepLastDeduplicator) };
    env.byte_array_from_slice(&dedup.snapshot())
        .expect("failed to allocate dedup snapshot array")
        .into_raw()
}

/// Rebuilds an eager deduplicator from a snapshot and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreKeepLastDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
    generate_update_before: jboolean,
    rowtime_ordered: jboolean,
    keep_first: jboolean,
    mini_batch: jboolean,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read dedup snapshot");
    let dedup = KeepLastDeduplicator::restore(
        partitions,
        timestamp_precisions,
        rt_column as usize,
        generate_update_before != 0,
        rowtime_ordered != 0,
        keep_first != 0,
        &bytes,
    )
    .with_mini_batch(mini_batch != 0)
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, dedup)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotKeepLastDeduplicatorPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    let dedup = unsafe { &mut *(handle as *mut KeepLastDeduplicator) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    keyed_state_partition_array(
        &mut env,
        dedup.snapshot_partitions(max_parallelism as usize, &precisions),
        "keep-last-dedup",
    )
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreKeepLastDeduplicatorPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
    generate_update_before: jboolean,
    rowtime_ordered: jboolean,
    keep_first: jboolean,
    mini_batch: jboolean,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let count = env
        .get_array_length(&snapshots)
        .expect("read keep-last dedup raw partition count");
    let mut restored = Vec::with_capacity(count as usize);
    for index in 0..count {
        let bytes = JByteArray::from(
            env.get_object_array_element(&snapshots, index)
                .expect("read keep-last dedup raw partition"),
        );
        restored.push(
            env.convert_byte_array(&bytes)
                .expect("read keep-last dedup raw partition bytes"),
        );
    }
    let dedup = KeepLastDeduplicator::restore_partitions(
        partitions,
        timestamp_precisions,
        rt_column as usize,
        generate_update_before != 0,
        rowtime_ordered != 0,
        keep_first != 0,
        &restored,
    )
    .with_mini_batch(mini_batch != 0)
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, dedup)
}
