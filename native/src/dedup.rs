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
    /// Deliberately exempt from state TTL, mirroring Flink's un-TTL'd timer state: the candidate
    /// is cleaned up by the watermark firing it, and expiring it early would lose data.
    pending: Option<RecordBatch>,
    /// Keys whose first row has already been emitted, as arrow-row bytes probed by borrowed slice
    /// (the steady-state row — a key already emitted — allocates nothing); later rows are ignored.
    /// The value is the firing's wall-clock millis (0 with TTL off) — the marker's only TTL'd
    /// write, Flink's `alreadyEmittedState.update(true)` in `onTimer`: probes never refresh it, so
    /// an emitted key expires a fixed retention after it fired and can then fire a second `+I`.
    emitted: HashMap<ByteKey, i64>,
    /// Idle-state retention millis (0 = off — Flink's default), applied to emitted markers only.
    ttl_ms: i64,
    /// When the last full marker sweep ran; the sweep reclaims markers never probed again, once
    /// per TTL period (expiry itself is enforced lazily at each probe).
    last_sweep_ms: i64,
    /// Rows dropped as late (rowtime below the watermark) over the handle's lifetime — the host
    /// feeds Flink's `numLateRecordsDropped` counter from it. Like any Flink metric it restarts
    /// at zero with the operator, so it is not checkpointed.
    pub(crate) late_drops: u64,
    key_converter: Option<RowConverter>,
    key_types: Vec<DataType>,
    schema: Option<SchemaRef>,
    snapshot_cache: Option<DedupSnapshotCache>,
    /// Persistent-state mode: the pending candidates and fired markers live in the persistent
    /// store; the in-memory batch and marker map stay empty, and the watermark firing is a range
    /// read over the pending table.
    #[cfg(feature = "rocksdb-state")]
    store: Option<crate::state::RocksKeepFirstDedupStore>,
    pub(crate) memory: OperatorMemory,
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
            emitted: HashMap::default(),
            ttl_ms: 0,
            last_sweep_ms: 0,
            late_drops: 0,
            key_converter: None,
            key_types: Vec::new(),
            schema: None,
            snapshot_cache: None,
            #[cfg(feature = "rocksdb-state")]
            store: None,
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds this deduplicator's state (the pending batch plus the emitted-key set) by the
    /// operator's task off-heap budget (negative = unaccounted).
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state = self
            .pending
            .as_ref()
            .map_or(0, |b| b.get_array_memory_size())
            + self
                .emitted
                .keys()
                .map(|k| byte_key_bytes(&k.0))
                .sum::<usize>();
        self.memory
            .attach("keep-first-deduplicate", budget_bytes, state)?;
        Ok(self)
    }

    /// Sets the idle-state retention (`table.exec.state.ttl`) in millis; 0 (Flink's default)
    /// disables expiry. Only the emitted markers expire — the pending candidates mirror Flink's
    /// deliberately un-TTL'd timer state (expiring one before its watermark would lose data).
    pub(crate) fn with_state_ttl(mut self, ttl_ms: i64) -> Self {
        self.ttl_ms = ttl_ms.max(0);
        self
    }

    /// Moves onto the persistent store, resuming the late-data watermark it persisted.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_store(mut self, store: crate::state::RocksKeepFirstDedupStore) -> Self {
        self.current_watermark = store.watermark();
        self.store = Some(store);
        self
    }

    /// Attaches the task off-heap budget for a backend that starts with nothing resident.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory
            .attach("keep-first-deduplicate", budget_bytes, 0)?;
        Ok(self)
    }

    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn store_mut(&mut self) -> &mut crate::state::RocksKeepFirstDedupStore {
        self.store.as_mut().expect("keep-first dedup rocksdb store")
    }

    /// Restore-time enable-TTL migration for the persistent path, exactly as `restore` stamps a
    /// raw snapshot: markers restored without a timestamp (a pre-TTL writer) are stamped the
    /// restore time instead of expiring on first probe.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn adopt_store_ttl(&mut self, now_ms: i64) -> Result<(), DataFusionError> {
        if self.ttl_ms > 0 {
            self.store_mut().adopt_ttl(now_ms)?;
        }
        Ok(())
    }

    /// Reclaims every marker whose TTL elapsed with no further probe — the lazy per-probe expiry
    /// never sees such a key again. Silent, like Flink's background cleanup.
    fn sweep_expired(&mut self, ttl: StateTtl) {
        let track = self.memory.tracking();
        let mut reclaimed = 0isize;
        self.emitted.retain(|key, fired_ms| {
            if ttl.expired(*fired_ms) {
                if track {
                    reclaimed += byte_key_bytes(&key.0) as isize;
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

    pub(crate) fn with_key_timestamp_precisions(
        mut self,
        key_timestamp_precisions: Vec<i32>,
    ) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        self
    }

    /// Buffers an input batch (no output); emission is watermark-driven (`flush`). `now_ms` is the
    /// host's wall-clock reading for this call (only read when state TTL is on).
    pub(crate) fn push(&mut self, batch: &RecordBatch, now_ms: i64) -> Result<(), DataFusionError> {
        self.snapshot_cache = None;
        let schema = batch.schema();
        self.schema = Some(schema.clone());
        // Drop late rows (rowtime already below the watermark) with a columnar filter, counting
        // them as Flink's processElement does before any per-key state is consulted.
        let rt = rt_to_millis(batch.column(self.rt_column));
        let live_mask: BooleanArray = rt
            .iter()
            .map(|v| Some(v.unwrap() >= self.current_watermark))
            .collect();
        let live = filter_record_batch(batch, &live_mask).expect("dedup late filter");
        self.late_drops += (batch.num_rows() - live.num_rows()) as u64;
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return self.push_store(&live, now_ms);
        }
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        // The sweep reclaims markers no later row ever probes. Once per TTL period bounds its
        // amortized cost at one map walk per period.
        if ttl.enabled() && now_ms >= self.last_sweep_ms + self.ttl_ms {
            self.sweep_expired(ttl);
            self.last_sweep_ms = now_ms;
        }
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
        let reduced = self.min_per_key(&combined, ttl);
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
    fn min_per_key(&mut self, batch: &RecordBatch, ttl: StateTtl) -> RecordBatch {
        let key_arrays: Vec<&ArrayRef> = self
            .partition_columns
            .iter()
            .map(|&i| batch.column(i))
            .collect();
        self.key_types = key_types(&key_arrays);
        let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
        let rt = rt_to_millis(batch.column(self.rt_column));
        let track = self.memory.tracking();
        let mut reclaimed = 0isize;
        // Both maps probe by borrowed key bytes: the per-batch reduction borrows straight from the
        // encoded batch, and the emitted-marker probe (the steady-state path — every row of an
        // already-fired key) allocates nothing.
        let mut best: HashMap<&[u8], (i64, u32)> = HashMap::default();
        for row in 0..batch.num_rows() {
            let key = keys_encoded.row(row).data();
            // An expired marker is deleted on read and reads as absent (Flink's
            // NeverReturnExpired): the key is fresh again — this row becomes a live candidate and
            // the key can fire a second +I. Probes never refresh the marker.
            match self.emitted.get(key) {
                Some(fired_ms) if ttl.expired(*fired_ms) => {
                    if track {
                        reclaimed += byte_key_bytes(key) as isize;
                    }
                    self.emitted.remove(key);
                }
                Some(_) => continue, // this key's first row already emitted
                None => {}
            }
            let rowtime = rt.value(row);
            match best.get(key) {
                Some((existing, _)) if *existing <= rowtime => {}
                _ => {
                    best.insert(key, (rowtime, row as u32));
                }
            }
        }
        if reclaimed != 0 {
            self.memory.record(-reclaimed);
        }
        let mut indices: Vec<u32> = best.into_values().map(|(_, idx)| idx).collect();
        indices.sort_unstable();
        let idx = UInt32Array::from(indices);
        let columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|c| take(c, &idx, None).expect("dedup take"))
            .collect();
        RecordBatch::try_new(batch.schema(), columns).expect("dedup compacted batch")
    }

    /// Persistent-state arrival path: the (already late-filtered) batch's touched keys probe the
    /// committed markers and candidates with one multi_get per table — an expired marker deletes
    /// on read and reads as absent, a live one drops the row — and only fresh keys and strict
    /// rowtime improvements write through, each under a fresh sequence so a later firing
    /// reproduces the memory path's emission order. Nothing is emitted here.
    #[cfg(feature = "rocksdb-state")]
    fn push_store(&mut self, live: &RecordBatch, now_ms: i64) -> Result<(), DataFusionError> {
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        let partition_columns = self.partition_columns.clone();
        let precisions = self.key_timestamp_precisions.clone();
        let store = self.store.as_mut().expect("keep-first dedup rocksdb store");
        let keys = store.entry_keys(live, &partition_columns, &precisions);
        let mut distinct = keys.clone();
        distinct.sort_unstable();
        distinct.dedup();
        let mut blocked: HashSet<ByteKey> = HashSet::default();
        let mut expired: Vec<ByteKey> = Vec::new();
        for (key, stamp) in store.markers(&distinct)? {
            match stamp {
                Some(stamp) if ttl.expired(stamp) => expired.push(key),
                _ => {
                    blocked.insert(key);
                }
            }
        }
        store.remove_markers(&expired)?;
        let open: Vec<ByteKey> = distinct
            .into_iter()
            .filter(|key| !blocked.contains(key))
            .collect();
        let committed = store.candidates(&open)?;
        let rt = rt_to_millis(live.column(self.rt_column));
        let mut best: HashMap<&ByteKey, (i64, Option<usize>)> = HashMap::default();
        for (key, (rowtime, _)) in &committed {
            best.insert(key, (*rowtime, None));
        }
        for row in 0..live.num_rows() {
            let key = &keys[row];
            if blocked.contains(key) {
                continue;
            }
            let rowtime = rt.value(row);
            match best.get(key) {
                Some((existing, _)) if *existing <= rowtime => {}
                _ => {
                    best.insert(key, (rowtime, Some(row)));
                }
            }
        }
        let mut winners: Vec<(usize, ByteKey, i64)> = best
            .into_iter()
            .filter_map(|(key, (rowtime, row))| row.map(|row| (row, key.clone(), rowtime)))
            .collect();
        winners.sort_unstable_by_key(|(row, _, _)| *row);
        self.store_mut().put_candidates(live, &winners)
    }

    /// Persistent-state firing path: the pending table's range read removes and returns every
    /// candidate the watermark released in the memory path's emission order, stamping their fired
    /// markers; the output is those rows' payload columns.
    #[cfg(feature = "rocksdb-state")]
    fn flush_store(&mut self, watermark: i64, now_ms: i64) -> Result<RecordBatch, DataFusionError> {
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        let stamp = ttl.enabled().then(|| ttl.now());
        let store = self.store.as_mut().expect("keep-first dedup rocksdb store");
        store.set_watermark(watermark);
        let ready = store.take_ready(watermark, stamp)?;
        if ready.is_empty() {
            return Ok(self.empty());
        }
        let store = self.store.as_ref().expect("keep-first dedup rocksdb store");
        store.decode(ready.iter().map(|candidate| candidate.row.as_ref()))
    }

    /// Emits each pending key's candidate whose rowtime the watermark has now reached (insert-only),
    /// records those keys as emitted, and keeps the rest. Both partitions are columnar filters.
    /// `now_ms` is the host's wall-clock reading (only read when state TTL is on): firing a
    /// candidate stamps its marker with it — the marker's single TTL'd write.
    pub(crate) fn flush(
        &mut self,
        watermark: i64,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        self.snapshot_cache = None;
        self.current_watermark = watermark;
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return self.flush_store(watermark, now_ms);
        }
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
        let not_ready = filter_record_batch(
            &pending,
            &arrow::compute::not(&ready_mask).expect("dedup not"),
        )
        .expect("dedup keep filter");
        if track && not_ready.num_rows() > 0 {
            delta += not_ready.get_array_memory_size() as isize;
        }
        self.pending = (not_ready.num_rows() > 0).then_some(not_ready);
        if ready.num_rows() > 0 {
            let key_arrays: Vec<&ArrayRef> = self
                .partition_columns
                .iter()
                .map(|&i| ready.column(i))
                .collect();
            self.key_types = key_types(&key_arrays);
            let keys_encoded = encode_keys(&mut self.key_converter, &key_arrays, ready.num_rows());
            let ttl = StateTtl::new(self.ttl_ms, now_ms);
            let stamp = if ttl.enabled() { ttl.now() } else { 0 };
            for row in 0..ready.num_rows() {
                let key = keys_encoded.row(row).data();
                // The emitted-key set grows for the operator's lifetime, so a flush can grow
                // state. Firing writes the marker — Flink's onTimer `update(true)` — so a marker
                // already present (live or expired) is re-stamped in place.
                match self.emitted.get_mut(key) {
                    Some(fired_ms) => *fired_ms = stamp,
                    None => {
                        self.emitted.insert(ByteKey::from(key), stamp);
                        if track {
                            delta += byte_key_bytes(key) as isize;
                        }
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

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Vec<u8> {
        self.snapshot_parts(self.pending.clone(), self.emitted_batch())
    }

    fn snapshot_parts(
        &self,
        pending_batch: Option<RecordBatch>,
        emitted_batch: Option<RecordBatch>,
    ) -> Vec<u8> {
        let mut out = self.current_watermark.to_le_bytes().to_vec();
        let pending = pending_batch
            .map(|batch| write_ipc(&batch))
            .unwrap_or_default();
        out.extend_from_slice(&(pending.len() as u32).to_le_bytes());
        out.extend_from_slice(&pending);
        out.extend_from_slice(
            &emitted_batch
                .map(|batch| write_ipc(&batch))
                .unwrap_or_default(),
        );
        out
    }

    /// The emitted keys as an IPC batch of the key columns (decoded from the stored key bytes),
    /// plus — only while TTL is on, so a TTL-off snapshot stays byte-identical to the pre-TTL
    /// format — a trailing column of each marker's firing timestamp.
    fn emitted_batch(&self) -> Option<RecordBatch> {
        if self.emitted.is_empty() {
            return None;
        }
        let mut keys: Vec<&[u8]> = Vec::with_capacity(self.emitted.len());
        let mut fired: Vec<i64> = Vec::with_capacity(self.emitted.len());
        for (key, fired_ms) in self.emitted.iter() {
            keys.push(key.0.as_ref());
            fired.push(*fired_ms);
        }
        let mut fields = key_fields(&self.key_types);
        let mut columns = decode_byte_keys(self.key_converter.as_ref(), &keys, &self.key_types);
        if self.ttl_ms > 0 {
            fields.push(Field::new(TTL_TS_COLUMN, DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(fired)));
        }
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
                    binary_row_hash(batch, &self.partition_columns, row, timestamp_precisions),
                    max_parallelism,
                ) as i32;
                pending_by_group
                    .entry(key_group)
                    .or_default()
                    .push(row as u32);
            }
        }
        if let Some(batch) = &emitted {
            // Only the key columns feed the key-group hash — the TTL timestamp column trails them.
            let key_columns: Vec<usize> = (0..self.key_types.len()).collect();
            for row in 0..batch.num_rows() {
                let key_group = flink_key_group(
                    binary_row_hash(batch, &key_columns, row, timestamp_precisions),
                    max_parallelism,
                ) as i32;
                emitted_by_group
                    .entry(key_group)
                    .or_default()
                    .push(row as u32);
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

    /// The marker timestamps are read by name when the writer had TTL on; a pre-TTL snapshot
    /// restored into a TTL'd deduplicator stamps every marker with the restore time (a full
    /// retention from now, Flink's enable-TTL migration) instead of 0, which would expire
    /// everything on first probe. The pending part carries no timestamps in either format.
    pub(crate) fn restore(
        partition_columns: Vec<usize>,
        rt_column: usize,
        bytes: &[u8],
        restored_at_ms: i64,
    ) -> Self {
        let mut dedup = KeepFirstDeduplicator::new(partition_columns, rt_column);
        if bytes.len() < 8 {
            return dedup;
        }
        dedup.current_watermark = i64::from_le_bytes(bytes[0..8].try_into().expect("watermark"));
        let pending_len =
            u32::from_le_bytes(bytes[8..12].try_into().expect("pending len")) as usize;
        for batch in read_ipc_if_present(&bytes[12..12 + pending_len]) {
            dedup.schema = Some(batch.schema());
            dedup.pending = Some(batch);
        }
        for batch in read_ipc_if_present(&bytes[12 + pending_len..]) {
            let fired = batch
                .column_by_name(TTL_TS_COLUMN)
                .is_some()
                .then(|| column_i64(&batch, TTL_TS_COLUMN));
            let key_arity = batch.num_columns() - fired.is_some() as usize;
            let key_arrays: Vec<&ArrayRef> = (0..key_arity).map(|i| batch.column(i)).collect();
            dedup.key_types = key_types(&key_arrays);
            let keys_encoded = encode_keys(&mut dedup.key_converter, &key_arrays, batch.num_rows());
            for row in 0..batch.num_rows() {
                dedup.emitted.insert(
                    ByteKey::from(keys_encoded.row(row).data()),
                    fired.as_ref().map_or(restored_at_ms, |ts| ts.value(row)),
                );
            }
        }
        dedup
    }

    fn restore_partitions(
        partition_columns: Vec<usize>,
        rt_column: usize,
        snapshots: &[Vec<u8>],
        restored_at_ms: i64,
    ) -> Self {
        let mut watermark = i64::MIN;
        let mut pending = Vec::new();
        let mut emitted = Vec::new();
        for bytes in snapshots {
            if bytes.len() < 12 {
                continue;
            }
            watermark = watermark.max(i64::from_le_bytes(
                bytes[0..8].try_into().expect("dedup watermark"),
            ));
            let pending_len =
                u32::from_le_bytes(bytes[8..12].try_into().expect("dedup pending len")) as usize;
            assert!(
                12 + pending_len <= bytes.len(),
                "truncated dedup raw key-group snapshot"
            );
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
        KeepFirstDeduplicator::restore(partition_columns, rt_column, &bytes, restored_at_ms)
    }
}

/// Exports the complete persistent state in the blob snapshot's per-key-group encoding
/// (`[watermark][framed pending ipc][emitted ipc]`), for backend-independent canonical savepoints.
#[cfg(feature = "rocksdb-state")]
impl KeepFirstDeduplicator {
    pub(crate) fn canonical_partitions(&self) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError> {
        use crate::state::{RocksKeepFirstDedupStore, StoredCandidate};
        let store = self.store.as_ref().expect("keep-first dedup rocksdb store");
        let pending = store.scan_pending()?;
        let markers = store.scan_markers()?;
        let mut pending_by_group: BTreeMap<i32, Vec<&StoredCandidate>> = BTreeMap::new();
        for candidate in &pending {
            pending_by_group
                .entry(RocksKeepFirstDedupStore::key_group(&candidate.key))
                .or_default()
                .push(candidate);
        }
        let mut markers_by_group: BTreeMap<i32, Vec<(&ByteKey, i64)>> = BTreeMap::new();
        for (key, stamp) in &markers {
            markers_by_group
                .entry(RocksKeepFirstDedupStore::key_group(key))
                .or_default()
                .push((key, stamp.unwrap_or(0)));
        }
        let mut groups: Vec<i32> = pending_by_group
            .keys()
            .chain(markers_by_group.keys())
            .copied()
            .collect();
        groups.sort_unstable();
        groups.dedup();
        let mut snapshots = BTreeMap::new();
        for group in groups {
            let pending_part = match pending_by_group.get_mut(&group) {
                Some(candidates) => {
                    candidates.sort_unstable_by_key(|candidate| candidate.seq);
                    Some(store.decode(candidates.iter().map(|candidate| candidate.row.as_ref()))?)
                }
                None => None,
            };
            let emitted_part = markers_by_group.get(&group).map(|entries| {
                let keys: Vec<&ByteKey> = entries.iter().map(|(key, _)| *key).collect();
                let mut fields = key_fields(store.key_types());
                let mut columns = store.decode_key_columns(&keys);
                if self.ttl_ms > 0 {
                    let stamps: Vec<i64> = entries.iter().map(|(_, stamp)| *stamp).collect();
                    fields.push(Field::new(TTL_TS_COLUMN, DataType::Int64, false));
                    columns.push(Arc::new(Int64Array::from(stamps)));
                }
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("dedup emitted")
            });
            snapshots.insert(group, self.snapshot_parts(pending_part, emitted_part));
        }
        Ok(snapshots)
    }
}

/// Eager (push→emit, no watermark buffering) deduplication keyed by a partition key. Serves every
/// dedup variant except the watermark-buffered event-time keep-first, which lives in
/// {@link KeepFirstDeduplicator}:
///   * **rowtime keep-last** — Flink's `RowTimeDeduplicateFunction`: keep the **maximum**-rowtime row;
///     the first row emits `+I`, a later row (rowtime `>=` the stored one) emits `-U`(previous, gated
///     on `generate_update_before`)/`+U`(new), and a smaller-rowtime row is ignored.
///   * **proctime keep-last** — Flink's `ProcTimeDeduplicateKeepLastRowFunction`: the same, but in
///     arrival order, so every later row replaces (no rowtime read or comparison).
///   * **proctime keep-first** — Flink's `ProcTimeDeduplicateKeepFirstRowFunction`: the first row per
///     key emits `+I` and every later row is dropped; insert-only output (no `$row_kind$`).
///   * **rowtime keep-first under mini-batch** — Flink then plans the same bundled retracting
///     function as keep-last with the comparator flipped (a strictly **smaller**-rowtime row
///     displaces with `-U`/`+U`, a tie keeps the incumbent), so the shape is the keep-last
///     machinery, not the insert-only watermark buffer — updating output with `$row_kind$`.
/// Every updating shape honors Flink's insert-sensitivity (see `generate_insert`): with it and
/// `generate_update_before` both false a fresh key emits a bare `+U` instead of `+I`, and the
/// proctime identical-row suppression is off. Insert-only input. The stored full row per key
/// lives as scalars and is rebuilt with `scalars_to_array` on emit, like the changelog
/// normalizer below.
pub(crate) struct KeepLastDeduplicator<S: KeyedStateStore<DedupRow> = MemoryDedupStore> {
    partition_columns: Vec<usize>,
    key_timestamp_precisions: Vec<i32>,
    rt_column: usize,
    generate_update_before: bool,
    /// Flink's `table.exec.deduplicate.insert-update-after-sensitive-enabled` (default true).
    /// With this AND `generate_update_before` both false — the option off under a consumer that
    /// requests only UPDATE_AFTER — Flink's emission helpers take their stateless else-branch:
    /// every emission is a bare `+U`, a brand-new key's first row included, and the proctime
    /// keep-last never consults state for suppression.
    generate_insert: bool,
    /// Whether the order is a rowtime (read + compared) or proctime (arrival order; rt ignored).
    rowtime_ordered: bool,
    /// Keep-first (insert-only, first row wins) vs keep-last (retract changelog, latest row wins).
    keep_first: bool,
    // Idle-state retention millis (0 = off — Flink's default). With TTL on, a key expires `ttl_ms`
    // after its last write, and the proctime keep-last identical-row suppression is disabled:
    // Flink always emits -U/+U under TTL to keep refreshing downstream state.
    ttl_ms: i64,
    // When the last full expiry sweep ran; the sweep reclaims keys never touched again, once per
    // TTL period (expiry itself is enforced lazily at each touch).
    last_sweep_ms: i64,
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
    /// Mini-batch compact-changes (`table.exec.deduplicate.mini-batch.compact-changes-enabled`,
    /// rowtime only) — Flink's `RowTimeMiniBatchLatestChangeDeduplicateFunction`: the flush nets
    /// each key's bundle to one transition (stored preimage to the bundle's final kept row)
    /// instead of the default full kept chain, and a bundle that keeps nothing writes nothing
    /// (in particular, no TTL refresh — see the ignored-row guard in `push`).
    compact_changes: bool,
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
    /// Index of this key's staged change while a mini-batch bundle is open (`None` otherwise);
    /// the rowtime shape appends every later kept row of the bundle to that entry's chain.
    staged: Option<u32>,
    /// The RowKind of Flink's stored row: false = INSERT (stored at creation, or by a suppressed
    /// duplicate), true = UPDATE_AFTER. Flink's proctime keep-last stores the row object BEFORE
    /// emitting it, and emission mutates that same object's kind to UPDATE_AFTER (heap-state
    /// aliasing); its generated equaliser compares kinds first, so an identical +I row is
    /// suppressed only while the stored kind is still INSERT — i.e. until the key's first emitted
    /// update. Replicated bit-for-bit because the suppression difference is parity-visible.
    update_kind: bool,
    /// Wall-clock millis of the key's last write (Flink state TTL, `OnCreateAndWrite`); stays 0
    /// while TTL is off.
    last_write_ms: i64,
}

struct DedupStagedChange {
    key: ByteKey,
    before: Option<Arc<[u8]>>,
    /// Every kept row of the bundle in arrival order, staged only by the rowtime shape: Flink's
    /// rowtime mini-batch flush emits one transition per kept row ("we output all changelog here
    /// rather than comparing the first and the last record in buffer" — a temporal join's
    /// versioned table needs every intermediate version), where the proctime shape compacts the
    /// bundle to its endpoint before the flush ever sees it.
    kept: Vec<Arc<[u8]>>,
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

/// The dedup persistent backend: the generic persistent store under the raw dedup codec.
#[cfg(feature = "rocksdb-state")]
pub(crate) type RocksDedupStore = crate::state::RocksStore<DedupStateCodec>;

/// The dedup value codec for the persistent store: raw — `[rowtime: i64 LE][update_kind: u8]`
/// followed by the stored row's arrow-row payload bytes, verbatim (the same payload the raw
/// keyed-state snapshot carries, so the two persistence paths cannot drift). Every payload shape
/// round-trips as opaque bytes, so every keep-last state shape is persistable.
#[cfg(feature = "rocksdb-state")]
pub(crate) struct DedupStateCodec;

#[cfg(feature = "rocksdb-state")]
const DEDUP_RAW_PREFIX: usize = 9;

#[cfg(feature = "rocksdb-state")]
impl crate::state::RocksStateCodec for DedupStateCodec {
    type Value = DedupRow;
    fn supported(&self) -> bool {
        true
    }
    fn value_fields(&self) -> Vec<(String, DataType)> {
        vec![("row".to_string(), DataType::Binary)]
    }
    fn encode(&self, _value: &DedupRow) -> Vec<ScalarValue> {
        unreachable!("raw codec")
    }
    fn decode(&self, _scalars: &[ScalarValue]) -> DedupRow {
        unreachable!("raw codec")
    }
    fn value_bytes(&self, value: &DedupRow) -> usize {
        DEDUP_RAW_PREFIX + value.payload.len()
    }
    fn write_ms(&self, value: &DedupRow) -> i64 {
        value.last_write_ms
    }
    fn stamp_write_ms(&self, value: &mut DedupRow, ts_ms: i64) {
        value.last_write_ms = ts_ms;
    }
    fn raw(&self) -> bool {
        true
    }
    fn raw_write(&self, value: &DedupRow, out: &mut Vec<u8>) {
        out.extend_from_slice(&value.rowtime.to_le_bytes());
        out.push(value.update_kind as u8);
        out.extend_from_slice(&value.payload);
    }
    fn from_raw(&self, bytes: &[u8]) -> DedupRow {
        DedupRow {
            rowtime: i64::from_le_bytes(bytes[..8].try_into().expect("dedup rowtime prefix")),
            payload: bytes[DEDUP_RAW_PREFIX..].into(),
            staged: None,
            update_kind: bytes[8] != 0,
            last_write_ms: 0,
        }
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
            generate_insert: true,
            rowtime_ordered,
            keep_first,
            ttl_ms: 0,
            last_sweep_ms: 0,
            schema: None,
            payload_converter: None,
            rows: MemoryDedupStore::default(),
            mini_batch: false,
            compact_changes: false,
            staged: Vec::new(),
            staged_bytes: 0,
            snapshot_cache: None,
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds this deduplicator's stored rows by the operator's task off-heap budget (negative =
    /// unaccounted), accounting any restored rows immediately.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .rows
            .iter()
            .map(|(key, row)| dedup_entry_bytes(&key.0, &row.payload))
            .sum();
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
            generate_insert: self.generate_insert,
            rowtime_ordered: self.rowtime_ordered,
            keep_first: self.keep_first,
            ttl_ms: self.ttl_ms,
            last_sweep_ms: self.last_sweep_ms,
            schema: self.schema,
            payload_converter: self.payload_converter,
            rows,
            mini_batch: self.mini_batch,
            compact_changes: self.compact_changes,
            staged: self.staged,
            staged_bytes: self.staged_bytes,
            snapshot_cache: None,
            memory: self.memory,
        }
    }

    /// Attaches the task off-heap budget for a backend that starts with nothing resident (a
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

    /// Proctime keep-first stays eager under mini-batch: Flink's bundled function emits the same
    /// insert-only first row per key, just at the flush. Rowtime keep-first is different — under
    /// mini-batch it IS the bundled retracting function — so it buffers like keep-last.
    pub(crate) fn with_mini_batch(mut self, mini_batch: bool) -> Self {
        self.mini_batch = mini_batch && (self.rowtime_ordered || !self.keep_first);
        self
    }

    pub(crate) fn with_compact_changes(mut self, compact_changes: bool) -> Self {
        self.compact_changes = compact_changes;
        self
    }

    /// Sets Flink's insert-sensitivity
    /// (`table.exec.deduplicate.insert-update-after-sensitive-enabled`); see `generate_insert`.
    pub(crate) fn with_generate_insert(mut self, generate_insert: bool) -> Self {
        self.generate_insert = generate_insert;
        self
    }

    /// The kind of a key's first emission: `+I`, unless neither update-befores nor inserts are
    /// requested — Flink then emits a bare `+U` even for a brand-new key.
    fn fresh_key_kind(&self) -> i8 {
        if self.generate_update_before || self.generate_insert {
            0
        } else {
            2
        }
    }

    /// Sets the idle-state retention (`table.exec.state.ttl`) in millis; 0 (Flink's default)
    /// disables expiry.
    pub(crate) fn with_state_ttl(mut self, ttl_ms: i64) -> Self {
        self.ttl_ms = ttl_ms.max(0);
        self
    }

    /// Reclaims every key whose TTL elapsed with no further touch — the lazy per-touch expiry
    /// never sees such a key again. Silent, like Flink's background cleanup.
    fn sweep_expired(&mut self, ttl: StateTtl) {
        let track = self.memory.tracking();
        let mut reclaimed = 0isize;
        self.rows.retain_live(&mut |key, row| {
            if ttl.expired(row.last_write_ms) {
                if track {
                    reclaimed += dedup_entry_bytes(key, &row.payload) as isize;
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
                (0..arity)
                    .map(|i| SortField::new(batch.column(i).data_type().clone()))
                    .collect(),
            )
            .expect("dedup payload converter"),
        );
    }

    /// Folds an input batch into the per-key kept rows and returns the changelog (or insert-only
    /// rows) it produces. `now_ms` is the host's wall-clock reading for this call (only read when
    /// state TTL is on).
    pub(crate) fn push(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        self.snapshot_cache = None;
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        // The sweep reclaims keys no later row ever touches. Once per TTL period bounds its
        // amortized cost at one map walk per period; it must not run mid-bundle, where removing a
        // staged key's state would strand its staged preimage at the flush.
        if ttl.enabled() && self.staged.is_empty() && now_ms >= self.last_sweep_ms + self.ttl_ms {
            self.sweep_expired(ttl);
            self.last_sweep_ms = now_ms;
        }
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        self.ensure_converters(batch, arity);
        self.rows.begin_batch(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        )?;
        let mut parts = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let payloads = self
            .payload_converter
            .as_ref()
            .unwrap()
            .convert_columns(&data_arrays)
            .expect("encode dedup payload");
        // The rowtime is read only for a rowtime order; proctime dedup uses arrival order.
        let rt = self
            .rowtime_ordered
            .then(|| rt_to_millis(batch.column(self.rt_column)));

        let keep_first = self.keep_first;
        let rowtime_ordered = self.rowtime_ordered;
        let generate_update_before = self.generate_update_before;
        let generate_insert = self.generate_insert;
        let fresh_key_kind = self.fresh_key_kind();
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let rows = &mut self.rows;
        let mut out_rows: Vec<Arc<[u8]>> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        for row in 0..batch.num_rows() {
            // Borrowed probe: the key bytes are copied into the map only when a key first appears,
            // and a dropped/ignored row allocates nothing at all.
            let key = parts.encode(row);
            // An expired key is deleted on read and treated as never seen (Flink's
            // NeverReturnExpired): the next row re-enters through the fresh +I path. A staged row
            // was written this bundle and is exempt until the flush ends the bundle — removing it
            // would strand its staged preimage and double-stage the key (the same rule that skips
            // the sweep mid-bundle); its expiry just delays to the first touch after the flush.
            let ttl_ts = |row: &DedupRow| {
                if row.staged.is_some() {
                    i64::MAX
                } else {
                    row.last_write_ms
                }
            };
            let on_expired = |row: &DedupRow| {
                if track {
                    delta -= dedup_entry_bytes(key, &row.payload) as isize;
                }
            };
            // Proctime keep-first: the first row per key wins, later rows are dropped
            // (insert-only). A dropped duplicate is not a state write, so it does not refresh the
            // key's TTL — Flink's processFirstRowOnProcTime writes state only for the first row.
            // (Rowtime keep-first — the mini-batch bundled shape — flows through the retracting
            // path below with the comparator flipped.)
            if keep_first && !rowtime_ordered {
                if ttl_contains(rows, key, ttl, ttl_ts, on_expired) {
                    continue;
                }
                let payload: Arc<[u8]> = Arc::from(payloads.row(row).data());
                if track {
                    delta += dedup_entry_bytes(key, &payload) as isize;
                }
                // Flink's OnCreateAndWrite: creation stamps the TTL clock.
                let last_write_ms = if ttl.enabled() { ttl.now() } else { 0 };
                out_rows.push(payload.clone());
                out_kinds.push(0); // +I — first row for the key
                rows.insert(
                    ByteKey::from(key),
                    DedupRow {
                        rowtime: 0,
                        payload,
                        staged: None,
                        update_kind: false,
                        last_write_ms,
                    },
                );
                continue;
            }
            let rowtime = rt.as_ref().map_or(0, |rt| rt.value(row));
            let current = payloads.row(row).data();
            match ttl_get_mut(rows, key, ttl, ttl_ts, on_expired) {
                None => {
                    let payload: Arc<[u8]> = Arc::from(current);
                    if track {
                        delta += dedup_entry_bytes(key, &payload) as isize;
                    }
                    let staged = self.mini_batch.then(|| self.staged.len() as u32);
                    let owned = ByteKey::from(key);
                    if staged.is_some() {
                        let retained = byte_key_bytes(key);
                        let kept = if rowtime_ordered {
                            vec![payload.clone()]
                        } else {
                            Vec::new()
                        };
                        self.staged.push(DedupStagedChange {
                            key: owned.clone(),
                            before: None,
                            kept,
                        });
                        self.staged_bytes += retained;
                        delta += retained as isize;
                    } else {
                        out_rows.push(payload.clone());
                        out_kinds.push(fresh_key_kind);
                    }
                    let last_write_ms = if ttl.enabled() { ttl.now() } else { 0 };
                    rows.insert(
                        owned,
                        DedupRow {
                            rowtime,
                            payload,
                            staged,
                            update_kind: false,
                            last_write_ms,
                        },
                    );
                }
                // A rowtime order ignores a non-improving row — Flink's shouldKeepCurrentRow:
                // keep-last ignores a smaller rowtime (a tie displaces), keep-first ignores at or
                // above the stored one (a tie keeps the incumbent); proctime always replaces.
                // An ignored row is not a state write, so it does not refresh the key's TTL —
                // Flink's rowtime helper returns before updateState when the row isn't kept —
                // except under the default (full-chain) mini-batch flush, whose unconditional
                // state.update at finishBundle re-stamps every key the bundle touched.
                // Compact-changes writes state only for a winning bundle, so a losing row stays a
                // pure read there.
                Some(stored)
                    if rowtime_ordered
                        && (if keep_first {
                            rowtime >= stored.rowtime
                        } else {
                            rowtime < stored.rowtime
                        }) =>
                {
                    if self.mini_batch && !self.compact_changes && ttl.enabled() {
                        stored.last_write_ms = ttl.now();
                    }
                    continue;
                }
                // Proctime keep-last suppresses an identical row only while the stored kind is
                // still INSERT (see `DedupRow::update_kind`) and TTL is off (with TTL on, Flink
                // always emits -U/+U so downstream state keeps refreshing instead of expiring too
                // early); the rowtime variant never suppresses — its helper emits through
                // updateDeduplicateResult with no equality check. Flink's suppression re-stores
                // the identical row with kind INSERT, so the flag stays false here. With neither
                // update-befores nor inserts requested, Flink's helper takes its stateless bare-+U
                // branch and never reaches the equality check, so nothing is suppressed.
                Some(stored)
                    if !rowtime_ordered
                        && (generate_update_before || generate_insert)
                        && !stored.update_kind
                        && stored.payload.as_ref() == current
                        && !ttl.enabled() =>
                {
                    continue;
                }
                Some(stored) => {
                    let payload: Arc<[u8]> = Arc::from(current);
                    if track {
                        // Same key: only the payload is replaced.
                        delta += payload.len() as isize - stored.payload.len() as isize;
                    }
                    if self.mini_batch {
                        match stored.staged {
                            None => {
                                let before = stored.payload.clone();
                                let retained = byte_key_bytes(key) + before.len();
                                let kept = if rowtime_ordered {
                                    vec![payload.clone()]
                                } else {
                                    Vec::new()
                                };
                                stored.staged = Some(self.staged.len() as u32);
                                self.staged.push(DedupStagedChange {
                                    key: ByteKey::from(key),
                                    before: Some(before),
                                    kept,
                                });
                                self.staged_bytes += retained;
                                delta += retained as isize;
                            }
                            // The rowtime shape stages every kept row for the flush to emit; the
                            // displaced intermediate stays retained by the chain instead of
                            // freeing with the payload swap below.
                            Some(index) if rowtime_ordered => {
                                let retained = stored.payload.len();
                                self.staged[index as usize].kept.push(payload.clone());
                                self.staged_bytes += retained;
                                delta += retained as isize;
                            }
                            Some(_) => {}
                        }
                    } else {
                        if generate_update_before {
                            out_rows.push(stored.payload.clone());
                            out_kinds.push(1); // -U the previous row
                        }
                        out_rows.push(payload.clone());
                        out_kinds.push(2); // +U the new (later) row
                                           // Emitting the update mutates Flink's stored row to UPDATE_AFTER.
                        stored.update_kind = true;
                    }
                    stored.rowtime = rowtime;
                    stored.payload = payload;
                    if ttl.enabled() {
                        // Every kept row is a state write, so it refreshes the key's TTL.
                        stored.last_write_ms = ttl.now();
                    }
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
        let fresh_key_kind = self.fresh_key_kind();
        let mut out_rows = Vec::with_capacity(changes.len() * 2);
        let mut out_kinds = Vec::with_capacity(changes.len() * 2);
        for DedupStagedChange { key, before, kept } in changes {
            let row = self
                .rows
                .get_mut(&key.0)
                .expect("staged dedup key remains in state");
            row.staged = None;
            if self.rowtime_ordered {
                if self.compact_changes {
                    // Compact-changes nets the bundle to its endpoint: one transition per key,
                    // stored preimage to the bundle's final kept row, with no equality check
                    // anywhere (an identical displacing row still emits its -U/+U pair).
                    let after = kept
                        .into_iter()
                        .next_back()
                        .expect("compacted chain keeps a row");
                    match before {
                        None => {
                            out_rows.push(after);
                            out_kinds.push(fresh_key_kind);
                        }
                        Some(before) => {
                            if self.generate_update_before {
                                out_rows.push(before);
                                out_kinds.push(1);
                            }
                            out_rows.push(after);
                            out_kinds.push(2);
                        }
                    }
                    continue;
                }
                // Flink's rowtime mini-batch flush walks the bundle's kept rows and emits every
                // transition, with no equality check anywhere on the rowtime path — a bundle of
                // n kept rows is n transitions, not the endpoint-only net one.
                let mut previous = before;
                for payload in kept {
                    match previous {
                        None => {
                            out_rows.push(payload.clone());
                            out_kinds.push(fresh_key_kind);
                        }
                        Some(before) => {
                            if self.generate_update_before {
                                out_rows.push(before);
                                out_kinds.push(1);
                            }
                            out_rows.push(payload.clone());
                            out_kinds.push(2);
                        }
                    }
                    previous = Some(payload);
                }
                continue;
            }
            let after = row.payload.clone();
            // The same rules as immediate mode (Flink's mini-batch flush runs the same
            // processLastRowOnProcTime): a bundle whose net transition leaves the row unchanged
            // is suppressed only with retention off and while the stored kind is still INSERT
            // (staged rows are exempt from the lazy expiry, so none can have expired here) —
            // and never when neither update-befores nor inserts are requested (the bare-+U
            // branch bypasses the equality check).
            if self.ttl_ms == 0
                && (self.generate_update_before || self.generate_insert)
                && !row.update_kind
                && before.as_ref() == Some(&after)
            {
                continue;
            }
            if before.is_some() {
                row.update_kind = true; // the emitted update mutates Flink's stored row to +U
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
                out_kinds.push(fresh_key_kind);
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
        let schema = self
            .schema
            .as_ref()
            .expect("schema set once a row was processed");
        let conv = self.payload_converter.as_ref().expect("converter set");
        // One vectorized row->columnar pass rebuilds every data column (cf. the per-cell scalar build).
        let parser = conv.parser();
        let mut columns: Vec<ArrayRef> = conv
            .convert_rows(out_rows.iter().map(|r| parser.parse(r)))
            .expect("decode dedup payloads");
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        // Proctime keep-first is insert-only (every emitted row is a +I), so it carries no
        // $row_kind$ column; keep-last — and rowtime keep-first, its mini-batch retracting twin —
        // emits a changelog and tags each row's kind.
        if !self.keep_first || self.rowtime_ordered {
            fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
            columns.push(Arc::new(Int8Array::from(out_kinds)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build keep-last dedup batch")
    }
}

/// The dedup raw snapshot stores the exact rowtime alongside the shared key/row columns (the
/// decoded format re-derived it from the typed rowtime column).
const RAW_SNAPSHOT_ROWTIME: &str = "__rowtime__";

/// The stored row's kind (see `DedupRow::update_kind`), written only by the proctime keep-last
/// shape — the only one whose suppression consults it. Flink's heap backend serializes the
/// mutated kind into its checkpoints, so it must survive ours too; a snapshot without the column
/// (another shape, or pre-flag) restores as INSERT.
const RAW_SNAPSHOT_UPDATE_KIND: &str = "__update_kind__";

/// One key group's raw snapshot blob, built from any backend's resident view of the selected keys.
impl<S: KeyedStateStore<DedupRow>> KeepLastDeduplicator<S> {
    /// Serializes the selected keys' stored rows as raw state bytes: the stored Flink-BinaryRow
    /// key, arrow-row payload, and rowtime, verbatim — no decode. The schema's metadata carries
    /// the typed payload schema so converters can be rebuilt before any input arrives. The
    /// optional columns ride only where they mean something — the stored kind for the proctime
    /// keep-last shape (the only suppression that consults it) and the TTL timestamps only while
    /// TTL is on — so every other snapshot stays byte-identical to its prior format.
    fn snapshot_keys(&self, selected: &[ByteKey]) -> Vec<u8> {
        let schema = self
            .schema
            .as_ref()
            .expect("schema set once a row was stored");
        let kind_on = !self.rowtime_ordered && !self.keep_first;
        let ttl_on = self.ttl_ms > 0;
        let mut keys = BinaryBuilder::new();
        let mut payloads = BinaryBuilder::new();
        let mut rowtimes = Int64Builder::new();
        let mut update_kinds = BooleanBuilder::new();
        let mut write_timestamps = Int64Builder::new();
        for key in selected {
            let row = self
                .rows
                .get(&key.0)
                .expect("snapshot key remains in dedup state");
            keys.append_value(&key.0);
            payloads.append_value(&row.payload);
            rowtimes.append_value(row.rowtime);
            update_kinds.append_value(row.update_kind);
            write_timestamps.append_value(row.last_write_ms);
        }
        let mut fields = vec![
            Field::new(RAW_SNAPSHOT_KEY, DataType::Binary, false),
            Field::new(RAW_SNAPSHOT_ROW, DataType::Binary, false),
            Field::new(RAW_SNAPSHOT_ROWTIME, DataType::Int64, false),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(keys.finish()),
            Arc::new(payloads.finish()),
            Arc::new(rowtimes.finish()),
        ];
        if kind_on {
            fields.push(Field::new(
                RAW_SNAPSHOT_UPDATE_KIND,
                DataType::Boolean,
                false,
            ));
            columns.push(Arc::new(update_kinds.finish()));
        }
        if ttl_on {
            fields.push(Field::new(TTL_TS_COLUMN, DataType::Int64, false));
            columns.push(Arc::new(write_timestamps.finish()));
        }
        let raw_schema = Arc::new(Schema::new_with_metadata(
            fields,
            std::collections::HashMap::from([(
                RAW_SNAPSHOT_PAYLOAD_SCHEMA.to_string(),
                encode_schema_metadata(schema),
            )]),
        ));
        let batch = RecordBatch::try_new(raw_schema, columns).expect("raw dedup snapshot batch");
        write_ipc(&batch)
    }
}

/// Commits the persistent store and exports the complete logical table in the same raw key-group
/// encoding the memory snapshot writes, for backend-independent canonical savepoints.
#[cfg(feature = "rocksdb-state")]
impl KeepLastDeduplicator<RocksDedupStore> {
    pub(crate) fn canonical_partitions(
        &mut self,
    ) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError> {
        let keys = self.rows.canonical_keys_by_group()?;
        if self.schema.is_none() && !keys.is_empty() {
            self.rows.finish_canonical_scan();
            return Err(DataFusionError::Execution(
                "keep-last dedup canonical snapshot needs the payload schema, which only arrives \
                 with input; take the savepoint after the operator has processed a batch"
                    .into(),
            ));
        }
        let partitions = keys
            .iter()
            .map(|(&group, selected)| (group, self.snapshot_keys(selected)))
            .collect();
        self.rows.finish_canonical_scan();
        Ok(partitions)
    }
}

/// The raw keyed-state snapshot/restore surface exists only on the memory backend — a persistent
/// store checkpoints through its own commit path instead of materializing the key space.
impl KeepLastDeduplicator {
    /// One IPC blob per key group of raw state bytes, the group one hash of the stored key's
    /// bytes per entry (that encoding's hash IS Flink's key-group input).
    fn raw_snapshot_groups(&self, max_parallelism: usize) -> BTreeMap<i32, Vec<u8>> {
        if self.schema.is_none() {
            return BTreeMap::new();
        }
        let mut keys_by_group: BTreeMap<i32, Vec<ByteKey>> = BTreeMap::new();
        for (key, _) in self.rows.iter() {
            let group = flink_key_group(hash_bytes_by_words(&key.0), max_parallelism) as i32;
            keys_by_group.entry(group).or_default().push(key.clone());
        }
        keys_by_group
            .iter()
            .map(|(&group, selected)| (group, self.snapshot_keys(selected)))
            .collect()
    }

    /// Serializes the stored last-row-per-key set.
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Vec<u8> {
        self.raw_snapshot_groups(1).remove(&0).unwrap_or_default()
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
        self.snapshot_cache = Some(DedupSnapshotCache {
            max_parallelism,
            timestamp_precisions: timestamp_precisions.to_vec(),
            snapshots: self.raw_snapshot_groups(max_parallelism),
        });
    }

    #[cfg(test)]
    pub(crate) fn restore(
        partition_columns: Vec<usize>,
        key_timestamp_precisions: Vec<i32>,
        rt_column: usize,
        generate_update_before: bool,
        rowtime_ordered: bool,
        keep_first: bool,
        bytes: &[u8],
        restored_at_ms: i64,
    ) -> Self {
        Self::restore_partitions(
            partition_columns,
            key_timestamp_precisions,
            rt_column,
            generate_update_before,
            rowtime_ordered,
            keep_first,
            &[bytes.to_vec()],
            restored_at_ms,
        )
    }

    /// Raw-format rows carry the stored key, payload, and rowtime verbatim — restoring is a
    /// straight map rebuild with no decode or re-encode. The optional trailing columns are read
    /// by name: the stored row kind when the writer was proctime keep-last (absent restores as
    /// INSERT), and the TTL timestamps when the writer had TTL on — a pre-TTL snapshot restored
    /// into a TTL'd deduplicator stamps every key with the restore time (a full retention from
    /// now, Flink's enable-TTL migration) instead of 0, which would expire everything on first
    /// touch.
    fn load_batch_raw(&mut self, batch: &RecordBatch, restored_at_ms: i64) {
        if self.schema.is_none() {
            let payload_schema =
                decode_schema_metadata(batch).expect("raw dedup snapshot payload schema");
            let empty = RecordBatch::new_empty(payload_schema.clone());
            self.ensure_converters(&empty, empty.num_columns());
            self.schema = Some(payload_schema);
        }
        let keys = column_binary(batch, RAW_SNAPSHOT_KEY);
        let payloads = column_binary(batch, RAW_SNAPSHOT_ROW);
        let rowtimes = column_i64(batch, RAW_SNAPSHOT_ROWTIME);
        let update_kinds = batch
            .column_by_name(RAW_SNAPSHOT_UPDATE_KIND)
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("dedup snapshot update-kind column must be boolean")
            });
        let write_timestamps = batch
            .column_by_name(TTL_TS_COLUMN)
            .is_some()
            .then(|| column_i64(batch, TTL_TS_COLUMN));
        for row in 0..batch.num_rows() {
            self.rows.insert(
                ByteKey::from(keys.value(row)),
                DedupRow {
                    rowtime: rowtimes.value(row),
                    payload: Arc::from(payloads.value(row)),
                    staged: None,
                    update_kind: update_kinds.as_ref().is_some_and(|kinds| kinds.value(row)),
                    last_write_ms: write_timestamps
                        .as_ref()
                        .map_or(restored_at_ms, |ts| ts.value(row)),
                },
            );
        }
    }

    /// Snapshots written before the raw format decoded the rows to typed columns; kept so
    /// existing savepoints keep restoring. The format predates TTL, so every key is stamped with
    /// the restore time (the enable-TTL migration).
    fn load_batch_decoded(&mut self, batch: &RecordBatch, restored_at_ms: i64) {
        let arity = batch.num_columns();
        self.schema = Some(batch.schema());
        self.ensure_converters(batch, arity);
        // The stored rowtime matters only to the rowtime-ordered comparison; proctime stores 0.
        let rt = self
            .rowtime_ordered
            .then(|| rt_to_millis(batch.column(self.rt_column)));
        let mut parts = BinaryRowBatchEncoder::new(
            batch,
            &self.partition_columns,
            &self.key_timestamp_precisions,
        );
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let payloads = self
            .payload_converter
            .as_ref()
            .unwrap()
            .convert_columns(&data_arrays)
            .expect("encode payload");
        let rows = &mut self.rows;
        for row in 0..batch.num_rows() {
            rows.insert(
                ByteKey::from(parts.encode(row)),
                DedupRow {
                    rowtime: rt.as_ref().map_or(0, |rt| rt.value(row)),
                    payload: Arc::from(payloads.row(row).data()),
                    staged: None,
                    update_kind: false,
                    last_write_ms: restored_at_ms,
                },
            );
        }
    }

    fn restore_partitions(
        partition_columns: Vec<usize>,
        key_timestamp_precisions: Vec<i32>,
        rt_column: usize,
        generate_update_before: bool,
        rowtime_ordered: bool,
        keep_first: bool,
        snapshots: &[Vec<u8>],
        restored_at_ms: i64,
    ) -> Self {
        let mut dedup = KeepLastDeduplicator::new(
            partition_columns,
            rt_column,
            generate_update_before,
            rowtime_ordered,
            keep_first,
        )
        .with_key_timestamp_precisions(key_timestamp_precisions);
        for bytes in snapshots {
            for batch in read_ipc_if_present(bytes) {
                if batch.schema_ref().field(0).name() == RAW_SNAPSHOT_KEY {
                    dedup.load_batch_raw(&batch, restored_at_ms);
                } else {
                    dedup.load_batch_decoded(&batch, restored_at_ms);
                }
            }
        }
        dedup
    }
}

state_bytes_getter!(
    Java_tech_streamfusion_Native_keepFirstDeduplicatorStateBytes,
    KeepFirstDeduplicator
);

/// Rows dropped as late over the handle's lifetime; the counter lives before the backend split,
/// so one getter serves the memory and persistent routes alike.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_keepFirstDeduplicatorLateDrops<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let dedup = unsafe { &*(handle as *const KeepFirstDeduplicator) };
        dedup.late_drops as jlong
    })
}

state_bytes_getter!(
    Java_tech_streamfusion_Native_keepLastDeduplicatorStateBytes,
    KeepLastDeduplicator
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_keepLastDeduplicatorStagingBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let dedup = unsafe { &*(handle as *const KeepLastDeduplicator) };
        dedup.staged_bytes as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_keepLastDeduplicatorStagedKeys<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let dedup = unsafe { &*(handle as *const KeepLastDeduplicator) };
        dedup.staged.len() as jlong
    })
}

/// Creates a keep-first deduplicator over the given partition-key columns and rowtime column.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createKeepFirstDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
    state_ttl_millis: jlong,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let dedup = KeepFirstDeduplicator::new(partitions, rt_column as usize)
            .with_key_timestamp_precisions(timestamp_precisions)
            .with_state_ttl(state_ttl_millis)
            .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, dedup)
    })
}

/// Buffers an input batch (no output); each key's minimum-rowtime row is emitted later, on the
/// watermark that reaches its rowtime.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushKeepFirstDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            dedup.push(&batch, now_millis)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Exports each key's first (minimum-rowtime) row whose rowtime the watermark has reached.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushKeepFirstDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
        // The emitted-key set grows here, so even a flush can exceed the budget.
        match dedup.flush(watermark_millis, now_millis) {
            Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Releases the deduplicator and its per-key state.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeKeepFirstDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<KeepFirstDeduplicator>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotKeepFirstDeduplicatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
        let precisions = read_i32_array(&env, &timestamp_precisions);
        keyed_state_partition_array(
            &mut env,
            dedup.snapshot_partitions(max_parallelism as usize, &precisions),
            "keep-first-dedup",
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreKeepFirstDeduplicatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
    state_ttl_millis: jlong,
    now_millis: jlong,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
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
            now_millis,
        )
        .with_key_timestamp_precisions(timestamp_precisions)
        .with_state_ttl(state_ttl_millis)
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, dedup)
    })
}

/// Creates an eager deduplicator (rowtime/proctime keep-last, proctime keep-first, or the
/// mini-batch rowtime keep-first) and returns an opaque handle.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createKeepLastDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
    generate_update_before: jboolean,
    generate_insert: jboolean,
    rowtime_ordered: jboolean,
    keep_first: jboolean,
    mini_batch: jboolean,
    compact_changes: jboolean,
    state_ttl_millis: jlong,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let dedup = KeepLastDeduplicator::new(
            partitions,
            rt_column as usize,
            generate_update_before != 0,
            rowtime_ordered != 0,
            keep_first != 0,
        )
        .with_generate_insert(generate_insert != 0)
        .with_mini_batch(mini_batch != 0)
        .with_compact_changes(compact_changes != 0)
        .with_key_timestamp_precisions(timestamp_precisions)
        .with_state_ttl(state_ttl_millis)
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, dedup)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushKeepLastDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut KeepLastDeduplicator) };
        match dedup.flush_mini_batch() {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Folds an input batch and returns the retract changelog it produces (emitted eagerly per row).
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushKeepLastDeduplicator<'local>(
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
        let dedup = unsafe { &mut *(handle as *mut KeepLastDeduplicator) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            dedup.push(&batch, now_millis)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeKeepLastDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<KeepLastDeduplicator>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotKeepLastDeduplicatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut KeepLastDeduplicator) };
        let precisions = read_i32_array(&env, &timestamp_precisions);
        keyed_state_partition_array(
            &mut env,
            dedup.snapshot_partitions(max_parallelism as usize, &precisions),
            "keep-last-dedup",
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreKeepLastDeduplicatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
    generate_update_before: jboolean,
    generate_insert: jboolean,
    rowtime_ordered: jboolean,
    keep_first: jboolean,
    mini_batch: jboolean,
    compact_changes: jboolean,
    state_ttl_millis: jlong,
    now_millis: jlong,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
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
            now_millis,
        )
        .with_generate_insert(generate_insert != 0)
        .with_mini_batch(mini_batch != 0)
        .with_compact_changes(compact_changes != 0)
        .with_state_ttl(state_ttl_millis)
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, dedup)
    })
}
