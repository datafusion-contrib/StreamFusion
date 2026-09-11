use crate::*;

/// The raw-snapshot column carrying each key's cleanup DEADLINE (not a last-write timestamp, so
/// not `TTL_TS_COLUMN`): the absolute wall-clock millis at which Flink's registered cleanup timer
/// would fire and clear the key. Present only while retention cleaning is on, as a third framed
/// section — a retention-off checkpoint stays byte-identical to the pre-TTL format.
pub(crate) const CLEANUP_AT_COLUMN: &str = "__cleanup_at__";

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
    /// Idle-state min retention millis (`table.exec.state.ttl`). Flink's temporal join does not use
    /// per-value `StateTtlConfig`: it keeps ONE per-key processing-time cleanup deadline and, when
    /// it fires, clears the key's entire state — both sides — silently. Cleaning is enabled iff
    /// this is `> 1` (Flink's literal `minRetentionTime > 1`; a 1ms retention disables cleaning).
    min_retention_ms: i64,
    /// The planner-derived max retention, `min * 3 / 2` (Flink `TableConfigUtils`), saturating.
    max_retention_ms: i64,
    /// Per-key cleanup deadline (Flink's `"cleanup-timestamp"` ValueState plus its registered
    /// timer): the key's state is observably gone once the wall clock reaches the deadline. A
    /// lazy check at key touch plus the periodic sweep stands in for the timer — firing emits
    /// nothing, so the substitution is invisible (divergences/28).
    cleanup_state: HashMap<GroupKey, i64>,
    /// When the last full deadline sweep ran; it reclaims keys never touched again, at most once
    /// per min-retention period.
    last_sweep_ms: i64,
    pub(crate) memory: OperatorMemory,
    /// Persistent-state mode: the probe rows, the versioned build side, and the cleanup deadlines
    /// live in the persistent store; the in-memory maps stay empty, and a watermark firing walks
    /// the store's key-major tables instead.
    #[cfg(feature = "rocksdb-state")]
    store: Option<crate::state::RocksTemporalJoinStore>,
    key_timestamp_precisions: Vec<i32>,
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
            min_retention_ms: 0,
            max_retention_ms: 0,
            cleanup_state: HashMap::default(),
            last_sweep_ms: 0,
            memory: OperatorMemory::unaccounted(),
            #[cfg(feature = "rocksdb-state")]
            store: None,
            key_timestamp_precisions: Vec::new(),
        }
    }

    pub(crate) fn with_key_timestamp_precisions(
        mut self,
        key_timestamp_precisions: Vec<i32>,
    ) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        self
    }

    /// Sets the idle-state retention (`table.exec.state.ttl`) in millis. The max deadline horizon
    /// is derived natively as Flink's planner does — `minRetentionTime * 3 / 2`, saturating.
    pub(crate) fn with_state_retention(mut self, min_retention_ms: i64) -> Self {
        self.min_retention_ms = min_retention_ms.max(0);
        self.max_retention_ms = self.min_retention_ms.saturating_mul(3) / 2;
        self
    }

    /// Flink's exact enablement quirk: strictly greater than ONE millisecond, not zero.
    fn cleaning_enabled(&self) -> bool {
        self.min_retention_ms > 1
    }

    /// Flink's `registerProcessingCleanupTimer`: the deadline moves to `now + maxRetention` only
    /// when the key has none, or the current one would land within a min-retention of now.
    fn register_cleanup(&mut self, key: &GroupKey, now_ms: i64, grew: &mut isize, track: bool) {
        match self.cleanup_state.get_mut(key) {
            Some(deadline) => {
                if now_ms.saturating_add(self.min_retention_ms) > *deadline {
                    *deadline = now_ms.saturating_add(self.max_retention_ms);
                }
            }
            None => {
                if track {
                    *grew += group_key_bytes(key) as isize;
                }
                self.cleanup_state
                    .insert(key.clone(), now_ms.saturating_add(self.max_retention_ms));
            }
        }
    }

    /// Flink's `cleanupState`: drops the key's ENTIRE state — both sides and the deadline —
    /// silently, returning the bytes reclaimed (0 when not tracking).
    fn clear_key(&mut self, key: &GroupKey, track: bool) -> usize {
        let mut freed = 0usize;
        if let Some(entries) = self.left_state.remove(key) {
            if track {
                freed += group_key_bytes(key) + entries.iter().map(left_entry_bytes).sum::<usize>();
            }
        }
        if let Some(versions) = self.right_state.remove(key) {
            if track {
                freed += group_key_bytes(key)
                    + versions
                        .values()
                        .map(|(row, _)| right_version_bytes(row))
                        .sum::<usize>();
            }
        }
        if self.cleanup_state.remove(key).is_some() && track {
            freed += group_key_bytes(key);
        }
        freed
    }

    /// Lazy stand-in for the fired cleanup timer at a key touch: a timer registered at T fires
    /// once processing time reaches T, so cleared state is observable at `now >= T`. Returns the
    /// bytes reclaimed, or None when the deadline has not passed (or none is registered).
    fn expire_if_due(&mut self, key: &GroupKey, now_ms: i64, track: bool) -> Option<usize> {
        match self.cleanup_state.get(key) {
            Some(&deadline) if now_ms >= deadline => Some(self.clear_key(key, track)),
            _ => None,
        }
    }

    /// Reclaims every key whose deadline passed with no further touch — the lazy check never sees
    /// such a key again. Silent, at most once per min-retention period.
    fn maybe_sweep(&mut self, now_ms: i64) {
        if now_ms < self.last_sweep_ms.saturating_add(self.min_retention_ms) {
            return;
        }
        let due: Vec<GroupKey> = self
            .cleanup_state
            .iter()
            .filter(|(_, &deadline)| now_ms >= deadline)
            .map(|(key, _)| key.clone())
            .collect();
        let track = self.memory.tracking();
        let mut freed = 0usize;
        for key in &due {
            freed += self.clear_key(key, track);
        }
        self.memory.forget(freed);
        self.memory.account_shrink();
        self.last_sweep_ms = now_ms;
    }

    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_store(mut self, store: crate::state::RocksTemporalJoinStore) -> Self {
        self.store = Some(store);
        self
    }

    /// Attaches the task off-heap budget for a backend that starts with nothing resident.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory.attach("temporal-join", budget_bytes, 0)?;
        Ok(self)
    }

    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn store_mut(&mut self) -> &mut crate::state::RocksTemporalJoinStore {
        self.store.as_mut().expect("temporal-join rocksdb store")
    }

    /// Restore-time enable-retention migration for the persistent path, exactly as `restore`
    /// stamps a raw snapshot: every key holding state on either side without a restored deadline
    /// is stamped a full max retention from the restore instead of expiring on first touch.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn adopt_store_retention(&mut self, now_ms: i64) -> Result<(), DataFusionError> {
        if self.cleaning_enabled() {
            let stamp = now_ms.saturating_add(self.max_retention_ms);
            self.store_mut().adopt_retention(stamp)?;
        }
        Ok(())
    }

    /// Bounds both sides' state by the operator's task off-heap budget (negative = unaccounted),
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
                    + versions
                        .values()
                        .map(|(row, _)| right_version_bytes(row))
                        .sum::<usize>()
            })
            .sum();
        let deadlines: usize = self.cleanup_state.keys().map(group_key_bytes).sum();
        self.memory
            .attach("temporal-join", budget_bytes, left + right + deadlines)?;
        Ok(self)
    }

    fn left_types(&self) -> Vec<DataType> {
        self.left_schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect()
    }

    fn right_types(&self) -> Vec<DataType> {
        self.right_schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect()
    }

    /// Buffers a probe-side batch (no output until a watermark). Each row is stored under its
    /// equi-join key with its event time and changelog kind, in arrival order within the key.
    /// `now_ms` is the host's processing-time reading — the cleanup-deadline clock.
    pub(crate) fn push_left(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<(), DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return self.push_store(batch, true, now_ms);
        }
        let cleaning = self.cleaning_enabled();
        if cleaning {
            self.maybe_sweep(now_ms);
        }
        let arity = data_arity(batch);
        let key_arrays: Vec<&ArrayRef> = self.left_keys.iter().map(|&i| batch.column(i)).collect();
        let times = rt_to_millis(batch.column(self.left_time));
        let kinds = row_kind_column(batch);
        let track = self.memory.tracking();
        let mut delta = 0isize;
        for row in 0..batch.num_rows() {
            let key = read_key(&key_arrays, row);
            let jrow: JoinRow = (0..arity)
                .map(|i| {
                    ScalarValue::try_from_array(batch.column(i), row).expect("temporal left scalar")
                })
                .collect();
            if cleaning {
                if let Some(reclaimed) = self.expire_if_due(&key, now_ms, track) {
                    delta -= reclaimed as isize;
                }
                self.register_cleanup(&key, now_ms, &mut delta, track);
            }
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
    pub(crate) fn push_right(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<(), DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return self.push_store(batch, false, now_ms);
        }
        let cleaning = self.cleaning_enabled();
        if cleaning {
            self.maybe_sweep(now_ms);
        }
        let arity = data_arity(batch);
        let key_arrays: Vec<&ArrayRef> = self.right_keys.iter().map(|&i| batch.column(i)).collect();
        let times = rt_to_millis(batch.column(self.right_time));
        let kinds = row_kind_column(batch);
        let track = self.memory.tracking();
        let mut delta = 0isize;
        for row in 0..batch.num_rows() {
            let key = read_key(&key_arrays, row);
            let jrow: JoinRow = (0..arity)
                .map(|i| {
                    ScalarValue::try_from_array(batch.column(i), row)
                        .expect("temporal right scalar")
                })
                .collect();
            if cleaning {
                if let Some(reclaimed) = self.expire_if_due(&key, now_ms, track) {
                    delta -= reclaimed as isize;
                }
                self.register_cleanup(&key, now_ms, &mut delta, track);
            }
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
    /// `now_ms` is the host's processing-time reading — the cleanup-deadline clock.
    pub(crate) fn advance(
        &mut self,
        watermark: i64,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return self.advance_store(watermark, now_ms);
        }
        let cleaning = self.cleaning_enabled();
        if cleaning {
            self.maybe_sweep(now_ms);
        }
        let has_pred = self.predicate.is_some();

        // Resolve each triggered left row to the build version valid at its time (an accumulate
        // version with the largest rightTime <= leftTime), collecting candidate pairs for one batched
        // predicate evaluation. Triggered rows are removed; later rows stay buffered.
        let mut decisions: Vec<(JoinRow, i8, Option<JoinRow>)> = Vec::new();
        let mut pred_pairs: Vec<JoinRow> = Vec::new();
        let mut pred_idx: Vec<usize> = Vec::new();
        let track = self.memory.tracking();
        let mut freed = 0usize;
        let mut grew = 0isize;
        let keys: Vec<GroupKey> = self.left_state.keys().cloned().collect();
        for key in &keys {
            // A passed cleanup deadline means Flink's timer fired (at wall time T, before this
            // watermark arrived at wall time now >= T): the key's state is gone, nothing emits.
            if cleaning {
                if let Some(reclaimed) = self.expire_if_due(key, now_ms, track) {
                    freed += reclaimed;
                    continue;
                }
            }
            let entries = self.left_state.remove(key).expect("left key present");
            let versions = self.right_state.get(key);
            let total = entries.len();
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
            let fired = remaining.len() < total;
            let remaining_empty = remaining.is_empty();
            if remaining_empty {
                if track {
                    freed += group_key_bytes(key);
                }
            } else {
                self.left_state.insert(key.clone(), remaining);
            }
            // Flink's `onEventTime` after emitting: state remaining on either side re-registers
            // the key's cleanup deadline; a key left empty on both sides drops it (Flink's
            // `cleanupLastTimer`). Version pruning below keeps at least the latest version, so
            // checking the right side before pruning matches Flink's after-prune check.
            if cleaning && fired {
                let right_live = self.right_state.get(key).is_some_and(|m| !m.is_empty());
                if !remaining_empty || right_live {
                    self.register_cleanup(key, now_ms, &mut grew, track);
                } else if self.cleanup_state.remove(key).is_some() && track {
                    freed += group_key_bytes(key);
                }
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
        if grew > 0 {
            self.memory.record(grew);
            self.memory.account()?;
        }
        self.memory.forget(freed);
        self.memory.account_shrink();

        Ok(self.finish_advance(decisions, pred_pairs, pred_idx))
    }

    /// The backend-independent tail of a watermark firing: the batched residual-predicate
    /// evaluation over the resolved candidates, then the output batch — a matched pair, a
    /// null-pad (LEFT), or nothing (INNER) per triggered probe row, each carrying its row's kind.
    fn finish_advance(
        &mut self,
        mut decisions: Vec<(JoinRow, i8, Option<JoinRow>)>,
        pred_pairs: Vec<JoinRow>,
        pred_idx: Vec<usize>,
    ) -> RecordBatch {
        let left_outer = self.join_type == JoinKind::LeftOuter;
        // A candidate that fails the residual non-equi predicate is not a match (Flink's
        // `joinCondition.apply`), so it falls back to a null-pad (LEFT) or is dropped (INNER).
        if !pred_pairs.is_empty() {
            let joined = joined_schema(&self.left_schema, &self.right_schema);
            let mask = self
                .predicate
                .as_mut()
                .expect("predicate present")
                .evaluate(&joined, &pred_pairs);
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
                    out_rows.push(
                        left.into_iter()
                            .chain(right_nulls.iter().cloned())
                            .collect(),
                    );
                    out_kinds.push(kind);
                }
                None => {}
            }
        }
        if out_rows.is_empty() {
            return empty_batch();
        }
        let types: Vec<DataType> = self
            .left_types()
            .into_iter()
            .chain(self.right_types())
            .collect();
        let mut fields: Vec<Field> = (0..types.len())
            .map(|j| Field::new(format!("c{j}"), types[j].clone(), true))
            .collect();
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
            .map(|j| {
                scalars_to_array(
                    rows.iter().map(|r| r[j].clone()).collect(),
                    fields[j].data_type(),
                )
            })
            .collect();
        fields.push(Field::new("__time__", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(times.to_vec())));
        fields.push(Field::new("__kind__", DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(kinds.to_vec())));
        write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("temporal side"),
        )
    }

    /// Serializes the per-key cleanup deadlines as `[key cols.., __cleanup_at__]` (empty when none).
    fn serialize_cleanup(&self) -> Vec<u8> {
        if self.cleanup_state.is_empty() {
            return Vec::new();
        }
        let key_types: Vec<DataType> = self
            .left_keys
            .iter()
            .map(|&i| self.left_schema.field(i).data_type().clone())
            .collect();
        let keys: Vec<GroupKey> = self.cleanup_state.keys().cloned().collect();
        let deadlines: Vec<i64> = keys.iter().map(|key| self.cleanup_state[key]).collect();
        let mut fields = key_fields(&key_types);
        let mut columns = key_columns(&keys, &key_types);
        fields.push(Field::new(CLEANUP_AT_COLUMN, DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(deadlines)));
        write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("temporal cleanup deadlines"),
        )
    }

    pub(crate) fn snapshot(&self) -> Vec<u8> {
        let mut sections = vec![self.serialize_left(), self.serialize_right()];
        // The deadline section rides only while cleaning is on, keeping retention-off
        // checkpoints byte-identical to the pre-TTL format.
        if self.cleaning_enabled() {
            sections.push(self.serialize_cleanup());
        }
        Self::snapshot_parts(sections)
    }

    fn snapshot_parts(sections: Vec<Vec<u8>>) -> Vec<u8> {
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
        // The deadline section's key columns lead its batch, in equi-key order.
        let cleanup_keys: Vec<usize> = (0..self.left_keys.len()).collect();
        let cleanup = sections.get(2).map(|bytes| {
            Self::side_raw_partitions(bytes, &cleanup_keys, max_parallelism, timestamp_precisions)
        });
        let mut groups: Vec<i32> = left.keys().chain(right.keys()).copied().collect();
        groups.sort_unstable();
        groups.dedup();
        let mut snapshots = BTreeMap::new();
        for key_group in groups {
            let mut parts = vec![
                left.get(&key_group)
                    .map(Self::merge_snapshot_batches)
                    .unwrap_or_default(),
                right
                    .get(&key_group)
                    .map(Self::merge_snapshot_batches)
                    .unwrap_or_default(),
            ];
            if let Some(cleanup) = &cleanup {
                parts.push(
                    cleanup
                        .get(&key_group)
                        .map(Self::merge_snapshot_batches)
                        .unwrap_or_default(),
                );
            }
            snapshots.insert(key_group, Self::snapshot_parts(parts));
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
                        take(column, &indices, None).expect("partition temporal snapshot")
                    })
                    .collect();
                partitions.entry(key_group).or_insert_with(Vec::new).push(
                    RecordBatch::try_new(batch.schema(), columns)
                        .expect("partitioned temporal snapshot"),
                );
            }
        }
        partitions
    }

    fn merge_snapshot_batches(batches: &Vec<RecordBatch>) -> Vec<u8> {
        write_ipc(
            &concat_batches(&batches[0].schema(), batches.iter())
                .expect("merge temporal raw partitions"),
        )
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
        min_retention_ms: i64,
        restored_at_ms: i64,
    ) -> Self {
        let mut joiner = TemporalJoiner::new(
            left_keys,
            right_keys,
            left_time,
            right_time,
            join_type,
            left_schema,
            right_schema,
            predicate,
        )
        .with_state_retention(min_retention_ms);
        if bytes.is_empty() {
            return joiner;
        }
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
            let key_arrays: Vec<&ArrayRef> =
                joiner.left_keys.iter().map(|&i| batch.column(i)).collect();
            for row in 0..batch.num_rows() {
                let key = read_key(&key_arrays, row);
                let jrow: JoinRow = (0..arity)
                    .map(|i| {
                        ScalarValue::try_from_array(batch.column(i), row)
                            .expect("temporal left scalar")
                    })
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
            let key_arrays: Vec<&ArrayRef> =
                joiner.right_keys.iter().map(|&i| batch.column(i)).collect();
            for row in 0..batch.num_rows() {
                let key = read_key(&key_arrays, row);
                let jrow: JoinRow = (0..arity)
                    .map(|i| {
                        ScalarValue::try_from_array(batch.column(i), row)
                            .expect("temporal right scalar")
                    })
                    .collect();
                joiner
                    .right_state
                    .entry(key)
                    .or_default()
                    .insert(times.value(row), (jrow, kinds.value(row)));
            }
        }
        if let Some(bytes) = sections.get(2) {
            for batch in read_ipc_if_present(bytes) {
                let deadlines = column_i64(&batch, CLEANUP_AT_COLUMN);
                let key_arrays: Vec<&ArrayRef> = (0..batch.num_columns() - 1)
                    .map(|i| batch.column(i))
                    .collect();
                for row in 0..batch.num_rows() {
                    joiner
                        .cleanup_state
                        .insert(read_key(&key_arrays, row), deadlines.value(row));
                }
            }
        }
        // Enable-retention migration: a key restored without a deadline (a pre-retention writer)
        // is stamped a full max retention from the restore instead of expiring on first touch.
        if joiner.cleaning_enabled() {
            let stamp = restored_at_ms.saturating_add(joiner.max_retention_ms);
            let TemporalJoiner {
                left_state,
                right_state,
                cleanup_state,
                ..
            } = &mut joiner;
            for key in left_state.keys().chain(right_state.keys()) {
                if !cleanup_state.contains_key(key) {
                    cleanup_state.insert(key.clone(), stamp);
                }
            }
        }
        joiner
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore_partitions(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_time: usize,
        right_time: usize,
        join_type: JoinKind,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        predicate: Option<JoinPredicate>,
        snapshots: &[Vec<u8>],
        min_retention_ms: i64,
        restored_at_ms: i64,
    ) -> Self {
        let mut left_batches = Vec::new();
        let mut right_batches = Vec::new();
        let mut cleanup_batches = Vec::new();
        for bytes in snapshots {
            let sections = read_framed_sections(bytes);
            if sections.len() >= 2 {
                left_batches.extend(read_ipc_if_present(&sections[0]));
                right_batches.extend(read_ipc_if_present(&sections[1]));
                if let Some(cleanup) = sections.get(2) {
                    cleanup_batches.extend(read_ipc_if_present(cleanup));
                }
            }
        }
        let merge = |batches: &Vec<RecordBatch>| {
            (!batches.is_empty())
                .then(|| Self::merge_snapshot_batches(batches))
                .unwrap_or_default()
        };
        let mut parts = vec![merge(&left_batches), merge(&right_batches)];
        if !cleanup_batches.is_empty() {
            parts.push(merge(&cleanup_batches));
        }
        TemporalJoiner::restore(
            left_keys,
            right_keys,
            left_time,
            right_time,
            join_type,
            left_schema,
            right_schema,
            predicate,
            &Self::snapshot_parts(parts),
            min_retention_ms,
            restored_at_ms,
        )
    }
}

#[cfg(feature = "rocksdb-state")]
impl TemporalJoiner {
    /// Persistent-state arrival path, shared by both sides: with cleaning on, every touched key
    /// expires first if its deadline passed (as the memory path does per row) and re-arms under
    /// the hysteresis; then probe rows append under fresh sequences and build rows upsert per
    /// (key, version). Nothing is emitted here — emission is watermark-driven.
    fn push_store(
        &mut self,
        batch: &RecordBatch,
        left: bool,
        now_ms: i64,
    ) -> Result<(), DataFusionError> {
        let cleaning = self.cleaning_enabled();
        if cleaning {
            self.store_sweep(now_ms)?;
        }
        let key_columns = if left {
            self.left_keys.clone()
        } else {
            self.right_keys.clone()
        };
        let time = if left {
            self.left_time
        } else {
            self.right_time
        };
        let times = rt_to_millis(batch.column(time));
        let precisions = self.key_timestamp_precisions.clone();
        let entry_keys = self
            .store_mut()
            .entry_keys(batch, &key_columns, &precisions);
        if cleaning {
            self.store_cleaning_touch(&entry_keys, now_ms)?;
        }
        let kinds = row_kind_column(batch);
        let store = self.store.as_mut().expect("temporal-join rocksdb store");
        if left {
            store.push_left(batch, &entry_keys, &times, kinds)
        } else {
            store.push_right(batch, &entry_keys, &times, kinds)
        }
    }

    /// The lazy expiry check plus re-arm at a batch of key touches. Every row of a batch shares
    /// one clock reading, so per key only the first touch can expire and the re-arm is idempotent
    /// within the batch — distinct keys once each is the memory path's per-row loop exactly.
    fn store_cleaning_touch(
        &mut self,
        entry_keys: &[ByteKey],
        now_ms: i64,
    ) -> Result<(), DataFusionError> {
        let mut seen: HashSet<ByteKey> = HashSet::default();
        for key in entry_keys {
            if !seen.insert(key.clone()) {
                continue;
            }
            if self
                .store_mut()
                .deadline(key)
                .is_some_and(|deadline| now_ms >= deadline)
            {
                self.store_mut().clear_key(key)?;
            }
            self.store_register_cleanup(key, now_ms)?;
        }
        Ok(())
    }

    /// `register_cleanup` for the persistent path — the same hysteresis over the store's resident
    /// deadline map, with a moved or created deadline written through to the deadlines table.
    fn store_register_cleanup(
        &mut self,
        key: &ByteKey,
        now_ms: i64,
    ) -> Result<(), DataFusionError> {
        let min_retention_ms = self.min_retention_ms;
        let max_retention_ms = self.max_retention_ms;
        let store = self.store_mut();
        match store.deadline(key) {
            Some(deadline) if now_ms.saturating_add(min_retention_ms) <= deadline => Ok(()),
            _ => store.set_deadline(key, now_ms.saturating_add(max_retention_ms)),
        }
    }

    /// `maybe_sweep` for the persistent path: reclaims every key whose deadline passed with no
    /// further touch. Silent, at most once per min-retention period.
    fn store_sweep(&mut self, now_ms: i64) -> Result<(), DataFusionError> {
        if now_ms < self.last_sweep_ms.saturating_add(self.min_retention_ms) {
            return Ok(());
        }
        for key in self.store_mut().due_keys(now_ms) {
            self.store_mut().clear_key(&key)?;
        }
        self.last_sweep_ms = now_ms;
        Ok(())
    }

    /// Persistent-state firing: the probe table's key-major scan yields every buffered left row
    /// per key in arrival order; each fired key resolves against its scanned version list (the
    /// memory path's ordered lookup), fired rows delete, and every key's stale versions prune
    /// under the exact memory-path bound. With cleaning on, a fired key whose deadline passed is
    /// decided BEFORE its rows resolve: Flink's timer fired before this watermark arrived, so the
    /// key's state is gone and its rows emit NOTHING — even for a LEFT join.
    fn advance_store(
        &mut self,
        watermark: i64,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        let cleaning = self.cleaning_enabled();
        if cleaning {
            self.store_sweep(now_ms)?;
        }
        let left = self.store_mut().scan_left()?;
        let right = self.store_mut().scan_right()?;
        let has_pred = self.predicate.is_some();
        let mut fired_rows: Vec<(&[u8], i8)> = Vec::new();
        let mut fired_versions: Vec<Option<&[u8]>> = Vec::new();
        for (key, entries) in &left {
            if cleaning
                && self
                    .store_mut()
                    .deadline(key)
                    .is_some_and(|deadline| now_ms >= deadline)
            {
                self.store_mut().clear_key(key)?;
                continue;
            }
            let versions = right.get(key);
            let mut fired_seqs: Vec<u64> = Vec::new();
            let mut remaining = 0usize;
            for entry in entries {
                if entry.time > watermark {
                    remaining += 1;
                    continue;
                }
                fired_seqs.push(entry.seq);
                let valid = versions.and_then(|versions| {
                    let at = versions.partition_point(|version| version.ts <= entry.time);
                    versions[..at].last().and_then(|version| {
                        // Only an accumulate version (+I/+U) is a row; a -D/-U marks "no row here".
                        (version.kind == 0 || version.kind == 2).then_some(version.row.as_ref())
                    })
                });
                fired_rows.push((entry.row.as_ref(), entry.kind));
                fired_versions.push(valid);
            }
            if fired_seqs.is_empty() {
                continue;
            }
            self.store_mut().remove_left(key, &fired_seqs)?;
            // Flink's `onEventTime` after emitting: state remaining on either side re-registers
            // the key's cleanup deadline; a key left empty on both sides drops it. Version pruning
            // below keeps at least the latest version, so checking the right side before pruning
            // matches Flink's after-prune check.
            if cleaning {
                let right_live = versions.is_some_and(|versions| !versions.is_empty());
                if remaining > 0 || right_live {
                    self.store_register_cleanup(key, now_ms)?;
                } else {
                    self.store_mut().remove_deadline(key)?;
                }
            }
        }

        // Drop versions older than the latest one still valid at the watermark, on every key —
        // the memory path prunes its whole map per advance. Resolution above already read the
        // unpruned scan, as the memory path resolves before pruning.
        for (key, versions) in &right {
            let keep_from = versions.partition_point(|version| version.ts <= watermark);
            if keep_from > 1 {
                let stale: Vec<i64> = versions[..keep_from - 1]
                    .iter()
                    .map(|version| version.ts)
                    .collect();
                self.store_mut().remove_right(key, &stale)?;
            }
        }

        let mut decisions: Vec<(JoinRow, i8, Option<JoinRow>)> =
            Vec::with_capacity(fired_rows.len());
        let mut pred_pairs: Vec<JoinRow> = Vec::new();
        let mut pred_idx: Vec<usize> = Vec::new();
        if !fired_rows.is_empty() {
            let store = self.store.as_ref().expect("temporal-join rocksdb store");
            let lefts = store.decode(
                true,
                &self.left_schema.clone(),
                fired_rows.iter().map(|(row, _)| *row),
            )?;
            let rights = store.decode(
                false,
                &self.right_schema.clone(),
                fired_versions.iter().flatten().copied(),
            )?;
            let mut right_row = 0usize;
            for (index, (_, kind)) in fired_rows.iter().enumerate() {
                let left_row: JoinRow = (0..lefts.num_columns())
                    .map(|column| {
                        ScalarValue::try_from_array(lefts.column(column), index)
                            .expect("temporal left scalar")
                    })
                    .collect();
                let valid: Option<JoinRow> = fired_versions[index].map(|_| {
                    let row: JoinRow = (0..rights.num_columns())
                        .map(|column| {
                            ScalarValue::try_from_array(rights.column(column), right_row)
                                .expect("temporal right scalar")
                        })
                        .collect();
                    right_row += 1;
                    row
                });
                let at = decisions.len();
                if has_pred {
                    if let Some(row) = &valid {
                        pred_pairs.push(left_row.iter().chain(row).cloned().collect());
                        pred_idx.push(at);
                    }
                }
                decisions.push((left_row, *kind, valid));
            }
        }
        Ok(self.finish_advance(decisions, pred_pairs, pred_idx))
    }

    /// Decodes restored blob key groups once at open and writes them through the typed store, so
    /// a canonical or raw restore continues on the direct persistent path. Probe rows append in
    /// blob order (fresh sequences reproduce arrival order per key), build versions key by their
    /// version timestamp, and deadlines land in the deadline table; `adopt_store_retention` then
    /// applies the enable-retention migration exactly as the memory restore does.
    pub(crate) fn import_partitions(
        &mut self,
        snapshots: &[Vec<u8>],
    ) -> Result<(), DataFusionError> {
        for bytes in snapshots {
            if bytes.is_empty() {
                continue;
            }
            let sections = read_framed_sections(bytes);
            for (left, section) in [(true, &sections[0]), (false, &sections[1])] {
                let key_columns = if left {
                    self.left_keys.clone()
                } else {
                    self.right_keys.clone()
                };
                for batch in read_ipc_if_present(section) {
                    let arity = batch.num_columns() - 2;
                    let times = column_i64(&batch, "__time__");
                    let kinds = batch
                        .column_by_name("__kind__")
                        .expect("__kind__")
                        .as_any()
                        .downcast_ref::<Int8Array>()
                        .expect("__kind__ i8")
                        .clone();
                    let data = batch.project(&(0..arity).collect::<Vec<_>>())?;
                    let store = self.store.as_mut().expect("temporal-join rocksdb store");
                    let entry_keys =
                        store.entry_keys(&data, &key_columns, &self.key_timestamp_precisions);
                    if left {
                        store.push_left(&data, &entry_keys, &times, Some(&kinds))?;
                    } else {
                        store.push_right(&data, &entry_keys, &times, Some(&kinds))?;
                    }
                }
            }
            if let Some(section) = sections.get(2) {
                for batch in read_ipc_if_present(section) {
                    let deadlines = column_i64(&batch, CLEANUP_AT_COLUMN);
                    let key_columns: Vec<usize> = (0..batch.num_columns() - 1).collect();
                    let store = self.store.as_mut().expect("temporal-join rocksdb store");
                    let entry_keys =
                        store.entry_keys(&batch, &key_columns, &self.key_timestamp_precisions);
                    for (row, key) in entry_keys.iter().enumerate() {
                        store.set_deadline(key, deadlines.value(row))?;
                    }
                }
            }
        }
        Ok(())
    }

    /// The complete persistent state in the blob snapshot's per-key-group framed-section encoding
    /// (probe rows, build versions, and — while cleaning is on — the deadline section), for
    /// backend-independent canonical savepoints.
    pub(crate) fn canonical_partitions(&self) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError> {
        let store = self.store.as_ref().expect("temporal-join rocksdb store");
        let left = store.scan_left()?;
        let right = store.scan_right()?;
        type Side<'a> = BTreeMap<i32, (Vec<&'a [u8]>, Vec<i64>, Vec<i8>)>;
        let mut left_by_group: Side = BTreeMap::new();
        for (key, entries) in &left {
            let slot = left_by_group
                .entry(crate::state::RocksTemporalJoinStore::key_group(key))
                .or_default();
            for entry in entries {
                slot.0.push(&entry.row);
                slot.1.push(entry.time);
                slot.2.push(entry.kind);
            }
        }
        let mut right_by_group: Side = BTreeMap::new();
        for (key, versions) in &right {
            let slot = right_by_group
                .entry(crate::state::RocksTemporalJoinStore::key_group(key))
                .or_default();
            for version in versions {
                slot.0.push(&version.row);
                slot.1.push(version.ts);
                slot.2.push(version.kind);
            }
        }
        let cleaning = self.cleaning_enabled();
        let mut cleanup_by_group: BTreeMap<i32, Vec<(&ByteKey, i64)>> = BTreeMap::new();
        if cleaning {
            for (key, &deadline) in store.all_deadlines() {
                cleanup_by_group
                    .entry(crate::state::RocksTemporalJoinStore::key_group(key))
                    .or_default()
                    .push((key, deadline));
            }
        }
        let mut groups: Vec<i32> = left_by_group
            .keys()
            .chain(right_by_group.keys())
            .copied()
            .collect();
        groups.sort_unstable();
        groups.dedup();
        let mut snapshots = BTreeMap::new();
        for group in groups {
            let side = |left_side: bool, sections: &Side| -> Result<Vec<u8>, DataFusionError> {
                let Some((rows, times, kinds)) = sections.get(&group) else {
                    return Ok(Vec::new());
                };
                let schema = if left_side {
                    &self.left_schema
                } else {
                    &self.right_schema
                };
                let data = store.decode(left_side, schema, rows.iter().copied())?;
                let mut fields: Vec<Field> =
                    schema.fields().iter().map(|f| f.as_ref().clone()).collect();
                let mut columns = data.columns().to_vec();
                fields.push(Field::new("__time__", DataType::Int64, false));
                columns.push(Arc::new(Int64Array::from(times.clone())));
                fields.push(Field::new("__kind__", DataType::Int8, false));
                columns.push(Arc::new(Int8Array::from(kinds.clone())));
                Ok(write_ipc(
                    &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                        .expect("temporal side"),
                ))
            };
            let mut parts = vec![side(true, &left_by_group)?, side(false, &right_by_group)?];
            if cleaning {
                let cleanup = match cleanup_by_group.get(&group) {
                    Some(entries) => {
                        let keys: Vec<&ByteKey> = entries.iter().map(|(key, _)| *key).collect();
                        let deadlines: Vec<i64> =
                            entries.iter().map(|(_, deadline)| *deadline).collect();
                        let key_types: Vec<DataType> = self
                            .left_keys
                            .iter()
                            .map(|&i| self.left_schema.field(i).data_type().clone())
                            .collect();
                        let mut fields = key_fields(&key_types);
                        let mut columns = store.decode_key_columns(&keys);
                        fields.push(Field::new(CLEANUP_AT_COLUMN, DataType::Int64, false));
                        columns.push(Arc::new(Int64Array::from(deadlines)));
                        write_ipc(
                            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                                .expect("temporal cleanup deadlines"),
                        )
                    }
                    None => Vec::new(),
                };
                parts.push(cleanup);
            }
            snapshots.insert(group, Self::snapshot_parts(parts));
        }
        Ok(snapshots)
    }
}

state_bytes_getter!(
    Java_tech_streamfusion_Native_temporalJoinerStateBytes,
    TemporalJoiner
);

/// Creates an event-time temporal-table joiner (`FOR SYSTEM_TIME AS OF probe.rowtime`) and returns an
/// opaque handle. `left_time`/`right_time` locate the rowtime column on each side; the two schema
/// addresses seed the per-side data schemas (so a LEFT join can type the null-padding); the encoded
/// arrays carry the optional residual non-equi predicate. The JVM owns the handle and must release it
/// with the matching close.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createTemporalJoiner<'local>(
    env: JNIEnv<'local>,
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
    state_ttl_millis: jlong,
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
        .with_state_retention(state_ttl_millis)
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, joiner)
    })
}

/// Buffers a probe-side (left) batch (no output until a watermark).
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushLeftTemporalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            joiner.push_left(&batch, now_millis)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Folds a build-side (right) changelog batch into the versioned state (no output until a watermark).
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRightTemporalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            joiner.push_right(&batch, now_millis)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Advances the watermark, emitting the joined rows for buffered probe rows it has passed and dropping
/// obsolete build versions.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_advanceTemporalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
        // Fallible in persistent-state mode (the firing reads the committed tables).
        match joiner.advance(watermark_millis, now_millis) {
            Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Releases the temporal joiner and its native state.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeTemporalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<TemporalJoiner>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotTemporalJoinerPartitions<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &*(handle as *const TemporalJoiner) };
        let precisions = read_i32_array(&env, &timestamp_precisions);
        keyed_state_partition_array(
            &mut env,
            joiner.snapshot_partitions(max_parallelism as usize, &precisions),
            "temporal-join",
        )
    })
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_tech_streamfusion_Native_restoreTemporalJoinerPartitions<'local>(
    env: JNIEnv<'local>,
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
    state_ttl_millis: jlong,
    now_millis: jlong,
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
            .expect("read temporal raw partition count");
        let mut restored = Vec::with_capacity(count as usize);
        for index in 0..count {
            let bytes = JByteArray::from(
                env.get_object_array_element(&snapshots, index)
                    .expect("read temporal raw partition"),
            );
            restored.push(
                env.convert_byte_array(&bytes)
                    .expect("read temporal raw partition bytes"),
            );
        }
        let joiner = TemporalJoiner::restore_partitions(
            left,
            right,
            left_time as usize,
            right_time as usize,
            JoinKind::from_code(join_type),
            left_schema,
            right_schema,
            predicate,
            &restored,
            state_ttl_millis,
            now_millis,
        )
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, joiner)
    })
}
