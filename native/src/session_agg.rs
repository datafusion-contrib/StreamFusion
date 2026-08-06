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
    /// Persistent-state mode: committed sessions live in the Paimon store; the decoded map holds
    /// only this interval's touched keys (seeded on first touch, staged wholesale at the barrier).
    #[cfg(feature = "paimon-state")]
    backend: Option<crate::state::PaimonSessionAggStore>,
    key_timestamp_precisions: Vec<i32>,
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
            #[cfg(feature = "paimon-state")]
            backend: None,
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

    #[cfg(feature = "paimon-state")]
    pub(crate) fn with_backend(mut self, store: crate::state::PaimonSessionAggStore) -> Self {
        self.backend = Some(store);
        self
    }

    /// Attaches the task off-heap budget for a backend that starts with nothing resident.
    #[cfg(feature = "paimon-state")]
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory.attach("session-aggregate", budget_bytes, 0)?;
        Ok(self)
    }

    #[cfg(feature = "paimon-state")]
    pub(crate) fn store_mut(&mut self) -> &mut crate::state::PaimonSessionAggStore {
        self.backend.as_mut().expect("session-agg paimon backend")
    }

    /// The key-field timestamp descriptors, defaulting to non-timestamp per key column — the
    /// aggregator learns its key arity from batches, not at construction.
    #[cfg(feature = "paimon-state")]
    fn key_precisions(&self, arity: usize) -> Vec<i32> {
        if self.key_timestamp_precisions.is_empty() {
            vec![-1; arity]
        } else {
            self.key_timestamp_precisions.clone()
        }
    }

    /// Persistent-state seeding: a key's first touch this interval reads its committed sessions
    /// into the decoded map through the per-batch key probe, so merges and firings see state
    /// written before the last barrier.
    #[cfg(feature = "paimon-state")]
    fn seed_batch_keys(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        if self.backend.is_none() || batch.num_rows() == 0 {
            return Ok(());
        }
        let schema = batch.schema();
        let key_indices: Vec<usize> = (0..)
            .map_while(|j| schema.index_of(&format!("key{j}")).ok())
            .collect();
        let precisions = self.key_precisions(key_indices.len());
        let mut encoder = BinaryRowBatchEncoder::new(batch, &key_indices, &precisions);
        let mut seen: std::collections::HashSet<ByteKey> = std::collections::HashSet::new();
        let mut unique: Vec<ByteKey> = Vec::new();
        for row in 0..batch.num_rows() {
            let key = encoder.encode(row);
            if !seen.contains(key) {
                let owned = ByteKey::from(key);
                seen.insert(owned.clone());
                unique.push(owned);
            }
        }
        let batches =
            self.backend.as_mut().expect("session-agg paimon backend").seed_scan(&unique)?;
        self.absorb_committed(batches)?;
        let delta =
            self.backend.as_mut().expect("session-agg paimon backend").footprint_delta();
        self.memory.record(delta);
        self.memory.account()
    }

    /// Reads committed (key, session) rows into the decoded map — the restore path's own
    /// merge_batch round trip. Rows arrive only for keys whose map is absent or being seeded, so
    /// inserts are unconditional (committed sessions are pairwise separated).
    #[cfg(feature = "paimon-state")]
    fn absorb_committed(&mut self, batches: Vec<RecordBatch>) -> Result<(), DataFusionError> {
        let field_counts: Vec<usize> =
            self.aggregates.iter().map(|a| a.state_fields().len()).collect();
        let state_total: usize = field_counts.iter().sum();
        let track = self.memory.tracking();
        for batch in batches {
            let arity = batch.num_columns() - 4 - state_total;
            let wss = batch.column(2).as_any().downcast_ref::<Int64Array>().expect("ws column");
            let wes = batch.column(3).as_any().downcast_ref::<Int64Array>().expect("we column");
            let key_arrays: Vec<&ArrayRef> = (0..arity).map(|j| batch.column(4 + j)).collect();
            self.key_types = key_types(&key_arrays);
            let keys_encoded =
                encode_keys(&mut self.key_converter, &key_arrays, batch.num_rows());
            for row in 0..batch.num_rows() {
                let key = keys_encoded.row(row).owned();
                let mut accumulators: Vec<Box<dyn Accumulator>> =
                    self.aggregates.iter().map(WindowAggregate::create_accumulator).collect();
                let mut column = 4 + arity;
                for (i, accumulator) in accumulators.iter_mut().enumerate() {
                    let count = field_counts[i];
                    let state: Vec<ArrayRef> = (column..column + count)
                        .map(|c| batch.column(c).slice(row, 1))
                        .collect();
                    accumulator.merge_batch(&state).expect("failed to seed session");
                    column += count;
                }
                let session = Session { end: wes.value(row), accumulators };
                let mut delta = 0isize;
                if track {
                    delta = session_bytes(&session) as isize
                        + if self.sessions.contains_key(&key) {
                            0
                        } else {
                            owned_row_bytes(&key) as isize
                        };
                }
                self.sessions.entry(key).or_default().insert(wss.value(row), session);
                if track {
                    self.memory.record(delta);
                }
            }
        }
        Ok(())
    }

    /// Persistent-state barrier: stages every open (key, session) as a whole-row rewrite plus a
    /// tombstone per committed start a merge consumed, drops the decoded map, and commits the
    /// region. Returns the manifest (the token is the plain snapshot id — the memory path
    /// persists no watermark).
    #[cfg(feature = "paimon-state")]
    pub(crate) fn checkpoint_backend(
        &mut self,
    ) -> Result<crate::state::PaimonCheckpointManifest, DataFusionError> {
        let state_types: Vec<DataType> = self
            .aggregates
            .iter()
            .flat_map(|a| a.state_fields().into_iter().map(|f| f.data_type().clone()))
            .collect();
        if self.memory.tracking() {
            let state: usize = self
                .sessions
                .iter()
                .map(|(key, map)| {
                    owned_row_bytes(key) + map.values().map(session_bytes).sum::<usize>()
                })
                .sum();
            self.memory.forget(state);
        }
        let sessions = std::mem::take(&mut self.sessions);
        for (key, mut map) in sessions {
            let rows = map.len();
            let keys = vec![key.clone(); rows];
            let key_columns = decode_keys(self.key_converter.as_ref(), &keys, &self.key_types);
            let precisions = self.key_precisions(self.key_types.len());
            let binary_keys =
                crate::window_agg::binary_row_keys(&key_columns, &self.key_types, &precisions, rows)?;
            let key_slices: Vec<&[u8]> = binary_keys.iter().map(|k| k.as_slice()).collect();
            let wss: Vec<i64> = map.keys().copied().collect();
            let wes: Vec<i64> = map.values().map(|s| s.end).collect();
            let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); state_types.len()];
            for session in map.values_mut() {
                let mut column = 0;
                for accumulator in session.accumulators.iter_mut() {
                    for scalar in accumulator.state().expect("state") {
                        state_columns[column].push(scalar);
                        column += 1;
                    }
                }
            }
            let state_arrays: Vec<ArrayRef> = state_columns
                .into_iter()
                .zip(&state_types)
                .map(|(scalars, data_type)| scalars_to_array(scalars, data_type))
                .collect();
            let store = self.backend.as_mut().expect("session-agg paimon backend");
            store.stage_upserts(&key_slices, &wss, &wes, key_columns, state_arrays)?;
            // Committed starts a merge consumed vanish: tombstone loaded starts not live anymore.
            let binary_key = &key_slices[0];
            let vanished: Vec<i64> = store
                .seeded_starts(binary_key)
                .iter()
                .copied()
                .filter(|start| !map.contains_key(start))
                .collect();
            if !vanished.is_empty() {
                let delete_keys = vec![*binary_key; vanished.len()];
                store.stage_deletes(&delete_keys, &vanished)?;
            }
        }
        let store = self.backend.as_mut().expect("session-agg paimon backend");
        let manifest = store.checkpoint()?;
        let delta = store.footprint_delta();
        self.memory.record(delta);
        self.memory.account()?;
        Ok(manifest)
    }

    /// Bounds this aggregator's state by the operator's task off-heap budget (negative =
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
        #[cfg(feature = "paimon-state")]
        self.seed_batch_keys(batch)?;
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
    pub(crate) fn flush(&mut self, watermark: i64) -> Result<RecordBatch, DataFusionError> {
        // Persistent state: sessions committed at earlier barriers whose keys were untouched
        // this interval still close now — hydrate them into the decoded map first.
        #[cfg(feature = "paimon-state")]
        if self.backend.is_some() {
            let batches = self
                .backend
                .as_mut()
                .expect("session-agg paimon backend")
                .fire_scan(watermark)?;
            self.absorb_committed(batches)?;
        }
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
        let mut emptied: Vec<OwnedRow> = Vec::new();
        self.sessions.retain(|key, map| {
            if map.is_empty() {
                if track {
                    freed += owned_row_bytes(key);
                }
                emptied.push(key.clone());
                return false;
            }
            true
        });
        if track {
            self.memory.forget(freed);
            self.memory.account_shrink();
        }
        rows.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        // Persistent state: every fired (key, start) leaves the store, and a key whose map
        // emptied tombstones every committed start its seed loaded — a merge may have consumed a
        // committed start whose session then fired under a different start, and once the key
        // drops from the map the barrier diff can no longer see it.
        #[cfg(feature = "paimon-state")]
        if self.backend.is_some() {
            let precisions = self.key_precisions(self.key_types.len());
            if !rows.is_empty() {
                let keys: Vec<OwnedRow> = rows.iter().map(|(key, ..)| key.clone()).collect();
                let key_columns =
                    decode_keys(self.key_converter.as_ref(), &keys, &self.key_types);
                let binary_keys = crate::window_agg::binary_row_keys(
                    &key_columns,
                    &self.key_types,
                    &precisions,
                    keys.len(),
                )?;
                let key_slices: Vec<&[u8]> = binary_keys.iter().map(|k| k.as_slice()).collect();
                let starts: Vec<i64> = rows.iter().map(|(_, start, ..)| *start).collect();
                let store = self.backend.as_mut().expect("session-agg paimon backend");
                store.stage_deletes(&key_slices, &starts)?;
            }
            if !emptied.is_empty() {
                let key_columns =
                    decode_keys(self.key_converter.as_ref(), &emptied, &self.key_types);
                let binary_keys = crate::window_agg::binary_row_keys(
                    &key_columns,
                    &self.key_types,
                    &precisions,
                    emptied.len(),
                )?;
                for binary_key in &binary_keys {
                    let loaded = self
                        .backend
                        .as_ref()
                        .expect("session-agg paimon backend")
                        .seeded_starts(binary_key)
                        .to_vec();
                    if !loaded.is_empty() {
                        let delete_keys = vec![binary_key.as_slice(); loaded.len()];
                        let store = self.backend.as_mut().expect("session-agg paimon backend");
                        store.stage_deletes(&delete_keys, &loaded)?;
                    }
                }
            }
            let store = self.backend.as_mut().expect("session-agg paimon backend");
            let delta = store.footprint_delta();
            self.memory.record(delta);
            self.memory.account()?;
        }

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
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build result batch"))
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

state_bytes_getter!(Java_tech_streamfusion_Native_sessionAggregatorStateBytes, SessionAggregator);

/// Creates a stateful session-window aggregator and returns an opaque handle. As with the tumbling
/// handle, the JVM owns the native state across calls and must release it with the matching close.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    gap_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_types = read_int_array(&env, &value_types);
        let aggregator = SessionAggregator::new(gap_millis, value_types, kinds)
            .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, aggregator)
    })
}

/// Folds a batch from the JVM into the session aggregator, merging sessions as elements bridge them.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_updateSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            aggregator.update(&batch)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Emits the sessions the given watermark has closed as a batch and drops them from state.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        // Fallible in persistent-state mode (the firing reads the committed table).
        match aggregator.flush(watermark_millis) {
            Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Releases the session aggregator and its native state.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<SessionAggregator>(handle));
        }
    })
}

/// Serializes the aggregator's open sessions so the JVM can store them in a checkpoint.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    crate::bridge::jni_guard(env, move |env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        env.byte_array_from_slice(&aggregator.snapshot())
            .expect("failed to allocate snapshot array")
            .into_raw()
    })
}

/// Rebuilds a session aggregator from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    gap_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_types = read_int_array(&env, &value_types);
        let bytes = env.convert_byte_array(&snapshot).expect("failed to read snapshot");
        let aggregator = SessionAggregator::restore(gap_millis, value_types, kinds, &bytes)
            .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, aggregator)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotSessionAggregatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        let precisions = read_i32_array(&env, &timestamp_precisions);
        keyed_state_partition_array(
            &mut env,
            aggregator.snapshot_partitions(max_parallelism as usize, &precisions),
            "session-window",
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreSessionAggregatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    gap_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
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
    })
}
