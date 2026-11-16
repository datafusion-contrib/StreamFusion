use super::*;

const OUTPUT_ROWS: usize = 1024;
const OUTPUT_BYTES: usize = 4 * 1024 * 1024;

type SessionResult = (OwnedRow, i64, i64, Vec<ScalarValue>);

struct SessionOutput {
    rows: Vec<SessionResult>,
    bytes: usize,
    work: Option<MemoryReservation>,
}

impl SessionOutput {
    fn new(operator: &SessionAggregator) -> Self {
        Self {
            rows: Vec::new(),
            bytes: 0,
            work: operator.memory.temporary_reservation(),
        }
    }

    fn push(
        &mut self,
        operator: &SessionAggregator,
        key: &OwnedRow,
        start: i64,
        end: i64,
        accumulators: &mut [Box<dyn Accumulator>],
        consume: &mut dyn FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let values = operator
            .aggregates
            .iter()
            .zip(accumulators.iter())
            .map(|(aggregate, accumulator)| {
                aggregate
                    .result_type()
                    .primitive_width()
                    .unwrap_or_else(|| accumulator.size())
                    + std::mem::size_of::<ScalarValue>()
            })
            .sum::<usize>();
        let bytes = key
            .row()
            .data()
            .len()
            .saturating_add(values)
            .saturating_add(64);
        if !self.rows.is_empty()
            && (self.rows.len() == OUTPUT_ROWS || self.bytes.saturating_add(bytes) > OUTPUT_BYTES)
        {
            self.emit(operator, consume)?;
        }
        if let Some(work) = &self.work {
            if work
                .try_resize(self.bytes.saturating_add(bytes).saturating_mul(3))
                .is_err()
                && !self.rows.is_empty()
            {
                self.emit(operator, consume)?;
            }
            self.work
                .as_ref()
                .unwrap()
                .try_resize(self.bytes.saturating_add(bytes).saturating_mul(3))?;
        }
        let results = accumulators
            .iter_mut()
            .map(|accumulator| accumulator.evaluate())
            .collect::<Result<Vec<_>, _>>()?;
        self.rows.push((key.clone(), start, end, results));
        self.bytes = self.bytes.saturating_add(bytes);
        if self.rows.len() == OUTPUT_ROWS || self.bytes >= OUTPUT_BYTES {
            self.emit(operator, consume)?;
        }
        Ok(())
    }

    fn emit(
        &mut self,
        operator: &SessionAggregator,
        consume: &mut dyn FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        consume(operator.emitted_batch(std::mem::take(&mut self.rows)))?;
        self.bytes = 0;
        if let Some(work) = &self.work {
            work.free();
        }
        Ok(())
    }
}

impl SessionAggregator {
    pub(crate) fn flush_to(
        &mut self,
        watermark: i64,
        consume: &mut dyn FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.require_complete_canonical_import()?;
        self.current_watermark = self.current_watermark.max(watermark);
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            return self.flush_store_to(watermark, consume);
        }
        let mut keys = std::mem::take(&mut self.sessions)
            .into_iter()
            .collect::<Vec<_>>();
        keys.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let mut output = SessionOutput::new(self);
        let mut next_end: Option<i64> = None;
        for (key, sessions) in keys {
            let mut sessions = sessions.into_iter();
            let mut pending = BTreeMap::new();
            loop {
                let rows = sessions.by_ref().take(OUTPUT_ROWS).collect::<Vec<_>>();
                if rows.is_empty() {
                    break;
                }
                let ends = rows
                    .iter()
                    .map(|(_, session)| session.end)
                    .collect::<Vec<_>>();
                let deadlines = self
                    .calendar
                    .as_ref()
                    .map_or_else(|| ends.clone(), |calendar| calendar.exclusive_ends(&ends));
                for ((start, mut session), deadline) in rows.into_iter().zip(deadlines) {
                    if deadline > watermark {
                        next_end = Some(next_end.map_or(deadline, |next| next.min(deadline)));
                        pending.insert(start, session);
                        continue;
                    }
                    if self.memory.tracking() {
                        self.memory.forget(session_bytes(&session));
                    }
                    self.memory.account_shrink();
                    output.push(
                        self,
                        &key,
                        start,
                        session.end,
                        &mut session.accumulators,
                        consume,
                    )?;
                }
            }
            if pending.is_empty() {
                if self.memory.tracking() {
                    self.memory.forget(owned_row_bytes(&key));
                }
            } else {
                self.sessions.insert(key, pending);
            }
        }
        self.next_timer_deadline = next_end.map_or(i64::MIN, |end| end.saturating_sub(1));
        self.memory.account_shrink();
        output.emit(self, consume)
    }

    #[cfg(feature = "rocksdb-state")]
    fn flush_store_to(
        &mut self,
        watermark: i64,
        consume: &mut dyn FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let mut store = self.store.take().expect("session-aggregate rocksdb store");
        let mut output = SessionOutput::new(self);
        let result = store.visit_closed_batches(
            watermark,
            self.calendar.as_ref(),
            self.memory.temporary_reservation(),
            OUTPUT_ROWS,
            OUTPUT_BYTES,
            &mut |sessions| {
                if sessions.is_empty() {
                    return output.emit(self, consume);
                }
                for session in sessions {
                    let key = self
                        .key_converter
                        .as_ref()
                        .expect("session store key converter")
                        .parser()
                        .parse(&session.key)
                        .owned();
                    let mut accumulators = self.accumulators_from_scalars(&session.state);
                    output.push(
                        self,
                        &key,
                        session.start,
                        session.end,
                        &mut accumulators,
                        consume,
                    )?;
                }
                Ok(())
            },
        );
        self.next_timer_deadline = store
            .next_exclusive_end()
            .map_or(i64::MIN, |end| end.saturating_sub(1));
        self.store = Some(store);
        result?;
        output.emit(self, consume)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operator() -> SessionAggregator {
        SessionAggregator::new(500, vec![0], vec![3]).with_key_timestamp_precisions(vec![-1])
    }

    fn input(rows: usize, width: usize, offset: i64) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "key0",
                Arc::new(StringArray::from_iter_values(
                    (0..rows).map(|i| format!("{i:06}{}", "x".repeat(width))),
                )) as ArrayRef,
            ),
            (
                "value0",
                Arc::new(Int64Array::from(vec![1; rows])) as ArrayRef,
            ),
            (
                "ts",
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|i| offset + (i % 3) as i64 * 1000 + 1),
                )) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn append(batch: RecordBatch, rows: &mut Vec<(String, i64, i64)>) {
        assert!(batch.num_rows() <= OUTPUT_ROWS);
        assert!(batch.get_array_memory_size() <= OUTPUT_BYTES || batch.num_rows() == 1);
        let key = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let start = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let count = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        rows.extend(
            (0..batch.num_rows())
                .map(|i| (key.value(i).to_owned(), start.value(i), count.value(i))),
        );
    }

    fn expected(batch: &RecordBatch) -> Vec<(String, i64, i64)> {
        let key = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let time = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut rows = (0..batch.num_rows())
            .map(|i| (key.value(i).to_owned(), time.value(i), 1))
            .collect::<Vec<_>>();
        rows.sort();
        rows
    }

    fn verify(operator: &mut SessionAggregator, rows: usize, width: usize) {
        let initial = input(rows, width, 0);
        let future = input(rows, width, 10_000);
        for data in [&initial, &future] {
            for offset in (0..rows).step_by(8) {
                operator
                    .update(&data.slice(offset, (rows - offset).min(8)))
                    .unwrap();
            }
        }
        let mut actual = Vec::new();
        let mut batches = 0;
        operator
            .flush_to(3000, &mut |batch| {
                batches += 1;
                append(batch, &mut actual);
                Ok(())
            })
            .unwrap();
        assert!(batches > 1);
        assert_eq!(actual, expected(&initial));
        assert_eq!(operator.next_timer_deadline, 10_500);
        operator.update(&initial.slice(0, 1)).unwrap();
        assert_eq!(operator.late_drops, 1);
        actual.clear();
        operator
            .flush_to(i64::MAX, &mut |batch| {
                append(batch, &mut actual);
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected(&future));
        assert_eq!(operator.next_timer_deadline, i64::MIN);
        operator
            .flush_to(i64::MAX, &mut |_| panic!("session emitted twice"))
            .unwrap();
    }

    #[test]
    fn memory_session_firing_bounds_output_and_preserves_pending_sessions_and_key_order() {
        for (rows, width) in [(4101, 0), (301, 17_000)] {
            verify(&mut operator(), rows, width);
        }
    }

    #[cfg(feature = "rocksdb-state")]
    fn config(path: &std::path::Path) -> RocksStoreConfig {
        RocksStoreConfig {
            table_dir: path.to_string_lossy().into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        }
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn persistent_sessions_stream_backlogs_larger_than_their_decode_and_output_budget() {
        for (rows, width, budget) in [(4101, 0, 32 << 10), (301, 17_000, 1 << 20)] {
            let directory = tempfile::tempdir().unwrap();
            let store =
                RocksSessionAggStore::create(config(directory.path()), &[DataType::Int64], 0..=127)
                    .unwrap();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(budget));
            let mut operator = operator().with_store(store, vec![DataType::Utf8]);
            operator.memory.attach_pool("session", &pool, 0).unwrap();
            verify(&mut operator, rows, width);
            assert_eq!(pool.reserved(), 0);
            assert!(operator
                .store
                .as_ref()
                .unwrap()
                .scan_all()
                .unwrap()
                .is_empty());
        }
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn session_receiver_failure_releases_workspace_and_restores_the_preceding_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let store = RocksSessionAggStore::create(
            config(&directory.path().join("live")),
            &[DataType::Int64],
            0..=127,
        )
        .unwrap();
        let mut operator = operator().with_store(store, vec![DataType::Utf8]);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        operator.memory.attach_pool("session", &pool, 0).unwrap();
        let data = input(2051, 0, 0);
        operator.update(&data).unwrap();
        let checkpoint = directory.path().join("checkpoint");
        let manifest = operator
            .checkpoint_store(i64::MIN, checkpoint.to_str().unwrap())
            .unwrap();
        assert!(operator
            .flush_to(i64::MAX, &mut |_| Err(DataFusionError::Execution(
                "receiver stopped".into()
            )))
            .is_err());
        drop(operator);
        assert_eq!(pool.reserved(), 0);
        let store = RocksSessionAggStore::open_merged(
            config(&directory.path().join("restored")),
            &[DataType::Int64],
            0..=127,
            &[(
                checkpoint.to_string_lossy().into_owned(),
                manifest.snapshot_id,
            )],
            true,
        )
        .unwrap();
        let mut restored = self::operator().with_store(store, vec![DataType::Utf8]);
        let mut actual = Vec::new();
        restored
            .flush_to(i64::MAX, &mut |batch| {
                append(batch, &mut actual);
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected(&data));
    }
}
