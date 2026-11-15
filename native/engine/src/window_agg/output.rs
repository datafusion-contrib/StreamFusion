use super::*;

pub(super) const OUTPUT_ROWS: usize = 1024;
pub(super) const OUTPUT_BYTES: usize = 4 * 1024 * 1024;

pub(super) struct WindowOutput {
    keys: Vec<OwnedRow>,
    starts: Vec<i64>,
    ends: Vec<i64>,
    results: Vec<WindowResultColumn>,
    bytes: usize,
    work: Option<MemoryReservation>,
    max_rows: usize,
    max_bytes: usize,
}

impl WindowOutput {
    pub(super) fn new(operator: &TumblingAggregator) -> Self {
        Self::bounded(operator, OUTPUT_ROWS, OUTPUT_BYTES)
    }

    pub(super) fn bounded(
        operator: &TumblingAggregator,
        max_rows: usize,
        max_bytes: usize,
    ) -> Self {
        assert!(max_rows > 0 && max_bytes > 0);
        Self {
            keys: Vec::new(),
            starts: Vec::new(),
            ends: Vec::new(),
            results: operator
                .aggregates
                .iter()
                .map(WindowResultColumn::new)
                .collect(),
            bytes: 0,
            work: operator.memory.temporary_reservation(),
            max_rows,
            max_bytes,
        }
    }

    pub(super) fn push(
        &mut self,
        operator: &TumblingAggregator,
        key: OwnedRow,
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
                let result = aggregate.result_type();
                result
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
        if !self.keys.is_empty()
            && (self.keys.len() == self.max_rows
                || self.bytes.saturating_add(bytes) > self.max_bytes)
        {
            self.emit(operator, consume)?;
        }
        let next_bytes = self.bytes.saturating_add(bytes);
        if let Some(work) = &self.work {
            // Scalar results and their final Arrow arrays overlap during assembly.
            if work.try_resize(next_bytes.saturating_mul(2)).is_err() && !self.keys.is_empty() {
                self.emit(operator, consume)?;
            }
            self.work
                .as_ref()
                .unwrap()
                .try_resize(self.bytes.saturating_add(bytes).saturating_mul(2))?;
        }
        self.keys.push(key);
        self.starts.push(start);
        self.ends.push(end);
        for (result, accumulator) in self.results.iter_mut().zip(accumulators) {
            result.push(accumulator.as_mut())?;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        if self.keys.len() == self.max_rows || self.bytes >= self.max_bytes {
            self.emit(operator, consume)?;
        }
        Ok(())
    }

    pub(super) fn emit(
        &mut self,
        operator: &TumblingAggregator,
        consume: &mut dyn FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if self.keys.is_empty() {
            return Ok(());
        }
        let results = std::mem::replace(
            &mut self.results,
            operator
                .aggregates
                .iter()
                .map(WindowResultColumn::new)
                .collect(),
        );
        let batch = operator.final_batch(
            std::mem::take(&mut self.keys),
            std::mem::take(&mut self.starts),
            std::mem::take(&mut self.ends),
            results,
        );
        consume(batch)?;
        self.bytes = 0;
        if let Some(work) = &self.work {
            work.free();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(rows: usize, width: usize) -> RecordBatch {
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
                    (0..rows).map(|i| (i % 3) as i64 * 1000 + 1),
                )) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn operator() -> TumblingAggregator {
        TumblingAggregator::new(1000, 1000, false, vec![0], vec![3])
            .with_key_timestamp_precisions(vec![-1])
    }

    fn append_rows(batch: RecordBatch, actual: &mut Vec<(i64, String, i64)>) {
        assert!(batch.num_rows() <= OUTPUT_ROWS);
        assert!(batch.get_array_memory_size() <= OUTPUT_BYTES || batch.num_rows() == 1);
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ends = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        actual.extend(
            (0..batch.num_rows())
                .map(|i| (ends.value(i), keys.value(i).to_owned(), counts.value(i))),
        );
    }

    fn expected(batch: &RecordBatch) -> Vec<(i64, String, i64)> {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut rows = (0..batch.num_rows())
            .map(|i| (((i % 3) as i64 + 1) * 1000, keys.value(i).to_owned(), 1))
            .collect::<Vec<_>>();
        rows.sort();
        rows
    }

    #[test]
    fn memory_firing_bounds_rows_and_bytes_without_changing_order_or_lateness() {
        for (rows, width) in [(4101, 0), (301, 17_000)] {
            let data = input(rows, width);
            let mut operator = operator();
            operator.update(&data).unwrap();
            let mut actual = Vec::new();
            let mut batches = 0;
            operator
                .flush_to(i64::MAX, &mut |batch| {
                    batches += 1;
                    append_rows(batch, &mut actual);
                    Ok(())
                })
                .unwrap();
            assert!(batches > 1);
            assert_eq!(actual, expected(&data));
            operator.update(&data).unwrap();
            assert_eq!(operator.late_drops, rows as u64);
            operator
                .flush_to(i64::MAX, &mut |_| panic!("closed windows re-emitted"))
                .unwrap();
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
    fn persistent_firing_streams_a_backlog_larger_than_its_workspace_budget() {
        for (rows, width, budget) in [(4101, 0, 32 << 10), (301, 17_000, 1 << 20)] {
            let directory = tempfile::tempdir().unwrap();
            let store =
                RocksWindowAggStore::create(config(directory.path()), &[DataType::Int64], 0..=127)
                    .unwrap();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(budget));
            let mut operator = operator()
                .with_store(store, vec![DataType::Utf8])
                .with_memory_pool(&pool)
                .unwrap();
            let data = input(rows, width);
            operator.update(&data).unwrap();
            let mut actual = Vec::new();
            operator
                .flush_to(i64::MAX, &mut |batch| {
                    append_rows(batch, &mut actual);
                    Ok(())
                })
                .unwrap();
            assert_eq!(actual, expected(&data));
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
    fn failed_receiver_releases_workspace_and_recovers_from_the_previous_checkpoint() {
        let directory = tempfile::tempdir().unwrap();
        let store = RocksWindowAggStore::create(
            config(&directory.path().join("live")),
            &[DataType::Int64],
            0..=127,
        )
        .unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let mut operator = operator()
            .with_store(store, vec![DataType::Utf8])
            .with_memory_pool(&pool)
            .unwrap();
        let data = input(4101, 0);
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
        let store = RocksWindowAggStore::open_merged(
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
                append_rows(batch, &mut actual);
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected(&data));
    }
}
