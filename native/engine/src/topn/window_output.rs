use super::*;

pub(super) const OUTPUT_ROWS: usize = 1024;
pub(super) const OUTPUT_BYTES: usize = 4 * 1024 * 1024;

/// A scalar buffer and its Arrow conversion overlap only for one bounded output chunk.
pub(super) struct RankOutput {
    rows: Vec<JoinRow>,
    ranks: Vec<i64>,
    bytes: usize,
    work: Option<MemoryReservation>,
}

impl RankOutput {
    pub(super) fn new(operator: &WindowRanker) -> Self {
        Self {
            rows: Vec::new(),
            ranks: Vec::new(),
            bytes: 0,
            work: operator.memory.temporary_reservation(),
        }
    }

    pub(super) fn push(
        &mut self,
        operator: &WindowRanker,
        row: JoinRow,
        rank: i64,
        consume: &mut dyn FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let bytes = scalar_row_bytes(&row).saturating_mul(2).saturating_add(128);
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
        self.bytes = self.bytes.saturating_add(bytes);
        self.rows.push(row);
        self.ranks.push(rank);
        if self.rows.len() == OUTPUT_ROWS || self.bytes >= OUTPUT_BYTES {
            self.emit(operator, consume)?;
        }
        Ok(())
    }

    pub(super) fn emit(
        &mut self,
        operator: &WindowRanker,
        consume: &mut dyn FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let batch = operator.emit(
            std::mem::take(&mut self.rows),
            std::mem::take(&mut self.ranks),
        );
        consume(batch)?;
        self.bytes = 0;
        if let Some(work) = &self.work {
            work.free();
        }
        Ok(())
    }
}

#[cfg(feature = "rocksdb-state")]
pub(super) fn ranked_batch(
    schema: &SchemaRef,
    mut columns: Vec<ArrayRef>,
    ranks: Vec<i64>,
    output_rank_number: bool,
) -> Result<RecordBatch, DataFusionError> {
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    if output_rank_number {
        fields.push(Field::new("w0$o0", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(ranks)));
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(rows: usize, width: usize, one_group: bool) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "key",
                Arc::new(Int64Array::from_iter_values((0..rows).map(|i| {
                    if one_group {
                        0
                    } else {
                        i as i64
                    }
                }))) as ArrayRef,
            ),
            (
                "value",
                Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
            ),
            (
                "payload",
                Arc::new(StringArray::from(vec!["x".repeat(width); rows])) as ArrayRef,
            ),
            (
                "window_start",
                Arc::new(Int64Array::from(vec![-1000; rows])) as ArrayRef,
            ),
            (
                "window_end",
                Arc::new(Int64Array::from(vec![1000; rows])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn operator(limit: i64) -> WindowRanker {
        WindowRanker::new(
            3,
            4,
            vec![0],
            vec![SortColumn {
                index: 1,
                ascending: true,
                nulls_first: false,
            }],
            limit,
            true,
        )
    }

    fn observe(batch: RecordBatch, actual: &mut Vec<(i64, i64, i64)>) {
        assert!(batch.num_rows() <= OUTPUT_ROWS);
        assert!(
            batch.get_array_memory_size() <= OUTPUT_BYTES || batch.num_rows() == 1,
            "{} rows own {} bytes",
            batch.num_rows(),
            batch.get_array_memory_size()
        );
        let ints = |column| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
        };
        actual.extend(
            (0..batch.num_rows()).map(|i| (ints(0).value(i), ints(1).value(i), ints(5).value(i))),
        );
    }

    #[test]
    fn memory_firing_splits_huge_groups_and_wide_payloads_without_restarting_ranks() {
        for (rows, width, one_group) in [
            (4101, 0, true),
            (301, 17000, true),
            (4101, 0, false),
            (3, OUTPUT_BYTES + 1, true),
        ] {
            let batch = input(rows, width, one_group);
            let mut ranker = operator(rows as i64);
            ranker.push(&batch).unwrap();
            // Attach after ingestion so this test observes firing, not admission of input state.
            let state = ranker
                .groups
                .iter()
                .map(|((_, _, key), rows)| {
                    group_key_bytes(key)
                        + rows
                            .iter()
                            .map(|row| scalar_row_bytes(row) + GROUP_ENTRY_OVERHEAD)
                            .sum::<usize>()
                })
                .sum();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 << 20));
            ranker
                .memory
                .attach_pool("rank-output", &pool, state)
                .unwrap();
            let mut actual = Vec::new();
            let mut batches = 0;
            ranker
                .flush_to(1000, &mut |batch| {
                    batches += 1;
                    observe(batch, &mut actual);
                    Ok(())
                })
                .unwrap();
            assert!(batches > 1);
            actual.sort_unstable();
            let expected = (0..rows as i64)
                .map(|i| {
                    (
                        if one_group { 0 } else { i },
                        i,
                        if one_group { i + 1 } else { 1 },
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            assert_eq!(
                pool.reserved(),
                0,
                "key and row state must be released exactly"
            );
            ranker
                .flush_to(1000, &mut |_| panic!("closed group re-emitted"))
                .unwrap();
            ranker.push(&batch).unwrap();
            assert_eq!(ranker.late_drops, rows as u64);
        }
    }

    #[cfg(feature = "rocksdb-state")]
    fn config(path: &std::path::Path) -> crate::state::RocksStoreConfig {
        crate::state::RocksStoreConfig {
            table_dir: path.to_string_lossy().into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        }
    }

    #[cfg(feature = "rocksdb-state")]
    fn persistent(path: &std::path::Path, batch: &RecordBatch) -> WindowRanker {
        let types = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let store =
            crate::state::RocksWindowRankStore::create(config(path), &types, 0..=127).unwrap();
        operator(batch.num_rows() as i64)
            .with_store(store, batch.schema())
            .with_key_timestamp_precisions(vec![-1])
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn persistent_firing_bounds_workspace_for_many_groups_and_one_large_group() {
        for (rows, width, one_group) in [(4101, 0, true), (301, 17000, true), (4101, 0, false)] {
            let directory = tempfile::tempdir().unwrap();
            let batch = input(rows, width, one_group);
            let mut ranker = persistent(directory.path(), &batch);
            ranker.push(&batch).unwrap();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256 << 10));
            ranker.memory.attach_pool("rank-output", &pool, 0).unwrap();
            let mut actual = Vec::new();
            let mut batches = 0;
            ranker
                .flush_to(1000, &mut |batch| {
                    batches += 1;
                    observe(batch, &mut actual);
                    Ok(())
                })
                .unwrap();
            assert!(batches > 1);
            if !one_group {
                assert!(batches < rows, "small groups should share output batches");
            }
            actual.sort_unstable();
            let expected = (0..rows as i64)
                .map(|i| {
                    (
                        if one_group { 0 } else { i },
                        i,
                        if one_group { i + 1 } else { 1 },
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            assert_eq!(pool.reserved(), 0);
            assert!(ranker
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
    fn receiver_failure_releases_workspace_and_checkpoint_restores_all_ranks() {
        let directory = tempfile::tempdir().unwrap();
        let batch = input(4101, 0, true);
        let mut ranker = persistent(&directory.path().join("live"), &batch);
        ranker.push(&batch).unwrap();
        let checkpoint = directory.path().join("checkpoint");
        let manifest = ranker
            .checkpoint_store(i64::MIN, checkpoint.to_str().unwrap())
            .unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256 << 10));
        ranker.memory.attach_pool("rank-output", &pool, 0).unwrap();
        let mut callbacks = 0;
        let failure = ranker
            .flush_to(1000, &mut |_| {
                callbacks += 1;
                if callbacks == 2 {
                    Err(DataFusionError::Execution("receiver stopped".into()))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        assert!(failure.to_string().contains("receiver stopped"));
        assert_eq!(pool.reserved(), 0);
        drop(ranker);
        let types = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let store = crate::state::RocksWindowRankStore::open_merged(
            config(&directory.path().join("restored")),
            &types,
            0..=127,
            &[(
                checkpoint.to_string_lossy().into_owned(),
                manifest.snapshot_id,
            )],
            true,
        )
        .unwrap();
        let mut restored = operator(4101).with_store(store, batch.schema());
        let mut actual = Vec::new();
        restored
            .flush_to(1000, &mut |batch| {
                observe(batch, &mut actual);
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, (0..4101).map(|i| (0, i, i + 1)).collect::<Vec<_>>());
    }
}
