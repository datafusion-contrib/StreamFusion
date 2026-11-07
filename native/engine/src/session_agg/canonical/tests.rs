use super::*;

fn create(path: Option<&std::path::Path>, keyless: bool) -> SessionAggregator {
    let mut value = SessionAggregator::new(1000, vec![0, 0, 0, 0, 0], vec![0, 3, 4, 1, 2])
        .with_key_timestamp_precisions(if keyless { vec![] } else { vec![-1] });
    #[cfg(feature = "rocksdb-state")]
    if let Some(path) = path {
        let types = value
            .canonical_state_fields()
            .unwrap()
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let store = crate::state::RocksSessionAggStore::create(
            crate::state::RocksStoreConfig {
                table_dir: path.to_str().unwrap().into(),
                max_parallelism: 128,
                options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 0,
                shared_resources: 0,
            },
            &types,
            0..=127,
        )
        .unwrap();
        value = value.with_store(
            store,
            if keyless {
                vec![]
            } else {
                vec![DataType::Int64]
            },
        );
    }
    value
}

fn input(times: &[i64], values: &[Option<i64>], keyless: bool) -> RecordBatch {
    let mut columns: Vec<(String, ArrayRef)> =
        vec![("ts".into(), Arc::new(Int64Array::from(times.to_vec())))];
    for index in 0..5 {
        columns.push((
            format!("value{index}"),
            Arc::new(Int64Array::from(values.to_vec())),
        ));
    }
    if !keyless {
        columns.push((
            "key0".into(),
            Arc::new(Int64Array::from(vec![Some(7); times.len()])),
        ));
    }
    let fields = columns
        .iter()
        .map(|(name, array)| Field::new(name, array.data_type().clone(), true))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns.into_iter().map(|(_, array)| array).collect(),
    )
    .unwrap()
}

fn rows(batch: &RecordBatch) -> Vec<String> {
    let mut rows = (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|column| format!("{:?}", ScalarValue::try_from_array(column, row).unwrap()))
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn bridged_sessions_continue_after_direct_handoffs_between_both_stores() {
    for source_rocks in [false, true] {
        for target_rocks in [false, true] {
            for keyless in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let mut source = create(
                    source_rocks.then_some(directory.path().join("source").as_path()),
                    keyless,
                );
                let mut target = create(
                    target_rocks.then_some(directory.path().join("target").as_path()),
                    keyless,
                );
                source
                    .update(&input(
                        &[-4000, 0, 2000],
                        &[None, Some(10), Some(30)],
                        keyless,
                    ))
                    .unwrap();
                source.flush(0).unwrap();
                source
                    .visit_canonical_state(|batch| {
                        target.import_canonical_state(&batch, usize::from(!keyless))
                    })
                    .unwrap();
                target.current_watermark = source.current_watermark;
                let mut reexported = create(None, keyless);
                target
                    .visit_canonical_state(|batch| {
                        reexported.import_canonical_state(&batch, usize::from(!keyless))
                    })
                    .unwrap();
                reexported.current_watermark = target.current_watermark;
                // This row touches both sessions at their boundaries and must produce one merged result.
                let suffix = input(&[1000, 100, 6000], &[Some(20), None, None], keyless);
                for value in [&mut source, &mut target, &mut reexported] {
                    value.update(&suffix).unwrap();
                }
                for watermark in [2999, 3000, 7000, i64::MAX] {
                    let expected = rows(&source.flush(watermark).unwrap());
                    assert_eq!(expected, rows(&target.flush(watermark).unwrap()));
                    assert_eq!(expected, rows(&reexported.flush(watermark).unwrap()));
                }
            }
        }
    }
}

#[test]
fn malformed_duplicate_and_overlapping_namespaces_do_not_mutate_sessions() {
    for rocks in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut source = create(None, false);
        source
            .update(&input(&[0, 2000], &[Some(10), Some(30)], false))
            .unwrap();
        let mut exported = Vec::new();
        source
            .visit_canonical_state(|batch| {
                exported.push(batch);
                Ok(())
            })
            .unwrap();
        let valid = &exported[0];
        for starts in [vec![0, 0], vec![0, 1000], vec![1000, 2000]] {
            let mut target = create(
                rocks.then_some(directory.path().join(format!("target{starts:?}")).as_path()),
                false,
            );
            let mut columns = valid.columns().to_vec();
            columns[0] = Arc::new(Int64Array::from(starts));
            let invalid = RecordBatch::try_new(valid.schema(), columns).unwrap();
            assert!(target.import_canonical_state(&invalid, 1).is_err());
            assert_eq!(target.flush(i64::MAX).unwrap().num_rows(), 0);
        }
        let mut target = create(
            rocks.then_some(directory.path().join("duplicate").as_path()),
            false,
        );
        target.import_canonical_state(valid, 1).unwrap();
        assert!(target.import_canonical_state(valid, 1).is_err());
        assert_eq!(
            rows(&source.flush(i64::MAX).unwrap()),
            rows(&target.flush(i64::MAX).unwrap())
        );
    }
}

#[test]
fn direct_session_export_is_bounded_and_receiver_failures_leave_state_intact() {
    for rocks in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut source = create(
            rocks.then_some(directory.path().join("source").as_path()),
            false,
        );
        let times = (0..2500).map(|row| row * 2000).collect::<Vec<_>>();
        source
            .update(&input(&times, &vec![Some(1); times.len()], false))
            .unwrap();
        assert!(source
            .visit_canonical_state(|_| Err(invalid("receiver stopped")))
            .is_err());
        let mut target = create(None, false);
        let mut sizes = Vec::new();
        source
            .visit_canonical_state(|batch| {
                sizes.push(batch.num_rows());
                target.import_canonical_state(&batch, 1)
            })
            .unwrap();
        assert_eq!(sizes, vec![1024, 1024, 452]);
        assert_eq!(
            rows(&source.flush(i64::MAX).unwrap()),
            rows(&target.flush(i64::MAX).unwrap())
        );
    }
}

#[test]
fn nullable_keys_remain_separate_and_cross_batch_overlaps_are_rejected() {
    for rocks in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut source = create(None, false);
        let batch = input(
            &[0, 0, 0, 3000],
            &[Some(10), Some(20), None, Some(40)],
            false,
        );
        let mut columns = batch.columns().to_vec();
        *columns.last_mut().unwrap() =
            Arc::new(Int64Array::from(vec![None, Some(1), Some(2), None]));
        source
            .update(&RecordBatch::try_new(batch.schema(), columns).unwrap())
            .unwrap();
        let mut target = create(
            rocks.then_some(directory.path().join("target").as_path()),
            false,
        );
        source
            .visit_canonical_state(|batch| target.import_canonical_state(&batch, 1))
            .unwrap();
        let mut overlap = create(None, false);
        let batch = input(&[1000], &[Some(30)], false);
        let mut columns = batch.columns().to_vec();
        *columns.last_mut().unwrap() = Arc::new(Int64Array::from(vec![None]));
        overlap
            .update(&RecordBatch::try_new(batch.schema(), columns).unwrap())
            .unwrap();
        overlap
            .visit_canonical_state(|batch| {
                assert!(target.import_canonical_state(&batch, 1).is_err());
                Ok(())
            })
            .unwrap();
        assert_eq!(
            rows(&source.flush(i64::MAX).unwrap()),
            rows(&target.flush(i64::MAX).unwrap())
        );
    }
}

#[test]
fn imported_session_memory_is_reserved_and_released_even_on_budget_failure() {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    let mut source = create(None, false);
    source
        .update(&input(&[0, 2000], &[Some(10), Some(30)], false))
        .unwrap();
    for budget in [1, 1 << 20] {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(budget));
        let mut target = create(None, false);
        target
            .memory
            .attach_pool("session-import", &pool, 0)
            .unwrap();
        let result = source.visit_canonical_state(|batch| target.import_canonical_state(&batch, 1));
        if budget == 1 {
            assert!(matches!(
                result,
                Err(DataFusionError::ResourcesExhausted(_))
            ));
        } else {
            result.unwrap();
            assert!(pool.reserved() > 0);
        }
        drop(target);
        assert_eq!(pool.reserved(), 0);
    }
}
