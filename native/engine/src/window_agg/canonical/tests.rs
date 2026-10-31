use super::*;

fn create(path: Option<&std::path::Path>, keyless: bool, count_only: bool) -> TumblingAggregator {
    let (types, kinds) = if count_only {
        (vec![0], vec![3])
    } else {
        (
            vec![0, 0, 0, 203802, 203802, 0, 0],
            vec![0, 3, 4, 0, 4, 1, 2],
        )
    };
    let mut value = TumblingAggregator::new(1000, 1000, false, types, kinds)
        .with_key_timestamp_precisions(if keyless { vec![] } else { vec![-1] });
    #[cfg(feature = "rocksdb-state")]
    if let Some(path) = path {
        let types = value
            .canonical_state_fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let store = crate::state::RocksWindowAggStore::create(
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

fn input(keyless: bool, count_only: bool, offset: usize, count: usize) -> RecordBatch {
    let mut columns: Vec<(String, ArrayRef)> = Vec::new();
    if !keyless {
        columns.push((
            "key0".into(),
            Arc::new(Int64Array::from_iter(
                (offset..offset + count).map(|row| (row % 11 != 0).then_some((row % 17) as i64)),
            )),
        ));
    }
    let fields = if count_only { 1 } else { 7 };
    for index in 0..fields {
        let column: ArrayRef = if index == 3 || index == 4 {
            Arc::new(
                Decimal128Array::from_iter(
                    (offset..offset + count).map(|row| (row % 5 != 0).then_some(row as i128 * 101)),
                )
                .with_precision_and_scale(38, 2)
                .unwrap(),
            )
        } else {
            Arc::new(Int64Array::from_iter(
                (offset..offset + count).map(|row| (row % 5 != 0).then_some(row as i64 - 100)),
            ))
        };
        columns.push((format!("value{index}"), column));
    }
    columns.push((
        "ts".into(),
        Arc::new(Int64Array::from_iter_values(
            (offset..offset + count).map(|row| (row % 3) as i64 * 1000 - 900),
        )),
    ));
    RecordBatch::try_from_iter(columns).unwrap()
}

fn rows(batch: &RecordBatch) -> Vec<String> {
    let mut rows = (0..batch.num_rows())
        .map(|row| {
            (0..batch.num_columns())
                .map(|column| {
                    format!(
                        "{:?}",
                        ScalarValue::try_from_array(batch.column(column), row).unwrap()
                    )
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn exported(value: &mut TumblingAggregator) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    value
        .visit_canonical_tumble(|batch| {
            batches.push(batch);
            Ok(())
        })
        .unwrap();
    batches
}

#[test]
fn scalar_accumulators_continue_across_both_stores_and_keyless_windows() {
    for source_rocks in [false, true] {
        for target_rocks in [false, true] {
            for keyless in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let source_path = directory.path().join("source");
                let target_path = directory.path().join("target");
                let mut source = create(
                    source_rocks.then_some(source_path.as_path()),
                    keyless,
                    false,
                );
                let mut target = create(
                    target_rocks.then_some(target_path.as_path()),
                    keyless,
                    false,
                );
                source.update(&input(keyless, false, 0, 45)).unwrap();
                source.flush(500).unwrap();
                for batch in exported(&mut source) {
                    target
                        .import_canonical_tumble(&batch, usize::from(!keyless))
                        .unwrap();
                }
                target.current_watermark = source.current_watermark;
                assert_eq!(
                    exported(&mut source)
                        .iter()
                        .flat_map(rows)
                        .collect::<std::collections::BTreeSet<_>>(),
                    exported(&mut target).iter().flat_map(rows).collect()
                );
                let suffix = input(keyless, false, 45, 50);
                source.update(&suffix).unwrap();
                target.update(&suffix).unwrap();
                assert_eq!(source.late_drops, target.late_drops);
                for watermark in [999, 1999, i64::MAX] {
                    assert_eq!(
                        rows(&source.flush(watermark).unwrap()),
                        rows(&target.flush(watermark).unwrap())
                    );
                }
            }
        }
    }
}

#[test]
fn pending_count_bundles_export_in_chunks_and_survive_a_failed_receiver() {
    for rocks in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source");
        let mut source = create(rocks.then_some(path.as_path()), false, true);
        let input = RecordBatch::try_from_iter(vec![
            (
                "key0",
                Arc::new(Int64Array::from_iter_values(0..2055)) as ArrayRef,
            ),
            ("value0", Arc::new(Int64Array::from(vec![1; 2055]))),
            ("ts", Arc::new(Int64Array::from(vec![100; 2055]))),
        ])
        .unwrap();
        source.update(&input).unwrap();
        let mut calls = 0;
        assert!(source
            .visit_canonical_tumble(|batch| {
                calls += 1;
                assert_eq!(batch.num_rows(), 1024);
                Err(invalid("receiver failed"))
            })
            .is_err());
        assert_eq!(calls, 1);
        let batches = exported(&mut source);
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![1024, 1024, 7]
        );
        let mut target = create(None, false, true);
        for batch in batches {
            target.import_canonical_tumble(&batch, 1).unwrap();
        }
        assert_eq!(
            rows(&source.flush(999).unwrap()),
            rows(&target.flush(999).unwrap())
        );
    }
}

#[test]
fn invalid_and_duplicate_batches_do_not_commit_partial_state() {
    let mut source = create(None, false, false);
    source.update(&input(false, false, 0, 45)).unwrap();
    let batch = exported(&mut source).remove(0);
    for rocks in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("target");
        let mut target = create(rocks.then_some(path.as_path()), false, false);
        let duplicate = arrow::compute::concat_batches(&batch.schema(), [&batch, &batch]).unwrap();
        assert!(target.import_canonical_tumble(&duplicate, 1).is_err());
        assert!(exported(&mut target).is_empty());
        let mut columns = batch.columns().to_vec();
        let mut ends = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect::<Vec<_>>();
        *ends.last_mut().unwrap() += 1;
        columns[1] = Arc::new(Int64Array::from(ends));
        let invalid = RecordBatch::try_new(batch.schema(), columns).unwrap();
        assert!(target.import_canonical_tumble(&invalid, 1).is_err());
        assert!(exported(&mut target).is_empty());
        target.import_canonical_tumble(&batch, 1).unwrap();
        assert!(target.import_canonical_tumble(&batch, 1).is_err());
        assert_eq!(
            rows(&batch)
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            exported(&mut target).iter().flat_map(rows).collect()
        );
    }
}

#[test]
fn shared_panes_and_distinct_require_their_own_adapter() {
    for (size, slide, cumulative, kind) in [
        (3000, 1000, false, 0),
        (3000, 1000, true, 0),
        (1000, 1000, false, 7),
    ] {
        let mut source = TumblingAggregator::new(size, slide, cumulative, vec![0], vec![kind]);
        assert!(source
            .visit_canonical_tumble(|_| panic!("unsupported state must not be emitted"))
            .is_err());
    }
}

#[test]
fn canonical_import_reserves_state_and_releases_the_pool_on_close() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
    let mut source = create(None, false, false);
    source.update(&input(false, false, 0, 45)).unwrap();
    {
        let mut target = create(None, false, false).with_memory_pool(&pool).unwrap();
        for batch in exported(&mut source) {
            target.import_canonical_tumble(&batch, 1).unwrap();
        }
        assert_eq!(pool.reserved(), target.computed_state_bytes());
        assert!(pool.reserved() > 0);
    }
    assert_eq!(pool.reserved(), 0);
}
