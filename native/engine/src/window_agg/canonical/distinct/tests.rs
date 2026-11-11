use super::*;
use arrow::array::Float64Array;

fn aggregator(shape: &str, value_type: i64) -> TumblingAggregator {
    let size = if shape == "TUMBLE" { 1000 } else { 3000 };
    let mut value = TumblingAggregator::new(
        size,
        1000,
        shape == "CUMULATE",
        vec![value_type, 0],
        vec![7, 3],
    );
    value.bind_canonical_distinct(0).unwrap();
    if shape != "TUMBLE" {
        value.enable_canonical_slicing(1).unwrap();
    }
    value
}

fn counted_aggregator(shape: &str, value_type: i64) -> TumblingAggregator {
    let size = if shape == "TUMBLE" { 1000 } else { 3000 };
    let mut value = TumblingAggregator::new(
        size,
        1000,
        shape == "CUMULATE",
        vec![value_type, 0],
        vec![COUNT_DISTINCT_WITH_COUNTS, 3],
    );
    value.bind_canonical_distinct(0).unwrap();
    if shape != "TUMBLE" {
        value.enable_canonical_slicing(1).unwrap();
    }
    value
}

#[test]
fn counted_views_preserve_exact_frequencies_through_checkpoint_and_engine_transfer() {
    for shape in ["TUMBLE", "HOP", "CUMULATE"] {
        for keyless in [false, true] {
            let mut source = counted_aggregator(shape, 0);
            for time in [100, 200, 1100, 1150] {
                source
                    .update(&input(
                        Arc::new(Int64Array::from(vec![10, 10, 20])),
                        keyless,
                        time,
                    ))
                    .unwrap();
            }
            source.flush(1000).unwrap();
            let original = views(&mut source);
            assert!(original.iter().any(|batch| batch
                .column(batch.num_columns() - 1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .flatten()
                .any(|count| count > 1)));
            let partitions = source
                .snapshot_partitions(
                    128,
                    if keyless {
                        &[i32::MIN]
                    } else {
                        &[i32::MIN, -1]
                    },
                )
                .into_values()
                .collect::<Vec<_>>();
            let mut checkpoint = TumblingAggregator::restore_partitions(
                if shape == "TUMBLE" { 1000 } else { 3000 },
                1000,
                shape == "CUMULATE",
                vec![0, 0],
                vec![COUNT_DISTINCT_WITH_COUNTS, 3],
                &partitions,
            );
            checkpoint.bind_canonical_distinct(0).unwrap();
            let collect = |batches: Vec<RecordBatch>| {
                let mut rows = batches.iter().flat_map(render).collect::<Vec<_>>();
                rows.sort();
                rows
            };
            assert_eq!(collect(original), collect(views(&mut checkpoint)));
            let mut target = counted_aggregator(shape, 0);
            for batch in state(&mut checkpoint) {
                target
                    .import_canonical_state(&batch, usize::from(!keyless), 1000)
                    .unwrap();
            }
            for batch in views(&mut checkpoint) {
                target.import_canonical_distinct(0, &batch).unwrap();
            }
            target.finish_canonical_distinct().unwrap();
            if shape != "TUMBLE" {
                checkpoint
                    .visit_canonical_timers(|batch| {
                        target.import_canonical_timers(&batch, usize::from(!keyless))
                    })
                    .unwrap();
            }
            target.current_watermark = checkpoint.current_watermark;
            for time in [1200, 1600, 2100] {
                let suffix = input(Arc::new(Int64Array::from(vec![10, 20, 30])), keyless, time);
                source.update(&suffix).unwrap();
                checkpoint.update(&suffix).unwrap();
                target.update(&suffix).unwrap();
            }
            assert_eq!(collect(views(&mut source)), collect(views(&mut target)));
            for watermark in [2000, 3000, 6000, i64::MAX] {
                let expected = render(&source.flush(watermark).unwrap());
                assert_eq!(expected, render(&checkpoint.flush(watermark).unwrap()));
                assert_eq!(expected, render(&target.flush(watermark).unwrap()));
            }
        }
    }
}

#[test]
fn counted_view_import_is_atomic_and_preserves_signed_counts_without_normalizing() {
    let mut source = counted_aggregator("HOP", 0);
    source
        .update(&input(
            Arc::new(Int64Array::from(vec![10, 10, 20])),
            true,
            100,
        ))
        .unwrap();
    let main = state(&mut source).remove(0);
    let view = views(&mut source).remove(0);
    for corrupt in ["duplicate", "zero", "orphan"] {
        let mut target = counted_aggregator("HOP", 0);
        target.import_canonical_state(&main, 0, 1000).unwrap();
        let damaged = match corrupt {
            "duplicate" => arrow::compute::concat_batches(
                &view.schema(),
                [&view.slice(0, 1), &view.slice(0, 1)],
            )
            .unwrap(),
            "zero" => RecordBatch::try_new(
                view.schema(),
                vec![
                    view.column(0).clone(),
                    view.column(1).clone(),
                    Arc::new(Int64Array::from(vec![0; view.num_rows()])),
                ],
            )
            .unwrap(),
            _ => RecordBatch::try_new(
                view.schema(),
                vec![
                    Arc::new(Int64Array::from(vec![2000; view.num_rows()])),
                    view.column(1).clone(),
                    view.column(2).clone(),
                ],
            )
            .unwrap(),
        };
        assert!(target.import_canonical_distinct(0, &damaged).is_err());
        target.import_canonical_distinct(0, &view).unwrap();
        target.finish_canonical_distinct().unwrap();
        assert_eq!(render(&view), render(&views(&mut target).remove(0)));
    }
    let signed = RecordBatch::try_new(
        view.schema(),
        vec![
            view.column(0).clone(),
            view.column(1).clone(),
            Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX])),
        ],
    )
    .unwrap();
    let mut target = counted_aggregator("HOP", 0);
    target.import_canonical_state(&main, 0, 1000).unwrap();
    target.import_canonical_distinct(0, &signed).unwrap();
    target.finish_canonical_distinct().unwrap();
    assert_eq!(render(&signed), render(&views(&mut target).remove(0)));
    let mut restored = TumblingAggregator::restore(
        3000,
        1000,
        false,
        vec![0, 0],
        vec![COUNT_DISTINCT_WITH_COUNTS, 3],
        &target.snapshot(),
    );
    restored.bind_canonical_distinct(0).unwrap();
    assert_eq!(render(&signed), render(&views(&mut restored).remove(0)));
}

fn input(values: ArrayRef, keyless: bool, time: i64) -> RecordBatch {
    let count = values.len();
    let mut columns = Vec::new();
    if !keyless {
        columns.push((
            "key0",
            Arc::new(Int64Array::from_iter(
                (0..count).map(|row| (row % 5 != 0).then_some((row % 3) as i64)),
            )) as ArrayRef,
        ));
    }
    columns.push(("value0", values));
    columns.push(("value1", Arc::new(Int64Array::from(vec![1; count]))));
    columns.push(("ts", Arc::new(Int64Array::from(vec![time; count]))));
    RecordBatch::try_from_iter(columns).unwrap()
}

fn render(batch: &RecordBatch) -> Vec<String> {
    let mut rows = (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|column| format!("{:?}", ScalarValue::try_from_array(column, row).unwrap()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn state(source: &mut TumblingAggregator) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    source
        .visit_canonical_state(|batch| {
            batches.push(batch);
            Ok(())
        })
        .unwrap();
    batches
}

fn views(source: &mut TumblingAggregator) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    source
        .visit_canonical_distinct(0, |batch| {
            batches.push(batch);
            Ok(())
        })
        .unwrap();
    batches
}

#[test]
fn counts_and_memberships_continue_through_each_slice_lifecycle() {
    let values: Vec<(i64, ArrayRef)> = vec![
        (
            0,
            Arc::new(Int64Array::from(vec![
                None,
                Some(10),
                Some(10),
                Some(20),
                Some(30),
                Some(10),
            ])),
        ),
        (
            3,
            Arc::new(StringArray::from(vec![
                None,
                Some("ten"),
                Some("ten"),
                Some("twenty"),
                Some(""),
                Some("ten"),
            ])),
        ),
        (
            1,
            Arc::new(Float64Array::from(vec![
                None,
                Some(0.0),
                Some(-0.0),
                Some(f64::from_bits(0x7ff8000000000001)),
                Some(f64::from_bits(0xfff8000000000002)),
                Some(f64::INFINITY),
            ])),
        ),
        (
            5802,
            Arc::new(
                Decimal128Array::from(vec![
                    None,
                    Some(i128::MAX / 2),
                    Some(i128::MAX / 2),
                    Some(-100),
                    Some(0),
                    Some(100),
                ])
                .with_precision_and_scale(38, 2)
                .unwrap(),
            ),
        ),
    ];
    for shape in ["TUMBLE", "HOP", "CUMULATE"] {
        for keyless in [false, true] {
            for (code, values) in &values {
                let mut source = aggregator(shape, *code);
                source.update(&input(values.clone(), keyless, 100)).unwrap();
                source
                    .update(&input(values.clone(), keyless, 1100))
                    .unwrap();
                source.flush(1000).unwrap();
                let mut target = aggregator(shape, *code);
                let arity = usize::from(!keyless);
                for batch in state(&mut source) {
                    target.import_canonical_state(&batch, arity, 1000).unwrap();
                }
                for batch in views(&mut source) {
                    target.import_canonical_distinct(0, &batch).unwrap();
                }
                target.finish_canonical_distinct().unwrap();
                if shape != "TUMBLE" {
                    source
                        .visit_canonical_timers(|batch| {
                            target.import_canonical_timers(&batch, arity)
                        })
                        .unwrap();
                }
                target.current_watermark = source.current_watermark;
                for timestamp in [1200, 2200, 3200] {
                    let suffix = input(values.clone(), keyless, timestamp);
                    source.update(&suffix).unwrap();
                    target.update(&suffix).unwrap();
                }
                for watermark in [2000, 3000, 4000, 6000, i64::MAX] {
                    assert_eq!(
                        render(&source.flush(watermark).unwrap()),
                        render(&target.flush(watermark).unwrap()),
                        "{shape}, type={code}, keyless={keyless}"
                    );
                }
            }
        }
    }
}

#[test]
fn unbound_and_non_distinct_factories_remain_rejected() {
    let mut value = TumblingAggregator::new(1000, 1000, false, vec![0], vec![7]);
    assert!(value
        .validate_canonical_tumble()
        .unwrap_err()
        .to_string()
        .contains("data-view adapter"));
    assert!(value.visit_canonical_distinct(0, |_| Ok(())).is_err());
    assert!(value.bind_canonical_distinct(1).is_err());
    value.bind_canonical_distinct(0).unwrap();
    value.validate_canonical_tumble().unwrap();
    let mut scalar = TumblingAggregator::new(1000, 1000, false, vec![0], vec![3]);
    assert!(scalar.bind_canonical_distinct(0).is_err());
}

#[test]
fn missing_or_excessive_membership_never_finishes_import() {
    let mut source = aggregator("TUMBLE", 0);
    source
        .update(&input(
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            true,
            100,
        ))
        .unwrap();
    let main = state(&mut source).remove(0);
    let view = views(&mut source).remove(0);
    let mut target = aggregator("TUMBLE", 0);
    assert!(target.import_canonical_distinct(0, &view).is_err());
    target.import_canonical_state(&main, 0, 1000).unwrap();
    assert!(target.finish_canonical_distinct().is_err());
    assert!(target.visit_canonical_state(|_| Ok(())).is_err());
    target
        .import_canonical_distinct(0, &view.slice(0, 2))
        .unwrap();
    assert!(target.finish_canonical_distinct().is_err());
    assert!(target
        .import_canonical_distinct(0, &view.slice(0, 1))
        .is_err());
    target
        .import_canonical_distinct(0, &view.slice(2, 1))
        .unwrap();
    target.finish_canonical_distinct().unwrap();
    assert_eq!(render(&main), render(&state(&mut target).remove(0)));
    assert!(target.import_canonical_distinct(0, &view).is_err());
}

#[test]
fn duplicate_or_orphan_in_a_batch_commits_no_memberships() {
    let mut source = aggregator("TUMBLE", 0);
    source
        .update(&input(
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            true,
            100,
        ))
        .unwrap();
    let main = state(&mut source).remove(0);
    let view = views(&mut source).remove(0);
    for orphan in [false, true] {
        let mut target = aggregator("TUMBLE", 0);
        target.import_canonical_state(&main, 0, 1000).unwrap();
        let corrupt = if orphan {
            RecordBatch::try_new(
                view.schema(),
                vec![
                    Arc::new(Int64Array::from(vec![1000, 2000])),
                    view.column(1).slice(0, 2),
                ],
            )
            .unwrap()
        } else {
            arrow::compute::concat_batches(&view.schema(), [&view.slice(0, 1), &view.slice(0, 1)])
                .unwrap()
        };
        assert!(target.import_canonical_distinct(0, &corrupt).is_err());
        target.import_canonical_distinct(0, &view).unwrap();
        target.finish_canonical_distinct().unwrap();
        assert_eq!(render(&main), render(&state(&mut target).remove(0)));
    }
}

#[test]
fn invalid_counts_and_schemas_do_not_commit_main_or_view_state() {
    let mut source = aggregator("TUMBLE", 0);
    source
        .update(&input(
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            true,
            100,
        ))
        .unwrap();
    let main = state(&mut source).remove(0);
    let view = views(&mut source).remove(0);
    let mut target = aggregator("TUMBLE", 0);
    let mut columns = main.columns().to_vec();
    columns[2] = Arc::new(Int64Array::from(vec![-1]));
    let corrupt = RecordBatch::try_new(main.schema(), columns).unwrap();
    assert!(target.import_canonical_state(&corrupt, 0, 1000).is_err());
    assert!(state(&mut target).is_empty());
    target.import_canonical_state(&main, 0, 1000).unwrap();
    let wrong_type = RecordBatch::try_from_iter(vec![
        (
            "window_end",
            Arc::new(Int64Array::from(vec![1000])) as ArrayRef,
        ),
        ("value", Arc::new(StringArray::from(vec!["ten"]))),
    ])
    .unwrap();
    assert!(target.import_canonical_distinct(0, &wrong_type).is_err());
    let null = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("window_end", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1000])),
            Arc::new(Int64Array::from(vec![None])),
        ],
    )
    .unwrap();
    assert!(target.import_canonical_distinct(0, &null).is_err());
    target.import_canonical_distinct(0, &view).unwrap();
    target.finish_canonical_distinct().unwrap();
}

#[test]
fn bounded_view_export_survives_receiver_failure_retry_and_native_checkpoint() {
    let mut source = aggregator("HOP", 0);
    source
        .update(&input(
            Arc::new(Int64Array::from_iter_values(0..1539)),
            true,
            100,
        ))
        .unwrap();
    let before = render(&state(&mut source).remove(0));
    assert!(source
        .visit_canonical_distinct(0, |_| Err(invalid("writer failed")))
        .is_err());
    assert_eq!(before, render(&state(&mut source).remove(0)));
    let batches = views(&mut source);
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![1024, 515]
    );
    let bytes = source.snapshot();
    let mut restored =
        TumblingAggregator::restore(3000, 1000, false, vec![0, 0], vec![7, 3], &bytes);
    assert!(restored.validate_canonical_shared().is_err());
    assert!(restored.visit_canonical_distinct(0, |_| Ok(())).is_err());
    restored.bind_canonical_distinct(0).unwrap();
    let collect = |batches: Vec<RecordBatch>| {
        let mut rows = batches.iter().flat_map(render).collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(collect(batches), collect(views(&mut restored)));
    source
        .update(&input(
            Arc::new(Int64Array::from(vec![10, 20, 2000])),
            true,
            200,
        ))
        .unwrap();
    restored
        .update(&input(
            Arc::new(Int64Array::from(vec![10, 20, 2000])),
            true,
            200,
        ))
        .unwrap();
    assert_eq!(
        render(&source.flush(6000).unwrap()),
        render(&restored.flush(6000).unwrap())
    );
}

#[test]
fn null_only_panes_transfer_zero_cardinality_without_losing_row_liveness() {
    for shape in ["TUMBLE", "HOP", "CUMULATE"] {
        let mut source = aggregator(shape, 3);
        source
            .update(&input(
                Arc::new(StringArray::from(vec![None::<&str>; 5])),
                true,
                100,
            ))
            .unwrap();
        let main = state(&mut source).remove(0);
        assert_eq!(
            main.column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
        assert_eq!(
            main.column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            5
        );
        assert!(views(&mut source).is_empty());
        let mut target = aggregator(shape, 3);
        target.import_canonical_state(&main, 0, 1000).unwrap();
        target.finish_canonical_distinct().unwrap();
        if shape != "TUMBLE" {
            source
                .visit_canonical_timers(|batch| target.import_canonical_timers(&batch, 0))
                .unwrap();
        }
        assert_eq!(
            render(&source.flush(6000).unwrap()),
            render(&target.flush(6000).unwrap())
        );
    }
}

#[test]
fn independent_distinct_slots_must_all_match_their_own_scalar_counts() {
    let make = || {
        let mut value = TumblingAggregator::new(3000, 1000, false, vec![0, 3, 0], vec![7, 7, 3]);
        value.bind_canonical_distinct(0).unwrap();
        assert!(value.enable_canonical_slicing(2).is_err());
        value.bind_canonical_distinct(1).unwrap();
        value.enable_canonical_slicing(2).unwrap();
        value
    };
    let mut source = make();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "value0",
            Arc::new(Int64Array::from(vec![10, 10, 20, 20])) as ArrayRef,
        ),
        (
            "value1",
            Arc::new(StringArray::from(vec![
                None,
                Some("one"),
                Some("two"),
                Some("three"),
            ])),
        ),
        ("value2", Arc::new(Int64Array::from(vec![1; 4]))),
        ("ts", Arc::new(Int64Array::from(vec![100; 4]))),
    ])
    .unwrap();
    source.update(&batch).unwrap();
    let mut target = make();
    for batch in state(&mut source) {
        target.import_canonical_state(&batch, 0, 1000).unwrap();
    }
    source
        .visit_canonical_distinct(0, |batch| target.import_canonical_distinct(0, &batch))
        .unwrap();
    assert!(target.finish_canonical_distinct().is_err());
    source
        .visit_canonical_distinct(1, |batch| target.import_canonical_distinct(1, &batch))
        .unwrap();
    target.finish_canonical_distinct().unwrap();
    source
        .visit_canonical_timers(|batch| target.import_canonical_timers(&batch, 0))
        .unwrap();
    source.update(&batch).unwrap();
    target.update(&batch).unwrap();
    assert_eq!(
        render(&source.flush(6000).unwrap()),
        render(&target.flush(6000).unwrap())
    );
}

#[test]
fn transient_counts_and_live_memberships_are_reserved_until_finalize_and_close() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
    let mut source = aggregator("TUMBLE", 3);
    source
        .update(&input(
            Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
            false,
            100,
        ))
        .unwrap();
    {
        let mut target = aggregator("TUMBLE", 3).with_memory_pool(&pool).unwrap();
        for batch in state(&mut source) {
            target.import_canonical_state(&batch, 1, 1000).unwrap();
        }
        assert_eq!(pool.reserved(), target.computed_state_bytes());
        for batch in views(&mut source) {
            target.import_canonical_distinct(0, &batch).unwrap();
        }
        assert_eq!(pool.reserved(), target.computed_state_bytes());
        for batch in views(&mut source) {
            assert!(target.import_canonical_distinct(0, &batch).is_err());
            assert_eq!(pool.reserved(), target.computed_state_bytes());
        }
        target.finish_canonical_distinct().unwrap();
        assert_eq!(pool.reserved(), target.computed_state_bytes());
        assert!(target.canonical_distinct_counts.is_empty());
        assert!(target
            .visit_canonical_distinct(0, |_| Err(invalid("writer failed")))
            .is_err());
        assert_eq!(pool.reserved(), target.computed_state_bytes());
        assert!(pool.reserved() > 0);
    }
    assert_eq!(pool.reserved(), 0);
}

#[cfg(feature = "rocksdb-state")]
fn persistent(
    shape: &str,
    counted: bool,
    keyless: bool,
    path: &std::path::Path,
) -> TumblingAggregator {
    use crate::state::rocks_store::window_memberships::WindowStateCodec;
    let base = if counted {
        counted_aggregator(shape, 0)
    } else {
        aggregator(shape, 0)
    };
    let mut store = crate::state::RocksWindowAggStore::create_with_codec(
        crate::state::RocksStoreConfig {
            table_dir: path.to_string_lossy().into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        },
        WindowStateCodec::new(&base.aggregates).unwrap(),
        0..=127,
    )
    .unwrap();
    store.set_shared_slices(shape != "TUMBLE");
    base.with_key_timestamp_precisions(if keyless {
        vec![i32::MIN]
    } else {
        vec![i32::MIN, -1]
    })
    .with_store(
        store,
        if keyless {
            vec![]
        } else {
            vec![DataType::Int64]
        },
    )
    .with_read_through_budget(64 * 1024 * 1024)
    .unwrap()
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn persistent_views_transfer_both_directions_without_resident_membership_maps() {
    for shape in ["TUMBLE", "HOP", "CUMULATE"] {
        for counted in [false, true] {
            for keyless in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let mut source = if counted {
                    counted_aggregator(shape, 0)
                } else {
                    aggregator(shape, 0)
                };
                let mut target = persistent(shape, counted, keyless, &temp.path().join("target"));
                let values = Arc::new(Int64Array::from_iter_values(0..2051)) as ArrayRef;
                source.update(&input(values.clone(), keyless, 100)).unwrap();
                for batch in state(&mut source) {
                    target
                        .import_canonical_state(&batch, usize::from(!keyless), 1000)
                        .unwrap();
                }
                let suffix = input(Arc::new(Int64Array::from(vec![10, 20, 3000])), keyless, 500);
                assert!(
                    target.update(&suffix).is_err(),
                    "unfinished restore must reject input"
                );
                assert!(target.visit_canonical_state(|_| Ok(())).is_err());
                for batch in views(&mut source) {
                    target.import_canonical_distinct(0, &batch).unwrap();
                }
                target.finish_canonical_distinct().unwrap();
                if shape != "TUMBLE" {
                    source
                        .visit_canonical_timers(|batch| {
                            target.import_canonical_timers(&batch, usize::from(!keyless))
                        })
                        .unwrap();
                }
                assert!(
                    target.windows.is_empty(),
                    "persistent canonical import must keep sets on disk"
                );
                let normalize = |batches: Vec<RecordBatch>| {
                    let mut rows = batches.iter().flat_map(render).collect::<Vec<_>>();
                    rows.sort();
                    rows
                };
                assert_eq!(normalize(state(&mut source)), normalize(state(&mut target)));
                assert_eq!(normalize(views(&mut source)), normalize(views(&mut target)));
                let mut returned = if counted {
                    counted_aggregator(shape, 0)
                } else {
                    aggregator(shape, 0)
                };
                for batch in state(&mut target) {
                    returned
                        .import_canonical_state(&batch, usize::from(!keyless), 1000)
                        .unwrap();
                }
                for batch in views(&mut target) {
                    returned.import_canonical_distinct(0, &batch).unwrap();
                }
                returned.finish_canonical_distinct().unwrap();
                if shape != "TUMBLE" {
                    target
                        .visit_canonical_timers(|batch| {
                            returned.import_canonical_timers(&batch, usize::from(!keyless))
                        })
                        .unwrap();
                }
                for owner in [&mut source, &mut target, &mut returned] {
                    owner.update(&suffix).unwrap();
                }
                for watermark in [1000, 2000, 4000] {
                    let expected = render(&source.flush(watermark).unwrap());
                    assert_eq!(
                        expected,
                        render(&target.flush(watermark).unwrap()),
                        "{shape}, counted={counted}, keyless={keyless}"
                    );
                    assert_eq!(expected, render(&returned.flush(watermark).unwrap()));
                }
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn previous_snapshot_backed_checkpoints_migrate_one_partition_at_a_time() {
    use crate::state::rocks_store::window_memberships::WindowStateCodec;
    use crate::state::{RocksSnapshotStore, RocksStoreConfig, RocksWindowAggStore};
    for shape in ["TUMBLE", "HOP", "CUMULATE"] {
        for aligned in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let mut source = counted_aggregator(shape, 0);
            source
                .update(&input(
                    Arc::new(Int64Array::from(vec![1, 1, 5])),
                    false,
                    100,
                ))
                .unwrap();
            let parts = source
                .snapshot_partitions(128, &[i32::MIN, -1])
                .into_iter()
                .map(|(group, payload)| {
                    let mut part = group.to_be_bytes().to_vec();
                    part.extend_from_slice(&payload);
                    part
                })
                .collect::<Vec<_>>();
            let config = |name: &str| RocksStoreConfig {
                table_dir: temp.path().join(name).to_string_lossy().into_owned(),
                max_parallelism: 128,
                options_json: include_str!("../../../../benches/fixtures/rocks-options.json")
                    .into(),
                ttl_ms: 0,
                shared_resources: 0,
            };
            let mut old =
                RocksSnapshotStore::open_merged(config("old"), &[], 0..=127, false).unwrap();
            let saved = temp.path().join("checkpoint");
            let manifest = old
                .checkpoint(&parts, 4567, saved.to_str().unwrap())
                .unwrap();
            drop(old);
            let base = counted_aggregator(shape, 0);
            let store = RocksWindowAggStore::open_merged_with_codec(
                config("typed"),
                WindowStateCodec::new(&base.aggregates).unwrap(),
                0..=127,
                &[(saved.to_string_lossy().into_owned(), manifest.snapshot_id)],
                aligned,
            )
            .unwrap();
            let mut target = base
                .with_key_timestamp_precisions(vec![i32::MIN, -1])
                .with_store(store, vec![DataType::Int64]);
            target.migrate_legacy_snapshot_store().unwrap();
            assert_eq!(target.store_timer_deadline(), 4567);
            let suffix = input(Arc::new(Int64Array::from(vec![1, 7])), false, 500);
            source.update(&suffix).unwrap();
            target.update(&suffix).unwrap();
            for watermark in [1000, 2000, 4000] {
                assert_eq!(
                    render(&source.flush(watermark).unwrap()),
                    render(&target.flush(watermark).unwrap())
                );
            }
            use crate::state::rocks_store::window_memberships::WindowMembershipState;
            assert!(target
                .store
                .as_ref()
                .unwrap()
                .legacy_snapshot_reader()
                .unwrap()
                .is_none());
        }
    }
}
