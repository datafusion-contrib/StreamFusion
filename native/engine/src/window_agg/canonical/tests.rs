use super::*;

fn shared(
    path: Option<&std::path::Path>,
    keyless: bool,
    cumulative: bool,
    count_only: bool,
) -> TumblingAggregator {
    let mut value = create(path, keyless, count_only);
    value.window_millis = 3000;
    value.cumulative = cumulative;
    value
        .enable_canonical_slicing(if count_only { 0 } else { 1 })
        .unwrap();
    value
}

fn shared_input(keyless: bool, offset: usize, count: usize) -> RecordBatch {
    let batch = input(keyless, false, offset, count);
    let mut columns = batch.columns().to_vec();
    let index = batch.schema().index_of("value1").unwrap();
    columns[index] = Arc::new(Int64Array::from(vec![1; count]));
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

fn copy_shared(source: &mut TumblingAggregator, target: &mut TumblingAggregator, keyless: bool) {
    source
        .visit_canonical_state(|batch| {
            target.import_canonical_state(&batch, usize::from(!keyless), 1000)
        })
        .unwrap();
    source
        .visit_canonical_timers(|batch| {
            target.import_canonical_timers(&batch, usize::from(!keyless))
        })
        .unwrap();
    target.current_watermark = source.current_watermark;
}

#[test]
fn shared_panes_and_cumulative_prefixes_continue_across_native_stores() {
    for cumulative in [false, true] {
        for source_rocks in [false, true] {
            for target_rocks in [false, true] {
                for keyless in [false, true] {
                    let directory = tempfile::tempdir().unwrap();
                    let source_path = directory.path().join("source");
                    let target_path = directory.path().join("target");
                    let mut source = shared(
                        source_rocks.then_some(source_path.as_path()),
                        keyless,
                        cumulative,
                        false,
                    );
                    let mut target = shared(
                        target_rocks.then_some(target_path.as_path()),
                        keyless,
                        cumulative,
                        false,
                    );
                    source.update(&shared_input(keyless, 0, 45)).unwrap();
                    source.flush(2000).unwrap();
                    copy_shared(&mut source, &mut target, keyless);
                    let suffix = shared_input(keyless, 45, 50);
                    source.update(&suffix).unwrap();
                    target.update(&suffix).unwrap();
                    for watermark in [2000, 3000, 4000, 6000, i64::MAX] {
                        assert_eq!(
                            rows(&source.flush(watermark).unwrap()),
                            rows(&target.flush(watermark).unwrap())
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn timer_only_namespaces_survive_bounded_transfer_and_failed_receiver() {
    for rocks in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source");
        let mut source = shared(rocks.then_some(path.as_path()), false, false, true);
        let batch = RecordBatch::try_from_iter(vec![
            (
                "key0",
                Arc::new(Int64Array::from_iter_values(0..1539)) as ArrayRef,
            ),
            ("value0", Arc::new(Int64Array::from(vec![1; 1539]))),
            ("ts", Arc::new(Int64Array::from(vec![100; 1539]))),
        ])
        .unwrap();
        source.update(&batch).unwrap();
        assert_eq!(source.flush(3000).unwrap().num_rows(), 1539 * 3);
        source
            .visit_canonical_state(|_| panic!("all panes expired"))
            .unwrap();
        assert!(source
            .visit_canonical_timers(|_| Err(invalid("receiver failed")))
            .is_err());
        let target_path = directory.path().join("target");
        let mut target = shared(
            (!rocks).then_some(target_path.as_path()),
            false,
            false,
            true,
        );
        let mut chunks = Vec::new();
        source
            .visit_canonical_timers(|batch| {
                chunks.push(batch.num_rows());
                target.import_canonical_timers(&batch, 1)
            })
            .unwrap();
        assert_eq!(chunks, vec![1024, 515]);
        assert!(source.flush(4000).unwrap().num_rows() == 0);
        assert!(target.flush(4000).unwrap().num_rows() == 0);
        source
            .visit_canonical_timers(|_| panic!("empty firing must clear the final timer"))
            .unwrap();
        target
            .visit_canonical_timers(|_| panic!("restored empty firing must clear the final timer"))
            .unwrap();
    }
}

#[test]
fn malformed_timer_batches_never_commit_partial_ledger_entries() {
    for rocks in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("target");
        let mut target = shared(rocks.then_some(path.as_path()), false, false, true);
        let batch = RecordBatch::try_from_iter(vec![
            (
                "window_end",
                Arc::new(Int64Array::from(vec![1000, 1000])) as ArrayRef,
            ),
            ("key0", Arc::new(Int64Array::from(vec![7, 7]))),
        ])
        .unwrap();
        assert!(target.import_canonical_timers(&batch, 1).is_err());
        target
            .visit_canonical_timers(|_| panic!("rejected duplicate ledger was committed"))
            .unwrap();
        let valid = batch.slice(0, 1);
        target.import_canonical_timers(&valid, 1).unwrap();
        assert!(target.import_canonical_timers(&valid, 1).is_err());
        let mut actual = Vec::new();
        target
            .visit_canonical_timers(|batch| {
                actual.extend(rows(&batch));
                Ok(())
            })
            .unwrap();
        assert_eq!(rows(&valid), actual);
    }
}

#[test]
fn fixed_offset_shared_assignment_uses_the_same_grid_as_its_namespaces() {
    for cumulative in [false, true] {
        let mut source = shared(None, false, cumulative, false);
        source.set_assignment_offset(750);
        source.update(&shared_input(false, 0, 45)).unwrap();
        source.flush(1250).unwrap();
        let mut target = shared(None, false, cumulative, false);
        target.set_assignment_offset(750);
        copy_shared(&mut source, &mut target, false);
        let suffix = shared_input(false, 45, 30);
        source.update(&suffix).unwrap();
        target.update(&suffix).unwrap();
        for watermark in [2250, 3250, 6250, i64::MAX] {
            assert_eq!(
                rows(&source.flush(watermark).unwrap()),
                rows(&target.flush(watermark).unwrap())
            );
        }
    }
}

#[test]
fn changing_a_populated_final_window_layout_to_shared_panes_is_rejected() {
    for rocks in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source");
        let mut source = create(rocks.then_some(path.as_path()), false, false);
        source.update(&input(false, false, 0, 3)).unwrap();
        assert!(source.enable_canonical_slicing(1).is_err());
    }
}

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

#[test]
fn successive_state_and_timer_batches_retain_the_original_key_converter() {
    for shape in 0..3 {
        for source_rocks in [false, true] {
            for target_rocks in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let source_path = directory.path().join("source");
                let target_path = directory.path().join("target");
                let make = |path| {
                    if shape == 0 {
                        create(path, false, true)
                    } else {
                        shared(path, false, shape == 2, true)
                    }
                };
                let mut source = make(source_rocks.then_some(source_path.as_path()));
                let mut target = make(target_rocks.then_some(target_path.as_path()));
                let batch = RecordBatch::try_from_iter(vec![
                    (
                        "key0",
                        Arc::new(Int64Array::from_iter_values(0..1539)) as ArrayRef,
                    ),
                    ("value0", Arc::new(Int64Array::from(vec![1; 1539]))),
                    ("ts", Arc::new(Int64Array::from(vec![100; 1539]))),
                ])
                .unwrap();
                source.update(&batch).unwrap();
                let mut expected = Vec::new();
                source
                    .visit_canonical_state(|batch| {
                        expected.extend(rows(&batch));
                        for offset in (0..batch.num_rows()).step_by(127) {
                            let chunk = batch.slice(offset, (batch.num_rows() - offset).min(127));
                            target.import_canonical_state(&chunk, 1, 1000)?;
                        }
                        Ok(())
                    })
                    .unwrap();
                if shape != 0 {
                    source
                        .visit_canonical_timers(|batch| {
                            for offset in (0..batch.num_rows()).step_by(127) {
                                target.import_canonical_timers(
                                    &batch.slice(offset, (batch.num_rows() - offset).min(127)),
                                    1,
                                )?;
                            }
                            Ok(())
                        })
                        .unwrap();
                }
                let mut actual = Vec::new();
                target
                    .visit_canonical_state(|batch| {
                        actual.extend(rows(&batch));
                        Ok(())
                    })
                    .unwrap();
                expected.sort();
                actual.sort();
                assert_eq!(expected, actual);
                for watermark in [1000, 2000, 3000, 4000] {
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
fn sql_offsets_survive_canonical_handoffs_across_window_shapes_and_stores() {
    for shape in 0..3 {
        for source_rocks in [false, true] {
            for target_rocks in [false, true] {
                for offset in [-1250, 1250] {
                    for zone_shift in [0, 750] {
                        let directory = tempfile::tempdir().unwrap();
                        let source_path = directory.path().join("source");
                        let target_path = directory.path().join("target");
                        let make = |path: Option<&std::path::Path>| {
                            let mut value = if shape == 0 {
                                create(path, false, true)
                            } else {
                                shared(path, false, shape == 2, true)
                            };
                            value.set_assignment_offset(zone_shift);
                            value.set_window_offset(offset);
                            value
                        };
                        let mut source = make(source_rocks.then_some(source_path.as_path()));
                        let mut target = make(target_rocks.then_some(target_path.as_path()));
                        source.update(&input(false, true, 0, 45)).unwrap();
                        source.flush(1000).unwrap();
                        copy_shared(&mut source, &mut target, false);
                        let suffix = input(false, true, 45, 50);
                        source.update(&suffix).unwrap();
                        target.update(&suffix).unwrap();
                        for watermark in [1250, 1750, 2250, 3250, 6250, i64::MAX] {
                            assert_eq!(
                                rows(&source.flush(watermark).unwrap()),
                                rows(&target.flush(watermark).unwrap()),
                                "shape={shape} offset={offset} zone={zone_shift}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn sql_offsets_survive_raw_checkpoint_partitions_and_reject_mixed_grids() {
    for shape in 0..3 {
        let mut source = if shape == 0 {
            create(None, false, true)
        } else {
            shared(None, false, shape == 2, true)
        };
        source.set_assignment_offset(750);
        source.set_window_offset(-1250);
        source.update(&input(false, true, 0, 45)).unwrap();
        let snapshots = source
            .snapshot_partitions(128, &[-1])
            .into_values()
            .collect::<Vec<_>>();
        assert!(snapshots.len() > 1);
        let size = if shape == 0 { 1000 } else { 3000 };
        let mut restored = TumblingAggregator::restore_partitions(
            size,
            1000,
            shape == 2,
            vec![0],
            vec![3],
            &snapshots,
        );
        assert_eq!(source.window_offset_millis, restored.window_offset_millis);
        assert_eq!(
            source.assignment_offset_millis,
            restored.assignment_offset_millis
        );
        if shape != 0 {
            restored.enable_canonical_slicing(0).unwrap();
        }
        let suffix = input(false, true, 45, 20);
        source.update(&suffix).unwrap();
        restored.update(&suffix).unwrap();
        assert_eq!(
            rows(&source.flush(i64::MAX).unwrap()),
            rows(&restored.flush(i64::MAX).unwrap())
        );
        let mut bad = snapshots;
        let batch = read_ipc_if_present(&bad[0]).remove(0);
        let mut metadata = batch.schema().metadata().clone();
        metadata.insert("window_offset_millis".into(), "123".into());
        let schema = Arc::new(batch.schema().as_ref().clone().with_metadata(metadata));
        bad[0] = write_ipc(&RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap());
        assert!(
            std::panic::catch_unwind(|| TumblingAggregator::restore_partitions(
                size,
                1000,
                shape == 2,
                vec![0],
                vec![3],
                &bad
            ))
            .is_err()
        );
    }
}

#[test]
fn final_window_partials_merge_once_and_handoff_variable_prefix_bounds() {
    for cumulative in [false, true] {
        for source_rocks in [false, true] {
            for target_rocks in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let source_path = directory.path().join("source");
                let target_path = directory.path().join("target");
                let make = |path: Option<&std::path::Path>| {
                    let mut value = create(path, false, true);
                    value.window_millis = 3000;
                    value.cumulative = cumulative;
                    value.set_window_offset(1250);
                    value.set_final_windows(true);
                    value
                };
                let mut source = make(source_rocks.then_some(source_path.as_path()));
                let mut target = make(target_rocks.then_some(target_path.as_path()));
                let partials = RecordBatch::try_from_iter(vec![
                    ("key0", Arc::new(Int64Array::from(vec![7; 3])) as ArrayRef),
                    ("partial0", Arc::new(Int64Array::from(vec![2, 3, 4]))),
                    (
                        "slice_end",
                        Arc::new(Int64Array::from(vec![250, 1250, 2250])),
                    ),
                ])
                .unwrap();
                source.update_partial(&partials).unwrap();
                let mut bad = None;
                source
                    .visit_canonical_tumble(|batch| {
                        let mut columns = batch.columns().to_vec();
                        let starts = column_i64(&batch, "window_start");
                        columns[0] = Arc::new(Int64Array::from_iter_values(
                            (0..batch.num_rows()).map(|i| starts.value(i) + 1000),
                        ));
                        bad = Some(RecordBatch::try_new(batch.schema(), columns).unwrap());
                        target.import_canonical_tumble(&batch, 1)
                    })
                    .unwrap();
                let mut rejected = make(None);
                assert!(rejected.import_canonical_tumble(&bad.unwrap(), 1).is_err());
                rejected
                    .visit_canonical_tumble(|_| panic!("invalid bounds committed"))
                    .unwrap();
                let partitions = if source_rocks {
                    source.canonical_partitions(128, &[-1]).unwrap()
                } else {
                    source.snapshot_partitions(128, &[-1])
                };
                let snapshots = partitions.into_values().collect::<Vec<_>>();
                assert!(!snapshots.is_empty());
                let mut restored = TumblingAggregator::restore_partitions(
                    3000,
                    1000,
                    cumulative,
                    vec![0],
                    vec![3],
                    &snapshots,
                );
                assert!(restored.final_windows);
                let batch = read_ipc_if_present(&snapshots[0]).remove(0);
                let mut metadata = batch.schema().metadata().clone();
                metadata.remove("final_windows");
                let schema = Arc::new(batch.schema().as_ref().clone().with_metadata(metadata));
                let incompatible =
                    write_ipc(&RecordBatch::try_new(schema, batch.columns().to_vec()).unwrap());
                let mixed = vec![snapshots[0].clone(), incompatible];
                assert!(
                    std::panic::catch_unwind(|| TumblingAggregator::restore_partitions(
                        3000,
                        1000,
                        cumulative,
                        vec![0],
                        vec![3],
                        &mixed
                    ))
                    .is_err()
                );
                source.update_partial(&partials).unwrap();
                target.update_partial(&partials).unwrap();
                restored.update_partial(&partials).unwrap();
                let actual = source.flush(i64::MAX).unwrap();
                assert_eq!(rows(&actual), rows(&target.flush(i64::MAX).unwrap()));
                assert_eq!(rows(&actual), rows(&restored.flush(i64::MAX).unwrap()));
                assert_eq!(actual.num_rows(), 3);
                let ends = column_i64(&actual, "window_end");
                let starts = column_i64(&actual, "window_start");
                let counts = column_i64(&actual, "result0");
                for (index, end) in [250, 1250, 2250].into_iter().enumerate() {
                    assert_eq!(ends.value(index), end);
                    assert_eq!(counts.value(index), [4, 6, 8][index]);
                    assert_eq!(
                        starts.value(index),
                        if cumulative {
                            [-1750, -1750, 1250][index]
                        } else {
                            end - 3000
                        }
                    );
                }
            }
        }
    }
}

#[test]
fn buffered_partial_namespaces_survive_late_barriers_and_dormant_state_handoffs() {
    for source_rocks in [false, true] {
        for target_rocks in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let source_path = directory.path().join("source");
            let target_path = directory.path().join("target");
            let mut source = shared(
                source_rocks.then_some(source_path.as_path()),
                false,
                true,
                true,
            );
            let mut target = shared(
                target_rocks.then_some(target_path.as_path()),
                false,
                true,
                true,
            );
            source.set_window_offset(1250);
            target.set_window_offset(1250);
            source.enable_stock_partial_buffer(true);
            target.enable_stock_partial_buffer(true);
            let partial = |end, count| {
                RecordBatch::try_from_iter(vec![
                    ("key0", Arc::new(Int64Array::from(vec![7])) as ArrayRef),
                    ("partial0", Arc::new(Int64Array::from(vec![count]))),
                    ("slice_end", Arc::new(Int64Array::from(vec![end]))),
                ])
                .unwrap()
            };
            source.flush(4151).unwrap();
            source.update_partial(&partial(5250, 1)).unwrap();
            assert!(source.require_flushed_partial_buffer().is_err());
            for mutation in 0..3 {
                assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || match mutation {
                        0 => source.set_window_offset(250),
                        1 => source.set_assignment_offset(500),
                        _ => source.set_final_windows(true),
                    }
                ))
                .is_err());
            }
            assert!(source
                .visit_canonical_state(|_| panic!("pending partial was exported"))
                .is_err());
            source.advance_stock_partials(5100, false).unwrap();
            assert!(source.require_flushed_partial_buffer().is_err());
            for watermark in [5101, 5251, 5651] {
                assert_eq!(0, source.flush(watermark).unwrap().num_rows());
            }
            source.advance_stock_partials(i64::MIN, true).unwrap();
            source.require_flushed_partial_buffer().unwrap();
            assert_eq!(source.memory.state_bytes, source.computed_state_bytes());
            let mut timers = 0;
            source
                .visit_canonical_timers(|batch| {
                    timers += batch.num_rows();
                    Ok(())
                })
                .unwrap();
            assert_eq!(0, timers, "late flush must not invent a cumulative trigger");
            copy_shared(&mut source, &mut target, false);
            assert_eq!(0, source.flush(i64::MAX).unwrap().num_rows());
            target.update_partial(&partial(7250, 2)).unwrap();
            target.advance_stock_partials(7250, false).unwrap();
            let output = target.flush(7251).unwrap();
            assert_eq!(1, output.num_rows());
            assert_eq!(3, column_i64(&output, "result0").value(0));
            assert_eq!(7250, column_i64(&output, "window_end").value(0));
            assert_eq!(0, target.flush(i64::MAX).unwrap().num_rows());
        }
    }
}

#[test]
fn legacy_timer_watermark_replays_only_before_the_first_post_restore_watermark() {
    for cumulative in [false, true] {
        for persistent in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("state");
            let mut source = shared(
                persistent.then_some(path.as_path()),
                false,
                cumulative,
                true,
            );
            let batch = |time| {
                RecordBatch::try_from_iter(vec![
                    ("key0", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                    ("value0", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                    ("ts", Arc::new(Int64Array::from(vec![time])) as ArrayRef),
                ])
                .unwrap()
            };
            source.update(&batch(100)).unwrap();
            assert_eq!(source.flush(1000).unwrap().num_rows(), 1);
            let mut target = shared(None, false, cumulative, true);
            copy_shared(&mut source, &mut target, false);
            target.timer_watermark_uninitialized = true;
            let mut captured = shared(None, false, cumulative, true);
            crate::state::canonical_memory::CanonicalMemoryState::copy_state(
                &mut target,
                &mut captured,
            )
            .unwrap();
            for value in [&mut target, &mut captured] {
                value.update(&batch(500)).unwrap();
                let output = value.flush(2000).unwrap();
                let ends = output
                    .column_by_name("window_end")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(ends.values().as_ref(), &[1000, 2000]);
                value.update(&batch(500)).unwrap();
                let output = value.flush(3000).unwrap();
                let ends = output
                    .column_by_name("window_end")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(ends.values().as_ref(), &[3000]);
            }
        }
    }
}

#[test]
fn memory_capture_reserves_actual_state_and_wrappers_then_keeps_live_accounting() {
    use crate::state::canonical_memory::CanonicalMemoryState;
    for cumulative in [false, true] {
        for tracked in [false, true] {
            let factory = || {
                TumblingAggregator::new(3000, 1000, cumulative, vec![0], vec![7])
                    .with_key_timestamp_precisions(vec![-1])
            };
            let input = |value| {
                RecordBatch::try_from_iter(vec![
                    ("key0", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                    (
                        "value0",
                        Arc::new(Int64Array::from(vec![value])) as ArrayRef,
                    ),
                    ("ts", Arc::new(Int64Array::from(vec![100])) as ArrayRef),
                ])
                .unwrap()
            };
            let mut source = factory();
            if tracked {
                source = source.with_memory_budget(1 << 20).unwrap();
            }
            source.update(&input(1)).unwrap();
            let original = source.computed_state_bytes();
            let mut denied = factory().with_memory_budget(1).unwrap();
            assert!(matches!(
                source.copy_state(&mut denied),
                Err(DataFusionError::ResourcesExhausted(_))
            ));
            assert_eq!(
                original,
                source.computed_state_bytes(),
                "denial must not wrap live state"
            );
            if tracked {
                assert_eq!(original, source.memory.state_bytes);
            }
            let mut frozen = factory().with_memory_budget(1 << 20).unwrap();
            source.copy_state(&mut frozen).unwrap();
            assert!(
                frozen.memory.state_bytes > original,
                "shared wrappers need reservation"
            );
            assert_eq!(frozen.computed_state_bytes(), frozen.memory.state_bytes);
            if tracked {
                assert_eq!(source.computed_state_bytes(), source.memory.state_bytes);
            }
            let reserved = frozen.memory.state_bytes;
            let mut second = factory().with_memory_budget(1 << 20).unwrap();
            source.copy_state(&mut second).unwrap();
            assert_eq!(
                reserved, second.memory.state_bytes,
                "recapture must not add wrappers twice"
            );
            source.update(&input(2)).unwrap();
            if tracked {
                assert_eq!(source.computed_state_bytes(), source.memory.state_bytes);
            }
            assert_eq!(source.flush(i64::MAX).unwrap().num_rows(), 3);
            let first = frozen.flush(i64::MAX).unwrap();
            let second = second.flush(i64::MAX).unwrap();
            assert_eq!(first, second);
            assert!(column_i64(&first, "result0")
                .values()
                .iter()
                .all(|count| *count == 1));
        }
    }
}

#[test]
fn legacy_hop_registers_all_windows_and_does_not_schedule_an_empty_tail() {
    use crate::state::canonical_memory::CanonicalMemoryState;
    let factory = || TumblingAggregator::new(6000, 2000, false, vec![0], vec![3]);
    let mut source = factory();
    source.legacy_slices = true;
    source.enable_canonical_slicing(-1).unwrap();
    let batch = RecordBatch::try_from_iter(vec![
        ("key0", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("value0", Arc::new(Int64Array::from(vec![None])) as ArrayRef),
        ("ts", Arc::new(Int64Array::from(vec![1000])) as ArrayRef),
    ])
    .unwrap();
    source.update(&batch).unwrap();
    assert_eq!(
        source.slice_timers.keys().copied().collect::<Vec<_>>(),
        vec![2000, 4000, 6000]
    );
    let mut captured = factory();
    source.copy_state(&mut captured).unwrap();
    assert!(captured.legacy_slices);
    for value in [&mut source, &mut captured] {
        let first = value.flush(2000).unwrap();
        assert_eq!(first.num_rows(), 1);
        assert_eq!(
            column_i64(&first, "result0").value(0),
            0,
            "NULL-only windows remain live without COUNT(*)"
        );
        assert_eq!(
            value.slice_timers.keys().copied().collect::<Vec<_>>(),
            vec![4000, 6000]
        );
        assert_eq!(value.flush(6000).unwrap().num_rows(), 2);
        assert!(
            value.slice_timers.is_empty(),
            "legacy timers must not schedule an orphan tail"
        );
        assert!(value.windows.is_empty());
    }
}
