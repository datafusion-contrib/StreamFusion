use super::super::tests::{batch, rows_of};
use super::*;

#[cfg(feature = "rocksdb-state")]
#[test]
fn floating_count_memberships_preserve_boxed_zero_and_nan_equality_between_stores() {
    for (data_type, code) in [(DataType::Float32, 6), (DataType::Float64, 1)] {
        let input = |suffix| {
            let values: ArrayRef = if data_type == DataType::Float32 {
                Arc::new(Float32Array::from(if suffix {
                    vec![
                        Some(f32::from_bits(0xffc00022)),
                        Some(-0.0),
                        Some(0.0),
                        Some(1.0),
                        Some(-0.0),
                        Some(f32::NAN),
                    ]
                } else {
                    vec![
                        Some(-0.0),
                        Some(0.0),
                        Some(f32::from_bits(0x7fc00011)),
                        None,
                        None,
                        Some(f32::INFINITY),
                    ]
                }))
            } else {
                Arc::new(Float64Array::from(if suffix {
                    vec![
                        Some(f64::from_bits(0xfff8000000000022)),
                        Some(-0.0),
                        Some(0.0),
                        Some(1.0),
                        Some(-0.0),
                        Some(f64::NAN),
                    ]
                } else {
                    vec![
                        Some(-0.0),
                        Some(0.0),
                        Some(f64::from_bits(0x7ff8000000000011)),
                        None,
                        None,
                        Some(f64::INFINITY),
                    ]
                }))
            };
            RecordBatch::try_from_iter(vec![
                (
                    "k",
                    Arc::new(Int64Array::from(vec![
                        Some(1),
                        Some(1),
                        Some(1),
                        Some(1),
                        Some(2),
                        None,
                    ])) as ArrayRef,
                ),
                ("v", values),
                ("rt", Arc::new(Int64Array::from(vec![0; 6])) as ArrayRef),
            ])
            .unwrap()
        };
        let create = |persistent, path: &std::path::Path| {
            let mut owner =
                OverWindowAggregator::new(vec![code], vec![103], 2, vec![1], vec![0], 3, 0, true)
                    .with_key_timestamp_precisions(vec![i32::MIN, -1]);
            if persistent {
                let store = crate::state::RocksOverAggStore::create(
                    crate::state::RocksStoreConfig {
                        table_dir: path.to_str().unwrap().into(),
                        max_parallelism: 128,
                        options_json: include_str!(
                            "../../../../benches/fixtures/rocks-options.json"
                        )
                        .into(),
                        ttl_ms: 0,
                        shared_resources: 0,
                    },
                    &owner.inner.store_state_types(),
                    &[],
                    &[data_type.clone()],
                    input(false).schema(),
                    0..=127,
                )
                .unwrap();
                owner = owner.with_store(store, vec![DataType::Int64]);
            }
            owner.bind_canonical_distinct(0).unwrap();
            owner
        };
        let counts = |batch: &RecordBatch| {
            batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        };
        for from_store in [false, true] {
            for to_store in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let mut source = create(from_store, &directory.path().join("source"));
                let mut target = create(to_store, &directory.path().join("target"));
                assert_eq!(
                    counts(&source.push_proctime(input(false), 100).unwrap()),
                    vec![1, 2, 3, 3, 0, 1]
                );
                source
                    .visit_canonical_folds(|batch| target.import_canonical_folds(&batch))
                    .unwrap();
                source
                    .visit_canonical_distinct(0, |batch| {
                        target.import_canonical_distinct(0, &batch)
                    })
                    .unwrap();
                target.finish_canonical_distinct().unwrap();
                let expected = source.push_proctime(input(true), 500).unwrap();
                let actual = target.push_proctime(input(true), 500).unwrap();
                assert_eq!(expected, actual);
                assert_eq!(counts(&actual), vec![3, 3, 3, 4, 1, 2]);
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn nullable_text_memberships_keep_utf8_equality_across_store_handoffs() {
    let input = |values: Vec<Option<&str>>| {
        RecordBatch::try_from_iter(vec![
            (
                "k",
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), None])) as ArrayRef,
            ),
            ("v", Arc::new(StringArray::from(values)) as ArrayRef),
            ("rt", Arc::new(Int64Array::from(vec![0; 4])) as ArrayRef),
        ])
        .unwrap()
    };
    let prefix = input(vec![Some("λ-雪"), None, None, Some("😀")]);
    let create = |persistent, path: &std::path::Path| {
        let mut owner =
            OverWindowAggregator::new(vec![3], vec![103], 2, vec![1], vec![0], 3, 0, true)
                .with_key_timestamp_precisions(vec![i32::MIN, -1]);
        if persistent {
            let store = crate::state::RocksOverAggStore::create(
                crate::state::RocksStoreConfig {
                    table_dir: path.to_str().unwrap().into(),
                    max_parallelism: 128,
                    options_json: include_str!("../../../../benches/fixtures/rocks-options.json")
                        .into(),
                    ttl_ms: 0,
                    shared_resources: 0,
                },
                &owner.inner.store_state_types(),
                &[],
                &[DataType::Utf8],
                prefix.schema(),
                0..=127,
            )
            .unwrap();
            owner = owner.with_store(store, vec![DataType::Int64]);
        }
        owner.bind_canonical_distinct(0).unwrap();
        owner
    };
    for from_store in [false, true] {
        for to_store in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut source = create(from_store, &directory.path().join("source"));
            let mut target = create(to_store, &directory.path().join("target"));
            source.push_proctime(prefix.clone(), 100).unwrap();
            source
                .visit_canonical_folds(|batch| target.import_canonical_folds(&batch))
                .unwrap();
            source
                .visit_canonical_distinct(0, |batch| target.import_canonical_distinct(0, &batch))
                .unwrap();
            target.finish_canonical_distinct().unwrap();
            for (values, counts) in [
                (
                    vec![Some("λ-雪"), Some(""), None, Some("😀")],
                    vec![1, 2, 0, 1],
                ),
                (
                    vec![Some(""), Some("λ-雪"), Some("λ-雪"), Some("雪")],
                    vec![2, 2, 1, 2],
                ),
            ] {
                let expected = source.push_proctime(input(values.clone()), 500).unwrap();
                let actual = target.push_proctime(input(values), 500).unwrap();
                assert_eq!(expected, actual);
                assert_eq!(
                    actual
                        .column(3)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .as_ref(),
                    counts.as_slice()
                );
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn represented_row_partitions_share_nullable_memberships_across_live_and_restored_transports() {
    let descriptor = [i32::MIN, -1, -1, -1];
    let input = |binary, prefix| {
        let input = crate::flink_key::represented_row_key_fixture(2051, binary, 0);
        let mut columns = input.columns().clone();
        columns[2] = Arc::new(Int64Array::from_iter(
            (0..2051).map(|key| (!prefix || key % 2 != 0).then_some(key as i64)),
        ));
        let mut fields = input
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields[2] = Field::new("v", DataType::Int64, true);
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    };
    let create = |persistent, path: &std::path::Path| {
        let schema =
            crate::flink_key::canonical_stock_row_key_batch(input(false, false), &[0], &descriptor)
                .unwrap()
                .schema();
        let mut owner =
            OverWindowAggregator::new(vec![0], vec![103], 1, vec![2], vec![0], 3, 0, true)
                .with_key_timestamp_precisions(descriptor.to_vec())
                .with_memory_budget(64 << 20)
                .unwrap();
        if persistent {
            let store = crate::state::RocksOverAggStore::create(
                crate::state::RocksStoreConfig {
                    table_dir: path.to_str().unwrap().into(),
                    max_parallelism: 128,
                    options_json: include_str!("../../../../benches/fixtures/rocks-options.json")
                        .into(),
                    ttl_ms: 0,
                    shared_resources: 0,
                },
                &owner.inner.store_state_types(),
                &[],
                &[DataType::Int64],
                schema.clone(),
                0..=127,
            )
            .unwrap();
            owner = owner.with_store(store, vec![schema.field(0).data_type().clone()]);
        }
        owner.bind_canonical_distinct(0).unwrap();
        owner
    };
    for from_store in [false, true] {
        for to_store in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut source = create(from_store, &directory.path().join("source"));
            let mut target = create(to_store, &directory.path().join("target"));
            source.push_proctime(input(false, true), 100).unwrap();
            source
                .visit_canonical_folds(|batch| target.import_canonical_folds(&batch))
                .unwrap();
            source
                .visit_canonical_distinct(0, |batch| target.import_canonical_distinct(0, &batch))
                .unwrap();
            target.finish_canonical_distinct().unwrap();
            for binary in [true, false] {
                let expected = source.push_proctime(input(binary, false), 500).unwrap();
                let actual = target.push_proctime(input(!binary, false), 500).unwrap();
                assert_eq!(expected, actual);
                let counts = actual
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert!(counts.values().iter().all(|&count| count == 1));
            }
        }
    }
}

#[test]
fn null_history_is_recorded_only_for_bound_processing_folds_without_ttl() {
    for bound in [false, true] {
        for ttl in [0, 1000] {
            let mut owner =
                OverWindowAggregator::new(vec![1], vec![103], 2, vec![1], vec![0], 3, 0, true)
                    .with_state_retention(ttl);
            if bound {
                owner.bind_canonical_distinct(0).unwrap();
            }
            owner
                .push_proctime(input(&[Some(1)], &[None], 0), 100)
                .unwrap();
            let OverInner::Aggregates(inner) = &owner.inner else {
                unreachable!()
            };
            assert_eq!(
                inner.keys.values().next().unwrap()[0].distinct_null,
                bound && ttl == 0
            );
        }
    }
}

#[test]
fn null_membership_outside_the_processing_domain_rejects_before_installation() {
    for (proctime, ttl) in [(false, 0), (true, 1000)] {
        let create = |processing, retention| {
            let mut owner = OverWindowAggregator::new(
                vec![1],
                vec![103],
                2,
                vec![1],
                vec![0],
                3,
                0,
                processing,
            )
            .with_state_retention(retention);
            owner.bind_canonical_distinct(0).unwrap();
            owner
        };
        let mut source = create(true, 0);
        let mut target = create(proctime, ttl);
        source
            .push_proctime(input(&[Some(1)], &[Some(7.0)], 0), 100)
            .unwrap();
        source
            .visit_canonical_folds(|batch| target.import_canonical_folds(&batch))
            .unwrap();
        let valid = members(&source, 0).pop().unwrap();
        let mut columns = valid.columns().clone();
        columns[1] = new_null_array(&DataType::Float64, 1);
        let invalid = RecordBatch::try_new(valid.schema(), columns).unwrap();
        assert!(target.import_canonical_distinct(0, &invalid).is_err());
        let OverInner::Aggregates(inner) = &target.inner else {
            unreachable!()
        };
        assert!(inner
            .keys
            .values()
            .all(|states| !states[0].distinct_null
                && states[0].distinct.as_ref().unwrap().len() == 0));
        target.import_canonical_distinct(0, &valid).unwrap();
        target.finish_canonical_distinct().unwrap();
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn null_membership_markers_preserve_empty_folds_across_canonical_and_native_recovery() {
    for keyless in [false, true] {
        for rows in [false, true] {
            for from_store in [false, true] {
                for to_store in [false, true] {
                    let directory = tempfile::tempdir().unwrap();
                    let descriptor = if keyless {
                        vec![i32::MIN]
                    } else {
                        vec![i32::MIN, -1]
                    };
                    let mut source = operator(
                        keyless,
                        rows,
                        true,
                        from_store,
                        &directory.path().join("source"),
                    )
                    .with_key_timestamp_precisions(descriptor.clone());
                    let mut canonical = operator(
                        keyless,
                        rows,
                        true,
                        to_store,
                        &directory.path().join("canonical"),
                    )
                    .with_key_timestamp_precisions(descriptor.clone());
                    push(
                        &mut source,
                        input(
                            &[Some(1), Some(1), None, Some(2), Some(2)],
                            &[None, Some(7.0), None, None, None],
                            0,
                        ),
                        100,
                        0,
                    );
                    source
                        .visit_canonical_folds(|batch| canonical.import_canonical_folds(&batch))
                        .unwrap();
                    for index in [0, 1, 2] {
                        let frames = members(&source, index);
                        assert!(frames
                            .iter()
                            .any(|batch| batch.column(batch.num_columns() - 1).null_count() != 0));
                        source
                            .visit_canonical_distinct(index, |batch| {
                                canonical.import_canonical_distinct(index, &batch)
                            })
                            .unwrap();
                    }
                    canonical.finish_canonical_distinct().unwrap();
                    let snapshots = if from_store {
                        source.canonical_partitions(128, &descriptor).unwrap()
                    } else {
                        source.snapshot_partitions(128, &descriptor)
                    }
                    .into_values()
                    .collect::<Vec<_>>();
                    let mut recovered = if to_store {
                        let mut target = operator(
                            keyless,
                            rows,
                            true,
                            true,
                            &directory.path().join("recovered"),
                        )
                        .with_key_timestamp_precisions(descriptor.clone());
                        target.import_partitions(&snapshots).unwrap();
                        target
                    } else {
                        let mut target = OverWindowAggregator::restore_partitions(
                            vec![1; 5],
                            vec![100, 103, 104, 1, 2],
                            2,
                            vec![1; 5],
                            if keyless { vec![] } else { vec![0] },
                            if rows { 3 } else { 0 },
                            0,
                            true,
                            &snapshots,
                            0,
                            500,
                        )
                        .with_key_timestamp_precisions(descriptor.clone());
                        for index in [0, 1, 2] {
                            target.bind_canonical_distinct(index).unwrap();
                        }
                        target
                    };
                    for index in [0, 1, 2] {
                        assert_eq!(
                            rows_of(&members(&source, index)),
                            rows_of(&members(&canonical, index))
                        );
                        assert_eq!(
                            rows_of(&members(&source, index)),
                            rows_of(&members(&recovered, index))
                        );
                    }
                    let future = input(
                        &[Some(1), Some(1), Some(2), Some(2), None, None],
                        &[None, Some(7.0), None, Some(3.0), None, Some(4.0)],
                        0,
                    );
                    let expected = rows_of(&[push(&mut source, future.clone(), 500, 0)]);
                    assert_eq!(
                        expected,
                        rows_of(&[push(&mut canonical, future.clone(), 500, 0)])
                    );
                    assert_eq!(expected, rows_of(&[push(&mut recovered, future, 500, 0)]));
                }
            }
        }
    }
}

pub(super) fn input(keys: &[Option<i64>], values: &[Option<f64>], time: i64) -> RecordBatch {
    let old = batch(keys, values);
    RecordBatch::try_new(
        old.schema(),
        vec![
            old.column(0).clone(),
            old.column(1).clone(),
            Arc::new(Int64Array::from(vec![time; keys.len()])),
        ],
    )
    .unwrap()
}

#[cfg(feature = "rocksdb-state")]
pub(super) fn operator(
    keyless: bool,
    rows: bool,
    proctime: bool,
    persistent: bool,
    path: &std::path::Path,
) -> OverWindowAggregator {
    let mut operator = OverWindowAggregator::new(
        vec![1; 5],
        vec![100, 103, 104, 1, 2],
        2,
        vec![1; 5],
        if keyless { vec![] } else { vec![0] },
        if rows { 3 } else { 0 },
        0,
        proctime,
    )
    .with_state_retention(if proctime { 0 } else { 1000 })
    .with_memory_budget(64 << 20)
    .unwrap();
    if persistent {
        let store = crate::state::RocksOverAggStore::create(
            crate::state::RocksStoreConfig {
                table_dir: path.to_str().unwrap().into(),
                max_parallelism: 128,
                options_json: include_str!("../../../../benches/fixtures/rocks-options.json")
                    .into(),
                ttl_ms: 0,
                shared_resources: 0,
            },
            &operator.inner.store_state_types(),
            &[],
            &vec![DataType::Float64; 3],
            batch(&[], &[]).schema(),
            0..=127,
        )
        .unwrap();
        operator = operator
            .with_key_timestamp_precisions(if keyless {
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
            );
    }
    for index in [0, 1, 2] {
        operator.bind_canonical_distinct(index).unwrap();
    }
    operator
}

fn push(
    operator: &mut OverWindowAggregator,
    batch: RecordBatch,
    now: i64,
    time: i64,
) -> RecordBatch {
    if operator.proctime {
        operator.push_proctime(batch, now).unwrap()
    } else {
        operator.push(batch, now).unwrap();
        operator.flush(time, now).unwrap()
    }
}

fn members(operator: &OverWindowAggregator, index: usize) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    operator
        .visit_canonical_distinct(index, |batch| {
            batches.push(batch);
            Ok(())
        })
        .unwrap();
    batches
}

#[test]
fn zero_initialized_sum_cannot_lose_membership_but_an_empty_fold_is_valid() {
    let create = || {
        let mut operator =
            OverWindowAggregator::new(vec![1], vec![122], 2, vec![1], vec![0], 3, 0, true);
        operator.bind_canonical_distinct(0).unwrap();
        operator
    };
    for value in [None, Some(2.0)] {
        let mut source = create();
        let mut target = create();
        source
            .push_proctime(input(&[Some(1)], &[value], 0), 100)
            .unwrap();
        source
            .visit_canonical_folds(|batch| target.import_canonical_folds(&batch))
            .unwrap();
        if value.is_some() {
            assert!(target
                .finish_canonical_distinct()
                .unwrap_err()
                .to_string()
                .contains("disagrees with membership"));
            source
                .visit_canonical_distinct(0, |batch| target.import_canonical_distinct(0, &batch))
                .unwrap();
        }
        target.finish_canonical_distinct().unwrap();
        assert_eq!(
            rows_of(&[source
                .push_proctime(input(&[Some(1)], &[Some(2.0)], 0), 200)
                .unwrap()]),
            rows_of(&[target
                .push_proctime(input(&[Some(1)], &[Some(2.0)], 0), 200)
                .unwrap()]),
        );
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn raw_distinct_sums_counts_and_membership_continue_between_both_stores() {
    for keyless in [false, true] {
        for rows in [false, true] {
            for proctime in [false, true] {
                for from_store in [false, true] {
                    for to_store in [false, true] {
                        let directory = tempfile::tempdir().unwrap();
                        let mut source = operator(
                            keyless,
                            rows,
                            proctime,
                            from_store,
                            &directory.path().join("source"),
                        );
                        let mut target = operator(
                            keyless,
                            rows,
                            proctime,
                            to_store,
                            &directory.path().join("target"),
                        );
                        push(
                            &mut source,
                            input(
                                &[Some(1), Some(1), Some(1), Some(1), None, None, Some(2)],
                                &[
                                    Some(1e16),
                                    Some(1.0),
                                    Some(-1e16),
                                    Some(1e16),
                                    Some(3.0),
                                    Some(3.0),
                                    None,
                                ],
                                0,
                            ),
                            100,
                            0,
                        );
                        source
                            .visit_canonical_folds(|batch| target.import_canonical_folds(&batch))
                            .unwrap();
                        assert!(target.finish_canonical_distinct().is_err());
                        assert!(target.visit_canonical_folds(|_| Ok(())).is_err());
                        assert!(target
                            .push_proctime(input(&[Some(1)], &[Some(99.0)], 100), 200)
                            .is_err());
                        for index in [0, 1, 2] {
                            source
                                .visit_canonical_distinct(index, |batch| {
                                    target.import_canonical_distinct(index, &batch)
                                })
                                .unwrap();
                        }
                        target.finish_canonical_distinct().unwrap();
                        for index in [0, 1, 2] {
                            assert_eq!(
                                rows_of(&members(&source, index)),
                                rows_of(&members(&target, index))
                            );
                        }
                        let suffix = input(
                            &[Some(1), Some(1), Some(1), Some(1), None, Some(2)],
                            &[
                                Some(1e16),
                                Some(1.0),
                                Some(-1e16),
                                Some(4.0),
                                Some(3.0),
                                Some(6.0),
                            ],
                            100,
                        );
                        assert_eq!(
                            rows_of(&[push(&mut source, suffix.clone(), 500, 100)]),
                            rows_of(&[push(&mut target, suffix, 500, 100)])
                        );
                    }
                }
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn large_membership_visitors_are_bounded_read_only_and_retryable() {
    for persistent in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut source = operator(false, true, true, persistent, directory.path());
        let values = (0..2500)
            .map(|value| Some(value as f64))
            .collect::<Vec<_>>();
        push(
            &mut source,
            input(&vec![Some(1); values.len()], &values, 0),
            100,
            0,
        );
        let mut calls = 0;
        assert!(source
            .visit_canonical_distinct(1, |batch| {
                assert!(batch.num_rows() <= CHUNK_ROWS);
                calls += 1;
                if calls == 2 {
                    return Err(invalid("receiver stopped"));
                }
                Ok(())
            })
            .is_err());
        assert_eq!(
            members(&source, 1)
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![1024, 1024, 452]
        );
        assert_eq!(
            members(&source, 0)
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2500
        );
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn malformed_membership_rejects_before_installation_and_incomplete_views_cannot_run() {
    for persistent in [false, true] {
        for mode in ["null-duplicate", "type", "key", "duplicate", "orphan"] {
            let directory = tempfile::tempdir().unwrap();
            let mut source = operator(false, true, true, false, &directory.path().join("source"));
            let mut target = operator(
                false,
                true,
                true,
                persistent,
                &directory.path().join("target"),
            );
            push(&mut source, input(&[Some(1)], &[Some(2.0)], 0), 100, 0);
            source
                .visit_canonical_folds(|batch| target.import_canonical_folds(&batch))
                .unwrap();
            let valid = members(&source, 1).pop().unwrap();
            let mut fields = valid
                .schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            let mut columns = valid.columns().to_vec();
            match mode {
                "null-duplicate" => {
                    fields[1] = Field::new("value", DataType::Float64, true);
                    columns[0] = Arc::new(Int64Array::from(vec![1, 1]));
                    columns[1] = Arc::new(arrow::array::Float64Array::from(vec![None, None]));
                }
                "type" => {
                    fields[1] = Field::new("value", DataType::Int64, false);
                    columns[1] = Arc::new(Int64Array::from(vec![2]));
                }
                "key" => {
                    fields[0] = Field::new("key0", DataType::Float64, false);
                    columns[0] = Arc::new(arrow::array::Float64Array::from(vec![1.0]));
                }
                "duplicate" => {
                    columns[0] = Arc::new(Int64Array::from(vec![1, 1]));
                    columns[1] = Arc::new(arrow::array::Float64Array::from(vec![2.0, 2.0]));
                }
                "orphan" => columns[0] = Arc::new(Int64Array::from(vec![9])),
                _ => unreachable!(),
            }
            let malformed = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            assert!(
                target.import_canonical_distinct(1, &malformed).is_err(),
                "{mode}"
            );
            for index in [0, 1, 2] {
                source
                    .visit_canonical_distinct(index, |batch| {
                        target.import_canonical_distinct(index, &batch)
                    })
                    .unwrap();
            }
            target.finish_canonical_distinct().unwrap();
            assert_eq!(rows_of(&members(&source, 1)), rows_of(&members(&target, 1)));
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn floating_membership_uses_java_nan_and_signed_zero_identity_between_stores() {
    for from_store in [false, true] {
        for to_store in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut source = operator(
                false,
                true,
                true,
                from_store,
                &directory.path().join("source"),
            );
            let mut target = operator(
                false,
                true,
                true,
                to_store,
                &directory.path().join("target"),
            );
            let values = [
                Some(f64::NAN),
                Some(f64::from_bits(0x7ff8000000000001)),
                Some(-0.0),
                Some(0.0),
                None,
            ];
            push(
                &mut source,
                input(&vec![Some(1); values.len()], &values, 0),
                100,
                0,
            );
            source.finish_canonical_distinct().unwrap();
            assert_eq!(
                members(&source, 1)
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                3
            );
            source
                .visit_canonical_folds(|batch| target.import_canonical_folds(&batch))
                .unwrap();
            for index in [0, 1, 2] {
                source
                    .visit_canonical_distinct(index, |batch| {
                        target.import_canonical_distinct(index, &batch)
                    })
                    .unwrap();
            }
            target.finish_canonical_distinct().unwrap();
            let suffix = input(
                &[Some(1); 4],
                &[
                    Some(f64::from_bits(0x7ff8000000000002)),
                    Some(-0.0),
                    Some(0.0),
                    Some(1.0),
                ],
                100,
            );
            assert_eq!(
                rows_of(&[push(&mut source, suffix.clone(), 200, 100)]),
                rows_of(&[push(&mut target, suffix, 200, 100)])
            );
            source.finish_canonical_distinct().unwrap();
            target.finish_canonical_distinct().unwrap();
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn retained_membership_duplicates_are_rejected_without_changing_valid_views() {
    for persistent in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut source = operator(false, true, true, false, &directory.path().join("source"));
        let mut target = operator(
            false,
            true,
            true,
            persistent,
            &directory.path().join("target"),
        );
        push(&mut source, input(&[Some(1)], &[Some(2.0)], 0), 100, 0);
        source
            .visit_canonical_folds(|batch| target.import_canonical_folds(&batch))
            .unwrap();
        let data = members(&source, 1).pop().unwrap();
        target.import_canonical_distinct(1, &data).unwrap();
        assert!(target.import_canonical_distinct(1, &data).is_err());
        for index in [0, 2] {
            source
                .visit_canonical_distinct(index, |batch| {
                    target.import_canonical_distinct(index, &batch)
                })
                .unwrap();
        }
        target.finish_canonical_distinct().unwrap();
        assert_eq!(rows_of(&members(&source, 1)), rows_of(&members(&target, 1)));
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn polling_timers_and_multiple_distinct_views_remain_independent_during_capture() {
    for rows in [false, true] {
        for global in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("state");
            let mut source = operator(global, rows, false, true, &path);
            source.bind_event_cleanup_timers().unwrap();
            source.bind_event_polling().unwrap();
            source
                .push(input(&[Some(7)], &[Some(2.0)], 10), 100)
                .unwrap();
            source
                .push(input(&[Some(7)], &[Some(3.0)], 20), 100)
                .unwrap();
            source.flush(10, 200).unwrap();
            let expected = (0..3)
                .map(|slot| rows_of(&members(&source, slot)))
                .collect::<Vec<_>>();
            assert!(expected.iter().all(|rows| rows.len() == 1));
            source
                .canonical_partitions(128, if global { &[i32::MIN] } else { &[i32::MIN, -1] })
                .unwrap();
            source.checkpoint_store("").unwrap();
            let snapshot = crate::state::rocks_store::FrozenDatabases::capture(&path).unwrap();
            source.flush(20, 900).unwrap();
            snapshot.with(|| {
                let mut reader = operator(global, rows, false, true, &path);
                reader.bind_event_cleanup_timers().unwrap();
                reader.bind_event_polling().unwrap();
                for (slot, expected) in expected.iter().enumerate() {
                    assert_eq!(&rows_of(&members(&reader, slot)), expected);
                }
            });
        }
    }
}
