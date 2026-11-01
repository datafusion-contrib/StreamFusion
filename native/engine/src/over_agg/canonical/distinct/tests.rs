use super::super::tests::{batch, rows_of};
use super::*;

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
        for mode in ["null", "type", "key", "duplicate", "orphan"] {
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
                "null" => {
                    fields[1] = Field::new("value", DataType::Float64, true);
                    columns[1] = Arc::new(arrow::array::Float64Array::from(vec![None]));
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
