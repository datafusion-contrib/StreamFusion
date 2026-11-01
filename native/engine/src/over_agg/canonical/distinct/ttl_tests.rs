use super::tests::{input, operator};
use super::*;

fn ttl_operator(
    keyless: bool,
    rows: bool,
    persistent: bool,
    path: &std::path::Path,
) -> OverWindowAggregator {
    operator(keyless, rows, true, persistent, path).with_state_retention(1000)
}

fn count(batch: &RecordBatch) -> i64 {
    batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

fn folds(operator: &OverWindowAggregator) -> usize {
    let mut rows = 0;
    operator
        .visit_canonical_folds(|batch| {
            rows += batch.num_rows();
            Ok(())
        })
        .unwrap();
    rows
}

fn transfer(source: &OverWindowAggregator, target: &mut OverWindowAggregator) {
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
}

#[test]
fn scalar_expiry_retains_distinct_memberships_and_refreshes_only_the_touched_key() {
    for persistent in [false, true] {
        for keyless in [false, true] {
            for rows in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let mut source = ttl_operator(keyless, rows, persistent, dir.path());
                source
                    .push_proctime(
                        input(
                            &[Some(1), Some(1), None],
                            &[Some(10.0), Some(20.0), Some(5.0)],
                            0,
                        ),
                        100,
                    )
                    .unwrap();
                source
                    .push_proctime(input(&[Some(1)], &[Some(10.0)], 0), 500)
                    .unwrap();
                let expired = source
                    .push_proctime(input(&[Some(1)], &[Some(10.0)], 0), 1600)
                    .unwrap();
                assert_eq!(count(&expired), 0);
                assert!(expired.column(3).is_null(0));
                assert!(expired.column(5).is_null(0));
                let repeated = source
                    .push_proctime(input(&[Some(1)], &[Some(20.0)], 0), 1700)
                    .unwrap();
                assert_eq!(count(&repeated), 0);
                let fresh = source
                    .push_proctime(input(&[Some(1)], &[Some(40.0)], 0), 1800)
                    .unwrap();
                assert_eq!(count(&fresh), 1);
                assert_eq!(
                    ScalarValue::try_from_array(fresh.column(3), 0).unwrap(),
                    ScalarValue::Float64(Some(40.0))
                );
                if !keyless {
                    assert!(source.last_write_ms.values().any(|&time| time == 100));
                }
            }
        }
    }
}

#[test]
fn raw_post_expiry_scalars_and_lifetime_memberships_transfer_without_replay() {
    for from_store in [false, true] {
        for to_store in [false, true] {
            for keyless in [false, true] {
                for rows in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let mut source =
                        ttl_operator(keyless, rows, from_store, &dir.path().join("source"));
                    let mut target =
                        ttl_operator(keyless, rows, to_store, &dir.path().join("target"));
                    source
                        .push_proctime(
                            input(&[Some(1), Some(1)], &[Some(10.0), Some(20.0)], 0),
                            100,
                        )
                        .unwrap();
                    assert_eq!(
                        count(
                            &source
                                .push_proctime(input(&[Some(1)], &[Some(10.0)], 0), 1200)
                                .unwrap()
                        ),
                        0
                    );
                    transfer(&source, &mut target);
                    assert_eq!(folds(&target), 1);
                    for value in [20.0, 40.0] {
                        let batch = input(&[Some(1)], &[Some(value)], 0);
                        let expected = source.push_proctime(batch.clone(), 1300).unwrap();
                        let actual = target.push_proctime(batch, 1300).unwrap();
                        assert_eq!(count(&actual), if value == 20.0 { 0 } else { 1 });
                        assert_eq!(
                            super::super::tests::rows_of(&[expected]),
                            super::super::tests::rows_of(&[actual])
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn membership_only_keys_survive_native_partition_checkpoints_between_stores() {
    for from_store in [false, true] {
        for to_store in [false, true] {
            for keyless in [false, true] {
                for rows in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let mut producer =
                        ttl_operator(keyless, rows, false, &dir.path().join("producer"));
                    producer
                        .push_proctime(input(&[Some(1)], &[Some(10.0)], 0), 100)
                        .unwrap();
                    let mut source =
                        ttl_operator(keyless, rows, from_store, &dir.path().join("source"));
                    for index in [0, 1, 2] {
                        producer
                            .visit_canonical_distinct(index, |batch| {
                                source.import_canonical_distinct(index, &batch)
                            })
                            .unwrap();
                    }
                    source.finish_canonical_distinct().unwrap();
                    assert_eq!(folds(&source), 0);
                    let descriptor = if keyless {
                        vec![i32::MIN]
                    } else {
                        vec![i32::MIN, -1]
                    };
                    let snapshots = if from_store {
                        source.canonical_partitions(128, &descriptor).unwrap()
                    } else {
                        source.snapshot_partitions(128, &descriptor)
                    };
                    assert!(!snapshots.is_empty());
                    let snapshots = snapshots.into_values().collect::<Vec<_>>();
                    let mut target = if to_store {
                        let mut target =
                            ttl_operator(keyless, rows, true, &dir.path().join("target"));
                        target.import_partitions(&snapshots).unwrap();
                        target.adopt_store_retention(500).unwrap();
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
                            1000,
                            500,
                        );
                        for index in [0, 1, 2] {
                            target.bind_canonical_distinct(index).unwrap();
                        }
                        target
                    };
                    assert_eq!(folds(&target), 0);
                    for index in [0, 1, 2] {
                        let mut n = 0;
                        target
                            .visit_canonical_distinct(index, |batch| {
                                n += batch.num_rows();
                                Ok(())
                            })
                            .unwrap();
                        assert_eq!(n, 1);
                    }
                    assert_eq!(
                        count(
                            &target
                                .push_proctime(input(&[Some(1)], &[Some(10.0)], 0), 600)
                                .unwrap()
                        ),
                        0
                    );
                    assert_eq!(
                        count(
                            &target
                                .push_proctime(input(&[Some(1)], &[Some(20.0)], 0), 700)
                                .unwrap()
                        ),
                        1
                    );
                }
            }
        }
    }
}

#[test]
fn legacy_nonnullable_clocks_merge_with_absent_folds_without_reviving_them() {
    for before_ttl in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut present = ttl_operator(false, true, false, &dir.path().join("present"))
            .with_state_retention(if before_ttl { 0 } else { 1000 });
        present
            .push_proctime(input(&[Some(1)], &[Some(10.0)], 0), 100)
            .unwrap();
        let bytes = present.snapshot_accumulators();
        let batch = read_ipc(&bytes).remove(0);
        let fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                if field.name() == TTL_TS_COLUMN {
                    Arc::new(field.as_ref().clone().with_nullable(false))
                } else {
                    field.clone()
                }
            })
            .collect::<Vec<_>>();
        let old =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap();
        let old = OverWindowAggregator::snapshot_parts(1, write_ipc(&old), vec![], vec![], vec![]);
        let mut producer = ttl_operator(false, true, false, &dir.path().join("producer"));
        producer
            .push_proctime(input(&[Some(2)], &[Some(10.0)], 0), 100)
            .unwrap();
        let mut absent = ttl_operator(false, true, false, &dir.path().join("absent"));
        for index in [0, 1, 2] {
            producer
                .visit_canonical_distinct(index, |batch| {
                    absent.import_canonical_distinct(index, &batch)
                })
                .unwrap();
        }
        absent.finish_canonical_distinct().unwrap();
        let mut restored = OverWindowAggregator::restore_partitions(
            vec![1; 5],
            vec![100, 103, 104, 1, 2],
            2,
            vec![1; 5],
            vec![0],
            3,
            0,
            true,
            &[old, absent.snapshot()],
            1000,
            500,
        );
        for index in [0, 1, 2] {
            restored.bind_canonical_distinct(index).unwrap();
        }
        assert_eq!(folds(&restored), 1);
        assert_eq!(
            restored.last_write_ms.values().copied().collect::<Vec<_>>(),
            vec![if before_ttl { 500 } else { 100 }]
        );
        for key in [1, 2] {
            assert_eq!(
                count(
                    &restored
                        .push_proctime(input(&[Some(key)], &[Some(10.0)], 0), 1700)
                        .unwrap()
                ),
                0
            );
        }
    }
}

#[test]
fn snapshots_before_ttl_was_enabled_adopt_the_restore_clock() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = operator(false, true, true, false, dir.path());
    source
        .push_proctime(input(&[Some(1)], &[Some(10.0)], 0), 100)
        .unwrap();
    let bytes = source.snapshot();
    let mut restored = OverWindowAggregator::restore_partitions(
        vec![1; 5],
        vec![100, 103, 104, 1, 2],
        2,
        vec![1; 5],
        vec![0],
        3,
        0,
        true,
        &[bytes],
        1000,
        500,
    );
    assert_eq!(
        count(
            &restored
                .push_proctime(input(&[Some(1)], &[Some(10.0)], 0), 600)
                .unwrap()
        ),
        1
    );
    assert_eq!(
        count(
            &restored
                .push_proctime(input(&[Some(1)], &[Some(10.0)], 0), 1700)
                .unwrap()
        ),
        0
    );
}

#[test]
fn an_expired_persistent_fold_cannot_rehydrate_between_periodic_sweeps() {
    for persistent in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut source = super::super::tests::operator(false, true, 1000, persistent, dir.path());
        for (key, time) in [(1, 100), (2, 900), (3, 1000)] {
            source
                .push_proctime(input(&[Some(key)], &[Some(10.0)], 0), time)
                .unwrap();
        }
        let output = source
            .push_proctime(input(&[Some(1)], &[Some(30.0)], 0), 1100)
            .unwrap();
        assert_eq!(count(&output), 1);
        assert_eq!(
            ScalarValue::try_from_array(output.column(3), 0).unwrap(),
            ScalarValue::Float64(Some(30.0))
        );
    }
}
