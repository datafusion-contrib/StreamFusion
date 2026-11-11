use super::*;
use arrow::array::Float64Array;

pub(super) fn batch(keys: &[Option<i64>], values: &[Option<f64>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("value", DataType::Float64, true),
            Field::new("time", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys.to_vec())),
            Arc::new(Float64Array::from(values.to_vec())),
            Arc::new(Int64Array::from(vec![0; keys.len()])),
        ],
    )
    .unwrap()
}

pub(super) fn memory(keyless: bool, rows: bool, ttl: i64) -> OverWindowAggregator {
    OverWindowAggregator::new(
        vec![1, 1, 1, 1, 1],
        vec![0, 3, 4, 1, 2],
        2,
        vec![1; 5],
        if keyless { vec![] } else { vec![0] },
        if rows { 3 } else { 0 },
        0,
        true,
    )
    .with_state_retention(ttl)
    .with_memory_budget(64 << 20)
    .unwrap()
}

#[cfg(feature = "rocksdb-state")]
pub(super) fn operator(
    keyless: bool,
    rows: bool,
    ttl: i64,
    persistent: bool,
    path: &std::path::Path,
) -> OverWindowAggregator {
    let mut operator = memory(keyless, rows, ttl);
    if persistent {
        let schema = batch(&[], &[]).schema();
        let types = operator.inner.store_state_types();
        let store = crate::state::RocksOverAggStore::create(
            crate::state::RocksStoreConfig {
                table_dir: path.to_str().unwrap().into(),
                max_parallelism: 128,
                options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 0,
                shared_resources: 0,
            },
            &types,
            &[],
            &[],
            schema,
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
    operator
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn raw_folds_continue_between_stores_without_replaying_float_sums_or_refreshing_ttl() {
    for keyless in [false, true] {
        for rows in [false, true] {
            for ttl in [0, 1000] {
                for from_store in [false, true] {
                    for to_store in [false, true] {
                        let directory = tempfile::tempdir().unwrap();
                        let mut source = operator(
                            keyless,
                            rows,
                            ttl,
                            from_store,
                            &directory.path().join("source"),
                        );
                        let mut target = operator(
                            keyless,
                            rows,
                            ttl,
                            to_store,
                            &directory.path().join("target"),
                        );
                        source
                            .push_proctime(
                                batch(
                                    &[Some(1), Some(1), Some(1), None, Some(2)],
                                    &[Some(1e16), Some(1.0), Some(-1e16), None, Some(10.0)],
                                ),
                                100,
                            )
                            .unwrap();
                        source
                            .visit_canonical_folds(|batch| target.import_canonical_folds(&batch))
                            .unwrap();
                        // No input before re-export, including state that is already expired.
                        let mut a = Vec::new();
                        source
                            .visit_canonical_folds(|batch| {
                                a.push(batch);
                                Ok(())
                            })
                            .unwrap();
                        let mut b = Vec::new();
                        target
                            .visit_canonical_folds(|batch| {
                                b.push(batch);
                                Ok(())
                            })
                            .unwrap();
                        assert_eq!(rows_of(&a), rows_of(&b));
                        for now in [500, 1501] {
                            let suffix =
                                batch(&[Some(1), None, Some(2)], &[Some(1.0), Some(2.0), None]);
                            assert_eq!(
                                source.push_proctime(suffix.clone(), now).unwrap(),
                                target.push_proctime(suffix, now).unwrap()
                            );
                        }
                    }
                }
            }
        }
    }
}

pub(super) fn rows_of(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        format!("{:?}", ScalarValue::try_from_array(column, row).unwrap())
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn bounded_visitors_leave_all_folds_available_after_receiver_failure() {
    for persistent in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut source = operator(false, true, 1000, persistent, directory.path());
        let keys = (0..2500).map(Some).collect::<Vec<_>>();
        let values = vec![Some(1.0); keys.len()];
        source.push_proctime(batch(&keys, &values), 100).unwrap();
        let mut count = 0;
        assert!(source
            .visit_canonical_folds(|batch| {
                count += batch.num_rows();
                assert!(batch.num_rows() <= CHUNK_ROWS);
                if count > CHUNK_ROWS {
                    return Err(invalid("receiver stopped"));
                };
                Ok(())
            })
            .is_err());
        let mut counts = Vec::new();
        source
            .visit_canonical_folds(|batch| {
                counts.push(batch.num_rows());
                Ok(())
            })
            .unwrap();
        assert_eq!(counts, vec![1024, 1024, 452]);
    }
}

#[test]
fn malformed_raw_accumulators_and_duplicate_partitions_fail_before_installation() {
    let mut source = memory(false, true, 1000);
    source
        .push_proctime(batch(&[Some(1)], &[Some(2.0)]), 100)
        .unwrap();
    let mut snapshots = Vec::new();
    source
        .visit_canonical_folds(|batch| {
            snapshots.push(batch);
            Ok(())
        })
        .unwrap();
    let valid = &snapshots[0];
    for mode in [
        "arity",
        "type",
        "null_sum",
        "null_count",
        "stamp",
        "duplicate",
    ] {
        let mut fields = valid
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        let mut columns = valid.columns().to_vec();
        match mode {
            "arity" => {
                fields.pop();
                columns.pop();
            }
            "type" => {
                fields[1] = Field::new("state0", DataType::Int64, true);
                columns[1] = Arc::new(Int64Array::from(vec![Some(2)]));
            }
            "null_sum" => {
                columns[3] = Arc::new(Float64Array::from(vec![None]));
            }
            "null_count" => {
                columns[4] = Arc::new(Int64Array::from(vec![None]));
            }
            "stamp" => {
                columns[valid.num_columns() - 1] = Arc::new(Int64Array::from(vec![None]));
                fields.last_mut().unwrap().set_nullable(true);
            }
            "duplicate" => {}
            _ => unreachable!(),
        }
        let malformed = if mode == "duplicate" {
            concat_batches(&valid.schema(), &[valid.clone(), valid.clone()]).unwrap()
        } else {
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
        };
        let mut target = memory(false, true, 1000);
        assert!(
            target.validate_canonical_folds(&malformed).is_err(),
            "{mode}"
        );
        assert!(target.import_canonical_folds(&malformed).is_err(), "{mode}");
        let mut rows = 0;
        target
            .visit_canonical_folds(|batch| {
                rows += batch.num_rows();
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, 0, "{mode}");
    }
    let mut target = memory(false, true, 1000);
    target.import_canonical_folds(valid).unwrap();
    assert!(target.import_canonical_folds(valid).is_err());
}

#[test]
fn fold_interface_rejects_distinct_and_frame_state_instead_of_dropping_it() {
    for (kind, frame) in [(100, 0), (0, 1), (0, 2), (10, 0)] {
        let source =
            OverWindowAggregator::new(vec![0], vec![kind], 2, vec![1], vec![0], frame, 10, true);
        assert!(source.visit_canonical_folds(|_| Ok(())).is_err());
    }
}

#[test]
fn imported_count_continues_with_the_hosts_wrapping_long_arithmetic() {
    let mut source = memory(false, true, 0);
    source
        .push_proctime(batch(&[Some(1)], &[Some(1.0)]), 0)
        .unwrap();
    let mut exported = Vec::new();
    source
        .visit_canonical_folds(|batch| {
            exported.push(batch);
            Ok(())
        })
        .unwrap();
    let original = &exported[0];
    let mut columns = original.columns().to_vec();
    columns[2] = Arc::new(Int64Array::from(vec![i64::MAX]));
    let retained = RecordBatch::try_new(original.schema(), columns).unwrap();
    let mut restored = memory(false, true, 0);
    restored.import_canonical_folds(&retained).unwrap();
    let output = restored
        .push_proctime(batch(&[Some(1)], &[Some(1.0)]), 0)
        .unwrap();
    assert_eq!(
        output
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MIN
    );
}

#[test]
fn whole_source_fold_validation_does_not_install_keys_types_counters_or_ttl_ages() {
    for keyless in [false, true] {
        for rows in [false, true] {
            for ttl in [0, 1000] {
                let mut source = memory(keyless, rows, ttl);
                let keys = (0..2050).map(Some).collect::<Vec<_>>();
                source
                    .push_proctime(batch(&keys, &vec![Some(2.0); keys.len()]), 100)
                    .unwrap();
                let mut target = memory(keyless, rows, ttl);
                let before = target.memory.state_bytes;
                let mut frames = Vec::new();
                source
                    .visit_canonical_folds(|frame| {
                        frames.push(frame);
                        Ok(())
                    })
                    .unwrap();
                for frame in &frames {
                    target.validate_canonical_folds(frame).unwrap();
                    assert_eq!(target.memory.state_bytes, before);
                    assert!(target.last_write_ms.is_empty());
                    assert!(target.cleanup_state.is_empty());
                    assert!(target.canonical_fold().unwrap().key_types.is_empty());
                    assert!(target.canonical_fold().unwrap().keys.is_empty());
                }
                for frame in &frames {
                    target.import_canonical_folds(frame).unwrap();
                }
                let mut restored = Vec::new();
                target
                    .visit_canonical_folds(|frame| {
                        restored.push(frame);
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(rows_of(&frames), rows_of(&restored));
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn persistent_fold_validation_leaves_store_and_retention_state_empty_until_apply() {
    for keyless in [false, true] {
        for ttl in [0, 1000] {
            let directory = tempfile::tempdir().unwrap();
            let mut source = memory(keyless, true, ttl);
            let keys = (0..2050).map(Some).collect::<Vec<_>>();
            source
                .push_proctime(batch(&keys, &vec![Some(2.0); keys.len()]), 100)
                .unwrap();
            let mut frames = Vec::new();
            source
                .visit_canonical_folds(|frame| {
                    frames.push(frame);
                    Ok(())
                })
                .unwrap();
            let mut target = operator(keyless, true, ttl, true, directory.path());
            let before = target.memory.state_bytes;
            for frame in &frames {
                target.validate_canonical_folds(frame).unwrap();
                assert_eq!(target.memory.state_bytes, before);
                assert!(target.last_write_ms.is_empty());
                let mut count = 0;
                target
                    .visit_canonical_folds(|batch| {
                        count += batch.num_rows();
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(count, 0);
            }
            for frame in &frames {
                target.import_canonical_folds(frame).unwrap();
            }
            let mut restored = Vec::new();
            target
                .visit_canonical_folds(|frame| {
                    restored.push(frame);
                    Ok(())
                })
                .unwrap();
            assert_eq!(rows_of(&frames), rows_of(&restored));
        }
    }
}

#[test]
fn malformed_later_fold_chunk_leaves_a_validated_target_empty_and_source_retryable() {
    let mut source = memory(false, true, 1000);
    let keys = (0..1025).map(Some).collect::<Vec<_>>();
    source
        .push_proctime(batch(&keys, &vec![Some(2.0); keys.len()]), 100)
        .unwrap();
    let mut frames = Vec::new();
    source
        .visit_canonical_folds(|frame| {
            frames.push(frame);
            Ok(())
        })
        .unwrap();
    assert_eq!(frames.len(), 2);
    let mut target = memory(false, true, 1000);
    target.validate_canonical_folds(&frames[0]).unwrap();
    let mut columns = frames[1].columns().to_vec();
    columns[2] = Arc::new(Int64Array::from(vec![None]));
    let invalid = RecordBatch::try_new(frames[1].schema(), columns).unwrap();
    assert!(target.validate_canonical_folds(&invalid).is_err());
    assert!(target.canonical_fold().unwrap().keys.is_empty());
    assert!(target.last_write_ms.is_empty());
    let mut retained = Vec::new();
    source
        .visit_canonical_folds(|frame| {
            retained.push(frame);
            Ok(())
        })
        .unwrap();
    assert_eq!(rows_of(&frames), rows_of(&retained));
    for frame in &frames {
        target.validate_canonical_folds(frame).unwrap();
    }
    for frame in &frames {
        target.import_canonical_folds(frame).unwrap();
    }
    assert_eq!(
        source
            .push_proctime(batch(&[Some(1)], &[Some(3.0)]), 200)
            .unwrap(),
        target
            .push_proctime(batch(&[Some(1)], &[Some(3.0)]), 200)
            .unwrap()
    );
}

#[test]
fn fold_validation_rejects_duplicate_targets_without_changing_retained_state() {
    let mut source = memory(false, true, 1000);
    source
        .push_proctime(batch(&[Some(1)], &[Some(2.0)]), 100)
        .unwrap();
    let mut frames = Vec::new();
    source
        .visit_canonical_folds(|frame| {
            frames.push(frame);
            Ok(())
        })
        .unwrap();
    let mut target = memory(false, true, 1000);
    target.import_canonical_folds(&frames[0]).unwrap();
    assert!(target.validate_canonical_folds(&frames[0]).is_err());
    let mut retained = Vec::new();
    target
        .visit_canonical_folds(|frame| {
            retained.push(frame);
            Ok(())
        })
        .unwrap();
    assert_eq!(rows_of(&frames), rows_of(&retained));
}

#[test]
fn memory_captures_keep_raw_float_folds_and_expired_ttl_ages_after_live_updates() {
    use crate::state::canonical_memory::CanonicalMemoryState;
    let rows = |op: &OverWindowAggregator| {
        let mut batches = Vec::new();
        op.visit_canonical_folds(|batch| {
            batches.push(batch);
            Ok(())
        })
        .unwrap();
        rows_of(&batches)
    };
    for keyless in [false, true] {
        for rows_frame in [false, true] {
            let mut live = memory(keyless, rows_frame, 1000);
            live.push_proctime(
                batch(
                    &[Some(1), Some(1), Some(1), None],
                    &[Some(1e16), Some(1.0), Some(-1e16), None],
                ),
                100,
            )
            .unwrap();
            let expected_first = rows(&live);
            // A stock restore can provide key state before any native input establishes its schema.
            live.input_schema = None;
            let mut first = memory(keyless, rows_frame, 1000);
            live.copy_state(&mut first).unwrap();
            live.push_proctime(batch(&[Some(1), None], &[Some(5.0), Some(7.0)]), 500)
                .unwrap();
            let expected_second = rows(&live);
            let mut second = memory(keyless, rows_frame, 1000);
            live.copy_state(&mut second).unwrap();
            live.push_proctime(batch(&[Some(1), None], &[Some(9.0), Some(11.0)]), 2500)
                .unwrap();
            drop(live);
            assert_eq!(rows(&second), expected_second);
            assert_eq!(rows(&first), expected_first);
        }
    }
}

#[test]
fn memory_capture_preserves_pending_event_rows_before_the_first_fold() {
    use crate::state::canonical_memory::CanonicalMemoryState;
    for keyless in [false, true] {
        let mut live = memory(keyless, false, 1000);
        live.proctime = false;
        let mut captured = memory(keyless, false, 1000);
        captured.proctime = false;
        live.push(
            batch(&[Some(1), None, Some(2)], &[Some(5.0), Some(7.0), None]),
            100,
        )
        .unwrap();
        let OverInner::Aggregates(inner) = &live.inner else {
            unreachable!()
        };
        assert!(
            inner.key_types.is_empty(),
            "no completed input has established fold key types"
        );
        live.copy_state(&mut captured).unwrap();
        assert_eq!(live.flush(0, 200).unwrap(), captured.flush(0, 200).unwrap());
    }
}
