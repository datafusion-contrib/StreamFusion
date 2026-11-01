#[cfg(feature = "rocksdb-state")]
use super::super::tests::operator;
use super::super::tests::{batch, memory, rows_of};
use super::*;
use arrow::array::{Float64Array, Int8Array, StringArray};

fn timed(keys: &[Option<i64>], values: &[Option<f64>], times: &[i64]) -> RecordBatch {
    let original = batch(keys, values);
    RecordBatch::try_new(
        original.schema(),
        vec![
            original.column(0).clone(),
            original.column(1).clone(),
            Arc::new(Int64Array::from(times.to_vec())),
        ],
    )
    .unwrap()
}

fn pending(operator: &OverWindowAggregator) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    operator
        .visit_canonical_pending(|batch| {
            batches.push(batch);
            Ok(())
        })
        .unwrap();
    batches
}

fn cleanup(operator: &OverWindowAggregator) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    operator
        .visit_canonical_cleanup(|batch| {
            batches.push(batch);
            Ok(())
        })
        .unwrap();
    batches
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn event_folds_pending_peers_and_cleanup_only_keys_continue_between_both_stores() {
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
                        source.proctime = false;
                        target.proctime = false;
                        source
                            .push(
                                timed(
                                    &[Some(1); 3],
                                    &[Some(1e16), Some(1.0), Some(-1e16)],
                                    &[1, 2, 3],
                                ),
                                100,
                            )
                            .unwrap();
                        source.flush(3, 101).unwrap();
                        source
                            .push(
                                timed(
                                    &[Some(1), Some(1), None, Some(2)],
                                    &[Some(1.0), Some(2.0), Some(5.0), None],
                                    &[10, 10, 11, 12],
                                ),
                                200,
                            )
                            .unwrap();
                        // A late-only key has a timer but no buffered input or accumulator.
                        source
                            .push(timed(&[Some(9)], &[Some(99.0)], &[1]), 201)
                            .unwrap();
                        let before_pending = rows_of(&pending(&source));
                        let before_cleanup = rows_of(&cleanup(&source));
                        source
                            .visit_canonical_folds(|batch| target.import_canonical_folds(&batch))
                            .unwrap();
                        source
                            .visit_canonical_pending(|batch| target.import_canonical_pending(batch))
                            .unwrap();
                        if ttl > 1 {
                            source
                                .visit_canonical_cleanup(|batch| {
                                    target.import_canonical_cleanup(&batch)
                                })
                                .unwrap();
                            assert_eq!(source.cleanup_state.len(), if keyless { 1 } else { 4 });
                        }
                        assert_eq!(rows_of(&pending(&target)), before_pending);
                        assert_eq!(rows_of(&cleanup(&target)), before_cleanup);
                        assert_eq!(rows_of(&pending(&source)), before_pending);
                        assert_eq!(rows_of(&cleanup(&source)), before_cleanup);
                        let suffix = timed(&[Some(1)], &[Some(4.0)], &[10]);
                        source.push(suffix.clone(), 250).unwrap();
                        target.push(suffix, 250).unwrap();
                        assert_eq!(
                            rows_of(&[source.flush(12, 251).unwrap()]),
                            rows_of(&[target.flush(12, 251).unwrap()])
                        );
                        assert_eq!(rows_of(&cleanup(&target)), rows_of(&cleanup(&source)));
                        // Cleanup must expire the same keys after the handoff.
                        source.flush(13, 1800).unwrap();
                        target.flush(13, 1800).unwrap();
                        assert!(source.cleanup_state.is_empty());
                        assert!(target.cleanup_state.is_empty());
                        assert!(source.pending_rows.is_empty());
                        assert!(target.pending_rows.is_empty());
                    }
                }
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn event_visitors_are_bounded_retryable_and_do_not_create_accumulators() {
    for persistent in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut source = operator(false, true, 1000, persistent, directory.path());
        source.proctime = false;
        let keys = (0..2500).map(Some).collect::<Vec<_>>();
        source
            .push(
                timed(&keys, &vec![Some(1.0); keys.len()], &vec![10; keys.len()]),
                100,
            )
            .unwrap();
        let mut count = 0;
        assert!(source
            .visit_canonical_pending(|batch| {
                count += batch.num_rows();
                if count > CHUNK_ROWS {
                    return Err(invalid("receiver stopped"));
                }
                Ok(())
            })
            .is_err());
        assert_eq!(
            pending(&source)
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![1024, 1024, 452]
        );
        let mut count = 0;
        assert!(source
            .visit_canonical_cleanup(|batch| {
                count += batch.num_rows();
                if count > CHUNK_ROWS {
                    return Err(invalid("receiver stopped"));
                }
                Ok(())
            })
            .is_err());
        assert_eq!(
            cleanup(&source)
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![1024, 1024, 452]
        );
        let mut folds = 0;
        source
            .visit_canonical_folds(|batch| {
                folds += batch.num_rows();
                Ok(())
            })
            .unwrap();
        assert_eq!(folds, 0);
        if !persistent {
            assert_eq!(
                pending(&source)[0].column(1).to_data().buffers()[0].as_ptr(),
                source.buffered[0].column(1).to_data().buffers()[0].as_ptr()
            );
        }
    }
}

#[test]
fn malformed_pending_rows_fail_before_buffer_or_retention_mutation() {
    for mode in [
        "missing",
        "type",
        "value_type",
        "null",
        "min",
        "complete",
        "rowkind",
        "null_kind",
        "kind_type",
        "schema",
    ] {
        let mut target = memory(false, true, 1000);
        target.proctime = false;
        let valid = timed(&[Some(1)], &[Some(1.0)], &[10]);
        let mut fields = valid
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        let mut columns = valid.columns().to_vec();
        match mode {
            "missing" => {
                fields.pop();
                columns.pop();
            }
            "type" => {
                fields[2] = Field::new("time", DataType::Utf8, false);
                columns[2] = Arc::new(StringArray::from(vec!["10"]));
            }
            "value_type" => {
                fields[1] = Field::new("value", DataType::Int64, false);
                columns[1] = Arc::new(Int64Array::from(vec![1]));
            }
            "null" => {
                fields[2] = Field::new("time", DataType::Int64, true);
                columns[2] = Arc::new(Int64Array::from(vec![None]));
            }
            "min" => columns[2] = Arc::new(Int64Array::from(vec![i64::MIN])),
            "complete" => target.watermark = 10,
            "rowkind" | "null_kind" => {
                fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, true));
                columns.push(Arc::new(Int8Array::from(vec![if mode == "rowkind" {
                    Some(1)
                } else {
                    None
                }])));
            }
            "kind_type" => {
                fields.push(Field::new(ROW_KIND_COLUMN, DataType::Float64, false));
                columns.push(Arc::new(Float64Array::from(vec![0.0])));
            }
            "schema" => target.input_schema = Some(Arc::new(Schema::empty())),
            _ => unreachable!(),
        }
        let malformed = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        assert!(
            target.import_canonical_pending(malformed).is_err(),
            "{mode}"
        );
        assert!(target.buffered.is_empty(), "{mode}");
        assert!(target.pending_rows.is_empty(), "{mode}");
        assert!(target.cleanup_state.is_empty(), "{mode}");
        assert_eq!(target.retention_bytes, 0, "{mode}");
    }
}

#[test]
fn malformed_cleanup_deadlines_fail_before_any_key_is_installed() {
    for mode in [
        "arity",
        "type",
        "name",
        "null",
        "min",
        "duplicate",
        "disabled",
    ] {
        let mut target = memory(false, true, if mode == "disabled" { 0 } else { 1000 });
        target.proctime = false;
        let mut fields = vec![
            Field::new("key0", DataType::Int64, true),
            Field::new(STAMP, DataType::Int64, true),
        ];
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![1600, 1700])),
        ];
        match mode {
            "arity" => {
                fields.pop();
                columns.pop();
            }
            "type" => {
                fields[1] = Field::new(STAMP, DataType::Utf8, false);
                columns[1] = Arc::new(StringArray::from(vec!["1600", "1700"]));
            }
            "name" => fields[1] = Field::new("other", DataType::Int64, false),
            "null" => columns[1] = Arc::new(Int64Array::from(vec![Some(1600), None])),
            "min" => columns[1] = Arc::new(Int64Array::from(vec![1600, i64::MIN])),
            "duplicate" => columns[0] = Arc::new(Int64Array::from(vec![1, 1])),
            "disabled" => {}
            _ => unreachable!(),
        }
        let malformed = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        assert!(
            target.import_canonical_cleanup(&malformed).is_err(),
            "{mode}"
        );
        assert!(target.cleanup_state.is_empty(), "{mode}");
        assert_eq!(target.retention_bytes, 0, "{mode}");
    }
}

fn seed_checkpoint_state(source: &mut OverWindowAggregator) {
    source
        .push(timed(&[Some(1)], &[Some(3.0)], &[1]), 100)
        .unwrap();
    source.flush(1, 101).unwrap();
    source
        .push(timed(&[Some(2)], &[Some(7.0)], &[10]), 200)
        .unwrap();
    source
        .push(timed(&[Some(9)], &[Some(99.0)], &[1]), 201)
        .unwrap();
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn raw_key_group_snapshots_preserve_pending_and_timer_only_keys_across_both_stores() {
    for keyless in [false, true] {
        for rows in [false, true] {
            for from_store in [false, true] {
                for to_store in [false, true] {
                    let directory = tempfile::tempdir().unwrap();
                    let mut source = operator(
                        keyless,
                        rows,
                        1000,
                        from_store,
                        &directory.path().join("source"),
                    );
                    source.proctime = false;
                    seed_checkpoint_state(&mut source);
                    let expected_pending = rows_of(&pending(&source));
                    let expected_cleanup = rows_of(&cleanup(&source));
                    let precisions: &[i32] = if keyless {
                        &[i32::MIN]
                    } else {
                        &[i32::MIN, -1]
                    };
                    let snapshots = if from_store {
                        source.canonical_partitions(128, precisions).unwrap()
                    } else {
                        source.snapshot_partitions(128, precisions)
                    };
                    let snapshots = snapshots.into_values().collect::<Vec<_>>();
                    let mut target = if to_store {
                        let mut target =
                            operator(keyless, rows, 1000, true, &directory.path().join("target"));
                        target.proctime = false;
                        target.import_partitions(&snapshots).unwrap();
                        target.adopt_store_retention(900).unwrap();
                        target
                    } else {
                        OverWindowAggregator::restore_partitions(
                            vec![1; 5],
                            vec![0, 3, 4, 1, 2],
                            2,
                            vec![1; 5],
                            if keyless { vec![] } else { vec![0] },
                            if rows { 3 } else { 0 },
                            0,
                            false,
                            &snapshots,
                            1000,
                            900,
                        )
                    };
                    assert_eq!(rows_of(&pending(&target)), expected_pending);
                    assert_eq!(rows_of(&cleanup(&target)), expected_cleanup);
                    // A second checkpoint before any input must retain timer-only groups too.
                    let second = if to_store {
                        target.canonical_partitions(128, precisions).unwrap()
                    } else {
                        target.snapshot_partitions(128, precisions)
                    };
                    assert_eq!(second.len(), snapshots.len());
                    assert_eq!(
                        rows_of(&[source.flush(10, 901).unwrap()]),
                        rows_of(&[target.flush(10, 901).unwrap()])
                    );
                    assert_eq!(rows_of(&cleanup(&target)), rows_of(&cleanup(&source)));
                }
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn aligned_rocks_checkpoint_preserves_late_only_and_pending_only_deadlines() {
    for keyless in [false, true] {
        for rows in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut source = operator(keyless, rows, 1000, true, &directory.path().join("source"));
            source.proctime = false;
            seed_checkpoint_state(&mut source);
            let expected_pending = rows_of(&pending(&source));
            let expected_cleanup = rows_of(&cleanup(&source));
            let checkpoint = directory.path().join("checkpoint");
            let manifest = source
                .checkpoint_store(checkpoint.to_str().unwrap())
                .unwrap();
            let types = source.inner.store_state_types();
            let store = crate::state::RocksOverAggStore::open_merged(
                crate::state::RocksStoreConfig {
                    table_dir: directory.path().join("restored").to_str().unwrap().into(),
                    max_parallelism: 128,
                    options_json: include_str!("../../../../benches/fixtures/rocks-options.json")
                        .into(),
                    ttl_ms: 0,
                    shared_resources: 0,
                },
                &types,
                &[],
                &[],
                batch(&[], &[]).schema(),
                0..=127,
                &[(checkpoint.to_str().unwrap().into(), manifest.snapshot_id)],
                true,
            )
            .unwrap();
            let mut target = memory(keyless, rows, 1000);
            target.proctime = false;
            target = target
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
            target.adopt_store_retention(900).unwrap();
            assert_eq!(rows_of(&pending(&target)), expected_pending);
            assert_eq!(rows_of(&cleanup(&target)), expected_cleanup);
            assert_eq!(
                rows_of(&[source.flush(10, 901).unwrap()]),
                rows_of(&[target.flush(10, 901).unwrap()])
            );
            source.flush(11, 2500).unwrap();
            target.flush(11, 2500).unwrap();
            assert!(source.cleanup_state.is_empty());
            assert!(target.cleanup_state.is_empty());
            let second = directory.path().join("after-cleanup");
            target.checkpoint_store(second.to_str().unwrap()).unwrap();
            assert!(target
                .store
                .as_ref()
                .unwrap()
                .scan_stamps()
                .unwrap()
                .is_empty());
        }
    }
}

#[test]
fn timestamps_keep_their_carriers_and_invalid_second_ranges_fail_without_mutation() {
    use arrow::array::{
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    let carriers: Vec<ArrayRef> = vec![
        Arc::new(TimestampSecondArray::from(vec![-1])),
        Arc::new(TimestampMillisecondArray::from(vec![-1001])),
        Arc::new(TimestampMicrosecondArray::from(vec![-1001])),
        Arc::new(TimestampNanosecondArray::from(vec![-1001])),
        Arc::new(streamfusion_bridge::timestamp::timestamp_array([Some(
            streamfusion_bridge::timestamp::TimestampValue::new(-1, 999_999).unwrap(),
        )])),
    ];
    for time in carriers {
        let mut target = memory(false, true, 1000);
        target.proctime = false;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, true),
                Field::new("value", DataType::Float64, true),
                Field::new("time", time.data_type().clone(), false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Float64Array::from(vec![1.0])),
                time,
            ],
        )
        .unwrap();
        target.import_canonical_pending(batch.clone()).unwrap();
        assert_eq!(pending(&target), vec![batch]);
        assert!(target.cleanup_state.is_empty());
    }
    let mut target = memory(false, true, 1000);
    target.proctime = false;
    let time: ArrayRef = Arc::new(TimestampSecondArray::from(vec![i64::MAX]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("value", DataType::Float64, true),
            Field::new("time", time.data_type().clone(), false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Float64Array::from(vec![1.0])),
            time,
        ],
    )
    .unwrap();
    assert!(target.import_canonical_pending(batch).is_err());
    assert!(target.buffered.is_empty());
    assert_eq!(target.retention_bytes, 0);
}

#[test]
fn older_native_snapshots_without_cleanup_footer_still_restore_fold_deadlines() {
    let mut source = memory(false, true, 1000);
    source.proctime = false;
    source
        .push(timed(&[Some(1)], &[Some(3.0)], &[1]), 100)
        .unwrap();
    source.flush(1, 101).unwrap();
    let snapshot = source.snapshot();
    let (old_format, footer) = OverWindowAggregator::split_cleanup(&snapshot);
    assert!(!footer.is_empty());
    let mut restored = OverWindowAggregator::restore(
        vec![1; 5],
        vec![0, 3, 4, 1, 2],
        2,
        vec![1; 5],
        vec![0],
        3,
        0,
        false,
        old_format,
        1000,
        900,
    );
    assert_eq!(rows_of(&cleanup(&source)), rows_of(&cleanup(&restored)));
    let input = timed(&[Some(1)], &[Some(4.0)], &[10]);
    source.push(input.clone(), 901).unwrap();
    restored.push(input, 901).unwrap();
    assert_eq!(
        source.flush(10, 902).unwrap(),
        restored.flush(10, 902).unwrap()
    );
}

#[test]
fn event_state_transfer_rejects_processing_time_bounded_and_distinct_shapes() {
    for (frame, distinct, proctime) in [
        (3, false, true),
        (0, false, true),
        (1, false, false),
        (2, false, false),
        (3, true, false),
        (0, true, false),
    ] {
        let mut target = OverWindowAggregator::new(
            vec![1],
            vec![if distinct { 100 } else { 0 }],
            2,
            vec![1],
            vec![0],
            frame,
            2,
            proctime,
        )
        .with_state_retention(1000);
        let valid = timed(&[Some(1)], &[Some(1.0)], &[10]);
        assert!(target.import_canonical_pending(valid).is_err());
        assert!(target.visit_canonical_pending(|_| Ok(())).is_err());
        assert!(target.visit_canonical_cleanup(|_| Ok(())).is_err());
        assert!(target.buffered.is_empty());
        assert!(target.cleanup_state.is_empty());
    }
}

#[test]
fn restored_pending_rows_accept_optional_insert_and_string_cache_companions() {
    use arrow::array::{Int8Array, StringArray};
    use streamfusion_bridge::string_cache::{string_cache_array, string_cache_field};
    for old_kind in [false, true] {
        for new_kind in [false, true] {
            for old_cache in [false, true] {
                for new_cache in [false, true] {
                    let make = |time: i64, kind: bool, cache: bool| {
                        let original = timed(&[Some(1)], &[Some(2.0)], &[time]);
                        let mut fields = original
                            .schema()
                            .fields()
                            .iter()
                            .map(|field| field.as_ref().clone())
                            .collect::<Vec<_>>();
                        let mut columns = original.columns().to_vec();
                        fields.push(Field::new("payload", DataType::Utf8, false));
                        columns.push(Arc::new(StringArray::from(vec!["😀"])));
                        if kind {
                            fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
                            columns.push(Arc::new(Int8Array::from(vec![0])));
                        }
                        if cache {
                            let units = "😀".encode_utf16().collect::<Vec<_>>();
                            let array = string_cache_array([Some(units.as_slice())]);
                            fields.push(string_cache_field(3, array.data_type().clone()));
                            columns.push(array);
                        }
                        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
                    };
                    let mut target = memory(false, true, 1000);
                    target.proctime = false;
                    let restored = make(10, old_kind, old_cache);
                    let address = restored.column(3).to_data().buffers()[1].as_ptr();
                    target.import_canonical_pending(restored).unwrap();
                    target.push(make(11, new_kind, new_cache), 100).unwrap();
                    assert_eq!(
                        target.buffered[0].column(3).to_data().buffers()[1].as_ptr(),
                        address
                    );
                    let output = target.flush(11, 101).unwrap();
                    assert_eq!(output.num_rows(), 2);
                    assert_eq!(
                        output
                            .column(3)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0),
                        "😀"
                    );
                    assert_eq!(
                        output
                            .column(4)
                            .as_any()
                            .downcast_ref::<arrow::array::Float64Array>()
                            .unwrap()
                            .values(),
                        &[2.0, 4.0]
                    );
                    if new_cache && !old_cache {
                        assert!(output.column_by_name("$string_java_3$").unwrap().is_null(0));
                    }
                }
            }
        }
    }
}

#[test]
fn pending_schema_change_rejects_logical_columns_before_touching_live_state() {
    let mut target = memory(false, true, 1000);
    target.proctime = false;
    target
        .import_canonical_pending(timed(&[Some(1)], &[Some(2.0)], &[10]))
        .unwrap();
    let before = pending(&target);
    let invalid = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
            Field::new("time", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int64Array::from(vec![11])),
        ],
    )
    .unwrap();
    assert!(target.push(invalid, 100).is_err());
    assert_eq!(pending(&target), before);
    assert!(target.cleanup_state.is_empty());
    assert_eq!(target.pending_rows.values().sum::<u32>(), 1);
}
