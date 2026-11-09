use super::*;
use arrow::array::{Float64Array, StringArray};

const KINDS: [i64; 8] = [0, 3, 4, 22, 100, 103, 104, 122];

fn input(key: Option<i64>, time: i64, value: Option<f64>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![key])) as ArrayRef),
        (
            "value",
            Arc::new(Float64Array::from(vec![value])) as ArrayRef,
        ),
        ("time", Arc::new(Int64Array::from(vec![time])) as ArrayRef),
        (
            "unused",
            Arc::new(StringArray::from(vec![Some("kept-😀")])) as ArrayRef,
        ),
        (TIME, Arc::new(Int64Array::from(vec![777])) as ArrayRef),
    ])
    .unwrap()
}

fn memory(shape: usize, keyless: bool, ttl: i64) -> OverWindowAggregator {
    OverWindowAggregator::new(
        vec![1; 8],
        KINDS.to_vec(),
        2,
        vec![1; 8],
        if keyless { vec![] } else { vec![0] },
        if shape == 2 { 2 } else { 1 },
        2,
        shape == 0,
    )
    .with_state_retention(ttl)
    .with_key_timestamp_precisions(if keyless { vec![] } else { vec![-1] })
}

#[cfg(feature = "rocksdb-state")]
fn config(path: &std::path::Path) -> crate::state::RocksStoreConfig {
    crate::state::RocksStoreConfig {
        table_dir: path.to_str().unwrap().into(),
        max_parallelism: 8,
        options_json: include_str!("../../../../benches/fixtures/rocks-options.json").into(),
        ttl_ms: 0,
        shared_resources: 0,
    }
}

#[cfg(feature = "rocksdb-state")]
fn operator(
    shape: usize,
    keyless: bool,
    ttl: i64,
    persistent: bool,
    path: &std::path::Path,
) -> OverWindowAggregator {
    if persistent {
        let store = crate::state::RocksOverAggStore::create(
            config(path),
            &[],
            &rocks_over_frame_value_types(&[1; 8], &KINDS, if shape == 2 { 2 } else { 1 }),
            &[],
            input(None, 0, None).schema(),
            0..=7,
        )
        .unwrap();
        memory(shape, keyless, ttl).with_store(
            store,
            if keyless {
                vec![]
            } else {
                vec![DataType::Int64]
            },
        )
    } else {
        memory(shape, keyless, ttl)
            .with_memory_budget(64 << 20)
            .unwrap()
    }
}

fn execute(
    op: &mut OverWindowAggregator,
    key: Option<i64>,
    time: i64,
    value: Option<f64>,
    now: i64,
) -> RecordBatch {
    if op.proctime {
        op.push_proctime(input(key, time, value), now).unwrap()
    } else {
        op.push(input(key, time, value), now).unwrap();
        op.flush(time, now).unwrap()
    }
}

fn frames(op: &OverWindowAggregator) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    op.visit_canonical_frames(|batch| {
        out.push(batch);
        Ok(())
    })
    .unwrap();
    out
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn direct_frames_continue_raw_arithmetic_and_original_inputs_between_both_stores() {
    for shape in 0..3 {
        for keyless in [false, true] {
            for ttl in [0, 1000] {
                for from in [false, true] {
                    for to in [false, true] {
                        let dir = tempfile::tempdir().unwrap();
                        let mut source =
                            operator(shape, keyless, ttl, from, &dir.path().join("source"));
                        let mut target =
                            operator(shape, keyless, ttl, to, &dir.path().join("target"));
                        source.bind_canonical_frame_inputs().unwrap();
                        for (index, value) in [1e16, 1.0, -1e16].into_iter().enumerate() {
                            execute(
                                &mut source,
                                None,
                                1000 + index as i64,
                                Some(value),
                                100 + index as i64,
                            );
                        }
                        let exported = frames(&source);
                        assert_eq!(exported.len(), 1);
                        assert_eq!(exported[0].schema().field(5).name(), "__over_frame_time_");
                        let offset = 5 + 3;
                        assert_eq!(
                            ScalarValue::try_from_array(exported[0].column(offset), 2).unwrap(),
                            ScalarValue::Float64(Some(0.0))
                        );
                        for batch in &exported {
                            target.import_canonical_frame(batch).unwrap();
                        }
                        drop(exported);
                        assert_eq!(frames(&source), frames(&target));
                        for step in 3..8 {
                            let value = (step == 7).then_some(7.0);
                            assert_eq!(
                                execute(&mut source, None, 1000 + step, value, 100 + step),
                                execute(&mut target, None, 1000 + step, value, 100 + step),
                            );
                        }
                        if shape != 2 && ttl != 0 {
                            assert_eq!(
                                execute(&mut source, None, 2000, Some(3.0), 2000),
                                execute(&mut target, None, 2000, Some(3.0), 2000)
                            );
                        }
                    }
                }
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn stale_range_cutoffs_and_pending_peers_transfer_without_replaying_the_frame() {
    for from in [false, true] {
        for to in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut source = operator(2, false, 0, from, &dir.path().join("source"));
            let mut target = operator(2, false, 0, to, &dir.path().join("target"));
            // A two-millisecond frame: old cleanup1004 fires while cleanup1014 is current.
            source.bind_canonical_frame_inputs().unwrap();
            execute(&mut source, Some(1), 1000, Some(1.0), 0);
            source.push(input(Some(1), 1010, Some(7.0)), 0).unwrap();
            source.flush(1005, 0).unwrap();
            for batch in frames(&source) {
                target.import_canonical_frame(&batch).unwrap();
            }
            source
                .visit_canonical_pending(|batch| target.import_canonical_pending(batch))
                .unwrap();
            source.push(input(Some(1), 1002, Some(99.0)), 0).unwrap();
            target.push(input(Some(1), 1002, Some(99.0)), 0).unwrap();
            assert_eq!(source.late_drops, 1);
            assert_eq!(target.late_drops, 1);
            assert_eq!(
                source.flush(1010, 0).unwrap(),
                target.flush(1010, 0).unwrap()
            );
            assert_eq!(frames(&source), frames(&target));
        }
    }
}

fn replaced(batch: &RecordBatch, column: usize, values: ArrayRef) -> RecordBatch {
    let mut columns = batch.columns().to_vec();
    columns[column] = values;
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

#[test]
fn malformed_frames_and_duplicate_keys_fail_before_mutating_a_resident_target() {
    let mut source = memory(0, false, 0).with_memory_budget(1 << 20).unwrap();
    source.bind_canonical_frame_inputs().unwrap();
    for (index, value) in [1.0, 2.0, 3.0].into_iter().enumerate() {
        execute(&mut source, Some(1), 1000 + index as i64, Some(value), 0);
    }
    let batch = frames(&source).pop().unwrap();
    let cases = vec![
        replaced(&batch, 0, Arc::new(Int64Array::from(vec![1, 2, 1]))),
        replaced(&batch, 7, Arc::new(Int64Array::from(vec![0, 1, 9]))),
        replaced(
            &batch,
            8,
            Arc::new(Float64Array::from(vec![Some(1.0), None, Some(6.0)])),
        ),
        replaced(
            &batch,
            9,
            Arc::new(Int64Array::from(vec![None, None, None])),
        ),
        replaced(&batch, 5, Arc::new(Int64Array::from(vec![2, 1, 3]))),
    ];
    for invalid in cases {
        let mut target = memory(0, false, 0).with_memory_budget(1 << 20).unwrap();
        assert!(target.import_canonical_frame(&invalid).is_err());
        assert!(target.inner.state_keys().is_empty());
        assert!(target.input_schema.is_none());
        target.import_canonical_frame(&batch).unwrap();
        assert!(target.import_canonical_frame(&batch).is_err());
        assert_eq!(frames(&target), vec![batch.clone()]);
    }
}

#[test]
fn nonfinite_empty_counts_and_receiver_failure_preserve_the_live_fold() {
    let mut source = memory(0, true, 0).with_memory_budget(1 << 20).unwrap();
    source.bind_canonical_frame_inputs().unwrap();
    for (index, value) in [
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        None,
        None,
        None,
    ]
    .into_iter()
    .enumerate()
    {
        execute(&mut source, None, 1000 + index as i64, value, 0);
    }
    let mut target = memory(0, true, 0).with_memory_budget(1 << 20).unwrap();
    for batch in frames(&source) {
        target.import_canonical_frame(&batch).unwrap();
    }
    let before = frames(&source);
    assert!(source
        .visit_canonical_frames(|_| Err(invalid("receiver stopped")))
        .is_err());
    assert_eq!(frames(&source), before);
    let output = execute(&mut target, None, 1005, Some(1.0), 0);
    assert!(
        matches!(ScalarValue::try_from_array(output.column(5), 0).unwrap(), ScalarValue::Float64(Some(value)) if value.is_nan())
    );
}

#[test]
fn nullable_decimal_overflow_is_not_refolded() {
    use arrow::array::Decimal128Array;
    let make = || {
        OverWindowAggregator::new(
            vec![5800; 3],
            vec![0, 4, 22],
            1,
            vec![0; 3],
            vec![],
            1,
            2,
            true,
        )
        .with_memory_budget(1 << 20)
        .unwrap()
    };
    let input = |value| {
        RecordBatch::try_from_iter(vec![
            (
                "value",
                Arc::new(
                    Decimal128Array::from(vec![value])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ) as ArrayRef,
            ),
            ("time", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
        ])
        .unwrap()
    };
    let mut source = make();
    source.bind_canonical_frame_inputs().unwrap();
    let high = 10_i128.pow(38) - 1;
    source.push_proctime(input(Some(high)), 0).unwrap();
    source.push_proctime(input(Some(high)), 0).unwrap();
    let exported = frames(&source);
    assert!(
        exported[0].column(5).is_null(1),
        "raw decimal SUM overflow remains NULL"
    );
    let mut target = make();
    target.import_canonical_frame(&exported[0]).unwrap();
    for value in [None, Some(-high), Some(1)] {
        assert_eq!(
            source.push_proctime(input(value), 0).unwrap(),
            target.push_proctime(input(value), 0).unwrap()
        );
    }
}

#[test]
fn typed_null_arguments_keep_their_original_type_and_raw_numeric_state() {
    // Typed NULL arguments use the existing aggregate slot's declared numeric state type.
    let original = input(None, 0, None);
    let mut columns = original.columns().to_vec();
    columns[1] = Arc::new(arrow::array::BooleanArray::from(vec![None]));
    let mut fields = original.schema().fields().to_vec();
    fields[1] = Arc::new(Field::new("value", DataType::Boolean, true));
    let original = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let mut source = memory(0, true, 0).with_memory_budget(1 << 20).unwrap();
    source.bind_canonical_frame_inputs().unwrap();
    source.push_proctime(original, 0).unwrap();
    let mut target = memory(0, true, 0).with_memory_budget(1 << 20).unwrap();
    for frame in frames(&source) {
        target.import_canonical_frame(&frame).unwrap();
    }
    assert_eq!(frames(&source), frames(&target));
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn imported_frames_and_protocol_survive_file_checkpoints_on_every_shape() {
    for shape in 0..3 {
        for aligned in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut source = operator(shape, false, 1000, false, &dir.path().join("source"));
            source.bind_canonical_frame_inputs().unwrap();
            for (index, value) in [1e16, 1.0, -1e16].into_iter().enumerate() {
                execute(&mut source, Some(7), 1000 + index as i64, Some(value), 100);
            }
            let mut target = operator(shape, false, 1000, true, &dir.path().join("target"));
            for batch in frames(&source) {
                target.import_canonical_frame(&batch).unwrap();
            }
            let snapshot = dir.path().join("checkpoint");
            let manifest = target.checkpoint_store(snapshot.to_str().unwrap()).unwrap();
            let store = crate::state::RocksOverAggStore::open_merged(
                config(&dir.path().join("restored")),
                &[],
                &rocks_over_frame_value_types(&[1; 8], &KINDS, if shape == 2 { 2 } else { 1 }),
                &[],
                input(None, 0, None).schema(),
                0..=7,
                &[(snapshot.to_str().unwrap().into(), manifest.snapshot_id)],
                aligned,
            )
            .unwrap();
            let mut restored = memory(shape, false, 1000).with_store(store, vec![DataType::Int64]);
            restored.adopt_store_retention(0).unwrap();
            assert_eq!(frames(&target), frames(&restored));
            assert_eq!(
                execute(&mut target, Some(7), 1003, None, 200),
                execute(&mut restored, Some(7), 1003, None, 200)
            );
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn an_unowned_partition_is_rejected_before_enabling_or_writing_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let key = (0..32)
        .find(|key| {
            let batch = input(Some(*key), 1000, Some(1.0));
            flink_key_group(binary_row_hash(&batch, &[0], 0, &[-1]), 8) != 0
        })
        .unwrap();
    let mut source = memory(0, false, 0).with_memory_budget(1 << 20).unwrap();
    source.bind_canonical_frame_inputs().unwrap();
    execute(&mut source, Some(key), 1000, Some(1.0), 0);
    let store = crate::state::RocksOverAggStore::create(
        config(dir.path()),
        &[],
        &rocks_over_frame_value_types(&[1; 8], &KINDS, 1),
        &[],
        input(None, 0, None).schema(),
        0..=0,
    )
    .unwrap();
    let mut target = memory(0, false, 0).with_store(store, vec![DataType::Int64]);
    assert!(target
        .import_canonical_frame(&frames(&source)[0])
        .err()
        .unwrap()
        .to_string()
        .contains("unowned"));
    assert!(target.input_schema.is_none());
    let store = target.store.as_ref().unwrap();
    assert!(!store.frame_inputs_enabled().unwrap());
    assert!(store.scan_frames().unwrap().is_empty());
}

#[test]
fn inconsistent_range_timer_cutoff_and_map_membership_are_rejected() {
    let mut source = memory(2, false, 0).with_memory_budget(1 << 20).unwrap();
    source.bind_canonical_frame_inputs().unwrap();
    for index in 0..3 {
        execute(&mut source, Some(7), 1000 + index, Some(1.0), 0);
    }
    let batch = frames(&source).pop().unwrap();
    let timer_column = batch.schema().index_of(TIMERS).unwrap();
    let order_column = batch.schema().index_of(ORDER).unwrap();
    let last_column = batch.schema().index_of(LAST).unwrap();
    for invalid in [
        replaced(
            &batch,
            timer_column,
            tail_list(Some(&[1004, 1004, 1006]), 3),
        ),
        replaced(&batch, timer_column, tail_list(Some(&[1004]), 3)),
        replaced(&batch, order_column, tail_list(Some(&[]), 3)),
        replaced(
            &batch,
            last_column,
            Arc::new(Int64Array::from(vec![None, None, None])),
        ),
    ] {
        let mut target = memory(2, false, 0).with_memory_budget(1 << 20).unwrap();
        assert!(target.import_canonical_frame(&invalid).is_err());
        assert!(target.input_schema.is_none());
        assert!(target.range_timers.is_empty());
        assert_eq!(target.retention_bytes, 0);
        target.import_canonical_frame(&batch).unwrap();
    }
    // Registering cleanup may wrap behind the last fired callback. A timer's numerical position
    // alone cannot establish whether Flink registered it before or after that callback.
    let with_earlier_timer = replaced(
        &batch,
        timer_column,
        tail_list(Some(&[1001, 1004, 1006]), 3),
    );
    let mut target = memory(2, false, 0);
    target.import_canonical_frame(&with_earlier_timer).unwrap();
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn importing_later_partitions_first_does_not_advance_the_processing_time_counter() {
    let sorted = |operator: &OverWindowAggregator| {
        let mut batches = frames(operator);
        batches.sort_by_key(|batch| column_at_i64(batch, 0).value(0));
        batches
    };
    for from in [false, true] {
        for to in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut source = operator(0, false, 0, from, &dir.path().join("source"));
            let mut target = operator(0, false, 0, to, &dir.path().join("target"));
            source.bind_canonical_frame_inputs().unwrap();
            for key in [1, 2] {
                for index in 0..3 {
                    execute(&mut source, Some(key), 1000 + index, Some(index as f64), 0);
                }
            }
            let mut batches = frames(&source);
            batches.sort_by_key(|batch| std::cmp::Reverse(column_at_i64(batch, 5).value(0)));
            for batch in batches {
                target.import_canonical_frame(&batch).unwrap();
            }
            assert_eq!(source.next_seq, target.next_seq);
            if let Some(store) = &target.store {
                assert_eq!(source.next_seq as u64, store.next_seq());
            }
            assert_eq!(sorted(&source), sorted(&target));
            for key in [1, 2] {
                assert_eq!(
                    execute(&mut source, Some(key), 1003, Some(7.0), 200),
                    execute(&mut target, Some(key), 1003, Some(7.0), 200)
                );
            }
            assert_eq!(sorted(&source), sorted(&target));
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn frozen_frame_readers_keep_metadata_on_disk_through_live_updates() {
    for shape in 0..3 {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state");
        let mut source = operator(shape, false, 1000, true, &path);
        source.bind_canonical_frame_inputs().unwrap();
        for key in 0..17 {
            execute(&mut source, Some(key), 1000, Some(key as f64), 100);
        }
        source.checkpoint_store("").unwrap();
        let frame_values = |value: &OverWindowAggregator| {
            let frames = frames(value)
                .into_iter()
                .map(|batch| {
                    batch
                        .project(&(0..batch.num_columns() - 4).collect::<Vec<_>>())
                        .unwrap()
                })
                .collect::<Vec<_>>();
            super::super::tests::rows_of(&frames)
        };
        let expected = frame_values(&source);
        let expected_cleanup = source
            .cleanup_state
            .iter()
            .map(|(key, &stamp)| (key.clone(), stamp))
            .collect::<Vec<_>>();
        let protocol = |value: &OverWindowAggregator| {
            let mut out = Vec::new();
            if shape == 0 {
                value
                    .visit_canonical_processing_rows(|batch| {
                        out.push(batch);
                        Ok(())
                    })
                    .unwrap();
            } else if shape == 2 {
                value
                    .visit_canonical_range(|batch| {
                        out.push(batch);
                        Ok(())
                    })
                    .unwrap();
            }
            if shape == 2 {
                out = out
                    .into_iter()
                    .map(|batch| {
                        batch
                            .project(&(0..batch.num_columns() - 1).collect::<Vec<_>>())
                            .unwrap()
                    })
                    .collect();
            }
            super::super::tests::rows_of(&out)
        };
        let expected_protocol = protocol(&source);
        let snapshot = crate::state::rocks_store::FrozenDatabases::capture(&path).unwrap();
        execute(&mut source, Some(0), 1001, Some(99.0), 200);
        source.checkpoint_store("").unwrap();
        snapshot.with(|| {
            let mut reader = operator(shape, false, 1000, true, &path);
            reader.bind_canonical_frame_inputs().unwrap();
            assert!(reader.cleanup_state.is_empty());
            assert!(reader.proc_rows.is_empty());
            assert!(reader.range_timers.is_empty());
            assert_eq!(frame_values(&reader), expected);
            for (key, stamp) in &expected_cleanup {
                assert_eq!(
                    reader.canonical_cleanup_stamp(&key.0).unwrap(),
                    Some(*stamp)
                );
            }
            assert_eq!(protocol(&reader), expected_protocol);
            assert_eq!(reader.retention_bytes, 0);
            assert!(reader.cleanup_state.is_empty());
            assert!(reader.proc_rows.is_empty());
            assert!(reader.range_timers.is_empty());
        });
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn captured_hot_partition_frames_obey_row_and_byte_bounds_without_early_accumulators() {
    for (count, payload) in [(2051usize, 0usize), (3, 2 * 1024 * 1024)] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state");
        let mut source = operator(2, false, 0, true, &path);
        let OverInner::Bounded(inner) = &mut source.inner else {
            unreachable!()
        };
        inner.offset = 10000;
        source.bind_canonical_frame_inputs().unwrap();
        let template = input(Some(7), 1000, Some(1.0));
        let mut columns = template
            .columns()
            .iter()
            .map(|column| {
                arrow::compute::take(
                    column,
                    &arrow::array::UInt32Array::from(vec![0; count]),
                    None,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        columns[2] = Arc::new(Int64Array::from_iter_values(
            (0..count).map(|i| 1000 + i as i64),
        ));
        if payload > 0 {
            columns[3] = Arc::new(StringArray::from(vec!["x".repeat(payload); count]));
        }
        source
            .push(
                RecordBatch::try_new(template.schema(), columns).unwrap(),
                100,
            )
            .unwrap();
        source.flush(4000, 101).unwrap();
        source.checkpoint_store("").unwrap();
        let snapshot = crate::state::rocks_store::FrozenDatabases::capture(&path).unwrap();
        snapshot.with(|| {
            let reader = operator(2, false, 0, true, &path);
            assert!(reader
                .visit_canonical_frames(|_| Err(invalid("receiver failed")))
                .is_err());
            let mut seen = 0;
            let mut chunks = 0;
            let mut final_chunks = 0;
            reader
                .visit_canonical_frames(|batch| {
                    assert!(batch.num_rows() <= CHUNK_ROWS);
                    if payload > 0 {
                        assert!(batch.num_rows() <= 2);
                    }
                    let positions = column_at_i64(&batch, 7);
                    assert_eq!(positions.value(0), seen as i64);
                    seen += batch.num_rows();
                    assert_eq!(positions.value(batch.num_rows() - 1), seen as i64 - 1);
                    let last =
                        batch.schema().metadata()["streamfusion.canonical.frame.final"] == "true";
                    if last {
                        final_chunks += 1;
                        assert_eq!(seen, count);
                        assert_eq!(
                            ScalarValue::try_from_array(batch.column(10), batch.num_rows() - 1)?,
                            ScalarValue::Int64(Some(count as i64))
                        );
                    } else {
                        for column in
                            &batch.columns()[8..8 + reader.canonical_frame()?.fold_types.len()]
                        {
                            assert_eq!(column.null_count(), batch.num_rows());
                        }
                    }
                    chunks += 1;
                    Ok(())
                })
                .unwrap();
            assert_eq!(seen, count);
            assert_eq!(final_chunks, 1);
            if payload == 0 {
                assert_eq!(chunks, 3);
            } else {
                assert!(chunks >= 2, "wide input must cross the byte bound");
            }
            snapshot.cancel();
            assert!(reader.visit_canonical_frames(|_| Ok(())).is_err());
        });
    }
}
