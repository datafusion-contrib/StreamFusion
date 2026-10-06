use super::*;
use arrow::array::TimestampSecondArray;
use arrow::datatypes::TimeUnit;
fn batch(times: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from_value(1, times.len())) as ArrayRef,
        ),
        ("rt", times),
    ])
    .unwrap()
}

fn joiner(
    left: &RecordBatch,
    right: &RecordBatch,
    lower: i64,
    upper: i64,
    kind: JoinKind,
) -> IntervalJoiner {
    IntervalJoiner::new(
        vec![0],
        vec![0],
        1,
        1,
        lower,
        upper,
        None,
        kind,
        left.schema(),
        right.schema(),
    )
}

#[test]
fn mixed_layout_bounds_keep_payloads_and_match_flags_after_restore() {
    let layouts: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1000])),
        Arc::new(TimestampSecondArray::from(vec![1])),
        Arc::new(TimestampMillisecondArray::from(vec![1000])),
        Arc::new(TimestampMicrosecondArray::from(vec![1_000_999])),
        Arc::new(TimestampNanosecondArray::from(vec![1_000_999_999])),
    ];
    for left in layouts {
        let left = batch(left);
        let right = batch(Arc::new(TimestampNanosecondArray::from(vec![
            1_000_000_001,
        ])));
        for kind in [
            JoinKind::Inner,
            JoinKind::LeftOuter,
            JoinKind::RightOuter,
            JoinKind::FullOuter,
        ] {
            for left_first in [true, false] {
                let mut original = joiner(&left, &right, 0, 0, kind);
                if left_first {
                    original.push_left(left.clone(), None).unwrap();
                } else {
                    original.push_right(right.clone(), None).unwrap();
                }
                let mut restored = IntervalJoiner::restore(
                    vec![0],
                    vec![0],
                    1,
                    1,
                    0,
                    0,
                    None,
                    kind,
                    left.schema(),
                    right.schema(),
                    &original.snapshot(),
                );
                let output = if left_first {
                    restored.push_right(right.clone(), None).unwrap()
                } else {
                    restored.push_left(left.clone(), None).unwrap()
                };
                assert_eq!(output.num_rows(), 1, "{}", left.column(1).data_type());
                assert_eq!(output.column(1), left.column(1));
                assert_eq!(output.column(3), right.column(1));
                let mut restored = IntervalJoiner::restore(
                    vec![0],
                    vec![0],
                    1,
                    1,
                    0,
                    0,
                    None,
                    kind,
                    left.schema(),
                    right.schema(),
                    &restored.snapshot(),
                );
                assert_eq!(restored.advance(1001).unwrap().num_rows(), 0);
                assert!(restored.left_buffered.is_empty());
                assert!(restored.right_buffered.is_empty());
            }
        }
    }
}

#[test]
fn wide_timestamps_and_offsets_do_not_require_nanosecond_durations() {
    for millis in [-100_000, 31_494_784_780_800_000, i64::MAX - 2000] {
        let left = batch(Arc::new(TimestampMillisecondArray::from(vec![millis])));
        let right = batch(Arc::new(TimestampMillisecondArray::from(vec![
            millis - 1000,
        ])));
        for left_first in [true, false] {
            let horizon = if millis < 0 { 102_000 } else { 1000 };
            let mut join = joiner(&left, &right, -horizon, horizon, JoinKind::Inner);
            let output = if left_first {
                join.push_left(left.clone(), None).unwrap();
                join.push_right(right.clone(), None).unwrap()
            } else {
                join.push_right(right.clone(), None).unwrap();
                join.push_left(left.clone(), None).unwrap()
            };
            assert_eq!(output.num_rows(), 1);
            assert_eq!(output.column(1), left.column(1));
            assert_eq!(output.column(3), right.column(1));
            join.advance(i64::MAX).unwrap();
        }
    }
    let left = batch(Arc::new(TimestampNanosecondArray::from(vec![
        1_000_000_000,
    ])));
    let right = batch(Arc::new(TimestampNanosecondArray::from(vec![
        5_000_000_000,
    ])));
    let mut join = joiner(
        &left,
        &right,
        -12_000_000_000_000,
        12_000_000_000_000,
        JoinKind::Inner,
    );
    join.push_left(left, None).unwrap();
    assert_eq!(join.push_right(right, None).unwrap().num_rows(), 1);
}

#[test]
fn bounds_preserve_java_overflow_direction_and_sql_nulls() {
    let bounds = IntervalBounds {
        left_time: 0,
        right_time: 0,
        lower: 0,
        upper: 10,
        incoming_left: true,
    };
    assert!(bounds.contains(i64::MAX - 5, i64::MAX - 7));
    assert!(!IntervalBounds {
        incoming_left: false,
        ..bounds
    }
    .contains(i64::MAX - 5, i64::MAX - 7));
    let left: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        Some(-1),
        None,
        Some(1),
    ]));
    let right = Arc::new(TimestampMillisecondArray::from(vec![
        Some(-1),
        Some(0),
        None,
    ])) as ArrayRef;
    let input = RecordBatch::try_from_iter(vec![("left", left), ("right", right)]).unwrap();
    let result = IntervalBounds { upper: 0, ..bounds }
        .expression(&input.schema(), 1)
        .evaluate(&input)
        .unwrap()
        .into_array(3)
        .unwrap();
    assert_eq!(
        result.as_ref(),
        &BooleanArray::from(vec![Some(true), None, None])
    );
    let predicate = IntervalPredicate {
        bounds,
        signature: Signature::any(2, Volatility::Immutable),
    };
    assert!(predicate
        .return_type(&[
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Int64
        ])
        .is_ok());
    assert!(predicate
        .return_type(&[DataType::Utf8, DataType::Int64])
        .is_err());
    assert!(predicate.return_type(&[]).is_err());
}

fn millis(value: i64) -> RecordBatch {
    batch(Arc::new(Int64Array::from(vec![value])))
}

#[test]
fn expired_arrivals_do_not_enter_state_and_outer_rows_pad_immediately() {
    for persistent in [false, true] {
        for kind in [
            JoinKind::Inner,
            JoinKind::LeftOuter,
            JoinKind::RightOuter,
            JoinKind::FullOuter,
        ] {
            for left_first in [true, false] {
                let input = millis(100);
                let mut operator = joiner(&input, &input, 0, 0, kind);
                let directory = tempfile::tempdir().unwrap();
                #[cfg(feature = "rocksdb-state")]
                if persistent {
                    operator = persistent_joiner(operator, directory.path(), input.schema());
                }
                #[cfg(not(feature = "rocksdb-state"))]
                if persistent {
                    continue;
                }
                operator.advance(1000).unwrap();
                let first = if left_first {
                    operator.push_left(input.clone(), None)
                } else {
                    operator.push_right(input.clone(), None)
                }
                .unwrap();
                let second = if left_first {
                    operator.push_right(input.clone(), None)
                } else {
                    operator.push_left(input.clone(), None)
                }
                .unwrap();
                let first_outer = if left_first {
                    kind.left_is_outer()
                } else {
                    kind.right_is_outer()
                };
                let second_outer = if left_first {
                    kind.right_is_outer()
                } else {
                    kind.left_is_outer()
                };
                assert_eq!(first.num_rows(), usize::from(first_outer));
                assert_eq!(second.num_rows(), usize::from(second_outer));
                if first_outer {
                    assert!(first.column(if left_first { 2 } else { 0 }).is_null(0));
                }
                if second_outer {
                    assert!(second.column(if left_first { 0 } else { 2 }).is_null(0));
                }
                assert_eq!(operator.advance(2000).unwrap().num_rows(), 0);
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
fn persistent_joiner(
    operator: IntervalJoiner,
    directory: &std::path::Path,
    schema: SchemaRef,
) -> IntervalJoiner {
    let config = RocksStoreConfig {
        table_dir: directory.join("db").to_string_lossy().into_owned(),
        max_parallelism: 128,
        options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
        ttl_ms: 0,
        shared_resources: 0,
    };
    operator.with_key_timestamp_precisions(vec![-1]).with_store(
        crate::state::RocksIntervalBuffer::create(config, schema.clone(), schema).unwrap(),
    )
}

#[test]
fn closed_arrivals_probe_live_state_before_retention_and_do_not_pad_matches() {
    for persistent in [false, true] {
        for left_first in [true, false] {
            let resident = millis(90);
            let arrival = millis(70);
            let mut operator = joiner(&resident, &arrival, -20, 30, JoinKind::FullOuter);
            let directory = tempfile::tempdir().unwrap();
            #[cfg(feature = "rocksdb-state")]
            if persistent {
                operator = persistent_joiner(operator, directory.path(), resident.schema());
            }
            #[cfg(not(feature = "rocksdb-state"))]
            if persistent {
                continue;
            }
            operator.advance(100).unwrap();
            let initial = if left_first {
                operator.push_left(resident, None)
            } else {
                operator.push_right(resident, None)
            }
            .unwrap();
            assert_eq!(initial.num_rows(), 0);
            let matched = if left_first {
                operator.push_right(arrival, None)
            } else {
                operator.push_left(arrival, None)
            }
            .unwrap();
            assert_eq!(matched.num_rows(), 1);
            assert_eq!(matched.column(0).null_count(), 0);
            assert_eq!(matched.column(2).null_count(), 0);
            assert_eq!(operator.advance(1000).unwrap().num_rows(), 0);
        }
    }
}

#[test]
fn initial_rowtime_frontier_and_restore_frontier_match_flink() {
    let input = millis(0);
    let mut operator = joiner(&input, &input, 0, 0, JoinKind::FullOuter);
    assert_eq!(
        operator.push_left(input.clone(), None).unwrap().num_rows(),
        1
    );
    assert_eq!(
        operator.push_right(input.clone(), None).unwrap().num_rows(),
        1
    );
    operator.advance(1000).unwrap();
    let snapshot = operator.snapshot();
    let input = millis(100);
    let mut restored = IntervalJoiner::restore(
        vec![0],
        vec![0],
        1,
        1,
        0,
        0,
        None,
        JoinKind::FullOuter,
        input.schema(),
        input.schema(),
        &snapshot,
    );
    assert_eq!(
        restored.push_left(input.clone(), None).unwrap().num_rows(),
        0
    );
    assert_eq!(restored.push_right(input, None).unwrap().num_rows(), 1);
}

#[test]
fn processing_time_arrivals_use_current_clock_and_strict_boundaries() {
    let input = millis(1);
    let mut operator = joiner(&input, &input, 0, 0, JoinKind::FullOuter);
    assert_eq!(
        operator
            .push_left(input.clone(), Some(100))
            .unwrap()
            .num_rows(),
        1
    );
    assert_eq!(
        operator
            .push_right(input.clone(), Some(100))
            .unwrap()
            .num_rows(),
        1
    );
    let mut operator = joiner(&input, &input, -20, 30, JoinKind::FullOuter);
    assert_eq!(
        operator
            .push_left(input.clone(), Some(100))
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(operator.push_right(input, Some(100)).unwrap().num_rows(), 1);
    assert_eq!(operator.advance(200).unwrap().num_rows(), 0);
}

#[test]
fn retained_horizon_equality_can_match_a_nonretained_arrival() {
    for persistent in [false, true] {
        let input = millis(100);
        let mut operator = joiner(&input, &input, 0, 0, JoinKind::FullOuter);
        let directory = tempfile::tempdir().unwrap();
        #[cfg(feature = "rocksdb-state")]
        if persistent {
            operator = persistent_joiner(operator, directory.path(), input.schema());
        }
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        assert_eq!(
            operator.push_left(input.clone(), None).unwrap().num_rows(),
            0
        );
        assert_eq!(operator.advance(100).unwrap().num_rows(), 0);
        let matches = operator.push_right(input, None).unwrap();
        assert_eq!(matches.num_rows(), 1);
        assert_eq!(matches.column(0).null_count(), 0);
        assert_eq!(operator.advance(101).unwrap().num_rows(), 0);
    }
}

#[test]
fn immediate_pads_and_matches_preserve_incoming_order() {
    for persistent in [false, true] {
        for incoming_left in [false, true] {
            let input = millis(90);
            let mut operator = joiner(&input, &input, -20, 30, JoinKind::FullOuter);
            let directory = tempfile::tempdir().unwrap();
            #[cfg(feature = "rocksdb-state")]
            if persistent {
                operator = persistent_joiner(operator, directory.path(), input.schema());
            }
            #[cfg(not(feature = "rocksdb-state"))]
            if persistent {
                continue;
            }
            operator.advance(100).unwrap();
            if incoming_left {
                operator.push_right(input, None).unwrap();
            } else {
                operator.push_left(input, None).unwrap();
            }
            let arrivals = batch(Arc::new(Int64Array::from(vec![1, 70, 2])));
            let output = if incoming_left {
                operator.push_left(arrivals, None)
            } else {
                operator.push_right(arrivals, None)
            }
            .unwrap();
            let times = output
                .column(if incoming_left { 1 } else { 3 })
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(times.values(), &[1, 70, 2]);
            let opposite = output.column(if incoming_left { 2 } else { 0 });
            assert!(opposite.is_null(0));
            assert!(!opposite.is_null(1));
            assert!(opposite.is_null(2));
            assert_eq!(output.num_columns(), 4);
        }
    }
}

#[test]
fn expired_arrival_does_not_consume_state_budget() {
    let input = millis(0);
    let mut operator = joiner(&input, &input, 0, 0, JoinKind::FullOuter)
        .with_memory_budget(0)
        .unwrap();
    assert_eq!(operator.push_left(input, None).unwrap().num_rows(), 1);
    assert!(operator.left_buffered.is_empty());
}

#[test]
fn first_cleanup_timer_survives_out_of_order_rows_and_backend_transitions() {
    for persistent in [false, true] {
        let input = millis(100);
        let mut original = joiner(&input, &input, 0, 0, JoinKind::FullOuter);
        let directory = tempfile::tempdir().unwrap();
        #[cfg(feature = "rocksdb-state")]
        if persistent {
            original = persistent_joiner(original, directory.path(), input.schema());
        }
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        original.push_left(input.clone(), None).unwrap();
        original.push_left(millis(90), None).unwrap();
        #[cfg(feature = "rocksdb-state")]
        let snapshots = if persistent {
            original.canonical_partitions().unwrap()
        } else {
            original.snapshot_partitions(128, &[-1])
        };
        #[cfg(not(feature = "rocksdb-state"))]
        let snapshots = original.snapshot_partitions(128, &[-1]);
        let snapshots = snapshots.into_values().collect::<Vec<_>>();
        for restore_persistent in [false, true] {
            let restored_directory = tempfile::tempdir().unwrap();
            let mut restored = IntervalJoiner::restore_partitions(
                vec![0],
                vec![0],
                1,
                1,
                0,
                0,
                None,
                JoinKind::FullOuter,
                input.schema(),
                input.schema(),
                &snapshots,
            );
            #[cfg(feature = "rocksdb-state")]
            if restore_persistent {
                restored = persistent_joiner(
                    joiner(&input, &input, 0, 0, JoinKind::FullOuter),
                    restored_directory.path(),
                    input.schema(),
                );
                restored.import_partitions(&snapshots, i64::MIN).unwrap();
            }
            #[cfg(not(feature = "rocksdb-state"))]
            if restore_persistent {
                continue;
            }
            assert_eq!(restored.advance(95).unwrap().num_rows(), 0);
            let output = restored.push_right(millis(90), None).unwrap();
            assert_eq!(output.num_rows(), 1);
            assert_eq!(output.column(0).null_count(), 0);
            let pads = restored.advance(101).unwrap();
            assert_eq!(
                pads.num_rows(),
                1,
                "source_persistent={persistent}, restore_persistent={restore_persistent}"
            );
            assert!(pads.column(2).is_null(0));
            assert_eq!(
                pads.column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                100
            );
        }
    }
}

#[test]
fn batched_arrivals_observe_eager_cleanup_after_the_first_probe() {
    for persistent in [false, true] {
        let input = millis(100);
        let mut operator = joiner(&input, &input, 0, 0, JoinKind::FullOuter);
        let directory = tempfile::tempdir().unwrap();
        #[cfg(feature = "rocksdb-state")]
        if persistent {
            operator = persistent_joiner(operator, directory.path(), input.schema());
        }
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        operator.push_left(input, None).unwrap();
        operator.push_left(millis(90), None).unwrap();
        operator.advance(95).unwrap();
        let incoming = batch(Arc::new(Int64Array::from(vec![90, 90])));
        let output = operator.push_right(incoming, None).unwrap();
        assert_eq!(output.num_rows(), 2);
        assert!(!output.column(0).is_null(0));
        assert!(output.column(0).is_null(1));
        assert_eq!(output.column(2).null_count(), 0);
    }
}

#[test]
fn global_probe_gate_is_replayed_between_keys_in_a_batch() {
    for persistent in [false, true] {
        let input = RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            ("rt", Arc::new(Int64Array::from(vec![-10, -10])) as ArrayRef),
        ])
        .unwrap();
        let mut operator = joiner(&input, &input, -20, 30, JoinKind::FullOuter);
        let directory = tempfile::tempdir().unwrap();
        #[cfg(feature = "rocksdb-state")]
        if persistent {
            operator = persistent_joiner(operator, directory.path(), input.schema());
        }
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        assert_eq!(operator.push_right(input, None).unwrap().num_rows(), 0);
        let incoming = RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(vec![1, 3, 2])) as ArrayRef),
            (
                "rt",
                Arc::new(Int64Array::from(vec![-20, 20, -20])) as ArrayRef,
            ),
        ])
        .unwrap();
        let output = operator.push_left(incoming, None).unwrap();
        assert_eq!(output.num_rows(), 2);
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 2]
        );
        assert!(output.column(2).is_null(0));
        assert!(!output.column(2).is_null(1));
    }
}

#[test]
fn timer_only_key_groups_survive_canonical_restore() {
    for persistent in [false, true] {
        let input = millis(100);
        let mut operator = joiner(&input, &input, 0, 0, JoinKind::FullOuter);
        let directory = tempfile::tempdir().unwrap();
        #[cfg(feature = "rocksdb-state")]
        if persistent {
            operator = persistent_joiner(operator, directory.path(), input.schema());
        }
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        // Eager cleanup can leave a scheduled timer after its row cache became empty.
        let mut encoder = BinaryRowBatchEncoder::new(&input, &[0], &[-1]);
        let key = ByteKey::from(encoder.encode(0));
        #[cfg(feature = "rocksdb-state")]
        if persistent {
            let group = operator.store_mut().key_group(hash_bytes_by_words(&key.0));
            operator
                .store_mut()
                .set_cleanup_timer(true, group, &key.0, Some(201))
                .unwrap();
        } else {
            operator.left_cleanup.insert(key.clone(), 201);
        }
        #[cfg(not(feature = "rocksdb-state"))]
        operator.left_cleanup.insert(key.clone(), 201);
        #[cfg(feature = "rocksdb-state")]
        let snapshots = if persistent {
            operator.canonical_partitions().unwrap()
        } else {
            operator.snapshot_partitions(128, &[-1])
        };
        #[cfg(not(feature = "rocksdb-state"))]
        let snapshots = operator.snapshot_partitions(128, &[-1]);
        assert!(!snapshots.is_empty());
        let snapshots = snapshots.into_values().collect::<Vec<_>>();
        let mut restored = IntervalJoiner::restore_partitions(
            vec![0],
            vec![0],
            1,
            1,
            0,
            0,
            None,
            JoinKind::FullOuter,
            input.schema(),
            input.schema(),
            &snapshots,
        );
        assert_eq!(restored.left_cleanup.get(key.0.as_ref()), Some(&201));
        assert_eq!(restored.advance(200).unwrap().num_rows(), 0);
        assert_eq!(restored.left_cleanup.get(key.0.as_ref()), Some(&201));
        assert_eq!(restored.advance(201).unwrap().num_rows(), 0);
        assert!(restored.left_cleanup.is_empty());
    }
}

#[test]
fn opposite_side_cleanup_timers_emit_in_deadline_order() {
    for persistent in [false, true] {
        let input = millis(100);
        let mut operator = joiner(&input, &input, 0, 0, JoinKind::FullOuter);
        let directory = tempfile::tempdir().unwrap();
        #[cfg(feature = "rocksdb-state")]
        if persistent {
            operator = persistent_joiner(operator, directory.path(), input.schema());
        }
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        operator.push_left(input, None).unwrap();
        operator.push_right(millis(90), None).unwrap();
        let pads = operator.advance(200).unwrap();
        assert_eq!(pads.num_rows(), 2);
        assert!(pads.column(0).is_null(0));
        assert_eq!(
            pads.column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            90
        );
        assert!(pads.column(2).is_null(1));
        assert_eq!(
            pads.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(1),
            100
        );
    }
}

#[test]
fn legacy_timestamp_precision_keys_regenerate_cleanup_timers() {
    let input = RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(TimestampNanosecondArray::from(vec![1_000_123_456])) as ArrayRef,
        ),
        ("rt", Arc::new(Int64Array::from(vec![100])) as ArrayRef),
    ])
    .unwrap();
    let mut original =
        joiner(&input, &input, 0, 0, JoinKind::FullOuter).with_key_timestamp_precisions(vec![9]);
    original.push_left(input.clone(), None).unwrap();
    let legacy = |bytes: Vec<u8>| {
        let sections = read_framed_sections(&bytes);
        IntervalJoiner::snapshot_parts([
            sections[0].clone(),
            sections[1].clone(),
            sections[2].clone(),
            sections[3].clone(),
        ])
    };
    let raw = legacy(original.snapshot());
    let partitions = original
        .snapshot_partitions(128, &[9])
        .into_values()
        .map(legacy)
        .collect::<Vec<_>>();
    for partitioned in [false, true] {
        let restored = if partitioned {
            IntervalJoiner::restore_partitions(
                vec![0],
                vec![0],
                1,
                1,
                0,
                0,
                None,
                JoinKind::FullOuter,
                input.schema(),
                input.schema(),
                &partitions,
            )
        } else {
            IntervalJoiner::restore(
                vec![0],
                vec![0],
                1,
                1,
                0,
                0,
                None,
                JoinKind::FullOuter,
                input.schema(),
                input.schema(),
                &raw,
            )
        };
        let mut restored = restored.with_key_timestamp_precisions(vec![9]);
        assert_eq!(restored.left_cleanup.len(), 1);
        assert_eq!(restored.advance(100).unwrap().num_rows(), 0);
        let pads = restored.advance(101).unwrap();
        assert_eq!(pads.num_rows(), 1, "partitioned={partitioned}");
        assert_eq!(pads.column(0), input.column(0));
        assert!(restored.left_cleanup.is_empty());
    }
}

#[test]
fn cleanup_silently_clears_a_nonpositive_surviving_timestamp() {
    for persistent in [false, true] {
        let input = millis(-50);
        let mut operator = joiner(&input, &input, -100, 100, JoinKind::FullOuter);
        let directory = tempfile::tempdir().unwrap();
        #[cfg(feature = "rocksdb-state")]
        if persistent {
            operator = persistent_joiner(operator, directory.path(), input.schema());
        }
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        operator.push_left(input, None).unwrap();
        operator.push_left(millis(0), None).unwrap();
        let pads = operator.advance(51).unwrap();
        assert_eq!(pads.num_rows(), 1);
        assert_eq!(
            pads.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            -50
        );
        // Stock removeExpiredRows clears its entire cache when the surviving earliest time is zero.
        assert_eq!(operator.push_right(millis(0), None).unwrap().num_rows(), 0);
        assert_eq!(operator.advance(101).unwrap().num_rows(), 1);
    }
}
