use super::*;

pub(crate) fn retracting_batch(rows: &[(i64, i64, Option<i64>, i8)]) -> RecordBatch {
    let values: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.2).collect::<Vec<_>>(),
    ));
    RecordBatch::try_from_iter(vec![
        (
            "ts",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "key0",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        ("value0", values.clone()),
        ("value1", values),
        (
            "value2",
            Arc::new(Int64Array::from_value(1, rows.len())) as ArrayRef,
        ),
        (
            ROW_KIND_COLUMN,
            Arc::new(Int8Array::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn retracting_windows_preserve_signed_partials_liveness_and_late_changes_after_restore() {
    let kinds = vec![RETRACT_SUM, RETRACT_COUNT, HIDDEN_LIVE_COUNT];
    for (size, cumulative) in [(5000, false), (10000, false), (15000, true)] {
        let create = |size, cumulative| {
            TumblingAggregator::new(size, 5000, cumulative, vec![0; 3], kinds.clone())
        };
        let mut single = create(size, cumulative);
        let mut local = create(5000, false);
        let mut global = create(size, cumulative);
        for rows in [
            vec![
                (1000, 1, Some(10), 0),
                (4000, 2, Some(7), 0),
                (2000, 3, None, 0),
                (1000, 4, Some(5), 0),
                (1000, 4, Some(5), 0),
            ],
            vec![
                (1000, 1, Some(10), 1),
                (2000, 1, Some(20), 2),
                (4000, 2, Some(7), 1),
                (6000, 2, Some(8), 2),
                (1000, 4, Some(5), 3),
                (1000, 5, Some(3), 3),
            ],
            vec![(1000, 4, Some(5), 3)],
        ] {
            let batch = retracting_batch(&rows);
            single.update(&batch).unwrap();
            local.update_local(&batch).unwrap();
            global.update_partial(&local.drain_partial()).unwrap();
            assert!(local.windows.is_empty());
            single = TumblingAggregator::restore(
                size,
                5000,
                cumulative,
                vec![0; 3],
                kinds.clone(),
                &single.snapshot(),
            );
            let partitions = global.snapshot_partitions(128, &[-1]);
            global = TumblingAggregator::restore_partitions(
                size,
                5000,
                cumulative,
                vec![0; 3],
                kinds.clone(),
                &partitions.into_values().collect::<Vec<_>>(),
            );
        }
        let first = single.flush(5000).unwrap();
        assert_eq!(first, global.flush(5000).unwrap());
        assert_eq!(column_i64(&first, "key0").values(), &[1, 3, 5]);
        assert_eq!(
            column_i64(&first, "result0").iter().collect::<Vec<_>>(),
            vec![Some(20), None, Some(-3)]
        );
        assert_eq!(column_i64(&first, "result1").values(), &[1, 0, -1]);
        assert_eq!(column_i64(&first, "result2").values(), &[1, 1, -1]);
        local = TumblingAggregator::restore(
            5000,
            5000,
            false,
            vec![0; 3],
            kinds.clone(),
            &local.snapshot(),
        );
        global = TumblingAggregator::restore(
            size,
            5000,
            cumulative,
            vec![0; 3],
            kinds.clone(),
            &global.snapshot(),
        );
        let late = retracting_batch(&[(2000, 1, Some(20), 1), (2000, 1, Some(30), 2)]);
        single.update(&late).unwrap();
        local.update_local(&late).unwrap();
        global.update_partial(&local.flush_partial(7000)).unwrap();
        let rest = single.flush(i64::MAX).unwrap();
        assert_eq!(rest, global.flush(i64::MAX).unwrap());
        for row in 0..rest.num_rows() {
            let key = column_i64(&rest, "key0").value(row);
            assert_ne!(key, 4, "a group with no live rows must disappear");
            if key == 1 {
                assert_eq!(column_i64(&rest, "result0").value(row), 30);
            }
        }
        global.update_partial(&local.drain_partial()).unwrap();
        assert_eq!(global.flush(i64::MAX).unwrap().num_rows(), 0);
        assert!(single.windows.is_empty());
        assert!(global.windows.is_empty());
    }
}

#[test]
fn distinct_window_partials_restore_union_and_expire() {
    fn batch(values: Vec<Option<&str>>) -> RecordBatch {
        let rows = values.len();
        RecordBatch::try_from_iter(vec![
            (
                "ts",
                Arc::new(Int64Array::from_value(500, rows)) as ArrayRef,
            ),
            (
                "key0",
                Arc::new(Int64Array::from_value(7, rows)) as ArrayRef,
            ),
            ("value0", Arc::new(StringArray::from(values)) as ArrayRef),
            (
                "value1",
                Arc::new(Int64Array::from_value(1, rows)) as ArrayRef,
            ),
        ])
        .unwrap()
    }
    let mut local = TumblingAggregator::new(1000, 1000, false, vec![3, 0], vec![7, 3]);
    let mut global = TumblingAggregator::new(2000, 1000, false, vec![3, 0], vec![7, 3]);
    local
        .update(&batch(vec![Some("a"), Some("a"), None, Some("b")]))
        .unwrap();
    let partial = local.drain_partial();
    assert!(local.windows.is_empty());
    global.update_partial(&partial).unwrap();
    // A second local worker/barrier contributes the same values, which must not add to cardinality.
    global.update_partial(&partial).unwrap();
    let snapshot = global.snapshot();
    let mut restored =
        TumblingAggregator::restore(2000, 1000, false, vec![3, 0], vec![7, 3], &snapshot);
    let partitions = restored.snapshot_partitions(128, &[-1]);
    let mut restored = TumblingAggregator::restore_partitions(
        2000,
        1000,
        false,
        vec![3, 0],
        vec![7, 3],
        &partitions.into_values().collect::<Vec<_>>(),
    );
    local
        .update(&batch(vec![Some("b"), Some("c"), None]))
        .unwrap();
    let final_partial = local.drain_partial();
    restored.update_partial(&final_partial).unwrap();
    let output = restored.flush(2000).unwrap();
    assert_eq!(column_i64(&output, "result0").values(), &[3, 3]);
    assert_eq!(column_i64(&output, "result1").values(), &[11, 11]);
    assert!(restored.windows.is_empty());
    let closed = restored.snapshot();
    let mut restored =
        TumblingAggregator::restore(2000, 1000, false, vec![3, 0], vec![7, 3], &closed);
    restored.update(&batch(vec![Some("late")])).unwrap();
    assert_eq!(restored.late_drops, 1);
    assert_eq!(restored.flush(i64::MAX).unwrap().num_rows(), 0);
    assert!(restored.windows.is_empty());
}

#[test]
fn snapshot_preserves_live_string_distinct_state() {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts",
            Arc::new(Int64Array::from(vec![500i64, 500])) as ArrayRef,
        ),
        (
            "key0",
            Arc::new(Int64Array::from(vec![7i64, 7])) as ArrayRef,
        ),
        (
            "value0",
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut aggregator = TumblingAggregator::new(1000, 1000, false, vec![3], vec![7]);
    aggregator.update(&batch).unwrap();

    let _snapshot = aggregator.snapshot();
    let output = aggregator.flush(1000).unwrap();

    assert_eq!(column_i64(&output, "result0").values(), &[2]);
}

#[test]
fn snapshot_preserves_live_builtin_aggregate_state() {
    fn batch(values: &[i64]) -> RecordBatch {
        let values = values.to_vec();
        RecordBatch::try_from_iter(vec![
            (
                "ts",
                Arc::new(Int64Array::from(vec![500; values.len()])) as ArrayRef,
            ),
            (
                "key0",
                Arc::new(Int64Array::from_value(7, values.len())) as ArrayRef,
            ),
            (
                "value0",
                Arc::new(Int64Array::from(values.clone())) as ArrayRef,
            ),
            (
                "value1",
                Arc::new(Int64Array::from(values.clone())) as ArrayRef,
            ),
            ("value2", Arc::new(Int64Array::from(values)) as ArrayRef),
        ])
        .unwrap()
    }

    let mut aggregator = TumblingAggregator::new(1000, 1000, false, vec![0, 0, 0], vec![3, 0, 4]);
    aggregator.update(&batch(&[1, 4])).unwrap();
    let _snapshot = aggregator.snapshot();
    aggregator.update(&batch(&[10])).unwrap();

    let output = aggregator.flush(1000).unwrap();
    assert_eq!(column_i64(&output, "result0").values(), &[3]);
    assert_eq!(column_i64(&output, "result1").values(), &[15]);
    assert_eq!(column_i64(&output, "result2").values(), &[5]);
}

#[test]
fn fixed_offset_assignment_preserves_payload_and_instant_window_time() {
    use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
    let values = vec![
        Some(TimestampValue::new(-1, 999_999).unwrap()),
        Some(TimestampValue::new(0, 123_456).unwrap()),
        Some(TimestampValue::from_millis(253_402_300_790_000)),
        None,
    ];
    let input = RecordBatch::try_from_iter(vec![(
        "ts",
        Arc::new(timestamp_array(values.clone())) as ArrayRef,
    )])
    .unwrap();
    for offset in [0, 28_800_000, -19_800_000] {
        for (slide, cumulative) in [(10_000, false), (5_000, false), (5_000, true)] {
            let assigned = assign_windows(
                &input,
                0,
                10_000,
                slide,
                cumulative,
                None,
                &streamfusion_bridge::timestamp::timestamp_type(),
                offset,
            )
            .unwrap();
            let payload = TimestampColumn::try_new(assigned.column(0).as_ref()).unwrap();
            let starts = TimestampColumn::try_new(assigned.column(1).as_ref())
                .unwrap()
                .to_millis()
                .unwrap();
            let ends = TimestampColumn::try_new(assigned.column(2).as_ref())
                .unwrap()
                .to_millis()
                .unwrap();
            let times = TimestampColumn::try_new(assigned.column(3).as_ref())
                .unwrap()
                .to_millis()
                .unwrap();
            for row in 0..assigned.num_rows() {
                let value = payload.value(row).unwrap();
                assert!(values.contains(&Some(value)));
                assert!(starts.value(row) <= value.millis() + offset);
                assert!(value.millis() + offset < ends.value(row));
                assert_eq!(times.value(row), ends.value(row) - offset - 1);
                assert_eq!(starts.value(row) % slide, 0);
            }
        }
    }
}
use arrow::array::TimestampSecondArray;
use arrow::datatypes::TimeUnit;

fn input(times: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from_value(7, times.len())) as ArrayRef,
        ),
        ("rt", times),
        (
            ROW_KIND_COLUMN,
            Arc::new(Int8Array::from_value(0, 3)) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn assignment_reads_all_layouts_and_keeps_payload_and_changelog() {
    let times: Vec<ArrayRef> = vec![
        Arc::new(TimestampSecondArray::from(vec![Some(-1), None, Some(1)])),
        Arc::new(TimestampMillisecondArray::from(vec![
            Some(-1),
            None,
            Some(1000),
        ])),
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(-1),
            None,
            Some(1_000_001),
        ])),
        Arc::new(TimestampNanosecondArray::from(vec![
            Some(-1),
            None,
            Some(1_000_000_001),
        ])),
    ];
    for times in times {
        let batch = input(times);
        for (size, step, cumulative) in
            [(1000, 1000, false), (2000, 1000, false), (2000, 1000, true)]
        {
            let out = assign_windows(
                &batch,
                1,
                size,
                step,
                cumulative,
                None,
                &DataType::Timestamp(TimeUnit::Nanosecond, None),
                0,
            )
            .unwrap();
            let expected_indices = if cumulative || size == step {
                vec![0, 2]
            } else {
                vec![0, 0, 2, 2]
            };
            let original =
                take(batch.column(1), &UInt32Array::from(expected_indices), None).unwrap();
            assert_eq!(out.column(1).to_data(), original.to_data());
            assert_eq!(out.schema().field(5).name(), ROW_KIND_COLUMN);
            for column in out.columns() {
                column.to_data().validate_full().unwrap();
            }
            let starts = TimestampColumn::try_new(out.column(2).as_ref())
                .unwrap()
                .to_millis()
                .unwrap();
            let ends = TimestampColumn::try_new(out.column(3).as_ref())
                .unwrap()
                .to_millis()
                .unwrap();
            assert_eq!(starts.value(0), if cumulative { -2000 } else { -1000 });
            assert_eq!(
                ends.value(0),
                if size == 2000 && !cumulative { 1000 } else { 0 }
            );
            let window_time = TimestampColumn::try_new(out.column(4).as_ref()).unwrap();
            for row in 0..out.num_rows() {
                assert_eq!(
                    window_time.value(row).unwrap().millis(),
                    ends.value(row) - 1
                );
                assert_eq!(window_time.value(row).unwrap().nano_of_milli(), 0);
            }
        }
    }
}

#[test]
fn boundary_output_layout_is_explicit_and_never_wraps() {
    let batch = input(Arc::new(TimestampNanosecondArray::from(vec![-1; 3])));
    for unit in [
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        let ty = DataType::Timestamp(unit, None);
        let out = assign_windows(&batch, 1, 1000, 1000, false, None, &ty, 0).unwrap();
        assert_eq!(out.column(2).data_type(), &ty);
        assert_eq!(
            TimestampColumn::try_new(out.column(4).as_ref())
                .unwrap()
                .value(0)
                .unwrap()
                .millis(),
            -1
        );
    }
    assert!(assign_windows(
        &batch,
        1,
        1000,
        1000,
        false,
        None,
        &DataType::Timestamp(TimeUnit::Second, None),
        0
    )
    .is_err());
    let wide = input(Arc::new(TimestampMillisecondArray::from(
        vec![10_000_000_000_000; 3],
    )));
    assert!(assign_windows(
        &wide,
        1,
        1000,
        1000,
        false,
        None,
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
        0
    )
    .is_err());
}

#[test]
fn proctime_assignment_ignores_null_payload_time() {
    let batch = input(Arc::new(TimestampNanosecondArray::new_null(3)));
    let out = assign_windows(
        &batch,
        1,
        1000,
        1000,
        false,
        Some(-1),
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
        0,
    )
    .unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(out.column(1).null_count(), 3);
    assert_eq!(
        TimestampColumn::try_new(out.column(2).as_ref())
            .unwrap()
            .value(0)
            .unwrap()
            .millis(),
        -1000
    );
}

#[test]
fn tvf_output_survives_downstream_window_join_restore() {
    let millis: i64 = -1;
    let batch = input(Arc::new(TimestampNanosecondArray::from(vec![
        millis * 1_000_000
            + 999999;
        3
    ])))
    .slice(1, 1);
    let assigned = assign_windows(
        &batch,
        1,
        1000,
        1000,
        false,
        None,
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
        0,
    )
    .unwrap();
    // Drop only the changelog sidecar, as the operator wrapper does before joining.
    let assigned = assigned.project(&[0, 1, 2, 3, 4]).unwrap();
    let schema = assigned.schema();
    let mut joiner = WindowJoiner::new(
        vec![0],
        vec![0],
        2,
        3,
        2,
        3,
        None,
        JoinKind::Inner,
        schema.clone(),
        schema.clone(),
    );
    joiner.push_left(assigned.clone()).unwrap();
    joiner.push_right(assigned.clone()).unwrap();
    let mut restored = WindowJoiner::restore(
        vec![0],
        vec![0],
        2,
        3,
        2,
        3,
        None,
        JoinKind::Inner,
        schema.clone(),
        schema,
        &joiner.snapshot(),
    );
    let end = millis - millis.rem_euclid(1000) + 1000;
    assert_eq!(restored.flush(end - 1).unwrap().num_rows(), 0);
    let out = restored.flush(end).unwrap();
    assert_eq!(out.num_rows(), 1);
    for column in [1, 6] {
        let time = TimestampColumn::try_new(out.column(column).as_ref())
            .unwrap()
            .value(0)
            .unwrap();
        assert_eq!((time.millis(), time.nano_of_milli()), (millis, 999999));
    }
    restored.push_left(assigned).unwrap();
    assert_eq!(restored.left_late_drops, 1);
}

#[test]
fn local_late_slices_update_only_unfired_final_windows_after_restore() {
    fn batch(value: i64) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("ts", Arc::new(Int64Array::from(vec![4000])) as ArrayRef),
            ("key0", Arc::new(Int64Array::from(vec![7])) as ArrayRef),
            (
                "value0",
                Arc::new(Int64Array::from(vec![value])) as ArrayRef,
            ),
            (
                "value1",
                Arc::new(Int64Array::from(vec![value])) as ArrayRef,
            ),
        ])
        .unwrap()
    }
    for (size, cumulative) in [(5000, false), (10000, false), (15000, true)] {
        let mut local = TumblingAggregator::new(5000, 5000, false, vec![0, 0], vec![0, 7]);
        let mut global = TumblingAggregator::new(size, 5000, cumulative, vec![0, 0], vec![0, 7]);
        local.update_local(&batch(10)).unwrap();
        global.update_partial(&local.flush_partial(5000)).unwrap();
        let first = global.flush(5000).unwrap();
        assert_eq!(column_i64(&first, "result0").values(), &[10]);
        assert_eq!(column_i64(&first, "result1").values(), &[1]);
        let mut local = TumblingAggregator::restore(
            5000,
            5000,
            false,
            vec![0, 0],
            vec![0, 7],
            &local.snapshot(),
        );
        let mut global = TumblingAggregator::restore(
            size,
            5000,
            cumulative,
            vec![0, 0],
            vec![0, 7],
            &global.snapshot(),
        );
        local.update_local(&batch(11)).unwrap();
        assert_eq!(local.late_drops, 0);
        let partial = local.flush_partial(7000);
        assert_eq!(
            partial.num_rows(),
            1,
            "a closed slice is not a closed final window"
        );
        global.update_partial(&partial).unwrap();
        assert_eq!(
            global.flush(7000).unwrap().num_rows(),
            0,
            "the fired window must not reopen"
        );
        let output = global.flush(size).unwrap();
        let remaining = (size / 5000 - 1) as usize;
        assert_eq!(output.num_rows(), remaining);
        assert_eq!(
            column_i64(&output, "result0").values().as_ref(),
            vec![21; remaining].as_slice()
        );
        assert_eq!(
            column_i64(&output, "result1").values().as_ref(),
            vec![2; remaining].as_slice()
        );
        global.update_partial(&partial).unwrap();
        assert_eq!(
            global.flush(i64::MAX).unwrap().num_rows(),
            0,
            "fully expired partials must be dropped"
        );
    }
}
