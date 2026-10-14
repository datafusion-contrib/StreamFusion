use super::*;

#[test]
fn timestamp_panes_match_direct_windows_across_boundaries_late_rows_and_restore() {
    let early = TimestampValue::new(-62_135_596_800_000, 1).unwrap();
    let late = TimestampValue::new(253_402_300_799_999, 999_999).unwrap();
    let batch = |times: Vec<i64>| {
        let values: ArrayRef = Arc::new(timestamp_array(times.iter().enumerate().map(
            |(row, _)| match row % 4 {
                0 => Some(early),
                1 => Some(late),
                _ => None,
            },
        )));
        RecordBatch::try_from_iter(vec![
            ("ts", Arc::new(Int64Array::from(times.clone())) as ArrayRef),
            (
                "key0",
                Arc::new(StringArray::from_iter(times.iter().enumerate().map(
                    |(row, _)| {
                        if row % 3 == 0 {
                            None
                        } else {
                            Some(if row % 2 == 0 { "A" } else { "B" })
                        }
                    },
                ))) as ArrayRef,
            ),
            ("value0", values.clone()),
            ("value1", values.clone()),
            (
                "value2",
                Arc::new(Int64Array::from_value(1, times.len())) as ArrayRef,
            ),
            ("value3", values),
        ])
        .unwrap()
    };
    let types = vec![1009, 1009, 0, 1009];
    let kinds = vec![1, 2, 3, 3];
    for offset in [0, 123, -345, 20_700_123] {
        for cumulative in [false, true] {
            let mut panes =
                TumblingAggregator::new(10_000, 2000, cumulative, types.clone(), kinds.clone())
                    .with_memory_budget(1024 * 1024)
                    .unwrap();
            let mut direct =
                TumblingAggregator::new(10_000, 2000, cumulative, types.clone(), kinds.clone());
            panes.set_assignment_offset(offset);
            direct.set_assignment_offset(offset);
            direct.exact_panes = false;
            for times in [
                vec![-2001, -2000, -1, 0, 1, 1999, 2000, 7999, 8000, 9999],
                vec![0, 3000, 8000, 9999, 10_000, 10_001, 15_999, 16_000],
            ] {
                let input = batch(times);
                panes.update(&input).unwrap();
                assert_eq!(panes.memory.state_bytes, panes.computed_state_bytes());
                direct.update(&input).unwrap();
                assert_eq!(panes.flush(3999).unwrap(), direct.flush(3999).unwrap());
                assert_eq!(panes.late_drops, direct.late_drops);
                let restore = |operator: &mut TumblingAggregator| {
                    TumblingAggregator::restore(
                        10_000,
                        2000,
                        cumulative,
                        types.clone(),
                        kinds.clone(),
                        &operator.snapshot(),
                    )
                };
                panes = restore(&mut panes);
                panes = panes.with_memory_budget(1024 * 1024).unwrap();
                direct = restore(&mut direct);
                direct.exact_panes = false;
            }
            assert_eq!(
                panes.flush(i64::MAX).unwrap(),
                direct.flush(i64::MAX).unwrap()
            );
            assert_eq!(panes.flush(i64::MAX).unwrap().num_rows(), 0);
            assert_eq!(panes.memory.state_bytes, 0);
        }
    }
}

#[test]
fn temporal_sessions_bridge_restored_values_without_retaining_source_batches() {
    use streamfusion_bridge::timestamp::{timestamp_array, TimestampColumn, TimestampValue};
    let early = TimestampValue::new(-62_135_596_800_000, 1).unwrap();
    let middle = TimestampValue::new(-1, 999_999).unwrap();
    let late = TimestampValue::new(253_402_300_799_999, 999_999).unwrap();
    let types = vec![1009, 1009];
    let kinds = vec![1, 2];
    let batch = |times: Vec<i64>, values: Vec<Option<TimestampValue>>| {
        let values: ArrayRef = Arc::new(timestamp_array(values));
        RecordBatch::try_from_iter(vec![
            ("ts", Arc::new(Int64Array::from(times)) as ArrayRef),
            (
                "key0",
                Arc::new(Int64Array::from(vec![1, 1, 2])) as ArrayRef,
            ),
            ("value0", values.clone()),
            ("value1", values),
        ])
        .unwrap()
    };
    let mut window = SessionAggregator::new(1000, types.clone(), kinds.clone());
    window
        .update(&batch(
            vec![100, 2100, 100],
            vec![Some(middle), Some(early), None],
        ))
        .unwrap();
    let snapshots = window
        .snapshot_partitions(128, &[-1])
        .into_values()
        .collect::<Vec<_>>();
    let mut restored = SessionAggregator::restore_partitions(1000, types, kinds, &snapshots);
    restored
        .update(&batch(vec![1100, 1200, 100], vec![Some(late), None, None]))
        .unwrap();
    let output = restored.flush(4000).unwrap();
    assert_eq!(output.num_rows(), 2);
    let minimum =
        TimestampColumn::try_new(output.column_by_name("result0").unwrap().as_ref()).unwrap();
    let maximum =
        TimestampColumn::try_new(output.column_by_name("result1").unwrap().as_ref()).unwrap();
    assert_eq!(column_i64(&output, "key0").values(), &[1, 2]);
    assert_eq!(minimum.value(0).unwrap(), early);
    assert_eq!(maximum.value(0).unwrap(), late);
    assert!(minimum.is_null(1));
    assert!(maximum.is_null(1));
    assert_eq!(column_i64(&output, "window_start").values(), &[100, 100]);
    assert_eq!(column_i64(&output, "window_end").values(), &[3100, 1100]);
    assert_eq!(restored.flush(4000).unwrap().num_rows(), 0);
}

#[test]
fn timestamp_extrema_partials_and_snapshots_keep_fractional_null_state() {
    use streamfusion_bridge::timestamp::{timestamp_array, TimestampColumn, TimestampValue};
    let early = TimestampValue::new(-62_135_596_800_000, 1).unwrap();
    let late = TimestampValue::new(253_402_300_799_999, 999_999).unwrap();
    let low = TimestampValue::new(-1, 999_998).unwrap();
    let high = TimestampValue::new(-1, 999_999).unwrap();
    let kinds = vec![1, 2, 3, 1, 2];
    let types = vec![1009, 1009, 0, 3, 3];
    let create = || TumblingAggregator::new(1000, 1000, false, types.clone(), kinds.clone());
    let mut single = create();
    let mut local = create();
    let mut global = create();
    for rows in [
        vec![(1, Some(high)), (2, None), (3, Some(high)), (1, None)],
        vec![(1, Some(early)), (1, Some(late)), (2, None), (3, Some(low))],
    ] {
        let rows: Vec<_> = [(-99, None)]
            .into_iter()
            .chain(rows)
            .chain([(-99, None)])
            .collect();
        let values: ArrayRef = Arc::new(timestamp_array(rows.iter().map(|row| row.1)));
        let strings: ArrayRef = Arc::new(StringArray::from(
            rows.iter()
                .map(|row| {
                    row.1.map(|value| {
                        if value == early {
                            "a\0"
                        } else if value == late {
                            "\u{e000}"
                        } else if value == low {
                            ""
                        } else {
                            "z"
                        }
                    })
                })
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_from_iter(vec![
            (
                "ts",
                Arc::new(Int64Array::from_value(100, rows.len())) as ArrayRef,
            ),
            (
                "key0",
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))) as ArrayRef,
            ),
            ("value0", values.clone()),
            ("value1", values),
            (
                "value2",
                Arc::new(Int64Array::from_value(1, rows.len())) as ArrayRef,
            ),
            ("value3", strings.clone()),
            ("value4", strings),
        ])
        .unwrap();
        let batch = batch.slice(1, rows.len() - 2);
        single.update(&batch).unwrap();
        local.update_local(&batch).unwrap();
        global.update_partial(&local.drain_partial()).unwrap();
        single = TumblingAggregator::restore(
            1000,
            1000,
            false,
            types.clone(),
            kinds.clone(),
            &single.snapshot(),
        );
        let partitions = global.snapshot_partitions(128, &[-1]);
        global = TumblingAggregator::restore_partitions(
            1000,
            1000,
            false,
            types.clone(),
            kinds.clone(),
            &partitions.into_values().collect::<Vec<_>>(),
        );
    }
    let result = single.flush(i64::MAX).unwrap();
    assert_eq!(result, global.flush(i64::MAX).unwrap());
    let keys = column_i64(&result, "key0");
    let minimum =
        TimestampColumn::try_new(result.column_by_name("result0").unwrap().as_ref()).unwrap();
    let maximum =
        TimestampColumn::try_new(result.column_by_name("result1").unwrap().as_ref()).unwrap();
    let string_column = |name| {
        result
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    };
    let string_min = string_column("result3");
    let string_max = string_column("result4");
    for row in 0..result.num_rows() {
        match keys.value(row) {
            1 => {
                assert_eq!(minimum.value(row).unwrap(), early);
                assert_eq!(maximum.value(row).unwrap(), late);
                assert_eq!(column_i64(&result, "result2").value(row), 4);
                assert_eq!(string_min.value(row), "a\0");
                assert_eq!(string_max.value(row), "\u{e000}");
            }
            2 => {
                assert!(minimum.is_null(row));
                assert!(maximum.is_null(row));
                assert_eq!(column_i64(&result, "result2").value(row), 2);
                assert!(string_min.is_null(row));
                assert!(string_max.is_null(row));
            }
            3 => {
                assert_eq!(minimum.value(row).unwrap(), low);
                assert_eq!(maximum.value(row).unwrap(), high);
                assert_eq!(column_i64(&result, "result2").value(row), 2);
                assert_eq!(string_min.value(row), "");
                assert_eq!(string_max.value(row), "z");
            }
            _ => panic!("unexpected key"),
        }
    }
}

#[test]
fn local_buffer_readiness_drains_future_slices_without_advancing_lateness() {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts",
            Arc::new(Int64Array::from(vec![100, 2100])) as ArrayRef,
        ),
        ("value0", Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef),
    ])
    .unwrap();
    let mut local = TumblingAggregator::new(1000, 1000, false, vec![0], vec![3]);
    local.update_local(&batch).unwrap();
    assert!(!local.local_buffer_ready(998));
    assert!(local.local_buffer_ready(999));
    assert_eq!(local.current_watermark, i64::MIN);
    let partials = local.drain_partial();
    assert_eq!(column_i64(&partials, "slice_end").values(), &[1000, 3000]);
    assert!(!local.local_buffer_ready(i64::MAX));
    assert_eq!(local.current_watermark, i64::MIN);
    local.update_local(&batch).unwrap();
    assert_eq!(local.drain_partial(), partials);
}

#[test]
fn basic_value_partials_keep_extrema_and_distinct_sets_across_checkpoint_restore() {
    use arrow::array::{BooleanArray, StringArray, Time32MillisecondArray};
    use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
    let timestamp = |millis, nanos| TimestampValue::new(millis, nanos).unwrap();
    let columns: Vec<(i64, ArrayRef, Vec<i64>)> = vec![
        (
            0,
            Arc::new(Int64Array::from(vec![None, Some(i64::MAX), Some(1), None])),
            vec![22],
        ),
        (
            5803,
            Arc::new(
                Decimal128Array::from(vec![None, Some(DECIMAL128_MAX - 1), Some(1), Some(-1)])
                    .with_precision_and_scale(38, 3)
                    .unwrap(),
            ),
            vec![22],
        ),
        (
            3,
            Arc::new(StringArray::from(vec![
                Some("z"),
                Some(""),
                Some("é😀"),
                None,
            ])),
            vec![1, 2],
        ),
        (
            1009,
            Arc::new(timestamp_array([
                Some(timestamp(253_402_300_799_999, 999_999)),
                Some(timestamp(-62_135_596_800_000, 1)),
                Some(timestamp(0, 2)),
                None,
            ])),
            vec![1, 2],
        ),
        (
            10,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                None,
            ])),
            vec![7],
        ),
        (
            9,
            Arc::new(Time32MillisecondArray::from(vec![
                Some(1),
                Some(999),
                Some(1),
                None,
            ])),
            vec![7],
        ),
    ];
    for (code, values, kinds) in columns {
        let mut fields = vec![
            ("ts", Arc::new(Int64Array::from(vec![100; 4])) as ArrayRef),
            ("key0", Arc::new(Int64Array::from(vec![0; 4])) as ArrayRef),
        ];
        fields.push(("value0", values.clone()));
        if kinds.len() == 2 {
            fields.push(("value1", values));
        }
        let batch = RecordBatch::try_from_iter(fields).unwrap();
        for (size, slide, cumulative) in
            [(1000, 1000, false), (1000, 500, false), (1000, 500, true)]
        {
            let create = || {
                TumblingAggregator::new(
                    size,
                    slide,
                    cumulative,
                    vec![code; kinds.len()],
                    kinds.clone(),
                )
            };
            let mut single = create();
            let mut local = TumblingAggregator::new(
                slide,
                slide,
                false,
                vec![code; kinds.len()],
                kinds.clone(),
            );
            let mut global = create();
            single.update(&batch).unwrap();
            local.update_local(&batch).unwrap();
            global.update_partial(&local.drain_partial()).unwrap();
            global = TumblingAggregator::restore(
                size,
                slide,
                cumulative,
                vec![code; kinds.len()],
                kinds.clone(),
                &global.snapshot(),
            );
            let expected = single.flush(1000).unwrap();
            let actual = global.flush(1000).unwrap();
            assert_eq!(actual, expected, "value code {code}");
            assert!(actual.num_rows() > 0);
            if kinds == [7] {
                assert_eq!(
                    actual
                        .column_by_name("result0")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    2
                );
            }
        }
    }
}

#[test]
fn string_extrema_partials_preserve_nulls_utf8_order_and_keyed_restore() {
    fn batch(continuation: bool) -> RecordBatch {
        let values: ArrayRef = Arc::new(StringArray::from(if continuation {
            vec![Some("\u{1f600}"), None, Some("aaa")]
        } else {
            vec![Some("\u{e000}"), None, Some("")]
        }));
        RecordBatch::try_from_iter(vec![
            ("ts", Arc::new(Int64Array::from_value(500, 3)) as ArrayRef),
            (
                "key0",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
            ("value0", values.clone()),
            ("value1", values),
            ("value2", Arc::new(Int64Array::from_value(1, 3)) as ArrayRef),
        ])
        .unwrap()
    }
    let types = vec![3, 3, 0];
    let kinds = vec![1, 2, 3];
    for (size, cumulative) in [(1000, false), (3000, false), (3000, true)] {
        let mut single =
            TumblingAggregator::new(size, 1000, cumulative, types.clone(), kinds.clone());
        let mut local = TumblingAggregator::new(1000, 1000, false, types.clone(), kinds.clone());
        let mut global =
            TumblingAggregator::new(size, 1000, cumulative, types.clone(), kinds.clone());
        for continuation in [false, true] {
            let input = batch(continuation);
            single.update(&input).unwrap();
            local.update_local(&input).unwrap();
            local = TumblingAggregator::restore(
                1000,
                1000,
                false,
                types.clone(),
                kinds.clone(),
                &local.snapshot(),
            );
            global.update_partial(&local.drain_partial()).unwrap();
            let partitions = global.snapshot_partitions(128, &[-1]);
            global = TumblingAggregator::restore_partitions(
                size,
                1000,
                cumulative,
                types.clone(),
                kinds.clone(),
                &partitions.into_values().collect::<Vec<_>>(),
            );
        }
        let output = global.flush(i64::MAX).unwrap();
        assert_eq!(output, single.flush(i64::MAX).unwrap());
        assert_eq!(output.num_rows(), if size == 1000 { 3 } else { 9 });
        let min = output
            .column_by_name("result0")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let max = output
            .column_by_name("result1")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..output.num_rows() {
            assert_eq!(column_i64(&output, "result2").value(row), 2);
            match column_i64(&output, "key0").value(row) {
                1 => {
                    assert_eq!(min.value(row), "\u{e000}");
                    assert_eq!(max.value(row), "\u{1f600}");
                }
                2 => {
                    assert!(min.is_null(row));
                    assert!(max.is_null(row));
                }
                3 => {
                    assert_eq!(min.value(row), "");
                    assert_eq!(max.value(row), "aaa");
                }
                _ => panic!("unexpected group"),
            }
        }
    }
}

#[test]
fn zero_sum_windows_preserve_wrapping_nulls_liveness_and_restored_partials() {
    for (code, max) in [
        (0, i64::MAX),
        (2, i32::MAX as i64),
        (4, i16::MAX as i64),
        (5, i8::MAX as i64),
    ] {
        for (size, cumulative) in [(1000, false), (2000, false), (3000, true)] {
            let kinds = vec![22, RETRACT_COUNT, HIDDEN_LIVE_COUNT];
            let types = vec![code, code, 0];
            let create = |width, cumulative| {
                TumblingAggregator::new(width, 1000, cumulative, types.clone(), kinds.clone())
            };
            let mut single = create(size, cumulative);
            let mut local = create(1000, false);
            let mut global = create(size, cumulative);
            for rows in [
                vec![
                    (100, 1, Some(7), 0),
                    (200, 2, None, 0),
                    (300, 3, Some(max), 0),
                    (400, 4, Some(3), 0),
                ],
                vec![
                    (100, 1, Some(7), 1),
                    (100, 1, Some(9), 2),
                    (300, 3, Some(1), 0),
                    (400, 4, Some(3), 3),
                ],
            ] {
                let batch = retracting_batch(&rows);
                let mut columns = batch.columns().to_vec();
                for column in &mut columns[2..4] {
                    *column = arrow::compute::cast(column, &value_data_type(code)).unwrap();
                }
                let fields = batch
                    .schema()
                    .fields()
                    .iter()
                    .zip(&columns)
                    .map(|(field, column)| {
                        Field::new(field.name(), column.data_type().clone(), true)
                    })
                    .collect::<Vec<_>>();
                let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
                single.update(&batch).unwrap();
                local.update_local(&batch).unwrap();
                // Checkpointing a live local must preserve its zero/overflow state before draining.
                local = TumblingAggregator::restore(
                    1000,
                    1000,
                    false,
                    types.clone(),
                    kinds.clone(),
                    &local.snapshot(),
                );
                global.update_partial(&local.drain_partial()).unwrap();
                single = TumblingAggregator::restore(
                    size,
                    1000,
                    cumulative,
                    types.clone(),
                    kinds.clone(),
                    &single.snapshot(),
                );
                global = TumblingAggregator::restore_partitions(
                    size,
                    1000,
                    cumulative,
                    types.clone(),
                    kinds.clone(),
                    &global
                        .snapshot_partitions(128, &[-1])
                        .into_values()
                        .collect::<Vec<_>>(),
                );
            }
            let output = single.flush(i64::MAX).unwrap();
            assert_eq!(output, global.flush(i64::MAX).unwrap());
            let keys = column_i64(&output, "key0");
            let result =
                arrow::compute::cast(output.column_by_name("result0").unwrap(), &DataType::Int64)
                    .unwrap();
            let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
            for row in 0..output.num_rows() {
                let expected = match keys.value(row) {
                    1 => 9,
                    2 => 0,
                    3 => match code {
                        0 => i64::MIN,
                        2 => i32::MIN as i64,
                        4 => i16::MIN as i64,
                        _ => i8::MIN as i64,
                    },
                    key => panic!("removed group emitted: {key}"),
                };
                assert_eq!(result.value(row), expected);
            }
            assert_eq!(output.num_rows(), 3 * (size / 1000) as usize);
        }
    }
}

#[test]
fn zero_sum_decimal_overflow_partials_latch_across_window_restore() {
    let code = 2000 + 38 * 100 + 2;
    let max = 10_i128.pow(38) - 1;
    for (size, cumulative) in [(1000, false), (2000, false), (3000, true)] {
        let mut single = TumblingAggregator::new(size, 1000, cumulative, vec![code], vec![22]);
        let mut local = TumblingAggregator::new(1000, 1000, false, vec![code], vec![22]);
        let mut global = TumblingAggregator::new(size, 1000, cumulative, vec![code], vec![22]);
        for values in [
            vec![Some(max), None],
            vec![Some(1), None],
            vec![Some(-1), None],
        ] {
            let batch = RecordBatch::try_from_iter(vec![
                ("ts", Arc::new(Int64Array::from(vec![100; 2])) as ArrayRef),
                ("key0", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
                (
                    "value0",
                    Arc::new(
                        Decimal128Array::from(values)
                            .with_precision_and_scale(38, 2)
                            .unwrap(),
                    ) as ArrayRef,
                ),
            ])
            .unwrap();
            single.update(&batch).unwrap();
            local.update_local(&batch).unwrap();
            global.update_partial(&local.drain_partial()).unwrap();
            single = TumblingAggregator::restore(
                size,
                1000,
                cumulative,
                vec![code],
                vec![22],
                &single.snapshot(),
            );
            global = TumblingAggregator::restore(
                size,
                1000,
                cumulative,
                vec![code],
                vec![22],
                &global.snapshot(),
            );
        }
        let output = single.flush(i64::MAX).unwrap();
        assert_eq!(output, global.flush(i64::MAX).unwrap());
        let keys = column_i64(&output, "key0");
        let results = output
            .column_by_name("result0")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        for row in 0..output.num_rows() {
            if keys.value(row) == 1 {
                assert!(results.is_null(row));
            } else {
                assert_eq!(results.value(row), 0);
                assert!(!results.is_null(row));
            }
        }
    }
}

#[test]
fn timestamp_distinct_groups_fit_the_budget_for_their_owned_state() {
    let rows = 4096;
    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts",
            Arc::new(Int64Array::from_value(100, rows)) as ArrayRef,
        ),
        (
            "key0",
            Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
        ),
        (
            "value0",
            Arc::new(streamfusion_bridge::timestamp::timestamp_array(
                (0..rows).map(|_| {
                    Some(streamfusion_bridge::timestamp::TimestampValue::from_millis(
                        100,
                    ))
                }),
            )) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut aggregate = TumblingAggregator::new(1000, 1000, false, vec![1009], vec![7])
        .with_memory_budget(32 * 1024 * 1024)
        .unwrap();
    aggregate.update(&batch).unwrap();
    let result = aggregate.flush(i64::MAX).unwrap();
    assert_eq!(result.num_rows(), rows);
    assert!(column_i64(&result, "result0")
        .iter()
        .all(|value| value == Some(1)));
}

#[test]
fn string_extrema_keep_sliced_nulls_and_mixed_group_positions_after_restore() {
    let wide = "long-\u{4e2d}".repeat(256);
    let batches = [
        vec![
            (-99, Some("outside")),
            (1, Some("z")),
            (1, None),
            (2, Some("\u{4e2d}")),
            (2, Some("a\0")),
            (3, None),
            (4, None),
            (-99, Some("outside")),
        ],
        vec![
            (-99, Some("outside")),
            (2, Some("\u{1f600}")),
            (1, Some("")),
            (2, Some("a ")),
            (1, Some("z")),
            (3, None),
            (4, Some(wide.as_str())),
            (-99, Some("outside")),
        ],
    ];
    let mut expected = BTreeMap::<i64, (i64, Vec<&str>)>::new();
    let create = || TumblingAggregator::new(1000, 1000, false, vec![3, 3, 0], vec![1, 2, 3]);
    let mut aggregate = create();
    for rows in &batches {
        let values: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_from_iter(vec![
            (
                "ts",
                Arc::new(Int64Array::from_value(100, rows.len())) as ArrayRef,
            ),
            (
                "key0",
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))) as ArrayRef,
            ),
            ("value0", values.clone()),
            ("value1", values),
            (
                "value2",
                Arc::new(Int64Array::from_value(1, rows.len())) as ArrayRef,
            ),
        ])
        .unwrap();
        aggregate.update(&batch.slice(1, rows.len() - 2)).unwrap();
        for &(key, value) in &rows[1..rows.len() - 1] {
            let (count, values) = expected.entry(key).or_default();
            *count += 1;
            values.extend(value);
        }
        aggregate = TumblingAggregator::restore(
            1000,
            1000,
            false,
            vec![3, 3, 0],
            vec![1, 2, 3],
            &aggregate.snapshot(),
        );
    }
    let result = aggregate.flush(i64::MAX).unwrap();
    assert_eq!(result.num_rows(), expected.len());
    let keys = column_i64(&result, "key0");
    let count = column_i64(&result, "result2");
    for (name, minimum) in [("result0", true), ("result1", false)] {
        let actual = result
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..result.num_rows() {
            let (expected_count, values) = &expected[&keys.value(row)];
            assert_eq!(count.value(row), *expected_count);
            let expected = if minimum {
                values.iter().min()
            } else {
                values.iter().max()
            }
            .copied();
            assert_eq!((!actual.is_null(row)).then(|| actual.value(row)), expected);
        }
    }
}

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

#[test]
fn fixed_offset_assignment_retains_epoch_bounds_and_snapshot_grid() {
    const DAY: i64 = 86_400_000;
    for offset in [28_800_000, -28_800_000, 20_700_000] {
        for (size, slide, cumulative) in [
            (DAY, DAY, false),
            (DAY, DAY / 4, false),
            (DAY, DAY / 4, true),
        ] {
            for timestamp in [-DAY * 2 + 1000, 1_704_067_200_000 + 1000] {
                let batch = RecordBatch::try_from_iter(vec![
                    (
                        "ts",
                        Arc::new(Int64Array::from(vec![timestamp])) as ArrayRef,
                    ),
                    ("value0", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
                ])
                .unwrap();
                let mut before = TumblingAggregator::new(size, slide, cumulative, vec![0], vec![3]);
                before.set_assignment_offset(offset);
                before.update(&batch).unwrap();
                let raw = before.snapshot();
                let mut restored =
                    TumblingAggregator::restore(size, slide, cumulative, vec![0], vec![3], &raw);
                assert_eq!(
                    before.assignment_offset_millis,
                    restored.assignment_offset_millis
                );
                let mut local = Vec::new();
                windows_for(timestamp + offset, size, slide, cumulative, &mut local);
                local.sort_by_key(|&(_, end)| end);
                let close = local.last().unwrap().1 - offset;
                assert_eq!(before.flush(close).unwrap(), restored.flush(close).unwrap());
                let mut actual =
                    TumblingAggregator::restore(size, slide, cumulative, vec![0], vec![3], &raw);
                let out = actual.flush(close).unwrap();
                let starts = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                let ends = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(
                    starts.values().as_ref(),
                    local
                        .iter()
                        .map(|(start, _)| start - offset)
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    ends.values().as_ref(),
                    local
                        .iter()
                        .map(|(_, end)| end - offset)
                        .collect::<Vec<_>>()
                );
            }
        }
    }
}

#[test]
fn fixed_offset_local_partials_merge_on_the_global_shifted_grid() {
    const DAY: i64 = 86_400_000;
    let offset = 20_700_000;
    let timestamp = -DAY + 1000;
    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts",
            Arc::new(Int64Array::from(vec![timestamp])) as ArrayRef,
        ),
        ("value0", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
    ])
    .unwrap();
    for cumulative in [false, true] {
        let mut local = TumblingAggregator::new(DAY / 4, DAY / 4, false, vec![0], vec![3]);
        local.set_assignment_offset(offset);
        local.update_local(&batch).unwrap();
        let partial = local.drain_partial();
        let mut global = TumblingAggregator::new(DAY, DAY / 4, cumulative, vec![0], vec![3]);
        global.set_assignment_offset(offset);
        global.update_partial(&partial).unwrap();
        let mut direct = TumblingAggregator::new(DAY, DAY / 4, cumulative, vec![0], vec![3]);
        direct.set_assignment_offset(offset);
        direct.update(&batch).unwrap();
        assert_eq!(
            global.flush(DAY * 2).unwrap(),
            direct.flush(DAY * 2).unwrap()
        );
    }
}
