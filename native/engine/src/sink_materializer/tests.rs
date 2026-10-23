use super::*;
use arrow::array::Float64Array;

fn batch(values: &[(i64, i64, i64, i8)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sink_key", DataType::Int64, false),
        Field::new("input_key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(values.iter().map(|v| v.0))),
            Arc::new(Int64Array::from_iter_values(values.iter().map(|v| v.1))),
            Arc::new(Int64Array::from_iter_values(values.iter().map(|v| v.2))),
            Arc::new(Int8Array::from_iter_values(values.iter().map(|v| v.3))),
        ],
    )
    .unwrap()
}

fn rows(batch: &RecordBatch) -> Vec<(i64, i64, i64, i8)> {
    if batch.num_rows() == 0 {
        return vec![];
    }
    (0..batch.num_rows())
        .map(|i| {
            (
                column_i64(batch, "sink_key").value(i),
                column_i64(batch, "input_key").value(i),
                column_i64(batch, "value").value(i),
                row_kind_column(batch).unwrap().value(i),
            )
        })
        .collect()
}

fn materializer(upsert: bool, ttl: i64) -> SinkMaterializer {
    SinkMaterializer::new(
        vec![0],
        vec![-1],
        if upsert { vec![1] } else { vec![] },
        ttl,
    )
}

#[test]
fn stale_retracts_are_silent_and_retracting_the_visible_row_restores_its_predecessor() {
    let mut m = materializer(false, 0);
    let actual = m
        .push(
            &batch(&[
                (1, 10, 100, 0),
                (1, 20, 200, 0),
                (1, 30, 300, 2),
                (1, 20, 200, 1),
                (1, 30, 300, 3),
                (1, 99, 999, 3),
                (1, 10, 100, 3),
            ]),
            0,
        )
        .unwrap();
    assert_eq!(
        rows(&actual),
        vec![
            (1, 10, 100, 0),
            (1, 20, 200, 2),
            (1, 30, 300, 2),
            (1, 10, 100, 2),
            (1, 10, 100, 3),
        ]
    );
    assert!(m.groups.iter().next().is_none());
}

#[test]
fn duplicate_retracts_remove_the_first_occurrence() {
    let mut m = materializer(false, 0);
    let actual = m
        .push(
            &batch(&[
                (1, 10, 100, 0),
                (1, 20, 200, 0),
                (1, 10, 100, 0),
                (1, 10, 100, 3),
                (1, 10, 100, 1),
            ]),
            0,
        )
        .unwrap();
    assert_eq!(
        rows(&actual),
        vec![
            (1, 10, 100, 0),
            (1, 20, 200, 2),
            (1, 10, 100, 2),
            (1, 20, 200, 2),
        ]
    );
}

#[test]
fn input_upsert_key_replaces_in_place_and_delete_emits_the_retract_payload() {
    let mut m = materializer(true, 0);
    let actual = m
        .push(
            &batch(&[
                (1, 10, 100, 0),
                (1, 20, 200, 0),
                (1, 10, 101, 2),
                (1, 20, -1, 3),
                (1, 10, -2, 1),
            ]),
            0,
        )
        .unwrap();
    assert_eq!(
        rows(&actual),
        vec![
            (1, 10, 100, 0),
            (1, 20, 200, 2),
            (1, 10, 101, 2),
            (1, 10, 101, 2),
            (1, 10, -2, 3),
        ]
    );
}

#[test]
fn raw_key_groups_restore_candidate_order_and_ttl_without_refreshing_missing_retracts() {
    let mut before = materializer(false, 100);
    before
        .push(
            &batch(&[(1, 10, 100, 0), (1, 20, 200, 0), (2, 10, 900, 0)]),
            1000,
        )
        .unwrap();
    before.push(&batch(&[(1, 999, 0, 3)]), 1090).unwrap();
    let snapshots: Vec<_> = before.snapshot(128).into_values().collect();
    let mut restored = materializer(false, 100);
    restored.restore(&snapshots, 1050);
    assert_eq!(
        rows(&restored.push(&batch(&[(1, 20, 200, 3)]), 1099).unwrap()),
        vec![(1, 10, 100, 2)]
    );
    assert_eq!(
        rows(
            &restored
                .push(&batch(&[(2, 10, 900, 3), (2, 20, 901, 0)]), 1100)
                .unwrap()
        ),
        vec![(2, 20, 901, 0)]
    );
    assert_eq!(
        rows(&restored.push(&batch(&[(1, 10, 100, 3)]), 1199).unwrap()),
        vec![]
    );
}

#[test]
fn memory_budget_accounts_retained_candidates_and_releases_deleted_keys() {
    let mut m = materializer(false, 0).with_budget(4096).unwrap();
    m.push(&batch(&[(1, 10, 100, 0), (1, 20, 200, 0)]), 0)
        .unwrap();
    let retained = m.memory.state_bytes;
    assert!(retained > 0);
    m.push(&batch(&[(1, 10, 100, 3)]), 0).unwrap();
    assert!(m.memory.state_bytes < retained);
    m.push(&batch(&[(1, 20, 200, 3)]), 0).unwrap();
    assert_eq!(m.memory.state_bytes, 0);
    assert!(materializer(false, 0)
        .with_budget(1)
        .unwrap()
        .push(&batch(&[(1, 1, 1, 0)]), 0)
        .is_err());
}

#[test]
fn enabling_ttl_on_restore_stamps_the_restore_clock() {
    let mut before = materializer(false, 0);
    before.push(&batch(&[(1, 10, 100, 0)]), 1).unwrap();
    let snapshots: Vec<_> = before.snapshot(128).into_values().collect();
    let mut restored = materializer(false, 100);
    restored.restore(&snapshots, 1000);
    assert_eq!(
        rows(&restored.push(&batch(&[(1, 10, 100, 3)]), 1099).unwrap()),
        vec![(1, 10, 100, 3)]
    );
}

#[test]
fn ordinary_upserts_share_input_value_buffers() {
    let input = batch(&[(1, 10, 100, 0), (1, 10, 101, 2)]);
    let output = materializer(true, 0).push(&input, 0).unwrap();
    for column in 0..3 {
        assert!(Arc::ptr_eq(input.column(column), output.column(column)));
    }
    assert_eq!(rows(&output), vec![(1, 10, 100, 0), (1, 10, 101, 2)]);
}

#[test]
fn hashed_and_adaptive_keys_preserve_signed_zero_across_restore() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("sink_key", DataType::Int64, false),
        Field::new("input_key", DataType::Float64, false),
        Field::new("value", DataType::Int64, false),
        Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
    ]));
    let make = |keys: Vec<f64>, values: Vec<i64>, kinds: Vec<i8>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1; keys.len()])),
                Arc::new(Float64Array::from(keys)),
                Arc::new(Int64Array::from(values)),
                Arc::new(Int8Array::from(kinds)),
            ],
        )
        .unwrap()
    };
    for strategy in [
        Strategy::Map { serialized: false },
        Strategy::Map { serialized: true },
        Strategy::Adaptive {
            high: 1,
            low: 0,
            serialized: false,
        },
        Strategy::Adaptive {
            high: 1,
            low: 0,
            serialized: true,
        },
    ] {
        let mut before = materializer(true, 0).with_strategy(strategy, vec![-1]);
        before
            .push(&make(vec![0.0, -0.0], vec![10, 20], vec![0, 0]), 0)
            .unwrap();
        let snapshots: Vec<_> = before.snapshot(128).into_values().collect();
        let mut after = materializer(true, 0).with_strategy(strategy, vec![-1]);
        after.restore(&snapshots, 1);
        let result = after.push(&make(vec![-0.0], vec![99], vec![3]), 1).unwrap();
        assert_eq!(column_i64(&result, "value").values().as_ref(), &[10]);
        assert_eq!(row_kind_column(&result).unwrap().values().as_ref(), &[2]);
    }
}

#[test]
fn linked_sequence_cursor_and_index_restore_without_reordering() {
    let strategy = Strategy::Map { serialized: false };
    let mut before = materializer(true, 0).with_strategy(strategy, vec![-1]);
    before
        .push(
            &batch(&[
                (1, 10, 10, 0),
                (1, 20, 20, 0),
                (1, 30, 30, 0),
                (1, 10, 11, 2),
                (1, 40, 40, 0),
            ]),
            0,
        )
        .unwrap();
    let snapshots: Vec<_> = before.snapshot(128).into_values().collect();
    let mut after = materializer(true, 0).with_strategy(strategy, vec![-1]);
    after.restore(&snapshots, 1);
    assert_eq!(
        rows(&after.push(&batch(&[(1, 40, 40, 3)]), 1).unwrap()),
        vec![(1, 10, 11, 2)]
    );
}

#[test]
fn repeated_single_candidate_upserts_preserve_every_change_and_restore_final_rows() {
    for strategy in [
        Strategy::Legacy,
        Strategy::Value,
        Strategy::Map { serialized: false },
        Strategy::Map { serialized: true },
        Strategy::Adaptive {
            high: 1,
            low: 0,
            serialized: true,
        },
    ] {
        let make = || {
            SinkMaterializer::new(vec![0], vec![-1], vec![0], 100).with_strategy(strategy, vec![-1])
        };
        let input = batch(
            &(0..100)
                .map(|i| (if i < 75 { 1 } else { 2 }, i, i * 10, 2))
                .collect::<Vec<_>>(),
        );
        let mut before = make();
        let output = before.push(&input, 1000).unwrap();
        let mut expected = rows(&input);
        for (i, row) in expected.iter_mut().enumerate() {
            row.3 = if i == 0 || i == 75 { 0 } else { 2 };
        }
        assert_eq!(rows(&output), expected);
        for column in 0..3 {
            assert!(Arc::ptr_eq(input.column(column), output.column(column)));
        }
        let snapshots: Vec<_> = before.snapshot(128).into_values().collect();
        let mut after = make();
        after.restore(&snapshots, 1050);
        assert_eq!(
            rows(
                &after
                    .push(&batch(&[(1, -1, -1, 3), (2, -2, -2, 1)]), 1050)
                    .unwrap()
            ),
            vec![(1, -1, -1, 3), (2, -2, -2, 3)]
        );
        assert!(after.groups.iter().next().is_none());
    }
}

#[test]
fn global_candidate_multisets_keep_every_row_without_an_inferred_key() {
    let mut m = SinkMaterializer::new(vec![], vec![], vec![], 0);
    let input = batch(&(0..100).map(|i| (1, i, i * 10, 0)).collect::<Vec<_>>());
    assert_eq!(m.push(&input, 0).unwrap().num_rows(), 100);
    assert_eq!(
        rows(&m.push(&batch(&[(1, 99, 990, 3)]), 0).unwrap()),
        vec![(1, 98, 980, 2)]
    );
}

#[test]
fn array_nan_serialization_retains_explicit_binary_carriers_across_restore() {
    let fields = arrow::datatypes::Fields::from(vec![
        Field::new("value", DataType::Float64, true),
        Field::new("array_copy_class", DataType::Int8, true)
            .with_metadata([("streamfusion.array-element".to_string(), "1".to_string())].into()),
    ]);
    let element = Arc::new(Field::new(
        "element",
        DataType::Struct(fields.clone()),
        false,
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("sink_key", DataType::Int64, false),
        Field::new("input_key", DataType::List(element.clone()), false),
        Field::new("value", DataType::Int64, false),
        Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
    ]));
    for binary in [false, true] {
        let make = |keys: Vec<f64>, values: Vec<i64>, kinds: Vec<i8>| {
            let count = keys.len();
            let entries = StructArray::new(
                fields.clone(),
                vec![
                    Arc::new(Float64Array::from(keys)),
                    Arc::new(Int8Array::from(vec![i8::from(binary); count])),
                ],
                None,
            );
            let list = ListArray::new(
                element.clone(),
                arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(
                    (0..=count as i32).collect::<Vec<_>>(),
                )),
                Arc::new(entries),
                None,
            );
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1; count])),
                    Arc::new(list),
                    Arc::new(Int64Array::from(values)),
                    Arc::new(Int8Array::from(kinds)),
                ],
            )
            .unwrap()
        };
        for strategy in [
            Strategy::Legacy,
            Strategy::Value,
            Strategy::Map { serialized: false },
            Strategy::Map { serialized: true },
            Strategy::Adaptive {
                high: 1,
                low: 0,
                serialized: false,
            },
            Strategy::Adaptive {
                high: 1,
                low: 0,
                serialized: true,
            },
        ] {
            let create = || materializer(true, 0).with_strategy(strategy, vec![-1, -1]);
            let nan1 = f64::from_bits(0x7ff8000000000001);
            let nan2 = f64::from_bits(0x7ff8000000000002);
            let mut before = create();
            let input = make(vec![nan1, 2.0, nan2], vec![10, 20, 11], vec![0, 0, 2]);
            let output = before.push(&input, 0).unwrap();
            let list = output
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let entries = list
                .values()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let floats = entries
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(
                floats.value(0).to_bits(),
                if binary {
                    nan1.to_bits()
                } else {
                    crate::flink_float::canonical_f64(nan1).to_bits()
                }
            );
            assert_eq!(Arc::ptr_eq(input.column(1), output.column(1)), binary);
            assert!(Arc::ptr_eq(input.column(2), output.column(2)));
            let snapshots: Vec<_> = before.snapshot(128).into_values().collect();
            let mut after = create();
            after.restore(&snapshots, 0);
            let visible = after.push(&make(vec![2.0], vec![20], vec![3]), 0).unwrap();
            if binary {
                assert_eq!(visible.num_rows(), 0);
            } else {
                assert_eq!(column_i64(&visible, "value").values().as_ref(), &[11]);
            }
            let result = after.push(&make(vec![nan2], vec![99], vec![3]), 0).unwrap();
            assert_eq!(
                column_i64(&result, "value").values().as_ref(),
                &[if binary { 10 } else { 99 }]
            );
            assert_eq!(
                row_kind_column(&result).unwrap().values().as_ref(),
                &[if binary { 2 } else { 3 }]
            );
        }
    }
}
