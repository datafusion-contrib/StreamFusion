use super::*;
use arrow::array::Float64Array;
use arrow::array::{Int32Builder, Int64Builder, ListBuilder, MapBuilder, StringBuilder};

fn lists() -> RecordBatch {
    let mut values = ListBuilder::new(Int64Builder::new());
    values.values().append_value(1);
    values.values().append_null();
    values.values().append_value(2);
    values.append(true);
    values.append(true);
    values.append(false);
    values.values().append_value(-1);
    values.append(true);
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])) as ArrayRef,
        ),
        ("items", Arc::new(values.finish()) as ArrayRef),
        (
            ROW_KIND_COLUMN,
            Arc::new(Int8Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn projected_lists_keep_order_null_padding_repeated_columns_and_all_row_kinds() {
    let input = lists();
    let output = unnest_projected(&input, 1, true, true, false, Some(&[3, 2, 0, 2]));
    assert_eq!(output.num_rows(), 6);
    assert_eq!(
        output.column(0).as_ref(),
        &Int32Array::from(vec![Some(1), Some(2), Some(3), None, None, Some(1)])
    );
    assert_eq!(
        output.column(1).as_ref(),
        &Int64Array::from(vec![Some(1), None, Some(2), None, None, Some(-1)])
    );
    assert_eq!(
        output.column(2).as_ref(),
        &Int64Array::from(vec![10, 10, 10, 20, 30, 40])
    );
    assert!(Arc::ptr_eq(output.column(1), output.column(3)));
    assert_eq!(
        output.column(4).as_ref(),
        &Int8Array::from(vec![0, 0, 0, 1, 2, 3])
    );
    let sliced = unnest_projected(&input.slice(1, 2), 1, true, true, false, Some(&[2, 3]));
    assert_eq!(sliced.num_rows(), 2);
    assert_eq!(sliced.column(0).null_count(), 2);
    assert_eq!(sliced.column(1).null_count(), 2);
}

#[test]
fn empty_projection_keeps_fanout_cardinality_with_or_without_row_kinds() {
    let input = lists();
    let signed = unnest_projected(&input, 1, false, true, false, Some(&[]));
    assert_eq!(signed.num_rows(), 6);
    assert_eq!(signed.num_columns(), 1);
    assert_eq!(
        signed.column(0).as_ref(),
        &Int8Array::from(vec![0, 0, 0, 1, 2, 3])
    );
    let unsigned = input.project(&[0, 1]).unwrap();
    for (left, rows) in [(false, 4), (true, 6)] {
        let empty = unnest_projected(&unsigned, 1, false, left, false, Some(&[]));
        assert_eq!(empty.num_columns(), 0);
        assert_eq!(empty.num_rows(), rows);
    }
}

#[test]
fn projected_map_and_multiset_preserve_entry_order_multiplicities_and_padding() {
    let mut maps = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    for (key, count) in [("a", 2), ("b", 0), ("c", -1)] {
        maps.keys().append_value(key);
        maps.values().append_value(count);
    }
    maps.append(true).unwrap();
    maps.append(true).unwrap();
    maps.append(false).unwrap();
    maps.keys().append_value("d");
    maps.values().append_value(1);
    maps.append(true).unwrap();
    let input = RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])) as ArrayRef,
        ),
        ("items", Arc::new(maps.finish()) as ArrayRef),
    ])
    .unwrap();
    let map = unnest_projected(&input, 1, true, true, false, Some(&[4, 2, 3]));
    assert_eq!(
        map.column(0).as_ref(),
        &Int32Array::from(vec![Some(1), Some(2), Some(3), None, None, Some(1)])
    );
    assert_eq!(
        map.column(1).as_ref(),
        &StringArray::from(vec![Some("a"), Some("b"), Some("c"), None, None, Some("d")])
    );
    assert_eq!(
        map.column(2).as_ref(),
        &Int32Array::from(vec![Some(2), Some(0), Some(-1), None, None, Some(1)])
    );
    let multiset = unnest_projected(&input, 1, true, true, true, Some(&[3, 2]));
    assert_eq!(
        multiset.column(0).as_ref(),
        &Int32Array::from(vec![Some(1), Some(2), None, None, Some(1)])
    );
    assert_eq!(
        multiset.column(1).as_ref(),
        &StringArray::from(vec![Some("a"), Some("a"), None, None, Some("d")])
    );
}

#[test]
fn timestamp_elements_stay_scalar_and_do_not_alias_an_ordinary_row_shape() {
    use arrow::buffer::OffsetBuffer;
    use streamfusion_bridge::timestamp::{timestamp_array, TimestampColumn, TimestampValue};
    let values = [
        Some(TimestampValue::from_nanos(i64::MAX as i128 * 1_000_000 + 123456).unwrap()),
        None,
        Some(TimestampValue::from_nanos(i64::MIN as i128 * 1_000_000 + 7).unwrap()),
    ];
    let timestamps = timestamp_array(values);
    let list = ListArray::new(
        Arc::new(Field::new("item", timestamps.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 3].into()),
        Arc::new(timestamps.clone()),
        None,
    );
    let batch = RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![12])) as ArrayRef),
        ("items", Arc::new(list) as ArrayRef),
    ])
    .unwrap();
    let actual = unnest_projected(&batch, 1, true, false, false, Some(&[2, 3]));
    let column = TimestampColumn::try_new(actual.column(0).as_ref()).unwrap();
    for (i, expected) in values.into_iter().enumerate() {
        assert_eq!(column.is_null(i), expected.is_none());
        if let Some(expected) = expected {
            assert_eq!(column.value(i).unwrap(), expected);
        }
    }
    assert_eq!(actual.column(1).as_ref(), &Int32Array::from(vec![1, 2, 3]));
    let fields = timestamps
        .fields()
        .iter()
        .map(|field| Arc::new(field.as_ref().clone().with_metadata(Default::default())))
        .collect::<Vec<_>>();
    let ordinary = StructArray::new(
        fields.into(),
        timestamps.columns().to_vec(),
        timestamps.nulls().cloned(),
    );
    let list = ListArray::new(
        Arc::new(Field::new("item", ordinary.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 3].into()),
        Arc::new(ordinary),
        None,
    );
    let batch = RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![12])) as ArrayRef),
        ("items", Arc::new(list) as ArrayRef),
    ])
    .unwrap();
    let actual = unnest_projected(&batch, 1, false, false, false, Some(&[2, 3]));
    assert_eq!(actual.num_rows(), 2);
    assert_eq!(actual.column(0).data_type(), &DataType::Int64);
    assert_eq!(actual.column(1).data_type(), &DataType::Int32);
}

#[test]
fn one_field_rows_with_ordinality_keep_null_elements_at_their_original_position() {
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    let values = StructArray::new(
        vec![Arc::new(Field::new("a", DataType::Int64, true))].into(),
        vec![Arc::new(Int64Array::from(vec![7, 999, 9]))],
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let list = ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 3].into()),
        Arc::new(values),
        None,
    );
    let batch = RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![12])) as ArrayRef),
        ("items", Arc::new(list) as ArrayRef),
    ])
    .unwrap();
    let actual = unnest_projected(&batch, 1, true, false, false, Some(&[2, 3]));
    assert_eq!(
        actual.column(0).as_ref(),
        &Int64Array::from(vec![Some(7), None, Some(9)])
    );
    assert_eq!(actual.column(1).as_ref(), &Int32Array::from(vec![1, 2, 3]));
}

fn constructor(operation: i64) -> crate::bench::CalcProgram {
    crate::bench::CalcProgram::new(
        vec![6, 0, 0],
        vec![operation, 0, 1],
        vec![2, 0, 0],
        vec![],
        vec![],
        vec![],
    )
}

#[test]
fn scalar_array_constructor_keeps_null_elements_narrow_types_and_floating_bits() {
    let tiny: ArrayRef = Arc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)]));
    let input = RecordBatch::try_from_iter(vec![("a", tiny.clone()), ("b", tiny)]).unwrap();
    let result = constructor(171).run(input);
    let arrays = result
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(arrays.null_count(), 0);
    assert_eq!(arrays.values().data_type(), &DataType::Int8);
    assert_eq!(
        arrays.values().as_ref(),
        &Int8Array::from(vec![
            Some(i8::MIN),
            Some(i8::MIN),
            None,
            None,
            Some(i8::MAX),
            Some(i8::MAX)
        ])
    );
    let first = f64::from_bits(0x7ff8000000000001);
    let second = f64::from_bits(0xfff8000000000002);
    let a: ArrayRef = Arc::new(Float64Array::from(vec![Some(first), Some(-0.0), None]));
    let b: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.0), Some(second), None]));
    let result =
        constructor(171).run(RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap());
    let arrays = result
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = arrays
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for (index, expected) in [first, 0.0, -0.0, second].into_iter().enumerate() {
        assert_eq!(values.value(index).to_bits(), expected.to_bits());
    }
    assert_eq!(values.null_count(), 2);
}

#[test]
fn timestamp_array_constructor_matches_compact_and_expanded_binary_array_storage() {
    use streamfusion_bridge::timestamp::{timestamp_array, TimestampColumn, TimestampValue};
    let first = TimestampValue::from_nanos(-1).unwrap();
    let second = TimestampValue::from_nanos(i64::MAX as i128 * 1_000_000 + 987654).unwrap();
    let a: ArrayRef = Arc::new(timestamp_array([Some(first), None]));
    let b: ArrayRef = Arc::new(timestamp_array([Some(second), Some(first)]));
    let input = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();
    for operation in [171, 172] {
        let result = constructor(operation).run(input.clone());
        let array = result
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = TimestampColumn::try_new(array.values().as_ref()).unwrap();
        let normalize = |value: TimestampValue| {
            if operation == 172 {
                TimestampValue::from_nanos(value.millis() as i128 * 1_000_000).unwrap()
            } else {
                value
            }
        };
        assert_eq!(values.value(0).unwrap(), normalize(first));
        assert_eq!(values.value(1).unwrap(), normalize(second));
        assert!(values.is_null(2));
        assert_eq!(values.value(3).unwrap(), normalize(first));
        assert_eq!(array.null_count(), 0);
    }
}

#[test]
fn represented_string_elements_stay_scalar_with_owned_caches_and_null_padding() {
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    let wide = "é🙂".repeat(1024);
    let utf8: ArrayRef = Arc::new(StringArray::from(vec![
        Some(wide.as_str()),
        None,
        Some("?"),
        Some("tail"),
    ]));
    let cache: ArrayRef = Arc::new(streamfusion_bridge::string_cache::string_cache_array([
        None,
        None,
        Some([0xd800u16].as_slice()),
        None,
    ]));
    let fields: arrow::datatypes::Fields = vec![
        Field::new("utf8", DataType::Utf8, true)
            .with_metadata([("streamfusion.nested-string".into(), "1".into())].into()),
        Field::new("java", cache.data_type().clone(), true),
        Field::new("array_copy_class", DataType::Int8, true),
    ]
    .into();
    let strings: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![utf8, cache, Arc::new(Int8Array::from(vec![0, 0, 0, 0]))],
        Some(NullBuffer::from(vec![true, false, true, true])),
    ));
    let constructed = constructor(171).run(
        RecordBatch::try_from_iter(vec![("a", strings.clone()), ("b", strings.clone())]).unwrap(),
    );
    let binary = constructed
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let binary_values = binary
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(binary_values.column(1).null_count(), binary_values.len());
    assert!(binary_values
        .column(2)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap()
        .iter()
        .all(|flag| flag == Some(1)));
    assert_eq!(
        strings
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(1)
            .null_count(),
        3
    );
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", strings.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 3, 3, 3, 4].into()),
        strings.clone(),
        Some(NullBuffer::from(vec![true, true, false, true])),
    ));
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("items", list.data_type().clone(), true)
                .with_metadata([("streamfusion.array-copy".into(), "1".into())].into()),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            list,
            Arc::new(Int8Array::from(vec![0, 1, 2, 3])),
        ],
    )
    .unwrap();
    let output = unnest_projected(&input, 1, true, true, false, Some(&[0, 1, 2, 3, 2]));
    assert_eq!(output.num_rows(), 6);
    assert_eq!(output.num_columns(), 6);
    assert_eq!(
        output.column(0).as_ref(),
        &Int64Array::from(vec![10, 10, 10, 20, 30, 40])
    );
    assert_eq!(
        output
            .schema()
            .field(1)
            .metadata()
            .get("streamfusion.array-copy")
            .map(String::as_str),
        Some("1")
    );
    let expected = take(
        strings.as_ref(),
        &UInt32Array::from(vec![Some(0), Some(1), Some(2), None, None, Some(3)]),
        None,
    )
    .unwrap();
    assert_eq!(output.column(2).as_ref(), expected.as_ref());
    assert!(Arc::ptr_eq(output.column(2), output.column(4)));
    assert_eq!(
        output.column(3).as_ref(),
        &Int32Array::from(vec![Some(1), Some(2), Some(3), None, None, Some(1)])
    );
    assert_eq!(
        output.column(5).as_ref(),
        &Int8Array::from(vec![0, 0, 0, 1, 2, 3])
    );
    let sliced = unnest_projected(&input.slice(1, 2), 1, true, true, false, Some(&[2, 3]));
    assert_eq!(sliced.num_rows(), 2);
    assert_eq!(sliced.column(0).null_count(), 2);
    let ordinary_fields = fields
        .iter()
        .map(|field| Arc::new(field.as_ref().clone().with_metadata(Default::default())))
        .collect::<Vec<_>>();
    let carrier = strings.as_any().downcast_ref::<StructArray>().unwrap();
    let ordinary: ArrayRef = Arc::new(StructArray::new(
        ordinary_fields.into(),
        carrier.columns().to_vec(),
        carrier.nulls().cloned(),
    ));
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", ordinary.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 4].into()),
        ordinary,
        None,
    ));
    let rows = RecordBatch::try_from_iter(vec![("items", list)]).unwrap();
    let actual = unnest_array(&rows, 0, false, false, false);
    assert_eq!(actual.num_columns(), 4);
    assert_eq!(actual.num_rows(), 3);
    assert_eq!(actual.column(1).data_type(), &DataType::Utf8);
    drop(input);
    drop(strings);
    assert_eq!(output.column(2).as_ref(), expected.as_ref());
}
