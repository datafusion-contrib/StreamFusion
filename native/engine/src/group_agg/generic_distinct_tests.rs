use super::ordered_retention::ValueKey;
use super::*;
use arrow::array::Float64Array;
use std::hash::{Hash, Hasher};

fn hash(value: &ValueKey) -> u64 {
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hash);
    hash.finish()
}

fn list(value: ScalarValue) -> ScalarValue {
    let data_type = value.data_type();
    ScalarValue::List(ScalarValue::new_list(&[value], &data_type, true))
}

#[test]
fn membership_ignores_nested_field_names_and_nullability() {
    let a = list(ScalarValue::Int32(Some(7)));
    let renamed = DataType::List(Arc::new(Field::new("$data$", DataType::Int32, false)));
    let array = arrow::compute::cast(&a.to_array().unwrap(), &renamed).unwrap();
    let b = ScalarValue::try_from_array(&array, 0).unwrap();
    assert!(a.data_type() != b.data_type());
    let a = ValueKey(a);
    let b = ValueKey(b);
    assert!(a == b);
    assert_eq!(hash(&a), hash(&b));
    let mut set = std::collections::HashSet::new();
    assert!(set.insert(a));
    assert!(!set.insert(b));
    assert_eq!(set.len(), 1);
}

#[test]
fn mixed_nested_scalar_schemas_are_aligned_before_concatenation() {
    let a = list(ScalarValue::Int32(Some(7)));
    let target = DataType::List(Arc::new(Field::new("$data$", DataType::Int32, true)));
    let b = ScalarValue::try_from_array(
        &arrow::compute::cast(&a.to_array().unwrap(), &target).unwrap(),
        0,
    )
    .unwrap();
    let output = crate::aggregates::scalars_to_array(vec![a, b], &target);
    assert_eq!(output.data_type(), &target);
    assert_eq!(output.len(), 2);
    assert!(
        ValueKey(ScalarValue::try_from_array(&output, 0).unwrap())
            == ValueKey(ScalarValue::try_from_array(&output, 1).unwrap())
    );
}

#[test]
fn nested_double_membership_canonicalizes_nan_but_preserves_signed_zero() {
    let a = list(ScalarValue::Float64(Some(f64::from_bits(
        0x7ff8000000000001,
    ))));
    let b = list(ScalarValue::Float64(Some(f64::from_bits(
        0xfff8000000000002,
    ))));
    assert!(ValueKey(a.clone()) == ValueKey(b.clone()));
    assert_eq!(hash(&ValueKey(a.clone())), hash(&ValueKey(b.clone())));
    assert!(!crate::flink_float::record_equal(&a, &b));
    let positive = ValueKey(list(ScalarValue::Float64(Some(0.0))));
    let negative = ValueKey(list(ScalarValue::Float64(Some(-0.0))));
    assert!(positive != negative);
    let a = ValueKey(list(a));
    let b = ValueKey(list(b));
    assert!(a == b);
    assert_eq!(hash(&a), hash(&b));
}

#[test]
fn nested_float_membership_canonicalizes_nan_but_preserves_signed_zero() {
    let a = ValueKey(list(ScalarValue::Float32(Some(f32::from_bits(0x7fc00001)))));
    let b = ValueKey(list(ScalarValue::Float32(Some(f32::from_bits(0xffc00002)))));
    assert!(a == b);
    assert_eq!(hash(&a), hash(&b));
    assert!(
        ValueKey(list(ScalarValue::Float32(Some(0.0))))
            != ValueKey(list(ScalarValue::Float32(Some(-0.0))))
    );
}

#[test]
fn uniform_nested_schema_cast_preserves_original_nan_payloads_and_zero_signs() {
    let bits = [0x7ff8000000000001, 0xfff8000000000002, 0, 1 << 63];
    let values = bits
        .iter()
        .map(|bits| list(ScalarValue::Float64(Some(f64::from_bits(*bits)))))
        .collect();
    let target = DataType::List(Arc::new(Field::new("$data$", DataType::Float64, true)));
    let output = crate::aggregates::scalars_to_array(values, &target);
    assert_eq!(output.data_type(), &target);
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    let values = output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        values
            .values()
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>(),
        bits
    );
}

fn represented_string_batch() -> RecordBatch {
    use arrow::buffer::NullBuffer;
    let fields: arrow::datatypes::Fields = vec![
        Field::new("utf8", DataType::Utf8, true).with_metadata(
            [("streamfusion.nested-string".to_owned(), "1".to_owned())]
                .into_iter()
                .collect(),
        ),
        Field::new(
            "java",
            DataType::List(Arc::new(Field::new("unit", DataType::UInt16, false))),
            true,
        ),
    ]
    .into();
    let bytes: ArrayRef = Arc::new(StringArray::from(vec![
        Some("é🚀"),
        Some("not a key"),
        Some("?"),
    ]));
    let DataType::List(unit) = fields[1].data_type() else {
        unreachable!()
    };
    let caches: ArrayRef = Arc::new(ListArray::new(
        unit.clone(),
        arrow::buffer::OffsetBuffer::new(vec![0, 0, 0, 1].into()),
        Arc::new(arrow::array::UInt16Array::from(vec![0xd800])),
        Some(NullBuffer::from(vec![false, false, true])),
    ));
    let represented: ArrayRef = Arc::new(arrow::array::StructArray::new(
        fields,
        vec![bytes, caches],
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let payload: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("element", represented.data_type().clone(), true)),
        arrow::buffer::OffsetBuffer::new(vec![0, 1, 2, 3].into()),
        represented.clone(),
        None,
    ));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", represented.data_type().clone(), true),
            Field::new("v", payload.data_type().clone(), true),
        ])),
        vec![represented, payload],
    )
    .unwrap()
}

#[test]
fn primitive_string_inputs_share_bytes_and_preserve_parent_nulls_and_nested_payloads() {
    let batch = represented_string_batch();
    let normalized = super::string_input::canonical(&batch).unwrap();
    assert_eq!(normalized.column(0).data_type(), &DataType::Utf8);
    assert!(normalized.column(0).is_null(1));
    assert!(Arc::ptr_eq(batch.column(1), normalized.column(1)));
    let original = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .unwrap();
    let bytes = original
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let actual = normalized
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(actual.value(0), "é🚀");
    assert_eq!(bytes.value_data().as_ptr(), actual.value_data().as_ptr());
}

#[test]
fn represented_strings_use_one_flink_key_descriptor_and_canonical_key_bytes() {
    let batch = represented_string_batch();
    let normalized = super::string_input::canonical(&batch).unwrap();
    let mut original = crate::flink_key::BinaryRowBatchEncoder::new(&batch, &[0], &[-1]);
    let mut canonical = crate::flink_key::BinaryRowBatchEncoder::new(&normalized, &[0], &[-1]);
    for row in 0..3 {
        assert_eq!(original.encode(row), canonical.encode(row));
    }
    let plain = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "k",
            DataType::List(Arc::new(Field::new("element", DataType::Utf8, true))),
            true,
        )])),
        vec![Arc::new(ListArray::new(
            Arc::new(Field::new("element", DataType::Utf8, true)),
            arrow::buffer::OffsetBuffer::new(vec![0, 1, 2, 3].into()),
            normalized.column(0).clone(),
            None,
        ))],
    )
    .unwrap();
    let mut represented = crate::flink_key::BinaryRowBatchEncoder::new(&batch, &[1], &[-1, -1]);
    let mut primitive = crate::flink_key::BinaryRowBatchEncoder::new(&plain, &[0], &[-1, -1]);
    for row in 0..3 {
        assert_eq!(represented.encode(row), primitive.encode(row));
    }
}

#[test]
fn represented_array_elements_keep_the_logical_flink_slot_width() {
    let fields: arrow::datatypes::Fields = vec![
        Field::new("value", DataType::Int32, true),
        Field::new("array_copy_class", DataType::Int8, true).with_metadata(
            [("streamfusion.array-element".to_owned(), "1".to_owned())]
                .into_iter()
                .collect(),
        ),
    ]
    .into();
    let values: ArrayRef = Arc::new(arrow::array::StructArray::new(
        fields,
        vec![
            Arc::new(Int32Array::from(vec![7, 9])),
            Arc::new(Int8Array::from(vec![0, 0])),
        ],
        None,
    ));
    let wrap = |values: ArrayRef| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "k",
                DataType::List(Arc::new(Field::new(
                    "element",
                    values.data_type().clone(),
                    true,
                ))),
                true,
            )])),
            vec![Arc::new(ListArray::new(
                Arc::new(Field::new("element", values.data_type().clone(), true)),
                arrow::buffer::OffsetBuffer::new(vec![0, 2].into()),
                values,
                None,
            ))],
        )
        .unwrap()
    };
    let represented = wrap(values);
    let primitive = wrap(Arc::new(Int32Array::from(vec![7, 9])));
    let mut a = crate::flink_key::BinaryRowBatchEncoder::new(&represented, &[0], &[-1, -1]);
    let mut b = crate::flink_key::BinaryRowBatchEncoder::new(&primitive, &[0], &[-1, -1]);
    assert_eq!(a.encode(0), b.encode(0));
}
