use super::*;
use arrow::array::BinaryArray;

#[test]
fn max_rowtime_skips_nulls_and_floors_millis() {
    use arrow::array::TimestampNanosecondArray;
    let rowtime: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        Some(1_000_000_123i64), // 1.000000123s -> floors to 1000ms
        None,
        Some(999_999_999),
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        )])),
        vec![rowtime],
    )
    .unwrap();
    assert_eq!(1000, max_rowtime_millis(&batch, 0));
}

#[test]
fn max_rowtime_floors_pre_epoch_and_signals_all_null() {
    use arrow::array::TimestampNanosecondArray;
    // -1ns is inside the millisecond before the epoch: Flink's TimestampData stores it as
    // millisecond -1 (floor), not 0 (truncation toward zero).
    let pre_epoch: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![Some(-1i64)]));
    let all_null: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        true,
    )]));
    let pre = RecordBatch::try_new(schema.clone(), vec![pre_epoch]).unwrap();
    let none = RecordBatch::try_new(schema, vec![all_null]).unwrap();
    assert_eq!(-1, max_rowtime_millis(&pre, 0));
    assert_eq!(i64::MIN, max_rowtime_millis(&none, 0));
}

#[test]
fn max_rowtime_reads_epoch_millis_bigint_verbatim() {
    // A TO_TIMESTAMP_LTZ(col, 3) computed rowtime: the physical column already holds epoch millis.
    let millis: ArrayRef = Arc::new(Int64Array::from(vec![Some(90_000i64), None, Some(10_000)]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "dateTime",
            DataType::Int64,
            true,
        )])),
        vec![millis],
    )
    .unwrap();
    assert_eq!(90_000, max_rowtime_millis(&batch, 0));
}

fn sample_batch() -> RecordBatch {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 6, 3, 9]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 0, 8, 2]));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ])),
        vec![a, b],
    )
    .unwrap()
}

fn values(batch: &RecordBatch, column: usize) -> Vec<i64> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn json_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]))
}

fn bodies(docs: Vec<Option<&[u8]>>) -> RecordBatch {
    let column: ArrayRef = Arc::new(BinaryArray::from(docs));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "body",
            DataType::Binary,
            true,
        )])),
        vec![column],
    )
    .unwrap()
}

/// A hand-built `FileDescriptorSet` for `bench.Row { int64 id=1; string name=2; double score=3; }`
/// — what the JVM would serialize off the generated message class for Flink's `protobuf` format.
fn proto_descriptor_set() -> Vec<u8> {
    use prost_reflect::prost::Message;
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };
    let field = |name: &str, number: i32, ty: Type| FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    };
    let message = DescriptorProto {
        name: Some("Row".to_string()),
        field: vec![
            field("id", 1, Type::Int64),
            field("name", 2, Type::String),
            field("score", 3, Type::Double),
        ],
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("bench.proto".to_string()),
        package: Some("bench".to_string()),
        message_type: vec![message],
        syntax: Some("proto3".to_string()),
        ..Default::default()
    };
    FileDescriptorSet { file: vec![file] }.encode_to_vec()
}

// Each body is one bare protobuf message (no framing); ptars decodes the wire format straight into
// Arrow arrays, deriving the batch schema from the descriptor (columns named by proto field).
#[test]
fn protobuf_decode_emits_one_row_per_message() {
    use prost_reflect::prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};

    let descriptor = proto_descriptor_set();
    let message = DescriptorPool::decode(descriptor.as_ref())
        .unwrap()
        .get_message_by_name("bench.Row")
        .unwrap();
    let encode = |id: i64, name: &str, score: f64| {
        let mut m = DynamicMessage::new(message.clone());
        m.set_field_by_name("id", Value::I64(id));
        m.set_field_by_name("name", Value::String(name.to_string()));
        m.set_field_by_name("score", Value::F64(score));
        m.encode_to_vec()
    };
    let row0 = encode(1, "a", 1.5);
    let row1 = encode(2, "b", 2.5);
    let body = bodies(vec![Some(row0.as_slice()), Some(row1.as_slice())]);

    let out = ProtobufDecoder::new(&descriptor, "bench.Row").decode(&body);

    assert_eq!(out.num_rows(), 2);
    let id = out
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.values(), &[1, 2]);
    let names = out
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!((names.value(0), names.value(1)), ("a", "b"));
    let scores = out
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(scores.values(), &[1.5, 2.5]);
}

#[test]
#[should_panic(expected = "protobuf cannot deserialize a null Kafka value")]
fn protobuf_decode_rejects_a_tombstone_like_flink() {
    let descriptor = proto_descriptor_set();
    ProtobufDecoder::new(&descriptor, "bench.Row").decode(&bodies(vec![None]));
}

/// A descriptor set with the complex field shapes: `bench.Complex { int64 id=1; repeated int64
/// nums=2; map<string,int64> tags=3; bench.Row nested=4; }` (plus `bench.Row` from above).
fn proto_complex_descriptor_set() -> Vec<u8> {
    use prost_reflect::prost::Message;
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        MessageOptions,
    };
    let field = |name: &str, number: i32, ty: Type| FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    };
    let row = DescriptorProto {
        name: Some("Row".to_string()),
        field: vec![
            field("id", 1, Type::Int64),
            field("name", 2, Type::String),
            field("score", 3, Type::Double),
        ],
        ..Default::default()
    };
    let tags_entry = DescriptorProto {
        name: Some("TagsEntry".to_string()),
        field: vec![
            field("key", 1, Type::String),
            field("value", 2, Type::Int64),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let complex = DescriptorProto {
        name: Some("Complex".to_string()),
        field: vec![
            field("id", 1, Type::Int64),
            FieldDescriptorProto {
                label: Some(Label::Repeated as i32),
                ..field("nums", 2, Type::Int64)
            },
            FieldDescriptorProto {
                label: Some(Label::Repeated as i32),
                type_name: Some(".bench.Complex.TagsEntry".to_string()),
                ..field("tags", 3, Type::Message)
            },
            FieldDescriptorProto {
                type_name: Some(".bench.Row".to_string()),
                ..field("nested", 4, Type::Message)
            },
        ],
        nested_type: vec![tags_entry],
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("bench.proto".to_string()),
        package: Some("bench".to_string()),
        message_type: vec![row, complex],
        syntax: Some("proto3".to_string()),
        ..Default::default()
    };
    FileDescriptorSet { file: vec![file] }.encode_to_vec()
}

// Fields absent from the wire must decode exactly as Flink's protobuf format does with its default
// `read-default-values = false`: a NULL nested row, a NULL array, and a NULL map — not empty
// containers. (In proto3 an empty repeated/map field is indistinguishable from an absent one on the
// wire, and Flink's `getXCount() > 0` / `hasX()` guards leave the Flink field null in both cases, so
// null is the exact decode, not an approximation.)
#[test]
fn protobuf_decode_yields_null_for_absent_complex_fields_like_flink() {
    use arrow::array::Array;
    use prost_reflect::prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};

    let descriptor = proto_complex_descriptor_set();
    let message = DescriptorPool::decode(descriptor.as_ref())
        .unwrap()
        .get_message_by_name("bench.Complex")
        .unwrap();
    let mut only_id = DynamicMessage::new(message);
    only_id.set_field_by_name("id", Value::I64(7));
    let body = only_id.encode_to_vec();

    let out = ProtobufDecoder::new(&descriptor, "bench.Complex").decode(&bodies(vec![Some(&body)]));

    assert_eq!(out.num_rows(), 1);
    let column = |name: &str| out.column_by_name(name).unwrap();
    assert!(
        column("nested").is_null(0),
        "absent nested message must be NULL, got {:?}",
        column("nested")
    );
    assert!(
        column("nums").is_null(0),
        "absent repeated field must be NULL like Flink, got {:?}",
        column("nums")
    );
    assert!(
        column("tags").is_null(0),
        "absent map field must be NULL like Flink, got {:?}",
        column("tags")
    );
}

// Scalars absent from the wire are indistinguishable from proto3 defaults, and Flink force-reads
// defaults for proto3 primitives — so they must decode as "" / 0, never NULL.
#[test]
fn protobuf_decode_yields_proto3_defaults_for_absent_scalars_like_flink() {
    use arrow::array::Array;
    use prost_reflect::prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};

    let descriptor = proto_descriptor_set();
    let message = DescriptorPool::decode(descriptor.as_ref())
        .unwrap()
        .get_message_by_name("bench.Row")
        .unwrap();
    let mut only_id = DynamicMessage::new(message);
    only_id.set_field_by_name("id", Value::I64(7));
    let body = only_id.encode_to_vec();

    let out = ProtobufDecoder::new(&descriptor, "bench.Row").decode(&bodies(vec![Some(&body)]));

    let names = out
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert!(
        !names.is_null(0),
        "absent proto3 string must decode as \"\" like Flink, not NULL"
    );
    assert_eq!(names.value(0), "");
    let scores = out
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert!(
        !scores.is_null(0),
        "absent proto3 double must decode as 0.0 like Flink, not NULL"
    );
    assert_eq!(scores.value(0), 0.0);
}

// Encode must invert decode exactly: decode → encode → decode is identity, including the sparse row
// whose NULL nested/repeated/map columns must go back to unset fields. Maps carry one entry — the
// wire order of a multi-entry map is undefined on both sides (Flink putAll's a HashMap), so
// multi-entry equality is only meaningful read back AS a map (covered below).
#[test]
fn protobuf_encode_round_trips_through_the_decoder() {
    use prost_reflect::prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage, MapKey, Value};

    let descriptor = proto_complex_descriptor_set();
    let pool = DescriptorPool::decode(descriptor.as_slice()).unwrap();
    let mut nested = DynamicMessage::new(pool.get_message_by_name("bench.Row").unwrap());
    nested.set_field_by_name("id", Value::I64(1));
    nested.set_field_by_name("name", Value::String("a".to_string()));
    nested.set_field_by_name("score", Value::F64(1.5));
    let complex = pool.get_message_by_name("bench.Complex").unwrap();
    let mut full = DynamicMessage::new(complex.clone());
    full.set_field_by_name("id", Value::I64(7));
    full.set_field_by_name("nums", Value::List(vec![Value::I64(1), Value::I64(2)]));
    full.set_field_by_name(
        "tags",
        Value::Map([(MapKey::String("k".to_string()), Value::I64(9))].into()),
    );
    full.set_field_by_name("nested", Value::Message(nested));
    let mut sparse = DynamicMessage::new(complex);
    sparse.set_field_by_name("id", Value::I64(8));
    let (full, sparse) = (full.encode_to_vec(), sparse.encode_to_vec());

    let decoder = ProtobufDecoder::new(&descriptor, "bench.Complex");
    let first = decoder.decode(&bodies(vec![Some(&full), Some(&sparse)]));
    let encoded = ProtobufEncoder::new(&descriptor, "bench.Complex", "").encode(&first);
    let second = decoder.decode(&bodies(vec![
        Some(encoded.message(0)),
        Some(encoded.message(1)),
    ]));

    assert_eq!(first, second);
}

// Flink guards every field with `if(!rowData.isNullAt(i))` (PbCodegenRowSerializer#codegen), so a
// null column leaves the proto3 field unset — absent from the wire — and an all-null row is the
// empty byte[].
#[test]
fn protobuf_encode_leaves_null_columns_off_the_wire() {
    use arrow::array::Float64Array;
    use prost_reflect::prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};

    let descriptor = proto_descriptor_set();
    let batch = RecordBatch::try_new(
        json_schema(),
        vec![
            Arc::new(Int64Array::from(vec![Some(7), None])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            Arc::new(Float64Array::from(vec![None::<f64>, None])),
        ],
    )
    .unwrap();

    let encoded = ProtobufEncoder::new(&descriptor, "bench.Row", "").encode(&batch);

    let message = DescriptorPool::decode(descriptor.as_slice())
        .unwrap()
        .get_message_by_name("bench.Row")
        .unwrap();
    let mut expected = DynamicMessage::new(message);
    expected.set_field_by_name("id", Value::I64(7));
    assert_eq!(encoded.len(), 2);
    assert_eq!(encoded.message(0), expected.encode_to_vec().as_slice());
    assert_eq!(encoded.message(1), &[] as &[u8]);
}

/// A descriptor exercising every container shape the encoder substitutes defaults into:
/// `bench.Containers { repeated string words=1; map<string,int64> tags=2; repeated int64 nums=3;
/// repeated bench.Row rows=4; }` (plus `bench.Row` from above).
fn proto_containers_descriptor_set() -> Vec<u8> {
    use prost_reflect::prost::Message;
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        MessageOptions,
    };
    let field = |name: &str, number: i32, ty: Type, label: Label| FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    };
    let row = DescriptorProto {
        name: Some("Row".to_string()),
        field: vec![
            field("id", 1, Type::Int64, Label::Optional),
            field("name", 2, Type::String, Label::Optional),
            field("score", 3, Type::Double, Label::Optional),
        ],
        ..Default::default()
    };
    let tags_entry = DescriptorProto {
        name: Some("TagsEntry".to_string()),
        field: vec![
            field("key", 1, Type::String, Label::Optional),
            field("value", 2, Type::Int64, Label::Optional),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let containers = DescriptorProto {
        name: Some("Containers".to_string()),
        field: vec![
            field("words", 1, Type::String, Label::Repeated),
            FieldDescriptorProto {
                type_name: Some(".bench.Containers.TagsEntry".to_string()),
                ..field("tags", 2, Type::Message, Label::Repeated)
            },
            field("nums", 3, Type::Int64, Label::Repeated),
            FieldDescriptorProto {
                type_name: Some(".bench.Row".to_string()),
                ..field("rows", 4, Type::Message, Label::Repeated)
            },
        ],
        nested_type: vec![tags_entry],
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("bench.proto".to_string()),
        package: Some("bench".to_string()),
        message_type: vec![row, containers],
        syntax: Some("proto3".to_string()),
        ..Default::default()
    };
    FileDescriptorSet { file: vec![file] }.encode_to_vec()
}

/// A `map<string,int64>` column with nullable keys and values, which arrow's MapBuilder cannot
/// produce but a transposed Flink MapData can carry.
fn string_int_map(rows: Vec<Option<Vec<(Option<&str>, Option<i64>)>>>) -> MapArray {
    let entry_fields: Fields = vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]
    .into();
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut offsets = vec![0i32];
    let mut nulls = NullBufferBuilder::new(rows.len());
    for row in rows {
        match row {
            Some(entries) => {
                for (key, value) in entries {
                    keys.push(key);
                    values.push(value);
                }
                nulls.append_non_null();
            }
            None => nulls.append_null(),
        }
        offsets.push(keys.len() as i32);
    }
    let entries = StructArray::new(
        entry_fields.clone(),
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
        None,
    );
    MapArray::new(
        Arc::new(Field::new("entries", DataType::Struct(entry_fields), false)),
        OffsetBuffer::new(offsets.into()),
        entries,
        nulls.finish(),
        false,
    )
}

// Protobuf containers cannot hold nulls, so Flink substitutes type defaults — 0 for ints, the
// write-null-string-literal for strings (keys included), the default instance for messages —
// per PbCodegenUtils#pbDefaultValueCode / #convertFlinkArrayElementToPbWithDefaultValueCode.
#[test]
fn protobuf_encode_substitutes_defaults_for_nulls_inside_containers() {
    use arrow::array::builder::ListBuilder;
    use arrow::array::Float64Array;
    use arrow::datatypes::Int64Type;
    use prost_reflect::{DescriptorPool, DynamicMessage, MapKey, Value};

    let mut words = ListBuilder::new(StringBuilder::new());
    words.values().append_value("x");
    words.values().append_null();
    words.append(true);
    let words = words.finish();
    let tags = string_int_map(vec![Some(vec![(Some("k"), None), (None, Some(5))])]);
    let nums = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(5), None])]);
    let row_fields: Fields = vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]
    .into();
    let rows = ListArray::new(
        Arc::new(Field::new(
            "item",
            DataType::Struct(row_fields.clone()),
            true,
        )),
        OffsetBuffer::new(vec![0i32, 1].into()),
        Arc::new(StructArray::new_null(row_fields, 1)),
        None,
    );
    let schema = Schema::new(vec![
        Field::new("words", words.data_type().clone(), true),
        Field::new("tags", tags.data_type().clone(), true),
        Field::new("nums", nums.data_type().clone(), true),
        Field::new("rows", rows.data_type().clone(), true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(words),
            Arc::new(tags),
            Arc::new(nums),
            Arc::new(rows),
        ],
    )
    .unwrap();

    let descriptor = proto_containers_descriptor_set();
    let encoded = ProtobufEncoder::new(&descriptor, "bench.Containers", "NULL").encode(&batch);

    let pool = DescriptorPool::decode(descriptor.as_slice()).unwrap();
    let message = pool.get_message_by_name("bench.Containers").unwrap();
    let decoded = DynamicMessage::decode(message, encoded.message(0)).unwrap();
    assert_eq!(
        decoded.get_field_by_name("words").unwrap().as_ref(),
        &Value::List(vec![
            Value::String("x".to_string()),
            Value::String("NULL".to_string())
        ]),
    );
    assert_eq!(
        decoded.get_field_by_name("tags").unwrap().as_ref(),
        &Value::Map(
            [
                (MapKey::String("k".to_string()), Value::I64(0)),
                (MapKey::String("NULL".to_string()), Value::I64(5)),
            ]
            .into()
        ),
    );
    assert_eq!(
        decoded.get_field_by_name("nums").unwrap().as_ref(),
        &Value::List(vec![Value::I64(5), Value::I64(0)]),
    );
    let default_row = DynamicMessage::new(pool.get_message_by_name("bench.Row").unwrap());
    assert_eq!(
        decoded.get_field_by_name("rows").unwrap().as_ref(),
        &Value::List(vec![Value::Message(default_row)]),
    );
}

// sint fields differ from int fields only in wire encoding (zigzag); the descriptor drives it, so
// the encoder sets plain i32/i64 values and the bytes must come out zigzag-coded.
#[test]
fn protobuf_encode_zigzag_encodes_sint_fields() {
    use prost_reflect::prost::Message;
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};

    let field = |name: &str, number: i32, ty: Type| FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        ..Default::default()
    };
    let message = DescriptorProto {
        name: Some("Zigzag".to_string()),
        field: vec![field("s32", 1, Type::Sint32), field("s64", 2, Type::Sint64)],
        ..Default::default()
    };
    let file = FileDescriptorProto {
        name: Some("bench.proto".to_string()),
        package: Some("bench".to_string()),
        message_type: vec![message],
        syntax: Some("proto3".to_string()),
        ..Default::default()
    };
    let descriptor = FileDescriptorSet { file: vec![file] }.encode_to_vec();

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("s32", DataType::Int32, true),
            Field::new("s64", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![-1])),
            Arc::new(Int64Array::from(vec![-2])),
        ],
    )
    .unwrap();

    let encoded = ProtobufEncoder::new(&descriptor, "bench.Zigzag", "").encode(&batch);

    let message = DescriptorPool::decode(descriptor.as_slice())
        .unwrap()
        .get_message_by_name("bench.Zigzag")
        .unwrap();
    let mut expected = DynamicMessage::new(message);
    expected.set_field_by_name("s32", Value::I32(-1));
    expected.set_field_by_name("s64", Value::I64(-2));
    assert_eq!(encoded.message(0), expected.encode_to_vec().as_slice());
    assert_eq!(encoded.message(0), &[0x08, 0x01, 0x10, 0x03]);
}

// A non-null nested row builds and sets the nested message (so it is present on the wire even when
// empty), while its null fields stay unset inside — the same `if(!rowData.isNullAt(i))` guard
// applies at every nesting level of PbCodegenRowSerializer.
#[test]
fn protobuf_encode_recurses_into_nested_messages() {
    use arrow::array::Float64Array;
    use prost_reflect::prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};

    let descriptor = proto_complex_descriptor_set();
    let row_fields: Fields = vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]
    .into();
    let nested = StructArray::new(
        row_fields.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Float64Array::from(vec![2.5])),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "nested",
            DataType::Struct(row_fields),
            true,
        )])),
        vec![Arc::new(nested)],
    )
    .unwrap();

    let encoded = ProtobufEncoder::new(&descriptor, "bench.Complex", "").encode(&batch);

    let pool = DescriptorPool::decode(descriptor.as_slice()).unwrap();
    let mut row = DynamicMessage::new(pool.get_message_by_name("bench.Row").unwrap());
    row.set_field_by_name("id", Value::I64(1));
    row.set_field_by_name("score", Value::F64(2.5));
    let mut expected = DynamicMessage::new(pool.get_message_by_name("bench.Complex").unwrap());
    expected.set_field_by_name("nested", Value::Message(row));
    assert_eq!(encoded.message(0), expected.encode_to_vec().as_slice());
}

// A present-but-empty ARRAY/MAP sets an empty repeated field (Flink addAll/putAll of an empty
// collection), which proto3 leaves off the wire entirely — consistent with the decode side, where
// empty and absent containers are indistinguishable and both normalize to NULL.
#[test]
fn protobuf_encode_writes_empty_containers_as_absent() {
    use arrow::datatypes::Int64Type;
    use prost_reflect::prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};

    let nums = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![])]);
    let tags = string_int_map(vec![Some(vec![])]);
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("nums", nums.data_type().clone(), true),
        Field::new("tags", tags.data_type().clone(), true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(nums),
            Arc::new(tags),
        ],
    )
    .unwrap();

    let descriptor = proto_complex_descriptor_set();
    let encoded = ProtobufEncoder::new(&descriptor, "bench.Complex", "").encode(&batch);

    let message = DescriptorPool::decode(descriptor.as_slice())
        .unwrap()
        .get_message_by_name("bench.Complex")
        .unwrap();
    let mut expected = DynamicMessage::new(message);
    expected.set_field_by_name("id", Value::I64(7));
    assert_eq!(encoded.message(0), expected.encode_to_vec().as_slice());
}

// Column types outside the descriptor gate (here an Int8 against an int64 field) must fail loudly:
// the sink wiring only routes gated shapes, so anything else is a planning bug, not data.
#[test]
#[should_panic(expected = "cannot encode from an Arrow Int8 column")]
fn protobuf_encode_rejects_column_types_outside_the_gate() {
    let descriptor = proto_descriptor_set();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int8, true)])),
        vec![Arc::new(Int8Array::from(vec![1i8]))],
    )
    .unwrap();
    ProtobufEncoder::new(&descriptor, "bench.Row", "").encode(&batch);
}

// Each body is one CSV record (no header); CSV decode (format 2) emits one typed row per record.
#[test]
fn csv_decode_emits_one_row_per_record() {
    let body = bodies(vec![Some(b"1,a,1.5"), Some(b"2,b,2.5")]);
    let out = MessageDecoder::new(FORMAT_CSV, json_schema(), "", "", 0, false, "").decode(&body);
    assert_eq!(out.num_rows(), 2);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2]);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!((names.value(0), names.value(1)), ("a", "b"));
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(scores.values(), &[1.5, 2.5]);
}

/// Keyed decode: the raw Kafka key composes with the value decode per record — Flink's key/value
/// merge with the raw key format's exactly-one key row. A JSON value fanning a top-level array
/// into N rows repeats the record's key N times; a dropped record (skip mode) contributes nothing;
/// a NULL Kafka key keeps the record with a NULL key column (raw's null-key rule); and the key
/// column position interleaves with the value positions in physical schema order.
#[test]
fn keyed_decode_composes_raw_keys_with_the_value_rows() {
    use arrow::array::{Array, StringArray};

    // Physical schema: [k BIGINT (the key, position 0), id BIGINT, name STRING] — EXCEPT_KEY, so
    // the value decode owns positions 1 and 2.
    let physical: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let keys: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(7i64.to_be_bytes().as_slice()),
        Some(8i64.to_be_bytes().as_slice()),
        None,
        Some(9i64.to_be_bytes().as_slice()),
    ]));
    let bodies: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(br#"{"id": 1, "name": "a"}"#.as_slice()),
        // A top-level array fans out into two rows sharing record 1's key.
        Some(br#"[{"id": 2, "name": "b"}, {"id": 3, "name": "c"}]"#.as_slice()),
        // A null Kafka key keeps the record: raw decodes it to a NULL key column.
        Some(br#"{"id": 4, "name": "d"}"#.as_slice()),
        // A malformed body drops the whole record in skip mode — key and all.
        Some(b"not json".as_slice()),
    ]));
    let records = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, true),
            Field::new("body", DataType::Binary, true),
        ])),
        vec![keys, bodies],
    )
    .unwrap();

    let decoder = MessageDecoder::new(
        FORMAT_JSON,
        physical,
        "",
        "",
        0,
        true,
        "keyed.key-position=0\nkeyed.value-positions=1,2\n",
    );
    let out = decoder.decode(&records);

    assert_eq!(out.num_rows(), 4);
    let k = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((k.value(0), k.value(1), k.value(2)), (7, 8, 8));
    assert!(k.is_null(3), "a null Kafka key must stay a NULL key column");
    let id = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 3, 4]);
    let names = out
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(3), "d");
}

// `raw` (format 3): the body bytes pass through as the single column, cast to the declared type.
#[test]
fn raw_decode_passes_bytes_through() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Utf8,
        true,
    )]));
    let body = bodies(vec![Some(b"hello"), Some(b"world")]);
    let out = MessageDecoder::new(FORMAT_RAW, schema, "", "", 0, false, "").decode(&body);
    assert_eq!(out.num_rows(), 2);
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!((col.value(0), col.value(1)), ("hello", "world"));
}

fn raw_decode(data_type: DataType, docs: Vec<Option<&[u8]>>, options: &str) -> RecordBatch {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("payload", data_type, true)]));
    MessageDecoder::new(FORMAT_RAW, schema, "", "", 0, false, options).decode(&bodies(docs))
}

// Raw fixed-width numerics read the exact-length message with the configured endianness
// (big-endian is Flink's default), and a null body stays a null field.
#[test]
fn raw_decode_reads_fixed_width_values_with_the_configured_endianness() {
    use arrow::array::{
        Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array,
    };
    let out = raw_decode(
        DataType::Int32,
        vec![Some(&0x12345678i32.to_be_bytes()), None],
        "",
    );
    let col = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(col.value(0), 0x12345678);
    assert!(col.is_null(1));

    let little = "raw.endianness=little-endian\n";
    let out = raw_decode(
        DataType::Int32,
        vec![Some(&0x12345678i32.to_le_bytes())],
        little,
    );
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        0x12345678
    );

    let out = raw_decode(DataType::Int16, vec![Some(&(-2i16).to_be_bytes())], "");
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        -2
    );
    let out = raw_decode(DataType::Int16, vec![Some(&(-2i16).to_le_bytes())], little);
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        -2
    );

    let out = raw_decode(DataType::Int64, vec![Some(&i64::MIN.to_be_bytes())], "");
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MIN
    );
    let out = raw_decode(DataType::Int64, vec![Some(&i64::MIN.to_le_bytes())], little);
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MIN
    );

    let out = raw_decode(DataType::Float32, vec![Some(&1.5f32.to_be_bytes())], "");
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0),
        1.5
    );
    let out = raw_decode(DataType::Float32, vec![Some(&1.5f32.to_le_bytes())], little);
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0),
        1.5
    );

    let out = raw_decode(DataType::Float64, vec![Some(&(-2.25f64).to_be_bytes())], "");
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        -2.25
    );
    let out = raw_decode(
        DataType::Float64,
        vec![Some(&(-2.25f64).to_le_bytes())],
        little,
    );
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        -2.25
    );

    // The one-byte types ignore endianness: TINYINT is the signed byte, BOOLEAN is `byte != 0`.
    let out = raw_decode(DataType::Int8, vec![Some(&[0xff]), Some(&[0x7f])], little);
    let col = out.column(0).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!((col.value(0), col.value(1)), (-1, 127));
    let out = raw_decode(
        DataType::Boolean,
        vec![Some(&[0]), Some(&[1]), Some(&[2]), None],
        "",
    );
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!(
        (col.value(0), col.value(1), col.value(2)),
        (false, true, true)
    );
    assert!(col.is_null(3));
}

// A wrong-length message fails the decode with Flink's own DeserializationException text
// (raw has no ignore-parse-errors; the job fails exactly as Flink's does).
#[test]
#[should_panic(expected = "Size of data received for deserializing INT type is not 4.")]
fn raw_decode_rejects_a_wrong_length_message_like_flink() {
    raw_decode(DataType::Int32, vec![Some(&[1, 2, 3])], "");
}

#[test]
#[should_panic(expected = "Size of data received for deserializing BOOLEAN type is not 1.")]
fn raw_decode_rejects_an_empty_boolean_message_like_flink() {
    raw_decode(DataType::Boolean, vec![Some(&[])], "");
}

// Invalid UTF-8 under a STRING column fails the decode loudly (the documented divergence: Flink
// smuggles the bytes through StringData unvalidated, Arrow strings cannot) — never a silent NULL.
#[test]
#[should_panic(expected = "raw format STRING message is not valid UTF-8")]
fn raw_decode_rejects_invalid_utf8_strings() {
    raw_decode(
        DataType::Utf8,
        vec![Some(b"ok"), Some(&[0xff, 0xfe, 0x68, 0x69])],
        "",
    );
}

// Binary columns take the message verbatim at any length (including empty), nulls staying null.
#[test]
fn raw_decode_passes_binary_bodies_verbatim() {
    use arrow::array::Array;
    let out = raw_decode(
        DataType::Binary,
        vec![Some(&[0xde, 0xad, 0xbe, 0xef]), Some(&[]), None],
        "",
    );
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(col.value(0), &[0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(col.value(1), b"");
    assert!(col.is_null(2));
}

// Bare Avro (format 4): each body is a raw datum (no Confluent framing), decoded against the reader
// schema we register at synthetic id 0 (the decoder prepends the id-0 header internally).
#[test]
fn bare_avro_decode_emits_one_row_per_datum() {
    // Avro binary datum for record { long id; string name; double score }, no framing.
    fn zigzag_varint(n: i64) -> Vec<u8> {
        let mut zz = ((n << 1) ^ (n >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            let mut b = (zz & 0x7f) as u8;
            zz >>= 7;
            if zz != 0 {
                b |= 0x80;
            }
            out.push(b);
            if zz == 0 {
                break;
            }
        }
        out
    }
    fn datum(id: i64, name: &str, score: f64) -> Vec<u8> {
        let mut v = zigzag_varint(id);
        v.extend(zigzag_varint(name.len() as i64));
        v.extend_from_slice(name.as_bytes());
        v.extend_from_slice(&score.to_le_bytes());
        v
    }
    let reader_schema = r#"{"type":"record","name":"Row","fields":[
            {"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"score","type":"double"}]}"#;
    let m0 = datum(1, "a", 1.5);
    let m1 = datum(2, "b", 2.5);
    let body = bodies(vec![Some(m0.as_slice()), Some(m1.as_slice())]);

    let out = MessageDecoder::new(
        FORMAT_AVRO,
        Arc::new(Schema::empty()),
        reader_schema,
        "",
        0,
        false,
        "",
    )
    .decode(&body);

    assert_eq!(out.num_rows(), 2);
    let id = out
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.values(), &[1, 2]);
    let names = out
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!((names.value(0), names.value(1)), ("a", "b"));
}

// Byte-exact decimal division/modulo: every expected value below was produced by running Java's
// own BigDecimal pipeline (divide with MathContext(38, HALF_UP), then setScale(s, HALF_UP), with
// DecimalData.fromBigDecimal's precision check) — the exact code Flink's runtime executes.
#[test]
fn decimal_divide_matches_bigdecimal() {
    fn div(a: i128, s1: i8, b: i128, s2: i8, p: u8, s: i8) -> Option<i128> {
        let (unscaled, scale) = quotient_38_digits(a, s1, b, s2);
        rescale_half_up(unscaled, scale, p, s)
    }
    // 7.00 / 3.00 → DECIMAL(23,13): the repeating quotient rounds at the declared scale.
    assert_eq!(div(700, 2, 300, 2, 23, 13), Some(23333333333333));
    // 2 / 3 → DECIMAL(38,6): the 38-significant-digit intermediate then rescales with HALF_UP.
    assert_eq!(div(2, 0, 3, 0, 38, 6), Some(666667));
    // Negative dividend: HALF_UP rounds away from zero.
    assert_eq!(div(-700, 2, 300, 2, 23, 13), Some(-23333333333333));
    assert_eq!(div(1, 0, 3, 0, 10, 2), Some(33));
    // 10.4 / 0.03 → 346.666667 (rounded up at the target scale).
    assert_eq!(div(104, 1, 3, 2, 12, 6), Some(346666667));
    // 99999999999999999999.5 / 0.1: an exact 21-digit quotient, rescaled to 22 digits — fits.
    assert_eq!(
        div(999999999999999999995, 1, 1, 1, 22, 1),
        Some(9999999999999999999950)
    );
    assert_eq!(div(0, 2, 525, 2, 23, 13), Some(0));
    // A quotient needing more digits than the declared precision reports NULL, like
    // DecimalData.fromBigDecimal.
    assert_eq!(
        div(123456789012345678901234567890123456, 6, 1, 6, 38, 6),
        None
    );
}

#[test]
fn decimal_mod_matches_bigdecimal() {
    fn modulo(a: i128, s1: i8, b: i128, s2: i8, p: u8, s: i8) -> Option<i128> {
        let (unscaled, scale) = remainder_exact(a, s1, b, s2);
        rescale_half_up(unscaled, scale, p, s)
    }
    // 7.5 % 2.1 = 1.2; the sign follows the dividend (Java remainder), the divisor's sign is
    // irrelevant.
    assert_eq!(modulo(75, 1, 21, 1, 12, 6), Some(1_200_000));
    assert_eq!(modulo(-75, 1, 21, 1, 12, 6), Some(-1_200_000));
    assert_eq!(modulo(75, 1, -21, 1, 12, 6), Some(1_200_000));
    // Mixed scales: 5.75 % 0.50 = 0.25.
    assert_eq!(modulo(575, 2, 50, 2, 12, 6), Some(250_000));
}

// Confluent Avro (format 1), registry-driven: the store starts empty, writer schemas arrive by id
// (as the JVM fetches them from the schema registry), and each message resolves against the reader
// schema. Covers the two things the single-schema path never exercised: a mid-batch schema-id
// switch (the decoder flushes internally; the flushes concatenate under the one reader shape) and
// a writer record named differently from the reader (the JVM rebuilds the fetched schema onto the
// reader's record names — mirroring Avro Java's lenient name check — and arrow-avro also accepts
// the historical alias form pinned here).
#[test]
fn confluent_avro_decodes_evolving_writer_schemas_against_reader() {
    fn zigzag_varint(n: i64) -> Vec<u8> {
        let mut zz = ((n << 1) ^ (n >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            let mut b = (zz & 0x7f) as u8;
            zz >>= 7;
            if zz != 0 {
                b |= 0x80;
            }
            out.push(b);
            if zz == 0 {
                break;
            }
        }
        out
    }
    fn string_field(s: &str) -> Vec<u8> {
        let mut v = zigzag_varint(s.len() as i64);
        v.extend_from_slice(s.as_bytes());
        v
    }
    fn framed(id: u32, datum: Vec<u8>) -> Vec<u8> {
        let mut v = vec![0x00];
        v.extend_from_slice(&id.to_be_bytes());
        v.extend(datum);
        v
    }
    let reader = r#"{"type":"record","name":"record","namespace":"org.apache.flink.avro.generated","fields":[
            {"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
    // Writer 7: a producer-named record with an extra trailing field the reader drops; the JVM
    // patches in the reader's full name as an alias so arrow-avro's name check passes (Avro Java
    // skips that check entirely).
    let writer_v1 = r#"{"type":"record","name":"User","namespace":"com.example",
            "aliases":["org.apache.flink.avro.generated.record"],"fields":[
            {"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"extra","type":"string"}]}"#;
    // Writer 9: evolved — fields reordered; resolution matches them by name.
    let writer_v2 = r#"{"type":"record","name":"UserV2","namespace":"com.example",
            "aliases":["org.apache.flink.avro.generated.record"],"fields":[
            {"name":"name","type":"string"},{"name":"id","type":"long"}]}"#;

    let mut decoder = MessageDecoder::new(
        FORMAT_AVRO_CONFLUENT,
        Arc::new(Schema::empty()),
        "",
        reader,
        0,
        false,
        "",
    );
    decoder.register_writer_schema(7, writer_v1);
    decoder.register_writer_schema(9, writer_v2);

    let mut d1 = zigzag_varint(1);
    d1.extend(string_field("a"));
    d1.extend(string_field("dropped"));
    let mut d2 = string_field("b");
    d2.extend(zigzag_varint(2));
    let mut d3 = zigzag_varint(3);
    d3.extend(string_field("c"));
    d3.extend(string_field("dropped"));
    let (m1, m2, m3) = (framed(7, d1), framed(9, d2), framed(7, d3));
    let body = bodies(vec![Some(&m1), Some(&m2), Some(&m3)]);

    let out = decoder.decode(&body);

    assert_eq!(out.num_rows(), 3);
    let id = out
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(id.values(), &[1, 2, 3]);
    let names = out
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(
        (names.value(0), names.value(1), names.value(2)),
        ("a", "b", "c")
    );
}

// --- debezium-avro-confluent (format 10): the Debezium envelope with Confluent-framed Avro
// bodies. The reader schema below is the shape Flink derives from the envelope row type
// ROW<before <physical>.nullable(), after <physical>.nullable(), op STRING>. The writer text is
// the wire contract the JVM registers after `ConfluentSchemaRegistry.alignedToReader`: the fetched
// Debezium schema's records rebuilt onto the reader's names — one inline copy per image position
// (the registry schema references its single Value record for both before and after) — with the
// writer's exact field layout kept, including envelope fields (source, ts_ms) and image fields
// (internal) the reader resolves away.

const DBZ_AVRO_READER: &str = r#"{"type":"record","name":"record","namespace":"org.apache.flink.avro.generated","fields":[
    {"name":"before","type":["null",{"type":"record","name":"record_before","fields":[
        {"name":"id","type":["null","long"],"default":null},
        {"name":"name","type":["null","string"],"default":null},
        {"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null}]}],"default":null},
    {"name":"after","type":["null",{"type":"record","name":"record_after","fields":[
        {"name":"id","type":["null","long"],"default":null},
        {"name":"name","type":["null","string"],"default":null},
        {"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null}]}],"default":null},
    {"name":"op","type":["null","string"],"default":null}]}"#;

const DBZ_AVRO_WRITER: &str = r#"{"type":"record","name":"record","namespace":"org.apache.flink.avro.generated","fields":[
    {"name":"before","type":["null",{"type":"record","name":"record_before","fields":[
        {"name":"id","type":["null","long"],"default":null},
        {"name":"name","type":["null","string"],"default":null},
        {"name":"internal","type":["null","string"],"default":null},
        {"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null}]}],"default":null},
    {"name":"after","type":["null",{"type":"record","name":"record_after","fields":[
        {"name":"id","type":["null","long"],"default":null},
        {"name":"name","type":["null","string"],"default":null},
        {"name":"internal","type":["null","string"],"default":null},
        {"name":"ts","type":["null",{"type":"long","logicalType":"timestamp-millis"}],"default":null}]}],"default":null},
    {"name":"source","type":["null","string"],"default":null},
    {"name":"op","type":"string"},
    {"name":"ts_ms","type":["null","long"],"default":null}]}"#;

fn dbz_zigzag(n: i64) -> Vec<u8> {
    let mut zz = ((n << 1) ^ (n >> 63)) as u64;
    let mut out = Vec::new();
    loop {
        let mut b = (zz & 0x7f) as u8;
        zz >>= 7;
        if zz != 0 {
            b |= 0x80;
        }
        out.push(b);
        if zz == 0 {
            break;
        }
    }
    out
}

fn dbz_string(s: &str) -> Vec<u8> {
    let mut v = dbz_zigzag(s.len() as i64);
    v.extend_from_slice(s.as_bytes());
    v
}

/// One fully-populated writer image datum in `DBZ_AVRO_WRITER`'s layout: id, name, the
/// writer-only `internal` field the reader skips, then ts — every field on union branch 1.
fn dbz_image(id: i64, name: &str, ts_millis: i64) -> Vec<u8> {
    let mut v = dbz_zigzag(1);
    v.extend(dbz_zigzag(id));
    v.extend(dbz_zigzag(1));
    v.extend(dbz_string(name));
    v.extend(dbz_zigzag(1));
    v.extend(dbz_string("writer-only"));
    v.extend(dbz_zigzag(1));
    v.extend(dbz_zigzag(ts_millis));
    v
}

/// A Confluent-framed writer envelope datum in `DBZ_AVRO_WRITER`'s field order, with a populated
/// `source` and a null `ts_ms` for the reader to resolve away.
fn dbz_message(
    schema_id: u32,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
    op: &str,
) -> Vec<u8> {
    let mut v = vec![0x00];
    v.extend_from_slice(&schema_id.to_be_bytes());
    for image in [before, after] {
        match image {
            None => v.extend(dbz_zigzag(0)),
            Some(bytes) => {
                v.extend(dbz_zigzag(1));
                v.extend(bytes);
            }
        }
    }
    v.extend(dbz_zigzag(1));
    v.extend(dbz_string("dbz-source"));
    v.extend(dbz_string(op));
    v.extend(dbz_zigzag(0));
    v
}

fn dbz_decoder() -> MessageDecoder {
    let physical = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        ),
    ]));
    let mut decoder = MessageDecoder::new(
        FORMAT_DEBEZIUM_AVRO_CONFLUENT,
        physical,
        "",
        DBZ_AVRO_READER,
        0,
        false,
        "",
    );
    decoder.register_writer_schema(5, DBZ_AVRO_WRITER);
    decoder
}

// The Avro envelope fans out exactly like the JSON dialect — c/r → INSERT from `after`, u →
// UPDATE_BEFORE + UPDATE_AFTER, d → DELETE from `before` — while null and empty bodies (Kafka
// tombstones, which Flink returns on without collecting) contribute no rows, and the image
// payloads land on the boundary column types (the timestamp long reads as epoch millis and scales
// to the boundary's nanoseconds, exactly like a plain avro-confluent column).
#[test]
fn debezium_avro_decode_emits_changelog() {
    let insert = dbz_message(5, None, Some(dbz_image(1, "a", 1_000)), "c");
    let read = dbz_message(5, None, Some(dbz_image(2, "b", 2_000)), "r");
    let update = dbz_message(
        5,
        Some(dbz_image(2, "b", 2_000)),
        Some(dbz_image(2, "b2", 3_000)),
        "u",
    );
    let delete = dbz_message(5, Some(dbz_image(1, "a", 1_000)), None, "d");
    let body = bodies(vec![
        Some(&insert),
        None,
        Some(&read),
        Some(&[]),
        Some(&update),
        Some(&delete),
    ]);

    let out = dbz_decoder().decode(&body);

    assert_eq!(out.num_rows(), 5);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 2, 2, 1]);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        (0..5).map(|i| names.value(i)).collect::<Vec<_>>(),
        vec!["a", "b", "b", "b2", "a"]
    );
    let ts = out
        .column(2)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(
        ts.values(),
        &[
            1_000_000_000,
            2_000_000_000,
            2_000_000_000,
            3_000_000_000,
            1_000_000_000
        ]
    );
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 0, 1, 2, 3]);
    assert_eq!(out.schema().field(3).name(), ROW_KIND_COLUMN);
}

// Schema evolution mid-batch: a second registered writer (aligned the same way) whose images lack
// the `ts` field — the reader default (null) fills it — interleaved with the first writer in one
// batch (the decoder flushes internally on the id switch; the flushes concatenate under the one
// reader shape).
#[test]
fn debezium_avro_decodes_evolving_writer_schemas() {
    let writer_v2 = r#"{"type":"record","name":"record","namespace":"org.apache.flink.avro.generated","fields":[
        {"name":"before","type":["null",{"type":"record","name":"record_before","fields":[
            {"name":"id","type":["null","long"],"default":null},
            {"name":"name","type":["null","string"],"default":null}]}],"default":null},
        {"name":"after","type":["null",{"type":"record","name":"record_after","fields":[
            {"name":"id","type":["null","long"],"default":null},
            {"name":"name","type":["null","string"],"default":null}]}],"default":null},
        {"name":"op","type":"string"}]}"#;
    // Writer-v2 envelope: {before, after, op} only, images without ts.
    let v2_message = |before: Option<Vec<u8>>, after: Option<Vec<u8>>, op: &str| {
        let mut v = vec![0x00];
        v.extend_from_slice(&9u32.to_be_bytes());
        for image in [before, after] {
            match image {
                None => v.extend(dbz_zigzag(0)),
                Some(bytes) => {
                    v.extend(dbz_zigzag(1));
                    v.extend(bytes);
                }
            }
        }
        v.extend(dbz_string(op));
        v
    };
    let v2_image = |id: i64, name: &str| {
        let mut v = dbz_zigzag(1);
        v.extend(dbz_zigzag(id));
        v.extend(dbz_zigzag(1));
        v.extend(dbz_string(name));
        v
    };
    let mut decoder = dbz_decoder();
    decoder.register_writer_schema(9, writer_v2);

    let m1 = dbz_message(5, None, Some(dbz_image(1, "a", 1_000)), "c");
    let m2 = v2_message(None, Some(v2_image(2, "b")), "c");
    let m3 = dbz_message(5, Some(dbz_image(1, "a", 1_000)), None, "d");
    let out = decoder.decode(&bodies(vec![Some(&m1), Some(&m2), Some(&m3)]));

    assert_eq!(out.num_rows(), 3);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 1]);
    let ts = out
        .column(2)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert!(!ts.is_null(0) && ts.is_null(1) && !ts.is_null(2));
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 0, 3]);
}

// A null "before" where the op reads it fails the job (Flink's REPLICA IDENTITY error path — this
// format has no ignore-parse-errors, so there is no skip mode to fall into).
#[test]
#[should_panic(expected = "null \"before\"")]
fn debezium_avro_null_before_update_fails() {
    let update = dbz_message(5, None, Some(dbz_image(2, "b", 2_000)), "u");
    dbz_decoder().decode(&bodies(vec![Some(&update)]));
}

#[test]
#[should_panic(expected = "null \"before\"")]
fn debezium_avro_null_before_delete_fails() {
    let delete = dbz_message(5, None, Some(dbz_image(2, "b", 2_000)), "d");
    dbz_decoder().decode(&bodies(vec![Some(&delete)]));
}

// An unrecognized op fails, matching Flink's IOException on an unknown "op" value.
#[test]
#[should_panic(expected = "unknown CDC operation")]
fn debezium_avro_unknown_op_fails() {
    let unknown = dbz_message(5, None, Some(dbz_image(1, "a", 1_000)), "t");
    dbz_decoder().decode(&bodies(vec![Some(&unknown)]));
}

// Debezium JSON (format 6): the `{before, after, op}` envelope fans out to a columnar changelog —
// c/r → one INSERT row from `after`, u → UPDATE_BEFORE (from `before`) + UPDATE_AFTER (from `after`),
// d → one DELETE row from `before` — with each row's `RowKind` on the trailing `$row_kind$` column.
#[test]
fn cdc_debezium_decode_emits_changelog() {
    let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op":"c","ts_ms":7}"#;
    let update =
        br#"{"before":{"id":2,"name":"b","score":2.5},"after":{"id":2,"name":"b2","score":3.5},"op":"u"}"#;
    let delete = br#"{"before":{"id":3,"name":"c","score":4.5},"after":null,"op":"d"}"#;
    let body = bodies(vec![Some(insert.as_slice()), Some(update), Some(delete)]);

    let out = MessageDecoder::new(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, false, "")
        .decode(&body);

    // 1 (insert) + 2 (update) + 1 (delete) physical rows.
    assert_eq!(out.num_rows(), 4);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 2, 3]);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(
        (0..4).map(|i| names.value(i)).collect::<Vec<_>>(),
        vec!["a", "b", "b2", "c"]
    );
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(scores.values(), &[1.5, 2.5, 3.5, 4.5]);
    // INSERT(0), UPDATE_BEFORE(1), UPDATE_AFTER(2), DELETE(3) — Flink's RowKind byte values.
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 1, 2, 3]);
    assert_eq!(out.schema().field(3).name(), ROW_KIND_COLUMN);
}

// A tombstone (null or zero-length body) is dropped, leaving the valid records — matching Flink,
// which skips them regardless of error handling.
#[test]
fn cdc_debezium_skips_tombstone() {
    let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op":"r"}"#;
    let body = bodies(vec![None, Some(b"".as_slice()), Some(insert.as_slice())]);

    let out = MessageDecoder::new(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, false, "")
        .decode(&body);

    assert_eq!(out.num_rows(), 1);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1]);
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0]); // "r" snapshot read → INSERT
}

// Flink's tombstone check is `message.length == 0` — a whitespace-only body reaches Jackson,
// yields no envelope, and the op read NPEs into "Corrupt ... JSON message": a job failure in
// default mode, never a silent skip.
#[test]
#[should_panic(expected = "Corrupt Debezium JSON message ' \n\t '.")]
fn cdc_whitespace_body_is_not_a_tombstone() {
    let body = bodies(vec![Some(b" \n\t ".as_slice())]);
    MessageDecoder::new(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, false, "").decode(&body);
}

#[test]
#[should_panic(expected = "Corrupt Maxwell JSON message '   '.")]
fn cdc_maxwell_whitespace_body_fails_with_its_dialect_name() {
    let body = bodies(vec![Some(b"   ".as_slice())]);
    MessageDecoder::new(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "").decode(&body);
}

// Under ignore-parse-errors the whitespace-only message drops whole (Flink's per-message catch),
// keeping its neighbors.
#[test]
fn cdc_whitespace_body_drops_in_skip_mode() {
    let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op":"c"}"#;
    let body = bodies(vec![Some(b"  ".as_slice()), Some(insert.as_slice())]);

    let out =
        MessageDecoder::new(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, true, "").decode(&body);

    assert_eq!(out.num_rows(), 1);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1]);
}

// An unrecognized op fails the decode rather than silently dropping the row — Flink throws on it by
// default, so failing keeps the result identical (the planner routes here only when the table does
// not set ignore-parse-errors, i.e. Flink is in throw mode too).
#[test]
#[should_panic(expected = "unknown CDC operation")]
fn cdc_unknown_op_fails() {
    let unknown = br#"{"before":null,"after":{"id":9,"name":"z","score":9.5},"op":"x"}"#;
    MessageDecoder::new(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, false, "")
        .decode(&bodies(vec![Some(unknown.as_slice())]));
}

// A null "before" on an update fails (Flink's REPLICA_IDENTITY error), not a silent drop.
#[test]
#[should_panic(expected = "null \"before\"")]
fn cdc_debezium_null_before_update_fails() {
    let update = br#"{"before":null,"after":{"id":2,"name":"b","score":2.5},"op":"u"}"#;
    MessageDecoder::new(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, false, "")
        .decode(&bodies(vec![Some(update.as_slice())]));
}

// Skip mode (`ignore-parse-errors`): every per-message failure — malformed JSON, an unknown op, a
// null pre-image on an update — drops that message, and the surrounding good messages still decode,
// matching Flink's catch-everything-per-message skip.
#[test]
fn cdc_debezium_skip_mode_drops_undecodable_messages() {
    let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op":"c"}"#;
    let malformed = br#"{"before":null,"after":{"id":2,"#;
    let unknown_op = br#"{"before":null,"after":{"id":3,"name":"x","score":3.5},"op":"x"}"#;
    let null_before = br#"{"before":null,"after":{"id":4,"name":"y","score":4.5},"op":"u"}"#;
    let delete = br#"{"before":{"id":5,"name":"c","score":5.5},"after":null,"op":"d"}"#;
    let body = bodies(vec![
        Some(insert.as_slice()),
        Some(malformed),
        Some(unknown_op),
        Some(null_before),
        Some(delete),
    ]);

    let out =
        MessageDecoder::new(FORMAT_DEBEZIUM_JSON, json_schema(), "", "", 0, true, "").decode(&body);

    assert_eq!(out.num_rows(), 2);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 5]);
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 3]); // INSERT + DELETE; the three bad messages vanish
}

// Skip mode on the plain JSON decode (`json` + ignore-parse-errors): a malformed body or an
// unconvertible value drops only that message.
#[test]
fn json_skip_mode_drops_undecodable_messages() {
    let good = br#"{"id":1,"name":"a","score":1.5}"#;
    let malformed = br#"{"id":2,"name":"#;
    let bad_type = br#"{"id":"abc","name":"c","score":3.5}"#;
    let also_good = br#"{"id":4,"name":"d","score":4.5}"#;
    let body = bodies(vec![
        Some(good.as_slice()),
        Some(malformed),
        Some(bad_type),
        Some(also_good),
    ]);

    let out = MessageDecoder::new(FORMAT_JSON, json_schema(), "", "", 0, true, "").decode(&body);

    // Flink's JSON ignore-parse-errors granularity (parity-pinned): a structurally bad document
    // drops the whole message, but a bad VALUE nulls just that field and keeps the row.
    assert_eq!(out.num_rows(), 3);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((id.value(0), id.value(2)), (1, 4));
    assert!(id.is_null(1));
    let name = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(name.value(1), "c");
}

// Skip mode with nothing to skip takes the batched fast path and decodes everything.
#[test]
fn json_skip_mode_clean_batch_decodes_in_full() {
    let a = br#"{"id":1,"name":"a","score":1.5}"#;
    let b = br#"{"id":2,"name":"b","score":2.5}"#;
    let body = bodies(vec![Some(a.as_slice()), Some(b)]);

    let out = MessageDecoder::new(FORMAT_JSON, json_schema(), "", "", 0, true, "").decode(&body);

    assert_eq!(out.num_rows(), 2);
}

// OGG JSON (format 7): same nested before/after layout as Debezium, but the op field is `op_type`
// with I/U/D codes.
#[test]
fn cdc_ogg_dialect_uses_op_type() {
    let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op_type":"I"}"#;
    let update =
        br#"{"before":{"id":2,"name":"b","score":2.5},"after":{"id":2,"name":"b2","score":3.5},"op_type":"U"}"#;
    let delete = br#"{"before":{"id":3,"name":"c","score":4.5},"after":null,"op_type":"D"}"#;
    let body = bodies(vec![Some(insert.as_slice()), Some(update), Some(delete)]);

    let out =
        MessageDecoder::new(FORMAT_OGG_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 4); // insert + (update→2) + delete
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 2, 3]);
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 1, 2, 3]);
}

// Maxwell JSON (format 8): `{data, old, type}` — `data` is the full post-image, `old` only the
// changed fields. An update's UPDATE_BEFORE is coalesce(old, data) per field (unchanged fields fall
// back to `data`); a delete reads the row from `data`, not `old`.
#[test]
fn cdc_maxwell_merges_partial_old_image() {
    let insert = br#"{"data":{"id":1,"name":"a","score":1.5},"type":"insert"}"#;
    // Only `name` changed (b → b2): `old` carries just `name`; id/score must come from `data`.
    let update = br#"{"data":{"id":2,"name":"b2","score":2.5},"old":{"name":"b"},"type":"update"}"#;
    let delete = br#"{"data":{"id":3,"name":"c","score":3.5},"type":"delete"}"#;
    let body = bodies(vec![Some(insert.as_slice()), Some(update), Some(delete)]);

    let out =
        MessageDecoder::new(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 4);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 2, 3]);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    // UPDATE_BEFORE keeps the old name "b"; the unchanged id/score are pulled from `data`.
    assert_eq!(
        (0..4).map(|i| names.value(i)).collect::<Vec<_>>(),
        vec!["a", "b", "b2", "c"]
    );
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(scores.values(), &[1.5, 2.5, 2.5, 3.5]);
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 1, 2, 3]);
}

// The DataOld pre-image follows KEY PRESENCE, not decoded nullability (Flink's findValue rule): a
// field present in `old` as an explicit null was changed FROM null and UPDATE_BEFORE keeps the
// null; an absent field is unchanged and copies from `data`.
#[test]
fn cdc_maxwell_keeps_explicit_null_in_old() {
    let update =
        br#"{"data":{"id":1,"name":"was-null","score":1.5},"old":{"name":null},"type":"update"}"#;
    let body = bodies(vec![Some(update.as_slice())]);

    let out =
        MessageDecoder::new(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 2);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert!(names.is_null(0)); // UPDATE_BEFORE: name was explicitly null before the update
    assert_eq!(names.value(1), "was-null");
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 1]); // absent from `old` → unchanged → copied from `data`
}

// Flink's findValue is a RECURSIVE depth-first search: a physical field name buried inside a
// nested container under `old` counts as present, and since the top-level decode of `old` saw
// nothing there, UPDATE_BEFORE keeps the null — it must NOT copy the post-image value.
#[test]
fn cdc_maxwell_old_presence_descends_nested_containers() {
    use arrow::array::Array;
    let nested_object =
        br#"{"data":{"id":1,"name":"a2","score":1.5},"old":{"junk":{"score":9}},"type":"update"}"#;
    let nested_array =
        br#"{"data":{"id":2,"name":"b2","score":2.5},"old":{"junk":[{"name":"hidden"}]},"type":"update"}"#;
    let body = bodies(vec![Some(nested_object.as_slice()), Some(nested_array)]);

    let out =
        MessageDecoder::new(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 4);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    // Message 0's UPDATE_BEFORE: id/name absent everywhere in old → copied; score found at
    // old.junk.score → present, keeps the top-level null (not 9, not data's 1.5).
    assert_eq!((id.value(0), names.value(0)), (1, "a2"));
    assert!(scores.is_null(0));
    // Message 1's UPDATE_BEFORE: name found inside an array element → present, kept null.
    assert_eq!((id.value(2), scores.value(2)), (2, 2.5));
    assert!(names.is_null(2));
}

// Jackson's tree build collapses a duplicate key to its LAST occurrence, both for the envelope's
// `old` field and inside it — names reachable only through a discarded earlier subtree are not
// found by findValue.
#[test]
fn cdc_maxwell_duplicate_keys_keep_the_last_occurrence() {
    use arrow::array::Array;
    let duplicate_old =
        br#"{"data":{"id":1,"name":"a2","score":1.5},"old":{"name":"x"},"old":{"score":9},"type":"update"}"#;
    let duplicate_inside =
        br#"{"data":{"id":2,"name":"b2","score":2.5},"old":{"junk":{"score":9},"junk":{"keep":1}},"type":"update"}"#;
    let body = bodies(vec![Some(duplicate_old.as_slice()), Some(duplicate_inside)]);

    let out =
        MessageDecoder::new(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 4);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    // Message 0: the second `old` wins — name is unchanged (copied from data), score kept from old.
    assert_eq!(names.value(0), "a2");
    assert_eq!(scores.value(0), 9.0);
    // Message 1: the second `junk` wins, discarding the subtree holding score → score copied.
    assert_eq!(scores.value(2), 2.5);
}

// Flink reads `old` unchecked on an update (row.getRow / old.getRow(i)), so a missing `old` or a
// Canal `old` array shorter than `data` is a corrupt message that fails the job — never a silent
// fall-back to the post-image.
#[test]
#[should_panic(expected = "null \"old\"/pre image")]
fn cdc_maxwell_update_without_old_fails() {
    let update = br#"{"data":{"id":1,"name":"x","score":1.5},"type":"update"}"#;
    let body = bodies(vec![Some(update.as_slice())]);
    MessageDecoder::new(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "").decode(&body);
}

#[test]
#[should_panic(expected = "\"old\" array is shorter")]
fn cdc_canal_uneven_update_arrays_fail() {
    let update = br#"{"data":[{"id":1,"name":"x","score":1.5},{"id":2,"name":"y","score":2.5}],"old":[{"name":"w"}],"type":"UPDATE"}"#;
    let body = bodies(vec![Some(update.as_slice())]);
    MessageDecoder::new(FORMAT_CANAL_JSON, json_schema(), "", "", 0, false, "").decode(&body);
}

// Canal's findValue presence scan covers the WHOLE `old` array: a key present in any element
// counts as present for every paired element, exactly as Flink's oldField.findValue over the
// array node behaves.
#[test]
fn cdc_canal_presence_is_per_message_across_elements() {
    let update = br#"{"data":[{"id":1,"name":"a2","score":1.5},{"id":2,"name":"b2","score":2.5}],"old":[{"name":"a"},{"id":2}],"type":"UPDATE"}"#;
    let body = bodies(vec![Some(update.as_slice())]);

    let out =
        MessageDecoder::new(FORMAT_CANAL_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    assert_eq!(out.num_rows(), 4); // two elements, UB+UA each
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    // Element 0's UPDATE_BEFORE: `name` present in old[0] → "a"; `id` present in old[1] (message-
    // wide presence!) but null in old[0] → keeps old[0]'s null, not data's 1 — Flink's exact quirk.
    assert!(id.is_null(0));
    assert_eq!(names.value(0), "a");
    // Element 1's UPDATE_BEFORE: `id` from old[1] = 2; `name` null in old[1] but present message-
    // wide → keeps the null.
    assert_eq!(id.value(2), 2);
    assert!(names.is_null(2));
}

// Canal JSON (format 9): `data`/`old` are arrays, so one message fans out per element. An INSERT
// with a two-row `data` emits two INSERTs; an UPDATE pairs `data[i]` with `old[i]` and merges the
// partial `old` like Maxwell (UPDATE_BEFORE coalesces old over data).
#[test]
fn cdc_canal_fans_out_arrays_and_merges_old() {
    // One INSERT message carrying two rows.
    let insert = br#"{"data":[{"id":1,"name":"a","score":1.5},{"id":2,"name":"b","score":2.5}],"type":"INSERT"}"#;
    // One UPDATE message, one element: only `score` changed (3.5 → 3.75); id/name come from data.
    let update =
        br#"{"data":[{"id":3,"name":"c","score":3.75}],"old":[{"score":3.5}],"type":"UPDATE"}"#;
    // A CREATE (DDL) message is skipped entirely.
    let ddl = br#"{"data":null,"type":"CREATE"}"#;
    let body = bodies(vec![Some(insert.as_slice()), Some(update), Some(ddl)]);

    let out =
        MessageDecoder::new(FORMAT_CANAL_JSON, json_schema(), "", "", 0, false, "").decode(&body);

    // 2 inserts + (update → UB + UA); CREATE dropped.
    assert_eq!(out.num_rows(), 4);
    let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(id.values(), &[1, 2, 3, 3]);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(
        (0..4).map(|i| names.value(i)).collect::<Vec<_>>(),
        vec!["a", "b", "c", "c"]
    );
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    // UPDATE_BEFORE keeps the old score 3.5; UPDATE_AFTER has the new 3.75.
    assert_eq!(scores.values(), &[1.5, 2.5, 3.5, 3.75]);
    let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!(kinds.values(), &[0, 0, 1, 2]);
}

// Each input row is one complete JSON document; the decoder emits one typed row per document,
// matching the target schema's columns and order.
#[test]
fn json_decode_emits_one_row_per_document() {
    let batch = bodies(vec![
        Some(br#"{"id": 1, "name": "a", "score": 1.5}"#),
        Some(br#"{"id": 2, "name": "b", "score": 2.5}"#),
    ]);
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(out.num_rows(), 2);
    assert_eq!(values(&out, 0), vec![1, 2]);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!((names.value(0), names.value(1)), ("a", "b"));
}

// Fields absent from a document and a null body both yield SQL NULLs, not failures.
#[test]
fn json_decode_tolerates_missing_fields_and_null_bodies() {
    let batch = bodies(vec![
        Some(br#"{"id": 1}"#),
        None,
        Some(br#"{"id": 3, "name": "c", "score": 9.0}"#),
    ]);
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
    // A null body contributes no row; the present documents decode in order.
    assert_eq!(out.num_rows(), 2);
    assert_eq!(values(&out, 0), vec![1, 3]);
    assert!(out.column(1).is_null(0));
}

// Flink's deserialize skips only a null or ZERO-LENGTH body before parsing; an all-whitespace
// document reaches Jackson and fails ("no content to map due to end-of-input") — the job dies in
// strict mode, the message drops under ignore-parse-errors. Both native subpaths (simd and the
// decimal-bearing arrow-json route) reproduce that split.
#[test]
fn json_decode_whitespace_only_body_fails_strict_and_drops_lenient() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let empty_and_blank = || {
        bodies(vec![
            Some(b"".as_slice()),
            Some(b" \t\r\n"),
            Some(br#"{"id": 1}"#),
        ])
    };
    let lenient = crate::json::JsonEnv {
        lenient: true,
        ..Default::default()
    };

    let strict = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default());
    assert!(catch_unwind(AssertUnwindSafe(|| strict.decode(&empty_and_blank()))).is_err());
    let out = JsonDecoder::new(json_schema(), lenient).decode(&empty_and_blank());
    assert_eq!(out.num_rows(), 1); // empty skipped silently, whitespace dropped, the document kept
    assert_eq!(values(&out, 0), vec![1]);
    // A zero-length body alone is not an error even in strict mode.
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default())
        .decode(&bodies(vec![Some(b"".as_slice())]));
    assert_eq!(out.num_rows(), 0);

    let decimal: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("dec", DataType::Decimal128(5, 2), true),
        Field::new("id", DataType::Int64, true),
    ]));
    let strict = JsonDecoder::new(decimal.clone(), crate::json::JsonEnv::default());
    assert!(catch_unwind(AssertUnwindSafe(|| strict.decode(&empty_and_blank()))).is_err());
    let out = JsonDecoder::new(decimal, lenient).decode(&empty_and_blank());
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 1), vec![1]);
}

// Flink's row converter counts every MATCHED key occurrence — duplicates included — and skips all
// remaining keys once the counter reaches the arity. So under (a, b): {"a":1,"b":2,"a":99} keeps
// a=1 (the duplicate lands after saturation), while {"a":1,"a":2} spends both slots on `a` (last
// wins) and leaves b null. The rule applies at every ROW nesting level.
#[test]
fn json_decode_duplicate_row_keys_saturate_flinks_field_counter() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let batch = bodies(vec![
        Some(br#"{"a": 1, "b": 2, "a": 99}"#),
        Some(br#"{"a": 1, "a": 2}"#),
        Some(br#"{"a": 1, "a": 2, "b": 5}"#),
        Some(br#"{"x": 0, "a": 1, "x": 0, "a": 2, "b": 7}"#), // unknown keys never count
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    let a = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let b = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((a.value(0), b.value(0)), (1, 2));
    assert_eq!(a.value(1), 2);
    assert!(b.is_null(1));
    assert_eq!(a.value(2), 2);
    assert!(b.is_null(2));
    assert_eq!(a.value(3), 2);
    assert!(b.is_null(3));

    let nested: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "r",
        DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ])),
        true,
    )]));
    let batch = bodies(vec![Some(br#"{"r": {"a": 1, "b": 2, "a": 99}}"#)]);
    let out = JsonDecoder::new(nested, crate::json::JsonEnv::default()).decode(&batch);
    let r = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let a = r.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let b = r.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((a.value(0), b.value(0)), (1, 2));
}

// simd-json rejects documents Jackson tokenizes fine: out-of-range number literals, raw control
// characters inside strings (Flink enables ALLOW_UNESCAPED_CONTROL_CHARS), and content trailing
// the root document (which Flink's pull parser never reads). Those messages retry through the
// Jackson-faithful token walk instead of failing the job / dropping the message.
#[test]
fn json_decode_retries_documents_only_jackson_tokenizes() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let batch = bodies(vec![
        // score is DOUBLE: a beyond-u64 int literal converts via parseDouble of the raw text,
        // and 1e999 overflows to Infinity — per-field successes in Flink, not job failures.
        Some(br#"{"score": 18446744073709551616}"#),
        Some(br#"{"score": 1e999}"#),
        Some(br#"{"id": 1}{"id": 2}"#), // Flink reads ONE document and ignores the rest
        Some(br#"{"id": 3} trailing garbage is never tokenized"#),
        Some(b"{\"name\": \"a\tb\"}"), // a raw TAB inside the string
        Some(br#"[{"score": 1e999}, {"id": 7}]"#), // the retry fans a top-level array out too
    ]);
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(out.num_rows(), 7);
    let scores = out
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(scores.value(0), 1.8446744073709552e19);
    assert_eq!(scores.value(1), f64::INFINITY);
    let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((ids.value(2), ids.value(3), ids.value(6)), (1, 3, 7));
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(4), "a\tb");
    assert_eq!(scores.value(5), f64::INFINITY);

    // id is BIGINT: a beyond-long literal fails Long.parseLong PER FIELD — the job dies in strict
    // mode with the row-converter error, and skip mode nulls just the field, keeping the row.
    let bad_long = bodies(vec![Some(
        br#"{"id": 18446744073709551616, "name": "keep"}"#,
    )]);
    let strict = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default());
    assert!(catch_unwind(AssertUnwindSafe(|| strict.decode(&bad_long))).is_err());
    let out = JsonDecoder::new(
        json_schema(),
        crate::json::JsonEnv {
            lenient: true,
            ..Default::default()
        },
    )
    .decode(&bad_long);
    assert_eq!(out.num_rows(), 1);
    assert!(out.column(0).is_null(0));
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "keep");
}

// A container token under a scalar (non-STRING) column makes Flink's pull-parser cursor drift:
// the converter coerces getText ("{") without consuming the container, and the row walk's next
// step lands INSIDE it. Under (b BOOLEAN, i INT), {"b":{},"i":7} converts b=false
// (parseBoolean("{")), then mistakes the inner END_OBJECT for the row's end — Flink SUCCEEDS with
// (false, null), so a native panic here would kill a job Flink runs. The message re-decodes
// through the token walk, which replays the drift.
#[test]
fn json_decode_replicates_flinks_cursor_drift() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let lenient = crate::json::JsonEnv {
        lenient: true,
        ..Default::default()
    };
    let bool_int: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("b", DataType::Boolean, true),
        Field::new("i", DataType::Int64, true),
    ]));
    for body in [
        br#"{"b": {}, "i": 7}"#.as_slice(),
        br#"{"b": {"x": 1}, "i": 7}"#,
    ] {
        let batch = bodies(vec![Some(body)]);
        for env in [crate::json::JsonEnv::default(), lenient] {
            let out = JsonDecoder::new(bool_int.clone(), env).decode(&batch);
            assert_eq!(out.num_rows(), 1);
            let b = out
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            assert!(!b.value(0)); // parseBoolean("{")
            assert!(out.column(1).is_null(0)); // `i` was never matched — the drift ate it
        }
    }

    // The reverse order: parseInt("{") fails the field, so strict mode dies (like Flink); in skip
    // mode the drift again ends the row early — (null, null), not (null, true).
    let int_bool: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("b", DataType::Boolean, true),
    ]));
    let batch = bodies(vec![Some(br#"{"i": {}, "b": true}"#.as_slice())]);
    let strict = JsonDecoder::new(int_bool.clone(), crate::json::JsonEnv::default());
    assert!(catch_unwind(AssertUnwindSafe(|| strict.decode(&batch))).is_err());
    let out = JsonDecoder::new(int_bool, lenient).decode(&batch);
    assert_eq!(out.num_rows(), 1);
    assert!(out.column(0).is_null(0));
    assert!(out.column(1).is_null(0));

    // Inside a MAP value the drifted walk exits the map at the inner END_OBJECT and the row walk
    // then skips the tail keys — one entry {k: null}, never k2.
    let map: SchemaRef = Arc::new(Schema::new(vec![Field::new_map(
        "m",
        "entries",
        Field::new("keys", DataType::Utf8, false),
        Field::new("values", DataType::Int64, true),
        false,
        true,
    )]));
    let batch = bodies(vec![Some(br#"{"m": {"k": {}, "k2": 5}}"#.as_slice())]);
    let out = JsonDecoder::new(map, lenient).decode(&batch);
    use arrow::array::MapArray;
    let maps = out.column(0).as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(maps.value_length(0), 1);
    let keys = maps
        .entries()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(keys.value(0), "k");
    assert!(maps.entries().column(1).is_null(0));

    // A nested-array ELEMENT drifts Flink's fan-out loop itself: the walk steps into [2], takes
    // its ']' for the fan-out's END_ARRAY, and stops — one row, the trailing element never read.
    let batch = bodies(vec![Some(br#"[{"id": 1}, [2], {"id": 3}]"#.as_slice())]);
    let out = JsonDecoder::new(json_schema(), lenient).decode(&batch);
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 0), vec![1]);
}

// A FLOAT column parses its literal at float width, like Jackson's getFloatValue (2.14+): the
// tape's f64-then-narrow double rounding is off by one ULP exactly when the f64 lands on an f32
// rounding midpoint — those messages re-decode through the token walk with the raw literal.
#[test]
fn json_decode_float_columns_parse_the_literal_at_float_width() {
    use arrow::array::Float32Array;
    let direct: f32 = "7.038531e-26".parse().unwrap();
    let double_rounded = "7.038531e-26".parse::<f64>().unwrap() as f32;
    assert_ne!(
        direct, double_rounded,
        "the probe literal must be a midpoint case"
    );

    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("fl", DataType::Float32, true),
        Field::new("id", DataType::Int64, true),
    ]));
    let batch = bodies(vec![
        Some(br#"{"fl": 7.038531e-26, "id": 1}"#),
        Some(br#"{"fl": 1.5, "id": 2}"#), // midpoint-free literals stay on the fast path
        Some(br#"[{"fl": 7.038531e-26}, {"fl": -7.038531e-26}]"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    let floats = out
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(floats.value(0), direct);
    assert_eq!(floats.value(1), 1.5);
    assert_eq!((floats.value(2), floats.value(3)), (direct, -direct));
    assert_eq!(values(&out, 1)[..2], [1, 2]);
}

// An empty input batch flushes to an empty batch of the target schema, not a panic.
#[test]
fn json_decode_empty_batch_yields_empty() {
    let out =
        JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&bodies(vec![]));
    assert_eq!(out.num_rows(), 0);
    assert_eq!(out.schema(), json_schema());
}

// Every scalar type the boundary admits decodes: numbers for the numeric widths (a float for an
// integer column truncates), true/false for BOOLEAN, and strings for DATE and for TIMESTAMP in
// both the SQL and ISO-8601 forms (a bare number is a raw nanosecond epoch).
#[test]
fn json_decode_covers_boundary_scalar_types() {
    use arrow::array::{
        BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array, Int8Array,
        TimestampNanosecondArray,
    };
    use arrow::datatypes::TimeUnit;
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("i8", DataType::Int8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("day", DataType::Date32, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
    ]));
    let batch = bodies(vec![
        Some(
            br#"{"i8": -3, "i16": 300, "i32": 70000, "i64": 5000000000, "f32": 1.5,
                    "f64": 2.5, "flag": true, "day": "2026-07-01", "ts": "2026-07-01 12:00:00.123"}"#,
        ),
        Some(
            br#"{"i8": 1, "i16": -2, "i32": 3.9, "i64": "42", "f32": 2, "f64": "Infinity",
                    "flag": "TRUE", "day": "1970-01-02", "ts": "2026-07-01 12:00:00.123Z"}"#,
        ),
        Some(br#"{"flag": 1}"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(out.num_rows(), 3);
    let i8s = out.column(0).as_any().downcast_ref::<Int8Array>().unwrap();
    assert_eq!((i8s.value(0), i8s.value(1)), (-3, 1));
    let i16s = out.column(1).as_any().downcast_ref::<Int16Array>().unwrap();
    assert_eq!((i16s.value(0), i16s.value(1)), (300, -2));
    let i32s = out.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
    // A float token truncates toward zero under INT/BIGINT (convertToInt); TINYINT/SMALLINT
    // reject float tokens outright (convertToByte falls through to parseByte) — parity-pinned.
    assert_eq!((i32s.value(0), i32s.value(1)), (70000, 3));
    assert_eq!(values(&out, 3), vec![5000000000, 42, 0]);
    assert!(out.column(3).is_null(2));
    let f32s = out
        .column(4)
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!((f32s.value(0), f32s.value(1)), (1.5, 2.0));
    let f64s = out
        .column(5)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(f64s.value(0), 2.5);
    assert_eq!(f64s.value(1), f64::INFINITY); // Java's Double.parseDouble spelling
    let flags = out
        .column(6)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(flags.value(0) && flags.value(1)); // parseBoolean is case-insensitive
    assert!(!flags.value(2)); // ... and a number is simply false, never an error
    let days = out
        .column(7)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(days.value(1), 1);
    let ts = out
        .column(8)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let expected = 1_782_907_200_123_000_000i64; // 2026-07-01T12:00:00.123Z
                                                 // The trailing 'Z' is the tolerated LTZ shape (divergences/21) — same instant.
    assert_eq!((ts.value(0), ts.value(1)), (expected, expected));
    assert!(ts.is_null(2));
}

// TIME parses SQL_TIME_FORMAT and stores whole seconds at the column's Arrow unit (Flink's
// toSecondOfDay() * 1000 discards the fraction whatever the declared precision); VARBINARY is
// Jackson's base64 read, declared length not enforced. Both hold on the simd path and — riding as
// text — on the decimal-bearing arrow-json path.
#[test]
fn json_decode_time_truncates_and_binary_follows_jackson() {
    use arrow::array::{Time32MillisecondArray, Time32SecondArray, Time64NanosecondArray};
    use arrow::datatypes::TimeUnit;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("t0", DataType::Time32(TimeUnit::Second), true),
        Field::new("t3", DataType::Time32(TimeUnit::Millisecond), true),
        Field::new("t9", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("b", DataType::Binary, true),
    ]));
    let batch = bodies(vec![Some(
        br#"{"t0": "12:34:56.789", "t3": "12:34:56.789", "t9": "12:34:56.123456789",
                "b": "AQIDBAUGBwg="}"#,
    )]);
    let out = JsonDecoder::new(schema.clone(), crate::json::JsonEnv::default()).decode(&batch);
    let secs = 12 * 3600 + 34 * 60 + 56;
    let t0 = out
        .column(0)
        .as_any()
        .downcast_ref::<Time32SecondArray>()
        .unwrap();
    assert_eq!(t0.value(0), secs as i32);
    let t3 = out
        .column(1)
        .as_any()
        .downcast_ref::<Time32MillisecondArray>()
        .unwrap();
    assert_eq!(t3.value(0), (secs * 1000) as i32); // the .789 is gone
    let t9 = out
        .column(2)
        .as_any()
        .downcast_ref::<Time64NanosecondArray>()
        .unwrap();
    assert_eq!(t9.value(0), secs as i64 * 1_000_000_000);
    let b = out
        .column(3)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(b.value(0), [1, 2, 3, 4, 5, 6, 7, 8]);
    // Missing base64 padding fails like Jackson's MIME read; under skip mode the field nulls.
    let bad = bodies(vec![Some(br#"{"b": "AQ"}"#)]);
    let strict = schema.clone();
    assert!(catch_unwind(AssertUnwindSafe(|| JsonDecoder::new(
        strict,
        crate::json::JsonEnv::default()
    )
    .decode(&bad)))
    .is_err());
    let lenient = JsonDecoder::new(
        schema,
        crate::json::JsonEnv {
            lenient: true,
            ..Default::default()
        },
    )
    .decode(&bad);
    assert!(lenient.column(3).is_null(0));

    // The decimal-bearing schema keeps the same envelope through the text-restored path.
    let mixed: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("dec", DataType::Decimal128(5, 2), true),
        Field::new("t3", DataType::Time32(TimeUnit::Millisecond), true),
        Field::new("b", DataType::Binary, true),
    ]));
    let batch = bodies(vec![Some(
        br#"{"dec": 1.235, "t3": "12:34:56.789", "b": "AQID"}"#,
    )]);
    let out = JsonDecoder::new(mixed, crate::json::JsonEnv::default()).decode(&batch);
    let dec = out
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(dec.value(0), 124); // HALF_UP
    let t3 = out
        .column(1)
        .as_any()
        .downcast_ref::<Time32MillisecondArray>()
        .unwrap();
    assert_eq!(t3.value(0), (secs * 1000) as i32);
    let b = out
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(b.value(0), [1, 2, 3]);
}

// A DECIMAL-bearing schema rides arrow-json, whose map decoder keeps every entry of a
// duplicate-keyed object; Flink builds a java.util.Map — one entry per key, last value. The
// decode collapses to last-value-first-position, like the simd path.
#[test]
fn json_decode_decimal_path_collapses_duplicate_map_keys() {
    use arrow::array::MapArray;
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new_map(
            "m",
            "entries",
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Decimal128(5, 2), true),
            false,
            true,
        ),
        Field::new("dec", DataType::Decimal128(5, 2), true),
    ]));
    let batch = bodies(vec![
        Some(br#"{"m": {"k": 1.234, "j": 2, "k": 3.456}, "dec": 1.5}"#),
        Some(br#"{"m": {"u": 7}}"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    let maps = out.column(0).as_any().downcast_ref::<MapArray>().unwrap();
    let keys = maps
        .entries()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = maps
        .entries()
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    // Row 0: k keeps its first position with the LAST value (HALF_UP of 3.456), j follows.
    assert_eq!(maps.value_length(0), 2);
    assert_eq!((keys.value(0), values.value(0)), ("k", 346));
    assert_eq!((keys.value(1), values.value(1)), ("j", 200));
    // Row 1 (no duplicates) is untouched.
    assert_eq!(maps.value_length(1), 1);
    assert_eq!((keys.value(2), values.value(2)), ("u", 700));
}

/// The SQL/ISO-8601 timestamp modes reject each other's separator, numbers fail a timestamp/date
/// column, and a float literal under a STRING column fails loudly (raw literal unrecoverable) —
/// the Flink envelope, per the JSON decode parity test.
#[test]
fn json_decode_rejects_off_mode_and_numeric_temporals() {
    use arrow::datatypes::TimeUnit;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let ts_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]));
    let sql = crate::json::JsonEnv::default();
    let iso = crate::json::JsonEnv {
        mode: flink_text::TimestampMode::Iso8601,
        ..Default::default()
    };
    let decode = |schema: &SchemaRef, env, body: &'static [u8]| {
        let schema = schema.clone();
        let batch = bodies(vec![Some(body)]);
        catch_unwind(AssertUnwindSafe(move || {
            JsonDecoder::new(schema, env).decode(&batch)
        }))
    };
    // SQL mode: space separator only; ISO mode: 'T' only (seconds optional there).
    assert!(decode(&ts_schema, sql, br#"{"ts": "2026-07-01T12:00:00"}"#).is_err());
    assert!(decode(&ts_schema, iso, br#"{"ts": "2026-07-01 12:00:00"}"#).is_err());
    assert!(decode(&ts_schema, iso, br#"{"ts": "2026-07-01T12:00"}"#).is_ok());
    // A bare number is not a Flink timestamp or date.
    assert!(decode(&ts_schema, sql, br#"{"ts": 123456789}"#).is_err());
    let day_schema: SchemaRef =
        Arc::new(Schema::new(vec![Field::new("day", DataType::Date32, true)]));
    assert!(decode(&day_schema, sql, br#"{"day": 42}"#).is_err());
    assert!(decode(&day_schema, sql, br#"{"day": "2026-7-1"}"#).is_err());
    // STRING coercions: ints/bools/containers echo exactly, a float literal fails loudly.
    let str_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let echoed = decode(
        &str_schema,
        sql,
        b"{\"s\": {\"a\": 1, \"b\": [true, null, \"x\\n\"], \"a\": 2}}",
    )
    .unwrap();
    let strings = echoed
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    // Duplicate keys collapse last-value-first-position, like Jackson's tree; the escaped
    // newline round-trips through Jackson-style escaping.
    assert_eq!(strings.value(0), "{\"a\":2,\"b\":[true,null,\"x\\n\"]}");
    let echoed = decode(&str_schema, sql, br#"{"s": 42}"#).unwrap();
    let strings = echoed
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(strings.value(0), "42");
    assert!(decode(&str_schema, sql, br#"{"s": 1.5}"#).is_err());
}

// Nested ROW/ARRAY/MAP decode recursively: a null or missing struct nulls its children, list
// elements keep order and admit nulls, and map keys parse as the key column's type.
#[test]
fn json_decode_covers_nested_types() {
    use arrow::array::{ListArray, MapArray, StructArray};
    let nested = Fields::from(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("row", DataType::Struct(nested.clone()), true),
        Field::new(
            "nums",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
        Field::new(
            "tags",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Int64, false),
                        Field::new("value", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]));
    let batch = bodies(vec![
        Some(br#"{"row": {"a": 1, "b": "x"}, "nums": [1, null, 3], "tags": {"7": "seven"}}"#),
        Some(br#"{"row": {"a": 2}, "nums": [], "tags": {}}"#),
        Some(br#"{"row": null}"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(out.num_rows(), 3);

    let row = out
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let a = row.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((a.value(0), a.value(1)), (1, 2));
    assert!(row.column(1).is_null(1)); // missing nested field -> null
    assert!(row.is_null(2)); // null struct -> null row

    let nums = out.column(1).as_any().downcast_ref::<ListArray>().unwrap();
    let first = nums.value(0);
    let first = first.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((first.value(0), first.value(2)), (1, 3));
    assert!(first.is_null(1));
    assert_eq!(nums.value_length(1), 0);
    assert!(nums.is_null(2)); // missing list -> null

    let tags = out.column(2).as_any().downcast_ref::<MapArray>().unwrap();
    let keys = tags.keys().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(keys.value(0), 7); // object key parsed as the BIGINT key type
    let map_values = tags
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(map_values.value(0), "seven");
    assert_eq!(tags.value_length(1), 0);
    assert!(tags.is_null(2));
}

// The decimal-bearing (raw-literals) path parses DECIMAL columns with Flink's exact semantics:
// the raw digit string (no f64 rounding), HALF_UP past the declared scale, and NULL — not an
// error — on precision overflow. arrow-json's own decimal parse truncates and errors, which
// silently diverged from Flink; the column decodes as raw text and converts here instead.
#[test]
fn json_decimal_rounds_half_up_and_nulls_on_overflow() {
    use arrow::array::Decimal128Array;
    let nested = Fields::from(vec![Field::new("p", DataType::Decimal128(5, 2), true)]);
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Decimal128(5, 2), true),
        Field::new("wide", DataType::Decimal128(38, 20), true),
        Field::new("row", DataType::Struct(nested.clone()), true),
    ]));
    let batch = bodies(vec![
        Some(br#"{"d": 1.235, "wide": 0.12345678901234567890123456789, "row": {"p": "-1.235"}}"#),
        Some(br#"{"d": 12345.6, "wide": null, "row": null}"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    let d = out
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(d.value(0), 124); // HALF_UP, not truncation
    assert!(d.is_null(1)); // precision overflow → NULL (DecimalData.fromBigDecimal), not an error
    let wide = out
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(wide.value(0), 12345678901234567890i128); // exact raw literal, HALF_UP at scale 20
    let row = out
        .column(2)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let p = row
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(p.value(0), -124); // nested decimals get the same conversion; strings trim+parse
    assert!(row.is_null(1));
}

// Unknown keys are skipped and a duplicated field keeps its last value — Jackson (hence Flink)
// and arrow-json agree on both.
#[test]
fn json_decode_skips_unknown_keys_and_keeps_last_duplicate() {
    let batch = bodies(vec![Some(
        br#"{"extra": [1, {"x": 2}], "id": 1, "name": "a", "id": 5}"#,
    )]);
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(values(&out, 0), vec![5]);
}

// DECIMAL columns route to the raw-literal (arrow-json) path: a number with more significant
// digits than an f64 carries still decodes exactly, in number and string position alike.
#[test]
fn json_decode_decimal_stays_exact_beyond_f64_precision() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "d",
        DataType::Decimal128(30, 10),
        true,
    )]));
    let batch = bodies(vec![
        Some(br#"{"d": 12345678901234567.8901234567}"#),
        Some(br#"{"d": "12345678901234567.8901234567"}"#),
    ]);
    let out = JsonDecoder::new(schema, crate::json::JsonEnv::default()).decode(&batch);
    let d = out
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let exact = 123456789012345678901234567i128;
    assert_eq!((d.value(0), d.value(1)), (exact, exact));
}

#[test]
#[should_panic(expected = "as Int64")]
fn json_decode_rejects_type_mismatch() {
    let batch = bodies(vec![Some(br#"{"id": true}"#)]);
    JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
}

#[test]
#[should_panic(expected = "single object")]
fn json_decode_rejects_non_object_document() {
    let batch = bodies(vec![Some(br#"42"#)]);
    JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
}

// Flink's `json` format fans a top-level array out into one row per element (`processArray`);
// an empty array contributes no row, and surrounding whitespace is insignificant.
#[test]
fn json_decode_fans_out_top_level_arrays() {
    let batch = bodies(vec![
        Some(br#"  [ {"id": 1, "name": "a"} , {"id": 2} ]  "#.as_slice()),
        Some(br#"[]"#),
        Some(br#"{"id": 3}"#),
        Some(br#"[{"id": 4, "score": 4.5}]"#),
    ]);
    let out = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default()).decode(&batch);
    assert_eq!(out.num_rows(), 4);
    assert_eq!(values(&out, 0), vec![1, 2, 3, 4]);
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "a");
    assert!(names.is_null(1));
}

// A non-object array element fails the whole message in strict mode (any element failure fails
// Flink's deserialize) and drops alone under ignore-parse-errors, keeping its good siblings. A bad
// *value* inside an element stays the usual per-field null.
#[test]
fn json_decode_array_element_granularity_follows_flink() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let strict = JsonDecoder::new(json_schema(), crate::json::JsonEnv::default());
    for body in [
        br#"[{"id": 1}, 5, {"id": 3}]"#.as_slice(),
        br#"[{"id": 1}, null]"#,
        br#"[[{"id": 1}]]"#,
    ] {
        let batch = bodies(vec![Some(body)]);
        assert!(
            catch_unwind(AssertUnwindSafe(|| strict.decode(&batch))).is_err(),
            "strict decode must fail: {}",
            String::from_utf8_lossy(body)
        );
    }
    let lenient = JsonDecoder::new(
        json_schema(),
        crate::json::JsonEnv {
            lenient: true,
            ..Default::default()
        },
    );
    let out = lenient.decode(&bodies(vec![Some(br#"[{"id": 1}, 5, null, {"id": 3}]"#)]));
    assert_eq!(values(&out, 0), vec![1, 3]);
    // A nested-array element drifts Flink's element loop itself (replayed via the token walk):
    // only the prefix before it survives, and the tail is never read.
    let out = lenient.decode(&bodies(vec![Some(
        br#"[{"id": 1}, 5, null, [7], {"id": 3}]"#,
    )]));
    assert_eq!(values(&out, 0), vec![1]);
    // A malformed array-rooted document still drops whole, never element by element.
    let out = lenient.decode(&bodies(vec![Some(br#"[{"id": 1}, {"id": }]"#)]));
    assert_eq!(out.num_rows(), 0);
    // A bad value inside an element nulls the field and keeps the element's row.
    let out = lenient.decode(&bodies(vec![Some(
        br#"[{"id": 1}, {"id": "junk"}, {"id": 3}]"#,
    )]));
    assert_eq!(out.num_rows(), 3);
    let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!((ids.value(0), ids.value(2)), (1, 3));
    assert!(ids.is_null(1));
}

// The decimal-bearing (arrow-json) subpath fans arrays out with the same granularity as the simd
// path, keeping each element's exact raw literal for the decimal parse.
#[test]
fn json_decimal_path_fans_out_top_level_arrays() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "d",
        DataType::Decimal128(30, 10),
        true,
    )]));
    let strict = JsonDecoder::new(schema.clone(), crate::json::JsonEnv::default());
    let out = strict.decode(&bodies(vec![
        Some(br#" [ {"d": 12345678901234567.8901234567}, {"d": 1.5} ] "#.as_slice()),
        Some(br#"[]"#),
        Some(br#"{"d": 2.5}"#),
    ]));
    assert_eq!(out.num_rows(), 3);
    let d = out
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    // The raw literal survives f64-impossible precision through the element split.
    assert_eq!(d.value(0), 123456789012345678901234567i128);
    assert_eq!(d.value(1), 15_000_000_000i128);
    for body in [
        br#"[{"d": 1.5}, 7]"#.as_slice(),
        br#"[{"d": 1.5}, null]"#,
        br#"[[{"d": 1}]]"#,
    ] {
        let batch = bodies(vec![Some(body)]);
        assert!(
            catch_unwind(AssertUnwindSafe(|| strict.decode(&batch))).is_err(),
            "strict decode must fail: {}",
            String::from_utf8_lossy(body)
        );
    }
    let lenient = JsonDecoder::new(
        schema,
        crate::json::JsonEnv {
            lenient: true,
            ..Default::default()
        },
    );
    let out = lenient.decode(&bodies(vec![Some(
        br#"[{"d": 1.5}, 7, null, [7], {"d": "junk"}, {"d": 2.5}]"#,
    )]));
    // Non-object elements drop alone; the bad decimal *value* nulls per field and keeps its row.
    assert_eq!(out.num_rows(), 3);
    let d = out
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(
        (d.value(0), d.value(2)),
        (15_000_000_000i128, 25_000_000_000i128)
    );
    assert!(d.is_null(1));
    let out = lenient.decode(&bodies(vec![Some(br#"[{"d": 1.5}, {"d": }]"#)]));
    assert_eq!(out.num_rows(), 0);
}

// A CDC envelope never fans out a top-level array. Debezium/OGG decode through Flink's
// deprecated one-row entry, which unwraps an array holding exactly one envelope — and in skip
// mode junk elements are dropped from the fan-out first, so a lone envelope among junk still
// decodes there. Every other shape is corrupt, and Maxwell/Canal (tree-converter entry) reject
// any array root — on the simd path and, with a decimal-bearing physical schema, on the
// arrow-json path.
#[test]
fn cdc_array_roots_unwrap_or_reject_like_flink() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let env1 = r#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op":"c"}"#;
    let env2 = r#"{"before":null,"after":{"id":2,"name":"b","score":2.5},"op":"c"}"#;
    let decimal_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("score", DataType::Decimal128(5, 2), true),
    ]));
    let denv1 = r#"{"before":null,"after":{"id":1,"score":1.5},"op":"c"}"#;
    let denv2 = r#"{"before":null,"after":{"id":2,"score":2.5},"op":"c"}"#;
    for (schema, env1, env2) in [(json_schema(), env1, env2), (decimal_schema, denv1, denv2)] {
        let strict =
            MessageDecoder::new(FORMAT_DEBEZIUM_JSON, schema.clone(), "", "", 0, false, "");
        let skipping = MessageDecoder::new(FORMAT_DEBEZIUM_JSON, schema, "", "", 0, true, "");
        let rows = |decoder: &MessageDecoder, body: &str| {
            decoder
                .decode(&bodies(vec![Some(body.as_bytes())]))
                .num_rows()
        };
        let panics = |decoder: &MessageDecoder, body: &str| {
            let batch = bodies(vec![Some(body.as_bytes())]);
            catch_unwind(AssertUnwindSafe(|| decoder.decode(&batch))).is_err()
        };
        let single = format!("[{env1}]");
        assert_eq!(rows(&strict, &single), 1);
        assert_eq!(rows(&skipping, &single), 1);
        let pair = format!("[{env1},{env2}]");
        assert!(panics(&strict, &pair));
        assert_eq!(rows(&skipping, &pair), 0);
        let with_junk = format!("[{env1},1]");
        assert!(panics(&strict, &with_junk));
        assert_eq!(rows(&skipping, &with_junk), 1);
        assert!(panics(&strict, "[]"));
        assert_eq!(rows(&skipping, "[]"), 0);
    }
    let maxwell = r#"[{"data":{"id":1,"name":"a","score":1.5},"type":"insert"}]"#;
    let batch = bodies(vec![Some(maxwell.as_bytes())]);
    let strict = MessageDecoder::new(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, false, "");
    assert!(catch_unwind(AssertUnwindSafe(|| strict.decode(&batch))).is_err());
    let skipping = MessageDecoder::new(FORMAT_MAXWELL_JSON, json_schema(), "", "", 0, true, "");
    assert_eq!(skipping.decode(&batch).num_rows(), 0);
}

// OVER (ORDER BY rt RANGE UNBOUNDED PRECEDING) running SUM: ties in rt share the post-fold value,
// and the running total persists across update calls.
#[test]
fn over_running_sum_shares_range_ties() {
    let rt: ArrayRef = Arc::new(Int64Array::from(vec![0i64, 1000, 1000, 2000]));
    let value: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30, 40]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("rt", DataType::Int64, false),
        Field::new("value0", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![rt, value]).unwrap();
    let mut over = OverAggregator::new(vec![0], vec![0]); // bigint value, SUM
                                                          // rt 1000 ties (20,30) both see 10+20+30=60; emitted in input order.
    assert_eq!(values(&over.update(&batch), 0), vec![10, 60, 60, 100]);

    // A later complete batch continues the running total (UNBOUNDED PRECEDING).
    let rt2: ArrayRef = Arc::new(Int64Array::from(vec![3000i64]));
    let value2: ArrayRef = Arc::new(Int64Array::from(vec![5i64]));
    let batch2 = RecordBatch::try_new(schema, vec![rt2, value2]).unwrap();
    assert_eq!(values(&over.update(&batch2), 0), vec![105]);
}

// PARTITION BY: each key has its own running SUM; rt ties within a key share the value.
#[test]
fn over_running_sum_per_partition_key() {
    let rt: ArrayRef = Arc::new(Int64Array::from(vec![0i64, 0, 1000, 1000, 2000]));
    let value: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 100, 20, 30, 40]));
    let key0: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 1, 1, 2]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("rt", DataType::Int64, false),
            Field::new("value0", DataType::Int64, true),
            Field::new("key0", DataType::Int64, false),
        ])),
        vec![rt, value, key0],
    )
    .unwrap();
    let mut over = OverAggregator::new(vec![0], vec![0]);
    // key 1: 10, then (20,30) tie -> 60, 60; key 2: 100, then 140.
    assert_eq!(values(&over.update(&batch), 0), vec![10, 100, 60, 60, 140]);
}

// The columnar (buffering) OVER passes input columns through and appends the running aggregate,
// emitting only the rows the watermark has completed.
#[test]
fn over_window_buffers_and_passes_through() {
    let k: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 2, 1]));
    let v: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 100, 40]));
    // rowtime in nanoseconds (millis 0, 1000, 500, 9000).
    let rt: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        0i64,
        1_000_000_000,
        500_000_000,
        9_000_000_000,
    ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new(
            "rt",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(schema, vec![k, v, rt]).unwrap();
    let mut over = OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 0, 0, false);
    over.push(batch, 0).unwrap();
    // Watermark 2000ms completes the first three rows (rt 0/1000/500); the rt=9000 row stays.
    let out = over.flush(2000, 0).unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(values(&out, 0), vec![1, 1, 2]); // k passed through
    assert_eq!(values(&out, 1), vec![10, 20, 100]); // v passed through
                                                    // running SUM per key: key 1 -> 10, 30; key 2 -> 100 (result is the last column).
    assert_eq!(values(&out, 3), vec![10, 30, 100]);
    // The pending row flushes once the watermark passes it.
    let rest = over.flush(10_000, 0).unwrap();
    assert_eq!(rest.num_rows(), 1);
    assert_eq!(values(&rest, 1), vec![40]); // v
    assert_eq!(values(&rest, 3), vec![70]); // key 1 running sum 10+20+40
}

#[test]
fn over_window_counts_rows_dropped_behind_the_watermark() {
    let mut over = OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 0, 0, false);
    over.flush(1000, 0).unwrap();
    over.push(join_batch(vec![1, 2], vec![10, 20], vec![999, 1000]), 0)
        .unwrap();
    // Flink drops timestamps strictly behind the current watermark. A row exactly on the
    // watermark remains on time and must still be emitted by the next firing.
    assert_eq!(over.late_drops, 1);
    let out = over.flush(1000, 0).unwrap();
    assert_eq!(values(&out, 1), vec![20]);
}

#[test]
fn over_state_partitions_and_restores_by_flink_key_group() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new(
                "rt",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                false,
            ),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(Int64Array::from(vec![10i64, 20])),
            Arc::new(TimestampNanosecondArray::from(vec![0i64, 0])),
        ],
    )
    .unwrap();
    let mut before = OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 0, 0, false);
    before.push(batch, 0).unwrap();
    let partitions = before.snapshot_partitions(128, &[-1]);
    assert!(
        partitions.len() >= 2,
        "test keys should cover distinct raw key groups"
    );
    let snapshots: Vec<Vec<u8>> = partitions.into_values().collect();
    let mut restored = OverWindowAggregator::restore_partitions(
        vec![0],
        vec![0],
        2,
        vec![1],
        vec![0],
        0,
        0,
        false,
        &snapshots,
        0,
        0,
    );
    let out = restored.flush(0, 0).unwrap();
    let mut rows: Vec<(i64, i64)> = values(&out, 0).into_iter().zip(values(&out, 3)).collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 10), (2, 20)]);
}

// Bounded ROWS frame (1 PRECEDING): each row's SUM covers only itself and the row before it
// within its partition, recomputed over the frame slice — and the trailing edge drops older rows.
#[test]
fn bounded_rows_over_sums_the_frame_slice() {
    let k: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 1, 2]));
    let v: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30, 100]));
    let rt: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        0i64,
        1_000_000_000,
        2_000_000_000,
        500_000_000,
    ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new(
            "rt",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(schema, vec![k, v, rt]).unwrap();
    // frame_kind 1 = bounded ROWS, offset 1 = one preceding row.
    let mut over = OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 1, 1, false);
    over.push(batch, 0).unwrap();
    let out = over.flush(2000, 0).unwrap();
    assert_eq!(out.num_rows(), 4);
    // SUM over {self, prev}: key 1 -> 10, 10+20, 20+30; key 2 (lone row) -> 100.
    assert_eq!(values(&out, 1), vec![10, 20, 30, 100]); // v passed through
    assert_eq!(values(&out, 3), vec![10, 30, 50, 100]);
}

// Bounded RANGE frame (1 SECOND PRECEDING): each row's SUM covers the rows within 1000ms of it,
// by rowtime interval rather than a physical row count.
#[test]
fn bounded_range_over_sums_the_time_interval() {
    let k: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 1]));
    let v: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30]));
    let rt: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        0i64,
        1_000_000_000,
        2_000_000_000,
    ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new(
            "rt",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(schema, vec![k, v, rt]).unwrap();
    // frame_kind 2 = bounded RANGE, offset 1000 = a 1000ms preceding interval.
    let mut over = OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 2, 1000, false);
    over.push(batch, 0).unwrap();
    let out = over.flush(2000, 0).unwrap();
    assert_eq!(out.num_rows(), 3);
    // SUM over rows within 1000ms: rt0 -> {10}, rt1000 -> {10,20}, rt2000 -> {20,30}.
    assert_eq!(values(&out, 3), vec![10, 30, 50]);
}

// Proctime OVER: rows fold in arrival order and emit immediately (no watermark). The running SUM
// per key advances row by row in the order the rows arrive.
#[test]
fn proctime_over_running_sum_in_arrival_order() {
    let k: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 1]));
    let v: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 100, 20]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(schema, vec![k, v]).unwrap();
    // rt_column is ignored in proctime mode (arrival order); value col 1, key col 0, unbounded.
    let mut over = OverWindowAggregator::new(vec![0], vec![0], 0, vec![1], vec![0], 0, 0, true);
    let out = over.push_proctime(batch, 0).unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(values(&out, 1), vec![10, 100, 20]); // v passed through
    assert_eq!(values(&out, 2), vec![10, 100, 30]); // running SUM per key, in arrival order
}

// Independent value columns in one OVER group: SUM(v0) and MAX(v1) read different input columns.
#[test]
fn over_independent_value_columns() {
    let k: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 1]));
    let v0: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30]));
    let v1: ArrayRef = Arc::new(Int64Array::from(vec![5i64, 15, 10]));
    let rt: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
        0i64,
        1_000_000_000,
        2_000_000_000,
    ]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v0", DataType::Int64, true),
        Field::new("v1", DataType::Int64, true),
        Field::new(
            "rt",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(schema, vec![k, v0, v1, rt]).unwrap();
    // value types [bigint, bigint]; columns [1, 2]; kinds [SUM, MAX]; rt col 3; key col 0; unbounded.
    let mut over =
        OverWindowAggregator::new(vec![0, 0], vec![0, 2], 3, vec![1, 2], vec![0], 0, 0, false);
    over.push(batch, 0).unwrap();
    let out = over.flush(2000, 0).unwrap();
    assert_eq!(out.num_rows(), 3);
    assert_eq!(values(&out, 4), vec![10, 30, 60]); // running SUM(v0)
    assert_eq!(values(&out, 5), vec![5, 15, 15]); // running MAX(v1)
}

/// Running SUM over key column 0, value column 1, rowtime column 2 (a `join_batch`), with the
/// given frame and idle-state retention.
fn retention_over(
    frame_kind: i64,
    frame_offset: i64,
    proctime: bool,
    retention_ms: i64,
) -> OverWindowAggregator {
    OverWindowAggregator::new(
        vec![0],
        vec![0],
        2,
        vec![1],
        vec![0],
        frame_kind,
        frame_offset,
        proctime,
    )
    .with_state_retention(retention_ms)
}

/// The per-key state batches of an OVER snapshot (the framed accumulators IPC section).
fn over_acc_batches(snapshot: &[u8]) -> Vec<RecordBatch> {
    let len = u32::from_le_bytes(snapshot[8..12].try_into().unwrap()) as usize;
    read_ipc_if_present(&snapshot[12..12 + len])
}

// Flink retention-bounds the rowtime OVER shapes with ONE per-key processing-time cleanup
// deadline (min = table.exec.state.ttl, max = 1.5x min), registered on every element: when the
// clock reaches it, the key's accumulator clears silently and the next fold restarts fresh. A
// timer registered at T fires once processing time reaches T, so the state is gone at `now >= T`.
#[test]
fn over_retention_idle_key_folds_fresh_at_exactly_the_deadline() {
    // Alive one millisecond inside the horizon: deadline = 5000 + 1.5 * 2000 = 8000.
    let mut alive = retention_over(0, 0, false, 2000);
    alive
        .push(join_batch(vec![1], vec![10], vec![100]), 5000)
        .unwrap();
    assert_eq!(values(&alive.flush(200, 5000).unwrap(), 3), vec![10]);
    alive
        .push(join_batch(vec![1], vec![5], vec![300]), 7999)
        .unwrap();
    assert_eq!(values(&alive.flush(400, 7999).unwrap(), 3), vec![15]);

    // Cleared at exactly the deadline: the running sum restarts from the new row alone.
    let mut expired = retention_over(0, 0, false, 2000);
    expired
        .push(join_batch(vec![1], vec![10], vec![100]), 5000)
        .unwrap();
    assert_eq!(values(&expired.flush(200, 5000).unwrap(), 3), vec![10]);
    expired
        .push(join_batch(vec![1], vec![5], vec![300]), 8000)
        .unwrap();
    assert_eq!(values(&expired.flush(400, 8000).unwrap(), 3), vec![5]);
}

// Flink's re-registration hysteresis: the deadline starts at now + max and moves (to now + max)
// only when a touch lands within a min-retention of it — `now + min > deadline`, strictly.
// Pinned with the three-write sequence: 1000 registers 4000, 2000 leaves it (2000 + min == 4000,
// not >), 2001 moves it to 5001.
#[test]
fn over_retention_moves_the_deadline_only_past_the_hysteresis() {
    let mut unmoved = retention_over(0, 0, false, 2000);
    unmoved
        .push(join_batch(vec![1], vec![10], vec![100]), 1000)
        .unwrap();
    assert_eq!(values(&unmoved.flush(200, 1000).unwrap(), 3), vec![10]);
    unmoved
        .push(join_batch(vec![1], vec![1], vec![300]), 2000)
        .unwrap();
    assert_eq!(values(&unmoved.flush(400, 2000).unwrap(), 3), vec![11]);
    // The touch at 2000 did NOT move the 4000 deadline: the key folds fresh at 4000.
    unmoved
        .push(join_batch(vec![1], vec![2], vec![500]), 4000)
        .unwrap();
    assert_eq!(values(&unmoved.flush(600, 4000).unwrap(), 3), vec![2]);

    let mut moved = retention_over(0, 0, false, 2000);
    moved
        .push(join_batch(vec![1], vec![10], vec![100]), 1000)
        .unwrap();
    assert_eq!(values(&moved.flush(200, 1000).unwrap(), 3), vec![10]);
    moved
        .push(join_batch(vec![1], vec![1], vec![300]), 2000)
        .unwrap();
    assert_eq!(values(&moved.flush(400, 2000).unwrap(), 3), vec![11]);
    moved
        .push(join_batch(vec![1], vec![3], vec![450]), 2001)
        .unwrap();
    assert_eq!(values(&moved.flush(500, 2001).unwrap(), 3), vec![14]);
    // The touch at 2001 moved the deadline to 5001: a touch at 3001 (3001 + min == 5001, not >
    // — deadline unmoved) still folds...
    moved
        .push(join_batch(vec![1], vec![2], vec![600]), 3001)
        .unwrap();
    assert_eq!(values(&moved.flush(700, 3001).unwrap(), 3), vec![16]);
    // ...and the key folds fresh at the moved deadline.
    moved
        .push(join_batch(vec![1], vec![4], vec![800]), 5001)
        .unwrap();
    assert_eq!(values(&moved.flush(900, 5001).unwrap(), 3), vec![4]);
}

// Flink's fired cleanup timer DEFERS a key that still has buffered rows the watermark has not
// folded (its onTimer re-registers and waits) — the sweep re-arms such a key instead of clearing
// it, so the buffered row later folds into the surviving accumulator.
#[test]
fn over_retention_defers_a_key_with_pending_rows() {
    let mut over = retention_over(0, 0, false, 2000);
    over.push(join_batch(vec![1], vec![10], vec![100]), 1000)
        .unwrap();
    assert_eq!(values(&over.flush(200, 1000).unwrap(), 3), vec![10]);
    // A row far above the watermark keeps key 1 pending past its 4000 deadline.
    over.push(join_batch(vec![1], vec![1], vec![9000]), 1000)
        .unwrap();
    // The touch of another key at 5000 sweeps: key 1 is due but deferred (re-armed to 8000).
    over.push(join_batch(vec![2], vec![99], vec![9000]), 5000)
        .unwrap();
    let out = over.flush(10_000, 5000).unwrap();
    assert_eq!(values(&out, 0), vec![1, 2]);
    assert_eq!(values(&out, 3), vec![11, 99]); // key 1 continued from 10, not fresh
}

// Keys never touched again are reclaimed by the silent once-per-min-retention sweep — the lazy
// per-touch check would never see them.
#[test]
fn over_retention_sweep_reclaims_untouched_keys_silently() {
    let mut over = retention_over(0, 0, false, 2000);
    over.push(join_batch(vec![1], vec![10], vec![100]), 1000)
        .unwrap();
    assert_eq!(values(&over.flush(200, 1000).unwrap(), 3), vec![10]);
    // Key 1 is never touched again; an ingest of another key past its 4000 deadline runs the
    // sweep, which drops key 1's accumulator and deadline with no output.
    over.push(join_batch(vec![2], vec![99], vec![300]), 4000)
        .unwrap();
    assert_eq!(values(&over.flush(400, 4000).unwrap(), 3), vec![99]);
    let accs = over_acc_batches(&over.snapshot());
    assert_eq!(
        accs.iter().flat_map(|b| values(b, 0)).collect::<Vec<_>>(),
        vec![2]
    );
}

// A rowtime bounded-ROWS frame clears its buffered frame rows at the deadline too: the next
// frame restarts short instead of reaching back across the expiry.
#[test]
fn bounded_rows_over_retention_clears_the_frame() {
    let mut alive = retention_over(1, 1, false, 2000);
    alive
        .push(join_batch(vec![1], vec![10], vec![1000]), 1000)
        .unwrap();
    assert_eq!(values(&alive.flush(1000, 1000).unwrap(), 3), vec![10]);
    alive
        .push(join_batch(vec![1], vec![20], vec![2000]), 3999)
        .unwrap();
    assert_eq!(values(&alive.flush(2000, 3999).unwrap(), 3), vec![30]);

    let mut expired = retention_over(1, 1, false, 2000);
    expired
        .push(join_batch(vec![1], vec![10], vec![1000]), 1000)
        .unwrap();
    assert_eq!(values(&expired.flush(1000, 1000).unwrap(), 3), vec![10]);
    expired
        .push(join_batch(vec![1], vec![20], vec![2000]), 4000)
        .unwrap();
    assert_eq!(values(&expired.flush(2000, 4000).unwrap(), 3), vec![20]);
}

// Flink's enablement quirk, replicated exactly: `stateCleaningEnabled = minRetentionTime > 1` —
// strictly greater than ONE millisecond, not zero. A 1ms retention never cleans, and its
// checkpoints stay byte-identical to the retention-off format (no stamp column).
#[test]
fn over_retention_of_one_millisecond_disables_cleaning() {
    let fold = |retention_ms: i64| {
        let mut over = retention_over(0, 0, false, retention_ms);
        over.push(join_batch(vec![1], vec![10], vec![100]), 1000)
            .unwrap();
        assert_eq!(values(&over.flush(200, 1000).unwrap(), 3), vec![10]);
        over.push(join_batch(vec![1], vec![5], vec![300]), i64::MAX)
            .unwrap();
        assert_eq!(values(&over.flush(400, i64::MAX).unwrap(), 3), vec![15]);
        over.snapshot()
    };
    assert_eq!(fold(1), fold(0));
    assert!(over_acc_batches(&fold(1))
        .iter()
        .all(|b| b.column_by_name(CLEANUP_AT_COLUMN).is_none()));
}

// The snapshot carries each key's ABSOLUTE deadline (a trailing per-key column, written only
// while cleaning is on); a restore keeps it as-is rather than re-stamping from the restore clock.
#[test]
fn over_retention_deadline_rides_the_snapshot_absolutely() {
    let mut writer = retention_over(0, 0, false, 2000);
    writer
        .push(join_batch(vec![1], vec![10], vec![100]), 5000)
        .unwrap();
    assert_eq!(values(&writer.flush(200, 5000).unwrap(), 3), vec![10]);
    let snapshot = writer.snapshot();
    assert!(over_acc_batches(&snapshot)
        .iter()
        .all(|b| b.column_by_name(CLEANUP_AT_COLUMN).is_some()));

    let restore = || {
        OverWindowAggregator::restore(
            vec![0],
            vec![0],
            2,
            vec![1],
            vec![0],
            0,
            0,
            false,
            &snapshot,
            2000,
            6000,
        )
    };
    // Alive at 7999 and fresh at exactly 8000 — the writer's deadline, not the restore-time
    // stamp (restoring at 6000 would have stamped 9000).
    let mut alive = restore();
    alive
        .push(join_batch(vec![1], vec![1], vec![300]), 7999)
        .unwrap();
    assert_eq!(values(&alive.flush(400, 7999).unwrap(), 3), vec![11]);
    let mut expired = restore();
    expired
        .push(join_batch(vec![1], vec![1], vec![300]), 8000)
        .unwrap();
    assert_eq!(values(&expired.flush(400, 8000).unwrap(), 3), vec![1]);
}

// Deadlines partition with their key groups and survive a partitioned restore. Restoring at 4000
// would stamp a missing deadline at 7000, so keys still folding at 7999 prove the column (with
// the writer's 8000) was read, per key group.
#[test]
fn over_retention_deadlines_partition_by_flink_key_group() {
    let mut before = retention_over(0, 0, false, 2000);
    before
        .push(join_batch(vec![1, 2], vec![10, 20], vec![0, 0]), 5000)
        .unwrap();
    assert_eq!(values(&before.flush(0, 5000).unwrap(), 3), vec![10, 20]);
    let partitions = before.snapshot_partitions(128, &[-1]);
    assert!(
        partitions.len() >= 2,
        "test keys should cover distinct raw key groups"
    );
    let snapshots: Vec<Vec<u8>> = partitions.into_values().collect();
    let mut restored = OverWindowAggregator::restore_partitions(
        vec![0],
        vec![0],
        2,
        vec![1],
        vec![0],
        0,
        0,
        false,
        &snapshots,
        2000,
        4000,
    );
    restored
        .push(join_batch(vec![1, 2], vec![1, 2], vec![100, 100]), 7999)
        .unwrap();
    let out = restored.flush(200, 7999).unwrap();
    let mut rows: Vec<(i64, i64)> = values(&out, 0).into_iter().zip(values(&out, 3)).collect();
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 11), (2, 22)]);
}

// A pre-retention snapshot restored into a retention-enabled OVER stamps every key a full max
// horizon from the restore (Flink's enable-TTL migration), instead of expiring on first touch.
#[test]
fn over_pre_retention_snapshot_stamps_a_full_deadline_at_restore() {
    let mut writer = retention_over(0, 0, false, 0);
    writer
        .push(join_batch(vec![1], vec![10], vec![100]), 0)
        .unwrap();
    assert_eq!(values(&writer.flush(200, 0).unwrap(), 3), vec![10]);
    let snapshot = writer.snapshot();

    let restore = || {
        OverWindowAggregator::restore(
            vec![0],
            vec![0],
            2,
            vec![1],
            vec![0],
            0,
            0,
            false,
            &snapshot,
            2000,
            10_000,
        )
    };
    // Stamped 10000 + max = 13000: alive at 12999, fresh at 13000.
    let mut alive = restore();
    alive
        .push(join_batch(vec![1], vec![1], vec![300]), 12_999)
        .unwrap();
    assert_eq!(values(&alive.flush(400, 12_999).unwrap(), 3), vec![11]);
    let mut expired = restore();
    expired
        .push(join_batch(vec![1], vec![1], vec![300]), 13_000)
        .unwrap();
    assert_eq!(values(&expired.flush(400, 13_000).unwrap(), 3), vec![1]);
}

// The bounded-RANGE rowtime frame takes NO retention: Flink's own function accepts none (its
// event-time frame eviction already bounds state), so a nonzero ttl changes nothing — no expiry,
// and no stamp column in the snapshot.
#[test]
fn bounded_range_over_ignores_retention() {
    let mut over = retention_over(2, 1000, false, 2000);
    over.push(join_batch(vec![1], vec![10], vec![0]), 1000)
        .unwrap();
    assert_eq!(values(&over.flush(0, 1000).unwrap(), 3), vec![10]);
    // Far past any would-be deadline, the 1000ms frame still reaches the earlier row.
    over.push(join_batch(vec![1], vec![20], vec![1000]), 1_000_000)
        .unwrap();
    assert_eq!(values(&over.flush(1000, 1_000_000).unwrap(), 3), vec![30]);
    assert!(over_acc_batches(&over.snapshot())
        .iter()
        .all(|b| retention_stamps(b).is_none()));
}

// The proctime unbounded fold runs Flink's per-value StateTtlConfig instead (OnCreateAndWrite /
// NeverReturnExpired): enabled at ANY positive retention (a 1ms ttl cleans — no `> 1` quirk),
// every write refreshes, and an expired accumulator reads as absent so the fold restarts.
#[test]
fn proctime_over_ttl_expires_an_idle_key_into_a_fresh_fold() {
    let mut over = retention_over(0, 0, true, 1);
    let out = over
        .push_proctime(join_batch(vec![1], vec![10], vec![0]), 5000)
        .unwrap();
    assert_eq!(values(&out, 3), vec![10]);
    // 5000 + 1 <= 5001: the 1ms ttl IS enabled and the fold restarts.
    let out = over
        .push_proctime(join_batch(vec![1], vec![5], vec![0]), 5001)
        .unwrap();
    assert_eq!(values(&out, 3), vec![5]);

    let mut refreshed = retention_over(0, 0, true, 1000);
    let out = refreshed
        .push_proctime(join_batch(vec![1], vec![10], vec![0]), 5000)
        .unwrap();
    assert_eq!(values(&out, 3), vec![10]);
    let out = refreshed
        .push_proctime(join_batch(vec![1], vec![1], vec![0]), 5900)
        .unwrap();
    assert_eq!(values(&out, 3), vec![11]);
    // The write at 5900 refreshed the clock: alive at 6800, expired at 7800 (inclusive).
    let out = refreshed
        .push_proctime(join_batch(vec![1], vec![2], vec![0]), 6800)
        .unwrap();
    assert_eq!(values(&out, 3), vec![13]);
    let out = refreshed
        .push_proctime(join_batch(vec![1], vec![4], vec![0]), 7800)
        .unwrap();
    assert_eq!(values(&out, 3), vec![4]);
}

// Window functions follow their order kind's scheme: under proctime the per-value TTL resets the
// counter state, visibly restarting ROW_NUMBER from 1 — observable and Flink-faithful.
#[test]
fn proctime_over_ttl_restarts_row_numbering() {
    let mut over = OverWindowAggregator::new(vec![], vec![10], 2, vec![], vec![0], 0, 0, true)
        .with_state_retention(2000);
    let out = over
        .push_proctime(join_batch(vec![1, 1], vec![0, 0], vec![0, 0]), 5000)
        .unwrap();
    assert_eq!(values(&out, 3), vec![1, 2]);
    let out = over
        .push_proctime(join_batch(vec![1], vec![0], vec![0]), 7000)
        .unwrap();
    assert_eq!(values(&out, 3), vec![1]); // 5000 + 2000 <= 7000: numbering restarted
}

// Proctime per-value TTL: last-write stamps ride the snapshot absolutely (the `__ttl_ts__`
// column), so expiry timing survives a restore.
#[test]
fn proctime_over_ttl_stamps_survive_snapshot_restore() {
    let mut writer = retention_over(0, 0, true, 2000);
    writer
        .push_proctime(join_batch(vec![1], vec![10], vec![0]), 5000)
        .unwrap();
    let snapshot = writer.snapshot();
    assert!(over_acc_batches(&snapshot)
        .iter()
        .all(|b| b.column_by_name(TTL_TS_COLUMN).is_some()));

    let restore = || {
        OverWindowAggregator::restore(
            vec![0],
            vec![0],
            2,
            vec![1],
            vec![0],
            0,
            0,
            true,
            &snapshot,
            2000,
            6500,
        )
    };
    // The adopted last-write is the writer's 5000 (expiry at 7000), not the restore-time 6500.
    let mut alive = restore();
    let out = alive
        .push_proctime(join_batch(vec![1], vec![1], vec![0]), 6999)
        .unwrap();
    assert_eq!(values(&out, 3), vec![11]);
    let mut expired = restore();
    let out = expired
        .push_proctime(join_batch(vec![1], vec![1], vec![0]), 7000)
        .unwrap();
    assert_eq!(values(&out, 3), vec![1]);
}

// Proctime per-value TTL: keys never written again fall to the once-per-ttl-period sweep.
#[test]
fn proctime_over_ttl_sweep_reclaims_idle_keys_silently() {
    let mut over = retention_over(0, 0, true, 2000);
    over.push_proctime(join_batch(vec![1], vec![10], vec![0]), 1000)
        .unwrap();
    // Key 1 is never written again; the ingest of key 2 at 4000 sweeps it out of state.
    over.push_proctime(join_batch(vec![2], vec![99], vec![0]), 4000)
        .unwrap();
    let accs = over_acc_batches(&over.snapshot());
    assert_eq!(
        accs.iter().flat_map(|b| values(b, 0)).collect::<Vec<_>>(),
        vec![2]
    );
}

// Proctime bounded ROWS keeps the cleanup DEADLINE scheme (not per-value TTL) and, unlike the
// rowtime shapes, its fired timer has no deferral: at the deadline the retract-frame buffer
// clears unconditionally, so the frame observably restarts short — exactly Flink's
// ProcTimeRowsBoundedPrecedingFunction.
#[test]
fn proctime_bounded_rows_retention_clears_the_frame_at_the_deadline() {
    let mut alive = retention_over(1, 1, true, 2000);
    let out = alive
        .push_proctime(join_batch(vec![1], vec![10], vec![0]), 5000)
        .unwrap();
    assert_eq!(values(&out, 3), vec![10]);
    let out = alive
        .push_proctime(join_batch(vec![1], vec![20], vec![0]), 7999)
        .unwrap();
    assert_eq!(values(&out, 3), vec![30]);

    let mut expired = retention_over(1, 1, true, 2000);
    let out = expired
        .push_proctime(join_batch(vec![1], vec![10], vec![0]), 5000)
        .unwrap();
    assert_eq!(values(&out, 3), vec![10]);
    let out = expired
        .push_proctime(join_batch(vec![1], vec![15], vec![0]), 8000)
        .unwrap();
    assert_eq!(values(&out, 3), vec![15]);
}

// Two-phase cumulative: per-slice SUM partials merge into the nested windows of their bucket.
#[test]
fn cumulative_two_phase_merges_nested_windows() {
    // max size 3 s, step 1 s, cumulative, bigint value, SUM.
    let mut agg = TumblingAggregator::new(3000, 1000, true, vec![0], vec![0]);
    let partial = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Int64, false),
            Field::new("partial0", DataType::Int64, true),
            Field::new("slice_end", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 1, 1])),
            Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            Arc::new(Int64Array::from(vec![1000i64, 2000, 3000])),
        ],
    )
    .unwrap();
    agg.update_partial(&partial).unwrap();
    let out = agg.flush(3000).unwrap();
    // Nested windows share the bucket start 0; each accumulates the slices up to its end:
    // (0,1000]=10, (0,2000]=10+20=30, (0,3000]=10+20+30=60.
    assert_eq!(values(&out, 1), vec![0, 0, 0]); // window_start
    assert_eq!(values(&out, 2), vec![1000, 2000, 3000]); // window_end
    assert_eq!(values(&out, 3), vec![10, 30, 60]); // running SUM
}

// Window-attached local half (q5): rows carry explicit window_start/window_end (epoch millis)
// instead of a rowtime to slice; each folds into the one window it names, and flush_partial emits
// the per-window partial keyed by window end. No late-data drop — a row whose window the watermark
// has already reached still folds (the upstream emits it exactly at that watermark).
#[test]
fn window_attached_local_folds_per_named_window() {
    // SUM over bigint, no grouping key (grouped only by window). window/slide are unused by the
    // attached ingest, so their values are immaterial.
    let mut agg = TumblingAggregator::new(10000, 10000, false, vec![0], vec![0]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
            Field::new("value0", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0i64, 0, 2000])),
            Arc::new(Int64Array::from(vec![10000i64, 10000, 12000])),
            Arc::new(Int64Array::from(vec![3i64, 5, 7])),
        ],
    )
    .unwrap();
    agg.update_attached(&batch).unwrap();
    let out = agg.flush_partial(20000);
    // Output columns: [partial0, slice_end]. Windows emitted in ascending end order.
    assert_eq!(values(&out, 1), vec![10000, 12000]); // slice_end == the named window ends
    assert_eq!(values(&out, 0), vec![8, 7]); // (0,10000] sums 3+5, (2000,12000] sums 7
}

// A `[ts, value0, key0]` batch (bigint value and key) for the memory-accounting tests.
fn keyed_window_batch(ts_millis: i64, keys: Vec<i64>) -> RecordBatch {
    let n = keys.len();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value0", DataType::Int64, true),
            Field::new("key0", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![ts_millis; n])),
            Arc::new(Int64Array::from(vec![1i64; n])),
            Arc::new(Int64Array::from(keys)),
        ],
    )
    .unwrap()
}

// Open-window state grows the pool reservation, tracks the full-scan footprint exactly, and
// returns to zero when the windows close — the release-on-close half of memory accounting.
#[test]
fn window_state_reserves_and_releases_memory() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
    let mut agg = TumblingAggregator::new(1000, 1000, false, vec![0], vec![0])
        .with_memory_pool(&pool)
        .unwrap();
    agg.update(&keyed_window_batch(0, (0..50).collect()))
        .unwrap();
    agg.update(&keyed_window_batch(1500, (0..20).collect()))
        .unwrap();
    assert!(pool.reserved() > 0);
    assert_eq!(agg.memory.state_bytes, agg.computed_state_bytes()); // incremental tracking must not drift
    let both_windows = pool.reserved();

    agg.flush(1000).unwrap(); // closes the first window only
    assert_eq!(agg.memory.state_bytes, agg.computed_state_bytes());
    assert!(pool.reserved() > 0 && pool.reserved() < both_windows);

    agg.flush(2000).unwrap(); // closes the rest
    assert_eq!(pool.reserved(), 0);
    drop(agg);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn window_state_partitions_and_restores_by_flink_key_group() {
    let mut before = TumblingAggregator::new(1000, 1000, false, vec![0], vec![0]);
    before.update(&keyed_window_batch(0, vec![1, 2])).unwrap();
    let partitions = before.snapshot_partitions(128, &[-1]);
    assert!(!partitions.is_empty());
    let snapshots: Vec<Vec<u8>> = partitions.into_values().collect();

    let mut restored =
        TumblingAggregator::restore_partitions(1000, 1000, false, vec![0], vec![0], &snapshots);
    let out = restored.flush(1000).unwrap();
    let keys = out
        .column_by_name("key0")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    let sums = out
        .column_by_name("result0")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(keys, vec![1, 2]);
    assert_eq!(sums, vec![1, 1]);
}

// Exceeding the budget is a clear, attributable failure — not a container OOM.
#[test]
fn window_state_over_budget_fails_clearly() {
    let mut agg = TumblingAggregator::new(1000, 1000, false, vec![0], vec![0])
        .with_memory_budget(256)
        .unwrap();
    let err = agg
        .update(&keyed_window_batch(0, (0..100).collect()))
        .unwrap_err();
    assert!(err.to_string().contains("task off-heap"), "{err}");
}

#[test]
fn window_aggregate_counts_assigned_windows_dropped_as_late() {
    let mut agg = TumblingAggregator::new(1000, 1000, false, vec![0], vec![0]);
    agg.flush(1000).unwrap();
    agg.update(&keyed_window_batch(0, vec![1, 2])).unwrap();
    assert_eq!(agg.late_drops, 2);
}

// Every rolled-out state shape enforces its budget: exceeding it is an error, not an overrun.
// One test per shape (accumulator maps, byte-row maps, buffered batches, bounded buffers).
#[test]
fn session_state_over_budget_fails_clearly() {
    let mut agg = SessionAggregator::new(1000, vec![0], vec![0])
        .with_memory_budget(256)
        .unwrap();
    let err = agg
        .update(&keyed_window_batch(0, (0..100).collect()))
        .unwrap_err();
    assert!(err.to_string().contains("task off-heap"), "{err}");
}

#[test]
fn session_aggregate_counts_rows_whose_session_already_closed() {
    let mut agg = SessionAggregator::new(1000, vec![0], vec![0]);
    agg.flush(1000).unwrap();
    agg.update(&keyed_window_batch(0, vec![1, 2])).unwrap();
    assert_eq!(agg.late_drops, 2);
}

#[test]
fn session_aggregate_accepts_late_candidate_that_merges_into_open_session() {
    let mut agg = SessionAggregator::new(1000, vec![0], vec![0]);
    agg.update(&keyed_window_batch(600, vec![1])).unwrap();
    agg.flush(1000).unwrap();
    agg.update(&keyed_window_batch(0, vec![1])).unwrap();
    assert_eq!(agg.late_drops, 0);
    let out = agg.flush(1600).unwrap();
    assert_eq!(values(&out, 3), vec![2]);
}

#[test]
fn session_state_partitions_and_restores_by_flink_key_group() {
    let mut before = SessionAggregator::new(1000, vec![0], vec![0]);
    before.update(&keyed_window_batch(0, vec![1, 2])).unwrap();
    let partitions = before.snapshot_partitions(128, &[-1]);
    assert!(
        partitions.len() >= 2,
        "test keys should cover distinct raw key groups"
    );
    let snapshots: Vec<Vec<u8>> = partitions.into_values().collect();

    let mut restored = SessionAggregator::restore_partitions(1000, vec![0], vec![0], &snapshots);
    let out = restored.flush(1000).unwrap();
    assert_eq!(values(&out, 0), vec![1, 2]);
    assert_eq!(values(&out, 3), vec![1, 1]);
}

#[test]
fn group_state_over_budget_fails_and_deletes_release() {
    // A generous budget: inserts fit, and retracting every record shrinks the tracking to zero.
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true)
        .with_memory_budget(1 << 20)
        .unwrap();
    agg.update(
        &group_changelog(vec![1, 2], vec![Some(10), Some(20)], vec![0, 0]),
        0,
    )
    .unwrap();
    assert!(agg.memory.state_bytes > 0);
    agg.update(
        &group_changelog(vec![1, 2], vec![Some(10), Some(20)], vec![3, 3]),
        0,
    )
    .unwrap();
    assert_eq!(agg.memory.state_bytes, 0); // both groups deleted -> fully released

    let mut tight = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true)
        .with_memory_budget(128)
        .unwrap();
    let keys: Vec<i64> = (0..100).collect();
    let values: Vec<Option<i64>> = keys.iter().map(|&k| Some(k)).collect();
    let err = tight
        .update(&group_changelog(keys, values, vec![0; 100]), 0)
        .unwrap_err();
    assert!(err.to_string().contains("task off-heap"), "{err}");
}

#[test]
fn dedup_state_over_budget_fails_clearly() {
    // Keep-last over distinct keys stores one row per key; 100 keys cannot fit 64 bytes.
    let mut dedup = KeepLastDeduplicator::new(vec![0], 2, true, false, false)
        .with_memory_budget(64)
        .unwrap();
    let keys: Vec<i64> = (0..100).collect();
    let values: Vec<i64> = (0..100).collect();
    let rts: Vec<i64> = vec![0; 100];
    let err = dedup.push(&join_batch(keys, values, rts), 0).unwrap_err();
    assert!(err.to_string().contains("task off-heap"), "{err}");
}

// Proctime only: Flink's proctime mini-batch buffers just the last row per key (addInput
// overwrites), so its flush emits one net transition per key — the rowtime variant emits every
// kept row instead (see below).
#[test]
fn proctime_mini_batch_emits_only_the_final_winner_per_key() {
    let mut dedup = KeepLastDeduplicator::new(vec![0], 2, true, false, false).with_mini_batch(true);
    let pending = dedup
        .push(
            &join_batch(vec![1, 1, 2], vec![10, 20, 5], vec![0, 1, 0]),
            0,
        )
        .unwrap();
    assert_eq!(pending.num_rows(), 0);
    let first = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&first, 0), vec![1, 2]);
    assert_eq!(values(&first, 1), vec![20, 5]);
    assert_eq!(row_kinds(&first), vec![0, 0]);

    dedup
        .push(&join_batch(vec![1, 1], vec![30, 40], vec![2, 3]), 0)
        .unwrap();
    let second = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&second, 1), vec![20, 40]);
    assert_eq!(row_kinds(&second), vec![1, 2]);
}

// A rowtime keep-last deduplicator over `[k, v, rt]` (key col 0, rt col 2) in mini-batch mode.
fn rowtime_mini_batch() -> KeepLastDeduplicator {
    KeepLastDeduplicator::new(vec![0], 2, true, true, false).with_mini_batch(true)
}

// Rowtime mini-batch replicates Flink's RowTimeMiniBatchDeduplicateFunction: the flush emits a
// transition for EVERY row of the bundle that displaces the kept row ("we output all changelog
// here rather than comparing the first and the last record in buffer" — a temporal join's
// versioned table needs each intermediate version), grouped per key, not just the endpoint.
#[test]
fn rowtime_mini_batch_emits_every_kept_intermediate() {
    let mut dedup = rowtime_mini_batch();
    // Key 1's improving rows interleave with key 2's single row; the flush groups per key.
    dedup
        .push(
            &join_batch(vec![1, 2, 1, 1], vec![10, 5, 20, 30], vec![1, 1, 2, 3]),
            0,
        )
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![10, 10, 20, 20, 30, 5]);
    assert_eq!(row_kinds(&out), vec![0, 1, 2, 1, 2, 0]);

    // The next bundle's first transition retracts the durable state, then walks the bundle.
    dedup
        .push(&join_batch(vec![1, 1], vec![40, 50], vec![4, 5]), 0)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![30, 40, 40, 50]);
    assert_eq!(row_kinds(&out), vec![1, 2, 1, 2]);
}

// A non-improving (smaller-rowtime) row mid-bundle is ignored exactly as in immediate mode: no
// transition, no state write. An equal rowtime improves (Flink's keep-last `<=`).
#[test]
fn rowtime_mini_batch_ignores_a_non_improving_row_mid_bundle() {
    let mut dedup = rowtime_mini_batch();
    dedup
        .push(
            &join_batch(vec![1, 1, 1, 1], vec![10, 20, 30, 40], vec![5, 3, 5, 4]),
            0,
        )
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![10, 10, 30]);
    assert_eq!(row_kinds(&out), vec![0, 1, 2]);
}

// The rowtime path has no equality check, so an identical kept row emits its -U/+U pair with
// retention off and on alike (the proctime endpoint suppression never applies to this shape).
#[test]
fn rowtime_mini_batch_never_suppresses_identical_transitions() {
    for ttl_ms in [0, 3_600_000] {
        let mut dedup = rowtime_mini_batch().with_state_ttl(ttl_ms);
        dedup
            .push(&join_batch(vec![1], vec![10], vec![1]), 5000)
            .unwrap();
        dedup.flush_mini_batch().unwrap();
        // Identical payload at an equal rowtime: kept (`<=`), and emitted verbatim.
        dedup
            .push(&join_batch(vec![1, 1], vec![10, 10], vec![1, 1]), 5001)
            .unwrap();
        let out = dedup.flush_mini_batch().unwrap();
        assert_eq!(values(&out, 1), vec![10, 10, 10, 10], "ttl={ttl_ms}");
        assert_eq!(row_kinds(&out), vec![1, 2, 1, 2], "ttl={ttl_ms}");
    }
}

// A key that expired between bundles chains from a fresh +I: the delete-on-read stages a None
// preimage and the bundle's later kept rows still each emit their transition.
#[test]
fn rowtime_mini_batch_chains_from_a_fresh_insert_after_expiry() {
    let mut dedup = rowtime_mini_batch().with_state_ttl(1000);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![1]), 5000)
        .unwrap();
    dedup.flush_mini_batch().unwrap();
    // 5000 + 1000 <= 6000: expired — the bundle restarts the key at +I and keeps chaining.
    dedup
        .push(&join_batch(vec![1, 1], vec![20, 30], vec![2, 3]), 6000)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![20, 20, 30]);
    assert_eq!(row_kinds(&out), vec![0, 1, 2]);
}

// The -U halves of the chain honor generate_update_before, like every keep-last emission.
#[test]
fn rowtime_mini_batch_chain_honors_generate_update_before() {
    let mut dedup = KeepLastDeduplicator::new(vec![0], 2, false, true, false).with_mini_batch(true);
    dedup
        .push(&join_batch(vec![1, 1], vec![10, 20], vec![1, 2]), 0)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![10, 20]);
    assert_eq!(row_kinds(&out), vec![0, 2]);
}

// Flink's default rowtime mini-batch flush runs state.update(preRow) for every key the bundle
// buffered rows for — even a bundle whose rows were ALL ignored re-stamps the key's TTL clock.
#[test]
fn rowtime_mini_batch_all_ignored_bundle_refreshes_ttl() {
    let mut dedup = rowtime_mini_batch().with_state_ttl(1000);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![100]), 5000)
        .unwrap();
    dedup.flush_mini_batch().unwrap();
    // An all-ignored bundle (older rowtime) emits nothing but refreshes the key at 5900.
    dedup
        .push(&join_batch(vec![1], vec![20], vec![50]), 5900)
        .unwrap();
    assert_eq!(dedup.flush_mini_batch().unwrap().num_rows(), 0);
    // 5000 + 1000 <= 6800 but 5900 + 1000 > 6800: still alive — a -U/+U chain, not a fresh +I.
    dedup
        .push(&join_batch(vec![1], vec![30], vec![200]), 6800)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![10, 30]);
    assert_eq!(row_kinds(&out), vec![1, 2]);
}

// A rowtime compact-changes deduplicator over `[k, v, rt]` (key col 0, rt col 2).
fn rowtime_compact_changes() -> KeepLastDeduplicator {
    rowtime_mini_batch().with_compact_changes(true)
}

// Compact-changes (RowTimeMiniBatchLatestChangeDeduplicateFunction) nets each key's bundle to one
// transition: a fresh key's whole improving chain collapses to a single +I, an existing key's to
// one -U(stored)/+U(endpoint) pair.
#[test]
fn compact_changes_nets_each_bundle_to_one_transition_per_key() {
    let mut dedup = rowtime_compact_changes();
    dedup
        .push(
            &join_batch(vec![1, 2, 1, 1], vec![10, 5, 20, 30], vec![1, 1, 2, 3]),
            0,
        )
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![30, 5]);
    assert_eq!(row_kinds(&out), vec![0, 0]);

    // The next bundle nets against the durable state: one pair from 30 to the endpoint 50, with
    // the mid-bundle non-improving row (rt 1 < stored 3) ignored.
    dedup
        .push(
            &join_batch(vec![1, 1, 1], vec![40, 15, 50], vec![4, 1, 5]),
            0,
        )
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![30, 50]);
    assert_eq!(row_kinds(&out), vec![1, 2]);
}

// A bundle whose rows all lose to the stored row emits nothing AND writes nothing — unlike the
// default flush there is no unconditional state.update, so the key's TTL is not refreshed.
#[test]
fn compact_changes_losing_bundle_emits_nothing_and_does_not_refresh_ttl() {
    let mut dedup = rowtime_compact_changes().with_state_ttl(1000);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![100]), 5000)
        .unwrap();
    dedup.flush_mini_batch().unwrap();
    dedup
        .push(&join_batch(vec![1], vec![20], vec![50]), 5900)
        .unwrap();
    assert_eq!(dedup.flush_mini_batch().unwrap().num_rows(), 0);
    // 5000 + 1000 <= 6000: expired despite the losing bundle at 5900 — a fresh +I.
    dedup
        .push(&join_batch(vec![1], vec![30], vec![10]), 6000)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![30]);
    assert_eq!(row_kinds(&out), vec![0]);
}

// Like every rowtime shape, compact-changes has no equality check: an identical row at an equal
// rowtime displaces (keep-last `<=`) and its -U/+U pair emits verbatim, TTL on or off.
#[test]
fn compact_changes_never_suppresses_an_identical_displacement() {
    for ttl_ms in [0, 3_600_000] {
        let mut dedup = rowtime_compact_changes().with_state_ttl(ttl_ms);
        dedup
            .push(&join_batch(vec![1], vec![10], vec![1]), 5000)
            .unwrap();
        dedup.flush_mini_batch().unwrap();
        dedup
            .push(&join_batch(vec![1], vec![10], vec![1]), 5001)
            .unwrap();
        let out = dedup.flush_mini_batch().unwrap();
        assert_eq!(values(&out, 1), vec![10, 10], "ttl={ttl_ms}");
        assert_eq!(row_kinds(&out), vec![1, 2], "ttl={ttl_ms}");
    }
}

// The netted pair's -U half honors generate_update_before, like every keep-last emission.
#[test]
fn compact_changes_honors_generate_update_before() {
    let mut dedup = KeepLastDeduplicator::new(vec![0], 2, false, true, false)
        .with_mini_batch(true)
        .with_compact_changes(true);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![1]), 0)
        .unwrap();
    dedup.flush_mini_batch().unwrap();
    dedup
        .push(&join_batch(vec![1, 1], vec![20, 30], vec![2, 3]), 0)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![30]);
    assert_eq!(row_kinds(&out), vec![2]);
}

// With neither update-befores nor inserts requested (Flink's insert-sensitivity option off under
// an only-update-after consumer), a fresh key's first emission is a bare +U — never +I — and a
// replacement stays a lone +U.
#[test]
fn insert_insensitive_fresh_key_emits_a_bare_update_after() {
    let mut dedup =
        KeepLastDeduplicator::new(vec![0], 2, false, true, false).with_generate_insert(false);
    let out = dedup
        .push(&join_batch(vec![1], vec![10], vec![1]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![2]);
    let out = dedup
        .push(&join_batch(vec![1], vec![20], vec![2]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![2]);
    assert_eq!(values(&out, 1), vec![20]);
}

// The rowtime mini-batch flush walks a fresh key's kept chain in all-+U transitions.
#[test]
fn insert_insensitive_mini_batch_chain_is_all_update_after() {
    let mut dedup = KeepLastDeduplicator::new(vec![0], 2, false, true, false)
        .with_mini_batch(true)
        .with_generate_insert(false);
    dedup
        .push(
            &join_batch(vec![1, 1, 2], vec![10, 20, 5], vec![1, 2, 1]),
            0,
        )
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![10, 20, 5]);
    assert_eq!(row_kinds(&out), vec![2, 2, 2]);
}

// Compact-changes nets a fresh key's bundle to a single bare +U endpoint.
#[test]
fn insert_insensitive_compact_changes_endpoint_is_a_bare_update_after() {
    let mut dedup = KeepLastDeduplicator::new(vec![0], 2, false, true, false)
        .with_mini_batch(true)
        .with_compact_changes(true)
        .with_generate_insert(false);
    dedup
        .push(&join_batch(vec![1, 1], vec![10, 20], vec![1, 2]), 0)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![20]);
    assert_eq!(row_kinds(&out), vec![2]);
}

// Proctime keep-last mirrors Flink's stateless bare-+U branch: the fresh key emits +U and an
// identical duplicate is NOT suppressed (the equality check lives in the insert/update-before
// branch, which this mode never enters).
#[test]
fn insert_insensitive_proctime_emits_every_row_unsuppressed() {
    let mut dedup =
        KeepLastDeduplicator::new(vec![0], 2, false, false, false).with_generate_insert(false);
    let out = dedup
        .push(&join_batch(vec![1], vec![10], vec![7]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![2]);
    let out = dedup
        .push(&join_batch(vec![1], vec![10], vec![7]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![2]); // identical — still emitted
    assert_eq!(values(&out, 1), vec![10]);
}

// The proctime mini-batch flush still nets each key's bundle to its endpoint, emitted as the same
// bare +U — a net no-op bundle included.
#[test]
fn insert_insensitive_proctime_mini_batch_flush_is_all_update_after() {
    let mut dedup = KeepLastDeduplicator::new(vec![0], 2, false, false, false)
        .with_mini_batch(true)
        .with_generate_insert(false);
    dedup
        .push(&join_batch(vec![1, 1], vec![10, 20], vec![7, 7]), 0)
        .unwrap();
    assert_eq!(row_kinds(&dedup.flush_mini_batch().unwrap()), vec![2]);
    dedup
        .push(&join_batch(vec![1], vec![20], vec![7]), 0)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(row_kinds(&out), vec![2]);
    assert_eq!(values(&out, 1), vec![20]);
}

// A rowtime keep-first deduplicator over `[k, v, rt]` in mini-batch mode — Flink's bundled
// retracting shape (RowTimeMiniBatchDeduplicateFunction with keepLastRow=false): keep-last's
// machinery with the comparator flipped, so a strictly smaller rowtime displaces with -U/+U.
fn rowtime_keep_first_mini_batch() -> KeepLastDeduplicator {
    KeepLastDeduplicator::new(vec![0], 2, true, true, true).with_mini_batch(true)
}

// The default flush emits a transition for EVERY kept (rowtime-decreasing) row of the bundle,
// grouped per key — the full chain, exactly like keep-last with the comparison flipped.
#[test]
fn keep_first_mini_batch_emits_every_kept_improvement() {
    let mut dedup = rowtime_keep_first_mini_batch();
    dedup
        .push(
            &join_batch(
                vec![1, 2, 1, 1],
                vec![10, 5, 20, 30],
                vec![300, 500, 200, 100],
            ),
            0,
        )
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![10, 10, 20, 20, 30, 5]);
    assert_eq!(row_kinds(&out), vec![0, 1, 2, 1, 2, 0]);

    // The next bundle's improvement retracts the durable state.
    dedup
        .push(&join_batch(vec![1], vec![40], vec![50]), 0)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![30, 40]);
    assert_eq!(row_kinds(&out), vec![1, 2]);
}

// Keep-first keeps a row only on a strictly smaller rowtime (Flink's `<`): a tie keeps the
// incumbent, and an at-or-above rowtime is ignored with no transition and no state write.
#[test]
fn keep_first_mini_batch_a_tie_keeps_the_incumbent() {
    let mut dedup = rowtime_keep_first_mini_batch();
    dedup
        .push(
            &join_batch(vec![1, 1, 1], vec![10, 20, 30], vec![100, 100, 150]),
            0,
        )
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![10]);
    assert_eq!(row_kinds(&out), vec![0]);
}

// The -U halves of the keep-first chain honor generate_update_before too.
#[test]
fn keep_first_mini_batch_chain_honors_generate_update_before() {
    let mut dedup = KeepLastDeduplicator::new(vec![0], 2, false, true, true).with_mini_batch(true);
    dedup
        .push(&join_batch(vec![1, 1], vec![10, 20], vec![300, 200]), 0)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![10, 20]);
    assert_eq!(row_kinds(&out), vec![0, 2]);
}

// Compact-changes applies to keep-first identically: each bundle nets to one transition per key,
// ending at the bundle's minimum-rowtime row.
#[test]
fn keep_first_compact_changes_nets_to_the_bundle_endpoint() {
    let mut dedup = rowtime_keep_first_mini_batch().with_compact_changes(true);
    dedup
        .push(
            &join_batch(vec![1, 1, 1], vec![10, 20, 15], vec![300, 200, 250]),
            0,
        )
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![20]);
    assert_eq!(row_kinds(&out), vec![0]);

    dedup
        .push(&join_batch(vec![1], vec![30], vec![100]), 0)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![20, 30]);
    assert_eq!(row_kinds(&out), vec![1, 2]);
}

// TTL: an idle keep-first key expires like any other; the next row — improving or not — re-enters
// through the fresh +I path (delete-on-read, Flink's NeverReturnExpired).
#[test]
fn keep_first_mini_batch_expires_an_idle_key_into_a_fresh_insert() {
    let mut dedup = rowtime_keep_first_mini_batch().with_state_ttl(1000);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![100]), 5000)
        .unwrap();
    dedup.flush_mini_batch().unwrap();
    // rt 200 >= stored 100 would be ignored while alive, but 5000 + 1000 <= 6000: expired.
    dedup
        .push(&join_batch(vec![1], vec![20], vec![200]), 6000)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 1), vec![20]);
    assert_eq!(row_kinds(&out), vec![0]);
}

// A proctime keep-last deduplicator over `[k, v, rt]` (key col 0; the rt column is ignored).
fn proctime_keep_last() -> KeepLastDeduplicator {
    KeepLastDeduplicator::new(vec![0], 2, true, false, false)
}

// State TTL: an idle key expires ttl millis after its last write; the next row is a fresh +I
// (Flink's NeverReturnExpired: expired reads as absent).
#[test]
fn dedup_ttl_expires_an_idle_key_into_a_fresh_insert() {
    let mut dedup = proctime_keep_last().with_state_ttl(1000);
    let out = dedup
        .push(&join_batch(vec![1], vec![10], vec![0]), 5000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // +I 10
                                          // ts 5000 + ttl 1000 <= 6000: expired exactly at the boundary — a fresh +I, not -U/+U.
    let out = dedup
        .push(&join_batch(vec![1], vec![5], vec![0]), 6000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![5]);
}

// A write refreshes the TTL (OnCreateAndWrite): steadily-touched keys never expire, and expiry
// is timed from the LAST write.
#[test]
fn dedup_ttl_refreshes_on_every_write() {
    let mut dedup = proctime_keep_last().with_state_ttl(1000);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![0]), 5000)
        .unwrap();
    let out = dedup
        .push(&join_batch(vec![1], vec![20], vec![0]), 5900)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // alive: -U(10)/+U(20)
                                             // 900ms later the original write is long past ttl, but the refresh at 5900 keeps it alive.
    let out = dedup
        .push(&join_batch(vec![1], vec![30], vec![0]), 6800)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]);
    assert_eq!(values(&out, 1), vec![20, 30]);
}

// A rowtime order ignores an older-rowtime row WITHOUT a state write (Flink's rowtime helper
// returns before updateState when the row isn't kept), so the key still expires on the schedule
// of its last kept row — and an expired stored row plus an older-rowtime arrival is a fresh +I,
// not an ignore (the expiry check runs before the rowtime comparison).
#[test]
fn dedup_ttl_ignored_older_rowtime_row_does_not_refresh() {
    let mut dedup = KeepLastDeduplicator::new(vec![0], 2, true, true, false).with_state_ttl(1000);
    let out = dedup
        .push(&join_batch(vec![1], vec![10], vec![100]), 5000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    // Older rowtime while alive: ignored, and NOT a TTL refresh.
    let out = dedup
        .push(&join_batch(vec![1], vec![20], vec![50]), 5900)
        .unwrap();
    assert_eq!(out.num_rows(), 0);
    // 5000 + 1000 <= 6000: expired despite the 5900 touch — the older-rowtime row re-enters fresh.
    let out = dedup
        .push(&join_batch(vec![1], vec![30], vec![10]), 6000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![30]);
}

// With TTL off, proctime keep-last suppresses an identical row exactly as Flink's
// processLastRowOnProcTime does on the default heap backend: its generated equaliser compares
// row kinds first, and emitting an update mutates the stored (aliased) row's kind to
// UPDATE_AFTER — so only a duplicate of a still-INSERT-stored row (a key that has never emitted
// an update) is suppressed; after any update, identical rows emit an identical -U/+U pair.
#[test]
fn dedup_ttl_off_suppresses_an_identical_proctime_row_until_the_first_update() {
    let mut dedup = proctime_keep_last();
    let out = dedup
        .push(&join_batch(vec![1], vec![10], vec![7]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    let out = dedup
        .push(&join_batch(vec![1], vec![10], vec![7]), 0)
        .unwrap();
    assert_eq!(out.num_rows(), 0); // stored kind INSERT — suppressed
    let out = dedup
        .push(&join_batch(vec![1], vec![10], vec![7]), 0)
        .unwrap();
    assert_eq!(out.num_rows(), 0); // a suppressed duplicate re-stores as INSERT — still suppressed
    let out = dedup
        .push(&join_batch(vec![1], vec![20], vec![7]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // state intact: -U(10)/+U(20)
    assert_eq!(values(&out, 1), vec![10, 20]);
    // The stored row is now kind UPDATE_AFTER: Flink's kind-sensitive equaliser no longer sees
    // the identical row as equal, so the pair emits.
    let out = dedup
        .push(&join_batch(vec![1], vec![20], vec![7]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // -U(20)/+U(20), no longer suppressed
    assert_eq!(values(&out, 1), vec![20, 20]);
}

// The stored row's kind survives a snapshot, exactly like Flink's heap backend serializing the
// mutated kind into its checkpoints: an updated key keeps emitting identical pairs after a
// restore, and a never-updated key keeps suppressing.
#[test]
fn dedup_stored_kind_survives_snapshot_restore() {
    let mut dedup = proctime_keep_last();
    dedup
        .push(&join_batch(vec![1, 2], vec![10, 50], vec![7, 7]), 0)
        .unwrap();
    dedup
        .push(&join_batch(vec![1], vec![20], vec![7]), 0)
        .unwrap(); // key 1 now UPDATE_AFTER
    let snapshot = dedup.snapshot();
    let mut restored =
        KeepLastDeduplicator::restore(vec![0], vec![-1], 2, true, false, false, &snapshot, 0);
    let out = restored
        .push(&join_batch(vec![1], vec![20], vec![7]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // identical, but kind +U — emits
    let out = restored
        .push(&join_batch(vec![2], vec![50], vec![7]), 0)
        .unwrap();
    assert_eq!(out.num_rows(), 0); // key 2 was never updated — still suppressed
}

// The mini-batch flush applies the same stored-kind rule (its flush runs the same
// processLastRowOnProcTime): a net no-op bundle is suppressed only until the key's first emitted
// update.
#[test]
fn dedup_mini_batch_suppresses_unchanged_bundles_until_the_first_update() {
    let mut dedup = proctime_keep_last().with_mini_batch(true);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![7]), 0)
        .unwrap();
    assert_eq!(row_kinds(&dedup.flush_mini_batch().unwrap()), vec![0]);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![7]), 0)
        .unwrap();
    assert_eq!(dedup.flush_mini_batch().unwrap().num_rows(), 0); // no-op bundle, kind INSERT
    dedup
        .push(&join_batch(vec![1], vec![20], vec![7]), 0)
        .unwrap();
    assert_eq!(row_kinds(&dedup.flush_mini_batch().unwrap()), vec![1, 2]); // kind now +U
    dedup
        .push(&join_batch(vec![1], vec![20], vec![7]), 0)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // -U(20)/+U(20) — no longer suppressed
    assert_eq!(values(&out, 1), vec![20, 20]);
}

// The suppression is proctime-only: Flink's rowtime helper emits through
// updateDeduplicateResult with no equality check, so an identical kept row still emits -U/+U.
#[test]
fn dedup_rowtime_keep_last_never_suppresses_an_identical_row() {
    let mut dedup = KeepLastDeduplicator::new(vec![0], 2, true, true, false);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![100]), 0)
        .unwrap();
    let out = dedup
        .push(&join_batch(vec![1], vec![10], vec![100]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // -U(10)/+U(10), never suppressed
}

// With TTL on, the identical-row suppression is disabled: Flink always emits -U/+U so
// downstream TTL state keeps refreshing (the deterministic, parity-testable TTL behavior).
#[test]
fn dedup_ttl_emits_the_identical_row_it_would_otherwise_suppress() {
    let mut dedup = proctime_keep_last().with_state_ttl(3_600_000);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![7]), 5000)
        .unwrap();
    let out = dedup
        .push(&join_batch(vec![1], vec![10], vec![7]), 5001)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // -U(10)/+U(10), not suppressed
    assert_eq!(values(&out, 1), vec![10, 10]);

    // The -U half still honors generate_update_before.
    let mut no_before =
        KeepLastDeduplicator::new(vec![0], 2, false, false, false).with_state_ttl(3_600_000);
    no_before
        .push(&join_batch(vec![1], vec![10], vec![7]), 5000)
        .unwrap();
    let out = no_before
        .push(&join_batch(vec![1], vec![10], vec![7]), 5001)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![2]);
}

// Proctime keep-first writes state only for the FIRST row (Flink's processFirstRowOnProcTime):
// a dropped duplicate does not refresh the TTL, so a hot key still expires and re-emits +I.
#[test]
fn dedup_ttl_keep_first_duplicate_does_not_refresh() {
    let mut dedup = KeepLastDeduplicator::new(vec![0], 2, true, false, true).with_state_ttl(1000);
    let out = dedup
        .push(&join_batch(vec![1], vec![10], vec![0]), 5000)
        .unwrap();
    assert_eq!(values(&out, 1), vec![10]); // first row emits (insert-only, no $row_kind$)
    let out = dedup
        .push(&join_batch(vec![1], vec![20], vec![0]), 5900)
        .unwrap();
    assert_eq!(out.num_rows(), 0); // duplicate dropped — and NOT a TTL refresh
                                   // 5000 + 1000 <= 6000: expired despite the 5900 duplicate — the key re-emits +I.
    let out = dedup
        .push(&join_batch(vec![1], vec![30], vec![0]), 6000)
        .unwrap();
    assert_eq!(values(&out, 1), vec![30]);
}

// TTL timestamps ride the snapshot as absolute millis: expiry after a restore is timed from
// the original write, not from the restore.
#[test]
fn dedup_ttl_timestamps_survive_snapshot_restore() {
    let mut dedup = proctime_keep_last().with_state_ttl(1000);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![0]), 5000)
        .unwrap();
    let snapshot = dedup.snapshot();
    let mut alive =
        KeepLastDeduplicator::restore(vec![0], vec![-1], 2, true, false, false, &snapshot, 5500)
            .with_state_ttl(1000);
    let out = alive
        .push(&join_batch(vec![1], vec![20], vec![0]), 5999)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // one ms inside the window — still alive
    let mut expired =
        KeepLastDeduplicator::restore(vec![0], vec![-1], 2, true, false, false, &snapshot, 5500)
            .with_state_ttl(1000);
    let out = expired
        .push(&join_batch(vec![1], vec![20], vec![0]), 6000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // ts 5000 + 1000 <= 6000 — fresh insert
    assert_eq!(values(&out, 1), vec![20]);
}

// A pre-TTL snapshot (no timestamp column) restored into a TTL'd deduplicator stamps every key
// with the restore time — a full retention from now, Flink's enable-TTL migration — instead of
// expiring everything on first touch.
#[test]
fn dedup_ttl_enable_migration_stamps_restore_time() {
    let mut dedup = proctime_keep_last();
    dedup
        .push(&join_batch(vec![1], vec![10], vec![0]), 0)
        .unwrap();
    let snapshot = dedup.snapshot(); // TTL off: no timestamp column
    let mut restored =
        KeepLastDeduplicator::restore(vec![0], vec![-1], 2, true, false, false, &snapshot, 5000)
            .with_state_ttl(1000);
    let out = restored
        .push(&join_batch(vec![1], vec![20], vec![0]), 5999)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // alive until restore + ttl
    let mut expired =
        KeepLastDeduplicator::restore(vec![0], vec![-1], 2, true, false, false, &snapshot, 5000)
            .with_state_ttl(1000);
    let out = expired
        .push(&join_batch(vec![1], vec![20], vec![0]), 6000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
}

// The periodic sweep reclaims keys that are never touched again, silently (expiry emits
// nothing).
#[test]
fn dedup_ttl_sweep_reclaims_idle_keys_silently() {
    let mut dedup = proctime_keep_last().with_state_ttl(1000);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![0]), 5000)
        .unwrap();
    dedup
        .push(&join_batch(vec![2], vec![20], vec![0]), 5000)
        .unwrap();
    // Touching only key 2 well past key 1's expiry triggers the once-per-period sweep; key 1's
    // row is gone from the snapshot without any -D or -U having been emitted.
    let out = dedup
        .push(&join_batch(vec![2], vec![1], vec![0]), 7000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // key 2 itself had expired too — fresh +I
    let snapshot = dedup.snapshot();
    // A TTL-off restore probes what survived: key 1 was swept (fresh +I), key 2 was rewritten.
    let mut probe =
        KeepLastDeduplicator::restore(vec![0], vec![-1], 2, true, false, false, &snapshot, 7000);
    let out = probe
        .push(&join_batch(vec![1], vec![99], vec![0]), 7100)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    let out = probe
        .push(&join_batch(vec![2], vec![99], vec![0]), 7100)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]);
    assert_eq!(values(&out, 1), vec![1, 99]);
}

// The mini-batch flush applies the same TTL rule: a bundle whose net transition leaves the row
// unchanged still emits -U/+U with retention on (Flink's mini-batch flush runs the same
// processLastRowOnProcTime gate).
#[test]
fn dedup_ttl_mini_batch_flush_emits_unchanged_transitions() {
    let mut dedup = proctime_keep_last()
        .with_state_ttl(3_600_000)
        .with_mini_batch(true);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![0]), 5000)
        .unwrap();
    dedup.flush_mini_batch().unwrap();
    dedup
        .push(&join_batch(vec![1], vec![10], vec![0]), 5001)
        .unwrap(); // net no-op bundle
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // -U(10)/+U(10), not suppressed
    assert_eq!(values(&out, 1), vec![10, 10]);
}

// A key that expires between the bundles stages a None preimage after the delete-on-read, so
// the flush emits the fresh +I Flink would.
#[test]
fn dedup_ttl_mini_batch_stages_no_preimage_for_an_expired_key() {
    let mut dedup = proctime_keep_last()
        .with_state_ttl(1000)
        .with_mini_batch(true);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![0]), 5000)
        .unwrap();
    dedup.flush_mini_batch().unwrap();
    // Key 9 opens the next bundle before key 1's expiry, so the sweep (skipped mid-bundle)
    // cannot reclaim key 1; its expiry is enforced by the delete-on-read probe, staging a None
    // preimage.
    dedup
        .push(&join_batch(vec![9], vec![90], vec![0]), 5500)
        .unwrap();
    dedup
        .push(&join_batch(vec![1], vec![20], vec![0]), 7000)
        .unwrap();
    let out = dedup.flush_mini_batch().unwrap();
    assert_eq!(values(&out, 0), vec![9, 1]);
    assert_eq!(values(&out, 1), vec![90, 20]);
    assert_eq!(row_kinds(&out), vec![0, 0]); // both fresh +I
}

// A watermark-buffered rowtime keep-first deduplicator over `[k, v, rt]` (key col 0, rt col 2).
fn rowtime_keep_first(ttl_ms: i64) -> KeepFirstDeduplicator {
    KeepFirstDeduplicator::new(vec![0], 2).with_state_ttl(ttl_ms)
}

// The late filter counts the rows it drops (rowtime strictly below the watermark — an exactly-
// at-watermark row is live), cumulatively across pushes: the host bridges the total into
// Flink's numLateRecordsDropped counter. Rows for an already-emitted key are ignored, not late.
#[test]
fn keep_first_counts_late_drops() {
    let mut dedup = rowtime_keep_first(0);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![1000]), 0)
        .unwrap();
    assert_eq!(dedup.late_drops, 0);
    let out = dedup.flush(2000, 0).unwrap();
    assert_eq!(values(&out, 1), vec![10]);
    // rt 1500 < wm 2000 is late; rt 2000 is not; key 1's live row is a non-late ignore.
    dedup
        .push(
            &join_batch(vec![2, 3, 1], vec![7, 8, 9], vec![1500, 2000, 2500]),
            0,
        )
        .unwrap();
    assert_eq!(dedup.late_drops, 1);
    dedup
        .push(&join_batch(vec![4], vec![6], vec![100]), 0)
        .unwrap();
    assert_eq!(dedup.late_drops, 2);
}

// The watermark-buffered keep-first TTLs only its fired markers (Flink's alreadyEmittedState,
// OnCreateAndWrite + NeverReturnExpired): an expired marker reads as absent, so the key buffers
// a new candidate and fires a second +I — append-only output, Flink accepts the duplicate
// insert.
#[test]
fn keep_first_ttl_expired_marker_lets_the_key_fire_again() {
    let mut dedup = rowtime_keep_first(1000);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![100]), 5000)
        .unwrap();
    let out = dedup.flush(100, 5000).unwrap();
    assert_eq!(values(&out, 1), vec![10]); // first fire — marker stamped at 5000
    dedup
        .push(&join_batch(vec![1], vec![20], vec![200]), 5500)
        .unwrap();
    assert_eq!(dedup.flush(200, 5500).unwrap().num_rows(), 0); // marker alive: row dropped
                                                               // 5000 + 1000 <= 6000: the marker expired — the row re-buffers and the key fires again.
    dedup
        .push(&join_batch(vec![1], vec![30], vec![300]), 6000)
        .unwrap();
    let out = dedup.flush(300, 6000).unwrap();
    assert_eq!(values(&out, 1), vec![30]);
}

// The marker is written ONCE, when the candidate fires (Flink's onTimer update); later rows for
// the emitted key are reads (`if alreadyEmitted return`) and never refresh it — a hot key still
// re-fires every retention period.
#[test]
fn keep_first_ttl_dropped_rows_do_not_refresh_the_marker() {
    let mut dedup = rowtime_keep_first(1000);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![100]), 1000)
        .unwrap();
    assert_eq!(values(&dedup.flush(100, 1000).unwrap(), 1), vec![10]);
    // Two probes deep into the retention: both dropped, neither a write.
    dedup
        .push(&join_batch(vec![1], vec![20], vec![200]), 1500)
        .unwrap();
    assert_eq!(dedup.flush(200, 1500).unwrap().num_rows(), 0);
    dedup
        .push(&join_batch(vec![1], vec![30], vec![300]), 1900)
        .unwrap();
    assert_eq!(dedup.flush(300, 1900).unwrap().num_rows(), 0);
    // Expiry stays timed from the fire at 1000 (1000 + 1000 <= 2000), not the 1900 probe.
    dedup
        .push(&join_batch(vec![1], vec![40], vec![400]), 2000)
        .unwrap();
    assert_eq!(values(&dedup.flush(400, 2000).unwrap(), 1), vec![40]);
}

// The pending candidate is deliberately exempt from TTL, mirroring Flink's un-TTL'd timer state:
// it is cleaned up by the watermark that fires it, and expiring it early would lose data. A
// candidate older than the whole retention still fires.
#[test]
fn keep_first_ttl_never_expires_a_pending_candidate() {
    let mut dedup = rowtime_keep_first(1000);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![1000]), 0)
        .unwrap();
    // 50 retention periods later another key's traffic runs the sweep; key 1's candidate (not a
    // marker) must survive it and the per-row probes.
    dedup
        .push(&join_batch(vec![2], vec![20], vec![400]), 50_000)
        .unwrap();
    let out = dedup.flush(500, 50_000).unwrap();
    assert_eq!(values(&out, 0), vec![2]);
    let out = dedup.flush(1000, 60_000).unwrap();
    assert_eq!(values(&out, 1), vec![10]); // the buffered candidate fires — no data loss
}

// Marker timestamps ride the snapshot as absolute millis: expiry after a restore is timed from
// the original fire, and the boundary stays inclusive (`ts + ttl <= now`).
#[test]
fn keep_first_ttl_marker_timestamps_survive_snapshot_restore() {
    let mut dedup = rowtime_keep_first(1000);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![100]), 5000)
        .unwrap();
    dedup.flush(100, 5000).unwrap(); // marker stamped at 5000
    let snapshot = dedup.snapshot();
    let mut alive =
        KeepFirstDeduplicator::restore(vec![0], 2, &snapshot, 5500).with_state_ttl(1000);
    alive
        .push(&join_batch(vec![1], vec![20], vec![200]), 5999)
        .unwrap();
    assert_eq!(alive.flush(200, 5999).unwrap().num_rows(), 0); // one ms inside the window
    let mut expired =
        KeepFirstDeduplicator::restore(vec![0], 2, &snapshot, 5500).with_state_ttl(1000);
    expired
        .push(&join_batch(vec![1], vec![20], vec![200]), 6000)
        .unwrap();
    assert_eq!(values(&expired.flush(200, 6000).unwrap(), 1), vec![20]); // 5000 + 1000 <= 6000
}

// A pre-TTL snapshot (no timestamp column) restored into a TTL'd deduplicator stamps every
// marker with the restore time — a full retention from now, Flink's enable-TTL migration —
// instead of expiring everything on first probe.
#[test]
fn keep_first_ttl_enable_migration_stamps_restore_time() {
    let mut dedup = rowtime_keep_first(0);
    dedup
        .push(&join_batch(vec![1], vec![10], vec![100]), 0)
        .unwrap();
    dedup.flush(100, 0).unwrap();
    let snapshot = dedup.snapshot(); // TTL off: no timestamp column
    let mut restored =
        KeepFirstDeduplicator::restore(vec![0], 2, &snapshot, 5000).with_state_ttl(1000);
    restored
        .push(&join_batch(vec![1], vec![20], vec![200]), 5999)
        .unwrap();
    assert_eq!(restored.flush(200, 5999).unwrap().num_rows(), 0); // alive until restore + ttl
    let mut expired =
        KeepFirstDeduplicator::restore(vec![0], 2, &snapshot, 5000).with_state_ttl(1000);
    expired
        .push(&join_batch(vec![1], vec![20], vec![200]), 6000)
        .unwrap();
    assert_eq!(values(&expired.flush(200, 6000).unwrap(), 1), vec![20]);
}

// The periodic sweep reclaims markers that are never probed again, silently (expiry emits
// nothing; only a later row for the key would make the re-fire visible).
#[test]
fn keep_first_ttl_sweep_reclaims_markers_silently() {
    let mut dedup = rowtime_keep_first(1000);
    dedup
        .push(&join_batch(vec![1, 2], vec![10, 20], vec![100, 100]), 1000)
        .unwrap();
    assert_eq!(dedup.flush(100, 1000).unwrap().num_rows(), 2); // both markers stamped at 1000
                                                               // Key 3's traffic well past the others' expiry triggers the once-per-period sweep; nothing
                                                               // is emitted for the swept keys.
    dedup
        .push(&join_batch(vec![3], vec![30], vec![200]), 3000)
        .unwrap();
    let out = dedup.flush(200, 3000).unwrap();
    assert_eq!(values(&out, 0), vec![3]);
    let snapshot = dedup.snapshot();
    // A TTL-off restore probes what survived: keys 1 and 2 were swept (they fire fresh), key 3's
    // marker remains (its row drops).
    let mut probe = KeepFirstDeduplicator::restore(vec![0], 2, &snapshot, 3000);
    probe
        .push(
            &join_batch(vec![1, 2, 3], vec![11, 21, 31], vec![300, 300, 300]),
            3100,
        )
        .unwrap();
    let out = probe.flush(300, 3100).unwrap();
    assert_eq!(values(&out, 0), vec![1, 2]);
    assert_eq!(values(&out, 1), vec![11, 21]);
}

#[test]
fn sort_buffer_over_budget_fails_and_flush_releases() {
    let mut sorter = TemporalSorter::new(2).with_memory_budget(1 << 20).unwrap();
    sorter
        .push(join_batch(vec![1, 2], vec![10, 20], vec![0, 1000]))
        .unwrap();
    assert!(sorter.memory.state_bytes > 0);
    sorter.flush(i64::MAX);
    assert_eq!(sorter.memory.state_bytes, 0); // everything emitted -> buffer released

    let mut tight = TemporalSorter::new(2).with_memory_budget(16).unwrap();
    let err = tight
        .push(join_batch(vec![1], vec![10], vec![0]))
        .unwrap_err();
    assert!(err.to_string().contains("task off-heap"), "{err}");
}

#[test]
fn interval_join_buffers_over_budget_fail_clearly() {
    let mut joiner = inner_interval_joiner(-1000, 1000)
        .with_memory_budget(16)
        .unwrap();
    let err = joiner
        .push_left(join_batch(vec![1], vec![10], vec![0]), None)
        .unwrap_err();
    assert!(err.to_string().contains("task off-heap"), "{err}");
}

// The hash join the operator delegates to DataFusion runs under the operator's pool, so its
// transient build side draws on the same budget as the buffered state: a buffer that fits can
// still fail at join time when the build side does not.
#[test]
fn join_working_memory_draws_on_the_operator_budget() {
    let n = 20_000usize;
    let keys: Vec<i64> = vec![1; n];
    let values: Vec<i64> = (0..n as i64).collect();
    let rts: Vec<i64> = vec![0; n];
    let big = join_batch(keys, values, rts);
    let budget = (big.get_array_memory_size() + (64 << 10)) as i64;

    let mut joiner = inner_interval_joiner(-1000, 1000)
        .with_memory_budget(budget)
        .unwrap();
    joiner.push_left(big, None).unwrap(); // buffers fit the budget
    let err = joiner
        .push_right(join_batch(vec![1], vec![100], vec![0]), None)
        .unwrap_err();
    assert!(err.to_string().contains("join working memory"), "{err}");
}

#[test]
fn local_group_state_over_budget_fails_and_flush_releases() {
    let mut agg = LocalGroupAggregator::new(vec![0], vec![0], vec![1], vec![], vec![0], vec![])
        .with_memory_budget(1 << 20)
        .unwrap();
    agg.update(&join_batch(vec![1, 2], vec![10, 20], vec![0, 0]))
        .unwrap();
    assert!(agg.memory.state_bytes > 0);
    agg.flush();
    assert_eq!(agg.memory.state_bytes, 0); // the mini-batch drained -> fully released

    let mut tight = LocalGroupAggregator::new(vec![0], vec![0], vec![1], vec![], vec![0], vec![])
        .with_memory_budget(64)
        .unwrap();
    let keys: Vec<i64> = (0..100).collect();
    let values: Vec<i64> = (0..100).collect();
    let err = tight
        .update(&join_batch(keys, values, vec![0; 100]))
        .unwrap_err();
    assert!(err.to_string().contains("task off-heap"), "{err}");
}

#[test]
fn updating_join_state_over_budget_fails_and_retract_releases() {
    let mut joiner = inner_joiner().with_memory_budget(1 << 20).unwrap();
    joiner
        .push(&changelog_join_batch(vec![1], vec![10], vec![0]), true, 0)
        .unwrap();
    assert!(joiner.memory.state_bytes > 0);
    joiner
        .push(&changelog_join_batch(vec![1], vec![10], vec![3]), true, 0)
        .unwrap();
    assert_eq!(joiner.memory.state_bytes, 0); // the only stored row retracted -> released

    let mut tight = inner_joiner().with_memory_budget(64).unwrap();
    let keys: Vec<i64> = (0..100).collect();
    let values: Vec<i64> = (0..100).collect();
    let err = tight
        .push(&changelog_join_batch(keys, values, vec![0; 100]), true, 0)
        .unwrap_err();
    assert!(err.to_string().contains("task off-heap"), "{err}");
}

#[test]
fn topn_buffer_stays_within_budget_under_eviction() {
    // A bounded Top-3 keeps its reservation bounded no matter how many rows stream through.
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 3, false, false)
        .with_memory_budget(1 << 20)
        .unwrap();
    for i in 0..50 {
        ranker.push(&topn_batch(vec![1], vec![i]), 0).unwrap();
    }
    let bounded = ranker.memory.state_bytes;
    for i in 50..100 {
        ranker.push(&topn_batch(vec![1], vec![i]), 0).unwrap();
    }
    assert_eq!(ranker.memory.state_bytes, bounded); // eviction keeps the tracked state flat
}

#[test]
fn topn_net_diff_staging_is_accounted_and_released_on_flush() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 3, false, true)
        .with_memory_budget(1 << 20)
        .unwrap();
    ranker
        .push(&topn_batch(vec![1, 1, 2], vec![5, 3, 7]), 0)
        .unwrap();
    assert_eq!(ranker.staged_partitions(), 2);
    assert!(ranker.staging_bytes() > 0);
    let bundled = ranker.memory.state_bytes;

    ranker.flush_net_diff();
    assert_eq!(ranker.staged_partitions(), 0);
    assert_eq!(ranker.staging_bytes(), 0);
    assert!(ranker.memory.state_bytes < bundled);
}

// A restored snapshot is accounted the moment the budget attaches, so state that no longer fits
// fails at restore rather than silently exceeding the budget.
#[test]
fn restored_state_is_accounted_against_budget() {
    let mut agg = TumblingAggregator::new(1000, 1000, false, vec![0], vec![0]);
    agg.update(&keyed_window_batch(0, (0..100).collect()))
        .unwrap();
    let snapshot = agg.snapshot();
    let restored = TumblingAggregator::restore(1000, 1000, false, vec![0], vec![0], &snapshot);
    assert!(restored.with_memory_budget(256).is_err());

    let restored = TumblingAggregator::restore(1000, 1000, false, vec![0], vec![0], &snapshot);
    let fits = restored.with_memory_budget(1 << 20).unwrap();
    assert_eq!(fits.memory.state_bytes, fits.computed_state_bytes());
    assert!(fits.memory.state_bytes > 0);
}

// A `[key0, value0, $row_kind$]` changelog batch (key/value bigint) for the GROUP BY tests;
// `kinds` is the RowKind byte per row (0 +I, 1 -U, 2 +U, 3 -D).
fn group_changelog(keys: Vec<i64>, values: Vec<Option<i64>>, kinds: Vec<i8>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Int64, false),
            Field::new("value0", DataType::Int64, true),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap()
}

// All-INSERT convenience for the append-only tests.
fn group_batch(keys: Vec<i64>, values: Vec<i64>) -> RecordBatch {
    let kinds = vec![0i8; keys.len()];
    group_changelog(keys, values.into_iter().map(Some).collect(), kinds)
}

#[test]
fn local_group_extremes_preserve_append_only_and_retracting_results() {
    let make =
        || LocalGroupAggregator::new(vec![1, 2], vec![0, 0], vec![1, 1], vec![], vec![0], vec![]);

    let mut append_only = make();
    append_only
        .update(&join_batch(vec![1, 1, 2], vec![10, 5, 7], vec![0, 0, 0]))
        .unwrap();
    let out = append_only.flush();
    assert_eq!(values(&out, 1), vec![5, 7]);
    assert_eq!(values(&out, 2), vec![10, 7]);

    let mut retracting = make();
    retracting
        .update(&group_changelog(
            vec![1, 1, 1],
            vec![Some(10), Some(5), Some(5)],
            vec![0, 0, 3],
        ))
        .unwrap();
    let out = retracting.flush();
    assert_eq!(values(&out, 1), vec![10]);
    assert_eq!(values(&out, 2), vec![10]);
}

fn row_kinds(batch: &RecordBatch) -> Vec<i8> {
    batch
        .column_by_name(ROW_KIND_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap()
        .values()
        .to_vec()
}

// GROUP BY changelog: a key's first row emits INSERT(0); a later row that changes the result
// emits UPDATE_BEFORE(1)+UPDATE_AFTER(2); a row that leaves the result unchanged emits nothing.
#[test]
fn group_by_emits_insert_then_update_changelog() {
    // SUM(bigint) over value column 1, grouping on key column 0, emitting -U.
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
    // keys a,a,b,a with values 1,2,5,0 — the last adds 0, leaving a's sum at 3 (suppressed).
    let out = agg
        .update(&group_batch(vec![1, 1, 2, 1], vec![1, 2, 5, 0]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2, 0]);
    assert_eq!(values(&out, 0), vec![1, 1, 1, 2]); // key
    assert_eq!(values(&out, 1), vec![1, 1, 3, 5]); // running sum (prev on -U, new on +U)
}

// COUNT(*) (no argument column) counts every row, alongside a SUM over a value column.
#[test]
fn group_by_counts_every_row_for_count_star() {
    // kinds COUNT(*), SUM; COUNT(*) has no column (-1), SUM reads column 1; group on column 0.
    let mut agg = GroupAggregator::new(vec![3, 0], vec![0, 0], vec![-1, 1], vec![0], true);
    let out = agg
        .update(&group_batch(vec![1, 1], vec![10, 5]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2]); // +I, then -U/+U
    assert_eq!(values(&out, 1), vec![1, 1, 2]); // COUNT(*): 1, then 1->2
    assert_eq!(values(&out, 2), vec![10, 10, 15]); // SUM: 10, then 10->15
}

// AVG(bigint) keeps a running sum + non-null count and emits sum/count with integer division
// truncating toward zero (Flink's AvgAggFunction), retracting the prior average on each change.
#[test]
fn group_by_avg_truncates_toward_zero() {
    let mut agg = GroupAggregator::new(vec![4], vec![0], vec![1], vec![0], true);
    // One key, values 10 then 1 → avg 10, then 11/2 = 5 (truncated from 5.5, not rounded).
    let out = agg
        .update(&group_batch(vec![1, 1], vec![10, 1]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2]); // +I, then -U/+U
    assert_eq!(values(&out, 1), vec![10, 10, 5]);
}

// COUNT(*) FILTER (WHERE flag): a row folds into the aggregate only where its filter boolean is
// TRUE — FALSE and NULL are skipped, matching SQL FILTER.
#[test]
fn group_by_filter_gates_each_aggregate() {
    let key: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1]));
    let flag: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Int64, false),
            Field::new("flag", DataType::Boolean, true),
        ])),
        vec![key, flag],
    )
    .unwrap();
    // COUNT(*) over a boolean filter in column 1; group on column 0.
    let mut agg = GroupAggregator::new(vec![3], vec![0], vec![-1], vec![0], true)
        .with_filter_columns(vec![1]);
    let out = agg.update(&batch, 0).unwrap();
    // Only the TRUE row counts → +I count=1; the FALSE/NULL rows leave it unchanged (suppressed).
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![1]);
}

// MIN/MAX over a string column: the Extremes multiset orders entries byte-lexicographically
// (Rust String Ord), retracting the prior extreme as it changes.
#[test]
fn group_by_min_max_string() {
    let key: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1]));
    let s: ArrayRef = Arc::new(StringArray::from(vec![
        Some("banana"),
        Some("apple"),
        Some("cherry"),
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
        ])),
        vec![key, s],
    )
    .unwrap();
    // MIN, MAX over the string column 1; group on column 0; value type code 3 (Utf8).
    let mut agg = GroupAggregator::new(vec![1, 2], vec![3, 3], vec![1, 1], vec![0], true);
    let out = agg.update(&batch, 0).unwrap();
    let last = out.num_rows() - 1;
    let min = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let max = out
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(min.value(last), "apple");
    assert_eq!(max.value(last), "cherry");
}

// A columnar input from an insert-only producer has no `$row_kind$` column; every row is then an
// INSERT (so the GROUP BY still emits its +I / -U / +U changelog).
#[test]
fn group_by_treats_absent_row_kind_as_insert() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Int64, false),
            Field::new("value0", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 1])),
            Arc::new(Int64Array::from(vec![10i64, 20])),
        ],
    )
    .unwrap();
    let out = agg.update(&batch, 0).unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2]); // +I(10); -U(10)/+U(30)
    assert_eq!(values(&out, 1), vec![10, 10, 30]);
}

#[test]
fn group_by_mini_batch_emits_one_final_change_across_physical_batches() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true).with_mini_batch();
    let first = agg
        .update(&group_batch(vec![1, 2], vec![10, 7]), 0)
        .unwrap();
    let second = agg.update(&group_batch(vec![1, 1], vec![5, 2]), 0).unwrap();
    assert_eq!(first.num_rows(), 0);
    assert_eq!(second.num_rows(), 0);

    let out = agg.flush_mini_batch().unwrap();
    assert_eq!(row_kinds(&out), vec![0, 0]);
    assert_eq!(values(&out, 0), vec![1, 2]);
    assert_eq!(values(&out, 1), vec![17, 7]);
}

#[test]
fn group_by_mini_batch_preserves_first_preimage_and_suppresses_cancelled_groups() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true).with_mini_batch();
    agg.update(&group_batch(vec![1], vec![10]), 0).unwrap();
    agg.flush_mini_batch().unwrap();

    agg.update(
        &group_changelog(
            vec![1, 1, 2, 2],
            vec![Some(5), Some(2), Some(9), Some(9)],
            vec![0, 0, 0, 3],
        ),
        0,
    )
    .unwrap();
    let out = agg.flush_mini_batch().unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]);
    assert_eq!(values(&out, 0), vec![1, 1]);
    assert_eq!(values(&out, 1), vec![10, 17]);
}

// With the host's update-before flag off, an update emits only the UPDATE_AFTER row.
#[test]
fn group_by_omits_update_before_when_disabled() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], false);
    let out = agg
        .update(&group_batch(vec![1, 1], vec![10, 5]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0, 2]); // +I(10), +U(15)
    assert_eq!(values(&out, 1), vec![10, 15]);
}

// A checkpoint preserves per-key state: a restored key is not "first", so a new row updates
// rather than re-inserting.
#[test]
fn group_by_survives_snapshot_restore() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
    agg.update(&group_batch(vec![1], vec![10]), 0);
    let snapshot = agg.snapshot();
    let mut restored =
        GroupAggregator::restore(vec![0], vec![0], vec![1], vec![0], true, &snapshot, 0);
    let out = restored.update(&group_batch(vec![1], vec![5]), 0).unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // -U(10), +U(15) — continues from 10
    assert_eq!(values(&out, 1), vec![10, 15]);
}

// State TTL: an idle group expires ttl millis after its last write; the next add is a fresh +I
// with a restarted accumulator (Flink's NeverReturnExpired: expired reads as absent).
#[test]
fn group_by_ttl_expires_an_idle_key_into_a_fresh_insert() {
    let mut agg =
        GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true).with_state_ttl(1000);
    let out = agg.update(&group_batch(vec![1], vec![10]), 5000).unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // +I 10
                                          // ts 5000 + ttl 1000 <= 6000: expired exactly at the boundary — the sum restarts at 5.
    let out = agg.update(&group_batch(vec![1], vec![5]), 6000).unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // +I, not -U/+U
    assert_eq!(values(&out, 1), vec![5]);
}

// A write refreshes the TTL (OnCreateAndWrite): steadily-touched keys never expire, and expiry is
// timed from the LAST write.
#[test]
fn group_by_ttl_refreshes_on_every_write() {
    let mut agg =
        GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true).with_state_ttl(1000);
    agg.update(&group_batch(vec![1], vec![10]), 5000).unwrap();
    let out = agg.update(&group_batch(vec![1], vec![5]), 5900).unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // alive: -U(10)/+U(15)
                                             // 900ms later the original write is long past ttl, but the refresh at 5900 keeps it alive.
    let out = agg.update(&group_batch(vec![1], vec![1]), 6800).unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]);
    assert_eq!(values(&out, 1), vec![15, 16]);
}

// A retraction reaching an expired (absent) group emits nothing and creates no state — Flink
// drops retractions with no accumulator.
#[test]
fn group_by_ttl_drops_a_retraction_against_an_expired_key() {
    let mut agg =
        GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true).with_state_ttl(1000);
    agg.update(&group_batch(vec![1], vec![10]), 5000).unwrap();
    let out = agg
        .update(&group_changelog(vec![1], vec![Some(10)], vec![1]), 7000)
        .unwrap();
    assert_eq!(out.num_rows(), 0);
}

// With TTL on, the unchanged-result suppression is disabled: Flink always emits -U/+U so
// downstream TTL state keeps refreshing (the deterministic, parity-testable TTL behavior).
#[test]
fn group_by_ttl_emits_the_unchanged_update_it_would_otherwise_suppress() {
    let mut agg =
        GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true).with_state_ttl(3_600_000);
    agg.update(&group_batch(vec![1], vec![10]), 5000).unwrap();
    // Adding 0 leaves the sum at 10 — suppressed without TTL (see the changelog test above).
    let out = agg.update(&group_batch(vec![1], vec![0]), 5001).unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // -U(10)/+U(10)
    assert_eq!(values(&out, 1), vec![10, 10]);
}

// TTL timestamps ride the snapshot as absolute millis: expiry after a restore is timed from the
// original write, not from the restore.
#[test]
fn group_by_ttl_timestamps_survive_snapshot_restore() {
    let mut agg =
        GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true).with_state_ttl(1000);
    agg.update(&group_batch(vec![1], vec![10]), 5000).unwrap();
    let snapshot = agg.snapshot();
    let mut alive =
        GroupAggregator::restore(vec![0], vec![0], vec![1], vec![0], true, &snapshot, 5500)
            .with_state_ttl(1000);
    let out = alive.update(&group_batch(vec![1], vec![5]), 5999).unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // one ms inside the window — still alive
    let mut expired =
        GroupAggregator::restore(vec![0], vec![0], vec![1], vec![0], true, &snapshot, 5500)
            .with_state_ttl(1000);
    let out = expired
        .update(&group_batch(vec![1], vec![5]), 6000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // ts 5000 + 1000 <= 6000 — fresh insert
    assert_eq!(values(&out, 1), vec![5]);
}

// A pre-TTL snapshot (no timestamp column) restored into a TTL'd aggregator stamps every group
// with the restore time — a full retention from now, Flink's enable-TTL migration — instead of
// expiring everything on first touch.
#[test]
fn group_by_ttl_enable_migration_stamps_restore_time() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
    agg.update(&group_batch(vec![1], vec![10]), 0).unwrap();
    let snapshot = agg.snapshot(); // TTL off: no timestamp column
    let mut restored =
        GroupAggregator::restore(vec![0], vec![0], vec![1], vec![0], true, &snapshot, 5000)
            .with_state_ttl(1000);
    let out = restored
        .update(&group_batch(vec![1], vec![5]), 5999)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // alive until restore + ttl
    assert_eq!(values(&out, 1), vec![10, 15]);
}

// The periodic sweep reclaims keys that are never touched again, silently (expiry emits nothing).
#[test]
fn group_by_ttl_sweep_reclaims_idle_keys_silently() {
    let mut agg =
        GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true).with_state_ttl(1000);
    agg.update(&group_batch(vec![1], vec![10]), 5000).unwrap();
    agg.update(&group_batch(vec![2], vec![20]), 5000).unwrap();
    // Touching only key 2 well past key 1's expiry triggers the once-per-period sweep; key 1's
    // state is gone from the snapshot without any -D having been emitted.
    let out = agg.update(&group_batch(vec![2], vec![1]), 7000).unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // key 2 itself had expired too — fresh +I
    let snapshot = agg.snapshot();
    let mut probe =
        GroupAggregator::restore(vec![0], vec![0], vec![1], vec![0], true, &snapshot, 7000)
            .with_state_ttl(1000);
    // Key 1 was swept: a retraction for it finds nothing and emits nothing.
    let out = probe
        .update(&group_changelog(vec![1], vec![Some(10)], vec![1]), 7100)
        .unwrap();
    assert_eq!(out.num_rows(), 0);
}

// The mini-batch flush applies the same TTL rule: a bundle whose net transition is a no-op still
// emits -U/+U with retention on (Flink's MiniBatchGroupAggFunction gate).
#[test]
fn group_by_ttl_mini_batch_flush_emits_unchanged_transitions() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true)
        .with_state_ttl(3_600_000)
        .with_mini_batch();
    agg.update(&group_batch(vec![1], vec![10]), 5000).unwrap();
    agg.flush_mini_batch().unwrap();
    agg.update(&group_batch(vec![1], vec![0]), 5001).unwrap(); // net no-op bundle
    let out = agg.flush_mini_batch().unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // -U(10)/+U(10), not suppressed
    assert_eq!(values(&out, 1), vec![10, 10]);
}

// Consuming a changelog: a -U input retracts a prior value, updating the running SUM.
#[test]
fn group_by_retracts_changelog_input() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
    // +I 10, +I 20 (sum 30), then -U 10 (retract -> sum 20), all key 1.
    let out = agg
        .update(
            &group_changelog(
                vec![1, 1, 1],
                vec![Some(10), Some(20), Some(10)],
                vec![0, 0, 1],
            ),
            0,
        )
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2, 1, 2]);
    assert_eq!(values(&out, 1), vec![10, 10, 30, 30, 20]); // +I10; -U10/+U30; -U30/+U20
}

// Retracting a key's last record empties the group and emits a DELETE.
#[test]
fn group_by_deletes_when_last_record_retracted() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
    let out = agg
        .update(
            &group_changelog(vec![1, 1], vec![Some(10), Some(10)], vec![0, 3]),
            0,
        )
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0, 3]); // +I(10), then -D(10)
    assert_eq!(values(&out, 1), vec![10, 10]);
}

// A SUM reports NULL once its last non-null value is retracted while a null-valued row keeps the
// group alive — matching the host's sum-with-retract.
#[test]
fn group_by_sum_is_null_after_last_value_retracted() {
    let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
    // +I 5, +I NULL (sum still 5, suppressed), -U 5 (no non-null left -> SUM NULL, group alive).
    let out = agg
        .update(
            &group_changelog(vec![1, 1, 1], vec![Some(5), None, Some(5)], vec![0, 0, 1]),
            0,
        )
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2]); // +I(5); -U(5)/+U(NULL)
    let result = out.column(1);
    assert_eq!(result.len(), 3);
    assert!(
        !result.is_null(0)
            && result
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
                == 5
    );
    assert!(result.is_null(2)); // the +U carries a NULL sum
}

// MIN over a changelog: retracting the current minimum reveals the next-smallest from the
// per-key value multiset (what a single running value could not do).
#[test]
fn group_by_min_recovers_next_after_retract() {
    // kind MIN (1) over value column 1, group on column 0, emit -U.
    let mut agg = GroupAggregator::new(vec![1], vec![0], vec![1], vec![0], true);
    // +I 5, +I 3, +I 8 (min 3), then -U 3 (min back to 5).
    let out = agg
        .update(
            &group_changelog(
                vec![1, 1, 1, 1],
                vec![Some(5), Some(3), Some(8), Some(3)],
                vec![0, 0, 0, 1],
            ),
            0,
        )
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0, 1, 2, 1, 2]);
    // min: 5; 5->3; (8 leaves min 3, suppressed); 3->5 after retracting the 3.
    assert_eq!(values(&out, 1), vec![5, 5, 3, 3, 5]);
}

// The MIN/MAX value multiset survives a checkpoint, so a post-restore retract still recovers the
// next extreme.
#[test]
fn group_by_min_multiset_survives_snapshot_restore() {
    let mut agg = GroupAggregator::new(vec![1], vec![0], vec![1], vec![0], true);
    agg.update(
        &group_changelog(vec![1, 1], vec![Some(5), Some(3)], vec![0, 0]),
        0,
    ); // min 3
    let snapshot = agg.snapshot();
    let mut restored =
        GroupAggregator::restore(vec![1], vec![0], vec![1], vec![0], true, &snapshot, 0);
    // Retract the 3 — the restored multiset still holds the 5, so the min becomes 5.
    let out = restored
        .update(&group_changelog(vec![1], vec![Some(3)], vec![1]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]); // -U(3), +U(5)
    assert_eq!(values(&out, 1), vec![3, 5]);
}

// A `[p, s, $row_kind$]` insert-only batch (partition p at col 0, sort key s at col 1) for the
// Top-N tests.
fn topn_batch(p: Vec<i64>, s: Vec<i64>) -> RecordBatch {
    let kinds = vec![0i8; p.len()];
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("p", DataType::Int64, false),
            Field::new("s", DataType::Int64, true),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(p)),
            Arc::new(Int64Array::from(s)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap()
}

fn asc(index: usize) -> SortColumn {
    SortColumn {
        index,
        ascending: true,
        nulls_first: false,
    }
}

// Top-2 by ascending sort key, one partition: a row entering the top-2 inserts and displaces the
// current 2nd (a DELETE); a row that would rank 3rd emits nothing.
#[test]
fn topn_keeps_smallest_n_per_partition() {
    // partition col 0, ORDER BY col 1 ASC, limit 2.
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, false, false);
    // s = 5, 3, 8, 1 for partition 1.
    let out = ranker
        .push(&topn_batch(vec![1, 1, 1, 1], vec![5, 3, 8, 1]), 0)
        .unwrap();
    // 5: +I5. 3: +I3 (top2 = {3,5}). 8: rank 3 -> nothing. 1: +I1, -D5 (top2 = {1,3}).
    assert_eq!(row_kinds(&out), vec![0, 0, 3, 0]);
    assert_eq!(values(&out, 1), vec![5, 3, 5, 1]); // the sort-key column of each emitted row
}

// Top-2 with the rank number projected: a row entering shifts the rows below it, emitting the
// UPDATE_BEFORE/UPDATE_AFTER cascade Flink does, and an INSERT for a brand-new rank.
#[test]
fn topn_with_rank_number_emits_cascade() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, true, false);
    let out = ranker
        .push(&topn_batch(vec![1, 1, 1, 1], vec![5, 3, 8, 1]), 0)
        .unwrap();
    // 5: +I(5,1). 3: -U(5,1) +U(3,1) +I(5,2). 8: rank 3 -> nothing.
    // 1: -U(3,1) +U(1,1) -U(5,2) +U(3,2)  [5 pushed past rank 2, retracted by the -U].
    assert_eq!(row_kinds(&out), vec![0, 1, 2, 0, 1, 2, 1, 2]);
    assert_eq!(values(&out, 1), vec![5, 5, 3, 5, 3, 1, 5, 3]); // sort-key column
    assert_eq!(values(&out, 2), vec![1, 1, 1, 2, 1, 1, 2, 2]); // appended rank (w0$o0)
}

// Partitions are independent: each keeps its own top-N.
#[test]
fn topn_is_per_partition() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 1, false, false);
    let out = ranker
        .push(&topn_batch(vec![1, 2, 1], vec![5, 7, 3]), 0)
        .unwrap();
    // p1: +I5; p2: +I7; p1 sees 3 < 5 -> -D5 then +I3 (delete first, as the host emits).
    assert_eq!(row_kinds(&out), vec![0, 0, 3, 0]);
    assert_eq!(values(&out, 0), vec![1, 2, 1, 1]); // partition of each emitted row
    assert_eq!(values(&out, 1), vec![5, 7, 5, 3]);
}

// Net-diff (mini-batch) mode collapses the same batch to the per-partition net change: the same
// four rows that cascade eight changelog entries above emit only the final top-2 state diff.
#[test]
fn topn_net_diff_emits_batch_delta_with_rank() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, true, true);
    let pending = ranker.push(&topn_batch(vec![1, 1], vec![5, 3]), 0).unwrap();
    assert_eq!(pending.num_rows(), 0);
    let pending = ranker.push(&topn_batch(vec![1, 1], vec![8, 1]), 0).unwrap();
    assert_eq!(pending.num_rows(), 0);
    let out = ranker.flush_net_diff();
    // Fresh partition: old top empty, new top = {1@rank1, 3@rank2} — two inserts, no cascade.
    assert_eq!(row_kinds(&out), vec![0, 0]);
    assert_eq!(values(&out, 1), vec![1, 3]);
    assert_eq!(values(&out, 2), vec![1, 2]);

    // Second batch: 2 enters at rank 2 (1 stays at rank 1) — one -U/+U pair, rank 1 untouched.
    ranker.push(&topn_batch(vec![1], vec![2]), 0).unwrap();
    let out = ranker.flush_net_diff();
    assert_eq!(row_kinds(&out), vec![1, 2]);
    assert_eq!(values(&out, 1), vec![3, 2]);
    assert_eq!(values(&out, 2), vec![2, 2]);
}

// Net-diff without the rank number: the diff is top-N membership — leavers delete, entrants insert.
#[test]
fn topn_net_diff_emits_membership_delta() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, false, true);
    ranker.push(&topn_batch(vec![1, 1], vec![5, 3]), 0).unwrap();
    ranker.push(&topn_batch(vec![1, 1], vec![8, 1]), 0).unwrap();
    let out = ranker.flush_net_diff();
    // New partition: final top-2 = {1, 3}; the transient 5 never surfaces.
    assert_eq!(row_kinds(&out), vec![0, 0]);
    assert_eq!(values(&out, 1), vec![1, 3]);

    // 2 displaces 3: one -D and one +I; a batch that changes nothing emits nothing.
    ranker.push(&topn_batch(vec![1], vec![2]), 0).unwrap();
    let out = ranker.flush_net_diff();
    assert_eq!(row_kinds(&out), vec![3, 0]);
    assert_eq!(values(&out, 1), vec![3, 2]);
    ranker.push(&topn_batch(vec![1], vec![9]), 0).unwrap();
    let out = ranker.flush_net_diff();
    assert_eq!(out.num_rows(), 0);
}

// The bounded buffer survives a checkpoint, so post-restore ranking continues correctly.
#[test]
fn topn_buffer_survives_snapshot_restore() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, false, false);
    ranker.push(&topn_batch(vec![1, 1], vec![5, 3]), 0); // top2 = {3, 5}
    let snapshot = ranker.snapshot();
    let mut restored = TopNRanker::restore(
        vec![0],
        vec![-1],
        vec![asc(1)],
        2,
        false,
        false,
        &snapshot,
        0,
    );
    // A 1 enters the restored top-2 and displaces the 5.
    let out = restored.push(&topn_batch(vec![1], vec![1]), 0).unwrap();
    assert_eq!(row_kinds(&out), vec![3, 0]); // -D5, +I1
    assert_eq!(values(&out, 1), vec![5, 1]);
}

// A `[p, s, $row_kind$]` changelog batch for the retracting Top-N TTL tests.
fn topn_changelog(p: Vec<i64>, s: Vec<i64>, kinds: Vec<i8>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("p", DataType::Int64, false),
            Field::new("s", DataType::Int64, true),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(p)),
            Arc::new(Int64Array::from(s)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap()
}

// State TTL: append-only Top-N expiry is per sort-key list, silent — no -D for an expired row
// (downstream only ever saw +I's), and subsequent output ranks against the survivors.
#[test]
fn topn_ttl_expired_rows_vanish_silently_and_rank_against_survivors() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, false, false).with_state_ttl(1000);
    let out = ranker
        .push(&topn_batch(vec![1, 1], vec![5, 3]), 5000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0, 0]);
    // ts 5000 + ttl 1000 <= 6000: both rows expired — the prune emits nothing and the new row is
    // a fresh +I against an empty buffer.
    let out = ranker.push(&topn_batch(vec![1], vec![8]), 6000).unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![8]);
    // Ranking continues against the survivor: 9 takes rank 2, then 7 displaces it.
    let out = ranker
        .push(&topn_batch(vec![1, 1], vec![9, 7]), 6000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0, 3, 0]);
    assert_eq!(values(&out, 1), vec![9, 9, 7]);
}

// With the rank number projected, the prune runs before the cascade positions are read: an
// expired top-2 admits the new row at rank 1 with a single +I, no -U/+U against expired rows.
#[test]
fn topn_ttl_prunes_before_the_rank_cascade() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, true, false).with_state_ttl(1000);
    ranker
        .push(&topn_batch(vec![1, 1], vec![5, 3]), 5000)
        .unwrap();
    let out = ranker.push(&topn_batch(vec![1], vec![4]), 6000).unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 2), vec![1]); // the appended rank column: fresh rank 1
}

// Byte-equal sort keys are one Flink list, rewritten whole on every insert: one tie member's
// arrival keeps the earlier members alive.
#[test]
fn topn_ttl_tie_insert_refreshes_the_whole_sort_key_list() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, false, false).with_state_ttl(1000);
    ranker.push(&topn_batch(vec![1], vec![5]), 5000).unwrap();
    ranker.push(&topn_batch(vec![1], vec![5]), 5600).unwrap();
    // At 6300 the first 5 is alive only through the tie refresh at 5600: both survive, so the 1
    // displaces the second 5 rather than sliding into a half-empty buffer.
    let out = ranker.push(&topn_batch(vec![1], vec![1]), 6300).unwrap();
    assert_eq!(row_kinds(&out), vec![3, 0]);
    assert_eq!(values(&out, 1), vec![5, 1]);
}

// Evicting one member of the last sort-key list writes the trimmed list back (Flink's
// updateState rewrite), refreshing the remaining members.
#[test]
fn topn_ttl_eviction_rewrite_refreshes_the_trimmed_sort_key_list() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, false, false).with_state_ttl(1000);
    ranker
        .push(&topn_batch(vec![1, 1], vec![9, 9]), 5000)
        .unwrap();
    // The 1 evicts the second 9; the trimmed {9} list is rewritten, refreshed to 5600.
    let out = ranker.push(&topn_batch(vec![1], vec![1]), 5600).unwrap();
    assert_eq!(row_kinds(&out), vec![3, 0]);
    // At 6300 the surviving 9 (written 5000) is alive only through that rewrite: the 5 displaces
    // it instead of filling a pruned buffer.
    let out = ranker.push(&topn_batch(vec![1], vec![5]), 6300).unwrap();
    assert_eq!(row_kinds(&out), vec![3, 0]);
    assert_eq!(values(&out, 1), vec![9, 5]);
}

// Timestamps are absolute and ride the snapshot: expiry after a restore is timed from the
// original write, inclusively at write + ttl (Flink's `ts + ttl <= now`).
#[test]
fn topn_ttl_timestamps_survive_snapshot_restore() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 1, false, false).with_state_ttl(1000);
    ranker.push(&topn_batch(vec![1], vec![5]), 5000).unwrap();
    let snapshot = ranker.snapshot();
    // One ms inside the window: the buffered 5 is alive, so the worse 7 never enters.
    let mut alive = TopNRanker::restore(
        vec![0],
        vec![-1],
        vec![asc(1)],
        1,
        false,
        false,
        &snapshot,
        5500,
    )
    .with_state_ttl(1000);
    assert_eq!(
        alive
            .push(&topn_batch(vec![1], vec![7]), 5999)
            .unwrap()
            .num_rows(),
        0
    );
    // Expired exactly at the boundary — the strictly worse row becomes a fresh top-1.
    let mut expired = TopNRanker::restore(
        vec![0],
        vec![-1],
        vec![asc(1)],
        1,
        false,
        false,
        &snapshot,
        5500,
    )
    .with_state_ttl(1000);
    let out = expired.push(&topn_batch(vec![1], vec![7]), 6000).unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![7]);
}

// A pre-TTL snapshot (no timestamp column) restored into a TTL'd ranker stamps every row with
// the restore time — a full retention from now, Flink's enable-TTL migration.
#[test]
fn topn_ttl_enable_migration_stamps_restore_time() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 1, false, false);
    ranker.push(&topn_batch(vec![1], vec![5]), 0).unwrap();
    let snapshot = ranker.snapshot(); // TTL off: no timestamp column
    let mut restored = TopNRanker::restore(
        vec![0],
        vec![-1],
        vec![asc(1)],
        1,
        false,
        false,
        &snapshot,
        5000,
    )
    .with_state_ttl(1000);
    assert_eq!(
        restored
            .push(&topn_batch(vec![1], vec![7]), 5999)
            .unwrap()
            .num_rows(),
        0
    );
    let mut expired = TopNRanker::restore(
        vec![0],
        vec![-1],
        vec![asc(1)],
        1,
        false,
        false,
        &snapshot,
        5000,
    )
    .with_state_ttl(1000);
    let out = expired.push(&topn_batch(vec![1], vec![7]), 6000).unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
}

// The periodic sweep reclaims partitions that are never touched again, silently.
#[test]
fn topn_ttl_sweep_reclaims_idle_partitions_silently() {
    let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 1, false, false).with_state_ttl(1000);
    ranker.push(&topn_batch(vec![1], vec![10]), 5000).unwrap();
    ranker.push(&topn_batch(vec![2], vec![20]), 5000).unwrap();
    // Touching only partition 2 well past both expiries triggers the once-per-period sweep;
    // partition 1's row is gone from the snapshot without anything having been emitted.
    let out = ranker.push(&topn_batch(vec![2], vec![21]), 7000).unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // partition 2's own row had expired too — fresh +I
    let snapshot = ranker.snapshot();
    // A TTL-off restore probes what survived: partition 1 was swept, so the worse 50 becomes a
    // fresh top-1 instead of being dropped against the old 10.
    let mut probe = TopNRanker::restore(
        vec![0],
        vec![-1],
        vec![asc(1)],
        1,
        false,
        false,
        &snapshot,
        7000,
    );
    let out = probe.push(&topn_batch(vec![1], vec![50]), 7000).unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![50]);
}

// Retracting Top-N models Flink's every-record treemap write as a whole-buffer clock on the head
// entry: an idle partition expires as one unit, a stale retraction then finds nothing and emits
// nothing (Flink's lenient skip), and the next accumulate re-seeds through the normal diff.
#[test]
fn retracting_topn_ttl_expires_the_whole_buffer_and_drops_stale_retractions() {
    let mut ranker =
        RetractableTopNRanker::new(vec![0], vec![asc(1)], 0, 2, false).with_state_ttl(1000);
    let out = ranker
        .push(
            &topn_changelog(vec![1, 1, 1], vec![10, 20, 30], vec![0, 0, 0]),
            5000,
        )
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0, 0]); // +I10, +I20 (30 is rank 3)
                                             // At 6000 the buffer expired whole; the retraction of 10 hits a cleared buffer — silence.
    let out = ranker
        .push(&topn_changelog(vec![1], vec![10], vec![3]), 6000)
        .unwrap();
    assert_eq!(out.num_rows(), 0);
    // An accumulate re-seeds a fresh buffer: one +I, nothing about the expired rows.
    let out = ranker
        .push(&topn_changelog(vec![1], vec![15], vec![0]), 6000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![15]);
}

// A retraction is a state write too (Flink rewrites the treemap on every record), so a
// retract-only stretch keeps the buffer alive.
#[test]
fn retracting_topn_ttl_retraction_refreshes_the_buffer_clock() {
    let mut ranker =
        RetractableTopNRanker::new(vec![0], vec![asc(1)], 0, 2, false).with_state_ttl(1000);
    ranker
        .push(
            &topn_changelog(vec![1, 1, 1], vec![10, 20, 30], vec![0, 0, 0]),
            5000,
        )
        .unwrap();
    // Retracting rank-3 changes no output but refreshes the whole buffer's clock.
    let out = ranker
        .push(&topn_changelog(vec![1], vec![30], vec![3]), 5800)
        .unwrap();
    assert_eq!(out.num_rows(), 0);
    // At 6300 the 5000 writes are past their ttl, but the retraction at 5800 kept the buffer:
    // the 5 displaces 20 out of the top-2 instead of seeding an empty one.
    let out = ranker
        .push(&topn_changelog(vec![1], vec![5], vec![0]), 6300)
        .unwrap();
    // Flink retracts the row leaving the rank window before inserting its replacement.
    assert_eq!(row_kinds(&out), vec![3, 0]);
    assert_eq!(values(&out, 1), vec![20, 5]);
}

// The head clock rides the snapshot (buffer order is preserved), with the inclusive boundary.
#[test]
fn retracting_topn_ttl_head_clock_survives_snapshot_restore() {
    let mut ranker =
        RetractableTopNRanker::new(vec![0], vec![asc(1)], 0, 2, false).with_state_ttl(1000);
    ranker
        .push(&topn_changelog(vec![1, 1], vec![10, 20], vec![0, 0]), 5000)
        .unwrap();
    let snapshot = ranker.snapshot();
    // One ms inside the window: the buffer is alive, so the accumulate displaces 20.
    let mut alive = RetractableTopNRanker::restore(
        vec![0],
        vec![-1],
        vec![asc(1)],
        0,
        2,
        false,
        &snapshot,
        5500,
    )
    .with_state_ttl(1000);
    let out = alive
        .push(&topn_changelog(vec![1], vec![5], vec![0]), 5999)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![3, 0]);
    // Expired exactly at the boundary: the whole buffer clears and the accumulate re-seeds.
    let mut expired = RetractableTopNRanker::restore(
        vec![0],
        vec![-1],
        vec![asc(1)],
        0,
        2,
        false,
        &snapshot,
        5500,
    )
    .with_state_ttl(1000);
    let out = expired
        .push(&topn_changelog(vec![1], vec![5], vec![0]), 6000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
}

// The sweep drops whole idle buffers by their head clock, silently.
#[test]
fn retracting_topn_ttl_sweep_drops_idle_buffers_silently() {
    let mut ranker =
        RetractableTopNRanker::new(vec![0], vec![asc(1)], 0, 2, false).with_state_ttl(1000);
    ranker
        .push(&topn_changelog(vec![1], vec![10], vec![0]), 5000)
        .unwrap();
    ranker
        .push(&topn_changelog(vec![2], vec![20], vec![0]), 5000)
        .unwrap();
    let out = ranker
        .push(&topn_changelog(vec![2], vec![21], vec![0]), 7000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // partition 2's buffer had expired too — fresh +I
    let snapshot = ranker.snapshot();
    // A TTL-off restore probes what survived: partition 1 was swept, so retracting its old 10
    // finds nothing and emits nothing (were it resident, the top-2 would emit a -D).
    let mut probe = RetractableTopNRanker::restore(
        vec![0],
        vec![-1],
        vec![asc(1)],
        0,
        2,
        false,
        &snapshot,
        7000,
    );
    let out = probe
        .push(&topn_changelog(vec![1], vec![10], vec![3]), 7000)
        .unwrap();
    assert_eq!(out.num_rows(), 0);
}

// A `[p, k, s]` batch (no `$row_kind$` — the update-fast input carries no retractions) for the
// update-fast Top-N TTL tests: partition, unique row key, sort key.
fn uf_batch(p: Vec<i64>, k: Vec<i64>, s: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("p", DataType::Int64, false),
            Field::new("k", DataType::Int64, false),
            Field::new("s", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(p)),
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(s)),
        ],
    )
    .unwrap()
}

fn uf_ranker(limit: i64) -> UpdatableTopNRanker {
    UpdatableTopNRanker::new(
        vec![0],
        vec![-1],
        vec![0, 1],
        vec![-1, -1],
        vec![asc(2)],
        limit,
        false,
        false,
    )
}

#[test]
fn update_fast_global_topn_ranks_across_composite_row_keys() {
    let mut ranker = UpdatableTopNRanker::new(
        vec![],
        vec![],
        vec![0, 1],
        vec![-1, -1],
        vec![SortColumn {
            index: 2,
            ascending: false,
            nulls_first: true,
        }],
        4,
        true,
        false,
    );
    ranker
        .push(&uf_batch(vec![1, 1, 1], vec![1, 2, 3], vec![1, 1, 1]), 0)
        .unwrap();
    let out = ranker
        .push(&uf_batch(vec![2], vec![1], vec![2]), 0)
        .unwrap();
    assert_eq!(values(&out, 3), vec![1, 2, 3, 4]);
}

#[test]
fn update_fast_ranked_topn_reemits_value_identical_updates() {
    let mut ranker = UpdatableTopNRanker::new(
        vec![0],
        vec![-1],
        vec![0, 1],
        vec![-1, -1],
        vec![asc(2)],
        4,
        true,
        true,
    );
    ranker
        .push(&uf_batch(vec![1], vec![7], vec![5]), 0)
        .unwrap();
    let out = ranker
        .push(&uf_batch(vec![1], vec![7], vec![5]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 2]);
    assert_eq!(values(&out, 3), vec![1, 1]);
}

#[test]
fn update_fast_ranked_topn_matches_flink_move_changelog_order() {
    let mut ranker = UpdatableTopNRanker::new(
        vec![0],
        vec![-1],
        vec![0, 1],
        vec![-1, -1],
        vec![SortColumn {
            index: 2,
            ascending: false,
            nulls_first: true,
        }],
        4,
        true,
        true,
    );
    ranker
        .push(&uf_batch(vec![1, 1, 1], vec![1, 2, 3], vec![1, 1, 1]), 0)
        .unwrap();
    let out = ranker
        .push(&uf_batch(vec![1], vec![2], vec![3]), 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![1, 1, 2, 2]);
    assert_eq!(values(&out, 1), vec![1, 2, 2, 1]);
    assert_eq!(values(&out, 3), vec![1, 2, 1, 2]);
}

#[test]
fn nested_update_fast_topn_uses_outer_rank() {
    let mut inner = UpdatableTopNRanker::new(
        vec![0],
        vec![-1],
        vec![0, 1],
        vec![-1, -1],
        vec![SortColumn {
            index: 2,
            ascending: false,
            nulls_first: true,
        }],
        4,
        true,
        false,
    );
    let mut outer = UpdatableTopNRanker::new(
        vec![],
        vec![],
        vec![0, 1],
        vec![-1, -1],
        vec![SortColumn {
            index: 2,
            ascending: false,
            nulls_first: true,
        }],
        4,
        true,
        false,
    );
    for input in [
        uf_batch(vec![1], vec![1], vec![1]),
        uf_batch(vec![1], vec![2], vec![1]),
        uf_batch(vec![1], vec![3], vec![1]),
    ] {
        let inner_out = inner.push(&input, 0).unwrap();
        outer.push(&inner_out, 0).unwrap();
    }
    let inner_out = inner.push(&uf_batch(vec![2], vec![1], vec![2]), 0).unwrap();
    let out = outer.push(&inner_out, 0).unwrap();
    assert_eq!(values(&out, 4), vec![1, 2, 3, 4]);
}

// Update-fast TTL granularity is the row-key entry: an expired entry reads as absent, so the row
// key's next version inserts fresh — no retraction of the expired payload.
#[test]
fn update_fast_topn_ttl_expired_row_key_updates_as_a_fresh_insert() {
    let mut ranker = uf_ranker(2).with_state_ttl(1000);
    let out = ranker
        .push(&uf_batch(vec![1], vec![7], vec![5]), 5000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    let out = ranker
        .push(&uf_batch(vec![1], vec![7], vec![9]), 6000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // un-expired this would be the +I(9)/-D(5) move diff
    assert_eq!(values(&out, 2), vec![9]);
}

// limit == 1 (FastTop1Function): a non-improving record is dropped WITHOUT a state write (no
// refresh), and once the single entry expires even a strictly worse row becomes the new top-1 —
// Flink's expired ValueState read.
#[test]
fn update_fast_topn_ttl_expired_top1_admits_a_strictly_worse_row() {
    let mut ranker = uf_ranker(1).with_state_ttl(1000);
    ranker
        .push(&uf_batch(vec![1], vec![7], vec![5]), 5000)
        .unwrap();
    assert_eq!(
        ranker
            .push(&uf_batch(vec![1], vec![8], vec![9]), 5900)
            .unwrap()
            .num_rows(),
        0
    );
    let out = ranker
        .push(&uf_batch(vec![1], vec![8], vec![9]), 6000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 2), vec![9]);
}

// An in-place replace (same row key, same sort key) is a state write and Flink emits it as an
// update-after even when every projected value is identical, so it also refreshes the entry.
#[test]
fn update_fast_topn_ttl_in_place_replace_refreshes_the_entry() {
    let mut ranker = uf_ranker(2).with_state_ttl(1000);
    ranker
        .push(&uf_batch(vec![1], vec![7], vec![5]), 5000)
        .unwrap();
    let out = ranker
        .push(&uf_batch(vec![1], vec![7], vec![5]), 5900)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![2]);
    // At 6300 the entry is alive only through the 5900 refresh: key 7's next version is a move
    // that updates the old payload rather than a fresh insert.
    let out = ranker
        .push(&uf_batch(vec![1], vec![7], vec![4]), 6300)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![2]);
    assert_eq!(values(&out, 2), vec![4]);
}

// Per-entry timestamps ride the raw snapshot, with the inclusive expiry boundary.
#[test]
fn update_fast_topn_ttl_timestamps_survive_snapshot_restore() {
    let mut ranker = uf_ranker(1).with_state_ttl(1000);
    ranker
        .push(&uf_batch(vec![1], vec![7], vec![5]), 5000)
        .unwrap();
    let snapshot = ranker.snapshot_partitions(1).remove(&0).unwrap();
    let mut alive = UpdatableTopNRanker::restore_partitions(
        vec![0],
        vec![-1],
        vec![0, 1],
        vec![-1, -1],
        vec![asc(2)],
        1,
        false,
        false,
        &[snapshot.clone()],
        5500,
    )
    .with_state_ttl(1000);
    assert_eq!(
        alive
            .push(&uf_batch(vec![1], vec![8], vec![9]), 5999)
            .unwrap()
            .num_rows(),
        0
    );
    let mut expired = UpdatableTopNRanker::restore_partitions(
        vec![0],
        vec![-1],
        vec![0, 1],
        vec![-1, -1],
        vec![asc(2)],
        1,
        false,
        false,
        &[snapshot],
        5500,
    )
    .with_state_ttl(1000);
    let out = expired
        .push(&uf_batch(vec![1], vec![8], vec![9]), 6000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
}

// The sweep prunes idle entries per row key, silently.
#[test]
fn update_fast_topn_ttl_sweep_reclaims_idle_entries_silently() {
    let mut ranker = uf_ranker(1).with_state_ttl(1000);
    ranker
        .push(&uf_batch(vec![1], vec![7], vec![5]), 5000)
        .unwrap();
    ranker
        .push(&uf_batch(vec![2], vec![8], vec![5]), 5000)
        .unwrap();
    let out = ranker
        .push(&uf_batch(vec![2], vec![8], vec![6]), 7000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // partition 2's own entry had expired too — fresh +I
    let snapshot = ranker.snapshot_partitions(1).remove(&0).unwrap();
    // A TTL-off restore probes what survived: partition 1 was swept, so a strictly worse row
    // becomes top-1 instead of being dropped against the old 5.
    let mut probe = UpdatableTopNRanker::restore_partitions(
        vec![0],
        vec![-1],
        vec![0, 1],
        vec![-1, -1],
        vec![asc(2)],
        1,
        false,
        false,
        &[snapshot],
        7000,
    );
    let out = probe
        .push(&uf_batch(vec![1], vec![9], vec![50]), 7000)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
}

// The `[k, v]` data schema (no `$row_kind$`) both sides carry in the updating-join tests.
fn kv_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]))
}

fn inner_joiner() -> UpdatingJoiner {
    UpdatingJoiner::new(
        vec![0],
        vec![0],
        JoinKind::Inner,
        kv_schema(),
        kv_schema(),
        None,
    )
}

// A `[k, v, $row_kind$]` changelog batch (k join key at col 0) for the updating-join tests.
fn changelog_join_batch(k: Vec<i64>, v: Vec<i64>, kinds: Vec<i8>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(v)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap()
}

// INNER updating join on column 0: a matched pair is emitted when the second side's row arrives,
// carrying the arriving row's kind; the output is left columns then right columns.
#[test]
fn updating_join_emits_matches_with_arriving_kind() {
    let mut joiner = inner_joiner();
    // Buffer a left row (k=1, v=10); no right yet, so nothing emits.
    assert_eq!(
        joiner
            .push(&changelog_join_batch(vec![1], vec![10], vec![0]), true, 0)
            .unwrap()
            .num_rows(),
        0
    );
    // A right row (k=1, v=100) matches it: emit +I (left ++ right).
    let out = joiner
        .push(&changelog_join_batch(vec![1], vec![100], vec![0]), false, 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 0), vec![1]); // left k
    assert_eq!(values(&out, 1), vec![10]); // left v
    assert_eq!(values(&out, 2), vec![1]); // right k
    assert_eq!(values(&out, 3), vec![100]); // right v
                                            // Retracting the left row emits the matching pair as a retraction.
    let retract = joiner
        .push(&changelog_join_batch(vec![1], vec![10], vec![3]), true, 0)
        .unwrap();
    assert_eq!(row_kinds(&retract), vec![3]); // -D
    assert_eq!(values(&retract, 1), vec![10]);
    assert_eq!(values(&retract, 3), vec![100]);
}

#[test]
fn unique_updating_join_replays_only_each_sides_final_bundle_change() {
    let mut joiner = inner_joiner().with_mini_batch(true);
    assert_eq!(
        joiner
            .push(&changelog_join_batch(vec![1], vec![100], vec![0]), false, 0)
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(joiner.flush_mini_batch().unwrap().num_rows(), 0);

    assert_eq!(
        joiner
            .push(
                &changelog_join_batch(vec![1, 1, 1], vec![10, 10, 20], vec![0, 3, 0]),
                true,
                0,
            )
            .unwrap()
            .num_rows(),
        0
    );
    assert_eq!(joiner.staged_keys(), 1);
    assert_eq!(joiner.staged_records(true), 1);
    let out = joiner.flush_mini_batch().unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![20]);
    assert_eq!(values(&out, 3), vec![100]);
}

#[test]
fn updating_join_bundle_metric_retains_both_records_of_an_update() {
    let mut joiner = inner_joiner().with_mini_batch(true);
    joiner
        .push(&changelog_join_batch(vec![1], vec![10], vec![0]), true, 0)
        .unwrap();
    joiner.flush_mini_batch().unwrap();
    joiner
        .push(
            &changelog_join_batch(vec![1, 1], vec![10, 20], vec![3, 0]),
            true,
            0,
        )
        .unwrap();
    assert_eq!(joiner.staged_records(true), 2);
}

#[test]
fn updating_join_unique_join_key_update_replaces_the_prior_row() {
    let mut joiner = inner_joiner().with_unique_join_keys(true, true);
    joiner
        .push(&changelog_join_batch(vec![1], vec![100], vec![0]), false, 0)
        .unwrap();
    joiner
        .push(&changelog_join_batch(vec![1], vec![200], vec![2]), false, 0)
        .unwrap();

    let out = joiner
        .push(&changelog_join_batch(vec![1], vec![10], vec![0]), true, 0)
        .unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 3), vec![200]);
}

// A left row matches every buffered right row of its key (cartesian per key); different keys
// never match.
#[test]
fn updating_join_is_cartesian_per_key() {
    let mut joiner = inner_joiner();
    joiner.push(
        &changelog_join_batch(vec![1, 1, 2], vec![100, 200, 300], vec![0, 0, 0]),
        false,
        0,
    );
    let out = joiner
        .push(&changelog_join_batch(vec![1], vec![10], vec![0]), true, 0)
        .unwrap();
    assert_eq!(out.num_rows(), 2); // matches both k=1 right rows, not the k=2 one
    let mut right_vs = values(&out, 3);
    right_vs.sort();
    assert_eq!(right_vs, vec![100, 200]);
}

// A null join key never matches (INNER `a.k = b.k` null semantics): the row is neither joined
// nor stored.
#[test]
fn updating_join_drops_null_keys() {
    let mut joiner = inner_joiner();
    // A right row with a null key, then a left row with a null key — no match either way.
    let right = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![None, Some(1)])),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(Int8Array::from(vec![0, 0])),
        ],
    )
    .unwrap();
    joiner.push(&right, false, 0);
    // Left null key matches nothing; left key=1 matches the stored right (1, 200).
    let left = RecordBatch::try_new(
        right.schema(),
        vec![
            Arc::new(Int64Array::from(vec![None, Some(1)])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Int8Array::from(vec![0, 0])),
        ],
    )
    .unwrap();
    let out = joiner.push(&left, true, 0).unwrap();
    assert_eq!(out.num_rows(), 1); // only key=1 pair, not the null-key rows
    assert_eq!(values(&out, 1), vec![20]); // left v
    assert_eq!(values(&out, 3), vec![200]); // right v
}

// The per-side multiset survives a checkpoint, so a post-restore arrival still finds its match.
#[test]
fn updating_join_state_survives_snapshot_restore() {
    let mut joiner = inner_joiner();
    joiner.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false, 0); // buffer right
    let snapshot = joiner.snapshot();
    let mut restored = UpdatingJoiner::restore(
        vec![0],
        vec![0],
        vec![-1],
        JoinKind::Inner,
        kv_schema(),
        kv_schema(),
        None,
        &snapshot,
        0,
    );
    let out = restored
        .push(&changelog_join_batch(vec![1], vec![10], vec![0]), true, 0)
        .unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 1), vec![10]);
    assert_eq!(values(&out, 3), vec![100]);
}

#[test]
fn updating_join_state_partitions_and_restores_by_flink_key_group() {
    let schema = kv_schema();
    let mut before = UpdatingJoiner::new(
        vec![0],
        vec![0],
        JoinKind::Inner,
        schema.clone(),
        schema.clone(),
        None,
    );
    before
        .push(
            &changelog_join_batch(vec![1, 2], vec![10, 20], vec![0, 0]),
            true,
            0,
        )
        .unwrap();
    let partitions = before.snapshot_partitions(128);
    assert!(
        partitions.len() >= 2,
        "test keys should cover distinct raw key groups"
    );
    let snapshots: Vec<Vec<u8>> = partitions.into_values().collect();
    let mut restored = UpdatingJoiner::restore_partitions(
        vec![0],
        vec![0],
        vec![-1],
        JoinKind::Inner,
        schema.clone(),
        schema,
        None,
        &snapshots,
        0,
    );
    let out = restored
        .push(
            &changelog_join_batch(vec![1, 2], vec![100, 200], vec![0, 0]),
            false,
            0,
        )
        .unwrap();
    assert_eq!(values(&out, 0), vec![1, 2]);
    assert_eq!(values(&out, 1), vec![10, 20]);
    assert_eq!(values(&out, 3), vec![100, 200]);
}

// LEFT OUTER: a left row with no right match emits a null-padded row immediately; when a right
// row later matches, the null-pad is retracted (-D) and the matched pair emitted (+I).
#[test]
fn updating_join_left_outer_null_pads_then_retracts() {
    let mut joiner = UpdatingJoiner::new(
        vec![0],
        vec![0],
        JoinKind::LeftOuter,
        kv_schema(),
        kv_schema(),
        None,
    );
    // Left row k=1, v=10: no right match → +I[left + null].
    let out = joiner
        .push(&changelog_join_batch(vec![1], vec![10], vec![0]), true, 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![10]); // left v
    assert!(out.column(3).is_null(0)); // right v nulled
                                       // Right row k=1, v=100 arrives: -D[left + null], +I[left + right].
    let out = joiner
        .push(&changelog_join_batch(vec![1], vec![100], vec![0]), false, 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![3, 0]);
    assert!(out.column(3).is_null(0)); // the retracted null-pad's right v
    assert!(!out.column(3).is_null(1)); // the matched pair's right v is present
    assert_eq!(values(&out, 1), vec![10, 10]); // both rows carry the left v
}

// LEFT OUTER on a left key that never matches: the null-pad is emitted once and retracted when
// the left row is deleted — net materialized result is empty.
#[test]
fn updating_join_left_outer_unmatched_retract() {
    let mut joiner = UpdatingJoiner::new(
        vec![0],
        vec![0],
        JoinKind::LeftOuter,
        kv_schema(),
        kv_schema(),
        None,
    );
    let out = joiner
        .push(&changelog_join_batch(vec![7], vec![70], vec![0]), true, 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // +I[left + null]
    let out = joiner
        .push(&changelog_join_batch(vec![7], vec![70], vec![3]), true, 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![3]); // -D[left + null]
    assert!(out.column(3).is_null(0));
}

// SEMI: a left row is emitted once it has a right match; ANTI would emit it while unmatched.
#[test]
fn updating_join_semi_emits_on_match() {
    let mut joiner = UpdatingJoiner::new(
        vec![0],
        vec![0],
        JoinKind::Semi,
        kv_schema(),
        kv_schema(),
        None,
    );
    // Left row with no right match → nothing (semi).
    assert_eq!(
        joiner
            .push(&changelog_join_batch(vec![1], vec![10], vec![0]), true, 0)
            .unwrap()
            .num_rows(),
        0
    );
    // Right row arrives → emit the left row (+I), one column-set (left only).
    let out = joiner
        .push(&changelog_join_batch(vec![1], vec![100], vec![0]), false, 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(out.num_columns(), 3); // left k, left v, $row_kind$ (no right columns)
    assert_eq!(values(&out, 1), vec![10]);
}

// ANTI: a left row is emitted while it has no match, and retracted (-D) once a match arrives.
#[test]
fn updating_join_anti_retracts_on_match() {
    let mut joiner = UpdatingJoiner::new(
        vec![0],
        vec![0],
        JoinKind::Anti,
        kv_schema(),
        kv_schema(),
        None,
    );
    let out = joiner
        .push(&changelog_join_batch(vec![1], vec![10], vec![0]), true, 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // +I[left] (no match yet)
    let out = joiner
        .push(&changelog_join_batch(vec![1], vec![100], vec![0]), false, 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![3]); // -D[left] (now matched)
    assert_eq!(values(&out, 1), vec![10]);
}

// State TTL: each stored row expires independently (Flink's per-entry MapState TTL), so a probe
// simply sees fewer rows in the bucket — the same key's live entries still match.
#[test]
fn updating_join_ttl_hides_expired_rows_per_entry() {
    let mut joiner = inner_joiner().with_state_ttl(1000, 0);
    // (1,10) twice (appear-times 2, last write 5500, expires 6500) and (1,11) at 5980 (expires
    // 6980); the key-2 row at 6100 runs the periodic sweep while both are live, pushing the next
    // sweep past the probe below so it exercises the lazy per-entry skip.
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![10], vec![0]),
            true,
            5000,
        )
        .unwrap();
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![10], vec![0]),
            true,
            5500,
        )
        .unwrap();
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![11], vec![0]),
            true,
            5980,
        )
        .unwrap();
    joiner
        .push(
            &changelog_join_batch(vec![2], vec![20], vec![0]),
            true,
            6100,
        )
        .unwrap();
    // At 6600 the (1,10) pair (last write 5500, expired 6500) is hidden mid-bucket — its
    // appear-times of 2 would otherwise emit two extra pairs — while (1,11) still matches.
    let out = joiner
        .push(
            &changelog_join_batch(vec![1], vec![100], vec![0]),
            false,
            6600,
        )
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![11]);
}

// A retraction whose input-side row has expired is a state no-op (the entry reads as absent), but
// the operator has no expiry awareness of its own: it still probes the other side and emits -D
// for the live matches there — exactly Flink's StreamingJoinOperator retract path.
#[test]
fn updating_join_ttl_retract_of_an_expired_row_still_emits_against_live_matches() {
    let mut joiner = inner_joiner().with_state_ttl(1000, 0);
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![10], vec![0]),
            true,
            5000,
        )
        .unwrap();
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![100], vec![0]),
            false,
            5000,
        )
        .unwrap();
    let out = joiner
        .push(
            &changelog_join_batch(vec![1], vec![10], vec![3]),
            true,
            6000,
        )
        .unwrap();
    assert_eq!(row_kinds(&out), vec![3]); // -D[10,100]: the emission ignores the expired state
    assert_eq!(values(&out, 1), vec![10]);
    assert_eq!(values(&out, 3), vec![100]);
}

// A retraction that leaves the entry live writes cnt-1 back (Flink `put`s the tuple), so it
// refreshes the survivor's TTL clock.
#[test]
fn updating_join_ttl_retract_leaving_a_live_count_refreshes_the_clock() {
    let mut joiner = inner_joiner().with_state_ttl(1000, 0);
    joiner
        .push(
            &changelog_join_batch(vec![1, 1], vec![10, 10], vec![0, 0]),
            true,
            5000,
        )
        .unwrap();
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![10], vec![3]),
            true,
            5800,
        )
        .unwrap();
    // The original write is past its ttl at 6300, but the decrement at 5800 restarted the clock:
    // the surviving appear-time still matches, exactly once.
    let out = joiner
        .push(
            &changelog_join_batch(vec![1], vec![100], vec![0]),
            false,
            6300,
        )
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![10]);
}

// LEFT outer: an expired left row is hidden from right probes silently — its null-pad is NOT
// retracted (Flink's operator never sees the expiry) — and re-adding it lands on the corpse as a
// fresh row with appear-times 1: Flink's addRecord resurrection (the "compatible for state ttl"
// family in OuterJoinRecordStateViews).
#[test]
fn updating_join_ttl_left_outer_hides_expired_rows_and_resurrects_on_re_add() {
    let mut joiner = UpdatingJoiner::new(
        vec![0],
        vec![0],
        JoinKind::LeftOuter,
        kv_schema(),
        kv_schema(),
        None,
    )
    .with_state_ttl(1000, 0);
    // (1,10) twice (appear-times 2, last write 5990, expires 6990); the key-2 row at 6100 runs
    // the periodic sweep while everything is live, pushing the next sweep past the probes below
    // so they exercise the lazy per-entry paths.
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![10], vec![0]),
            true,
            5000,
        )
        .unwrap();
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![10], vec![0]),
            true,
            5990,
        )
        .unwrap();
    joiner
        .push(
            &changelog_join_batch(vec![2], vec![20], vec![0]),
            true,
            6100,
        )
        .unwrap();
    let out = joiner
        .push(
            &changelog_join_batch(vec![1], vec![11], vec![0]),
            true,
            6995,
        )
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // +I[11+null]
                                          // At 7000 the (1,10) pair is expired and hidden: the first right row retracts exactly one
                                          // null-pad and emits exactly one pair; a live (1,10) would have contributed two more pairs.
    let out = joiner
        .push(
            &changelog_join_batch(vec![1], vec![100], vec![0]),
            false,
            7000,
        )
        .unwrap();
    assert_eq!(row_kinds(&out), vec![3, 0]); // -D[11+null], +I[11,100]
    assert!(out.column(3).is_null(0));
    assert_eq!(values(&out, 1), vec![11, 11]);
    // Re-adding (1,10) lands on the corpse in place (the next sweep is not due until 7100): it
    // reads as absent, so this is a fresh matched row, stored with appear-times reset to 1.
    let out = joiner
        .push(
            &changelog_join_batch(vec![1], vec![10], vec![0]),
            true,
            7050,
        )
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // +I[10,100]
    assert_eq!(values(&out, 1), vec![10]);
    // A second right row pairs the resurrected row exactly once — stale appear-times (2 old + 1
    // new) would triple it.
    let out = joiner
        .push(
            &changelog_join_batch(vec![1], vec![101], vec![0]),
            false,
            7060,
        )
        .unwrap();
    assert_eq!(row_kinds(&out), vec![0, 0]); // +I[10,101], +I[11,101] (bucket order not fixed)
    let mut left_vs = values(&out, 1);
    left_vs.sort();
    assert_eq!(left_vs, vec![10, 11]);
}

// Each side snapshots its own TTL timestamps (absolute millis): expiry after a restore is timed
// from the original write, per side — asymmetric retentions restore asymmetrically, and the
// boundary is Flink's inclusive `ts + ttl <= now`.
#[test]
fn updating_join_ttl_timestamps_survive_snapshot_restore_per_side() {
    let mut joiner = inner_joiner().with_state_ttl(1000, 2000);
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![10], vec![0]),
            true,
            5000,
        )
        .unwrap();
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![100], vec![0]),
            false,
            5000,
        )
        .unwrap();
    let snapshot = joiner.snapshot();
    let restore = |at: i64| {
        UpdatingJoiner::restore(
            vec![0],
            vec![0],
            vec![-1],
            JoinKind::Inner,
            kv_schema(),
            kv_schema(),
            None,
            &snapshot,
            at,
        )
        .with_state_ttl(1000, 2000)
    };
    // One ms inside the left row's window: the restored right probe still matches it.
    let mut alive = restore(5500);
    let out = alive
        .push(
            &changelog_join_batch(vec![1], vec![101], vec![0]),
            false,
            5999,
        )
        .unwrap();
    assert_eq!(values(&out, 1), vec![10]);
    // Exactly at the boundary (5000 + 1000 <= 6000) the left row is gone — while the right row's
    // 2000ms retention keeps ITS side alive for a left probe at the same instant.
    let mut expired = restore(5500);
    let out = expired
        .push(
            &changelog_join_batch(vec![1], vec![101], vec![0]),
            false,
            6000,
        )
        .unwrap();
    assert_eq!(out.num_rows(), 0);
    let out = expired
        .push(
            &changelog_join_batch(vec![1], vec![11], vec![0]),
            true,
            6000,
        )
        .unwrap();
    let mut right_vs = values(&out, 3);
    right_vs.sort();
    assert_eq!(right_vs, vec![100, 101]);
}

// A pre-TTL snapshot (no timestamp columns) restored into a TTL'd joiner stamps every row with
// the restore time — a full retention from now, Flink's enable-TTL migration — instead of
// expiring everything on first touch.
#[test]
fn updating_join_ttl_enable_migration_stamps_restore_time() {
    let mut joiner = inner_joiner(); // TTL off: the snapshot carries no timestamp columns
    joiner
        .push(&changelog_join_batch(vec![1], vec![10], vec![0]), true, 0)
        .unwrap();
    let snapshot = joiner.snapshot();
    let restore = |at: i64| {
        UpdatingJoiner::restore(
            vec![0],
            vec![0],
            vec![-1],
            JoinKind::Inner,
            kv_schema(),
            kv_schema(),
            None,
            &snapshot,
            at,
        )
        .with_state_ttl(1000, 1000)
    };
    let out = restore(5000)
        .push(
            &changelog_join_batch(vec![1], vec![100], vec![0]),
            false,
            5999,
        )
        .unwrap();
    assert_eq!(values(&out, 1), vec![10]); // alive until restore + ttl
    let out = restore(5000)
        .push(
            &changelog_join_batch(vec![1], vec![100], vec![0]),
            false,
            6000,
        )
        .unwrap();
    assert_eq!(out.num_rows(), 0);
}

// The periodic sweep reclaims rows never touched again, silently (expiry emits nothing), and
// drops the emptied bucket and key with them.
#[test]
fn updating_join_ttl_sweep_reclaims_idle_rows_silently() {
    let mut joiner = inner_joiner().with_state_ttl(1000, 1000);
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![10], vec![0]),
            true,
            5000,
        )
        .unwrap();
    // A push a full period later sweeps both sides; nothing is emitted for the reclaimed row.
    let out = joiner
        .push(
            &changelog_join_batch(vec![2], vec![20], vec![0]),
            true,
            7000,
        )
        .unwrap();
    assert_eq!(out.num_rows(), 0);
    // The swept row is gone from the snapshot: a TTL-off restore (which would never expire it
    // lazily) no longer finds a match for key 1, but still does for the live key 2.
    let snapshot = joiner.snapshot();
    let mut probe = UpdatingJoiner::restore(
        vec![0],
        vec![0],
        vec![-1],
        JoinKind::Inner,
        kv_schema(),
        kv_schema(),
        None,
        &snapshot,
        7000,
    );
    let out = probe
        .push(
            &changelog_join_batch(vec![1], vec![100], vec![0]),
            false,
            7000,
        )
        .unwrap();
    assert_eq!(out.num_rows(), 0);
    let out = probe
        .push(
            &changelog_join_batch(vec![2], vec![200], vec![0]),
            false,
            7000,
        )
        .unwrap();
    assert_eq!(values(&out, 1), vec![20]);
}

// Mini-batch: the durable-first-row capture reads its own side under TTL, so a bundle replacing
// an expired stored row replays a fresh insert rather than a retraction of the corpse.
#[test]
fn updating_join_ttl_mini_batch_ignores_expired_durable_rows() {
    let mut joiner = inner_joiner().with_mini_batch(true).with_state_ttl(0, 1000);
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![10], vec![0]),
            true,
            5000,
        )
        .unwrap();
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![100], vec![0]),
            false,
            5000,
        )
        .unwrap();
    let out = joiner.flush_mini_batch().unwrap();
    assert_eq!(row_kinds(&out), vec![0]); // +I[10,100]
                                          // The left update keeps the bundle's staging non-empty (no mid-bundle sweep); the right
                                          // replacement then reads its stored (1,100) as expired at 6000, staging a fresh insert.
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![11], vec![0]),
            true,
            5900,
        )
        .unwrap();
    joiner
        .push(
            &changelog_join_batch(vec![1], vec![101], vec![0]),
            false,
            6000,
        )
        .unwrap();
    let out = joiner.flush_mini_batch().unwrap();
    // One fresh pair; a durable probe that ignored expiry would also replay -D[11,100].
    assert_eq!(row_kinds(&out), vec![0]);
    assert_eq!(values(&out, 1), vec![11]);
    assert_eq!(values(&out, 3), vec![101]);
}

// The `[k, v, rt]` data schema (rt an i64 millis column) both sides carry in the temporal-join
// tests; `rt_to_millis` reads an i64 rowtime directly.
fn temporal_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("rt", DataType::Int64, false),
    ]))
}

fn temporal_probe_batch(k: Vec<i64>, v: Vec<i64>, rt: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        temporal_schema(),
        vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(v)),
            Arc::new(Int64Array::from(rt)),
        ],
    )
    .unwrap()
}

fn temporal_build_batch(k: Vec<i64>, v: Vec<i64>, rt: Vec<i64>, kinds: Vec<i8>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("rt", DataType::Int64, false),
            Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(v)),
            Arc::new(Int64Array::from(rt)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap()
}

fn temporal_joiner(join_type: JoinKind) -> TemporalJoiner {
    TemporalJoiner::new(
        vec![0],
        vec![0],
        2,
        2,
        join_type,
        temporal_schema(),
        temporal_schema(),
        None,
    )
}

// Each probe row joins the build version valid at its rowtime — the latest accumulate version
// whose rightTime <= the probe time; emission is gated on the watermark.
#[test]
fn temporal_join_picks_version_valid_at_probe_time() {
    let mut joiner = temporal_joiner(JoinKind::Inner);
    // key 1: rate 10@100 then rate 20@300 (+U); key 2: rate 99@100.
    joiner.push_right(
        &temporal_build_batch(vec![1], vec![10], vec![100], vec![0]),
        0,
    );
    joiner.push_right(
        &temporal_build_batch(vec![1], vec![20], vec![300], vec![2]),
        0,
    );
    joiner.push_right(
        &temporal_build_batch(vec![2], vec![99], vec![100], vec![0]),
        0,
    );
    joiner.push_left(
        &temporal_probe_batch(vec![1, 1, 2], vec![1, 2, 3], vec![200, 500, 150]),
        0,
    );
    let out = joiner.advance(i64::MAX, 0).unwrap();
    assert_eq!(out.num_rows(), 3);
    // probe@200 -> 10, probe@500 -> 20 (the +U version), probe@150 -> 99 (cross-key order varies).
    let mut right_rate = values(&out, 4);
    right_rate.sort();
    assert_eq!(right_rate, vec![10, 20, 99]);
}

// A LEFT temporal join null-pads a probe row whose valid version is missing or a delete marker.
#[test]
fn temporal_join_left_pads_on_delete_or_missing() {
    let mut joiner = temporal_joiner(JoinKind::LeftOuter);
    joiner.push_right(
        &temporal_build_batch(vec![1], vec![10], vec![100], vec![0]),
        0,
    );
    joiner.push_right(
        &temporal_build_batch(vec![2], vec![99], vec![100], vec![0]),
        0,
    );
    joiner.push_right(
        &temporal_build_batch(vec![2], vec![99], vec![400], vec![3]),
        0,
    ); // delete @400
    joiner.push_left(
        &temporal_probe_batch(
            vec![1, 2, 1],
            vec![1, 2, 3],
            vec![50, 500, 200], // 50: before any version; 500: after key-2 delete; 200: -> 10
        ),
        0,
    );
    let out = joiner.advance(i64::MAX, 0).unwrap();
    assert_eq!(out.num_rows(), 3);
    // Exactly one row matched (right rate present); the other two are null-padded.
    let matched = (0..out.num_rows())
        .filter(|&i| !out.column(4).is_null(i))
        .count();
    assert_eq!(matched, 1);
}

// A probe row buffered below the watermark stays until a later watermark passes its time, and then
// resolves against a version that arrived in the meantime; state survives a checkpoint.
#[test]
fn temporal_join_buffers_and_survives_snapshot_restore() {
    let mut joiner = temporal_joiner(JoinKind::Inner);
    joiner.push_right(
        &temporal_build_batch(vec![1], vec![10], vec![100], vec![0]),
        0,
    );
    joiner.push_left(&temporal_probe_batch(vec![1], vec![1], vec![500]), 0);
    assert_eq!(joiner.advance(200, 0).unwrap().num_rows(), 0); // watermark 200 < probe time 500
    let snapshot = joiner.snapshot();
    let mut restored = TemporalJoiner::restore(
        vec![0],
        vec![0],
        2,
        2,
        JoinKind::Inner,
        temporal_schema(),
        temporal_schema(),
        None,
        &snapshot,
        0,
        0,
    );
    restored.push_right(
        &temporal_build_batch(vec![1], vec![20], vec![300], vec![2]),
        0,
    ); // +U @300
    let out = restored.advance(i64::MAX, 0).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 4), vec![20]); // resolves to the version valid at 500 (rate 20 @300)
}

#[test]
fn temporal_join_state_partitions_and_restores_by_flink_key_group() {
    let mut before = temporal_joiner(JoinKind::Inner);
    let _ = before.push_right(
        &temporal_build_batch(vec![1, 2], vec![10, 20], vec![100, 100], vec![0, 0]),
        0,
    );
    let _ = before.push_left(
        &temporal_probe_batch(vec![1, 2], vec![1, 2], vec![500, 500]),
        0,
    );
    let partitions = before.snapshot_partitions(128, &[-1]);
    assert!(
        partitions.len() >= 2,
        "test keys should cover distinct raw key groups"
    );
    let snapshots: Vec<Vec<u8>> = partitions.into_values().collect();

    let mut restored = TemporalJoiner::restore_partitions(
        vec![0],
        vec![0],
        2,
        2,
        JoinKind::Inner,
        temporal_schema(),
        temporal_schema(),
        None,
        &snapshots,
        0,
        0,
    );
    let out = restored.advance(i64::MAX, 0).unwrap();
    let mut rates = values(&out, 4);
    rates.sort_unstable();
    assert_eq!(rates, vec![10, 20]);
}

// A residual non-equi predicate gates the version match: the version valid at the probe time is
// joined only when the pair also satisfies the predicate, else (INNER) the probe row is dropped.
// Joined row is [lk, lamount, lrt, rk, rrate, rrt] = indices [0..6]; predicate is amount > rate.
#[test]
fn temporal_join_applies_non_equi_predicate() {
    let predicate = JoinPredicate {
        kinds: vec![6, 0, 0],    // CALL(>), input_ref, input_ref
        payload: vec![10, 1, 4], // op GREATER_THAN; probe.amount (col 1) > build.rate (col 4)
        child_counts: vec![2, 0, 0],
        longs: vec![],
        doubles: vec![],
        strings: vec![],
        compiled: None,
    };
    let mut joiner = TemporalJoiner::new(
        vec![0],
        vec![0],
        2,
        2,
        JoinKind::Inner,
        temporal_schema(),
        temporal_schema(),
        Some(predicate),
    );
    // key 1: rate 5@100 then rate 50@300 (+U).
    joiner.push_right(
        &temporal_build_batch(vec![1], vec![5], vec![100], vec![0]),
        0,
    );
    joiner.push_right(
        &temporal_build_batch(vec![1], vec![50], vec![300], vec![2]),
        0,
    );
    // amount 10 @200 -> version rate 5, 10 > 5 matches; amount 10 @500 -> version rate 50, fails.
    joiner.push_left(
        &temporal_probe_batch(vec![1, 1], vec![10, 10], vec![200, 500]),
        0,
    );
    let out = joiner.advance(i64::MAX, 0).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 4), vec![5]); // only the pair passing amount > rate
}

// Flink retention-bounds the temporal join with ONE per-key processing-time cleanup deadline
// (min = table.exec.state.ttl, max = 1.5x min), not per-value TTL: registered at every touch,
// and when the clock reaches it the key's ENTIRE state — both sides — clears silently. A timer
// registered at T fires once processing time reaches T, so the key is gone at `now >= T`.
#[test]
fn temporal_join_retention_clears_the_key_at_exactly_the_deadline() {
    // Alive one millisecond inside the horizon: deadline = 5000 + 1.5 * 2000 = 8000.
    let mut alive = temporal_joiner(JoinKind::LeftOuter).with_state_retention(2000);
    alive
        .push_right(
            &temporal_build_batch(vec![1], vec![10], vec![100], vec![0]),
            5000,
        )
        .unwrap();
    alive
        .push_left(&temporal_probe_batch(vec![1], vec![1], vec![200]), 7999)
        .unwrap();
    assert_eq!(values(&alive.advance(i64::MAX, 7999).unwrap(), 4), vec![10]);

    // Cleared at exactly the deadline: the probe's touch at 8000 finds the versions gone and
    // null-pads per the normal absent-version LEFT behavior.
    let mut expired = temporal_joiner(JoinKind::LeftOuter).with_state_retention(2000);
    expired
        .push_right(
            &temporal_build_batch(vec![1], vec![10], vec![100], vec![0]),
            5000,
        )
        .unwrap();
    expired
        .push_left(&temporal_probe_batch(vec![1], vec![1], vec![200]), 8000)
        .unwrap();
    let out = expired.advance(i64::MAX, 8000).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert!(out.column(4).is_null(0));
}

// Flink's re-registration hysteresis: the deadline starts at now + max and moves (to now + max)
// only when a touch lands within a min-retention of it — `now + min > deadline`. Pinned with a
// three-write sequence: 1000 registers 4000, 2000 leaves it (2000 + min == 4000, not >), 2001
// moves it to 5001.
#[test]
fn temporal_join_retention_moves_the_deadline_only_past_the_hysteresis() {
    let mut unmoved = temporal_joiner(JoinKind::LeftOuter).with_state_retention(2000);
    unmoved
        .push_right(
            &temporal_build_batch(vec![1], vec![10], vec![100], vec![0]),
            1000,
        )
        .unwrap();
    unmoved
        .push_right(
            &temporal_build_batch(vec![1], vec![20], vec![300], vec![2]),
            2000,
        )
        .unwrap();
    // The write at 2000 did NOT move the 4000 deadline: the key clears at 4000.
    unmoved
        .push_left(&temporal_probe_batch(vec![1], vec![1], vec![400]), 4000)
        .unwrap();
    let out = unmoved.advance(i64::MAX, 4000).unwrap();
    assert!(out.column(4).is_null(0));

    let mut moved = temporal_joiner(JoinKind::LeftOuter).with_state_retention(2000);
    moved
        .push_right(
            &temporal_build_batch(vec![1], vec![10], vec![100], vec![0]),
            1000,
        )
        .unwrap();
    moved
        .push_right(
            &temporal_build_batch(vec![1], vec![20], vec![300], vec![2]),
            2000,
        )
        .unwrap();
    moved
        .push_right(
            &temporal_build_batch(vec![1], vec![30], vec![500], vec![2]),
            2001,
        )
        .unwrap();
    // A probe at 3001 (3001 + min == 5001, not > — deadline unmoved) still sees the versions...
    moved
        .push_left(&temporal_probe_batch(vec![1], vec![1], vec![600]), 3001)
        .unwrap();
    assert_eq!(values(&moved.advance(i64::MAX, 3001).unwrap(), 4), vec![30]);
    // ...and the key clears at the moved deadline 5001.
    moved
        .push_left(&temporal_probe_batch(vec![1], vec![2], vec![700]), 5001)
        .unwrap();
    let out = moved.advance(i64::MAX, 5001).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert!(out.column(4).is_null(0));
}

// Flink's `cleanupState` clears the key's ENTIRE state: buffered probe rows below the watermark
// vanish with the versions, silently — only rows probed after the expiry emit (null-padded).
#[test]
fn temporal_join_retention_cleanup_drops_buffered_probe_rows_too() {
    let mut joiner = temporal_joiner(JoinKind::LeftOuter).with_state_retention(2000);
    joiner
        .push_right(
            &temporal_build_batch(vec![1], vec![10], vec![100], vec![0]),
            1000,
        )
        .unwrap();
    joiner
        .push_left(&temporal_probe_batch(vec![1], vec![1], vec![500]), 1000)
        .unwrap();
    assert_eq!(joiner.advance(200, 1000).unwrap().num_rows(), 0); // buffered below the watermark
    joiner
        .push_left(&temporal_probe_batch(vec![1], vec![2], vec![600]), 4000)
        .unwrap();
    let out = joiner.advance(i64::MAX, 4000).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 1), vec![2]); // the pre-expiry buffered probe row emitted nothing
    assert!(out.column(4).is_null(0));
}

// A key whose buffered probe rows fire re-registers its deadline when state remains on either
// side (Flink's onEventTime), under the same hysteresis rule.
#[test]
fn temporal_join_retention_watermark_fire_re_registers_the_deadline() {
    let mut joiner = temporal_joiner(JoinKind::Inner).with_state_retention(2000);
    joiner
        .push_right(
            &temporal_build_batch(vec![1], vec![10], vec![100], vec![0]),
            1000,
        )
        .unwrap();
    joiner
        .push_left(&temporal_probe_batch(vec![1], vec![1], vec![500]), 1000)
        .unwrap();
    // The fire at 3999 emits and re-registers (3999 + min > 4000): the deadline moves to 6999...
    assert_eq!(values(&joiner.advance(600, 3999).unwrap(), 4), vec![10]);
    // ...so a probe at 4000 — the original deadline — still finds the version.
    joiner
        .push_left(&temporal_probe_batch(vec![1], vec![2], vec![700]), 4000)
        .unwrap();
    assert_eq!(
        values(&joiner.advance(i64::MAX, 4000).unwrap(), 4),
        vec![10]
    );
}

// Flink's enablement quirk, replicated exactly: `stateCleaningEnabled = minRetentionTime > 1` —
// strictly greater than ONE millisecond, not zero. A 1ms retention never cleans, and its
// checkpoints stay in the two-section pre-retention format.
#[test]
fn temporal_join_retention_of_one_millisecond_disables_cleaning() {
    let mut joiner = temporal_joiner(JoinKind::Inner).with_state_retention(1);
    joiner
        .push_right(
            &temporal_build_batch(vec![1], vec![10], vec![100], vec![0]),
            1000,
        )
        .unwrap();
    joiner
        .push_left(&temporal_probe_batch(vec![1], vec![1], vec![200]), i64::MAX)
        .unwrap();
    assert_eq!(
        values(&joiner.advance(i64::MAX, i64::MAX).unwrap(), 4),
        vec![10]
    );
    assert_eq!(read_framed_sections(&joiner.snapshot()).len(), 2);
}

// Keys never touched again are reclaimed by the silent once-per-min-retention sweep — the lazy
// per-touch check would never see them.
#[test]
fn temporal_join_retention_sweep_reclaims_untouched_keys_silently() {
    let mut joiner = temporal_joiner(JoinKind::Inner).with_state_retention(2000);
    joiner
        .push_right(
            &temporal_build_batch(vec![1], vec![10], vec![100], vec![0]),
            1000,
        )
        .unwrap();
    // Key 1 is never touched again; an ingest of another key past its 4000 deadline runs the
    // sweep, which drops key 1's versions and deadline with no output.
    joiner
        .push_right(
            &temporal_build_batch(vec![2], vec![99], vec![100], vec![0]),
            4000,
        )
        .unwrap();
    let sections = read_framed_sections(&joiner.snapshot());
    let right_keys: Vec<i64> = read_ipc_if_present(&sections[1])
        .iter()
        .flat_map(|b| values(b, 0))
        .collect();
    assert_eq!(right_keys, vec![2]);
    let deadline_keys: Vec<i64> = read_ipc_if_present(&sections[2])
        .iter()
        .flat_map(|b| values(b, 0))
        .collect();
    assert_eq!(deadline_keys, vec![2]);
}

// The snapshot carries each key's ABSOLUTE deadline (a third framed section, written only while
// cleaning is on); a restore keeps it as-is rather than re-stamping from the restore clock.
#[test]
fn temporal_join_retention_deadline_rides_the_snapshot_absolutely() {
    let mut writer = temporal_joiner(JoinKind::LeftOuter).with_state_retention(2000);
    writer
        .push_right(
            &temporal_build_batch(vec![1], vec![10], vec![100], vec![0]),
            5000,
        )
        .unwrap();
    let snapshot = writer.snapshot();
    assert_eq!(read_framed_sections(&snapshot).len(), 3);

    let restore = || {
        TemporalJoiner::restore(
            vec![0],
            vec![0],
            2,
            2,
            JoinKind::LeftOuter,
            temporal_schema(),
            temporal_schema(),
            None,
            &snapshot,
            2000,
            6000,
        )
    };
    // Alive at 7999 and cleared at exactly 8000 — the writer's deadline, not the restore-time
    // stamp (restoring at 6000 would have stamped 9000).
    let mut alive = restore();
    alive
        .push_left(&temporal_probe_batch(vec![1], vec![1], vec![200]), 7999)
        .unwrap();
    assert_eq!(values(&alive.advance(i64::MAX, 7999).unwrap(), 4), vec![10]);
    let mut expired = restore();
    expired
        .push_left(&temporal_probe_batch(vec![1], vec![1], vec![200]), 8000)
        .unwrap();
    let out = expired.advance(i64::MAX, 8000).unwrap();
    assert!(out.column(4).is_null(0));
}

// Deadlines partition with their key groups and survive a partitioned restore. Restoring at 4000
// would stamp a missing deadline at 7000, so a key still alive at 7999 proves the section (with
// the writer's 8000) was read, per key group.
#[test]
fn temporal_join_retention_deadlines_partition_by_flink_key_group() {
    let mut before = temporal_joiner(JoinKind::LeftOuter).with_state_retention(2000);
    before
        .push_right(
            &temporal_build_batch(vec![1, 2], vec![10, 20], vec![100, 100], vec![0, 0]),
            5000,
        )
        .unwrap();
    let partitions = before.snapshot_partitions(128, &[-1]);
    assert!(
        partitions.len() >= 2,
        "test keys should cover distinct raw key groups"
    );
    let snapshots: Vec<Vec<u8>> = partitions.into_values().collect();
    let mut restored = TemporalJoiner::restore_partitions(
        vec![0],
        vec![0],
        2,
        2,
        JoinKind::LeftOuter,
        temporal_schema(),
        temporal_schema(),
        None,
        &snapshots,
        2000,
        4000,
    );
    restored
        .push_left(
            &temporal_probe_batch(vec![1, 2], vec![1, 2], vec![200, 200]),
            7999,
        )
        .unwrap();
    let mut rates = values(&restored.advance(i64::MAX, 7999).unwrap(), 4);
    rates.sort_unstable();
    assert_eq!(rates, vec![10, 20]);
}

// A pre-retention snapshot restored into a retention-enabled joiner stamps every key a full max
// horizon from the restore (Flink's enable-TTL migration), instead of expiring on first touch.
#[test]
fn temporal_join_pre_retention_snapshot_stamps_a_full_deadline_at_restore() {
    let mut writer = temporal_joiner(JoinKind::LeftOuter);
    writer
        .push_right(
            &temporal_build_batch(vec![1], vec![10], vec![100], vec![0]),
            0,
        )
        .unwrap();
    let snapshot = writer.snapshot();
    assert_eq!(read_framed_sections(&snapshot).len(), 2); // retention off: pre-TTL format

    let restore = || {
        TemporalJoiner::restore(
            vec![0],
            vec![0],
            2,
            2,
            JoinKind::LeftOuter,
            temporal_schema(),
            temporal_schema(),
            None,
            &snapshot,
            2000,
            10_000,
        )
    };
    // Stamped 10000 + max = 13000: alive at 12999, cleared at 13000.
    let mut alive = restore();
    alive
        .push_left(&temporal_probe_batch(vec![1], vec![1], vec![200]), 12_999)
        .unwrap();
    assert_eq!(
        values(&alive.advance(i64::MAX, 12_999).unwrap(), 4),
        vec![10]
    );
    let mut expired = restore();
    expired
        .push_left(&temporal_probe_batch(vec![1], vec![1], vec![200]), 13_000)
        .unwrap();
    assert!(expired
        .advance(i64::MAX, 13_000)
        .unwrap()
        .column(4)
        .is_null(0));
}

// A residual non-equi predicate gates which same-key pairs are matches. `left.v > right.v`
// (cols [k, lv, k0, rv] = indices [0,1,2,3]) over an INNER join: of two buffered right rows only
// the one whose v is below the left's v matches.
#[test]
fn updating_join_applies_non_equi_predicate() {
    let predicate = JoinPredicate {
        kinds: vec![6, 0, 0],    // CALL(>), input_ref, input_ref
        payload: vec![10, 1, 3], // op GREATER_THAN; left.v (col 1) > right.v (col 3)
        child_counts: vec![2, 0, 0],
        longs: vec![],
        doubles: vec![],
        strings: vec![],
        compiled: None,
    };
    let mut joiner = UpdatingJoiner::new(
        vec![0],
        vec![0],
        JoinKind::Inner,
        kv_schema(),
        kv_schema(),
        Some(predicate),
    );
    // Buffer two right rows for k=1: v=5 and v=20.
    joiner.push(
        &changelog_join_batch(vec![1, 1], vec![5, 20], vec![0, 0]),
        false,
        0,
    );
    // Left row k=1, v=10 → matches only the right v=5 (10 > 5), not v=20.
    let out = joiner
        .push(&changelog_join_batch(vec![1], vec![10], vec![0]), true, 0)
        .unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 3), vec![5]); // the one right row passing left.v > right.v
}

// The degree survives a checkpoint: a restored LEFT OUTER joiner still retracts the null-pad when
// the first match arrives post-restore.
#[test]
fn updating_join_outer_degree_survives_snapshot_restore() {
    let mut joiner = UpdatingJoiner::new(
        vec![0],
        vec![0],
        JoinKind::LeftOuter,
        kv_schema(),
        kv_schema(),
        None,
    );
    joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true, 0); // +I[left+null], degree 0
    let snapshot = joiner.snapshot();
    let mut restored = UpdatingJoiner::restore(
        vec![0],
        vec![0],
        vec![-1],
        JoinKind::LeftOuter,
        kv_schema(),
        kv_schema(),
        None,
        &snapshot,
        0,
    );
    let out = restored
        .push(&changelog_join_batch(vec![1], vec![100], vec![0]), false, 0)
        .unwrap();
    assert_eq!(row_kinds(&out), vec![3, 0]); // -D[left+null], +I[left+right]
}

// The `[k, v, rt]` data schema both sides of the interval-join tests carry.
fn interval_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("rt", DataType::Int64, false),
    ]))
}

// An INNER interval joiner over the `[k, v, rt]` schema (key col 0, rowtime col 2).
fn inner_interval_joiner(lower: i64, upper: i64) -> IntervalJoiner {
    IntervalJoiner::new(
        vec![0],
        vec![0],
        2,
        2,
        lower,
        upper,
        None,
        JoinKind::Inner,
        interval_schema(),
        interval_schema(),
    )
}

// A `[k, v, rt]` batch with int64 rowtime (epoch millis) for the interval-join tests.
fn join_batch(k: Vec<i64>, v: Vec<i64>, rt: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        interval_schema(),
        vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(v)),
            Arc::new(Int64Array::from(rt)),
        ],
    )
    .unwrap()
}

// INNER interval join: a left row matches a buffered right row of the same key whose rowtime is
// within [rt + lower, rt + upper]; output columns are left ++ right.
#[test]
fn interval_join_emits_matched_pairs() {
    // a.rt BETWEEN b.rt - 1000 AND b.rt + 1000, single equi-key on column 0, rt is column 2.
    let mut joiner = inner_interval_joiner(-1000, 1000);
    // Buffer two right rows for key 1 (rt 5500 in range of left 5000, rt 7000 out of range).
    assert_eq!(
        joiner
            .push_right(
                join_batch(vec![1, 1], vec![100, 200], vec![5500, 7000]),
                None
            )
            .unwrap()
            .num_rows(),
        0
    );
    // A left row (k=1, rt=5000): matches the rt=5500 right row only (delta -500 in [-1000,1000]).
    let out = joiner
        .push_left(join_batch(vec![1], vec![10], vec![5000]), None)
        .unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 0), vec![1]); // left k
    assert_eq!(values(&out, 1), vec![10]); // left v
    assert_eq!(values(&out, 2), vec![5000]); // left rt
    assert_eq!(values(&out, 3), vec![1]); // right k
    assert_eq!(values(&out, 4), vec![100]); // right v
    assert_eq!(values(&out, 5), vec![5500]); // right rt
}

// Different keys never match, and a pair is emitted once — when its second side arrives —
// regardless of which side arrived first.
#[test]
fn interval_join_matches_on_key_and_emits_once() {
    let mut joiner = inner_interval_joiner(-1000, 1000);
    // Left first: buffer a left row, no right yet.
    assert_eq!(
        joiner
            .push_left(join_batch(vec![1], vec![10], vec![5000]), None)
            .unwrap()
            .num_rows(),
        0
    );
    // A right row with a different key does not match.
    assert_eq!(
        joiner
            .push_right(join_batch(vec![2], vec![100], vec![5000]), None)
            .unwrap()
            .num_rows(),
        0
    );
    // A matching right row emits the pair exactly once.
    let out = joiner
        .push_right(join_batch(vec![1], vec![100], vec![5500]), None)
        .unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 1), vec![10]);
    assert_eq!(values(&out, 4), vec![100]);
}

// The watermark evicts rows past their last useful rowtime, so a later arrival can no longer
// match an evicted row.
#[test]
fn interval_join_evicts_dead_rows_on_watermark() {
    let mut joiner = inner_interval_joiner(-1000, 1000);
    joiner.push_left(join_batch(vec![1], vec![10], vec![5000]), None);
    // Watermark 6000: left.rt - lower = 5000 - (-1000) = 6000, not > 6000, so the row is evicted.
    joiner.advance(6000).unwrap();
    // A right row that would otherwise match (delta -500) finds nothing buffered.
    assert_eq!(
        joiner
            .push_right(join_batch(vec![1], vec![100], vec![5500]), None)
            .unwrap()
            .num_rows(),
        0
    );
}

// The `[k, v, window_start, window_end]` data schema the window-join tests carry.
fn window_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("window_start", DataType::Int64, false),
        Field::new("window_end", DataType::Int64, false),
    ]))
}

// A window joiner of the given kind (key col 0, window bounds cols 2/3) over `window_schema`.
fn window_joiner(kind: JoinKind) -> WindowJoiner {
    WindowJoiner::new(
        vec![0],
        vec![0],
        2,
        3,
        2,
        3,
        None,
        kind,
        window_schema(),
        window_schema(),
    )
}

// A `[k, v, window_start, window_end]` batch (window bounds as int64 millis) for window-join tests.
fn window_batch(k: Vec<i64>, v: Vec<i64>, ws: Vec<i64>, we: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        window_schema(),
        vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(v)),
            Arc::new(Int64Array::from(ws)),
            Arc::new(Int64Array::from(we)),
        ],
    )
    .unwrap()
}

// The matched (left v, right v) pairs of a join output, sorted (the hash join does not promise
// an output order; parity is over the result set).
fn left_right_values(batch: &RecordBatch) -> Vec<(i64, i64)> {
    let mut pairs: Vec<(i64, i64)> = values(batch, 1).into_iter().zip(values(batch, 5)).collect();
    pairs.sort_unstable();
    pairs
}

// INNER window join: left and right rows of the same key in the same window join (their cross
// product) once the watermark closes the window; other windows/keys do not match.
#[test]
fn window_join_emits_matches_when_window_closes() {
    // keys col 0; window_start col 2, window_end col 3 on both sides.
    let mut joiner = window_joiner(JoinKind::Inner);
    // Window [0,1000): left k=1 (two rows) and k=2; right k=1 and k=3.
    joiner.push_left(window_batch(
        vec![1, 1, 2],
        vec![10, 11, 20],
        vec![0, 0, 0],
        vec![1000, 1000, 1000],
    ));
    joiner.push_right(window_batch(
        vec![1, 3],
        vec![100, 300],
        vec![0, 0],
        vec![1000, 1000],
    ));
    // A later window [1000,2000) for k=1 on both sides (should not mix with [0,1000)).
    joiner.push_left(window_batch(vec![1], vec![40], vec![1000], vec![2000]));
    joiner.push_right(window_batch(vec![1], vec![400], vec![1000], vec![2000]));

    // Watermark 1000 closes only [0,1000): k=1 matches (2 left × 1 right = 2 rows), k=2/k=3 don't.
    let out = joiner.flush(1000).expect("window join flush");
    assert_eq!(left_right_values(&out), vec![(10, 100), (11, 100)]);

    // Watermark 2000 closes [1000,2000): k=1 matches once.
    let rest = joiner.flush(2000).expect("window join flush");
    assert_eq!(left_right_values(&rest), vec![(40, 400)]);
}

#[test]
fn window_join_counts_late_rows_per_input() {
    let mut joiner = window_joiner(JoinKind::Inner);
    joiner.flush(1000).unwrap();
    joiner
        .push_left(window_batch(vec![1], vec![10], vec![0], vec![1000]))
        .unwrap();
    joiner
        .push_right(window_batch(vec![1], vec![100], vec![0], vec![1000]))
        .unwrap();
    assert_eq!(joiner.left_late_drops, 1);
    assert_eq!(joiner.right_late_drops, 1);
}

// Buffered window-join rows survive a snapshot/restore round trip.
#[test]
fn window_join_restores_buffered_rows() {
    let mut joiner = window_joiner(JoinKind::Inner);
    joiner.push_left(window_batch(vec![1], vec![10], vec![0], vec![1000]));
    joiner.push_right(window_batch(vec![1], vec![100], vec![0], vec![1000]));
    let snapshot = joiner.snapshot();
    let mut restored = WindowJoiner::restore(
        vec![0],
        vec![0],
        2,
        3,
        2,
        3,
        None,
        JoinKind::Inner,
        window_schema(),
        window_schema(),
        &snapshot,
    );
    let out = restored.flush(1000).expect("window join flush");
    assert_eq!(left_right_values(&out), vec![(10, 100)]);
}

#[test]
fn window_join_state_partitions_and_restores_by_flink_key_group() {
    let mut before = window_joiner(JoinKind::Inner);
    let _ = before.push_left(window_batch(
        vec![1, 2],
        vec![10, 20],
        vec![0, 0],
        vec![1000, 1000],
    ));
    let _ = before.push_right(window_batch(
        vec![1, 2],
        vec![100, 200],
        vec![0, 0],
        vec![1000, 1000],
    ));
    let partitions = before.snapshot_partitions(128, &[-1]);
    assert!(
        partitions.len() >= 2,
        "test keys should cover distinct raw key groups"
    );
    let snapshots: Vec<Vec<u8>> = partitions.into_values().collect();

    let mut restored = WindowJoiner::restore_partitions(
        vec![0],
        vec![0],
        2,
        3,
        2,
        3,
        None,
        JoinKind::Inner,
        window_schema(),
        window_schema(),
        &snapshots,
    );
    let out = restored.flush(1000).unwrap();
    assert_eq!(left_right_values(&out), vec![(10, 100), (20, 200)]);
}

// LEFT window join: a left row whose window has no matching right row is null-padded when the
// window closes (append-only — emitted once at flush).
#[test]
fn window_left_join_null_pads_unmatched() {
    let mut joiner = window_joiner(JoinKind::LeftOuter);
    // Window [0,1000): left k=1 (matches right) and k=2 (no right match); right k=1 only.
    joiner.push_left(window_batch(
        vec![1, 2],
        vec![10, 20],
        vec![0, 0],
        vec![1000, 1000],
    ));
    joiner.push_right(window_batch(vec![1], vec![100], vec![0], vec![1000]));
    let out = joiner.flush(1000).expect("window join flush");
    // k=1 emits the matched pair [10,100]; k=2 emits [20, null].
    assert_eq!(out.num_rows(), 2);
    let mut left_vs = values(&out, 1);
    left_vs.sort_unstable();
    assert_eq!(left_vs, vec![10, 20]);
    // Exactly one row (k=2) has a null right v (column 5).
    let null_right = (0..out.num_rows())
        .filter(|&i| out.column(5).is_null(i))
        .count();
    assert_eq!(null_right, 1);
}

// Buffered rows survive a snapshot/restore round trip and still match afterward.
#[test]
fn interval_join_restores_buffered_rows() {
    let mut joiner = inner_interval_joiner(-1000, 1000);
    joiner.push_right(join_batch(vec![1], vec![100], vec![5500]), None);
    let snapshot = joiner.snapshot();
    let mut restored = IntervalJoiner::restore(
        vec![0],
        vec![0],
        2,
        2,
        -1000,
        1000,
        None,
        JoinKind::Inner,
        interval_schema(),
        interval_schema(),
        &snapshot,
    );
    let out = restored
        .push_left(join_batch(vec![1], vec![10], vec![5000]), None)
        .unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 4), vec![100]);
}

fn left_interval_joiner(lower: i64, upper: i64) -> IntervalJoiner {
    IntervalJoiner::new(
        vec![0],
        vec![0],
        2,
        2,
        lower,
        upper,
        None,
        JoinKind::LeftOuter,
        interval_schema(),
        interval_schema(),
    )
}

// LEFT interval join: a left row that never matches is null-padded once its interval is evicted by
// the watermark (append-only — emitted once). A left row evicts when `rt - lower <= watermark`.
#[test]
fn interval_left_join_null_pads_unmatched_on_eviction() {
    let mut joiner = left_interval_joiner(-1000, 1000);
    // Left row k=1, v=10, rt=5000; no right buffered → no immediate match.
    assert_eq!(
        joiner
            .push_left(join_batch(vec![1], vec![10], vec![5000]), None)
            .unwrap()
            .num_rows(),
        0
    );
    // Watermark below the eviction point: not yet evicted, nothing emitted.
    assert_eq!(joiner.advance(5000).unwrap().num_rows(), 0);
    // Watermark at/above 5000 - (-1000) = 6000: the left row is evicted unmatched → [left+null]
    // (append-only, so no $row_kind$ column — just the padded row).
    let out = joiner.advance(6000).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 1), vec![10]); // left v
    assert!(out.column(3).is_null(0)); // right k nulled
    assert!(out.column(4).is_null(0)); // right v nulled
}

// LEFT interval join: a left row that matches a right row is emitted as a pair and not
// null-padded at eviction.
#[test]
fn interval_left_join_matched_row_not_padded() {
    let mut joiner = left_interval_joiner(-1000, 1000);
    joiner.push_left(join_batch(vec![1], vec![10], vec![5000]), None);
    // Right row k=1, rt=5000 within [rt-1000, rt+1000] of the left → emits the matched pair.
    let out = joiner
        .push_right(join_batch(vec![1], vec![100], vec![5000]), None)
        .unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 4), vec![100]);
    // Evict the left row: it matched, so no null-pad.
    assert_eq!(joiner.advance(10000).unwrap().num_rows(), 0);
}

// The match flags survive a checkpoint: a restored LEFT interval joiner does not re-pad a left
// row that matched before the snapshot.
#[test]
fn interval_left_join_match_flags_survive_restore() {
    let mut joiner = left_interval_joiner(-1000, 1000);
    joiner.push_left(join_batch(vec![1], vec![10], vec![5000]), None);
    joiner.push_right(join_batch(vec![1], vec![100], vec![5000]), None); // marks the left row matched
    let snapshot = joiner.snapshot();
    let mut restored = IntervalJoiner::restore(
        vec![0],
        vec![0],
        2,
        2,
        -1000,
        1000,
        None,
        JoinKind::LeftOuter,
        interval_schema(),
        interval_schema(),
        &snapshot,
    );
    // Evicting the (matched) left row post-restore must emit no null-pad.
    assert_eq!(restored.advance(10000).unwrap().num_rows(), 0);
}

// Raw keyed state can merge key groups that originated on different subtasks. Outer-join row ids
// are subtask-local, so two such groups may each contain id zero; their matched flags must remain
// attached to the row from their original key group after restore.
#[test]
fn interval_outer_raw_state_remaps_subtask_local_row_ids() {
    let mut matched = left_interval_joiner(-1000, 1000);
    let _ = matched.push_left(join_batch(vec![1], vec![10], vec![5000]), None);
    let _ = matched.push_right(join_batch(vec![1], vec![100], vec![5000]), None);
    let matched_partitions = matched.snapshot_partitions(128, &[-1]);
    assert_eq!(matched_partitions.len(), 1);

    let mut unmatched = left_interval_joiner(-1000, 1000);
    let _ = unmatched.push_left(join_batch(vec![2], vec![20], vec![5000]), None);
    let unmatched_partitions = unmatched.snapshot_partitions(128, &[-1]);
    assert_eq!(unmatched_partitions.len(), 1);
    assert_ne!(
        matched_partitions.keys().next(),
        unmatched_partitions.keys().next(),
        "test keys need distinct raw key groups"
    );

    let snapshots = matched_partitions
        .into_values()
        .chain(unmatched_partitions.into_values())
        .collect::<Vec<_>>();
    let mut restored = IntervalJoiner::restore_partitions(
        vec![0],
        vec![0],
        2,
        2,
        -1000,
        1000,
        None,
        JoinKind::LeftOuter,
        interval_schema(),
        interval_schema(),
        &snapshots,
    );
    let out = restored.advance(10_000).unwrap();
    assert_eq!(out.num_rows(), 1);
    assert_eq!(values(&out, 0), vec![2]);
    assert_eq!(values(&out, 1), vec![20]);
    assert!(out.column(4).is_null(0));
}

// ROW_NUMBER over (PARTITION BY key0 ORDER BY rt): a per-key counter in rowtime order, surviving
// across update calls (the unbounded frame).
#[test]
fn window_function_row_number_counts_per_key() {
    let batch = |rt: Vec<i64>, key0: Vec<i64>| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("rt", DataType::Int64, false),
                Field::new("key0", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(rt)),
                Arc::new(Int64Array::from(key0)),
            ],
        )
        .unwrap()
    };
    let mut over = WindowFunctionOver::new(vec![10]); // ROW_NUMBER
                                                      // Out of rowtime order within the batch: ROW_NUMBER follows rowtime, emitted in input order.
    assert_eq!(
        values(&over.update(&batch(vec![0, 1000, 0], vec![1, 1, 2])), 0),
        vec![1, 2, 1]
    );
    // The counter continues per key across calls.
    assert_eq!(
        values(&over.update(&batch(vec![2000, 1000], vec![1, 2])), 0),
        vec![3, 2]
    );
}

// RANK and DENSE_RANK over (ORDER BY rt): tied rowtimes share a rank; RANK leaves gaps after a
// tie (next jumps to the row position), DENSE_RANK does not.
#[test]
fn window_function_rank_and_dense_rank_handle_ties() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("rt", DataType::Int64, false),
            Field::new("key0", DataType::Int64, false),
        ])),
        // One key, rowtimes 10, 10 (tie), 20, 30.
        vec![
            Arc::new(Int64Array::from(vec![10i64, 10, 20, 30])),
            Arc::new(Int64Array::from(vec![1i64, 1, 1, 1])),
        ],
    )
    .unwrap();
    let mut rank = WindowFunctionOver::new(vec![11]); // RANK
    assert_eq!(values(&rank.update(&batch), 0), vec![1, 1, 3, 4]);
    let mut dense = WindowFunctionOver::new(vec![12]); // DENSE_RANK
    assert_eq!(values(&dense.update(&batch), 0), vec![1, 1, 2, 3]);
}

// Decoder over the pre-order encoding: CALL gt ( INPUT_REF a , LIT_LONG 5 ).
#[test]
fn filters_column_greater_than_literal() {
    let mut expression = FilterExpression {
        kinds: vec![6, 0, 1],
        payload: vec![10, 0, 0],
        child_counts: vec![2, 0, 0],
        longs: vec![5],
        doubles: vec![],
        strings: vec![],
        compiled: None,
    };
    let out = expression.filter(sample_batch());
    assert_eq!(values(&out, 0), vec![6, 9]);
}

// Arithmetic inside the predicate: CALL gt ( CALL plus ( INPUT_REF a , INPUT_REF b ) , LIT 10 ).
#[test]
fn filters_arithmetic_predicate() {
    let mut expression = FilterExpression {
        kinds: vec![6, 6, 0, 0, 1],
        payload: vec![10, 0, 0, 1, 0],
        child_counts: vec![2, 2, 0, 0, 0],
        longs: vec![10],
        doubles: vec![],
        strings: vec![],
        compiled: None,
    };
    let out = expression.filter(sample_batch());
    assert_eq!(values(&out, 0), vec![1, 3, 9]);
}

// An int32 literal keeps the arithmetic in int32, so `v * 2` wraps on overflow like the host
// rather than widening: CALL gt ( CALL times ( INPUT_REF v , LIT_INT 2 ) , LIT_INT 50 ).
#[test]
fn integer_arithmetic_wraps_in_declared_width() {
    let v: ArrayRef = Arc::new(Int32Array::from(vec![30i32, 2_000_000_000]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)])),
        vec![v],
    )
    .unwrap();
    let mut expression = FilterExpression {
        kinds: vec![6, 6, 0, 7, 7],
        payload: vec![10, 2, 0, 0, 1],
        child_counts: vec![2, 2, 0, 0, 0],
        longs: vec![2, 50],
        doubles: vec![],
        strings: vec![],
        compiled: None,
    };
    let out = expression.filter(batch);
    let kept = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    // 30*2=60 > 50 keeps 30; 2e9*2 overflows int32 to a negative value, excluded.
    assert_eq!(kept.values(), &[30]);
}

// The native sink writes a batch to Parquet; reading it back yields the same rows.
#[test]
#[cfg(feature = "parquet")]
fn writes_and_reads_parquet() {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let batch = sample_batch();
    let path = std::env::temp_dir().join("streamfusion_parquet_roundtrip.parquet");
    let path = path.to_str().unwrap();
    write_parquet(&batch, path);

    let file = std::fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut rows = 0usize;
    let mut first = Vec::new();
    for read in reader {
        let read = read.unwrap();
        let column = read
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        first.extend_from_slice(column.values());
        rows += read.num_rows();
    }
    assert_eq!(rows, batch.num_rows());
    assert_eq!(first, values(&batch, 0));
}

fn ab_batch() -> RecordBatch {
    let a: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30]));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ])),
        vec![a, b],
    )
    .unwrap()
}

// A Calc with no condition projects computed columns: [a + b, a].
#[test]
fn calc_projects_computed_columns() {
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 0, 0],
        payload: vec![0, 0, 1, 0], // CALL(+), col a, col b; col a
        child_counts: vec![2, 0, 0, 0],
        longs: vec![],
        doubles: vec![],
        strings: vec![],
        projection_roots: vec![0, 3],
        condition_root: -1,
        output_names: vec!["sum".to_string(), "a".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(ab_batch());
    assert_eq!(out.schema().field(0).name(), "sum");
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[11, 22, 33]
    );
    assert_eq!(
        out.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 2, 3]
    );
}

// A Calc filters by the condition (a > 2), then projects the survivors.
#[test]
fn calc_filters_then_projects() {
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 1, 0],
        payload: vec![10, 0, 0, 0], // CALL(>), col a, lit; col a
        child_counts: vec![2, 0, 0, 0],
        longs: vec![2],
        doubles: vec![],
        strings: vec![],
        projection_roots: vec![3],
        condition_root: 0,
        output_names: vec!["a".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(ab_batch());
    assert_eq!(out.num_rows(), 1);
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[3]
    );
}

// A Calc projects a field pulled out of a ROW/struct column (kind 13 → get_field), the Nexmark
// view shape (`bid.price`).
#[test]
fn calc_extracts_struct_field() {
    let auction: ArrayRef = Arc::new(Int64Array::from(vec![100, 101, 102]));
    let price: ArrayRef = Arc::new(Int64Array::from(vec![99, 40, 200]));
    let bid = StructArray::from(vec![
        (
            Arc::new(Field::new("auction", DataType::Int64, true)),
            auction,
        ),
        (Arc::new(Field::new("price", DataType::Int64, true)), price),
    ]);
    let et: ArrayRef = Arc::new(Int64Array::from(vec![2, 2, 2]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("event_type", DataType::Int64, true),
            Field::new("bid", bid.data_type().clone(), true),
        ])),
        vec![et, Arc::new(bid)],
    )
    .unwrap();

    let mut calc = CalcExpression {
        kinds: vec![13, 0],  // FIELD_ACCESS("price"), col bid
        payload: vec![0, 1], // strings[0]="price"; bid is column 1
        child_counts: vec![1, 0],
        longs: vec![],
        doubles: vec![],
        strings: vec![Some("price".to_string())],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["price".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    assert_eq!(out.schema().field(0).name(), "price");
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[99, 40, 200]
    );
}

// A Calc projects SQL subscripts (kind 19 → ITEM): `nums[1]` over an ARRAY column and
// `tags['a']` over a MAP column, both NULL for an empty/null collection or an absent key.
#[test]
fn calc_subscripts_array_and_map() {
    use arrow::array::{Int64Builder, MapBuilder};
    let nums = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(vec![
        Some(vec![Some(10), Some(20)]),
        Some(vec![]),
        None,
    ]);
    let mut tags = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    tags.keys().append_value("a");
    tags.values().append_value(5);
    tags.keys().append_value("b");
    tags.values().append_value(6);
    tags.append(true).unwrap();
    tags.keys().append_value("b");
    tags.values().append_value(7);
    tags.append(true).unwrap();
    tags.append(false).unwrap();
    let tags = tags.finish();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("nums", nums.data_type().clone(), true),
            Field::new("tags", tags.data_type().clone(), true),
        ])),
        vec![Arc::new(nums), Arc::new(tags)],
    )
    .unwrap();

    let mut calc = CalcExpression {
        // Root 0: ITEM(col nums, lit-int 1); root 1: ITEM(col tags, lit-string "a").
        kinds: vec![19, 0, 7, 19, 0, 3],
        payload: vec![0, 0, 0, 0, 1, 0],
        child_counts: vec![2, 0, 0, 2, 0, 0],
        longs: vec![1],
        doubles: vec![],
        strings: vec![Some("a".to_string())],
        projection_roots: vec![0, 3],
        condition_root: -1,
        output_names: vec!["first_num".to_string(), "tag_a".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    let first_num = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(first_num.value(0), 10);
    assert!(first_num.is_null(1) && first_num.is_null(2));
    let tag_a = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(tag_a.value(0), 5);
    assert!(tag_a.is_null(1) && tag_a.is_null(2));
}

// A Calc projecting a mixed-case top-level column (INPUT_REF) must resolve it by its exact name;
// `col()` would lower-case "dateTime" to "datetime" and fail to compile (the Nexmark q0/q1 rowtime).
#[test]
fn calc_projects_mixed_case_column() {
    let value: ArrayRef = Arc::new(Int64Array::from(vec![5, 7, 9]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "dateTime",
            DataType::Int64,
            true,
        )])),
        vec![value],
    )
    .unwrap();
    let mut calc = CalcExpression {
        kinds: vec![0],
        payload: vec![0],
        child_counts: vec![0],
        longs: vec![],
        doubles: vec![],
        strings: vec![],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["dateTime".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    assert_eq!(out.schema().field(0).name(), "dateTime");
    assert_eq!(
        out.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[5, 7, 9]
    );
}

// SPLIT_INDEX(url, '/', 3) over the Calc path: 0-based whole-separator split, NULL out of range /
// for an empty input / for a null argument (Flink's splitByWholeSeparatorPreserveAllTokens).
#[test]
fn calc_split_index_matches_flink() {
    let url: ArrayRef = Arc::new(StringArray::from(vec![
        Some("http://h/a/b"),
        Some("x"),
        Some(""),
        None,
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("url", DataType::Utf8, true)])),
        vec![url],
    )
    .unwrap();
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 3, 7],    // CALL(SPLIT_INDEX), col url, lit "/", lit 3
        payload: vec![85, 0, 0, 0], // op 85; col 0; strings[0]; longs[0]
        child_counts: vec![3, 0, 0, 0],
        longs: vec![3],
        doubles: vec![],
        strings: vec![Some("/".to_string())],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["dir".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "a"); // ["http:","","h","a","b"][3]
    assert!(col.is_null(1)); // ["x"] has no index 3
    assert!(col.is_null(2)); // empty input -> no tokens
    assert!(col.is_null(3)); // null url
}

// DATE_FORMAT(ts, '%Y-%m-%d') over the Calc path: formats the timestamp's UTC wall-clock, NULL for
// a null input (the JVM encoder supplies the chrono pattern).
#[test]
fn calc_date_format_matches_flink() {
    let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
        Some(0),
        Some(86_400_000),
        None,
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        )])),
        vec![ts],
    )
    .unwrap();
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 3],    // CALL(DATE_FORMAT), col ts, lit "%Y-%m-%d"
        payload: vec![86, 0, 0], // op 86; col 0; strings[0]
        child_counts: vec![2, 0, 0],
        longs: vec![],
        doubles: vec![],
        strings: vec![Some("%Y-%m-%d".to_string())],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["d".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "1970-01-01");
    assert_eq!(col.value(1), "1970-01-02");
    assert!(col.is_null(2));
}

// EXTRACT(HOUR FROM ts) over the Calc path (q14's HOUR): the integer field of the timestamp's UTC
// wall-clock, NULL for a null input. epoch 0 = 1970-01-01T00:00 (hour 0); 86_400_000 + 3_600_000 =
// 1970-01-02T01:00 (hour 1).
#[test]
fn calc_extract_hour_matches_flink() {
    let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
        Some(0),
        Some(86_400_000 + 3_600_000),
        None,
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        )])),
        vec![ts],
    )
    .unwrap();
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 3],    // CALL(EXTRACT), col ts, lit "hour"
        payload: vec![89, 0, 0], // op 89; col 0; strings[0]
        child_counts: vec![2, 0, 0],
        longs: vec![],
        doubles: vec![],
        strings: vec![Some("hour".to_string())],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["h".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(col.value(0), 0);
    assert_eq!(col.value(1), 1);
    assert!(col.is_null(2));
}

#[test]
fn calc_regexp_extract_matches_flink() {
    let url: ArrayRef = Arc::new(StringArray::from(vec![
        Some("channel_id=apple&x=1"),       // matches at ^, group 2 = "apple"
        Some("https://h?a=1&channel_id=9"), // matches after &, group 2 = "9"
        Some("no channel here"),            // no match -> NULL
        None,                               // null input -> NULL
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("url", DataType::Utf8, true)])),
        vec![url],
    )
    .unwrap();
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 3, 7], // CALL(REGEXP_EXTRACT), col url, lit pattern, lit 2
        payload: vec![88, 0, 0, 0], // op 88; col 0; strings[0]; longs[0]
        child_counts: vec![3, 0, 0, 0],
        longs: vec![2],
        doubles: vec![],
        strings: vec![Some("(&|^)channel_id=([^&]*)".to_string())],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["channel_id".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(col.value(0), "apple");
    assert_eq!(col.value(1), "9");
    assert!(col.is_null(2));
    assert!(col.is_null(3));
}

// TIMESTAMP - INTERVAL arithmetic (q7's join residual): a day-time interval literal subtracted
// from a timestamp yields a timestamp (millis - millis), NULL for a null input.
#[test]
fn calc_timestamp_minus_interval() {
    let ts: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![Some(10_000), None]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        )])),
        vec![ts],
    )
    .unwrap();
    let mut calc = CalcExpression {
        kinds: vec![6, 0, 15],  // CALL(MINUS), col ts, INTERVAL literal
        payload: vec![1, 0, 0], // op 1 (MINUS); col 0; longs[0]
        child_counts: vec![2, 0, 0],
        longs: vec![5_000], // 5 seconds
        doubles: vec![],
        strings: vec![],
        projection_roots: vec![0],
        condition_root: -1,
        output_names: vec!["earlier".to_string()],
        compiled: None,
    };
    let out = calc.evaluate(batch);
    let col = out
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(col.value(0), 5_000); // 10s - 5s
    assert!(col.is_null(1));
}

// The by-key split sends every row with the same key to the same partition and preserves all
// rows, for any partition count.
#[test]
fn partitions_a_batch_by_key() {
    use std::collections::HashMap;
    let n = 1000usize;
    let key: ArrayRef = Arc::new(Int64Array::from(
        (0..n as i64).map(|i| i % 37).collect::<Vec<_>>(),
    ));
    let value: ArrayRef = Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>()));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![key, value],
    )
    .unwrap();

    for num_partitions in [1usize, 3, 8] {
        let parts = partition_batch(&batch, &[0], &[-1], num_partitions, num_partitions);
        let mut rows = 0usize;
        let mut key_to_partition: HashMap<i64, usize> = HashMap::default();
        for (partition, sub) in &parts {
            assert!(*partition < num_partitions);
            let keys = sub.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..sub.num_rows() {
                // Each key is consistently assigned to one partition.
                let prev = key_to_partition.insert(keys.value(i), *partition);
                if let Some(p) = prev {
                    assert_eq!(
                        p,
                        *partition,
                        "key {} split across partitions",
                        keys.value(i)
                    );
                }
            }
            rows += sub.num_rows();
        }
        assert_eq!(
            rows, n,
            "all rows preserved for {num_partitions} partitions"
        );
    }
}

// The compiled predicate is cached after the first batch and reused.
#[test]
fn compiles_once_and_reuses() {
    let mut expression = FilterExpression {
        kinds: vec![6, 0, 1],
        payload: vec![12, 0, 0],
        child_counts: vec![2, 0, 0],
        longs: vec![5],
        doubles: vec![],
        strings: vec![],
        compiled: None,
    };
    let first = expression.filter(sample_batch());
    assert!(expression.compiled.is_some());
    let second = expression.filter(sample_batch());
    assert_eq!(values(&first, 0), values(&second, 0));
    assert_eq!(values(&first, 0), vec![1, 3]);
}

// The digit-writing fast plan renders byte-identically to chrono's own formatter across the
// admitted patterns, including 4-digit-year edges the plan pads (year 500) and the out-of-range
// years it must hand back to chrono.
#[test]
fn date_format_fast_plan_matches_chrono() {
    use std::fmt::Write as _;
    let patterns = ["%Y-%m-%d", "%H:%M", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"];
    let millis: Vec<i64> = vec![
        0,
        -1,
        -62_135_596_800_000, // year 0001
        -46_388_649_600_000, // year 0500 — zero-padded 4-digit year
        1_700_000_000_123,
        253_402_300_799_999, // year 9999 upper edge
        253_402_300_800_000, // year 10000 — must fall back to chrono
    ];
    let mut compiled = CompiledFormat::new();
    let mut buf = String::new();
    for pattern in patterns {
        let items = chrono::format::StrftimeItems::new(pattern)
            .parse_to_owned()
            .expect("pattern");
        for &t in &millis {
            let wall = chrono::DateTime::from_timestamp_millis(t)
                .expect("timestamp")
                .naive_utc();
            compiled
                .format_into(&mut buf, wall, pattern)
                .expect("format");
            let mut expected = String::new();
            write!(expected, "{}", wall.format_with_items(items.iter())).expect("chrono render");
            assert_eq!(buf, expected, "pattern {pattern} at {t}ms");
        }
    }
}

// A panic escaping a skip-mode decode must not leave the thread's panics silenced: the marker
// resets on unwind, so the next unexpected failure still reaches the panic hook.
#[test]
fn skip_mode_silencing_resets_after_an_escaping_panic() {
    let escaped = std::panic::catch_unwind(|| silence_expected_decode_panics(|| panic!("boom")));
    assert!(escaped.is_err());
    assert!(!IN_SKIP_DECODE.with(std::cell::Cell::get));
}

// The driver-init handshake fills the vtable only for an ABI version this library speaks — anything
// else is refused, which the connector treats as "stay on the JVM-mediated decode" — and fills only
// the requested version's prefix: a version-1 caller's struct ends at the version-1 fields.
#[test]
fn format_driver_init_gates_on_version() {
    extern "C" fn sentinel(_: i64, _: i64, _: i64, _: i64, _: i64) -> i32 {
        99
    }
    extern "C" fn error_sentinel(_: i64, _: *mut i32) -> *const u8 {
        std::ptr::null()
    }
    let mut driver = FormatDriver {
        decode_body_batch: sentinel,
        decode_last_error: error_sentinel,
    };
    assert_ne!(
        streamfusion_format_driver_init(FORMAT_DRIVER_VERSION_2 + 1, &mut driver),
        0
    );
    assert_eq!(driver.decode_body_batch as usize, sentinel as usize);
    assert_eq!(
        streamfusion_format_driver_init(FORMAT_DRIVER_VERSION_1, &mut driver),
        0
    );
    assert_ne!(driver.decode_body_batch as usize, sentinel as usize);
    assert_eq!(driver.decode_last_error as usize, error_sentinel as usize);
    assert_eq!(
        streamfusion_format_driver_init(FORMAT_DRIVER_VERSION_2, &mut driver),
        0
    );
    assert_ne!(driver.decode_last_error as usize, error_sentinel as usize);
    assert_ne!(
        streamfusion_format_driver_init(FORMAT_DRIVER_VERSION_1, std::ptr::null_mut()),
        0
    );
}

// A decode failure crossing the C ABI keeps its cause: the nonzero return is followed by the
// version-2 error channel serving the format's real panic text, which the connector folds into
// the failure it raises with the bucket's topic/partition/offsets.
#[test]
fn c_abi_decode_failure_serves_the_panic_text() {
    use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    extern "C" fn sentinel(_: i64, _: i64, _: i64, _: i64, _: i64) -> i32 {
        99
    }
    extern "C" fn error_sentinel(_: i64, _: *mut i32) -> *const u8 {
        std::ptr::null()
    }
    let mut driver = FormatDriver {
        decode_body_batch: sentinel,
        decode_last_error: error_sentinel,
    };
    assert_eq!(
        streamfusion_format_driver_init(FORMAT_DRIVER_VERSION_2, &mut driver),
        0
    );

    let decoder = MessageDecoder::new(FORMAT_JSON, json_schema(), "", "", 0, false, "");
    let body = bodies(vec![Some(b"{\"id\": not json")]);
    let mut in_array = FFI_ArrowArray::empty();
    let mut in_schema = FFI_ArrowSchema::empty();
    export_record_batch(
        body,
        &mut in_array as *mut FFI_ArrowArray as i64,
        &mut in_schema as *mut FFI_ArrowSchema as i64,
    );
    let mut out_array = FFI_ArrowArray::empty();
    let mut out_schema = FFI_ArrowSchema::empty();
    let rc = silence_expected_decode_panics(|| {
        (driver.decode_body_batch)(
            &decoder as *const MessageDecoder as i64,
            &mut in_array as *mut FFI_ArrowArray as i64,
            &mut in_schema as *mut FFI_ArrowSchema as i64,
            &mut out_array as *mut FFI_ArrowArray as i64,
            &mut out_schema as *mut FFI_ArrowSchema as i64,
        )
    });
    assert_ne!(rc, 0);

    let mut len = 0i32;
    let pointer = (driver.decode_last_error)(&decoder as *const MessageDecoder as i64, &mut len);
    assert!(!pointer.is_null());
    let message =
        std::str::from_utf8(unsafe { std::slice::from_raw_parts(pointer, len as usize) }).unwrap();
    assert!(
        message.contains("failed to decode JSON record"),
        "error channel must carry the decode's own panic text, got: {message}"
    );
}

// ---------------------------------------------------------------------------------------------
// persistent group state: read-through probes, barrier commits, restore, rescale.
// Everything runs on the vortex file format — the production configuration.
// ---------------------------------------------------------------------------------------------
