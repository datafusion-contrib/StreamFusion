use streamfusion_bridge::prelude::*;
use streamfusion_kafka::ProtobufEncoder;
use streamfusion_protobuf::ProtobufDecoder;

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
