use super::*;

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

fn proto_oneof_descriptor_set() -> Vec<u8> {
    use prost_reflect::prost::Message;
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        OneofDescriptorProto,
    };
    let member = |name: &str, number: i32, ty: Type| FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        oneof_index: Some(0),
        ..Default::default()
    };
    let message = DescriptorProto {
        name: Some("Choice".to_string()),
        field: vec![
            member("label", 1, Type::String),
            member("number", 2, Type::Int64),
        ],
        oneof_decl: vec![OneofDescriptorProto {
            name: Some("kind".to_string()),
            ..Default::default()
        }],
        ..Default::default()
    };
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("choice.proto".to_string()),
            package: Some("bench".to_string()),
            message_type: vec![message],
            syntax: Some("proto3".to_string()),
            ..Default::default()
        }],
    }
    .encode_to_vec()
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

// Each body is one bare protobuf message (no framing); the owned decoder reads the wire format
// straight into Arrow arrays, deriving the batch schema from the descriptor.
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
fn protobuf_prepares_once_per_decoder_and_reuses_the_plan() {
    use prost_reflect::prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};

    let descriptor = proto_descriptor_set();
    let message = DescriptorPool::decode(descriptor.as_ref())
        .unwrap()
        .get_message_by_name("bench.Row")
        .unwrap();
    let mut row = DynamicMessage::new(message);
    row.set_field_by_name("id", Value::I64(42));
    row.set_field_by_name("name", Value::String("prepared".to_string()));
    row.set_field_by_name("score", Value::F64(3.5));
    let bytes = row.encode_to_vec();
    let input = bodies(vec![Some(&bytes)]);

    let decoder = ProtobufDecoder::new(&descriptor, "bench.Row");
    let serial = decoder.plan_serial();
    let first = decoder.decode(&input);
    let second = decoder.decode(&input);

    assert_eq!(first, second);
    assert_eq!(decoder.plan_serial(), serial);
}

#[test]
fn protobuf_prepared_plan_matches_direct_decoder_for_complex_fields() {
    use prost_reflect::prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage, MapKey, Value};

    let descriptor = proto_complex_descriptor_set();
    let pool = DescriptorPool::decode(descriptor.as_slice()).unwrap();
    let mut nested = DynamicMessage::new(pool.get_message_by_name("bench.Row").unwrap());
    nested.set_field_by_name("id", Value::I64(1));
    nested.set_field_by_name("name", Value::String("nested".to_string()));
    nested.set_field_by_name("score", Value::F64(2.5));
    let mut message = DynamicMessage::new(pool.get_message_by_name("bench.Complex").unwrap());
    message.set_field_by_name("id", Value::I64(7));
    message.set_field_by_name("nums", Value::List(vec![Value::I64(3), Value::I64(4)]));
    message.set_field_by_name(
        "tags",
        Value::Map([(MapKey::String("x".to_string()), Value::I64(9))].into()),
    );
    message.set_field_by_name("nested", Value::Message(nested));
    let bytes = message.encode_to_vec();
    let input = arrow::array::BinaryArray::from_vec(vec![bytes.as_slice()]);
    let message = pool.get_message_by_name("bench.Complex").unwrap();
    let config = crate::protobuf_decode::PtarsConfig::default();
    let plan = crate::protobuf_decode::PreparedMessagePlan::new(&message);
    let prepared =
        crate::protobuf_decode::binary_array_to_record_batch_prepared(&input, &plan, &config)
            .unwrap();
    let direct =
        crate::protobuf_decode::binary_array_to_record_batch_direct(&input, &message, &config)
            .unwrap();

    assert_eq!(prepared, direct);
}

#[test]
fn protobuf_prepared_decoder_skips_unknown_fields() {
    use prost_reflect::prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};

    let descriptor = proto_descriptor_set();
    let message = DescriptorPool::decode(descriptor.as_ref())
        .unwrap()
        .get_message_by_name("bench.Row")
        .unwrap();
    let mut row = DynamicMessage::new(message);
    row.set_field_by_name("id", Value::I64(7));
    let clean = row.encode_to_vec();
    let mut bytes = clean.clone();
    // Unknown field 99, varint wire type, value 12345.
    bytes.extend_from_slice(&[0x98, 0x06, 0xb9, 0x60]);
    let decoder = ProtobufDecoder::new(&descriptor, "bench.Row");
    let unknown = decoder.decode(&bodies(vec![Some(&bytes)]));
    let expected = decoder.decode(&bodies(vec![Some(&clean)]));

    assert_eq!(unknown, expected);
}

#[test]
fn protobuf_prepared_decoder_rejects_malformed_input() {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    let descriptor = proto_descriptor_set();
    let decoder = ProtobufDecoder::new(&descriptor, "bench.Row");
    for malformed in [&[0x80][..], &[0x00][..]] {
        let input = bodies(vec![Some(malformed)]);
        assert!(catch_unwind(AssertUnwindSafe(|| decoder.decode(&input))).is_err());
    }
}

#[test]
fn protobuf_oneof_keeps_only_the_last_wire_member() {
    let descriptor = proto_oneof_descriptor_set();
    let decoder = ProtobufDecoder::new(&descriptor, "bench.Choice");
    // Each message deliberately contains both members. Generated protobuf decoders clear the
    // earlier member when the later one appears, even though both remain present on the wire.
    let number_last = [0x0a, 0x05, b'f', b'i', b'r', b's', b't', 0x10, 0x07];
    let label_last = [0x10, 0x09, 0x0a, 0x04, b'l', b'a', b's', b't'];
    let output = decoder.decode(&bodies(vec![Some(&number_last), Some(&label_last)]));
    let labels = output
        .column_by_name("label")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let numbers = output
        .column_by_name("number")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!((labels.value(0), numbers.value(0)), ("", 7));
    assert_eq!((labels.value(1), numbers.value(1)), ("last", 0));
}

#[test]
#[should_panic(expected = "protobuf cannot deserialize a null Kafka value")]
fn protobuf_decode_rejects_a_tombstone_like_flink() {
    let descriptor = proto_descriptor_set();
    ProtobufDecoder::new(&descriptor, "bench.Row").decode(&bodies(vec![None]));
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
