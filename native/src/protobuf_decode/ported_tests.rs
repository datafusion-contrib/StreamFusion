#[cfg(test)]
mod tests {
    // Ported from ptars' Apache-2.0 decode-side corpus. Reverse-conversion tests are intentionally
    // excluded because StreamFusion owns a separate Flink-exact protobuf encoder.
    use super::super::{
        binary_array_to_record_batch_direct, messages_to_record_batch,
        messages_to_record_batch_with_config, PtarsConfig,
    };
    use arrow::array::Array;
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    };
    use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value};

    fn file_descriptor_proto_fixture() -> FileDescriptorProto {
        FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("TestMessage".to_string()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("id".to_string()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("name".to_string()),
                        number: Some(2),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::String.into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn dynamic_messages_fixture(message_descriptor: &MessageDescriptor) -> Vec<DynamicMessage> {
        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name("id", prost_reflect::Value::I32(1));
        message1.set_field_by_name("name", prost_reflect::Value::String("test".to_string()));

        let mut message2 = DynamicMessage::new(message_descriptor.clone());
        message2.set_field_by_name("id", prost_reflect::Value::I32(2));
        message2.set_field_by_name("name", prost_reflect::Value::String("test2".to_string()));

        vec![message1, message2]
    }

    fn create_pool_with_message(file_descriptor: FileDescriptorProto) -> DescriptorPool {
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file_descriptor).unwrap();
        pool
    }

    #[test]
    fn test_file_descriptor_to_message_descriptor() {
        let file_descriptor_proto = file_descriptor_proto_fixture();
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file_descriptor_proto)
            .unwrap();
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();

        assert_eq!(message_descriptor.name(), "TestMessage");
        assert_eq!(message_descriptor.fields().len(), 2);
        let id_field = message_descriptor.get_field_by_name("id").unwrap();
        assert_eq!(id_field.kind(), prost_reflect::Kind::Int32);
        let name_field = message_descriptor.get_field_by_name("name").unwrap();
        assert_eq!(name_field.kind(), prost_reflect::Kind::String);
    }

    #[test]
    fn test_message_descriptor_fields() {
        let file_descriptor_proto = file_descriptor_proto_fixture();
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file_descriptor_proto)
            .unwrap();
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();

        let id_field = message_descriptor.get_field_by_name("id").unwrap();
        assert_eq!(id_field.number(), 1);
        assert_eq!(id_field.cardinality(), prost_reflect::Cardinality::Optional);

        let name_field = message_descriptor.get_field_by_name("name").unwrap();
        assert_eq!(name_field.number(), 2);
        assert_eq!(
            name_field.cardinality(),
            prost_reflect::Cardinality::Optional
        );
    }

    #[test]
    fn test_dynamic_messages_to_record_batch() {
        let file_descriptor_proto = file_descriptor_proto_fixture();
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file_descriptor_proto)
            .unwrap();
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();
        let messages = dynamic_messages_fixture(&message_descriptor);

        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        assert_eq!(record_batch.num_rows(), 2);
        assert_eq!(record_batch.num_columns(), 2);

        let id_array = record_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(id_array, &arrow::array::Int32Array::from(vec![1, 2]));

        let name_array = record_batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(
            name_array,
            &arrow::array::StringArray::from(vec!["test", "test2"])
        );
    }

    #[test]
    fn invalid_utf8_string_is_rejected() {
        let pool = create_pool_with_message(file_descriptor_proto_fixture());
        let descriptor = pool.get_message_by_name("test.TestMessage").unwrap();
        // field 2 (string), one-byte payload, invalid UTF-8
        let messages = arrow::array::BinaryArray::from(vec![b"\x12\x01\xff".as_slice()]);
        let error =
            binary_array_to_record_batch_direct(&messages, &descriptor, &PtarsConfig::default())
                .unwrap_err();
        assert!(error.to_string().contains("invalid UTF-8"));
    }

    fn create_repeated_message_descriptor(
        field_name: &str,
        field_type: Type,
    ) -> (DescriptorPool, MessageDescriptor) {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("RepeatedMessage".to_string()),
                field: vec![FieldDescriptorProto {
                    name: Some(field_name.to_string()),
                    number: Some(1),
                    label: Some(Label::Repeated.into()),
                    r#type: Some(field_type.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.RepeatedMessage").unwrap();
        (pool, message_descriptor)
    }

    #[test]
    fn test_list_value_name_config() {
        use arrow::datatypes::DataType;
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Int32);

        let mut message = DynamicMessage::new(message_descriptor.clone());
        message.set_field_by_name("values", Value::List(vec![Value::I32(1), Value::I32(2)]));

        // Test with custom list_value_name
        let config = PtarsConfig::default().with_list_value_name("element");
        let record_batch =
            messages_to_record_batch_with_config(&[message], &message_descriptor, &config);

        // Verify the schema has the custom value field name
        let schema = record_batch.schema();
        let list_field = schema.field_with_name("values").unwrap();
        if let DataType::List(value_field) = list_field.data_type() {
            assert_eq!(value_field.name(), "element");
        } else {
            panic!("Expected list type");
        }
    }

    // ==================== Nested Message Tests ====================

    #[test]
    fn test_nested_message_conversion() {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("InnerMessage".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("inner_id".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Int32.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("inner_name".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::String.into()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("OuterMessage".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("outer_id".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Int32.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("inner".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Message.into()),
                            type_name: Some(".test.InnerMessage".to_string()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let outer_descriptor = pool.get_message_by_name("test.OuterMessage").unwrap();
        let inner_descriptor = pool.get_message_by_name("test.InnerMessage").unwrap();

        let mut inner1 = DynamicMessage::new(inner_descriptor.clone());
        inner1.set_field_by_name("inner_id", Value::I32(100));
        inner1.set_field_by_name("inner_name", Value::String("inner_one".to_string()));

        let mut outer1 = DynamicMessage::new(outer_descriptor.clone());
        outer1.set_field_by_name("outer_id", Value::I32(1));
        outer1.set_field_by_name("inner", Value::Message(inner1));

        let messages = vec![outer1];
        let record_batch = messages_to_record_batch(&messages, &outer_descriptor);

        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 2);

        let outer_id = record_batch
            .column_by_name("outer_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(outer_id.value(0), 1);

        let inner_struct = record_batch
            .column_by_name("inner")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        let inner_id = inner_struct
            .column_by_name("inner_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(inner_id.value(0), 100);

        let inner_name = inner_struct
            .column_by_name("inner_name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(inner_name.value(0), "inner_one");
    }

    // ==================== Empty Message Tests ====================

    #[test]
    fn test_empty_message_list() {
        let file_descriptor_proto = file_descriptor_proto_fixture();
        let pool = create_pool_with_message(file_descriptor_proto);
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();

        let messages: Vec<DynamicMessage> = vec![];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);

        assert_eq!(record_batch.num_rows(), 0);
        assert_eq!(record_batch.num_columns(), 2);
    }

    #[test]
    fn test_message_with_empty_fields() {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("EmptyFieldsMessage".to_string()),
                field: vec![],
                ..Default::default()
            }],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.EmptyFieldsMessage").unwrap();

        let message = DynamicMessage::new(message_descriptor.clone());
        let messages = vec![message];

        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 0);
    }

    // ==================== Map Field Tests ====================

    #[test]
    fn test_map_field_conversion() {
        use std::collections::HashMap;
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("MapEntry".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("key".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::String.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("value".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Int32.into()),
                            ..Default::default()
                        },
                    ],
                    options: Some(prost_reflect::prost_types::MessageOptions {
                        map_entry: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("MessageWithMap".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("my_map".to_string()),
                        number: Some(1),
                        label: Some(Label::Repeated.into()),
                        r#type: Some(Type::Message.into()),
                        type_name: Some(".test.MapEntry".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.MessageWithMap").unwrap();

        let mut map_value: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map_value.insert(
            prost_reflect::MapKey::String("key1".to_string()),
            Value::I32(100),
        );
        map_value.insert(
            prost_reflect::MapKey::String("key2".to_string()),
            Value::I32(200),
        );

        let mut message = DynamicMessage::new(message_descriptor.clone());
        message.set_field_by_name("my_map", Value::Map(map_value));

        let messages = vec![message];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);

        assert_eq!(record_batch.num_rows(), 1);

        let map_array = record_batch
            .column_by_name("my_map")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .unwrap();
        assert_eq!(map_array.len(), 1);
        assert_eq!(map_array.value_length(0), 2);
    }

    #[test]
    fn test_map_value_name_config() {
        use arrow::datatypes::DataType;
        use std::collections::HashMap;
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("MapEntry".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("key".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::String.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("value".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Int32.into()),
                            ..Default::default()
                        },
                    ],
                    options: Some(prost_reflect::prost_types::MessageOptions {
                        map_entry: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("MessageWithMap".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("my_map".to_string()),
                        number: Some(1),
                        label: Some(Label::Repeated.into()),
                        r#type: Some(Type::Message.into()),
                        type_name: Some(".test.MapEntry".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.MessageWithMap").unwrap();

        let mut map_value: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map_value.insert(
            prost_reflect::MapKey::String("key1".to_string()),
            Value::I32(100),
        );

        let mut message = DynamicMessage::new(message_descriptor.clone());
        message.set_field_by_name("my_map", Value::Map(map_value));

        // Test with custom map_value_name
        let config = PtarsConfig::default().with_map_value_name("custom_val");
        let record_batch =
            messages_to_record_batch_with_config(&[message], &message_descriptor, &config);

        // Verify the schema has the custom value field name
        let schema = record_batch.schema();
        let map_field = schema.field_with_name("my_map").unwrap();
        if let DataType::Map(entries_field, _) = map_field.data_type() {
            if let DataType::Struct(fields) = entries_field.data_type() {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "key");
                assert_eq!(fields[1].name(), "custom_val");
            } else {
                panic!("Expected struct type for map entries");
            }
        } else {
            panic!("Expected map type");
        }
    }
}
