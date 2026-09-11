use crate::*;
/// Decodes a binary "body" batch (one bare protobuf message per row) into typed Arrow, matching Flink's
/// `protobuf` format: each message is the *whole* serialized protobuf (no Confluent framing), parsed
/// against a descriptor the JVM serialized off the generated message class into a `FileDescriptorSet`.
/// `prost-reflect` builds the descriptor pool at open time; the owned decoder walks the wire format
/// straight into Arrow arrays (no per-row `DynamicMessage`) and then reconciles those arrays with
/// Flink's requested table schema.
pub struct ProtobufDecoder {
    message: prost_reflect::MessageDescriptor,
    plan: crate::protobuf_decode::PreparedMessagePlan,
    config: crate::protobuf_decode::PtarsConfig,
    target_schema: Option<SchemaRef>,
    read_defaults: bool,
}

/// Prunes a `FileDescriptorSet` so the root message — and, recursively, the nested message types its
/// kept fields reference — declare only the fields named in `schema` (the query's projected columns).
/// The decoder builds one column per descriptor field and skips wire tags it has no field for, so decoding
/// against the pruned descriptor materializes only the read fields straight from the bytes; the unread
/// ones are skipped on the wire. Fields are matched to the schema by name (Flink maps a proto field to
/// the like-named column). An identity schema (the full row type) prunes nothing.
pub(crate) fn prune_descriptor_set(bytes: &[u8], root_message: &str, schema: &Schema) -> Vec<u8> {
    use prost::Message as _;
    use prost_types::FileDescriptorSet;
    let mut set = FileDescriptorSet::decode(bytes).expect("decode FileDescriptorSet");

    // Walk the schema (which drives what to keep) building, per message full-name, the set of field
    // names to retain; descend into a nested message via the proto field's type_name when the schema
    // field is a Struct. Read-only over `set` here.
    let mut keep: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::default();
    let mut work: Vec<(String, arrow::datatypes::Fields)> = vec![(
        root_message.trim_start_matches('.').to_string(),
        schema.fields().clone(),
    )];
    while let Some((name, fields)) = work.pop() {
        let names: std::collections::HashSet<String> =
            fields.iter().map(|f| f.name().clone()).collect();
        if let Some(descriptor) = find_message(&set, &name) {
            for field in fields.iter() {
                if let DataType::Struct(sub) = field.data_type() {
                    if let Some(proto_field) =
                        descriptor.field.iter().find(|pf| pf.name() == field.name())
                    {
                        if !proto_field.type_name().is_empty() {
                            work.push((
                                proto_field.type_name().trim_start_matches('.').to_string(),
                                sub.clone(),
                            ));
                        }
                    }
                }
            }
        }
        keep.insert(name, names);
    }

    for file in &mut set.file {
        let package = file.package().to_string();
        for message in &mut file.message_type {
            prune_message(message, &qualify(&package, message.name()), &keep);
        }
    }
    set.encode_to_vec()
}

/// Retains only `keep`-listed fields of `message` (and recurses into nested message definitions); a
/// message absent from `keep` is left whole (it is unreferenced after the root is pruned).
pub(crate) fn prune_message(
    message: &mut prost_types::DescriptorProto,
    full_name: &str,
    keep: &std::collections::HashMap<String, std::collections::HashSet<String>>,
) {
    if let Some(fields) = keep.get(full_name) {
        message.field.retain(|f| fields.contains(f.name()));
    }
    for nested in &mut message.nested_type {
        let nested_name = qualify(full_name, nested.name());
        prune_message(nested, &nested_name, keep);
    }
}

/// Finds a message by its fully-qualified name (package + nesting), searching top-level and nested types.
pub(crate) fn find_message<'a>(
    set: &'a prost_types::FileDescriptorSet,
    full_name: &str,
) -> Option<&'a prost_types::DescriptorProto> {
    for file in &set.file {
        let package = file.package();
        for message in &file.message_type {
            if let Some(found) =
                find_message_in(message, &qualify(package, message.name()), full_name)
            {
                return Some(found);
            }
        }
    }
    None
}

pub(crate) fn find_message_in<'a>(
    message: &'a prost_types::DescriptorProto,
    message_full_name: &str,
    target: &str,
) -> Option<&'a prost_types::DescriptorProto> {
    if message_full_name == target {
        return Some(message);
    }
    for nested in &message.nested_type {
        let nested_name = qualify(message_full_name, nested.name());
        if let Some(found) = find_message_in(nested, &nested_name, target) {
            return Some(found);
        }
    }
    None
}

pub(crate) fn qualify(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}

impl ProtobufDecoder {
    /// `descriptor_set` is an encoded protobuf `FileDescriptorSet` (the message's file + its transitive
    /// dependencies); `message_name` is the fully-qualified message type to decode each body as.
    pub fn new(descriptor_set: &[u8], message_name: &str) -> ProtobufDecoder {
        Self::new_with_schema(descriptor_set, message_name, None, false)
    }

    pub(crate) fn new_with_schema(
        descriptor_set: &[u8],
        message_name: &str,
        target_schema: Option<SchemaRef>,
        read_defaults: bool,
    ) -> ProtobufDecoder {
        let pool = prost_reflect::DescriptorPool::decode(descriptor_set)
            .expect("failed to decode protobuf FileDescriptorSet");
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("protobuf message {message_name} not found in descriptor"));
        let plan = crate::protobuf_decode::PreparedMessagePlan::new(&message);
        // ConfluentWirePolicy::Raw (the default) = bare protobuf bytes, which is what Flink's `protobuf`
        // format carries; the Confluent variant (strip magic+id+message-index) would set it here.
        ProtobufDecoder {
            message,
            plan,
            config: crate::protobuf_decode::PtarsConfig::default(),
            target_schema,
            read_defaults,
        }
    }

    #[cfg(test)]
    pub(crate) fn plan_serial(&self) -> usize {
        self.plan.serial()
    }

    /// Decodes the single binary body column into a typed batch (schema derived from the descriptor).
    /// Flink's protobuf converter rejects a null byte array in strict mode; ignore-parse-errors protobuf
    /// tables already stay on Flink, so a tombstone must fail rather than becoming a synthetic null row.
    pub fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        use arrow::array::{Array, BinaryArray};
        let column = bodies
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary body");
        assert_eq!(
            column.null_count(),
            0,
            "protobuf cannot deserialize a null Kafka value"
        );
        let batch = crate::protobuf_decode::binary_array_to_record_batch_prepared(
            column,
            &self.plan,
            &self.config,
        )
        .expect("failed to decode protobuf batch");
        let batch = match &self.target_schema {
            Some(target_schema) => crate::protobuf_decode::align_to_flink_schema(
                &batch,
                &self.message,
                target_schema,
                self.read_defaults,
            )
            .expect("failed to align protobuf batch to Flink schema"),
            None => batch,
        };
        if self.read_defaults {
            return batch;
        }
        let columns = batch
            .columns()
            .iter()
            .cloned()
            .map(null_empty_containers)
            .collect();
        let fields: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .map(nullable_containers)
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to rebuild protobuf batch")
    }
}

/// Rewrites empty ARRAY/MAP values to NULL, recursively, to match Flink's protobuf decode. The wire reader
/// materializes an absent repeated/map field as an empty container, but in proto3 an empty
/// repeated/map field is indistinguishable from an absent one on the wire, and Flink's generated
/// `getXCount() > 0` guard (with its default `read-default-values = false`, the only mode the planner
/// routes natively) leaves the Flink field NULL in both cases — so NULL is the exact decode of every
/// zero-length container, not an approximation. Recursion covers repeated/map fields inside nested
/// messages and inside repeated-message elements. Rebuilt arrays carry the `nullable_containers`
/// field shapes, since its direct Arrow schema declares repeated/map columns non-nullable.
fn null_empty_containers(array: ArrayRef) -> ArrayRef {
    use arrow::array::{ListArray, MapArray, StructArray};
    use arrow::buffer::NullBuffer;
    match array.data_type() {
        DataType::List(_) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("list column");
            let (field, offsets, values, nulls) = list.clone().into_parts();
            let non_empty =
                NullBuffer::from_iter(offsets.windows(2).map(|window| window[1] > window[0]));
            let nulls = NullBuffer::union(nulls.as_ref(), Some(&non_empty));
            Arc::new(ListArray::new(
                nullable_containers(&field),
                offsets,
                null_empty_containers(values),
                nulls,
            ))
        }
        DataType::Map(_, _) => {
            let map = array
                .as_any()
                .downcast_ref::<MapArray>()
                .expect("map column");
            let (field, offsets, entries, nulls, ordered) = map.clone().into_parts();
            let non_empty =
                NullBuffer::from_iter(offsets.windows(2).map(|window| window[1] > window[0]));
            let nulls = NullBuffer::union(nulls.as_ref(), Some(&non_empty));
            let entries = null_empty_containers(Arc::new(entries));
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("map entries")
                .clone();
            Arc::new(MapArray::new(
                nullable_containers(&field),
                offsets,
                entries,
                nulls,
                ordered,
            ))
        }
        DataType::Struct(_) => {
            let strukt = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("struct column");
            let (fields, children, nulls) = strukt.clone().into_parts();
            let fields: Vec<_> = fields.iter().map(nullable_containers).collect();
            let children = children.into_iter().map(null_empty_containers).collect();
            Arc::new(StructArray::new(fields.into(), children, nulls))
        }
        _ => array,
    }
}

/// The field shape `null_empty_containers` produces: every ARRAY/MAP field in the tree marked
/// nullable (they can now hold the NULLs standing in for absent proto fields), everything else as
/// the wire reader declared it.
fn nullable_containers(field: &FieldRef) -> FieldRef {
    use arrow::datatypes::Fields;
    match field.data_type() {
        DataType::List(element) => Arc::new(Field::new(
            field.name(),
            DataType::List(nullable_containers(element)),
            true,
        )),
        DataType::Map(entries, ordered) => Arc::new(Field::new(
            field.name(),
            DataType::Map(nullable_containers(entries), *ordered),
            true,
        )),
        DataType::Struct(children) => {
            let children: Fields = children.iter().map(nullable_containers).collect();
            Arc::new(Field::new(
                field.name(),
                DataType::Struct(children),
                field.is_nullable(),
            ))
        }
        _ => field.clone(),
    }
}

/// Creates a protobuf message decoder (Flink's `protobuf` format: bare message bytes, no framing) and
/// returns an opaque `MessageDecoder` handle, released with `closeDecoder` like any other decoder.
/// `descriptor` is an encoded `FileDescriptorSet` the JVM serialized off the generated message class
/// (the message's `.proto` file + transitive dependencies); `messageName` is the fully-qualified type
/// to decode each body as. When supplied, the imported Arrow schema narrows the descriptor and output
/// columns to the projection selected by the planner; legacy callers may omit it with zero addresses.
pub(crate) fn create_protobuf_decoder<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    descriptor: JByteArray<'local>,
    message_name: JString<'local>,
    read_defaults: jboolean,
    schema_array_address: jlong,
    schema_address: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |env| {
        let descriptor = env
            .convert_byte_array(&descriptor)
            .expect("failed to read proto descriptor");
        let message_name: String = env
            .get_string(&message_name)
            .expect("failed to read message name")
            .into();
        // When the planner pushed a projection into the decode, it exports the narrowed output
        // schema (0/0 otherwise): prune the descriptor so the decoder builds only read columns.
        let target_schema = if schema_array_address != 0 {
            Some(import_record_batch(schema_array_address, schema_address).schema())
        } else {
            None
        };
        let descriptor = match &target_schema {
            Some(schema) if !schema.fields().is_empty() => {
                prune_descriptor_set(&descriptor, &message_name, schema)
            }
            _ => descriptor,
        };
        let decoder = MessageDecoder {
            decoder: Box::new(ProtobufDecoder::new_with_schema(
                &descriptor,
                &message_name,
                target_schema,
                read_defaults != 0,
            )),
            skip_errors: false,
        };
        into_handle(decoder)
    })
}

impl Decoder for ProtobufDecoder {
    fn decode(&self, body: &RecordBatch) -> RecordBatch {
        self.decode(body)
    }
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_format_protobuf_NativeProtobufFormat_createDecoder<
    'local,
>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    descriptor: JByteArray<'local>,
    message_name: JString<'local>,
    read_defaults: jboolean,
    schema_array_address: jlong,
    schema_address: jlong,
) -> jlong {
    create_protobuf_decoder(
        env,
        class,
        descriptor,
        message_name,
        read_defaults,
        schema_array_address,
        schema_address,
    )
}
