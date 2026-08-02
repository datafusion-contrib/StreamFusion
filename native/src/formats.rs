use crate::*;

// The format-code wire protocol lives in `format_codes` (ungated — connectors dispatch on the
// codes even in builds that compile no format implementation).

/// Decodes a binary "body" batch (one bare protobuf message per row) into typed Arrow, matching Flink's
/// `protobuf` format: each message is the *whole* serialized protobuf (no Confluent framing), parsed
/// against a descriptor the JVM serialized off the generated message class into a `FileDescriptorSet`.
/// `prost-reflect` builds the descriptor pool at open time; `ptars` walks the wire format straight into
/// Arrow arrays (no per-row `DynamicMessage`), deriving the batch schema from the message descriptor.
pub(crate) struct ProtobufDecoder {
    message: prost_reflect::MessageDescriptor,
    config: ptars::PtarsConfig,
}

/// Prunes a `FileDescriptorSet` so the root message — and, recursively, the nested message types its
/// kept fields reference — declare only the fields named in `schema` (the query's projected columns).
/// ptars builds one column per descriptor field and skips wire tags it has no field for, so decoding
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
    let mut work: Vec<(String, arrow::datatypes::Fields)> =
        vec![(root_message.trim_start_matches('.').to_string(), schema.fields().clone())];
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
            if let Some(found) = find_message_in(message, &qualify(package, message.name()), full_name)
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
    pub(crate) fn new(descriptor_set: &[u8], message_name: &str) -> ProtobufDecoder {
        let pool = prost_reflect::DescriptorPool::decode(descriptor_set)
            .expect("failed to decode protobuf FileDescriptorSet");
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("protobuf message {message_name} not found in descriptor"));
        // ConfluentWirePolicy::Raw (the default) = bare protobuf bytes, which is what Flink's `protobuf`
        // format carries; the Confluent variant (strip magic+id+message-index) would set it here.
        ProtobufDecoder { message, config: ptars::PtarsConfig::default() }
    }

    /// Decodes the single binary body column into a typed batch (schema derived from the descriptor).
    /// Flink's protobuf converter rejects a null byte array in strict mode; ignore-parse-errors protobuf
    /// tables already stay on Flink, so a tombstone must fail rather than becoming a synthetic null row.
    pub(crate) fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        use arrow::array::{Array, BinaryArray};
        let column = bodies.column(0).as_any().downcast_ref::<BinaryArray>().expect("binary body");
        assert_eq!(
            column.null_count(),
            0,
            "protobuf cannot deserialize a null Kafka value"
        );
        let batch = ptars::binary_array_to_record_batch_direct(column, &self.message, &self.config)
            .expect("failed to decode protobuf batch");
        let columns = batch.columns().iter().cloned().map(null_empty_containers).collect();
        let fields: Vec<_> = batch.schema().fields().iter().map(nullable_containers).collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to rebuild protobuf batch")
    }
}

/// Rewrites empty ARRAY/MAP values to NULL, recursively, to match Flink's protobuf decode. ptars
/// materializes an absent repeated/map field as an empty container, but in proto3 an empty
/// repeated/map field is indistinguishable from an absent one on the wire, and Flink's generated
/// `getXCount() > 0` guard (with its default `read-default-values = false`, the only mode the planner
/// routes natively) leaves the Flink field NULL in both cases — so NULL is the exact decode of every
/// zero-length container, not an approximation. Recursion covers repeated/map fields inside nested
/// messages and inside repeated-message elements. Rebuilt arrays carry the `nullable_containers`
/// field shapes, since ptars declares repeated/map columns non-nullable.
fn null_empty_containers(array: ArrayRef) -> ArrayRef {
    use arrow::array::{ListArray, MapArray, StructArray};
    use arrow::buffer::NullBuffer;
    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().expect("list column");
            let (field, offsets, values, nulls) = list.clone().into_parts();
            let non_empty = NullBuffer::from_iter(
                offsets.windows(2).map(|window| window[1] > window[0]),
            );
            let nulls = NullBuffer::union(nulls.as_ref(), Some(&non_empty));
            Arc::new(ListArray::new(
                nullable_containers(&field),
                offsets,
                null_empty_containers(values),
                nulls,
            ))
        }
        DataType::Map(_, _) => {
            let map = array.as_any().downcast_ref::<MapArray>().expect("map column");
            let (field, offsets, entries, nulls, ordered) = map.clone().into_parts();
            let non_empty = NullBuffer::from_iter(
                offsets.windows(2).map(|window| window[1] > window[0]),
            );
            let nulls = NullBuffer::union(nulls.as_ref(), Some(&non_empty));
            let entries = null_empty_containers(Arc::new(entries));
            let entries = entries.as_any().downcast_ref::<StructArray>().expect("map entries").clone();
            Arc::new(MapArray::new(nullable_containers(&field), offsets, entries, nulls, ordered))
        }
        DataType::Struct(_) => {
            let strukt = array.as_any().downcast_ref::<StructArray>().expect("struct column");
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
/// ptars declared it.
fn nullable_containers(field: &FieldRef) -> FieldRef {
    use arrow::datatypes::Fields;
    match field.data_type() {
        DataType::List(element) => Arc::new(
            Field::new(field.name(), DataType::List(nullable_containers(element)), true),
        ),
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

/// Decodes Flink's `raw` format: each message body is the single column's value verbatim. Strings
/// and bytes pass through (the plan gate admits only UTF-8 `raw.charset` values, so string bodies are
/// already in the column's encoding); BOOLEAN reads one byte as `!= 0`; the fixed-width numerics read
/// an exact-length buffer with the table's `raw.endianness`, failing the job on a wrong-length
/// message with Flink's own error text. 1:1 with the input rows — a null body stays a null field.
pub(crate) struct RawDecoder {
    schema: SchemaRef,
    little_endian: bool,
}

impl RawDecoder {
    fn new(schema: SchemaRef, little_endian: bool) -> RawDecoder {
        RawDecoder { schema, little_endian }
    }

    fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        use arrow::datatypes::{Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type};
        let body = bodies.column(0);
        let column = match self.schema.field(0).data_type() {
            DataType::Boolean => self.decode_booleans(body),
            DataType::Int8 => {
                self.fixed::<Int8Type, 1>(body, "TINYINT", i8::from_be_bytes, i8::from_le_bytes)
            }
            DataType::Int16 => {
                self.fixed::<Int16Type, 2>(body, "SMALLINT", i16::from_be_bytes, i16::from_le_bytes)
            }
            DataType::Int32 => {
                self.fixed::<Int32Type, 4>(body, "INT", i32::from_be_bytes, i32::from_le_bytes)
            }
            DataType::Int64 => {
                self.fixed::<Int64Type, 8>(body, "BIGINT", i64::from_be_bytes, i64::from_le_bytes)
            }
            DataType::Float32 => {
                self.fixed::<Float32Type, 4>(body, "FLOAT", f32::from_be_bytes, f32::from_le_bytes)
            }
            DataType::Float64 => {
                self.fixed::<Float64Type, 8>(body, "DOUBLE", f64::from_be_bytes, f64::from_le_bytes)
            }
            // Strings must be validated: Flink passes the bytes through unvalidated
            // (StringData.fromBytes) but Arrow strings cannot hold invalid UTF-8, so the recorded
            // divergence (docs/coverage-and-fallbacks.md) is a loud decode failure — never the
            // silent NULL a safe cast would produce.
            target @ DataType::Utf8 => {
                let strict = arrow::compute::CastOptions {
                    safe: false,
                    ..arrow::compute::CastOptions::default()
                };
                arrow::compute::cast_with_options(body, target, &strict).unwrap_or_else(|e| {
                    panic!("raw format STRING message is not valid UTF-8 ({e})")
                })
            }
            target => arrow::compute::cast(body, target).expect("failed to cast raw column"),
        };
        RecordBatch::try_new(self.schema.clone(), vec![column]).expect("failed to build raw batch")
    }

    fn decode_booleans(&self, body: &ArrayRef) -> ArrayRef {
        let mut builder = arrow::array::BooleanBuilder::with_capacity(body.len());
        for row in 0..body.len() {
            match binary_body(body, row) {
                None => builder.append_null(),
                Some(bytes) => builder.append_value(exact::<1>(bytes, "BOOLEAN")[0] != 0),
            }
        }
        Arc::new(builder.finish())
    }

    fn fixed<T: arrow::datatypes::ArrowPrimitiveType, const N: usize>(
        &self,
        body: &ArrayRef,
        type_name: &str,
        from_be: fn([u8; N]) -> T::Native,
        from_le: fn([u8; N]) -> T::Native,
    ) -> ArrayRef {
        let convert = if self.little_endian { from_le } else { from_be };
        let mut builder = arrow::array::PrimitiveBuilder::<T>::with_capacity(body.len());
        for row in 0..body.len() {
            match binary_body(body, row) {
                None => builder.append_null(),
                Some(bytes) => builder.append_value(convert(exact::<N>(bytes, type_name))),
            }
        }
        Arc::new(builder.finish())
    }
}

/// Flink's raw-format length check with its exact `DeserializationException` text: a fixed-width
/// value must arrive as exactly `N` bytes or the job fails.
fn exact<const N: usize>(bytes: &[u8], type_name: &str) -> [u8; N] {
    bytes.try_into().unwrap_or_else(|_| {
        panic!("Size of data received for deserializing {type_name} type is not {N}.")
    })
}

/// Reads row `row` of a binary "body" column as bytes, or `None` if the column is null there. Shared by
/// the JSON/CSV decoders, which accept a Binary, LargeBinary, or Utf8 body column.
pub(crate) fn binary_body(column: &ArrayRef, row: usize) -> Option<&[u8]> {
    use arrow::array::{Array, BinaryArray, LargeBinaryArray, StringArray};
    match column.data_type() {
        DataType::Binary => {
            let a = column.as_any().downcast_ref::<BinaryArray>().unwrap();
            a.is_valid(row).then(|| a.value(row))
        }
        DataType::LargeBinary => {
            let a = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            a.is_valid(row).then(|| a.value(row))
        }
        DataType::Utf8 => {
            let a = column.as_any().downcast_ref::<StringArray>().unwrap();
            a.is_valid(row).then(|| a.value(row).as_bytes())
        }
        other => panic!("unsupported body column type {other:?}"),
    }
}

/// Which CDC changelog JSON dialect an envelope is in. The wire codec is plain JSON either way; the
/// dialect fixes the two image fields, the operation field, the op-code → action mapping, and how the
/// pre-image is recovered (see `CdcShape`). Debezium/OGG and Maxwell are scalar (one image per message);
/// Canal (`data`/`old` arrays — a message fans out per element) is a follow-up.
#[derive(Clone, Copy)]
pub(crate) enum CdcDialect {
    /// Debezium JSON: `{before, after, op}`, op ∈ {`c`/`r` → insert, `u` → update, `d` → delete}.
    /// Mirrors `DebeziumJsonDeserializationSchema` (`r` is a snapshot read, treated as an insert).
    Debezium,
    /// Oracle GoldenGate JSON: `{before, after, op_type}`, op ∈ {`I` → insert, `U` → update,
    /// `D` → delete, `T` truncate → skipped}. Mirrors `OggJsonDeserializationSchema`.
    Ogg,
    /// Maxwell JSON: `{data, old, type}`, type ∈ {`insert`, `update`, `delete`}. `data` is the full
    /// post-image, `old` a *partial* pre-image (only changed fields); delete carries the row in `data`.
    /// Mirrors `MaxwellJsonDeserializationSchema`.
    Maxwell,
    /// Canal JSON: `{data, old, type}` where `data`/`old` are *arrays* of rows (one message fans out
    /// per element), type ∈ {`INSERT`, `UPDATE`, `DELETE`, `CREATE` (DDL → skipped)}. Same partial-`old`
    /// merge as Maxwell, applied per element pair. Mirrors `CanalJsonDeserializationSchema`.
    Canal,
}

/// A CDC envelope's change action, before fanning out to physical rows. An update emits two rows
/// (UPDATE_BEFORE + UPDATE_AFTER); insert/delete emit one.
pub(crate) enum CdcAction {
    Insert,
    Update,
    Delete,
}

impl CdcAction {
    fn name(&self) -> &'static str {
        match self {
            CdcAction::Insert => "INSERT",
            CdcAction::Update => "UPDATE",
            CdcAction::Delete => "DELETE",
        }
    }
}

/// What to do with one envelope row's operation. `Skip` is a deliberate no-op Flink also drops (Canal's
/// `CREATE` DDL); `Unknown` is an unrecognized op, which Flink *fails the job* on by default — we match
/// that (rather than silently dropping the row) so the result is identical, and only the planner's
/// fallback gate lets `ignore-parse-errors` tables (which skip) run on Flink instead.
pub(crate) enum CdcOp {
    Change(CdcAction),
    Skip,
    Unknown,
}

/// How a dialect lays out its pre/post images, which determines how UPDATE_BEFORE and DELETE rows are
/// built.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum CdcShape {
    /// Debezium/OGG: `before` is the full pre-image and `after` the full post-image. DELETE reads
    /// `before`; an update's UPDATE_BEFORE is `before` verbatim — a null `before` skips the record
    /// (Flink throws; we match `ignore-parse-errors`).
    BeforeAfter,
    /// Maxwell/Canal: `data` is the full post-image and `old` a *partial* pre-image (only changed
    /// fields). DELETE reads `data` (it holds the deleted row); an update's UPDATE_BEFORE reads a
    /// field from `old` when its KEY is present there — at ANY depth, since Flink's `findValue`
    /// searches the whole subtree: an explicit top-level null (a field changed *to* null) keeps
    /// the null, and so does a key found only inside a nested container (the top-level decode saw
    /// nothing) — and copies `data` only when the key appears nowhere. Reproduced by a per-message
    /// key scan of the raw `old` JSON (the decoded image alone can't distinguish present-null from
    /// absent).
    DataOld,
}

/// The fixed per-dialect envelope layout the decoder reads.
pub(crate) struct CdcSpec {
    /// JSON field holding the pre-image (`before` / `old`) — envelope column 0.
    before_field: &'static str,
    /// JSON field holding the post-image (`after` / `data`) — envelope column 1.
    after_field: &'static str,
    /// JSON field holding the operation — envelope column 2.
    op_field: &'static str,
    shape: CdcShape,
    /// Whether the images are JSON *arrays* of rows (Canal) rather than single rows: one message then
    /// fans out per element, pairing `data[i]` with `old[i]`.
    arrays: bool,
}

impl CdcDialect {
    /// The dialect a CDC format code selects — the only thing that differs between the CDC arms of
    /// [`MessageDecoder::new`].
    fn for_format(format: i32) -> CdcDialect {
        match format {
            FORMAT_DEBEZIUM_JSON => CdcDialect::Debezium,
            FORMAT_OGG_JSON => CdcDialect::Ogg,
            FORMAT_MAXWELL_JSON => CdcDialect::Maxwell,
            FORMAT_CANAL_JSON => CdcDialect::Canal,
            other => panic!("format code {other} is not a CDC format"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            CdcDialect::Debezium => "Debezium",
            CdcDialect::Ogg => "Ogg",
            CdcDialect::Maxwell => "Maxwell",
            CdcDialect::Canal => "Canal",
        }
    }

    fn spec(self) -> CdcSpec {
        match self {
            CdcDialect::Debezium => CdcSpec {
                before_field: "before",
                after_field: "after",
                op_field: "op",
                shape: CdcShape::BeforeAfter,
                arrays: false,
            },
            CdcDialect::Ogg => CdcSpec {
                before_field: "before",
                after_field: "after",
                op_field: "op_type",
                shape: CdcShape::BeforeAfter,
                arrays: false,
            },
            CdcDialect::Maxwell => CdcSpec {
                before_field: "old",
                after_field: "data",
                op_field: "type",
                shape: CdcShape::DataOld,
                arrays: false,
            },
            CdcDialect::Canal => CdcSpec {
                before_field: "old",
                after_field: "data",
                op_field: "type",
                shape: CdcShape::DataOld,
                arrays: true,
            },
        }
    }

    /// Classifies an op string. An unrecognized op is `Unknown` (Flink throws on it by default — see
    /// `CdcOp`); Canal's `CREATE` is a `Skip` (Flink drops DDL). Mirrors each `*JsonDeserializationSchema`.
    fn classify(self, op: &str) -> CdcOp {
        match self {
            CdcDialect::Debezium => match op {
                "c" | "r" => CdcOp::Change(CdcAction::Insert),
                "u" => CdcOp::Change(CdcAction::Update),
                "d" => CdcOp::Change(CdcAction::Delete),
                _ => CdcOp::Unknown,
            },
            CdcDialect::Ogg => match op {
                "I" => CdcOp::Change(CdcAction::Insert),
                "U" => CdcOp::Change(CdcAction::Update),
                "D" => CdcOp::Change(CdcAction::Delete),
                _ => CdcOp::Unknown, // including "T" truncate, which Flink treats as an unknown op
            },
            CdcDialect::Maxwell => match op {
                "insert" => CdcOp::Change(CdcAction::Insert),
                "update" => CdcOp::Change(CdcAction::Update),
                "delete" => CdcOp::Change(CdcAction::Delete),
                _ => CdcOp::Unknown,
            },
            CdcDialect::Canal => match op {
                "INSERT" => CdcOp::Change(CdcAction::Insert),
                "UPDATE" => CdcOp::Change(CdcAction::Update),
                "DELETE" => CdcOp::Change(CdcAction::Delete),
                "CREATE" => CdcOp::Skip, // a DDL change event Flink drops
                _ => CdcOp::Unknown,
            },
        }
    }
}

/// Appends the output row(s) for one change-event unit (one envelope row, or one array element for
/// Canal): `before_idx`/`after_idx` are the rows to read in the pre/post-image struct arrays (equal for
/// scalar dialects; distinct flattened indices for Canal). An update fans out to UPDATE_BEFORE +
/// UPDATE_AFTER. A null image where the dialect requires one fails the job, exactly where Flink's
/// deserializer hits a NullPointerException and reports a corrupt message: a null pre-image on a
/// `BeforeAfter` update/delete (the REPLICA IDENTITY case), a null `old` on a `DataOld` update, and a
/// null row wherever the emitted row would read from.
#[allow(clippy::too_many_arguments)]
pub(crate) fn cdc_emit(
    action: &CdcAction,
    before_idx: usize,
    after_idx: usize,
    shape: CdcShape,
    old_presence: u128,
    before: &StructArray,
    after: &StructArray,
    out: &mut Vec<(i8, usize, usize, RowSource)>,
) {
    let require = |image: &StructArray, idx: usize, name: &str| {
        if !image.is_valid(idx) {
            panic!("CDC {action} has a null {name} image", action = action.name());
        }
    };
    match action {
        CdcAction::Insert => {
            require(after, after_idx, "post");
            out.push((0, before_idx, after_idx, RowSource::After));
        }
        CdcAction::Update => {
            let before_source = match shape {
                CdcShape::BeforeAfter => {
                    require(before, before_idx, "\"before\"/pre (REPLICA IDENTITY not FULL?)");
                    RowSource::Before
                }
                CdcShape::DataOld => {
                    require(before, before_idx, "\"old\"/pre");
                    RowSource::Coalesce(old_presence)
                }
            };
            require(after, after_idx, "post");
            out.push((1, before_idx, after_idx, before_source));
            out.push((2, before_idx, after_idx, RowSource::After));
        }
        CdcAction::Delete => match shape {
            CdcShape::BeforeAfter => {
                require(before, before_idx, "\"before\"/pre (REPLICA IDENTITY not FULL?)");
                out.push((3, before_idx, after_idx, RowSource::Before));
            }
            // Maxwell/Canal: the deleted row lives in the post-image (`data`).
            CdcShape::DataOld => {
                require(after, after_idx, "post");
                out.push((3, before_idx, after_idx, RowSource::After));
            }
        },
    }
}

/// Which image an output row reads its columns from. `Coalesce` (Maxwell/Canal UPDATE_BEFORE) reads a
/// field from the pre-image when its key appears anywhere under the message's `old` (Flink's
/// recursive `findValue` presence rule — an explicit null is present, a key absent from the whole
/// subtree copies the post-image) — a per-field choice, so it can't share one gather index across
/// columns. The bitmask holds bit `i` for physical field `i` present in `old` (the arity is capped
/// at 128, enforced at construction).
#[derive(Clone, Copy)]
pub(crate) enum RowSource {
    /// The pre-image (`before` / `old`), envelope column 0.
    Before,
    /// The post-image (`after` / `data`), envelope column 1.
    After,
    /// Per field: pre-image where its key is present in `old`, else post-image.
    Coalesce(u128),
}

/// Decodes a scalar CDC changelog JSON format (Debezium/OGG/Maxwell) straight to a columnar changelog
/// batch: the physical columns plus a trailing `$row_kind$` byte, with one input message fanning out to
/// 0–2 output rows (an update becomes UPDATE_BEFORE + UPDATE_AFTER; a tombstone/empty message, zero).
/// An unknown op or a null pre-image on an update/delete *fails* (Flink's default throw), never a silent
/// drop — matching Flink's default mode; with `ignore-parse-errors` the wrapping [`MessageDecoder`]
/// isolates each message and turns those failures into per-message skips, matching Flink's skip mode.
/// This mirrors Flink's `*JsonDeserializationSchema` — decode the envelope to a row, then emit the
/// physical row(s) by op with a `RowKind` — but vectorized: every body's envelope is decoded in one
/// `arrow-json` pass, then each physical column is gathered with a single `interleave` choosing the
/// right pre/post-image struct child per output row. RisingWave's row-at-a-time `DebeziumChangeEvent`
/// (`access_field(before/after)` + an `Ops` array) is the reference; this is its batch form, where
/// `$row_kind$` is our columnar `RowKind` (divergences/13). It feeds the existing native changelog
/// operators, so a CDC → GROUP BY/join/Top-N pipeline materializes zero rows end to end.
pub(crate) struct CdcJsonDecoder {
    /// Decodes the envelope: the pre/post images as nested structs of the physical columns (made
    /// nullable, since the absent side / unchanged fields are null), plus the op field as Utf8.
    /// Envelope fields not in this schema (`source`, `ts_ms`, `database`, …) are ignored.
    envelope: JsonDecoder,
    /// Output schema: the physical columns (nullable) + trailing `$row_kind$` Int8.
    output: SchemaRef,
    /// Number of physical columns (envelope/output arity excludes op and `$row_kind$`).
    arity: usize,
    /// Physical column names, for the `old`-key presence scan of the `DataOld` dialects.
    field_names: Vec<String>,
    dialect: CdcDialect,
    /// Flink's `ignore-parse-errors`, handled here (not by a generic wrapper) because the CDC skip
    /// has three granularities at once: a structurally bad message drops whole, a bad value inside
    /// an image nulls just that field (the inner JSON schema gets the flag), and a failure while
    /// fanning a message out KEEPS the rows emitted before it — Flink's deserializers accumulate
    /// into a list and collect whatever it holds after the catch.
    skip_errors: bool,
}

impl CdcJsonDecoder {
    fn new(
        physical: SchemaRef,
        dialect: CdcDialect,
        env: crate::json::JsonEnv,
        skip_errors: bool,
    ) -> CdcJsonDecoder {
        let spec = dialect.spec();
        // The images are null on the absent side / for unchanged fields, so the nested physical fields
        // must be nullable regardless of the table's declared nullability.
        let nullable: Fields = physical
            .fields()
            .iter()
            .map(|f| Arc::new(f.as_ref().clone().with_nullable(true)))
            .collect();
        let image = DataType::Struct(nullable.clone());
        // Canal wraps each image in a JSON array of rows.
        let image = if spec.arrays {
            DataType::List(Arc::new(Field::new("item", image, true)))
        } else {
            image
        };
        // Column 0 = pre-image, 1 = post-image, 2 = op (arrow-json matches the JSON keys by name).
        let envelope = Arc::new(Schema::new(vec![
            Field::new(spec.before_field, image.clone(), true),
            Field::new(spec.after_field, image, true),
            Field::new(spec.op_field, DataType::Utf8, true),
        ]));
        let mut output_fields: Vec<FieldRef> = nullable.iter().cloned().collect();
        output_fields.push(Arc::new(Field::new(ROW_KIND_COLUMN, DataType::Int8, false)));
        let output = Arc::new(Schema::new(output_fields));
        if spec.shape == CdcShape::DataOld {
            assert!(nullable.len() <= 128, "the old-key presence bitmask carries up to 128 columns");
        }
        // A CDC envelope never fans a top-level array out the way the plain `json` format does:
        // Maxwell/Canal hand the root to the tree converter (any array is corrupt), while
        // Debezium/OGG decode through Flink's deprecated one-row entry, which unwraps an array
        // holding exactly one envelope — see `ArrayRootPolicy`.
        let array_roots = match dialect {
            CdcDialect::Debezium | CdcDialect::Ogg => ArrayRootPolicy::UnwrapSingle,
            CdcDialect::Maxwell | CdcDialect::Canal => ArrayRootPolicy::Corrupt,
        };
        CdcJsonDecoder {
            envelope: JsonDecoder::single_object(
                envelope,
                crate::json::JsonEnv {
                    mode: env.mode,
                    lenient: skip_errors,
                    tree_duplicates: env.tree_duplicates,
                },
                array_roots,
            ),
            output,
            arity: nullable.len(),
            field_names: physical.fields().iter().map(|f| f.name().clone()).collect(),
            dialect,
            skip_errors,
        }
    }

    /// Emits one envelope row's — one message's — output rows. Any failure in here is the
    /// message's own corruption (unknown op, null image, uneven Canal arrays), which fails the job
    /// in default mode and is caught per message in skip mode, keeping the rows already emitted.
    #[allow(clippy::too_many_arguments)]
    fn emit_message(
        &self,
        row: usize,
        spec: &CdcSpec,
        presence: &[u128],
        envelope: &RecordBatch,
        before: &StructArray,
        after: &StructArray,
        out_rows: &mut Vec<(i8, usize, usize, RowSource)>,
    ) {
        use arrow::array::ListArray;
        let ops = envelope.column(2).as_any().downcast_ref::<StringArray>().expect("op string");
        // A missing op field is malformed; Flink fails on it (NPE caught → rethrown). Match that.
        let op = if ops.is_valid(row) {
            ops.value(row)
        } else {
            panic!("CDC message has no operation field");
        };
        let action = match self.dialect.classify(op) {
            CdcOp::Change(action) => action,
            CdcOp::Skip => return,
            // Flink throws on an unrecognized op by default; we fail too (never drop it silently).
            CdcOp::Unknown => panic!("unknown CDC operation \"{op}\""),
        };
        let mask = presence.get(row).copied().unwrap_or(0);
        if spec.arrays {
            let after_list = envelope.column(1).as_any().downcast_ref::<ListArray>().unwrap();
            let before_list = envelope.column(0).as_any().downcast_ref::<ListArray>().unwrap();
            if after_list.is_null(row) {
                // Flink reads row.getArray(0) unconditionally for a change op — a null `data`
                // is a corrupt message, not an empty fan-out.
                panic!("CDC {} has no \"data\" array", action.name());
            }
            let (after_off, after_len) =
                (after_list.value_offsets()[row] as usize, after_list.value_length(row) as usize);
            let (before_off, before_len) =
                (before_list.value_offsets()[row] as usize, before_list.value_length(row) as usize);
            for i in 0..after_len {
                // Canal pairs data[i] with old[i]. Flink indexes `old` unchecked for an UPDATE, so
                // a shorter (or absent) `old` array is a corrupt message there; other ops never
                // read it.
                let before_idx = match action {
                    CdcAction::Update if i >= before_len => {
                        panic!("CDC UPDATE \"old\" array is shorter than \"data\"")
                    }
                    _ if i < before_len => before_off + i,
                    _ => after_off + i,
                };
                cdc_emit(
                    &action,
                    before_idx,
                    after_off + i,
                    spec.shape,
                    mask,
                    before,
                    after,
                    out_rows,
                );
            }
        } else {
            cdc_emit(&action, row, row, spec.shape, mask, before, after, out_rows);
        }
    }

    /// Enforces Flink's CDC tombstone rule on the body batch before anything decodes it: the CDC
    /// deserializers skip only a null or ZERO-LENGTH message, so a whitespace-only body is not a
    /// tombstone — it reaches Jackson, yields no envelope row, and the op read NPEs: a corrupt
    /// message (job failure in default mode, a whole-message drop under `ignore-parse-errors`).
    /// The plain JSON decode underneath has its own whitespace rule (it drops such a body without
    /// error, per its Flink parity), so the CDC granularity is applied here, never inherited.
    fn strip_tombstones(&self, bodies: &RecordBatch) -> RecordBatch {
        let column = bodies.column(0);
        let mut kept: Vec<u32> = Vec::with_capacity(bodies.num_rows());
        for row in 0..bodies.num_rows() {
            match binary_body(column, row) {
                None | Some([]) => {}
                Some(bytes) if bytes.iter().all(u8::is_ascii_whitespace) => {
                    if !self.skip_errors {
                        panic!(
                            "Corrupt {} JSON message '{}'.",
                            self.dialect.name(),
                            String::from_utf8_lossy(bytes)
                        );
                    }
                }
                Some(_) => kept.push(row as u32),
            }
        }
        if kept.len() == bodies.num_rows() {
            return bodies.clone();
        }
        let indices = arrow::array::UInt32Array::from(kept);
        let column = take(column, &indices, None).expect("failed to drop CDC tombstones");
        RecordBatch::try_new(bodies.schema(), vec![column])
            .expect("failed to rebuild the CDC body batch")
    }

    /// The `DataOld` presence scan: per surviving body (same null/whitespace skips as the envelope
    /// decode, asserted below), the set of physical field keys Flink's `oldField.findValue` would
    /// find under the message's `old` node — a recursive depth-first search of the whole subtree
    /// (nested objects AND arrays; for Canal the array node's elements fall out of the same
    /// descent). The envelope's `old` is the LAST top-level occurrence, matching the Jackson tree
    /// `root.get` reads. A message without a usable `old` contributes an empty mask (only updates
    /// read it, and a null `old` on an update already failed in `cdc_emit`).
    fn old_key_presence(&self, bodies: &RecordBatch) -> Vec<u128> {
        use simd_json::prelude::*;
        let spec = self.dialect.spec();
        let column = bodies.column(0);
        let mut masks = Vec::with_capacity(bodies.num_rows());
        let mut scratch: Vec<u8> = Vec::new();
        let mut buffers = simd_json::Buffers::default();
        for row in 0..bodies.num_rows() {
            let Some(bytes) = binary_body(column, row) else { continue };
            if bytes.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            scratch.clear();
            scratch.extend_from_slice(bytes);
            let Ok(tape) = simd_json::to_tape_with_buffers(&mut scratch, &mut buffers) else {
                if self.skip_errors {
                    continue; // the lenient envelope decode dropped this message too
                }
                masks.push(0); // the strict envelope decode fails this body first
                continue;
            };
            let root = tape.as_value();
            if self.skip_errors && root.as_object().is_none() {
                continue; // ditto: a non-object root is dropped by the lenient envelope decode
            }
            let old = root
                .as_object()
                .and_then(|envelope| {
                    envelope.iter().filter(|(key, _)| *key == spec.before_field).last()
                })
                .map(|(_, value)| value);
            let mut mask = 0u128;
            if let Some(value) = old {
                self.find_value_mask(value, &mut mask);
            }
            masks.push(mask);
        }
        masks
    }

    /// Jackson `findValue` presence over a tape subtree: sets bit `i` when physical field `i`'s
    /// name appears as an object key anywhere under `value`. A duplicate key within an object
    /// collapses to its LAST occurrence first (Jackson's tree build overwrites the earlier value,
    /// so names reachable only through a discarded subtree are not found).
    fn find_value_mask(&self, value: simd_json::tape::Value<'_, '_>, mask: &mut u128) {
        use simd_json::prelude::*;
        if let Some(object) = value.as_object() {
            let entries: Vec<(&str, simd_json::tape::Value<'_, '_>)> = object.iter().collect();
            for (i, (key, child)) in entries.iter().enumerate() {
                if entries[i + 1..].iter().any(|(later, _)| later == key) {
                    continue;
                }
                if let Some(field) = self.field_names.iter().position(|name| name == key) {
                    *mask |= 1 << field;
                }
                self.find_value_mask(*child, mask);
            }
        } else if let Some(array) = value.as_array() {
            for element in &array {
                self.find_value_mask(element, mask);
            }
        }
    }

    fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        use arrow::array::ListArray;
        let bodies = &self.strip_tombstones(bodies);
        let envelope = self.envelope.decode(bodies);
        if envelope.num_rows() == 0 {
            return RecordBatch::new_empty(self.output.clone());
        }

        let spec = self.dialect.spec();
        let ops = envelope.column(2).as_any().downcast_ref::<StringArray>().expect("op string");

        // The pre/post images as struct arrays the gather reads from. For Canal they are the *flattened*
        // values of the `old`/`data` list columns, and a list's element pairs `old[i]` with `data[i]`;
        // for scalar dialects each envelope row is itself the single unit (pre/post index = the row).
        let (before, after) = if spec.arrays {
            let before_list = envelope.column(0).as_any().downcast_ref::<ListArray>().expect("old list");
            let after_list = envelope.column(1).as_any().downcast_ref::<ListArray>().expect("data list");
            (before_list.values().clone(), after_list.values().clone())
        } else {
            (envelope.column(0).clone(), envelope.column(1).clone())
        };
        let before = before.as_any().downcast_ref::<StructArray>().expect("pre-image struct");
        let after = after.as_any().downcast_ref::<StructArray>().expect("post-image struct");

        // The DataOld dialects need per-message key presence in `old` (Flink's findValue rule);
        // the scan mirrors the envelope decode's skip conditions, asserted here.
        let presence = if spec.shape == CdcShape::DataOld {
            let masks = self.old_key_presence(bodies);
            assert_eq!(masks.len(), envelope.num_rows(), "old-key presence misaligned");
            masks
        } else {
            Vec::new()
        };

        // Per output row: its RowKind byte (0 +I, 1 -U, 2 +U, 3 -D — `RowKind.toByteValue()`), and the
        // rows to read in the pre/post-image struct arrays, and which image to read each column from.
        let mut out_rows: Vec<(i8, usize, usize, RowSource)> = Vec::with_capacity(envelope.num_rows());
        for row in 0..envelope.num_rows() {
            if self.skip_errors {
                // Flink's skip keeps whatever a message emitted before its failure: the
                // deserializer accumulates rows into a list and collects it after the catch, so a
                // Canal fan-out that dies mid-array still emits the earlier elements. out_rows is
                // append-only, so the partial state is exactly that list.
                use std::panic::{catch_unwind, AssertUnwindSafe};
                let _ = silence_expected_decode_panics(|| {
                    catch_unwind(AssertUnwindSafe(|| {
                        self.emit_message(row, &spec, &presence, &envelope, before, after, &mut out_rows)
                    }))
                });
            } else {
                self.emit_message(row, &spec, &presence, &envelope, before, after, &mut out_rows);
            }
        }

        gather_cdc_batch(&out_rows, before, after, self.arity, &self.output)
    }
}

/// Builds the fanned-out changelog batch: each physical column gathered from the pre/post-image
/// struct children per output row, plus the trailing `$row_kind$` bytes. The source is the same
/// across columns except for `Coalesce`, which picks per field by the key's presence in the
/// message's `old` — so the gather index is built per field.
pub(crate) fn gather_cdc_batch(
    out_rows: &[(i8, usize, usize, RowSource)],
    before: &StructArray,
    after: &StructArray,
    arity: usize,
    output: &SchemaRef,
) -> RecordBatch {
    const BEFORE: usize = 0;
    const AFTER: usize = 1;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(arity + 1);
    for field in 0..arity {
        let before_child = before.column(field);
        let after_child = after.column(field);
        let indices: Vec<(usize, usize)> = out_rows
            .iter()
            .map(|&(_, before_idx, after_idx, source)| match source {
                RowSource::Before => (BEFORE, before_idx),
                RowSource::After => (AFTER, after_idx),
                RowSource::Coalesce(present) => {
                    if present & (1u128 << field) != 0 {
                        (BEFORE, before_idx)
                    } else {
                        (AFTER, after_idx)
                    }
                }
            })
            .collect();
        let sources = [before_child.as_ref(), after_child.as_ref()];
        columns.push(
            arrow::compute::interleave(&sources, &indices).expect("failed to gather CDC column"),
        );
    }
    columns.push(Arc::new(Int8Array::from(
        out_rows.iter().map(|&(kind, _, _, _)| kind).collect::<Vec<i8>>(),
    )));
    RecordBatch::try_new(output.clone(), columns).expect("failed to build CDC batch")
}

/// Decodes Flink's `debezium-avro-confluent` format: the same `{before, after, op}` Debezium
/// envelope as the JSON dialect, but with Confluent-framed Avro bodies. The composition mirrors
/// Flink's own deserializer — an ordinary registry-Avro decode of the envelope row type (the
/// physical row nullable as both images, plus the op string), then the Debezium op fan-out — so
/// the envelope decode reuses [`crate::avro::AvroDecoder`] whole (id-keyed writer store, schema
/// evolution mid-batch, reconciliation of the image payloads onto the boundary column types) and
/// the fan-out reuses the JSON dialects' emit/gather machinery. A null or empty message is a
/// tombstone, skipped inside the Avro decode; the format has no `ignore-parse-errors`, so every
/// corruption (unknown op, null pre-image on update/delete) fails the job exactly where Flink's
/// deserializer throws.
pub(crate) struct AvroCdcDecoder {
    /// Decodes and reconciles the envelope: `before`/`after` as nullable structs of the (nullable)
    /// physical columns, `op` as Utf8.
    envelope: crate::avro::AvroDecoder,
    /// Output schema: the physical columns (nullable) + trailing `$row_kind$` Int8.
    output: SchemaRef,
    arity: usize,
}

impl AvroCdcDecoder {
    fn new(physical: SchemaRef, reader: Option<arrow_avro::schema::AvroSchema>) -> AvroCdcDecoder {
        let nullable: Fields = physical
            .fields()
            .iter()
            .map(|f| Arc::new(f.as_ref().clone().with_nullable(true)))
            .collect();
        let image = DataType::Struct(nullable.clone());
        let envelope_target = Arc::new(Schema::new(vec![
            Field::new("before", image.clone(), true),
            Field::new("after", image, true),
            Field::new("op", DataType::Utf8, true),
        ]));
        let mut output_fields: Vec<FieldRef> = nullable.iter().cloned().collect();
        output_fields.push(Arc::new(Field::new(ROW_KIND_COLUMN, DataType::Int8, false)));
        AvroCdcDecoder {
            envelope: crate::avro::AvroDecoder::confluent("", 0, reader, envelope_target)
                .skipping_empty_bodies(),
            output: Arc::new(Schema::new(output_fields)),
            arity: nullable.len(),
        }
    }

    fn register_writer_schema(&mut self, id: u32, schema: &str) {
        self.envelope.register_writer_schema(id, schema);
    }

    fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        let envelope = self.envelope.decode(bodies);
        let before =
            envelope.column(0).as_any().downcast_ref::<StructArray>().expect("pre-image struct");
        let after =
            envelope.column(1).as_any().downcast_ref::<StructArray>().expect("post-image struct");
        let ops = envelope.column(2).as_any().downcast_ref::<StringArray>().expect("op string");
        let mut out_rows: Vec<(i8, usize, usize, RowSource)> =
            Vec::with_capacity(envelope.num_rows());
        for row in 0..envelope.num_rows() {
            let op = if ops.is_valid(row) {
                ops.value(row)
            } else {
                panic!("CDC message has no operation field");
            };
            let action = match CdcDialect::Debezium.classify(op) {
                CdcOp::Change(action) => action,
                CdcOp::Skip => continue,
                CdcOp::Unknown => panic!("unknown CDC operation \"{op}\""),
            };
            cdc_emit(&action, row, row, CdcShape::BeforeAfter, 0, before, after, &mut out_rows);
        }
        gather_cdc_batch(&out_rows, before, after, self.arity, &self.output)
    }
}

/// The single, format-dispatched decode core shared by every ingest path: it turns a batch of one
/// binary column — raw message bodies, one per row — into a typed Arrow batch. JSON goes through
/// the simd-json tape walk (arrow-json for decimal-bearing schemas — see `JsonDecoder`), CSV
/// through `arrow-csv`, Avro (bare or Confluent-framed) through `arrow-avro` against a
/// local schema-id store, protobuf through `prost-reflect`/`ptars`, the CDC changelog formats through
/// `CdcJsonDecoder`, and `raw` is a passthrough. Both the shallow path (Flink polls bytes, hands them
/// here) and the native source (rdkafka polls bytes, hands them here) feed the *same* `MessageDecoder`;
/// only who produces the body batch differs.
///
/// `skip_errors` is Flink's `ignore-parse-errors`: an undecodable message contributes no rows instead
/// of failing the decode. Flink implements it as a catch-everything around each message's decode, so
/// the native equivalent is per-message isolation of the whole pipeline (JSON parse, envelope shape,
/// value conversion) — see [`MessageDecoder::decode`].
pub(crate) struct MessageDecoder {
    pub(crate) decoder: FormatDecoder,
    pub(crate) skip_errors: bool,
}

/// The decode-relevant format options the JVM plumbs through as `key=value` lines (one per line,
/// split on the first `=`): the table's `csv.*` Jackson knobs, the JSON family's
/// `timestamp-format.standard`, and `raw.endianness`. Only options the planner has vetted reach
/// here — anything unsupported already fell back — so an unknown key is a wiring bug, not user input.
pub(crate) struct FormatOptions {
    pub(crate) csv: crate::csv::CsvOptions,
    pub(crate) timestamp_mode: crate::flink_text::TimestampMode,
    pub(crate) raw_little_endian: bool,
    pub(crate) keyed: Option<KeyedSpec>,
}

/// A keyed table's decode composition (Flink's `key.format` on the source side): which physical
/// position the raw-decoded Kafka key fills, which positions the value decode fills (its row type
/// is the physical schema projected to them), and the key's `key.raw.endianness`. Rides the same
/// option lines as the format options so the connector needs no new JNI surface.
#[derive(Clone)]
pub(crate) struct KeyedSpec {
    pub(crate) key_position: usize,
    pub(crate) value_positions: Vec<usize>,
    pub(crate) key_little_endian: bool,
}

pub(crate) fn parse_format_options(encoded: &str) -> FormatOptions {
    let mut csv = crate::csv::CsvOptions::default();
    let mut timestamp_mode = crate::flink_text::TimestampMode::default();
    let mut raw_little_endian = false;
    let mut keyed_key_position = None;
    let mut keyed_value_positions = None;
    let mut keyed_key_little_endian = false;
    for line in encoded.lines().filter(|l| !l.is_empty()) {
        let (key, value) = line.split_once('=').expect("format option is not key=value");
        let single_byte = || -> u8 {
            assert_eq!(value.len(), 1, "format option {key} must be one ASCII char");
            value.as_bytes()[0]
        };
        match key {
            "csv.field-delimiter" => csv.delimiter = single_byte(),
            "csv.quote-character" => csv.quote = Some(single_byte()),
            "csv.disable-quote-character" => csv.quote = None,
            "csv.allow-comments" => csv.comments = true,
            "csv.null-literal" => csv.null_literal = Some(value.to_string()),
            "timestamp-format" => {
                timestamp_mode = match value {
                    "ISO-8601" => crate::flink_text::TimestampMode::Iso8601,
                    "SQL" => crate::flink_text::TimestampMode::Sql,
                    other => panic!("unknown timestamp-format {other}"),
                }
            }
            "raw.endianness" => {
                raw_little_endian = match value {
                    "little-endian" => true,
                    "big-endian" => false,
                    other => panic!("unknown raw.endianness {other}"),
                }
            }
            "keyed.key-position" => {
                keyed_key_position = Some(value.parse::<usize>().expect("keyed.key-position"))
            }
            "keyed.value-positions" => {
                keyed_value_positions = Some(if value.is_empty() {
                    Vec::new()
                } else {
                    value
                        .split(',')
                        .map(|position| position.parse::<usize>().expect("keyed.value-positions"))
                        .collect()
                })
            }
            "keyed.key-endianness" => {
                keyed_key_little_endian = match value {
                    "little-endian" => true,
                    "big-endian" => false,
                    other => panic!("unknown keyed.key-endianness {other}"),
                }
            }
            other => panic!("unknown format option {other}"),
        }
    }
    let keyed = keyed_key_position.map(|key_position| KeyedSpec {
        key_position,
        value_positions: keyed_value_positions.expect("keyed decode carries no value positions"),
        key_little_endian: keyed_key_little_endian,
    });
    FormatOptions { csv, timestamp_mode, raw_little_endian, keyed }
}

pub(crate) enum FormatDecoder {
    Json(JsonDecoder),
    Csv(crate::csv::CsvDecoder),
    Raw(RawDecoder),
    /// Avro, bare or Confluent-framed — see `avro::AvroDecoder`.
    Avro(crate::avro::AvroDecoder),
    Protobuf(ProtobufDecoder),
    /// CDC changelog JSON (Debezium/OGG): envelope → physical rows + `$row_kind$`, fanning out updates.
    Cdc(CdcJsonDecoder),
    /// Debezium envelope with Confluent-framed Avro bodies — see `AvroCdcDecoder`.
    AvroCdc(AvroCdcDecoder),
    /// A keyed table: raw-decoded Kafka keys composed with the value decode — see `KeyedDecoder`.
    Keyed(Box<KeyedDecoder>),
}

/// Composes a keyed table's two decodes the way Flink's key/value merge does. The input batch is
/// two binary columns `[key, body]`. Each record's VALUE decodes alone (its own skip semantics —
/// a JSON body may fan a top-level array into N rows or drop under `ignore-parse-errors`), and
/// the record's key bytes are gathered once per produced row, so every output row carries its
/// record's key — Flink's per-record cartesian with the raw key format's exactly-one key row. The
/// keys then decode through the parity-pinned `RawDecoder` (a null Kafka key stays a row with a
/// NULL key column — raw's special null-key rule) and scatter into the physical schema, value
/// columns written after the key column so an `ALL` projection's value fields win the overlap,
/// exactly `OutputProjectionCollector.emitRow`'s field order.
pub(crate) struct KeyedDecoder {
    value: Box<MessageDecoder>,
    key: RawDecoder,
    key_position: usize,
    value_positions: Vec<usize>,
    output: SchemaRef,
}

impl KeyedDecoder {
    fn decode(&self, records: &RecordBatch) -> RecordBatch {
        let keys = records.column(0);
        let bodies = records
            .project(&[1])
            .expect("keyed decode expects a two-column [key, body] batch");
        let mut kept = Vec::new();
        let mut sources = Vec::with_capacity(records.num_rows());
        for row in 0..records.num_rows() {
            let decoded = self.value.decode(&bodies.slice(row, 1));
            for _ in 0..decoded.num_rows() {
                sources.push(row as i32);
            }
            if decoded.num_rows() > 0 {
                kept.push(decoded);
            }
        }
        let value_schema: SchemaRef = Arc::new(Schema::new(
            self.value_positions
                .iter()
                .map(|position| self.output.field(*position).clone())
                .collect::<Vec<_>>(),
        ));
        let values = match kept.len() {
            0 => RecordBatch::new_empty(value_schema),
            1 => kept.into_iter().next().unwrap(),
            _ => concat_batches(&value_schema, &kept).expect("keyed value concat failed"),
        };
        let indices = Int32Array::from(sources);
        let gathered = take(keys, &indices, None).expect("failed to gather Kafka keys");
        let key_input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("key", gathered.data_type().clone(), true)])),
            vec![gathered],
        )
        .expect("failed to build the key decode batch");
        let key_column = self.key.decode(&key_input).column(0).clone();
        let mut columns: Vec<Option<ArrayRef>> = vec![None; self.output.fields().len()];
        columns[self.key_position] = Some(key_column);
        for (index, position) in self.value_positions.iter().enumerate() {
            columns[*position] = Some(values.column(index).clone());
        }
        let columns = columns
            .into_iter()
            .map(|column| column.expect("keyed decode left a physical column unfilled"))
            .collect();
        RecordBatch::try_new(self.output.clone(), columns)
            .expect("failed to compose the keyed decode batch")
    }
}

impl FormatDecoder {
    fn decode(&self, body: &RecordBatch) -> RecordBatch {
        match self {
            FormatDecoder::Json(decoder) => decoder.decode(body),
            FormatDecoder::Csv(decoder) => decoder.decode(body),
            FormatDecoder::Raw(decoder) => decoder.decode(body),
            FormatDecoder::Avro(decoder) => decoder.decode(body),
            FormatDecoder::Protobuf(decoder) => decoder.decode(body),
            FormatDecoder::Cdc(decoder) => decoder.decode(body),
            FormatDecoder::AvroCdc(decoder) => decoder.decode(body),
            FormatDecoder::Keyed(decoder) => decoder.decode(body),
        }
    }

    /// The output schema an empty skip-mode batch is built with.
    fn output_schema(&self) -> SchemaRef {
        match self {
            FormatDecoder::Json(decoder) => decoder.schema.clone(),
            FormatDecoder::Cdc(decoder) => decoder.output.clone(),
            _ => panic!("skip-mode decode is only wired for JSON and CDC formats"),
        }
    }
}

thread_local! {
    /// Whether the current thread is inside a skip-mode per-message decode (see
    /// [`silence_expected_decode_panics`]).
    pub(crate) static IN_SKIP_DECODE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Marks the current thread as inside a skip-mode per-message decode, silencing the panic hook for
/// the expected decode failures (Flink's `ignore-parse-errors` skips silently; a hook line per bad
/// message would flood the log). The hook replacement happens once, delegating to the previous hook
/// for every panic outside a skip-mode decode.
pub(crate) fn silence_expected_decode_panics<R>(work: impl FnOnce() -> R) -> R {
    use std::cell::Cell;
    use std::sync::Once;
    static INSTALL_HOOK: Once = Once::new();
    INSTALL_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !IN_SKIP_DECODE.with(Cell::get) {
                previous(info);
            }
        }));
    });
    // Reset on drop, not fallthrough: a panic escaping `work` (a failure beyond the expected
    // per-message skips) must not leave the thread's panics permanently silenced.
    struct Unsilence;
    impl Drop for Unsilence {
        fn drop(&mut self) {
            IN_SKIP_DECODE.with(|flag| flag.set(false));
        }
    }
    IN_SKIP_DECODE.with(|flag| flag.set(true));
    let _unsilence = Unsilence;
    work()
}

impl MessageDecoder {
    /// `format` is a `FORMAT_*` code (mirroring `FormatCodes.java`). JSON, CSV, raw, and the CDC
    /// envelopes decode against `output_schema` (CDC treats it as the physical columns); the Avro
    /// variants decode via `avro_schema` (registered at `schema_id` for Confluent, synthetic id 0
    /// for bare) and reconcile the decoded batch onto `output_schema`. (Protobuf is built via
    /// `createProtobufDecoder`, not here.)
    /// `format_options` carries the table's decode-relevant format options (see
    /// [`parse_format_options`]).
    pub(crate) fn new(
        format: i32,
        output_schema: SchemaRef,
        avro_schema: &str,
        reader_avro_schema: &str,
        schema_id: i32,
        skip_errors: bool,
        format_options: &str,
    ) -> MessageDecoder {
        // A non-empty reader schema projects the writer record to a subset of fields (Avro resolution),
        // set when the planner pushes the query's projection into the decode.
        let reader = if reader_avro_schema.is_empty() {
            None
        } else {
            Some(arrow_avro::schema::AvroSchema::new(reader_avro_schema.to_string()))
        };
        let options = parse_format_options(format_options);
        if let Some(keyed) = options.keyed.clone() {
            // A keyed table: build the value decoder against the physical schema projected to the
            // value positions (the keyed option lines stripped so the recursion is plain), and the
            // raw key decoder against the key column; the wrapper owns per-record composition.
            let value_options: String = format_options
                .lines()
                .filter(|line| !line.is_empty() && !line.starts_with("keyed."))
                .map(|line| format!("{line}\n"))
                .collect();
            let value_schema: SchemaRef = Arc::new(Schema::new(
                keyed
                    .value_positions
                    .iter()
                    .map(|position| output_schema.field(*position).clone())
                    .collect::<Vec<_>>(),
            ));
            let value = Box::new(MessageDecoder::new(
                format,
                value_schema,
                avro_schema,
                reader_avro_schema,
                schema_id,
                skip_errors,
                &value_options,
            ));
            let key_schema: SchemaRef = Arc::new(Schema::new(vec![output_schema
                .field(keyed.key_position)
                .clone()]));
            return MessageDecoder {
                decoder: FormatDecoder::Keyed(Box::new(KeyedDecoder {
                    value,
                    key: RawDecoder::new(key_schema, keyed.key_little_endian),
                    key_position: keyed.key_position,
                    value_positions: keyed.value_positions,
                    output: output_schema,
                })),
                skip_errors: false,
            };
        }
        // Every JSON-decoded format handles its own skip granularity (CdcJsonDecoder /
        // JsonDecoder's lenient appenders / CsvDecoder), so the generic per-message retry below
        // only serves a CDC batch whose ENVELOPE decode fails structurally.
        // The CDC dialects decode through Flink's TREE deserializer (readTree), whose duplicate
        // keys collapse last-wins with no field-counter saturation — unlike plain `json`'s
        // parser path.
        let cdc_env = crate::json::JsonEnv {
            mode: options.timestamp_mode,
            lenient: false,
            tree_duplicates: true,
        };
        let decoder = match format {
            FORMAT_AVRO_CONFLUENT => FormatDecoder::Avro(crate::avro::AvroDecoder::confluent(
                avro_schema,
                schema_id as u32,
                reader,
                output_schema,
            )),
            FORMAT_AVRO => {
                FormatDecoder::Avro(crate::avro::AvroDecoder::bare(avro_schema, reader, output_schema))
            }
            // CSV owns its skip mode: Flink's ignore-parse-errors granularity for CSV is per FIELD
            // (a bad value nulls the field, a short row pads, only a record-level failure drops the
            // row), which the generic per-message retry below cannot reproduce.
            FORMAT_CSV => {
                return MessageDecoder {
                    decoder: FormatDecoder::Csv(crate::csv::CsvDecoder::new(
                        output_schema,
                        options.csv,
                        skip_errors,
                    )),
                    skip_errors: false,
                }
            }
            FORMAT_RAW => {
                FormatDecoder::Raw(RawDecoder::new(output_schema, options.raw_little_endian))
            }
            FORMAT_DEBEZIUM_JSON..=FORMAT_CANAL_JSON => FormatDecoder::Cdc(CdcJsonDecoder::new(
                output_schema,
                CdcDialect::for_format(format),
                cdc_env,
                skip_errors,
            )),
            // No skip mode: the format defines no ignore-parse-errors, so a corrupt message always
            // fails the job (Flink's own throw-and-wrap behavior).
            FORMAT_DEBEZIUM_AVRO_CONFLUENT => {
                FormatDecoder::AvroCdc(AvroCdcDecoder::new(output_schema, reader))
            }
            _ => {
                return MessageDecoder {
                    decoder: FormatDecoder::Json(JsonDecoder::new(
                        output_schema,
                        crate::json::JsonEnv {
                            mode: options.timestamp_mode,
                            lenient: skip_errors,
                            tree_duplicates: false,
                        },
                    )),
                    skip_errors: false,
                }
            }
        };
        MessageDecoder { decoder, skip_errors }
    }

    pub(crate) fn decode(&self, body: &RecordBatch) -> RecordBatch {
        if !self.skip_errors {
            return self.decoder.decode(body);
        }
        // `ignore-parse-errors`: Flink wraps each message's whole decode in a catch-everything and
        // skips the message on any failure — malformed JSON, a bad envelope shape, an unconvertible
        // value alike. The native equivalent: decode the batch optimistically, and only when
        // something in it fails, redo it message by message, dropping the messages that fail. The
        // per-message state is fresh each try, so a failed attempt leaves nothing behind.
        use std::panic::{catch_unwind, AssertUnwindSafe};
        silence_expected_decode_panics(|| {
            if let Ok(batch) = catch_unwind(AssertUnwindSafe(|| self.decoder.decode(body))) {
                return batch;
            }
            let mut kept = Vec::new();
            for row in 0..body.num_rows() {
                let single = body.slice(row, 1);
                if let Ok(batch) = catch_unwind(AssertUnwindSafe(|| self.decoder.decode(&single))) {
                    if batch.num_rows() > 0 {
                        kept.push(batch);
                    }
                }
            }
            match kept.len() {
                0 => RecordBatch::new_empty(self.decoder.output_schema()),
                1 => kept.into_iter().next().unwrap(),
                _ => {
                    let schema = kept[0].schema();
                    arrow::compute::concat_batches(&schema, &kept)
                        .expect("skip-mode batch concat failed")
                }
            }
        })
    }

    /// Registers a writer schema under a Confluent schema id, so subsequent decodes resolve messages
    /// framed with that id. Only the Confluent-framed Avro decoder carries an id-keyed store; calling
    /// this on any other format is a wiring bug.
    pub(crate) fn register_writer_schema(&mut self, id: u32, schema: &str) {
        match &mut self.decoder {
            FormatDecoder::Avro(decoder) => decoder.register_writer_schema(id, schema),
            FormatDecoder::AvroCdc(decoder) => decoder.register_writer_schema(id, schema),
            _ => panic!("registerAvroSchema on a non-Confluent-Avro decoder"),
        }
    }
}

/// Creates a format-dispatched message decoder and returns an opaque handle, released with
/// `closeDecoder`. Every format receives the target schema the JVM exports as an empty batch:
/// JSON/CSV/raw decode against it, and the Avro variants reconcile the arrow-avro decode onto it.
/// Formats 1/4 (Confluent/bare Avro) decode via `avroSchema` (registered under `schemaId` for
/// Confluent, synthetic id 0 for bare); a format-1 decoder built with an empty `avroSchema` starts
/// with an empty store — the registry-driven path, where the JVM registers each writer schema by id
/// via `registerAvroSchema` as messages carry it.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createDecoder<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    format: jint,
    schema_array_address: jlong,
    schema_address: jlong,
    avro_schema: JString<'local>,
    reader_avro_schema: JString<'local>,
    schema_id: jint,
    skip_parse_errors: jboolean,
    format_options: JString<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |env| {
        // Every format decodes against (or, for Avro, reconciles onto) the exported target schema.
        // Only the Avro benchmark counting path passes 0/0 — it never exports the decoded batch.
        let schema = if schema_array_address == 0 {
            Arc::new(Schema::empty())
        } else {
            import_record_batch(schema_array_address, schema_address).schema()
        };
        let avro_schema: String = env.get_string(&avro_schema).map(Into::into).unwrap_or_default();
        // Empty unless the planner pushed a projection into an Avro decode: the narrowed reader schema.
        let reader_avro_schema: String =
            env.get_string(&reader_avro_schema).map(Into::into).unwrap_or_default();
        let format_options: String =
            env.get_string(&format_options).map(Into::into).unwrap_or_default();
        into_handle(MessageDecoder::new(
            format,
            schema,
            &avro_schema,
            &reader_avro_schema,
            schema_id,
            skip_parse_errors != 0,
            &format_options,
        ))
    })
}

/// Creates a protobuf message decoder (Flink's `protobuf` format: bare message bytes, no framing) and
/// returns an opaque `MessageDecoder` handle, released with `closeDecoder` like any other decoder.
/// `descriptor` is an encoded `FileDescriptorSet` the JVM serialized off the generated message class
/// (the message's `.proto` file + transitive dependencies); `messageName` is the fully-qualified type
/// to decode each body as. The Arrow batch schema is derived from the descriptor by ptars (no schema
/// C-structs needed, unlike JSON).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createProtobufDecoder<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    descriptor: JByteArray<'local>,
    message_name: JString<'local>,
    schema_array_address: jlong,
    schema_address: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |env| {
        let descriptor = env.convert_byte_array(&descriptor).expect("failed to read proto descriptor");
        let message_name: String = env.get_string(&message_name).expect("failed to read message name").into();
        // When the planner pushed a projection into the decode, it exports the narrowed output schema (0/0
        // otherwise): prune the descriptor to those fields so ptars builds only the read columns.
        let descriptor = if schema_array_address != 0 {
            let schema = import_record_batch(schema_array_address, schema_address).schema();
            prune_descriptor_set(&descriptor, &message_name, &schema)
        } else {
            descriptor
        };
        let decoder = MessageDecoder {
            decoder: FormatDecoder::Protobuf(ProtobufDecoder::new(&descriptor, &message_name)),
            skip_errors: false,
        };
        into_handle(decoder)
    })
}

/// Registers a writer schema under a Confluent schema id on an existing Confluent-Avro decoder. The
/// JVM operator calls this the first time a batch carries an id it hasn't seen: it fetches the schema
/// from the schema registry (as Flink's own `avro-confluent` deserializer does) and feeds it here, so
/// the store grows with the topic's schema evolution instead of being fixed at plan time.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_registerAvroSchema<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    schema_id: jint,
    schema: JString<'local>,
) {
    crate::bridge::jni_guard(env, move |env| {
        let decoder = unsafe { &mut *(handle as *mut MessageDecoder) };
        let schema: String = env.get_string(&schema).expect("failed to read avro schema").into();
        decoder.register_writer_schema(schema_id as u32, &schema);
    })
}

/// Decodes one body batch into a typed batch, exporting it into the consumer-allocated C structs.
/// A decode failure (bad data outside skip mode) surfaces as a Java `RuntimeException` — the task
/// fails the way Flink's own deserializer failure does — rather than unwinding across the JNI
/// boundary, which would abort the whole process.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_decodeInto<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let decoder = unsafe { &*(handle as *mut MessageDecoder) };
    let decoded = catch_unwind(AssertUnwindSafe(|| {
        let bodies = import_record_batch(in_array_address, in_schema_address);
        decoder.decode(&bodies)
    }));
    match decoded {
        Ok(batch) => export_record_batch(batch, out_array_address, out_schema_address),
        Err(panic) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("native decode failed: {}", panic_message(panic)),
            );
        }
    }
}

/// Benchmark-only: decode a body batch and return the decoded row count without exporting the result —
/// so the shallow path can terminate with Arrow in Rust (counted in Rust), symmetric with the native
/// consumer, for an apples-to-apples comparison.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_decodeCount<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let decoder = unsafe { &*(handle as *mut MessageDecoder) };
        let bodies = import_record_batch(in_array_address, in_schema_address);
        decoder.decode(&bodies).num_rows() as jlong
    })
}

/// Releases a message decoder handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeDecoder<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<MessageDecoder>(handle));
        }
    })
}

thread_local! {
    /// The panic text of this thread's most recent failed C-ABI decode, served by
    /// `decode_last_error`. Thread-local rather than per-decoder because the connector reads it
    /// synchronously on the thread that decoded; the success path never touches it.
    static LAST_DECODE_ERROR: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
}

/// This format library's decode behind the cross-DSO driver ABI (`format_abi`): an opaque decoder
/// handle and Arrow C Data addresses, nothing language-specific. A panic is contained and reported
/// as nonzero — its text stashed for `decode_last_error` — and the caller raises the failure on
/// its own JNI surface.
extern "C" fn decode_body_batch(
    handle: i64,
    in_array_address: i64,
    in_schema_address: i64,
    out_array_address: i64,
    out_schema_address: i64,
) -> i32 {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let decoder = unsafe { &*(handle as *mut MessageDecoder) };
        let bodies = import_record_batch(in_array_address, in_schema_address);
        let decoded = decoder.decode(&bodies);
        export_record_batch(decoded, out_array_address, out_schema_address);
    }));
    match outcome {
        Ok(()) => 0,
        Err(payload) => {
            LAST_DECODE_ERROR
                .with(|slot| *slot.borrow_mut() = crate::bridge::panic_message(payload));
            1
        }
    }
}

/// The version-2 error channel: the text of this thread's most recent failed decode. The returned
/// pointer is into a thread-local this library owns; it stays valid until the thread's next failed
/// decode, and the connector copies it out immediately after the nonzero return.
extern "C" fn decode_last_error(_decoder_handle: i64, len_out: *mut i32) -> *const u8 {
    LAST_DECODE_ERROR.with(|slot| {
        let message = slot.borrow();
        unsafe { *len_out = message.len() as i32 };
        message.as_ptr()
    })
}

/// The exported driver init (ADBC's `AdbcDriverInit` pattern): a connector states the ABI version it
/// speaks and passes the matching vtable to fill; this library fills it or refuses with nonzero, and
/// a refusal leaves the caller on the JVM-mediated decode path. Only the requested version's prefix
/// of the vtable is written — an older caller's struct ends there. The connector obtains this
/// function's address through the format's Java facade — by handoff, never by symbol linkage
/// (divergences/25).
#[no_mangle]
pub extern "C" fn streamfusion_format_driver_init(version: i32, driver: *mut FormatDriver) -> i32 {
    if !(FORMAT_DRIVER_VERSION_1..=FORMAT_DRIVER_VERSION_2).contains(&version) || driver.is_null() {
        return 1;
    }
    unsafe {
        (*driver).decode_body_batch = decode_body_batch;
        if version >= FORMAT_DRIVER_VERSION_2 {
            (*driver).decode_last_error = decode_last_error;
        }
    }
    0
}

// Format artifacts expose separate Java facades so the core DSO never owns message-decoder JNI. The
// decoder implementation remains shared in this crate, but the symbols below are compiled only into the
// corresponding extension build; loading a format JAR therefore cannot accidentally make a connector
// or the core artifact provide that format.
//
// Four of the five facade entry points are identical across formats — only the exported class name
// differs — so one macro stamps them per artifact. Each format keeps its own `createDecoder` (and
// avro its schema registration), the one place the Java signatures genuinely diverge.
macro_rules! format_jni_facade {
    (
        $feature:literal,
        $driver_init_address:ident,
        $is_loaded:ident,
        $native_build_version:ident,
        $decode_into:ident,
        $close_decoder:ident
    ) => {
        #[cfg(feature = $feature)]
        #[no_mangle]
        pub extern "system" fn $driver_init_address<'local>(
            env: JNIEnv<'local>,
            _class: JClass<'local>,
        ) -> jlong {
            crate::bridge::jni_guard(env, move |_env| {
                streamfusion_format_driver_init as usize as jlong
            })
        }

        #[cfg(feature = $feature)]
        #[no_mangle]
        pub extern "system" fn $is_loaded<'local>(
            env: JNIEnv<'local>,
            _class: JClass<'local>,
        ) -> jboolean {
            crate::bridge::jni_guard(env, move |_env| 1)
        }

        #[cfg(feature = $feature)]
        #[no_mangle]
        pub extern "system" fn $native_build_version<'local>(
            env: JNIEnv<'local>,
            class: JClass<'local>,
        ) -> jstring {
            crate::bridge::Java_io_github_jordepic_streamfusion_Native_version(env, class)
        }

        #[cfg(feature = $feature)]
        #[no_mangle]
        pub extern "system" fn $decode_into<'local>(
            env: JNIEnv<'local>, class: JClass<'local>, handle: jlong, in_array: jlong,
            in_schema: jlong, out_array: jlong, out_schema: jlong,
        ) {
            Java_io_github_jordepic_streamfusion_Native_decodeInto(
                env, class, handle, in_array, in_schema, out_array, out_schema,
            )
        }

        #[cfg(feature = $feature)]
        #[no_mangle]
        pub extern "system" fn $close_decoder<'local>(
            env: JNIEnv<'local>, class: JClass<'local>, handle: jlong,
        ) {
            Java_io_github_jordepic_streamfusion_Native_closeDecoder(env, class, handle)
        }
    };
}

format_jni_facade!(
    "json",
    Java_io_github_jordepic_streamfusion_format_json_NativeJsonFormat_driverInitAddress,
    Java_io_github_jordepic_streamfusion_format_json_NativeJsonFormat_isLoaded,
    Java_io_github_jordepic_streamfusion_format_json_NativeJsonFormat_nativeBuildVersion,
    Java_io_github_jordepic_streamfusion_format_json_NativeJsonFormat_decodeInto,
    Java_io_github_jordepic_streamfusion_format_json_NativeJsonFormat_closeDecoder
);

format_jni_facade!(
    "csv",
    Java_io_github_jordepic_streamfusion_format_csv_NativeCsvFormat_driverInitAddress,
    Java_io_github_jordepic_streamfusion_format_csv_NativeCsvFormat_isLoaded,
    Java_io_github_jordepic_streamfusion_format_csv_NativeCsvFormat_nativeBuildVersion,
    Java_io_github_jordepic_streamfusion_format_csv_NativeCsvFormat_decodeInto,
    Java_io_github_jordepic_streamfusion_format_csv_NativeCsvFormat_closeDecoder
);

format_jni_facade!(
    "raw",
    Java_io_github_jordepic_streamfusion_format_raw_NativeRawFormat_driverInitAddress,
    Java_io_github_jordepic_streamfusion_format_raw_NativeRawFormat_isLoaded,
    Java_io_github_jordepic_streamfusion_format_raw_NativeRawFormat_nativeBuildVersion,
    Java_io_github_jordepic_streamfusion_format_raw_NativeRawFormat_decodeInto,
    Java_io_github_jordepic_streamfusion_format_raw_NativeRawFormat_closeDecoder
);

format_jni_facade!(
    "avro",
    Java_io_github_jordepic_streamfusion_format_avro_NativeAvroFormat_driverInitAddress,
    Java_io_github_jordepic_streamfusion_format_avro_NativeAvroFormat_isLoaded,
    Java_io_github_jordepic_streamfusion_format_avro_NativeAvroFormat_nativeBuildVersion,
    Java_io_github_jordepic_streamfusion_format_avro_NativeAvroFormat_decodeInto,
    Java_io_github_jordepic_streamfusion_format_avro_NativeAvroFormat_closeDecoder
);

format_jni_facade!(
    "protobuf",
    Java_io_github_jordepic_streamfusion_format_protobuf_NativeProtobufFormat_driverInitAddress,
    Java_io_github_jordepic_streamfusion_format_protobuf_NativeProtobufFormat_isLoaded,
    Java_io_github_jordepic_streamfusion_format_protobuf_NativeProtobufFormat_nativeBuildVersion,
    Java_io_github_jordepic_streamfusion_format_protobuf_NativeProtobufFormat_decodeInto,
    Java_io_github_jordepic_streamfusion_format_protobuf_NativeProtobufFormat_closeDecoder
);

#[cfg(feature = "json")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_json_NativeJsonFormat_createDecoder<'local>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    format: jint,
    schema_array_address: jlong,
    schema_address: jlong,
    skip_parse_errors: jboolean,
    format_options: JString<'local>,
) -> jlong {
    let empty_writer = env.new_string("").expect("empty writer schema");
    let empty_reader = env.new_string("").expect("empty reader schema");
    Java_io_github_jordepic_streamfusion_Native_createDecoder(
        env,
        class,
        format,
        schema_array_address,
        schema_address,
        empty_writer,
        empty_reader,
        0,
        skip_parse_errors,
        format_options,
    )
}

#[cfg(feature = "csv")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_csv_NativeCsvFormat_createDecoder<'local>(
    mut env: JNIEnv<'local>, class: JClass<'local>, schema_array_address: jlong, schema_address: jlong,
    skip_parse_errors: jboolean, format_options: JString<'local>,
) -> jlong {
    let empty_writer = env.new_string("").expect("empty writer schema");
    let empty_reader = env.new_string("").expect("empty reader schema");
    Java_io_github_jordepic_streamfusion_Native_createDecoder(
        env, class, FORMAT_CSV, schema_array_address, schema_address, empty_writer, empty_reader, 0,
        skip_parse_errors, format_options,
    )
}

#[cfg(feature = "raw")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_raw_NativeRawFormat_createDecoder<'local>(
    mut env: JNIEnv<'local>, class: JClass<'local>, schema_array_address: jlong, schema_address: jlong,
    format_options: JString<'local>,
) -> jlong {
    let empty_writer = env.new_string("").expect("empty writer schema");
    let empty_reader = env.new_string("").expect("empty reader schema");
    Java_io_github_jordepic_streamfusion_Native_createDecoder(
        env, class, FORMAT_RAW, schema_array_address, schema_address, empty_writer, empty_reader, 0,
        0, format_options,
    )
}

#[cfg(feature = "avro")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_avro_NativeAvroFormat_createDecoder<'local>(
    mut env: JNIEnv<'local>, class: JClass<'local>, confluent: jboolean, writer_schema: JString<'local>,
    reader_schema: JString<'local>, schema_array_address: jlong, schema_address: jlong,
) -> jlong {
    let empty_options = env.new_string("").expect("empty format options");
    Java_io_github_jordepic_streamfusion_Native_createDecoder(
        env,
        class,
        if confluent != 0 { FORMAT_AVRO_CONFLUENT } else { FORMAT_AVRO },
        schema_array_address,
        schema_address,
        writer_schema,
        reader_schema,
        0,
        0,
        empty_options,
    )
}

#[cfg(feature = "avro")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_avro_NativeAvroFormat_createDebeziumDecoder<'local>(
    mut env: JNIEnv<'local>, class: JClass<'local>, reader_schema: JString<'local>,
    schema_array_address: jlong, schema_address: jlong,
) -> jlong {
    let empty_writer = env.new_string("").expect("empty writer schema");
    let empty_options = env.new_string("").expect("empty format options");
    Java_io_github_jordepic_streamfusion_Native_createDecoder(
        env,
        class,
        FORMAT_DEBEZIUM_AVRO_CONFLUENT,
        schema_array_address,
        schema_address,
        empty_writer,
        reader_schema,
        0,
        0,
        empty_options,
    )
}

#[cfg(feature = "avro")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_avro_NativeAvroFormat_registerWriterSchema<'local>(
    env: JNIEnv<'local>, class: JClass<'local>, handle: jlong, schema_id: jint, schema: JString<'local>,
) {
    Java_io_github_jordepic_streamfusion_Native_registerAvroSchema(env, class, handle, schema_id, schema)
}

#[cfg(feature = "protobuf")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_protobuf_NativeProtobufFormat_createDecoder<'local>(
    env: JNIEnv<'local>, class: JClass<'local>, descriptor: JByteArray<'local>, message_name: JString<'local>,
    schema_array_address: jlong, schema_address: jlong,
) -> jlong {
    Java_io_github_jordepic_streamfusion_Native_createProtobufDecoder(
        env, class, descriptor, message_name, schema_array_address, schema_address,
    )
}

