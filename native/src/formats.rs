use crate::*;

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
        ptars::binary_array_to_record_batch_direct(column, &self.message, &self.config)
            .expect("failed to decode protobuf batch")
    }
}

/// Decodes Flink's `raw` format: the message bytes pass through as a single column. The body is already
/// a binary column, so this just casts it to the target column's type (Binary passthrough, or Binary →
/// Utf8 for a STRING column) and renames it. 1:1 with the input rows (a null stays null).
pub(crate) struct RawDecoder {
    schema: SchemaRef,
}

impl RawDecoder {
    fn new(schema: SchemaRef) -> RawDecoder {
        RawDecoder { schema }
    }

    fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        let target = self.schema.field(0).data_type();
        let column = arrow::compute::cast(bodies.column(0), target).expect("failed to cast raw column");
        RecordBatch::try_new(self.schema.clone(), vec![column]).expect("failed to build raw batch")
    }
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
    /// field from `old` when its KEY is present there (even as an explicit null — a field changed
    /// *to* null keeps the null) and copies `data` otherwise — Flink's `findValue` presence check,
    /// reproduced by a per-message key scan of the raw `old` JSON (the decoded image alone can't
    /// distinguish present-null from absent).
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
/// field from the pre-image when its key appears under the message's `old` (Flink's `findValue`
/// presence rule — an explicit null is present, an absent key copies the post-image) — a per-field
/// choice, so it can't share one gather index across columns. The bitmask holds bit `i` for physical
/// field `i` present in `old` (the arity is capped at 128, enforced at construction).
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
        CdcJsonDecoder {
            envelope: JsonDecoder::new(
                envelope,
                crate::json::JsonEnv { mode: env.mode, lenient: skip_errors },
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

    /// The `DataOld` presence scan: per surviving body (same null/whitespace skips as the envelope
    /// decode, asserted below), the set of physical field keys present under `old` — for Canal, the
    /// union across the array's elements, which is exactly what Flink's `findValue` over the array
    /// node sees. A message without a usable `old` contributes an empty mask (only updates read it,
    /// and a null `old` on an update already failed in `cdc_emit`).
    fn old_key_presence(&self, bodies: &RecordBatch) -> Vec<u128> {
        use simd_json::prelude::*;
        let spec = self.dialect.spec();
        let column = bodies.column(0);
        let mut masks = Vec::with_capacity(bodies.num_rows());
        let mut scratch: Vec<u8> = Vec::new();
        let mut buffers = simd_json::Buffers::default();
        let mask_of = |object: &simd_json::tape::Object<'_, '_>| -> u128 {
            let mut mask = 0u128;
            for (key, _) in object {
                if let Some(i) = self.field_names.iter().position(|name| name == key) {
                    mask |= 1 << i;
                }
            }
            mask
        };
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
                .and_then(|envelope| envelope.iter().find(|(key, _)| *key == spec.before_field))
                .map(|(_, value)| value);
            let mask = match old {
                Some(value) if spec.arrays => value
                    .as_array()
                    .map(|elements| {
                        elements
                            .iter()
                            .filter_map(|e| e.as_object().map(|o| mask_of(&o)))
                            .fold(0u128, |acc, m| acc | m)
                    })
                    .unwrap_or(0),
                Some(value) => value.as_object().map(|o| mask_of(&o)).unwrap_or(0),
                None => 0,
            };
            masks.push(mask);
        }
        masks
    }

    fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        use arrow::array::ListArray;
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

        // Gather each physical column, choosing the pre/post-image child per output row. The source is
        // the same across columns except for `Coalesce`, which picks per field by the key's presence
        // in the message's `old` — so the gather index is built per field.
        const BEFORE: usize = 0;
        const AFTER: usize = 1;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.arity + 1);
        for field in 0..self.arity {
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
        RecordBatch::try_new(self.output.clone(), columns).expect("failed to build CDC batch")
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
/// split on the first `=`): the table's `csv.*` Jackson knobs and the JSON family's
/// `timestamp-format.standard`. Only options the planner has vetted reach here — anything
/// unsupported already fell back — so an unknown key is a wiring bug, not user input.
pub(crate) struct FormatOptions {
    pub(crate) csv: crate::csv::CsvOptions,
    pub(crate) timestamp_mode: crate::flink_text::TimestampMode,
}

pub(crate) fn parse_format_options(encoded: &str) -> FormatOptions {
    let mut csv = crate::csv::CsvOptions::default();
    let mut timestamp_mode = crate::flink_text::TimestampMode::default();
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
            other => panic!("unknown format option {other}"),
        }
    }
    FormatOptions { csv, timestamp_mode }
}

pub(crate) enum FormatDecoder {
    Json(JsonDecoder),
    Csv(crate::csv::CsvDecoder),
    Raw(RawDecoder),
    /// Confluent-framed Avro: each message is `0x00` + 4-byte BE schema id + datum, resolved by id. The
    /// optional reader schema projects the writer's record to a subset of fields (Avro resolution).
    Avro(arrow_avro::schema::SchemaStore, Option<arrow_avro::schema::AvroSchema>),
    /// Bare Avro (Flink's `avro`): each message is just the datum, decoded against the one writer schema
    /// registered at synthetic id 0 — we prepend the 5-byte id-0 header so the framed decoder applies. An
    /// optional reader schema projects it to a subset (the query's columns/fields) via Avro resolution.
    BareAvro(arrow_avro::schema::SchemaStore, Option<arrow_avro::schema::AvroSchema>),
    Protobuf(ProtobufDecoder),
    /// CDC changelog JSON (Debezium/OGG): envelope → physical rows + `$row_kind$`, fanning out updates.
    Cdc(CdcJsonDecoder),
}

impl FormatDecoder {
    fn decode(&self, body: &RecordBatch) -> RecordBatch {
        match self {
            FormatDecoder::Json(decoder) => decoder.decode(body),
            FormatDecoder::Csv(decoder) => decoder.decode(body),
            FormatDecoder::Raw(decoder) => decoder.decode(body),
            FormatDecoder::Avro(store, reader) => decode_avro_body(store, reader, body, false),
            FormatDecoder::BareAvro(store, reader) => decode_avro_body(store, reader, body, true),
            FormatDecoder::Protobuf(decoder) => decoder.decode(body),
            FormatDecoder::Cdc(decoder) => decoder.decode(body),
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

/// Marks the current thread as inside a skip-mode per-message decode, silencing the panic hook for
/// the expected decode failures (Flink's `ignore-parse-errors` skips silently; a hook line per bad
/// message would flood the log). The hook replacement happens once, delegating to the previous hook
/// for every panic outside a skip-mode decode.
pub(crate) fn silence_expected_decode_panics<R>(work: impl FnOnce() -> R) -> R {
    use std::cell::Cell;
    use std::sync::Once;
    thread_local! {
        static IN_SKIP_DECODE: Cell<bool> = const { Cell::new(false) };
    }
    static INSTALL_HOOK: Once = Once::new();
    INSTALL_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !IN_SKIP_DECODE.with(Cell::get) {
                previous(info);
            }
        }));
    });
    IN_SKIP_DECODE.with(|flag| flag.set(true));
    let result = work();
    IN_SKIP_DECODE.with(|flag| flag.set(false));
    result
}

impl MessageDecoder {
    /// `format`: 0 = JSON, 2 = CSV, 3 = `raw`, 6 = debezium-json, 7 = ogg-json, 8 = maxwell-json,
    /// 9 = canal-json — all decoded against `output_schema` (CDC treats it as the physical columns);
    /// 1 = Confluent-Avro
    /// (`avro_schema` registered at `schema_id`); 4 = bare Avro (`avro_schema` as the reader schema,
    /// registered at synthetic id 0). (Protobuf is built via `createProtobufDecoder`, not here.)
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
        // Every JSON-decoded format handles its own skip granularity (CdcJsonDecoder /
        // JsonDecoder's lenient appenders / CsvDecoder), so the generic per-message retry below
        // only serves a CDC batch whose ENVELOPE decode fails structurally.
        let cdc_env = crate::json::JsonEnv { mode: options.timestamp_mode, lenient: false };
        let decoder = match format {
            1 => FormatDecoder::Avro(avro_store(avro_schema, schema_id as u32), reader),
            4 => FormatDecoder::BareAvro(avro_store(avro_schema, 0), reader),
            // CSV owns its skip mode: Flink's ignore-parse-errors granularity for CSV is per FIELD
            // (a bad value nulls the field, a short row pads, only a record-level failure drops the
            // row), which the generic per-message retry below cannot reproduce.
            2 => {
                return MessageDecoder {
                    decoder: FormatDecoder::Csv(crate::csv::CsvDecoder::new(
                        output_schema,
                        options.csv,
                        skip_errors,
                    )),
                    skip_errors: false,
                }
            }
            3 => FormatDecoder::Raw(RawDecoder::new(output_schema)),
            6 => FormatDecoder::Cdc(CdcJsonDecoder::new(
                output_schema,
                CdcDialect::Debezium,
                cdc_env,
                skip_errors,
            )),
            7 => FormatDecoder::Cdc(CdcJsonDecoder::new(
                output_schema,
                CdcDialect::Ogg,
                cdc_env,
                skip_errors,
            )),
            8 => FormatDecoder::Cdc(CdcJsonDecoder::new(
                output_schema,
                CdcDialect::Maxwell,
                cdc_env,
                skip_errors,
            )),
            9 => FormatDecoder::Cdc(CdcJsonDecoder::new(
                output_schema,
                CdcDialect::Canal,
                cdc_env,
                skip_errors,
            )),
            _ => {
                return MessageDecoder {
                    decoder: FormatDecoder::Json(JsonDecoder::new(
                        output_schema,
                        crate::json::JsonEnv { mode: options.timestamp_mode, lenient: skip_errors },
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
        use arrow_avro::schema::{AvroSchema, Fingerprint};
        match &mut self.decoder {
            FormatDecoder::Avro(store, _) => {
                store
                    .set(Fingerprint::Id(id), AvroSchema::new(schema.to_string()))
                    .expect("failed to register avro schema");
            }
            _ => panic!("registerAvroSchema on a non-Confluent-Avro decoder"),
        }
    }
}

/// An arrow-avro writer store keyed by integer id (the Confluent / id-framing layout). An empty
/// schema string builds an empty store — the Confluent path starts with no writer schemas and feeds
/// them in by id as the JVM fetches them from the schema registry (`registerAvroSchema`).
pub(crate) fn avro_store(avro_schema: &str, id: u32) -> arrow_avro::schema::SchemaStore {
    use arrow_avro::schema::{AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore};
    let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
    if !avro_schema.is_empty() {
        store
            .set(Fingerprint::Id(id), AvroSchema::new(avro_schema.to_string()))
            .expect("failed to register avro schema");
    }
    store
}

/// Creates a format-dispatched message decoder and returns an opaque handle, released with
/// `closeDecoder`. Formats 0/2/3 (JSON/CSV/raw) decode against the target schema the JVM exports as an
/// empty batch; formats 1/4 (Confluent/bare Avro) derive their schema from `avroSchema` (registered
/// under `schemaId` for Confluent, synthetic id 0 for bare) and ignore the schema C structs. A format-1
/// decoder built with an empty `avroSchema` starts with an empty store — the registry-driven path,
/// where the JVM registers each writer schema by id via `registerAvroSchema` as messages carry it.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createDecoder<'local>(
    mut env: JNIEnv<'local>,
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
    // Avro (1, 4) derives its own schema from the writer schema, so those callers pass 0/0 for the
    // schema C structs; JSON/CSV/raw (0, 2, 3) decode against the exported target schema.
    let schema = if format == 1 || format == 4 {
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
}

/// Creates a protobuf message decoder (Flink's `protobuf` format: bare message bytes, no framing) and
/// returns an opaque `MessageDecoder` handle, released with `closeDecoder` like any other decoder.
/// `descriptor` is an encoded `FileDescriptorSet` the JVM serialized off the generated message class
/// (the message's `.proto` file + transitive dependencies); `messageName` is the fully-qualified type
/// to decode each body as. The Arrow batch schema is derived from the descriptor by ptars (no schema
/// C-structs needed, unlike JSON).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createProtobufDecoder<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    descriptor: JByteArray<'local>,
    message_name: JString<'local>,
    schema_array_address: jlong,
    schema_address: jlong,
) -> jlong {
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
}

/// Registers a writer schema under a Confluent schema id on an existing Confluent-Avro decoder. The
/// JVM operator calls this the first time a batch carries an id it hasn't seen: it fetches the schema
/// from the schema registry (as Flink's own `avro-confluent` deserializer does) and feeds it here, so
/// the store grows with the topic's schema evolution instead of being fixed at plan time.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_registerAvroSchema<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    schema_id: jint,
    schema: JString<'local>,
) {
    let decoder = unsafe { &mut *(handle as *mut MessageDecoder) };
    let schema: String = env.get_string(&schema).expect("failed to read avro schema").into();
    decoder.register_writer_schema(schema_id as u32, &schema);
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
                format!("native decode failed: {}", panic_message(&panic)),
            );
        }
    }
}

pub(crate) fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> &str {
    panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("panic")
}

/// Benchmark-only: decode a body batch and return the decoded row count without exporting the result —
/// so the shallow path can terminate with Arrow in Rust (counted in Rust), symmetric with the native
/// consumer, for an apples-to-apples comparison.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_decodeCount<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) -> jlong {
    let decoder = unsafe { &*(handle as *mut MessageDecoder) };
    let bodies = import_record_batch(in_array_address, in_schema_address);
    decoder.decode(&bodies).num_rows() as jlong
}

/// Releases a message decoder handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeDecoder<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<MessageDecoder>(handle));
    }
}

/// This format library's decode behind the cross-DSO driver ABI (`format_abi`): an opaque decoder
/// handle and Arrow C Data addresses, nothing language-specific. A panic is contained and reported
/// as nonzero, and the caller raises the failure on its own JNI surface.
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
        Err(_) => 1,
    }
}

/// The exported driver init (ADBC's `AdbcDriverInit` pattern): a connector states the ABI version it
/// speaks and passes the matching vtable to fill; this library fills it or refuses with nonzero, and
/// a refusal leaves the caller on the JVM-mediated decode path. The connector obtains this function's
/// address through the format's Java facade — by handoff, never by symbol linkage (divergences/25).
#[no_mangle]
pub extern "C" fn streamfusion_format_driver_init(version: i32, driver: *mut FormatDriver) -> i32 {
    if version != FORMAT_DRIVER_VERSION_1 || driver.is_null() {
        return 1;
    }
    unsafe {
        (*driver).decode_body_batch = decode_body_batch;
    }
    0
}

// Format artifacts expose separate Java facades so the core DSO never owns message-decoder JNI. The
// decoder implementation remains shared in this crate, but the symbols below are compiled only into the
// corresponding extension build; loading a format JAR therefore cannot accidentally make a connector
// or the core artifact provide that format.

#[cfg(feature = "json")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_json_NativeJsonFormat_driverInitAddress<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    streamfusion_format_driver_init as usize as jlong
}

#[cfg(feature = "json")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_json_NativeJsonFormat_isLoaded<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jboolean {
    1
}

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

#[cfg(feature = "json")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_json_NativeJsonFormat_decodeInto<'local>(
    env: JNIEnv<'local>, class: JClass<'local>, handle: jlong, in_array: jlong, in_schema: jlong,
    out_array: jlong, out_schema: jlong,
) {
    Java_io_github_jordepic_streamfusion_Native_decodeInto(
        env, class, handle, in_array, in_schema, out_array, out_schema,
    )
}

#[cfg(feature = "json")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_json_NativeJsonFormat_closeDecoder<'local>(
    env: JNIEnv<'local>, class: JClass<'local>, handle: jlong,
) {
    Java_io_github_jordepic_streamfusion_Native_closeDecoder(env, class, handle)
}

#[cfg(feature = "csv")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_csv_NativeCsvFormat_driverInitAddress<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    streamfusion_format_driver_init as usize as jlong
}

#[cfg(feature = "csv")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_csv_NativeCsvFormat_isLoaded<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jboolean {
    1
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
        env, class, 2, schema_array_address, schema_address, empty_writer, empty_reader, 0,
        skip_parse_errors, format_options,
    )
}

#[cfg(feature = "csv")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_csv_NativeCsvFormat_decodeInto<'local>(
    env: JNIEnv<'local>, class: JClass<'local>, handle: jlong, in_array: jlong, in_schema: jlong,
    out_array: jlong, out_schema: jlong,
) {
    Java_io_github_jordepic_streamfusion_Native_decodeInto(
        env, class, handle, in_array, in_schema, out_array, out_schema,
    )
}

#[cfg(feature = "csv")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_csv_NativeCsvFormat_closeDecoder<'local>(
    env: JNIEnv<'local>, class: JClass<'local>, handle: jlong,
) {
    Java_io_github_jordepic_streamfusion_Native_closeDecoder(env, class, handle)
}

#[cfg(feature = "raw")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_raw_NativeRawFormat_driverInitAddress<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    streamfusion_format_driver_init as usize as jlong
}

#[cfg(feature = "raw")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_raw_NativeRawFormat_isLoaded<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jboolean {
    1
}

#[cfg(feature = "raw")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_raw_NativeRawFormat_createDecoder<'local>(
    mut env: JNIEnv<'local>, class: JClass<'local>, schema_array_address: jlong, schema_address: jlong,
) -> jlong {
    let empty_writer = env.new_string("").expect("empty writer schema");
    let empty_reader = env.new_string("").expect("empty reader schema");
    let empty_options = env.new_string("").expect("empty format options");
    Java_io_github_jordepic_streamfusion_Native_createDecoder(
        env, class, 3, schema_array_address, schema_address, empty_writer, empty_reader, 0, 0,
        empty_options,
    )
}

#[cfg(feature = "raw")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_raw_NativeRawFormat_decodeInto<'local>(
    env: JNIEnv<'local>, class: JClass<'local>, handle: jlong, in_array: jlong, in_schema: jlong,
    out_array: jlong, out_schema: jlong,
) {
    Java_io_github_jordepic_streamfusion_Native_decodeInto(
        env, class, handle, in_array, in_schema, out_array, out_schema,
    )
}

#[cfg(feature = "raw")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_raw_NativeRawFormat_closeDecoder<'local>(
    env: JNIEnv<'local>, class: JClass<'local>, handle: jlong,
) {
    Java_io_github_jordepic_streamfusion_Native_closeDecoder(env, class, handle)
}

#[cfg(feature = "avro")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_avro_NativeAvroFormat_driverInitAddress<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    streamfusion_format_driver_init as usize as jlong
}

#[cfg(feature = "avro")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_avro_NativeAvroFormat_isLoaded<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jboolean {
    1
}

#[cfg(feature = "avro")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_avro_NativeAvroFormat_createDecoder<'local>(
    mut env: JNIEnv<'local>, class: JClass<'local>, confluent: jboolean, writer_schema: JString<'local>,
    reader_schema: JString<'local>,
) -> jlong {
    let empty_options = env.new_string("").expect("empty format options");
    Java_io_github_jordepic_streamfusion_Native_createDecoder(
        env,
        class,
        if confluent != 0 { 1 } else { 4 },
        0,
        0,
        writer_schema,
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

#[cfg(feature = "avro")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_avro_NativeAvroFormat_decodeInto<'local>(
    env: JNIEnv<'local>, class: JClass<'local>, handle: jlong, in_array: jlong, in_schema: jlong,
    out_array: jlong, out_schema: jlong,
) {
    Java_io_github_jordepic_streamfusion_Native_decodeInto(
        env, class, handle, in_array, in_schema, out_array, out_schema,
    )
}

#[cfg(feature = "avro")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_avro_NativeAvroFormat_closeDecoder<'local>(
    env: JNIEnv<'local>, class: JClass<'local>, handle: jlong,
) {
    Java_io_github_jordepic_streamfusion_Native_closeDecoder(env, class, handle)
}

#[cfg(feature = "protobuf")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_protobuf_NativeProtobufFormat_driverInitAddress<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    streamfusion_format_driver_init as usize as jlong
}

#[cfg(feature = "protobuf")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_protobuf_NativeProtobufFormat_isLoaded<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jboolean {
    1
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

#[cfg(feature = "protobuf")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_protobuf_NativeProtobufFormat_decodeInto<'local>(
    env: JNIEnv<'local>, class: JClass<'local>, handle: jlong, in_array: jlong, in_schema: jlong,
    out_array: jlong, out_schema: jlong,
) {
    Java_io_github_jordepic_streamfusion_Native_decodeInto(
        env, class, handle, in_array, in_schema, out_array, out_schema,
    )
}

#[cfg(feature = "protobuf")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_format_protobuf_NativeProtobufFormat_closeDecoder<'local>(
    env: JNIEnv<'local>, class: JClass<'local>, handle: jlong,
) {
    Java_io_github_jordepic_streamfusion_Native_closeDecoder(env, class, handle)
}

/// Decodes a binary "body" batch into typed Arrow via arrow-avro against the local schema-id store. When
/// `bare`, each message is a raw datum (Flink's `avro`) and we prepend the 5-byte id-0 Confluent header
/// (`0x00` + 4-byte 0) so arrow-avro's framed decoder resolves it against the schema at id 0; otherwise
/// each message already carries its `0x00` + id prefix (Confluent `avro-confluent`). A null body is
/// skipped. Used by `MessageDecoder` for both Avro variants.
pub(crate) fn decode_avro_body(
    store: &arrow_avro::schema::SchemaStore,
    reader: &Option<arrow_avro::schema::AvroSchema>,
    body: &RecordBatch,
    bare: bool,
) -> RecordBatch {
    use arrow::array::{Array, BinaryArray};
    let column = body.column(0).as_any().downcast_ref::<BinaryArray>().expect("binary body");
    let mut builder = arrow_avro::reader::ReaderBuilder::new()
        .with_writer_schema_store(store.clone())
        .with_batch_size(column.len().max(1));
    // With a reader schema, Avro resolution decodes the full writer datum but materializes only the
    // reader's (subset of) fields — projection pushed into the decode. Writer fields the reader omits
    // are parsed and discarded, never built into Arrow.
    if let Some(reader_schema) = reader {
        builder = builder.with_reader_schema(reader_schema.clone());
    }
    let mut decoder = builder.build_decoder().expect("failed to build avro decoder");
    let mut framed = Vec::new();
    // A message framed with a different schema id than its predecessor makes the decoder stop
    // consuming until the rows decoded so far are flushed (it can't mix writer schemas in one build),
    // so decode in a loop, flushing whenever a message is only partially consumed. With a reader
    // schema every flushed batch has the same (reader) shape, so the flushes concatenate.
    let mut batches = Vec::new();
    for i in 0..column.len() {
        if !column.is_valid(i) {
            continue;
        }
        let bytes = if bare {
            framed.clear();
            framed.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00]); // id-0 Confluent header
            framed.extend_from_slice(column.value(i));
            &framed[..]
        } else {
            column.value(i)
        };
        let mut consumed = 0;
        while consumed < bytes.len() {
            let n = decoder.decode(&bytes[consumed..]).expect("avro decode failed");
            consumed += n;
            if consumed < bytes.len() {
                match decoder.flush().expect("avro flush failed") {
                    Some(batch) => batches.push(batch),
                    // No progress and nothing to flush: the message is truncated/malformed.
                    None if n == 0 => panic!("avro decode stalled on a malformed message"),
                    None => {}
                }
            }
        }
    }
    if let Some(batch) = decoder.flush().expect("avro flush failed") {
        batches.push(batch);
    }
    match batches.len() {
        0 => panic!("empty avro batch"),
        1 => batches.into_iter().next().unwrap(),
        _ => {
            let schema = batches[0].schema();
            arrow::compute::concat_batches(&schema, &batches).expect("avro batch concat failed")
        }
    }
}
