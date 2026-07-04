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
    /// A null body decodes to a null row (ptars keeps the batch 1:1 with the input column).
    pub(crate) fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        use arrow::array::BinaryArray;
        let column = bodies.column(0).as_any().downcast_ref::<BinaryArray>().expect("binary body");
        ptars::binary_array_to_record_batch_direct(column, &self.message, &self.config)
            .expect("failed to decode protobuf batch")
    }
}

/// Decodes a binary "body" batch (one CSV record per row, no header) into a batch of the target schema
/// via `arrow-csv`, matching Flink's `csv` format. Records are fed newline-terminated so the streaming
/// decoder sees each message as a complete row; a null body contributes no row (like JSON).
pub(crate) struct CsvDecoder {
    schema: SchemaRef,
}

impl CsvDecoder {
    fn new(schema: SchemaRef) -> CsvDecoder {
        CsvDecoder { schema }
    }

    fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        let column = bodies.column(0);
        let row_count = bodies.num_rows();
        let body = |row: usize| -> Option<&[u8]> { binary_body(column, row) };
        // Join the rows into one newline-delimited buffer (each Kafka message is one record with no
        // trailing newline), then stream it through the CSV decoder in one flush.
        let mut buf = Vec::new();
        for row in 0..row_count {
            if let Some(bytes) = body(row) {
                buf.extend_from_slice(bytes);
                if !bytes.ends_with(b"\n") {
                    buf.push(b'\n');
                }
            }
        }
        let mut decoder = arrow::csv::ReaderBuilder::new(self.schema.clone())
            .with_header(false)
            .with_batch_size(row_count.max(1))
            .build_decoder();
        let mut offset = 0;
        while offset < buf.len() {
            let consumed = decoder.decode(&buf[offset..]).expect("failed to decode CSV record");
            if consumed == 0 {
                break;
            }
            offset += consumed;
        }
        decoder
            .flush()
            .expect("failed to flush CSV batch")
            .unwrap_or_else(|| RecordBatch::new_empty(self.schema.clone()))
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
    /// fields). DELETE reads `data` (it holds the deleted row); an update's UPDATE_BEFORE is
    /// `coalesce(old, data)` per field — a field absent from `old` is unchanged, so it falls back to
    /// `data`. (Divergence: a field deliberately changed *to* null can't be told apart from an absent
    /// one once decoded, so it falls back to `data`; Flink keeps the null. Rare; documented.)
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
/// UPDATE_AFTER; a `BeforeAfter` dialect with a null pre-image skips the update/delete (Flink throws).
pub(crate) fn cdc_emit(
    action: &CdcAction,
    before_idx: usize,
    after_idx: usize,
    shape: CdcShape,
    before: &StructArray,
    out: &mut Vec<(i8, usize, usize, RowSource)>,
) {
    // Debezium/OGG fail the job on a null pre-image for an update/delete (Flink's REPLICA_IDENTITY
    // error); we match that rather than silently dropping the row, so the result is identical.
    match action {
        CdcAction::Insert => out.push((0, before_idx, after_idx, RowSource::After)),
        CdcAction::Update => {
            let before_source = match shape {
                CdcShape::BeforeAfter if !before.is_valid(before_idx) => {
                    panic!("CDC UPDATE has a null \"before\"/pre-image (REPLICA IDENTITY not FULL?)")
                }
                CdcShape::BeforeAfter => RowSource::Before,
                CdcShape::DataOld => RowSource::Coalesce,
            };
            out.push((1, before_idx, after_idx, before_source));
            out.push((2, before_idx, after_idx, RowSource::After));
        }
        CdcAction::Delete => match shape {
            CdcShape::BeforeAfter if !before.is_valid(before_idx) => {
                panic!("CDC DELETE has a null \"before\"/pre-image (REPLICA IDENTITY not FULL?)")
            }
            CdcShape::BeforeAfter => out.push((3, before_idx, after_idx, RowSource::Before)),
            // Maxwell/Canal: the deleted row lives in the post-image (`data`).
            CdcShape::DataOld => out.push((3, before_idx, after_idx, RowSource::After)),
        },
    }
}

/// Which image an output row reads its columns from. `Coalesce` (Maxwell/Canal UPDATE_BEFORE) reads the
/// pre-image where that field is present and falls back to the post-image otherwise — a per-field choice,
/// so it can't share one gather index across columns.
#[derive(Clone, Copy)]
pub(crate) enum RowSource {
    /// The pre-image (`before` / `old`), envelope column 0.
    Before,
    /// The post-image (`after` / `data`), envelope column 1.
    After,
    /// Per field: pre-image where present, else post-image.
    Coalesce,
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
    dialect: CdcDialect,
}

impl CdcJsonDecoder {
    fn new(physical: SchemaRef, dialect: CdcDialect) -> CdcJsonDecoder {
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
        CdcJsonDecoder { envelope: JsonDecoder::new(envelope), output, arity: nullable.len(), dialect }
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

        // Per output row: its RowKind byte (0 +I, 1 -U, 2 +U, 3 -D — `RowKind.toByteValue()`), and the
        // rows to read in the pre/post-image struct arrays, and which image to read each column from.
        let mut out_rows: Vec<(i8, usize, usize, RowSource)> = Vec::with_capacity(envelope.num_rows());
        for row in 0..envelope.num_rows() {
            // A missing op field is malformed; Flink fails on it (NPE caught → rethrown). Match that.
            let op = if ops.is_valid(row) {
                ops.value(row)
            } else {
                panic!("CDC message has no operation field");
            };
            let action = match self.dialect.classify(op) {
                CdcOp::Change(action) => action,
                CdcOp::Skip => continue,
                // Flink throws on an unrecognized op by default; we fail too (never drop it silently).
                CdcOp::Unknown => panic!("unknown CDC operation \"{op}\""),
            };
            if spec.arrays {
                let after_list = envelope.column(1).as_any().downcast_ref::<ListArray>().unwrap();
                let before_list = envelope.column(0).as_any().downcast_ref::<ListArray>().unwrap();
                let (after_off, after_len) =
                    (after_list.value_offsets()[row] as usize, after_list.value_length(row) as usize);
                let (before_off, before_len) =
                    (before_list.value_offsets()[row] as usize, before_list.value_length(row) as usize);
                for i in 0..after_len {
                    // Canal pairs data[i] with old[i]; if `old` is shorter (or absent) for this element,
                    // there is no paired pre-image, so fall back to the post-image (coalesce → no change).
                    let before_idx = if i < before_len { before_off + i } else { after_off + i };
                    cdc_emit(&action, before_idx, after_off + i, spec.shape, before, &mut out_rows);
                }
            } else {
                cdc_emit(&action, row, row, spec.shape, before, &mut out_rows);
            }
        }

        // Gather each physical column, choosing the pre/post-image child per output row. The source is
        // the same across columns except for `Coalesce`, which picks per field by the pre-image's
        // validity there — so the gather index is built per field.
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
                    RowSource::Coalesce => {
                        if before_child.is_valid(before_idx) {
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

pub(crate) enum FormatDecoder {
    Json(JsonDecoder),
    Csv(CsvDecoder),
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
    pub(crate) fn new(
        format: i32,
        output_schema: SchemaRef,
        avro_schema: &str,
        reader_avro_schema: &str,
        schema_id: i32,
        skip_errors: bool,
    ) -> MessageDecoder {
        // A non-empty reader schema projects the writer record to a subset of fields (Avro resolution),
        // set when the planner pushes the query's projection into the decode.
        let reader = if reader_avro_schema.is_empty() {
            None
        } else {
            Some(arrow_avro::schema::AvroSchema::new(reader_avro_schema.to_string()))
        };
        let decoder = match format {
            1 => FormatDecoder::Avro(avro_store(avro_schema, schema_id as u32), reader),
            4 => FormatDecoder::BareAvro(avro_store(avro_schema, 0), reader),
            2 => FormatDecoder::Csv(CsvDecoder::new(output_schema)),
            3 => FormatDecoder::Raw(RawDecoder::new(output_schema)),
            6 => FormatDecoder::Cdc(CdcJsonDecoder::new(output_schema, CdcDialect::Debezium)),
            7 => FormatDecoder::Cdc(CdcJsonDecoder::new(output_schema, CdcDialect::Ogg)),
            8 => FormatDecoder::Cdc(CdcJsonDecoder::new(output_schema, CdcDialect::Maxwell)),
            9 => FormatDecoder::Cdc(CdcJsonDecoder::new(output_schema, CdcDialect::Canal)),
            _ => FormatDecoder::Json(JsonDecoder::new(output_schema)),
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
    into_handle(MessageDecoder::new(
        format,
        schema,
        &avro_schema,
        &reader_avro_schema,
        schema_id,
        skip_parse_errors != 0,
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
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_decodeInto<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let decoder = unsafe { &*(handle as *mut MessageDecoder) };
    let bodies = import_record_batch(in_array_address, in_schema_address);
    export_record_batch(decoder.decode(&bodies), out_array_address, out_schema_address);
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
