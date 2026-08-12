use crate::*;
/// One encoded record per row, all in a single encode buffer: producing and JNI materialization
/// read the per-row slices in place, so no per-record allocation or copy happens on this side.
#[cfg(feature = "kafka")]
pub struct EncodedLines {
    bytes: Vec<u8>,
    lines: Vec<std::ops::Range<usize>>,
}

#[cfg(feature = "kafka")]
impl EncodedLines {
    pub(crate) fn new(bytes: Vec<u8>, lines: Vec<std::ops::Range<usize>>) -> EncodedLines {
        EncodedLines { bytes, lines }
    }

    /// Wraps a buffer of concatenated records delimited by `offsets` (`rows + 1` entries).
    pub(crate) fn from_offsets(bytes: Vec<u8>, offsets: &[usize]) -> EncodedLines {
        let lines = offsets.windows(2).map(|pair| pair[0]..pair[1]).collect();
        EncodedLines { bytes, lines }
    }

    pub fn len(&self) -> usize {
        self.lines.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    pub fn line(&self, index: usize) -> &[u8] {
        &self.bytes[self.lines[index].clone()]
    }

    fn metadata(&self, nulls: &[bool]) -> Result<(Vec<i32>, Vec<i32>), String> {
        self.lines
            .iter()
            .enumerate()
            .map(|(index, range)| {
                let offset = i32::try_from(range.start)
                    .map_err(|_| "encoded Kafka batch exceeds JVM array limits".to_string())?;
                let length = if nulls.get(index).copied().unwrap_or(false) {
                    -1
                } else {
                    i32::try_from(range.len())
                        .map_err(|_| "encoded Kafka record exceeds JVM array limits".to_string())?
                };
                Ok((offset, length))
            })
            .collect::<Result<Vec<_>, String>>()
            .map(|metadata| metadata.into_iter().unzip())
    }
}

/// One JSON format instance's encode-affecting options — Flink configures the Jackson mapper and
/// converter family per format instance from the `json.*` option set, so the native encoder takes
/// the same set wherever a format instance would exist. The defaults are the json format
/// factory's own.
#[cfg(feature = "kafka")]
pub(crate) struct JsonEncodeOptions {
    pub(crate) ignore_null_fields: bool,
    pub(crate) iso_8601: bool,
    pub(crate) decimal_as_plain_number: bool,
    pub(crate) map_null_key_mode: MapNullKeyMode,
    pub(crate) map_null_key_literal: String,
}

#[cfg(feature = "kafka")]
impl Default for JsonEncodeOptions {
    fn default() -> JsonEncodeOptions {
        JsonEncodeOptions {
            ignore_null_fields: false,
            iso_8601: false,
            decimal_as_plain_number: false,
            map_null_key_mode: MapNullKeyMode::Fail,
            map_null_key_literal: "null".to_string(),
        }
    }
}

/// Flink's `json.map-null-key.mode`: what a serialized map does with a null key.
#[cfg(feature = "kafka")]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum MapNullKeyMode {
    Fail,
    Drop,
    Literal,
}

/// Parses one format instance's `EncodeFormat` option lines. Only options the planner has resolved
/// reach here — anything unsupported already fell back — so an unknown key is a wiring bug.
#[cfg(feature = "kafka")]
fn parse_json_encode_options(encoded: &str) -> Result<JsonEncodeOptions, String> {
    let mut options = JsonEncodeOptions::default();
    for line in encoded.lines().filter(|line| !line.is_empty()) {
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("encode option is not key=value: {line}"))?;
        match key {
            "timestamp-format" => options.iso_8601 = value == "ISO-8601",
            "encode.ignore-null-fields" => options.ignore_null_fields = value == "true",
            "encode.decimal-as-plain-number" => options.decimal_as_plain_number = value == "true",
            "map-null-key.mode" => {
                options.map_null_key_mode = match value {
                    "FAIL" => MapNullKeyMode::Fail,
                    "DROP" => MapNullKeyMode::Drop,
                    "LITERAL" => MapNullKeyMode::Literal,
                    other => return Err(format!("unknown map-null-key.mode {other}")),
                }
            }
            "map-null-key.literal" => options.map_null_key_literal = value.to_string(),
            other => return Err(format!("unknown JSON encode option {other}")),
        }
    }
    Ok(options)
}

/// One resolved sink format instance: the parsed options of whichever format the wire code named.
/// The JSON family shares one variant — the four CDC dialects are the JSON row encode plus a
/// changelog envelope wrapped around each row, forwarding the same `json.*` option set to the
/// nested row serializer exactly as Flink's envelope schemas do. Formats whose implementation
/// rides its own cargo feature gate their variant, so a connector build without that feature
/// still compiles — the dispatch then reports the format unsupported and the JVM's capability
/// probe (`encodeFormatSupported`) keeps the planner falling back.
#[cfg(feature = "kafka")]
pub(crate) enum EncodeOptions {
    Json {
        envelope: Option<CdcEnvelope>,
        options: JsonEncodeOptions,
    },
    #[cfg(feature = "csv")]
    Csv(crate::csv_encode::CsvEncodeOptions),
    #[cfg(feature = "avro")]
    Avro(crate::avro::AvroEncodeOptions),
    #[cfg(feature = "avro")]
    DebeziumAvro(crate::avro::AvroEncodeOptions),
    #[cfg(feature = "protobuf")]
    Protobuf(crate::protobuf_encode::ProtobufEncodeOptions),
    #[cfg(feature = "raw")]
    Raw(crate::raw_encode::RawEncodeOptions),
}

/// The CDC JSON dialect whose envelope wraps each encoded row on the sink side.
#[cfg(feature = "kafka")]
#[derive(Clone, Copy)]
pub(crate) enum CdcEnvelope {
    /// `{"before", "after", "op"}`, op ∈ {`c`, `d`} — `DebeziumJsonSerializationSchema`.
    Debezium,
    /// `{"before", "after", "op_type"}`, op ∈ {`I`, `D`} — `OggJsonSerializationSchema`.
    Ogg,
    /// `{"data", "type"}`, type ∈ {`insert`, `delete`} — `MaxwellJsonSerializationSchema`.
    Maxwell,
    /// `{"data": [row], "type"}`, type ∈ {`INSERT`, `DELETE`} — `CanalJsonSerializationSchema`.
    Canal,
}

/// The encode dispatch of the sink's format seam: the JVM passes a `FormatCodes` wire code plus the
/// format's resolved option lines; each natively encoded sink format adds an arm here.
#[cfg(feature = "kafka")]
fn parse_encode_format(format: i32, encoded: &str) -> Result<EncodeOptions, String> {
    let envelope = match format {
        FORMAT_JSON => None,
        FORMAT_DEBEZIUM_JSON => Some(CdcEnvelope::Debezium),
        FORMAT_OGG_JSON => Some(CdcEnvelope::Ogg),
        FORMAT_MAXWELL_JSON => Some(CdcEnvelope::Maxwell),
        FORMAT_CANAL_JSON => Some(CdcEnvelope::Canal),
        #[cfg(feature = "csv")]
        FORMAT_CSV => {
            return crate::csv_encode::parse_csv_encode_options(encoded).map(EncodeOptions::Csv)
        }
        #[cfg(feature = "avro")]
        FORMAT_AVRO => {
            return crate::avro::AvroEncodeOptions::parse(encoded, false).map(EncodeOptions::Avro)
        }
        #[cfg(feature = "avro")]
        FORMAT_AVRO_CONFLUENT => {
            return crate::avro::AvroEncodeOptions::parse(encoded, true).map(EncodeOptions::Avro)
        }
        #[cfg(feature = "avro")]
        FORMAT_DEBEZIUM_AVRO_CONFLUENT => {
            return crate::avro::AvroEncodeOptions::parse(encoded, true)
                .map(EncodeOptions::DebeziumAvro)
        }
        #[cfg(feature = "protobuf")]
        FORMAT_PROTOBUF => {
            return crate::protobuf_encode::ProtobufEncodeOptions::parse(encoded)
                .map(EncodeOptions::Protobuf)
        }
        #[cfg(feature = "raw")]
        FORMAT_RAW => {
            return crate::raw_encode::RawEncodeOptions::parse(encoded).map(EncodeOptions::Raw)
        }
        other => return Err(format!("format code {other} is not natively encoded")),
    };
    Ok(EncodeOptions::Json {
        envelope,
        options: parse_json_encode_options(encoded)?,
    })
}

#[cfg(feature = "kafka")]
pub(crate) fn encode_json_batch(
    batch: &RecordBatch,
    options: &JsonEncodeOptions,
    logical_types: &[String],
    field_names: &[String],
) -> Result<EncodedLines, String> {
    let batch = annotate_flink_types(batch, logical_types, field_names)?;
    let encoder_options = json_encoder_options(options);
    encode_json_rows(&batch, &encoder_options)
}

#[cfg(feature = "kafka")]
fn json_encoder_options(options: &JsonEncodeOptions) -> arrow::json::writer::EncoderOptions {
    let mut encoder_options = arrow::json::writer::EncoderOptions::default()
        .with_explicit_nulls(!options.ignore_null_fields)
        .with_time_format("%H:%M:%S".to_string())
        .with_encoder_factory(Arc::new(FlinkJsonEncoderFactory {
            iso_8601: options.iso_8601,
            decimal_as_plain_number: options.decimal_as_plain_number,
            map_null_key_mode: options.map_null_key_mode,
            map_null_key_literal: {
                let mut literal = Vec::new();
                encode_json_string_value(options.map_null_key_literal.as_bytes(), &mut literal);
                literal
            },
        }));
    if options.iso_8601 {
        encoder_options = encoder_options
            .with_timestamp_format("%Y-%m-%dT%H:%M:%S%.f".to_string())
            .with_timestamp_tz_format("%Y-%m-%dT%H:%M:%S%.fZ".to_string());
    } else {
        encoder_options = encoder_options
            .with_timestamp_format("%Y-%m-%d %H:%M:%S%.f".to_string())
            .with_timestamp_tz_format("%Y-%m-%d %H:%M:%S%.fZ".to_string());
    }
    encoder_options
}

#[cfg(feature = "kafka")]
fn encode_json_rows(
    batch: &RecordBatch,
    encoder_options: &arrow::json::writer::EncoderOptions,
) -> Result<EncodedLines, String> {
    use arrow::json::writer::make_encoder;

    // Arrow buffers are a reliable lower bound for row-oriented JSON. Starting there avoids the
    // repeated grow-and-copy sequence that otherwise dominates wide Kafka batches, without
    // retaining an oversized encoder buffer after this batch returns.
    let mut bytes = Vec::with_capacity(batch.get_array_memory_size());
    let array = StructArray::from(batch.clone());
    let field = Arc::new(Field::new_struct(
        "",
        batch.schema().fields().clone(),
        false,
    ));
    let mut encoder = make_encoder(&field, &array, encoder_options)
        .map_err(|error| format!("failed to create Kafka JSON encoder: {error}"))?;
    let mut lines = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        let start = bytes.len();
        encoder.encode(index, &mut bytes);
        lines.push(start..bytes.len());
    }
    Ok(EncodedLines { bytes, lines })
}

/// The value lines of one resolved sink format: the format's plain rows, or — for a CDC format —
/// each JSON row spliced into its dialect's changelog envelope. `kinds` is the batch's
/// `$row_kind$` column; an absent column is an insert-only edge (every row is an INSERT),
/// matching the transpose contract in `changelog.rs`.
#[cfg(feature = "kafka")]
fn encode_value_lines(
    batch: &RecordBatch,
    kinds: Option<&Int8Array>,
    format: &EncodeOptions,
    logical_types: &[String],
    field_names: &[String],
    prepared_json: Option<&PreparedJsonEncoder>,
) -> Result<EncodedLines, String> {
    match format {
        EncodeOptions::Json { envelope, options } => {
            let rows = match prepared_json {
                Some(prepared) => prepared.encode(batch, logical_types, field_names)?,
                None => encode_json_batch(batch, options, logical_types, field_names)?,
            };
            match envelope {
                None => Ok(rows),
                Some(dialect) => {
                    wrap_cdc_envelopes(&rows, kinds, *dialect, options.ignore_null_fields)
                }
            }
        }
        #[cfg(feature = "csv")]
        EncodeOptions::Csv(options) => {
            crate::csv_encode::encode_csv_batch(batch, options, logical_types, field_names)
        }
        #[cfg(feature = "avro")]
        EncodeOptions::Avro(options) => {
            crate::avro::encode_avro_batch(batch, options, logical_types, field_names)
        }
        #[cfg(feature = "avro")]
        EncodeOptions::DebeziumAvro(options) => crate::avro::encode_debezium_avro_batch(
            batch,
            kinds,
            options,
            logical_types,
            field_names,
        ),
        #[cfg(feature = "protobuf")]
        EncodeOptions::Protobuf(options) => {
            // Columns map to proto fields by name, so the batch must carry the declared sink
            // field names, not the plan's generated expression names. An all-unset row is a
            // zero-length message (Flink's serializer produces the same empty byte[], not a
            // tombstone).
            let batch = annotate_flink_types(batch, logical_types, field_names)?;
            let (bytes, rows) = options.encoder().encode(&batch).into_parts();
            Ok(EncodedLines::new(bytes, rows))
        }
        #[cfg(feature = "raw")]
        EncodeOptions::Raw(options) => {
            let (bytes, rows) = crate::raw_encode::encode_raw_batch(batch, options)?;
            Ok(EncodedLines::new(bytes, rows))
        }
    }
}

/// Reusable portion of Arrow's JSON writer. The row encoder itself borrows the current batch, but
/// its format/factory configuration and the Flink logical-type annotation plan are task-local.
#[cfg(feature = "kafka")]
struct PreparedJsonEncoder {
    encoder_options: arrow::json::writer::EncoderOptions,
    annotation: std::cell::RefCell<Option<BatchAnnotationPlan>>,
}

#[cfg(feature = "kafka")]
impl PreparedJsonEncoder {
    fn new(options: &JsonEncodeOptions) -> Self {
        Self {
            encoder_options: json_encoder_options(options),
            annotation: std::cell::RefCell::new(None),
        }
    }

    fn encode(
        &self,
        batch: &RecordBatch,
        logical_types: &[String],
        field_names: &[String],
    ) -> Result<EncodedLines, String> {
        let batch = {
            let mut annotation = self.annotation.borrow_mut();
            if annotation.is_none() {
                *annotation = Some(BatchAnnotationPlan::new(batch, logical_types, field_names)?);
            }
            annotation.as_ref().unwrap().apply(batch)?
        };
        encode_json_rows(&batch, &self.encoder_options)
    }
}

/// Splices each pre-encoded JSON row into its CDC changelog envelope. The row bytes are identical
/// whether the object is top-level or nested, and the envelope fields around it are constants in
/// the dialect's declared field order — so parity with Flink's envelope serializers reduces to the
/// row encode parity already pinned for the plain JSON sink. Flink serializes the envelope with
/// the same JSON options as the row, so `encode.ignore-null-fields` also drops the envelope's
/// null `before`/`after` key.
#[cfg(feature = "kafka")]
fn wrap_cdc_envelopes(
    rows: &EncodedLines,
    kinds: Option<&Int8Array>,
    dialect: CdcEnvelope,
    ignore_null_fields: bool,
) -> Result<EncodedLines, String> {
    let mut bytes = Vec::with_capacity(rows.bytes.len() + rows.len() * 40);
    let mut lines = Vec::with_capacity(rows.len());
    for index in 0..rows.len() {
        // RowKind byte values: INSERT=0, UPDATE_BEFORE=1, UPDATE_AFTER=2, DELETE=3. Insert and
        // update-after carry the row as the post-image; update-before and delete as the pre-image.
        let insert = match kinds.map_or(0, |kinds| kinds.value(index)) {
            0 | 2 => true,
            1 | 3 => false,
            other => return Err(format!("Unsupported operation '{other}' for row kind.")),
        };
        let (prefix, suffix) = cdc_envelope(dialect, insert, ignore_null_fields);
        let start = bytes.len();
        bytes.extend_from_slice(prefix);
        bytes.extend_from_slice(rows.line(index));
        bytes.extend_from_slice(suffix);
        lines.push(start..bytes.len());
    }
    Ok(EncodedLines { bytes, lines })
}

/// The constant envelope bytes around the row image, per dialect and changelog side, in each
/// Flink serializer's declared field order.
#[cfg(feature = "kafka")]
fn cdc_envelope(
    dialect: CdcEnvelope,
    insert: bool,
    ignore_null_fields: bool,
) -> (&'static [u8], &'static [u8]) {
    match dialect {
        CdcEnvelope::Debezium => match (insert, ignore_null_fields) {
            (true, false) => (b"{\"before\":null,\"after\":", b",\"op\":\"c\"}"),
            (true, true) => (b"{\"after\":", b",\"op\":\"c\"}"),
            (false, false) => (b"{\"before\":", b",\"after\":null,\"op\":\"d\"}"),
            (false, true) => (b"{\"before\":", b",\"op\":\"d\"}"),
        },
        CdcEnvelope::Ogg => match (insert, ignore_null_fields) {
            (true, false) => (b"{\"before\":null,\"after\":", b",\"op_type\":\"I\"}"),
            (true, true) => (b"{\"after\":", b",\"op_type\":\"I\"}"),
            (false, false) => (b"{\"before\":", b",\"after\":null,\"op_type\":\"D\"}"),
            (false, true) => (b"{\"before\":", b",\"op_type\":\"D\"}"),
        },
        CdcEnvelope::Canal => {
            if insert {
                (b"{\"data\":[", b"],\"type\":\"INSERT\"}")
            } else {
                (b"{\"data\":[", b"],\"type\":\"DELETE\"}")
            }
        }
        CdcEnvelope::Maxwell => {
            if insert {
                (b"{\"data\":", b",\"type\":\"insert\"}")
            } else {
                (b"{\"data\":", b",\"type\":\"delete\"}")
            }
        }
    }
}

/// Per-row key/value slices into the two encode buffers; upsert DELETE/UPDATE_BEFORE rows read
/// back as tombstones (a key with no value).
#[cfg(feature = "kafka")]
struct EncodedKafkaRecords {
    keys: Option<EncodedLines>,
    values: EncodedLines,
    tombstones: Vec<bool>,
}

#[cfg(feature = "kafka")]
fn project_strings(values: &[String], fields: &[usize]) -> Result<Vec<String>, String> {
    fields
        .iter()
        .map(|index| {
            values
                .get(*index)
                .cloned()
                .ok_or_else(|| format!("Kafka encoder projection index {index} is out of bounds"))
        })
        .collect()
}

#[cfg(feature = "kafka")]
impl EncodedKafkaRecords {
    fn len(&self) -> usize {
        self.values.len()
    }

    fn key(&self, index: usize) -> Option<&[u8]> {
        self.keys.as_ref().map(|keys| keys.line(index))
    }

    fn value(&self, index: usize) -> Option<&[u8]> {
        if self.tombstones.get(index).copied().unwrap_or(false) {
            None
        } else {
            Some(self.values.line(index))
        }
    }
}

#[cfg(feature = "kafka")]
fn encode_records(
    batch: &RecordBatch,
    options: &EncodeOptions,
    key_options: &EncodeOptions,
    key_fields: &[usize],
    value_fields: &[usize],
    key_logical_types: &[String],
    key_field_names: &[String],
    value_logical_types: &[String],
    value_field_names: &[String],
    upsert: bool,
    key_json: Option<&PreparedJsonEncoder>,
    value_json: Option<&PreparedJsonEncoder>,
) -> Result<EncodedKafkaRecords, String> {
    let key_batch = batch
        .project(key_fields)
        .map_err(|error| format!("failed to project Kafka key fields: {error}"))?;
    let value_batch = batch
        .project(value_fields)
        .map_err(|error| format!("failed to project Kafka value fields: {error}"))?;
    let keys = if key_fields.is_empty() {
        None
    } else {
        // The key format never carries a CDC envelope (CDC formats are not legal key formats)
        // and the key row has no changelog kind of its own.
        Some(encode_value_lines(
            &key_batch,
            None,
            key_options,
            key_logical_types,
            key_field_names,
            key_json,
        )?)
    };
    let values = encode_value_lines(
        &value_batch,
        row_kind_column(batch),
        options,
        value_logical_types,
        value_field_names,
        value_json,
    )?;
    let key_lines = keys.as_ref().map_or(batch.num_rows(), EncodedLines::len);
    if values.len() != batch.num_rows() || key_lines != batch.num_rows() {
        return Err(format!(
            "Kafka encoder produced {} values and {} keys for {} Arrow rows",
            values.len(),
            key_lines,
            batch.num_rows()
        ));
    }
    let tombstones = if upsert {
        match row_kind_column(batch) {
            Some(kinds) => (0..batch.num_rows())
                .map(|index| matches!(kinds.value(index), 1 | 3))
                .collect(),
            // An insert-only edge carries no hidden row-kind column (the transpose contract in
            // `changelog.rs`): every row is an INSERT and serializes a value, never a tombstone —
            // Flink's upsert sink over an append stream behaves the same.
            None => vec![false; batch.num_rows()],
        }
    } else {
        Vec::new()
    };
    Ok(EncodedKafkaRecords {
        keys,
        values,
        tombstones,
    })
}

/// Task-local sink encoder plan. Flink constructs one serialization schema per operator instance;
/// mirror that lifecycle natively so JNI strings/arrays and format options are not reparsed for
/// every Arrow batch.
#[cfg(feature = "kafka")]
struct NativeKafkaEncoder {
    options: EncodeOptions,
    key_options: EncodeOptions,
    key_fields: Vec<usize>,
    value_fields: Vec<usize>,
    key_logical_types: Vec<String>,
    key_field_names: Vec<String>,
    value_logical_types: Vec<String>,
    value_field_names: Vec<String>,
    upsert: bool,
    key_json: Option<PreparedJsonEncoder>,
    value_json: Option<PreparedJsonEncoder>,
}

#[cfg(feature = "kafka")]
impl NativeKafkaEncoder {
    fn encode(&self, batch: &RecordBatch) -> Result<EncodedKafkaRecords, String> {
        encode_records(
            batch,
            &self.options,
            &self.key_options,
            &self.key_fields,
            &self.value_fields,
            &self.key_logical_types,
            &self.key_field_names,
            &self.value_logical_types,
            &self.value_field_names,
            self.upsert,
            self.key_json.as_ref(),
            self.value_json.as_ref(),
        )
    }
}

/// Rebuilds the batch onto the declared sink boundary: each column takes its declared field name
/// (the input plan may carry generated expression names) and has its TIMESTAMP_LTZ leaves re-marked
/// (see `mark_ltz_leaves`), so the encoders below need no side channel.
#[cfg(feature = "kafka")]
pub(crate) fn annotate_flink_types(
    batch: &RecordBatch,
    logical_types: &[String],
    field_names: &[String],
) -> Result<RecordBatch, String> {
    BatchAnnotationPlan::new(batch, logical_types, field_names)?.apply(batch)
}

#[cfg(feature = "kafka")]
struct BatchAnnotationPlan {
    input_types: Vec<DataType>,
    schema: SchemaRef,
    columns: Vec<LtzMarkPlan>,
}

#[cfg(feature = "kafka")]
impl BatchAnnotationPlan {
    fn new(
        batch: &RecordBatch,
        logical_types: &[String],
        field_names: &[String],
    ) -> Result<Self, String> {
        if logical_types.is_empty() && field_names.is_empty() {
            return Ok(Self {
                input_types: batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect(),
                schema: batch.schema(),
                columns: vec![LtzMarkPlan::Identity; batch.num_columns()],
            });
        }
        if logical_types.len() != batch.num_columns() || field_names.len() != batch.num_columns() {
            return Err(format!(
                "Kafka JSON encoder received {} logical types and {} names for {} columns",
                logical_types.len(),
                field_names.len(),
                batch.num_columns()
            ));
        }
        let columns = batch
            .schema()
            .fields()
            .iter()
            .zip(logical_types)
            .map(|(field, descriptor)| LtzMarkPlan::new(field.data_type(), descriptor))
            .collect::<Result<Vec<_>, String>>()?;
        let annotated = columns
            .iter()
            .zip(batch.columns())
            .map(|(plan, column)| plan.apply(column.clone()))
            .collect::<Result<Vec<_>, String>>()?;
        let fields = batch
            .schema()
            .fields()
            .iter()
            .zip(field_names)
            .zip(&annotated)
            .map(|((field, name), column)| {
                field
                    .as_ref()
                    .clone()
                    .with_name(name)
                    .with_data_type(column.data_type().clone())
            })
            .collect::<Vec<_>>();
        Ok(Self {
            input_types: batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect(),
            schema: Arc::new(Schema::new_with_metadata(
                fields,
                batch.schema().metadata().clone(),
            )),
            columns,
        })
    }

    fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch, String> {
        if batch.num_columns() != self.columns.len()
            || batch
                .schema()
                .fields()
                .iter()
                .zip(&self.input_types)
                .any(|(field, expected)| field.data_type() != expected)
        {
            return Err("Kafka encoder input schema changed after operator open".to_string());
        }
        let columns = self
            .columns
            .iter()
            .zip(batch.columns())
            .map(|(plan, column)| plan.apply(column.clone()))
            .collect::<Result<Vec<_>, String>>()?;
        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|error| format!("failed to annotate Kafka JSON schema: {error}"))
    }
}

#[cfg(feature = "kafka")]
#[derive(Clone)]
enum LtzMarkPlan {
    Identity,
    Timestamp,
    Struct(Vec<LtzMarkPlan>),
    List(Box<LtzMarkPlan>),
    LargeList(Box<LtzMarkPlan>),
    Map(Box<LtzMarkPlan>, Box<LtzMarkPlan>),
}

#[cfg(feature = "kafka")]
impl LtzMarkPlan {
    fn new(data_type: &DataType, descriptor: &str) -> Result<Self, String> {
        if !descriptor.contains("TIMESTAMP_LTZ") {
            return Ok(Self::Identity);
        }
        let children = |expected: usize| -> Result<Vec<&str>, String> {
            let children = descriptor_children(descriptor).ok_or_else(|| {
                format!("Flink descriptor {descriptor} does not match an Arrow container")
            })?;
            if children.len() != expected {
                return Err(format!(
                    "Flink descriptor {descriptor} has {} children for {expected} Arrow children",
                    children.len()
                ));
            }
            Ok(children)
        };
        match data_type {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
                if descriptor.starts_with("TIMESTAMP_LTZ") =>
            {
                Ok(Self::Timestamp)
            }
            DataType::Struct(fields) => Ok(Self::Struct(
                fields
                    .iter()
                    .zip(children(fields.len())?)
                    .map(|(field, child)| Self::new(field.data_type(), child))
                    .collect::<Result<Vec<_>, String>>()?,
            )),
            DataType::List(field) => Ok(Self::List(Box::new(Self::new(
                field.data_type(),
                children(1)?[0],
            )?))),
            DataType::LargeList(field) => Ok(Self::LargeList(Box::new(Self::new(
                field.data_type(),
                children(1)?[0],
            )?))),
            DataType::Map(field, _) => {
                let DataType::Struct(fields) = field.data_type() else {
                    return Err("Arrow map entry type is not a struct".to_string());
                };
                let descriptors = children(2)?;
                Ok(Self::Map(
                    Box::new(Self::new(fields[0].data_type(), descriptors[0])?),
                    Box::new(Self::new(fields[1].data_type(), descriptors[1])?),
                ))
            }
            _ => Ok(Self::Identity),
        }
    }

    fn apply(&self, array: ArrayRef) -> Result<ArrayRef, String> {
        use arrow::array::cast::AsArray;
        use arrow::array::{LargeListArray, ListArray, MapArray, StructArray};
        use arrow::datatypes::TimestampNanosecondType;

        match self {
            Self::Identity => Ok(array),
            Self::Timestamp => Ok(Arc::new(
                array
                    .as_primitive::<TimestampNanosecondType>()
                    .clone()
                    .with_timezone("UTC"),
            )),
            Self::Struct(plans) => {
                let (fields, columns, nulls) = array.as_struct().clone().into_parts();
                let columns = columns
                    .into_iter()
                    .zip(plans)
                    .map(|(column, plan)| plan.apply(column))
                    .collect::<Result<Vec<_>, String>>()?;
                let fields = fields
                    .iter()
                    .zip(&columns)
                    .map(|(field, column)| {
                        Arc::new(
                            field
                                .as_ref()
                                .clone()
                                .with_data_type(column.data_type().clone()),
                        )
                    })
                    .collect::<Vec<_>>();
                Ok(Arc::new(StructArray::new(fields.into(), columns, nulls)))
            }
            Self::List(plan) => {
                let (field, offsets, values, nulls) = array.as_list::<i32>().clone().into_parts();
                let values = plan.apply(values)?;
                let field = Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(values.data_type().clone()),
                );
                Ok(Arc::new(ListArray::new(field, offsets, values, nulls)))
            }
            Self::LargeList(plan) => {
                let (field, offsets, values, nulls) = array.as_list::<i64>().clone().into_parts();
                let values = plan.apply(values)?;
                let field = Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(values.data_type().clone()),
                );
                Ok(Arc::new(LargeListArray::new(field, offsets, values, nulls)))
            }
            Self::Map(key_plan, value_plan) => {
                let map = array.as_map();
                let offsets = map.offsets().clone();
                let map_nulls = map.nulls().cloned();
                let ordered = match array.data_type() {
                    DataType::Map(_, ordered) => *ordered,
                    _ => unreachable!("map plan applied to non-map array"),
                };
                let (fields, columns, nulls) = map.entries().clone().into_parts();
                let plans = [key_plan.as_ref(), value_plan.as_ref()];
                let columns = columns
                    .into_iter()
                    .zip(plans)
                    .map(|(column, plan)| plan.apply(column))
                    .collect::<Result<Vec<_>, String>>()?;
                let fields = fields
                    .iter()
                    .zip(&columns)
                    .map(|(field, column)| {
                        Arc::new(
                            field
                                .as_ref()
                                .clone()
                                .with_data_type(column.data_type().clone()),
                        )
                    })
                    .collect::<Vec<_>>();
                let entries = StructArray::new(fields.into(), columns, nulls);
                let DataType::Map(entry_field, _) = array.data_type() else {
                    unreachable!("map plan applied to non-map array");
                };
                let entry_field = Arc::new(
                    entry_field
                        .as_ref()
                        .clone()
                        .with_data_type(entries.data_type().clone()),
                );
                Ok(Arc::new(MapArray::new(
                    entry_field,
                    offsets,
                    entries,
                    map_nulls,
                    ordered,
                )))
            }
        }
    }
}

/// The child descriptors of a container descriptor (`ROW<...>`, `ARRAY<...>`, `MAP<k,v>`), or None
/// for a scalar leaf. Children are comma-split at bracket depth zero, so a scalar spelling with
/// inner commas (`DECIMAL(10, 2)`) stays whole.
#[cfg(feature = "kafka")]
fn descriptor_children(descriptor: &str) -> Option<Vec<&str>> {
    let (root, rest) = descriptor.split_once('<')?;
    if !matches!(root, "ROW" | "ARRAY" | "MAP") {
        return None;
    }
    let inner = rest.strip_suffix('>')?;
    let mut children = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (index, byte) in inner.bytes().enumerate() {
        match byte {
            b'<' | b'(' => depth += 1,
            b'>' | b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                children.push(inner[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    children.push(inner[start..].trim());
    Some(children)
}

#[cfg(feature = "kafka")]
/// Overrides the Arrow JSON defaults whose wire representation differs from Flink's Jackson format.
#[derive(Debug)]
struct FlinkJsonEncoderFactory {
    iso_8601: bool,
    decimal_as_plain_number: bool,
    map_null_key_mode: MapNullKeyMode,
    /// The `map-null-key.literal` pre-rendered as a quoted, escaped JSON field name.
    map_null_key_literal: Vec<u8>,
}

#[cfg(feature = "kafka")]
impl arrow::json::writer::EncoderFactory for FlinkJsonEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a arrow::json::writer::EncoderOptions,
    ) -> Result<Option<arrow::json::writer::NullableEncoder<'a>>, arrow::error::ArrowError> {
        use arrow::array::cast::AsArray;
        use arrow::datatypes::{Decimal128Type, TimestampNanosecondType};
        use arrow::json::writer::{Encoder, NullableEncoder};

        let encoder: Option<Box<dyn Encoder + 'a>> = match array.data_type() {
            DataType::Utf8 => Some(Box::new(FlinkStringEncoder {
                array: array.as_string::<i32>(),
            })),
            DataType::LargeUtf8 => Some(Box::new(FlinkStringEncoder {
                array: array.as_string::<i64>(),
            })),
            DataType::Binary => Some(Box::new(FlinkBinaryEncoder {
                array: array.as_binary::<i32>(),
            })),
            DataType::LargeBinary => Some(Box::new(FlinkBinaryEncoder {
                array: array.as_binary::<i64>(),
            })),
            // BINARY(n) crosses the boundary as FixedSizeBinary; arrow-json's stock encoder
            // renders that as hex where Flink base64-encodes every binary flavor alike.
            DataType::FixedSizeBinary(_) => Some(Box::new(FlinkFixedSizeBinaryEncoder {
                array: array.as_fixed_size_binary(),
            })),
            DataType::Decimal128(_, scale) => Some(Box::new(FlinkDecimal128Encoder {
                array: array.as_primitive::<Decimal128Type>(),
                scale: *scale,
                plain: self.decimal_as_plain_number,
            })),
            // TIMESTAMP_LTZ is an instant Flink renders at UTC with a 'Z' designator; plain
            // TIMESTAMP is the same wall-clock digit layout without any zone. Both trim the
            // fraction to its shortest form (appendFraction(NANO_OF_SECOND, 0, 9, true)). The
            // boundary marks LTZ leaves with a UTC timezone (`mark_ltz_leaves`), so the Arrow type
            // alone selects the designator — at any nesting depth, since arrow-json's container
            // encoders consult this factory recursively.
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, timezone) => {
                Some(Box::new(FlinkTimestampEncoder {
                    array: array.as_primitive::<TimestampNanosecondType>(),
                    iso_8601: self.iso_8601,
                    zulu: timezone.is_some(),
                }))
            }
            // Flink's map converter differs from arrow-json's stock one in every null rule: null
            // VALUES are always written (ignore-null-fields governs row fields only), and null
            // KEYS follow `json.map-null-key.mode` instead of being rejected outright.
            DataType::Map(_, _) => Some(Box::new(FlinkMapEncoder::try_new(
                array.as_map(),
                options,
                self.map_null_key_mode,
                self.map_null_key_literal.clone(),
            )?)),
            // DATE is ISO_LOCAL_DATE with SignStyle.EXCEEDS_PAD years ('+' past four digits, '-'
            // for negative years); arrow-json's stock chrono rendering differs outside [0, 9999].
            DataType::Date32 => Some(Box::new(FlinkDateEncoder {
                array: array.as_primitive::<arrow::datatypes::Date32Type>(),
            })),
            // FLOAT/DOUBLE take the JVM's legacy Double.toString spelling (see jdk_double), not
            // arrow-json's shortest-digits rendering; the plan-time probe guarantees the host JVM
            // still spells that way. Non-finite values follow Jackson's default
            // QUOTE_NON_NUMERIC_NUMBERS: "NaN", "Infinity", "-Infinity" as quoted strings.
            DataType::Float64 => Some(Box::new(FlinkDoubleEncoder {
                array: array.as_primitive::<arrow::datatypes::Float64Type>(),
            })),
            DataType::Float32 => Some(Box::new(FlinkFloatEncoder {
                array: array.as_primitive::<arrow::datatypes::Float32Type>(),
            })),
            _ => None,
        };
        Ok(encoder.map(|encoder| NullableEncoder::new(encoder, array.nulls().cloned())))
    }
}

/// Flink's DOUBLE spelling: `Double.toString` digits raw when finite, quoted `String.valueOf`
/// otherwise (Jackson's non-numeric quoting) — none of which ever needs JSON escaping.
#[cfg(feature = "kafka")]
struct FlinkDoubleEncoder<'a> {
    array: &'a arrow::array::Float64Array,
}

#[cfg(feature = "kafka")]
impl arrow::json::writer::Encoder for FlinkDoubleEncoder<'_> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        let value = self.array.value(index);
        if value.is_finite() {
            crate::jdk_double::jdk_double_to_string(value, output);
        } else {
            output.push(b'"');
            crate::jdk_double::jdk_double_to_string(value, output);
            output.push(b'"');
        }
    }
}

/// Flink's FLOAT spelling: `Float.toString` of the single-precision value (Flink's converter
/// builds a `FloatNode`, never promoting to double).
#[cfg(feature = "kafka")]
struct FlinkFloatEncoder<'a> {
    array: &'a arrow::array::Float32Array,
}

#[cfg(feature = "kafka")]
impl arrow::json::writer::Encoder for FlinkFloatEncoder<'_> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        let value = self.array.value(index);
        if value.is_finite() {
            crate::jdk_double::jdk_float_to_string(value, output);
        } else {
            output.push(b'"');
            crate::jdk_double::jdk_float_to_string(value, output);
            output.push(b'"');
        }
    }
}

/// Flink's DATE spelling — `ISO_LOCAL_DATE` with `SignStyle.EXCEEDS_PAD` years — shared with the
/// CSV sink's writer, quoted for JSON.
#[cfg(feature = "kafka")]
struct FlinkDateEncoder<'a> {
    array: &'a arrow::array::Date32Array,
}

#[cfg(feature = "kafka")]
impl arrow::json::writer::Encoder for FlinkDateEncoder<'_> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        output.push(b'"');
        iso_local_date(i64::from(self.array.value(index)), output);
        output.push(b'"');
    }
}

/// Flink's JSON object encoding of MAP and MULTISET (a MULTISET arrives as MAP<element, INT>).
/// Entries land in Jackson `ObjectNode` semantics: a duplicate key keeps its first position but
/// takes the last value, which is also how several null keys collapse onto one LITERAL spelling —
/// hence the per-row buffering. Key and value child encoders come from `make_encoder`, so every
/// Flink override applies inside the map.
#[cfg(feature = "kafka")]
struct FlinkMapEncoder<'a> {
    offsets: arrow::buffer::OffsetBuffer<i32>,
    keys: arrow::json::writer::NullableEncoder<'a>,
    values: arrow::json::writer::NullableEncoder<'a>,
    null_key_mode: MapNullKeyMode,
    /// Pre-rendered quoted, escaped `map-null-key.literal`.
    null_key_literal: Vec<u8>,
    scratch: Vec<u8>,
}

#[cfg(feature = "kafka")]
impl<'a> FlinkMapEncoder<'a> {
    fn try_new(
        array: &'a arrow::array::MapArray,
        options: &'a arrow::json::writer::EncoderOptions,
        null_key_mode: MapNullKeyMode,
        null_key_literal: Vec<u8>,
    ) -> Result<FlinkMapEncoder<'a>, arrow::error::ArrowError> {
        use arrow::json::writer::make_encoder;

        let DataType::Map(entry_field, _) = array.data_type() else {
            unreachable!("FlinkMapEncoder built for a non-map array");
        };
        let DataType::Struct(entry_fields) = entry_field.data_type() else {
            return Err(arrow::error::ArrowError::JsonError(
                "map entries are not a struct".to_string(),
            ));
        };
        if !matches!(
            array.keys().data_type(),
            DataType::Utf8 | DataType::LargeUtf8
        ) {
            // The planner declines non-string keys (Flink itself throws for them); reaching this
            // is a wiring bug, not user input.
            return Err(arrow::error::ArrowError::JsonError(format!(
                "JSON format doesn't support non-string as key type of map: {}",
                array.keys().data_type()
            )));
        }
        Ok(FlinkMapEncoder {
            offsets: array.offsets().clone(),
            keys: make_encoder(&entry_fields[0], array.keys().as_ref(), options)?,
            values: make_encoder(&entry_fields[1], array.values().as_ref(), options)?,
            null_key_mode,
            null_key_literal,
            scratch: Vec::new(),
        })
    }
}

#[cfg(feature = "kafka")]
impl arrow::json::writer::Encoder for FlinkMapEncoder<'_> {
    fn encode(&mut self, idx: usize, output: &mut Vec<u8>) {
        let start = self.offsets[idx] as usize;
        let end = self.offsets[idx + 1] as usize;
        self.scratch.clear();
        let mut entries: Vec<(std::ops::Range<usize>, std::ops::Range<usize>)> =
            Vec::with_capacity(end - start);
        for index in start..end {
            let key_start = self.scratch.len();
            if self.keys.is_null(index) {
                match self.null_key_mode {
                    MapNullKeyMode::Fail => panic!(
                        "JSON format doesn't support to serialize map data with null keys. You \
                         can drop null key entries or encode null in literals by specifying the \
                         json.map-null-key.mode option."
                    ),
                    MapNullKeyMode::Drop => continue,
                    MapNullKeyMode::Literal => {
                        self.scratch.extend_from_slice(&self.null_key_literal)
                    }
                }
            } else {
                self.keys.encode(index, &mut self.scratch);
            }
            let key = key_start..self.scratch.len();
            let value_start = self.scratch.len();
            if self.values.is_null(index) {
                self.scratch.extend_from_slice(b"null");
            } else {
                self.values.encode(index, &mut self.scratch);
            }
            let value = value_start..self.scratch.len();
            if let Some(existing) = entries
                .iter_mut()
                .find(|(seen, _)| self.scratch[seen.clone()] == self.scratch[key.clone()])
            {
                existing.1 = value;
            } else {
                entries.push((key, value));
            }
        }
        output.push(b'{');
        for (index, (key, value)) in entries.iter().enumerate() {
            if index != 0 {
                output.push(b',');
            }
            output.extend_from_slice(&self.scratch[key.clone()]);
            output.push(b':');
            output.extend_from_slice(&self.scratch[value.clone()]);
        }
        output.push(b'}');
    }
}

/// arrow-json's string path hands every value to serde_json's scalar per-byte escape scan. The
/// overwhelmingly common value needs no escaping at all, so this encoder finds that out with a
/// word-at-a-time scan and bulk-copies; values that do escape go through a loop applying
/// Jackson's table — serde_json's escape set (`\"`, `\\`, named controls, `\u00XX`) but with
/// Jackson's default UPPERCASE hex digits (`WRITE_HEX_UPPER_CASE`), where serde_json spells
/// lowercase. Pinned against Flink's serializer in the Java parity suite.
#[cfg(feature = "kafka")]
struct FlinkStringEncoder<'a, O: arrow::array::OffsetSizeTrait> {
    array: &'a arrow::array::GenericStringArray<O>,
}

#[cfg(feature = "kafka")]
impl<O: arrow::array::OffsetSizeTrait> arrow::json::writer::Encoder for FlinkStringEncoder<'_, O> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        encode_json_string_value(self.array.value(index).as_bytes(), output);
    }
}

/// One quoted, Jackson-escaped JSON string value.
#[cfg(feature = "kafka")]
fn encode_json_string_value(value: &[u8], output: &mut Vec<u8>) {
    output.reserve(value.len() + 2);
    output.push(b'"');
    if json_needs_escape(value) {
        encode_escaped_json(value, output);
    } else {
        output.extend_from_slice(value);
    }
    output.push(b'"');
}

/// Whether any byte is a control character, `"`, or `\` — the only bytes JSON string encoding
/// touches (UTF-8 continuation bytes are ≥ 0x80 and never match). Eight bytes per step via the
/// usual SWAR zero-byte/less-than masks.
#[cfg(feature = "kafka")]
fn json_needs_escape(bytes: &[u8]) -> bool {
    const LOW: u64 = 0x0101_0101_0101_0101;
    const HIGH: u64 = 0x8080_8080_8080_8080;
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let word = u64::from_le_bytes(chunk.try_into().expect("8-byte chunk"));
        let control = word.wrapping_sub(LOW * 0x20) & !word;
        let quote = word ^ (LOW * u64::from(b'"'));
        let quote = quote.wrapping_sub(LOW) & !quote;
        let backslash = word ^ (LOW * u64::from(b'\\'));
        let backslash = backslash.wrapping_sub(LOW) & !backslash;
        if (control | quote | backslash) & HIGH != 0 {
            return true;
        }
    }
    chunks
        .remainder()
        .iter()
        .any(|&byte| byte < 0x20 || byte == b'"' || byte == b'\\')
}

/// Jackson's escape table, applied over unescaped runs (without the surrounding quotes): the
/// same escape set as serde_json, but `\u00XX` hex digits are uppercase — Jackson's
/// `WRITE_HEX_UPPER_CASE` default, visible on 0x0B, 0x0E, 0x0F, and 0x1A–0x1F.
#[cfg(feature = "kafka")]
fn encode_escaped_json(bytes: &[u8], output: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut start = 0;
    for (index, &byte) in bytes.iter().enumerate() {
        let escape: &[u8] = match byte {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            0x08 => b"\\b",
            b'\t' => b"\\t",
            b'\n' => b"\\n",
            0x0c => b"\\f",
            b'\r' => b"\\r",
            byte if byte < 0x20 => &[],
            _ => continue,
        };
        output.extend_from_slice(&bytes[start..index]);
        if escape.is_empty() {
            output.extend_from_slice(&[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[(byte >> 4) as usize],
                HEX[(byte & 0xf) as usize],
            ]);
        } else {
            output.extend_from_slice(escape);
        }
        start = index + 1;
    }
    output.extend_from_slice(&bytes[start..]);
}

#[cfg(feature = "kafka")]
struct FlinkBinaryEncoder<'a, O: arrow::array::OffsetSizeTrait> {
    array: &'a arrow::array::GenericBinaryArray<O>,
}

#[cfg(feature = "kafka")]
impl<O: arrow::array::OffsetSizeTrait> arrow::json::writer::Encoder for FlinkBinaryEncoder<'_, O> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        use base64::Engine;
        output.push(b'"');
        let input = self.array.value(index);
        let start = output.len();
        let encoded_len = base64::encoded_len(input.len(), true).expect("base64 output length");
        output.resize(start + encoded_len, 0);
        base64::engine::general_purpose::STANDARD
            .encode_slice(input, &mut output[start..])
            .expect("sized base64 output");
        output.push(b'"');
    }
}

#[cfg(feature = "kafka")]
struct FlinkFixedSizeBinaryEncoder<'a> {
    array: &'a arrow::array::FixedSizeBinaryArray,
}

#[cfg(feature = "kafka")]
impl arrow::json::writer::Encoder for FlinkFixedSizeBinaryEncoder<'_> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        use base64::Engine;
        output.push(b'"');
        let input = self.array.value(index);
        let start = output.len();
        let encoded_len = base64::encoded_len(input.len(), true).expect("base64 output length");
        output.resize(start + encoded_len, 0);
        base64::engine::general_purpose::STANDARD
            .encode_slice(input, &mut output[start..])
            .expect("sized base64 output");
        output.push(b'"');
    }
}

#[cfg(feature = "kafka")]
struct FlinkDecimal128Encoder<'a> {
    array: &'a arrow::array::Decimal128Array,
    scale: i8,
    plain: bool,
}

#[cfg(feature = "kafka")]
impl arrow::json::writer::Encoder for FlinkDecimal128Encoder<'_> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        encode_java_big_decimal(self.array.value(index), self.scale, self.plain, output);
    }
}

/// Jackson's two `BigDecimal` spellings, replicated over the raw (unscaled, scale) pair. Plain mode
/// (`WRITE_BIGDECIMAL_AS_PLAIN`) is `toPlainString()` of the column's exact scale — trailing zeros
/// kept. Default mode is `stripTrailingZeros().toString()`: Java's `toString()` switches to
/// scientific notation when the stripped scale goes negative or the adjusted exponent drops below
/// -6, so `100.00` becomes `1E+2` while `123.450` becomes `123.45`.
#[cfg(feature = "kafka")]
pub(crate) fn encode_java_big_decimal(
    unscaled: i128,
    scale: i8,
    plain: bool,
    output: &mut Vec<u8>,
) {
    if plain {
        return encode_plain_decimal(unscaled, i64::from(scale), output);
    }
    let mut unscaled = unscaled;
    let mut scale = i64::from(scale);
    if unscaled == 0 {
        scale = 0;
    } else {
        while unscaled % 10 == 0 {
            unscaled /= 10;
            scale -= 1;
        }
    }
    let digits = unscaled.unsigned_abs().to_string();
    let adjusted_exponent = digits.len() as i64 - 1 - scale;
    if scale >= 0 && adjusted_exponent >= -6 {
        encode_plain_decimal(unscaled, scale, output);
        return;
    }
    if unscaled < 0 {
        output.push(b'-');
    }
    output.push(digits.as_bytes()[0]);
    if digits.len() > 1 {
        output.push(b'.');
        output.extend_from_slice(&digits.as_bytes()[1..]);
    }
    output.push(b'E');
    if adjusted_exponent > 0 {
        output.push(b'+');
    }
    output.extend_from_slice(adjusted_exponent.to_string().as_bytes());
}

/// `BigDecimal.toPlainString()`: positional digits with exactly `scale` fraction digits.
#[cfg(feature = "kafka")]
fn encode_plain_decimal(unscaled: i128, scale: i64, output: &mut Vec<u8>) {
    if unscaled < 0 {
        output.push(b'-');
    }
    let digits = unscaled.unsigned_abs().to_string();
    if scale <= 0 {
        output.extend_from_slice(digits.as_bytes());
        for _ in 0..-scale {
            output.push(b'0');
        }
    } else if digits.len() as i64 > scale {
        let split = digits.len() - scale as usize;
        output.extend_from_slice(digits[..split].as_bytes());
        output.push(b'.');
        output.extend_from_slice(digits[split..].as_bytes());
    } else {
        output.extend_from_slice(b"0.");
        for _ in 0..scale - digits.len() as i64 {
            output.push(b'0');
        }
        output.extend_from_slice(digits.as_bytes());
    }
}

#[cfg(feature = "kafka")]
struct FlinkTimestampEncoder<'a> {
    array: &'a arrow::array::TimestampNanosecondArray,
    iso_8601: bool,
    zulu: bool,
}

#[cfg(feature = "kafka")]
impl arrow::json::writer::Encoder for FlinkTimestampEncoder<'_> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        // Flink's formatters never consult the column's declared precision: the fraction is
        // whatever the value's nanoseconds carry, trimmed of trailing zeros, so the full
        // nine-digit width is always offered.
        encode_flink_timestamp(self.array.value(index), 9, self.iso_8601, self.zulu, output);
    }
}

#[cfg(feature = "kafka")]
pub(crate) fn encode_local_timestamp(
    value: i64,
    precision: usize,
    iso_8601: bool,
    output: &mut Vec<u8>,
) {
    encode_flink_timestamp(value, precision, iso_8601, true, output);
}

#[cfg(feature = "kafka")]
fn encode_flink_timestamp(
    value: i64,
    precision: usize,
    iso_8601: bool,
    zulu: bool,
    output: &mut Vec<u8>,
) {
    let seconds = value.div_euclid(1_000_000_000);
    let nanos = value.rem_euclid(1_000_000_000) as u32;
    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400) as u32;
    let (year, month, day) = civil_date_from_epoch_days(days);
    encode_timestamp_parts(
        year,
        month,
        day,
        second_of_day / 3_600,
        (second_of_day / 60) % 60,
        second_of_day % 60,
        nanos,
        precision,
        iso_8601,
        zulu,
        output,
    );
}

#[cfg(feature = "kafka")]
pub(crate) fn encode_local_timestamp_chrono_components(
    value: i64,
    precision: usize,
    iso_8601: bool,
    output: &mut Vec<u8>,
) {
    use chrono::{DateTime, Datelike, Timelike, Utc};
    let seconds = value.div_euclid(1_000_000_000);
    let nanos = value.rem_euclid(1_000_000_000) as u32;
    let timestamp = DateTime::<Utc>::from_timestamp(seconds, nanos).expect("valid Flink timestamp");
    encode_timestamp_parts(
        timestamp.year(),
        timestamp.month(),
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second(),
        nanos,
        precision,
        iso_8601,
        true,
        output,
    );
}

#[cfg(feature = "kafka")]
fn encode_timestamp_parts(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    nanos: u32,
    precision: usize,
    iso_8601: bool,
    zulu: bool,
    output: &mut Vec<u8>,
) {
    output.push(b'"');
    push_four_digits(output, year as u32);
    output.push(b'-');
    push_two_digits(output, month);
    output.push(b'-');
    push_two_digits(output, day);
    output.push(if iso_8601 { b'T' } else { b' ' });
    push_two_digits(output, hour);
    output.push(b':');
    push_two_digits(output, minute);
    output.push(b':');
    push_two_digits(output, second);
    if precision > 0 {
        let mut width = precision.min(9);
        let mut fraction = nanos / 10_u32.pow((9 - width) as u32);
        while fraction != 0 && fraction % 10 == 0 {
            fraction /= 10;
            width -= 1;
        }
        if fraction != 0 {
            output.push(b'.');
            let mut divisor = 10_u32.pow((width - 1) as u32);
            while divisor != 0 {
                output.push(b'0' + ((fraction / divisor) % 10) as u8);
                divisor /= 10;
            }
        }
    }
    if zulu {
        output.push(b'Z');
    }
    output.push(b'"');
}

#[cfg(feature = "kafka")]
/// `DateTimeFormatter.ISO_LOCAL_DATE`: the year at width four with `SignStyle.EXCEEDS_PAD`
/// (`+` past four digits, `-` for negative years), two-digit month and day. Shared by the JSON
/// sink's DATE encoder and the CSV sink's date writer (lives here so a connector build without
/// the csv feature still compiles it).
pub(crate) fn iso_local_date(epoch_days: i64, out: &mut Vec<u8>) {
    use std::io::Write;

    let (year, month, day) = civil_date_from_epoch_days(epoch_days);
    if year < 0 {
        out.push(b'-');
    } else if year >= 10_000 {
        out.push(b'+');
    }
    write!(out, "{:04}", year.unsigned_abs()).expect("year digits");
    out.push(b'-');
    push_two_digits(out, month);
    out.push(b'-');
    push_two_digits(out, day);
}

pub(crate) fn civil_date_from_epoch_days(days: i64) -> (i32, u32, u32) {
    // Decompose the proleptic Gregorian calendar into 400-year eras; the shift aligns Unix day 0.
    let shifted = days + 719_468;
    let era = (if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    }) / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

#[cfg(feature = "kafka")]
pub(crate) fn push_two_digits(output: &mut Vec<u8>, value: u32) {
    output.push(b'0' + (value / 10) as u8);
    output.push(b'0' + (value % 10) as u8);
}

#[cfg(feature = "kafka")]
fn push_four_digits(output: &mut Vec<u8>, value: u32) {
    output.push(b'0' + (value / 1000) as u8);
    output.push(b'0' + ((value / 100) % 10) as u8);
    output.push(b'0' + ((value / 10) % 10) as u8);
    output.push(b'0' + (value % 10) as u8);
}

#[cfg(feature = "kafka")]
pub(crate) fn encode_local_timestamp_chrono(
    value: i64,
    precision: usize,
    iso_8601: bool,
    output: &mut Vec<u8>,
) {
    use chrono::{DateTime, Utc};
    use std::io::Write;
    let seconds = value.div_euclid(1_000_000_000);
    let nanos = value.rem_euclid(1_000_000_000) as u32;
    let timestamp = DateTime::<Utc>::from_timestamp(seconds, nanos).expect("valid Flink timestamp");
    output.push(b'"');
    let separator = if iso_8601 { 'T' } else { ' ' };
    write!(
        output,
        "{}{separator}{}",
        timestamp.format("%Y-%m-%d"),
        timestamp.format("%H:%M:%S")
    )
    .expect("write timestamp");
    if precision > 0 {
        let mut precision = precision.min(9);
        let mut fraction = nanos / 10_u32.pow((9 - precision) as u32);
        while fraction != 0 && fraction % 10 == 0 {
            fraction /= 10;
            precision -= 1;
        }
        if fraction != 0 {
            write!(output, ".{fraction:0precision$}").expect("write timestamp fraction");
        }
    }
    output.extend_from_slice(b"Z\"");
}

#[cfg(all(test, feature = "kafka"))]
mod timestamp_encoder_tests {
    use super::{encode_local_timestamp, encode_local_timestamp_chrono};

    #[test]
    fn direct_timestamp_encoding_matches_chrono() {
        let mut values = vec![
            i64::MIN,
            -2_208_988_800_999_999_999,
            -1,
            0,
            951_827_696_123_456_789,
            1_700_000_000_100_000_000,
            i64::MAX,
        ];
        values.extend(
            (0..=2048_i128)
                .map(|index| (i64::MIN as i128 + (u64::MAX as i128 * index / 2048)) as i64),
        );
        for value in values {
            for precision in 0..=12 {
                for iso_8601 in [false, true] {
                    let mut direct = Vec::new();
                    let mut chrono = Vec::new();
                    encode_local_timestamp(value, precision, iso_8601, &mut direct);
                    encode_local_timestamp_chrono(value, precision, iso_8601, &mut chrono);
                    assert_eq!(
                        direct, chrono,
                        "value={value}, precision={precision}, iso={iso_8601}"
                    );
                }
            }
        }
    }
}

#[cfg(feature = "kafka")]
fn kafka_jni<T, F>(env: &mut JNIEnv, default: T, f: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> Result<T, String>,
{
    connector_jni(env, default, "native Kafka serialization panic", f)
}

/// The codec extension's build stamp for the loader compatibility check.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_nativeBuildVersion<'local>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
) -> jstring {
    crate::bridge::Java_tech_streamfusion_Native_version(env, class)
}

/// Whether the Kafka serialization extension loaded successfully.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_isLoaded<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jni::sys::jboolean {
    crate::bridge::jni_guard(env, move |_env| true as jni::sys::jboolean)
}

/// Imports a whole Arrow batch once and materializes the final `byte[]` values consumed by
/// KafkaProducer. The JNI boundary is batch-grained even though Kafka's Java API requires one heap
/// array per record.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_encodeKafkaBatch<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    array_address: jlong,
    schema_address: jlong,
    format: jint,
    format_options: JString<'local>,
    logical_types: JObjectArray<'local>,
    field_names: JObjectArray<'local>,
) -> jni::sys::jobjectArray {
    kafka_jni(&mut env, std::ptr::null_mut(), |env| {
        let batch = import_record_batch(array_address, schema_address);
        let options = read_encode_format(env, format, &format_options)?;
        let logical_types = read_string_array(env, &logical_types);
        let field_names = read_string_array(env, &field_names);
        let data_fields = (0..data_arity(&batch)).collect::<Vec<_>>();
        let data = batch
            .project(&data_fields)
            .map_err(|error| format!("failed to project Kafka value fields: {error}"))?;
        let encoded = encode_value_lines(
            &data,
            row_kind_column(&batch),
            &options,
            &logical_types,
            &field_names,
            None,
        )?;
        if encoded.len() != batch.num_rows() {
            return Err(format!(
                "Kafka encoder produced {} records for {} Arrow rows",
                encoded.len(),
                batch.num_rows()
            ));
        }
        let values = byte_array_array(env, encoded.len(), |index| Some(encoded.line(index)))?;
        Ok(values.into_raw())
    })
}

/// The planner's capability probe: whether THIS build of the connector library encodes the format
/// code. A format compiled out (its cargo feature off) reports false so the planner falls back to
/// Flink instead of accepting a query the runtime dispatch would fail.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_encodeFormatSupported<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    format: jint,
) -> jboolean {
    crate::bridge::jni_guard(env, move |_env| u8::from(encode_format_compiled(format)))
}

/// The FLOAT/DOUBLE admission probe's data plane: spells every value with the legacy JDK
/// algorithm (`jdk_double`), newline-terminated, doubles before floats. The JVM compares the
/// result against its own `Double.toString`/`Float.toString` over a corpus where the legacy and
/// shortest-representation algorithms are known to disagree, so a JDK 19+ host fails the probe
/// and FLOAT/DOUBLE columns fall back instead of silently diverging.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_spellFloatingPoint<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    doubles: JDoubleArray<'local>,
    floats: JFloatArray<'local>,
) -> jbyteArray {
    kafka_jni(&mut env, std::ptr::null_mut(), |env| {
        let mut out = Vec::new();
        for value in read_doubles(env, &doubles) {
            crate::jdk_double::jdk_double_to_string(value, &mut out);
            out.push(b'\n');
        }
        for value in read_floats(env, &floats) {
            crate::jdk_double::jdk_float_to_string(value, &mut out);
            out.push(b'\n');
        }
        let result = env
            .byte_array_from_slice(&out)
            .map_err(|error| format!("failed to materialize float spellings: {error}"))?;
        Ok(result.into_raw())
    })
}

/// Whether this build compiles an encode arm for the format code — capability only, never option
/// validation (a format like Avro has required options the probe cannot supply).
#[cfg(feature = "kafka")]
fn encode_format_compiled(format: i32) -> bool {
    match format {
        FORMAT_JSON | FORMAT_DEBEZIUM_JSON | FORMAT_OGG_JSON | FORMAT_MAXWELL_JSON
        | FORMAT_CANAL_JSON => true,
        #[cfg(feature = "csv")]
        FORMAT_CSV => true,
        #[cfg(feature = "avro")]
        FORMAT_AVRO | FORMAT_AVRO_CONFLUENT | FORMAT_DEBEZIUM_AVRO_CONFLUENT => true,
        #[cfg(feature = "protobuf")]
        FORMAT_PROTOBUF => true,
        #[cfg(feature = "raw")]
        FORMAT_RAW => true,
        _ => false,
    }
}

#[cfg(feature = "kafka")]
fn read_encode_format(
    env: &mut JNIEnv<'_>,
    format: jint,
    format_options: &JString<'_>,
) -> Result<EncodeOptions, String> {
    let encoded: String = env
        .get_string(format_options)
        .map_err(|error| format!("failed to read encode format options: {error}"))?
        .into();
    parse_encode_format(format, &encoded)
}

#[cfg(feature = "kafka")]
fn byte_array_array<'slices, 'local>(
    env: &mut JNIEnv<'local>,
    len: usize,
    mut record: impl FnMut(usize) -> Option<&'slices [u8]>,
) -> Result<jni::objects::JObjectArray<'local>, String> {
    let result = env
        .new_object_array(len as i32, "[B", jni::objects::JObject::null())
        .map_err(|error| format!("failed to allocate Kafka JSON result: {error}"))?;
    for index in 0..len {
        if let Some(value) = record(index) {
            let value = env
                .byte_array_from_slice(value)
                .map_err(|error| format!("failed to materialize Kafka JSON value: {error}"))?;
            env.set_object_array_element(&result, index as i32, value)
                .map_err(|error| format!("failed to store Kafka JSON value: {error}"))?;
        }
    }
    Ok(result)
}

#[cfg(feature = "kafka")]
fn int_array<'local>(
    env: &mut JNIEnv<'local>,
    values: &[i32],
) -> Result<JIntArray<'local>, String> {
    let output = env
        .new_int_array(values.len() as i32)
        .map_err(|error| format!("failed to allocate Kafka batch metadata: {error}"))?;
    env.set_int_array_region(&output, 0, values)
        .map_err(|error| format!("failed to materialize Kafka batch metadata: {error}"))?;
    Ok(output)
}

#[cfg(feature = "kafka")]
fn encoded_batch_object<'local>(
    env: &mut JNIEnv<'local>,
    records: &EncodedKafkaRecords,
) -> Result<jni::sys::jobject, String> {
    let (key_bytes, key_offsets, key_lengths) = match records.keys.as_ref() {
        Some(keys) => {
            let (offsets, lengths) = keys.metadata(&[])?;
            (&keys.bytes[..], offsets, lengths)
        }
        None => (&[][..], vec![0; records.len()], vec![-1; records.len()]),
    };
    let (value_offsets, value_lengths) = records.values.metadata(&records.tombstones)?;
    let key_bytes = env
        .byte_array_from_slice(key_bytes)
        .map_err(|error| format!("failed to materialize Kafka key slab: {error}"))?;
    let key_offsets = int_array(env, &key_offsets)?;
    let key_lengths = int_array(env, &key_lengths)?;
    let value_bytes = env
        .byte_array_from_slice(&records.values.bytes)
        .map_err(|error| format!("failed to materialize Kafka value slab: {error}"))?;
    let value_offsets = int_array(env, &value_offsets)?;
    let value_lengths = int_array(env, &value_lengths)?;
    let class = env
        .find_class("tech/streamfusion/kafka/NativeKafkaEncodedBatch")
        .map_err(|error| format!("failed to resolve Kafka batch class: {error}"))?;
    let result = env
        .new_object(
            class,
            "([B[I[I[B[I[I)V",
            &[
                jni::objects::JValue::Object(&key_bytes),
                jni::objects::JValue::Object(&key_offsets),
                jni::objects::JValue::Object(&key_lengths),
                jni::objects::JValue::Object(&value_bytes),
                jni::objects::JValue::Object(&value_offsets),
                jni::objects::JValue::Object(&value_lengths),
            ],
        )
        .map_err(|error| format!("failed to construct Kafka encoded batch: {error}"))?;
    Ok(result.into_raw())
}

/// Parses all sink format options and projections once at operator open, matching Flink's
/// task-local SerializationSchema lifecycle.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_createKafkaEncoder<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    format: jint,
    format_options: JString<'local>,
    key_format: jint,
    key_format_options: JString<'local>,
    logical_types: JObjectArray<'local>,
    field_names: JObjectArray<'local>,
    key_fields: JIntArray<'local>,
    value_fields: JIntArray<'local>,
    upsert: jboolean,
) -> jlong {
    kafka_jni(&mut env, 0, |env| {
        let options = read_encode_format(env, format, &format_options)?;
        let key_options = read_encode_format(env, key_format, &key_format_options)?;
        let value_json = match &options {
            EncodeOptions::Json { options, .. } => Some(PreparedJsonEncoder::new(options)),
            #[allow(unreachable_patterns)]
            _ => None,
        };
        let key_json = match &key_options {
            EncodeOptions::Json { options, .. } => Some(PreparedJsonEncoder::new(options)),
            #[allow(unreachable_patterns)]
            _ => None,
        };
        let logical_types = read_string_array(env, &logical_types);
        let field_names = read_string_array(env, &field_names);
        let key_fields = read_int_array(env, &key_fields)
            .into_iter()
            .map(|index| index as usize)
            .collect::<Vec<_>>();
        let value_fields = read_int_array(env, &value_fields)
            .into_iter()
            .map(|index| index as usize)
            .collect::<Vec<_>>();
        let encoder = NativeKafkaEncoder {
            options,
            key_options,
            key_logical_types: project_strings(&logical_types, &key_fields)?,
            key_field_names: project_strings(&field_names, &key_fields)?,
            value_logical_types: project_strings(&logical_types, &value_fields)?,
            value_field_names: project_strings(&field_names, &value_fields)?,
            key_fields,
            value_fields,
            upsert: upsert != 0,
            key_json,
            value_json,
        };
        Ok(crate::bridge::into_handle(encoder))
    })
}

#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_encodeKafkaRecordBatchWithHandle<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array_address: jlong,
    schema_address: jlong,
) -> jni::sys::jobject {
    kafka_jni(&mut env, std::ptr::null_mut(), |env| {
        let batch = import_record_batch(array_address, schema_address);
        let encoder = unsafe { &*(handle as *mut NativeKafkaEncoder) };
        let records = encoder.encode(&batch)?;
        encoded_batch_object(env, &records)
    })
}

#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_closeKafkaEncoder<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(crate::bridge::from_handle::<NativeKafkaEncoder>(handle));
    })
}

/// Serializes projected key/value rows together so an upsert batch crosses JNI once. Null values
/// are Kafka tombstones for DELETE and UPDATE_BEFORE, matching Flink's upsert-kafka schema.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_encodeKafkaRecords<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    array_address: jlong,
    schema_address: jlong,
    format: jint,
    format_options: JString<'local>,
    key_format: jint,
    key_format_options: JString<'local>,
    logical_types: JObjectArray<'local>,
    field_names: JObjectArray<'local>,
    key_fields: JIntArray<'local>,
    value_fields: JIntArray<'local>,
    upsert: jboolean,
) -> jni::sys::jobjectArray {
    kafka_jni(&mut env, std::ptr::null_mut(), |env| {
        let batch = import_record_batch(array_address, schema_address);
        let options = read_encode_format(env, format, &format_options)?;
        let key_options = read_encode_format(env, key_format, &key_format_options)?;
        let logical_types = read_string_array(env, &logical_types);
        let field_names = read_string_array(env, &field_names);
        let key_fields = read_int_array(env, &key_fields)
            .into_iter()
            .map(|index| index as usize)
            .collect::<Vec<_>>();
        let value_fields = read_int_array(env, &value_fields)
            .into_iter()
            .map(|index| index as usize)
            .collect::<Vec<_>>();
        let key_logical_types = project_strings(&logical_types, &key_fields)?;
        let key_field_names = project_strings(&field_names, &key_fields)?;
        let value_logical_types = project_strings(&logical_types, &value_fields)?;
        let value_field_names = project_strings(&field_names, &value_fields)?;
        let records = encode_records(
            &batch,
            &options,
            &key_options,
            &key_fields,
            &value_fields,
            &key_logical_types,
            &key_field_names,
            &value_logical_types,
            &value_field_names,
            upsert != 0,
            None,
            None,
        )?;
        let keys = byte_array_array(env, records.len(), |index| records.key(index))?;
        let values = byte_array_array(env, records.len(), |index| records.value(index))?;
        let result = env
            .new_object_array(2, "[[B", jni::objects::JObject::null())
            .map_err(|error| format!("failed to allocate Kafka record result: {error}"))?;
        env.set_object_array_element(&result, 0, keys)
            .map_err(|error| format!("failed to store Kafka keys: {error}"))?;
        env.set_object_array_element(&result, 1, values)
            .map_err(|error| format!("failed to store Kafka values: {error}"))?;
        Ok(result.into_raw())
    })
}

/// Returns each key/value column as one byte slab plus offsets and lengths. A negative length is a
/// null key or Kafka tombstone; zero remains a present empty byte array. This follows Comet's bulk
/// native-row handoff shape while keeping the slabs JVM-owned because Kafka retains each record.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_kafka_NativeKafka_encodeKafkaRecordBatch<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    array_address: jlong,
    schema_address: jlong,
    format: jint,
    format_options: JString<'local>,
    key_format: jint,
    key_format_options: JString<'local>,
    logical_types: JObjectArray<'local>,
    field_names: JObjectArray<'local>,
    key_fields: JIntArray<'local>,
    value_fields: JIntArray<'local>,
    upsert: jboolean,
) -> jni::sys::jobject {
    kafka_jni(&mut env, std::ptr::null_mut(), |env| {
        let batch = import_record_batch(array_address, schema_address);
        let options = read_encode_format(env, format, &format_options)?;
        let key_options = read_encode_format(env, key_format, &key_format_options)?;
        let logical_types = read_string_array(env, &logical_types);
        let field_names = read_string_array(env, &field_names);
        let key_fields = read_int_array(env, &key_fields)
            .into_iter()
            .map(|index| index as usize)
            .collect::<Vec<_>>();
        let value_fields = read_int_array(env, &value_fields)
            .into_iter()
            .map(|index| index as usize)
            .collect::<Vec<_>>();
        let key_logical_types = project_strings(&logical_types, &key_fields)?;
        let key_field_names = project_strings(&field_names, &key_fields)?;
        let value_logical_types = project_strings(&logical_types, &value_fields)?;
        let value_field_names = project_strings(&field_names, &value_fields)?;
        let records = encode_records(
            &batch,
            &options,
            &key_options,
            &key_fields,
            &value_fields,
            &key_logical_types,
            &key_field_names,
            &value_logical_types,
            &value_field_names,
            upsert != 0,
            None,
            None,
        )?;

        let (key_bytes, key_offsets, key_lengths) = match records.keys.as_ref() {
            Some(keys) => {
                let (offsets, lengths) = keys.metadata(&[])?;
                (&keys.bytes[..], offsets, lengths)
            }
            None => (&[][..], vec![0; records.len()], vec![-1; records.len()]),
        };
        let (value_offsets, value_lengths) = records.values.metadata(&records.tombstones)?;
        let key_bytes = env
            .byte_array_from_slice(key_bytes)
            .map_err(|error| format!("failed to materialize Kafka key slab: {error}"))?;
        let key_offsets = int_array(env, &key_offsets)?;
        let key_lengths = int_array(env, &key_lengths)?;
        let value_bytes = env
            .byte_array_from_slice(&records.values.bytes)
            .map_err(|error| format!("failed to materialize Kafka value slab: {error}"))?;
        let value_offsets = int_array(env, &value_offsets)?;
        let value_lengths = int_array(env, &value_lengths)?;
        let class = env
            .find_class("tech/streamfusion/kafka/NativeKafkaEncodedBatch")
            .map_err(|error| format!("failed to resolve Kafka batch class: {error}"))?;
        let result = env
            .new_object(
                class,
                "([B[I[I[B[I[I)V",
                &[
                    jni::objects::JValue::Object(&key_bytes),
                    jni::objects::JValue::Object(&key_offsets),
                    jni::objects::JValue::Object(&key_lengths),
                    jni::objects::JValue::Object(&value_bytes),
                    jni::objects::JValue::Object(&value_offsets),
                    jni::objects::JValue::Object(&value_lengths),
                ],
            )
            .map_err(|error| format!("failed to construct Kafka encoded batch: {error}"))?;
        Ok(result.into_raw())
    })
}
