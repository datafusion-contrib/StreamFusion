use crate::*;

/// Errors the Java Kafka consumer absorbs while its metadata/network layer reconnects. librdkafka
/// can surface these as queue events even though the same consumer remains usable; permanent data,
/// offset, authentication, and authorization errors deliberately are not on this list.
#[cfg(feature = "kafka")]
fn transient_consumer_error(error: rdkafka::bindings::rd_kafka_resp_err_t) -> bool {
    use rdkafka::bindings::rd_kafka_resp_err_t::*;

    matches!(
        error,
        RD_KAFKA_RESP_ERR__TRANSPORT
            | RD_KAFKA_RESP_ERR__RESOLVE
            | RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN
            | RD_KAFKA_RESP_ERR__TIMED_OUT
            | RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE
            | RD_KAFKA_RESP_ERR__WAIT_COORD
            | RD_KAFKA_RESP_ERR__IN_PROGRESS
            | RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS
            | RD_KAFKA_RESP_ERR__WAIT_CACHE
            | RD_KAFKA_RESP_ERR__INTR
            | RD_KAFKA_RESP_ERR__RETRY
            | RD_KAFKA_RESP_ERR__NODE_UPDATE
            | RD_KAFKA_RESP_ERR__DESTROY_BROKER
    )
}

/// One encoded record per row, all in a single encode buffer: producing and JNI materialization
/// read the per-row slices in place, so no per-record allocation or copy happens on this side
/// (librdkafka copies borrowed payloads into its own queue on produce).
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
            "encode.decimal-as-plain-number" => {
                options.decimal_as_plain_number = value == "true"
            }
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
    use arrow::json::writer::{LineDelimited, WriterBuilder};

    let batch = annotate_flink_types(batch, logical_types, field_names)?;

    let mut builder =
        WriterBuilder::new()
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
        builder = builder
            .with_timestamp_format("%Y-%m-%dT%H:%M:%S%.f".to_string())
            .with_timestamp_tz_format("%Y-%m-%dT%H:%M:%S%.fZ".to_string());
    } else {
        builder = builder
            .with_timestamp_format("%Y-%m-%d %H:%M:%S%.f".to_string())
            .with_timestamp_tz_format("%Y-%m-%d %H:%M:%S%.fZ".to_string());
    }
    let mut bytes = Vec::new();
    {
        let mut writer = builder.build::<_, LineDelimited>(&mut bytes);
        writer
            .write(&batch)
            .map_err(|error| format!("failed to encode Kafka JSON batch: {error}"))?;
        writer
            .finish()
            .map_err(|error| format!("failed to finish Kafka JSON batch: {error}"))?;
    }
    let mut lines = Vec::with_capacity(batch.num_rows());
    let mut start = 0;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            if index > start {
                lines.push(start..index);
            }
            start = index + 1;
        }
    }
    if bytes.len() > start {
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
) -> Result<EncodedLines, String> {
    match format {
        EncodeOptions::Json { envelope, options } => {
            let rows = encode_json_batch(batch, options, logical_types, field_names)?;
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
            // Columns map to proto fields by name; an all-unset row is a zero-length message
            // (Flink's serializer produces the same empty byte[], not a tombstone).
            let (bytes, rows) = options.encoder().encode(batch).into_parts();
            Ok(EncodedLines::new(bytes, rows))
        }
        #[cfg(feature = "raw")]
        EncodeOptions::Raw(options) => {
            let (bytes, rows) = crate::raw_encode::encode_raw_batch(batch, options)?;
            Ok(EncodedLines::new(bytes, rows))
        }
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
    logical_types: &[String],
    field_names: &[String],
    key_fields: &[usize],
    value_fields: &[usize],
    upsert: bool,
) -> Result<EncodedKafkaRecords, String> {
    let key_batch = batch
        .project(key_fields)
        .map_err(|error| format!("failed to project Kafka key fields: {error}"))?;
    let value_batch = batch
        .project(value_fields)
        .map_err(|error| format!("failed to project Kafka value fields: {error}"))?;
    let project_types = |fields: &[usize]| {
        fields
            .iter()
            .map(|index| logical_types[*index].clone())
            .collect::<Vec<_>>()
    };
    let project_names = |fields: &[usize]| {
        fields
            .iter()
            .map(|index| field_names[*index].clone())
            .collect::<Vec<_>>()
    };
    let keys = if key_fields.is_empty() {
        None
    } else {
        // The key format never carries a CDC envelope (CDC formats are not legal key formats)
        // and the key row has no changelog kind of its own.
        Some(encode_value_lines(
            &key_batch,
            None,
            key_options,
            &project_types(key_fields),
            &project_names(key_fields),
        )?)
    };
    let values = encode_value_lines(
        &value_batch,
        row_kind_column(batch),
        options,
        &project_types(value_fields),
        &project_names(value_fields),
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
        let kinds = row_kind_column(batch).ok_or_else(|| {
            "upsert Kafka serialization requires the hidden row-kind column".to_string()
        })?;
        (0..batch.num_rows())
            .map(|index| matches!(kinds.value(index), 1 | 3))
            .collect()
    } else {
        Vec::new()
    };
    Ok(EncodedKafkaRecords {
        keys,
        values,
        tombstones,
    })
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
    if logical_types.is_empty() && field_names.is_empty() {
        return Ok(batch.clone());
    }
    if logical_types.len() != batch.num_columns() || field_names.len() != batch.num_columns() {
        return Err(format!(
            "Kafka JSON encoder received {} logical types and {} names for {} columns",
            logical_types.len(),
            field_names.len(),
            batch.num_columns()
        ));
    }
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (index, field) in batch.schema().fields().iter().enumerate() {
        let column = mark_ltz_leaves(batch.column(index).clone(), &logical_types[index])?;
        fields.push(
            field
                .as_ref()
                .clone()
                .with_name(&field_names[index])
                .with_data_type(column.data_type().clone()),
        );
        columns.push(column);
    }
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns)
        .map_err(|error| format!("failed to annotate Kafka JSON schema: {error}"))
}

/// Re-marks every TIMESTAMP_LTZ leaf with a UTC timezone by walking the column's Flink logical
/// type descriptor in lockstep with the Arrow tree. The Java boundary maps both of Flink's
/// timestamp flavors to timezone-less nanoseconds, but the JSON encoder must render an LTZ instant
/// with Flink's 'Z' designator — at any nesting depth. Only the type tree is rebuilt (buffers are
/// shared), and a column whose descriptor carries no LTZ leaf passes through untouched.
#[cfg(feature = "kafka")]
fn mark_ltz_leaves(array: ArrayRef, descriptor: &str) -> Result<ArrayRef, String> {
    use arrow::array::cast::AsArray;
    use arrow::array::{LargeListArray, ListArray, MapArray, StructArray};
    use arrow::datatypes::TimestampNanosecondType;

    if !descriptor.contains("TIMESTAMP_LTZ") {
        return Ok(array);
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
    match array.data_type() {
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
            if descriptor.starts_with("TIMESTAMP_LTZ") =>
        {
            let marked =
                array.as_primitive::<TimestampNanosecondType>().clone().with_timezone("UTC");
            Ok(Arc::new(marked))
        }
        DataType::Struct(struct_fields) => {
            let children = children(struct_fields.len())?;
            let (fields, columns, nulls) = array.as_struct().clone().into_parts();
            let mut new_fields = Vec::with_capacity(columns.len());
            let mut new_columns = Vec::with_capacity(columns.len());
            for ((field, column), child) in fields.iter().zip(columns).zip(children) {
                let column = mark_ltz_leaves(column, child)?;
                new_fields.push(Arc::new(
                    field.as_ref().clone().with_data_type(column.data_type().clone()),
                ));
                new_columns.push(column);
            }
            Ok(Arc::new(StructArray::new(new_fields.into(), new_columns, nulls)))
        }
        DataType::List(_) => {
            let element = children(1)?[0];
            let (field, offsets, values, nulls) = array.as_list::<i32>().clone().into_parts();
            let values = mark_ltz_leaves(values, element)?;
            let field =
                Arc::new(field.as_ref().clone().with_data_type(values.data_type().clone()));
            Ok(Arc::new(ListArray::new(field, offsets, values, nulls)))
        }
        DataType::LargeList(_) => {
            let element = children(1)?[0];
            let (field, offsets, values, nulls) = array.as_list::<i64>().clone().into_parts();
            let values = mark_ltz_leaves(values, element)?;
            let field =
                Arc::new(field.as_ref().clone().with_data_type(values.data_type().clone()));
            Ok(Arc::new(LargeListArray::new(field, offsets, values, nulls)))
        }
        DataType::Map(_, ordered) => {
            let ordered = *ordered;
            let children = children(2)?;
            let map = array.as_map();
            let offsets = map.offsets().clone();
            let map_nulls = map.nulls().cloned();
            let (fields, columns, nulls) = map.entries().clone().into_parts();
            let mut new_fields = Vec::with_capacity(2);
            let mut new_columns = Vec::with_capacity(2);
            for ((field, column), child) in fields.iter().zip(columns).zip(children) {
                let column = mark_ltz_leaves(column, child)?;
                new_fields.push(Arc::new(
                    field.as_ref().clone().with_data_type(column.data_type().clone()),
                ));
                new_columns.push(column);
            }
            let entries = StructArray::new(new_fields.into(), new_columns, nulls);
            let DataType::Map(entry_field, _) = array.data_type() else {
                unreachable!("matched Map above");
            };
            let entry_field = Arc::new(
                entry_field.as_ref().clone().with_data_type(entries.data_type().clone()),
            );
            Ok(Arc::new(MapArray::new(entry_field, offsets, entries, map_nulls, ordered)))
        }
        _ => Ok(array),
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
            _ => None,
        };
        Ok(encoder.map(|encoder| NullableEncoder::new(encoder, array.nulls().cloned())))
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
/// word-at-a-time scan and bulk-copies; values that do escape go through a loop replicating
/// serde_json's exact table (`\"`, `\\`, named controls, lowercase `\u00XX`), so output bytes
/// are identical either way (pinned by a parity test against the stock arrow-json writer).
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

/// One quoted, serde_json-escaped JSON string value.
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
    chunks.remainder().iter().any(|&byte| byte < 0x20 || byte == b'"' || byte == b'\\')
}

/// serde_json's escape table, applied over unescaped runs (without the surrounding quotes).
#[cfg(feature = "kafka")]
fn encode_escaped_json(bytes: &[u8], output: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
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
    let era = (if shifted >= 0 { shifted } else { shifted - 146_096 }) / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year =
        day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
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
        values.extend((0..=2048_i128).map(|index| {
            (i64::MIN as i128 + (u64::MAX as i128 * index / 2048)) as i64
        }));
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

/// The production native Kafka consumer for one Flink subtask: a single rdkafka `BaseConsumer` that
/// multiplexes all of the subtask's assigned partitions (Flink-parity — one consumer, not one per
/// split). Each `poll` buckets the drained payloads by partition directly into Arrow binary body
/// batches and, when a format decoder is attached, decodes each bucket to its typed batch in the same
/// call while the bytes are cache-hot — the decode dispatches through a C-ABI entry the format DSO
/// handed over (never symbol linkage), so the connector stays format-neutral. Without an attached
/// decoder the bodies cross to the JVM and the split reader decodes them there. Manual
/// `assign()`+seek, never `subscribe()`/rebalance.
#[cfg(feature = "kafka")]
pub(crate) struct KafkaSplitReader {
    consumer: rdkafka::consumer::BaseConsumer,
    /// The consumer's message queue, drained via the callback API (see `poll`).
    consumer_queue: *mut rdkafka::bindings::rd_kafka_queue_t,
    body_schema: SchemaRef,
    /// The attached format decode: the format DSO's version-1 driver vtable and its opaque decoder
    /// handle, obtained through the driver-init handshake (see `format_abi`).
    decode: Option<(DecodeBodyBatch, i64)>,
    /// Next offset to consume per assigned partition — the split's checkpoint position.
    next_offsets: HashMap<(String, i32), i64>,
    /// Concrete bounded stopping offsets. The poll callback drops records at or beyond this boundary.
    stopping_offsets: HashMap<(String, i32), i64>,
    /// Topics whose broker metadata has been primed (see `reassign`).
    warmed_topics: std::collections::HashSet<String>,
    /// Body (or decoded, when a decoder is attached) batches ready for the JVM to drain one split at a
    /// time, in arrival (offset) order so a split's offset never goes backwards when several of its
    /// batches are drained in one cycle. Fields: (topic, partition, next offset, batch).
    pending: std::collections::VecDeque<(String, i32, i64, RecordBatch, i64, i64, i64)>,
}

#[cfg(feature = "kafka")]
impl Drop for KafkaSplitReader {
    fn drop(&mut self) {
        if !self.consumer_queue.is_null() {
            unsafe { rdkafka::bindings::rd_kafka_queue_destroy(self.consumer_queue) };
        }
    }
}

#[cfg(feature = "kafka")]
impl KafkaSplitReader {
    fn open(config: &[(String, String)]) -> Result<KafkaSplitReader, String> {
        use rdkafka::config::ClientConfig;

        let mut client = ClientConfig::new();
        for (key, value) in config {
            client.set(key, value);
        }
        client.set("enable.partition.eof", "true");
        // Surface librdkafka's own message (bad mechanism, missing trust material, unsupported
        // protocol) instead of a panic — misconfigured auth is an expected failure here.
        let consumer: rdkafka::consumer::BaseConsumer = client
            .create()
            .map_err(|e| format!("failed to create kafka consumer: {e}"))?;
        // The consumer's queue, for draining. (assign/seek still go through the BaseConsumer.)
        let consumer_queue = unsafe {
            use rdkafka::consumer::Consumer;
            rdkafka::bindings::rd_kafka_queue_get_consumer(consumer.client().native_ptr())
        };

        Ok(KafkaSplitReader {
            consumer,
            consumer_queue,
            body_schema: Arc::new(Schema::new(vec![Field::new("body", DataType::Binary, true)])),
            decode: None,
            next_offsets: HashMap::default(),
            stopping_offsets: HashMap::default(),
            warmed_topics: std::collections::HashSet::default(),
            pending: std::collections::VecDeque::new(),
        })
    }

    /// Adds splits (idempotent) and re-assigns the whole set: each newly added partition seeks to its
    /// given start offset, each existing one stays at its tracked next offset. assign() with explicit
    /// offsets both assigns and seeks, so no subscribe/rebalance is involved.
    ///
    /// A negative start offset is one of Flink's `KafkaPartitionSplit` markers, which the enumerator
    /// leaves for the reader to resolve: -2 EARLIEST -> beginning, -1 LATEST -> end, -3 COMMITTED ->
    /// the group's stored offset. A concrete (>= 0) offset seeks to exactly there.
    fn assign_splits(
        &mut self,
        topics: &[String],
        partitions: &[i64],
        offsets: &[i64],
        stopping_offsets: &[i64],
    ) {
        assert_eq!(topics.len(), partitions.len());
        assert_eq!(topics.len(), offsets.len());
        assert_eq!(topics.len(), stopping_offsets.len());
        for i in 0..topics.len() {
            let key = (topics[i].clone(), partitions[i] as i32);
            self.next_offsets
                .entry(key.clone())
                .or_insert(offsets[i]);
            if stopping_offsets[i] != i64::MIN {
                self.stopping_offsets.insert(key, stopping_offsets[i]);
            }
        }
        self.reassign();
    }

    /// Removes the given splits (which reached their stopping offset) from the assignment so the
    /// consumer no longer fetches or blocks on them — mirroring the connector's `unassignPartitions`.
    /// Without this a finished partition makes `poll` block for the timeout at the bounded tail.
    fn unassign_splits(&mut self, topics: &[String], partitions: &[i64]) {
        for i in 0..topics.len() {
            self.next_offsets.remove(&(topics[i].clone(), partitions[i] as i32));
            self.stopping_offsets
                .remove(&(topics[i].clone(), partitions[i] as i32));
        }
        self.reassign();
    }

    /// Commits a completed Flink checkpoint for Kafka-side monitoring. Flink state remains the
    /// recovery authority; the synchronous mode lets the Java reader distinguish a real broker ack
    /// from a failed commit without sharing the native handle across threads.
    fn commit_offsets(
        &self,
        topics: &[String],
        partitions: &[i64],
        offsets: &[i64],
    ) -> Result<(), String> {
        use rdkafka::consumer::{CommitMode, Consumer};
        use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

        let mut tpl = TopicPartitionList::with_capacity(topics.len());
        for i in 0..topics.len() {
            tpl.add_partition_offset(
                &topics[i],
                partitions[i] as i32,
                Offset::Offset(offsets[i]),
            )
            .map_err(|error| format!("failed to build Kafka offset commit: {error}"))?;
        }
        self.consumer
            .commit(&tpl, CommitMode::Sync)
            .map_err(|error| format!("failed to commit Kafka offsets: {error}"))
    }

    /// Pauses or resumes only the requested assigned partitions for Flink watermark alignment.
    fn set_paused(
        &self,
        topics: &[String],
        partitions: &[i64],
        paused: bool,
    ) -> Result<(), String> {
        use rdkafka::consumer::Consumer;
        use rdkafka::topic_partition_list::TopicPartitionList;

        let mut tpl = TopicPartitionList::with_capacity(topics.len());
        for i in 0..topics.len() {
            tpl.add_partition(&topics[i], partitions[i] as i32);
        }
        let result = if paused {
            self.consumer.pause(&tpl)
        } else {
            self.consumer.resume(&tpl)
        };
        result.map_err(|error| {
            format!(
                "failed to {} Kafka partitions: {error}",
                if paused { "pause" } else { "resume" }
            )
        })
    }

    /// (Re)assigns the consumer to exactly the currently-tracked partitions, each seeked to its tracked
    /// offset (or start marker). assign() with explicit offsets replaces the whole assignment.
    fn reassign(&mut self) {
        use rdkafka::consumer::Consumer;
        use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

        if self.next_offsets.is_empty() {
            self.consumer.unassign().expect("failed to unassign");
            return;
        }
        // Prime broker metadata for topics this consumer hasn't resolved yet, BEFORE assigning:
        // an assign on a cold connection parks each partition in leader-query until librdkafka's
        // periodic metadata refresh resolves it — measured as ~0.5s of dead time before the first
        // fetch. An explicit blocking metadata fetch resolves leaders now (the same warm-up the
        // Java client gets from its initial metadata round). Failure is ignored: assign still
        // works through the refresh cycle, just slower.
        for topic in
            self.next_offsets.keys().map(|(topic, _)| topic.clone()).collect::<Vec<_>>()
        {
            if self.warmed_topics.insert(topic.clone()) {
                let _ = self
                    .consumer
                    .fetch_metadata(Some(&topic), std::time::Duration::from_secs(10));
            }
        }
        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), &offset) in &self.next_offsets {
            let position = match offset {
                -2 => Offset::Beginning,
                -1 => Offset::End,
                -3 => Offset::Stored,
                concrete if concrete >= 0 => Offset::Offset(concrete),
                _ => Offset::Beginning,
            };
            tpl.add_partition_offset(topic, *partition, position)
                .expect("failed to add partition offset");
        }
        self.consumer.assign(&tpl).expect("failed to assign partitions");
    }

    /// Polls up to `max_records` messages, buckets them by partition, and decodes one typed Arrow batch
    /// per partition into `pending`, advancing each split's next offset. Returns the number of
    /// per-partition batches now pending (0 on a poll timeout).
    fn poll(&mut self, max_records: usize, timeout: std::time::Duration) -> Result<usize, String> {
        use arrow::array::BinaryBuilder;
        use rdkafka::bindings as rdsys;
        use rdkafka::consumer::Consumer;

        // Fetcher thread: drain the consumer queue with the CALLBACK API — one queue-mutex acquisition
        // moves the whole queued backlog local (rd_kafka_consume_batch_queue re-locks per message,
        // contending with the broker thread's enqueue), each payload is copied into a per-partition
        // binary builder from the callback, and librdkafka frees each op after its callback returns.
        // `max_records` is enforced with rd_kafka_yield (a thread-local stop flag): the dispatch loop
        // stops and prepends the untaken remainder back onto the queue head.
        struct PollContext {
            rk: *mut rdsys::rd_kafka_t,
            max_records: usize,
            seen: usize,
            buffered: usize,
            /// Per-partition buckets: a subtask holds a handful of partitions and a fetch response
            /// delivers a partition's records contiguously, so a last-bucket cache + linear scan
            /// beats a per-message hash lookup.
            buckets: Vec<(
                *mut rdsys::rd_kafka_topic_t,
                i32,
                String,
                BinaryBuilder,
                i64,
                Option<i64>,
                i64,
            )>,
            last_bucket: usize,
            stopping_offsets: *const HashMap<(String, i32), i64>,
            partition_eofs: Vec<(String, i32, i64)>,
            error: Option<String>,
        }
        unsafe extern "C" fn bucket_message(
            message: *mut rdsys::rd_kafka_message_t,
            opaque: *mut std::os::raw::c_void,
        ) {
            let context = &mut *(opaque as *mut PollContext);
            context.seen += 1;
            let message = &*message;
            if message.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                let index = if context
                    .buckets
                    .get(context.last_bucket)
                    .is_some_and(|bucket| bucket.0 == message.rkt && bucket.1 == message.partition)
                {
                    context.last_bucket
                } else if let Some(found) =
                    context
                        .buckets
                        .iter()
                        .position(|bucket| bucket.0 == message.rkt && bucket.1 == message.partition)
                {
                    found
                } else {
                    // Topic resolved once per partition (not per message); pre-size so the binary
                    // buffers don't reallocate as the batch fills.
                    let topic =
                        std::ffi::CStr::from_ptr(rdsys::rd_kafka_topic_name(message.rkt))
                            .to_string_lossy()
                            .into_owned();
                    // Pre-size for the poll cap (bounded — the cap can be huge when a caller wants
                    // an unchunked drain; the builder grows amortized past this).
                    let presize = context.max_records.min(65536);
                    let stop = (*context.stopping_offsets)
                        .get(&(topic.clone(), message.partition))
                        .copied();
                    context.buckets.push((
                        message.rkt,
                        message.partition,
                        topic,
                        BinaryBuilder::with_capacity(presize, presize * 64),
                        0,
                        stop,
                        0,
                    ));
                    context.buckets.len() - 1
                };
                context.last_bucket = index;
                let bucket = &mut context.buckets[index];
                if bucket.5.is_none_or(|stop| message.offset < stop) {
                    if message.payload.is_null() {
                        bucket.3.append_null();
                    } else {
                        let payload =
                            std::slice::from_raw_parts(message.payload as *const u8, message.len);
                        bucket.3.append_value(payload);
                        bucket.6 += message.len as i64;
                    }
                    bucket.4 = message.offset + 1;
                    context.buffered += 1;
                }
            } else if message.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__PARTITION_EOF {
                let topic = std::ffi::CStr::from_ptr(rdsys::rd_kafka_topic_name(message.rkt))
                    .to_string_lossy()
                    .into_owned();
                context
                    .partition_eofs
                    .push((topic, message.partition, message.offset));
            } else if !transient_consumer_error(message.err) && context.error.is_none() {
                let name = std::ffi::CStr::from_ptr(rdsys::rd_kafka_err2name(message.err))
                    .to_string_lossy();
                let description = std::ffi::CStr::from_ptr(rdsys::rd_kafka_err2str(message.err))
                    .to_string_lossy();
                context.error = Some(format!("Kafka consumer error {name}: {description}"));
            }
            if context.seen >= context.max_records {
                rdsys::rd_kafka_yield(context.rk);
            }
        }
        let mut context = PollContext {
            rk: self.consumer.client().native_ptr(),
            max_records,
            seen: 0,
            buffered: 0,
            buckets: Vec::new(),
            last_bucket: 0,
            stopping_offsets: &self.stopping_offsets,
            partition_eofs: Vec::new(),
            error: None,
        };
        unsafe {
            rdsys::rd_kafka_consume_callback_queue(
                self.consumer_queue,
                timeout.as_millis() as std::os::raw::c_int,
                Some(bucket_message),
                &mut context as *mut PollContext as *mut std::os::raw::c_void,
            )
        };
        if let Some(error) = context.error {
            return Err(error);
        }
        // One batch per partition, straight into `pending` (the JVM drains all of them right after this
        // returns, so nothing is ever left behind on a bounded finish). With a decoder attached, each
        // body batch is decoded to its typed batch here, while its bytes are still cache-hot from the
        // callback's copies — deferring the decode to a later pass re-streamed the payload bytes cold
        // and, with the JVM round trip, measured at roughly half the fused throughput.
        self.pending.clear();
        let mut positions = self
            .consumer
            .position()
            .map_err(|error| format!("failed to retrieve Kafka consumer positions: {error}"))?
            .elements()
            .into_iter()
            .filter_map(|position| match position.offset() {
                rdkafka::Offset::Offset(offset) => Some((
                    (position.topic().to_owned(), position.partition()),
                    offset,
                )),
                _ => None,
            })
            .collect::<HashMap<_, _>>();
        for (topic, partition, offset) in context.partition_eofs {
            positions.insert((topic, partition), offset);
        }
        let mut reported = HashSet::default();
        for (_rkt, partition, topic, mut builder, payload_next_offset, stop, bytes) in context.buckets {
            let key = (topic.clone(), partition);
            let position = positions.get(&key).copied().unwrap_or(payload_next_offset);
            let next_offset = stop.map_or(position, |stop| position.min(stop));
            let body = RecordBatch::try_new(self.body_schema.clone(), vec![Arc::new(builder.finish())])
                .expect("failed to build kafka body batch");
            let records = body.num_rows() as i64;
            let batch = match (self.decode, body.num_rows()) {
                (_, 0) | (None, _) => body,
                (Some((entry, decoder)), _) => Self::decode_bucket(entry, decoder, body),
            };
            self.next_offsets.insert((topic.clone(), partition), next_offset);
            let high_watermark = self.cached_high_watermark(&topic, partition);
            self.pending.push_back((
                topic,
                partition,
                next_offset,
                batch,
                bytes,
                records,
                high_watermark,
            ));
            reported.insert(key);
        }
        // Empty partitions, null-only tails, and read_committed control records can advance Kafka's
        // position without producing a payload bucket. Emit an empty body batch so the JVM advances the
        // split state and can report a bounded split finished.
        for (key, position) in positions {
            if reported.contains(&key) {
                continue;
            }
            let previous = self.next_offsets.get(&key).copied();
            let next_offset = self
                .stopping_offsets
                .get(&key)
                .map_or(position, |stop| position.min(*stop));
            let reached_stop = self
                .stopping_offsets
                .get(&key)
                .is_some_and(|stop| position >= *stop);
            if previous == Some(next_offset) && !reached_stop {
                continue;
            }
            self.next_offsets.insert(key.clone(), next_offset);
            let body = RecordBatch::new_empty(self.body_schema.clone());
            let high_watermark = self.cached_high_watermark(&key.0, key.1);
            self.pending.push_back((
                key.0,
                key.1,
                next_offset,
                body,
                0,
                0,
                high_watermark,
            ));
        }
        Ok(self.pending.len())
    }

    /// Returns librdkafka's locally cached high watermark without a broker round trip.
    fn cached_high_watermark(&self, topic: &str, partition: i32) -> i64 {
        use rdkafka::consumer::Consumer;

        let Ok(topic) = std::ffi::CString::new(topic) else {
            return -1;
        };
        let mut low = -1;
        let mut high = -1;
        let result = unsafe {
            rdkafka::bindings::rd_kafka_get_watermark_offsets(
                self.consumer.client().native_ptr(),
                topic.as_ptr(),
                partition,
                &mut low,
                &mut high,
            )
        };
        if result == rdkafka::bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            high
        } else {
            -1
        }
    }

    /// Runs the attached format's C-ABI decode on one body batch. In and out cross as Arrow C Data on
    /// this stack frame; ownership follows each structure's release callback into its producing DSO.
    fn decode_bucket(entry: DecodeBodyBatch, decoder: i64, body: RecordBatch) -> RecordBatch {
        use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
        let mut in_array = FFI_ArrowArray::empty();
        let mut in_schema = FFI_ArrowSchema::empty();
        export_record_batch(
            body,
            &mut in_array as *mut FFI_ArrowArray as jlong,
            &mut in_schema as *mut FFI_ArrowSchema as jlong,
        );
        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();
        let rc = entry(
            decoder,
            &mut in_array as *mut FFI_ArrowArray as i64,
            &mut in_schema as *mut FFI_ArrowSchema as i64,
            &mut out_array as *mut FFI_ArrowArray as i64,
            &mut out_schema as *mut FFI_ArrowSchema as i64,
        );
        assert_eq!(rc, 0, "attached format decode failed (rc {rc})");
        import_record_batch(
            &mut out_array as *mut FFI_ArrowArray as jlong,
            &mut out_schema as *mut FFI_ArrowSchema as jlong,
        )
    }
}

#[cfg(all(test, feature = "kafka"))]
mod kafka_error_tests {
    use super::{
        encode_java_big_decimal, encode_json_batch, transient_consumer_error, JsonEncodeOptions,
    };
    use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use rdkafka::bindings::rd_kafka_resp_err_t::*;
    use std::sync::Arc;

    #[test]
    fn retries_transport_but_surfaces_semantic_and_security_failures() {
        assert!(transient_consumer_error(RD_KAFKA_RESP_ERR__TRANSPORT));
        assert!(transient_consumer_error(RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN));
        assert!(!transient_consumer_error(RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE));
        assert!(!transient_consumer_error(RD_KAFKA_RESP_ERR__AUTO_OFFSET_RESET));
        assert!(!transient_consumer_error(RD_KAFKA_RESP_ERR__AUTHENTICATION));
        assert!(!transient_consumer_error(RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED));
        assert!(!transient_consumer_error(RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC));
    }

    /// Pins the security surface of the vendored librdkafka build: TLS and SCRAM must be present
    /// (the `ssl-vendored` feature), GSSAPI deliberately absent (no cyrus-sasl — the planner
    /// declines Kerberos to Flink). If a feature change ever drops OpenSSL, these fail loudly
    /// instead of secured production jobs dying at task start.
    #[test]
    fn vendored_build_speaks_tls_and_scram_but_not_gssapi() {
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::BaseConsumer;

        let mut scram = ClientConfig::new();
        scram
            .set("security.protocol", "SASL_SSL")
            .set("sasl.mechanisms", "SCRAM-SHA-256")
            .set("sasl.username", "user")
            .set("sasl.password", "pass");
        // Config validation and provider selection happen at create(); connection does not.
        scram.create::<BaseConsumer>().expect(
            "SASL_SSL + SCRAM consumer must build: the DSO lost its OpenSSL (ssl-vendored)",
        );

        let mut gssapi = ClientConfig::new();
        gssapi
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanisms", "GSSAPI");
        let err = match gssapi.create::<BaseConsumer>() {
            Ok(_) => panic!("GSSAPI must stay out of the portable build (no cyrus-sasl)"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("No provider for SASL mechanism GSSAPI"),
            "unexpected GSSAPI refusal: {err}"
        );
    }

    #[test]
    fn encodes_a_whole_arrow_batch_as_individual_json_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("one"), None])),
                Arc::new(BooleanArray::from(vec![true, false])),
            ],
        )
        .unwrap();

        let explicit =
            encode_json_batch(&batch, &JsonEncodeOptions::default(), &[], &[])
                .unwrap();
        assert_eq!(explicit.line(0), br#"{"id":1,"name":"one","active":true}"#.as_slice());
        assert_eq!(explicit.line(1), br#"{"id":2,"name":null,"active":false}"#.as_slice());
        let omitted =
            encode_json_batch(
                &batch,
                &JsonEncodeOptions { ignore_null_fields: true, ..JsonEncodeOptions::default() },
                &[],
                &[],
            )
                .unwrap();
        assert_eq!(omitted.line(1), br#"{"id":2,"active":false}"#.as_slice());
    }

    /// Pins both Jackson decimal spellings against Java-derived expectations:
    /// `stripTrailingZeros().toString()` (default, scientific when the stripped scale goes
    /// negative or the adjusted exponent drops below -6) and `toPlainString()` at the column
    /// scale (`WRITE_BIGDECIMAL_AS_PLAIN`).
    #[test]
    fn decimal_spellings_match_java_big_decimal() {
        let cases: &[(i128, i8, &str, &str)] = &[
            (10000, 2, "1E+2", "100.00"),           // 100.00
            (100, 2, "1", "1.00"),                  // 1.00
            (0, 2, "0", "0.00"),                    // 0.00
            (123450, 3, "123.45", "123.450"),       // 123.450
            (-1, 2, "-0.01", "-0.01"),              // -0.01
            (-10000, 2, "-1E+2", "-100.00"),        // -100.00
            (10, 9, "1E-8", "0.000000010"),         // 0.000000010
            (1, 6, "0.000001", "0.000001"),         // adjusted exponent exactly -6 stays plain
            (12345, 0, "12345", "12345"),
            (1234500, 0, "1.2345E+6", "1234500"),   // strips into a negative scale
            (
                i128::MIN + 1,
                38,
                "-1.70141183460469231731687303715884105727",
                "-1.70141183460469231731687303715884105727",
            ),
            (1, 9, "1E-9", "0.000000001"), // adjusted exponent below -6 goes scientific
        ];
        for (unscaled, scale, stripped, plain) in cases {
            let mut output = Vec::new();
            encode_java_big_decimal(*unscaled, *scale, false, &mut output);
            assert_eq!(std::str::from_utf8(&output).unwrap(), *stripped, "default {unscaled}/{scale}");
            output.clear();
            encode_java_big_decimal(*unscaled, *scale, true, &mut output);
            assert_eq!(std::str::from_utf8(&output).unwrap(), *plain, "plain {unscaled}/{scale}");
        }
    }

    /// The bulk-scan string encoder must match arrow-json's stock (serde_json) escaping byte for
    /// byte, across every escape class and both scan paths (word chunks and the tail remainder).
    #[test]
    fn string_escaping_matches_stock_arrow_json() {
        let mut values: Vec<String> = vec![
            String::new(),
            "plain ascii value".into(),
            "long clean run long clean run long clean run long clean run".into(),
            "ünïcodé — 統一碼 🚀 verbatim".into(),
            r#"say "hello""#.into(),
            r"back\slash".into(),
            "tab\there newline\nthere\r\x0c\x08".into(),
            "escape at end\"".into(),
            "\"escape at start".into(),
            "1234567\"9 escape in second word 0123456\u{1f}".into(),
            "seven b\u{7f}".into(),
        ];
        for control in 0u8..0x20 {
            values.push(format!("ctl {}", control as char));
        }
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let strings: Vec<Option<&str>> = values.iter().map(|value| Some(value.as_str())).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(strings)) as ArrayRef],
        )
        .unwrap();

        let mut stock = Vec::new();
        {
            let mut writer = arrow::json::writer::WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, arrow::json::writer::LineDelimited>(&mut stock);
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let stock_lines: Vec<&[u8]> =
            stock.split(|byte| *byte == b'\n').filter(|line| !line.is_empty()).collect();

        let ours =
            encode_json_batch(&batch, &JsonEncodeOptions::default(), &[], &[])
                .unwrap();
        assert_eq!(ours.len(), stock_lines.len());
        for index in 0..ours.len() {
            assert_eq!(
                ours.line(index),
                stock_lines[index],
                "row {index}: {:?}",
                values[index]
            );
        }
    }
}

#[cfg(feature = "kafka")]
fn kafka_jni<T, F>(env: &mut JNIEnv, default: T, f: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> Result<T, String>,
{
    connector_jni(env, default, "native Kafka reader panic", f)
}

/// Whether this extension library carries the native Kafka source.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_isLoaded<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jni::sys::jboolean {
    crate::bridge::jni_guard(env, move |_env| {
        cfg!(feature = "kafka") as jni::sys::jboolean
    })
}

/// Opens a native Kafka split reader for one subtask and returns an opaque handle, released with
/// `closeKafkaConsumer`. `configKeys`/`configValues` are the translated librdkafka config (applied
/// verbatim). It produces raw Kafka value bodies as Arrow binary batches; the following format extension
/// owns decoding. Splits are added later via `assignKafkaSplits`.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_openKafkaConsumer<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
) -> jlong {
    kafka_jni(&mut env, 0, |env| {
        let keys = read_string_array(env, &config_keys);
        let values = read_string_array(env, &config_values);
        let config: Vec<(String, String)> = keys.into_iter().zip(values).collect();
        let reader = KafkaSplitReader::open(&config)?;
        Ok(into_handle(reader))
    })
}

/// Attaches a format library's decode to this consumer through the driver-init handshake:
/// `initAddress` is the format's exported `streamfusion_format_driver_init`, called with the ABI
/// version this connector speaks; the format fills the vtable or refuses. Returns whether the attach
/// happened — a refusal (a format artifact from another release) leaves the caller on the
/// JVM-mediated decode. Subsequent polls of an attached consumer emit typed batches instead of
/// binary bodies. The decoder's lifecycle stays with its Java owner, which must outlive this
/// consumer.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_attachKafkaDecoder<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    init_address: jlong,
    decoder_handle: jlong,
) -> jboolean {
    crate::bridge::jni_guard(env, move |_env| {
        let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
        let init: FormatDriverInit = unsafe { std::mem::transmute(init_address as usize) };
        let mut driver = FormatDriver { decode_body_batch: unsupported_decode };
        if init(FORMAT_DRIVER_VERSION_1, &mut driver) != 0 {
            return 0;
        }
        reader.decode = Some((driver.decode_body_batch, decoder_handle));
        1
    })
}

/// Placeholder the driver struct is initialized with before the handshake fills it; never invoked
/// (a failed init leaves the consumer unattached).
#[cfg(feature = "kafka")]
extern "C" fn unsupported_decode(_: i64, _: i64, _: i64, _: i64, _: i64) -> i32 {
    1
}

/// Adds splits to the reader and re-assigns: `topics`/`partitions`/`startOffsets` are index-aligned;
/// new partitions seek to their start offset, existing ones keep their tracked position. Concrete
/// `stoppingOffsets` are enforced inside the poll callback; `i64::MIN` means unbounded.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_assignKafkaSplits<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topics: JObjectArray<'local>,
    partitions: JLongArray<'local>,
    start_offsets: JLongArray<'local>,
    stopping_offsets: JLongArray<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
        let topics = read_string_array(&mut env, &topics);
        let partitions = read_longs(&env, &partitions);
        let offsets = read_longs(&env, &start_offsets);
        let stopping_offsets = read_longs(&env, &stopping_offsets);
        reader.assign_splits(&topics, &partitions, &offsets, &stopping_offsets);
    })
}

/// Removes finished splits (reached their bounded stopping offset) from the assignment so the consumer
/// stops fetching/blocking on them. Index-aligned `topics`/`partitions`.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_unassignKafkaSplits<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topics: JObjectArray<'local>,
    partitions: JLongArray<'local>,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
        let topics = read_string_array(&mut env, &topics);
        let partitions = read_longs(&env, &partitions);
        reader.unassign_splits(&topics, &partitions);
    })
}

/// Commits checkpoint positions from a split-fetcher task, serializing the operation with native
/// poll/assign/close access to this handle.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_commitKafkaOffsets<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topics: JObjectArray<'local>,
    partitions: JLongArray<'local>,
    offsets: JLongArray<'local>,
) {
    kafka_jni(&mut env, (), |env| {
        let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
        let topics = read_string_array(env, &topics);
        let partitions = read_longs(env, &partitions);
        let offsets = read_longs(env, &offsets);
        if topics.len() != partitions.len() || topics.len() != offsets.len() {
            return Err("Kafka offset commit arrays have different lengths".to_string());
        }
        reader.commit_offsets(&topics, &partitions, &offsets)
    });
}

/// Applies Flink's split-level pause/resume request on the fetcher-owned native consumer.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_setKafkaSplitsPaused<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topics: JObjectArray<'local>,
    partitions: JLongArray<'local>,
    paused: jboolean,
) {
    kafka_jni(&mut env, (), |env| {
        let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
        let topics = read_string_array(env, &topics);
        let partitions = read_longs(env, &partitions);
        if topics.len() != partitions.len() {
            return Err("Kafka pause arrays have different lengths".to_string());
        }
        reader.set_paused(&topics, &partitions, paused != 0)
    });
}

/// Polls one cycle, producing one Arrow binary-body batch per partition that had messages. Returns the number of
/// per-partition batches now pending; the JVM drains each with `drainKafkaSplit`.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_pollKafkaBatch<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_records: jint,
    timeout_ms: jlong,
) -> jint {
    kafka_jni(&mut env, 0, |_env| {
        let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
        Ok(reader.poll(
            max_records as usize,
            std::time::Duration::from_millis(timeout_ms as u64),
        )? as jint)
    })
}

/// Interrupts an in-flight queue poll. `rd_kafka_queue_yield` is thread-safe and intentionally
/// touches only librdkafka's queue object, so Flink's task thread need not borrow the fetcher-owned
/// Rust reader while requesting cancellation.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_wakeKafkaConsumer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let reader = handle as *const KafkaSplitReader;
        let queue = unsafe { (*reader).consumer_queue };
        unsafe { rdkafka::bindings::rd_kafka_queue_yield(queue) };
    })
}

/// Imports a whole Arrow batch once and materializes the final `byte[]` values consumed by
/// KafkaProducer. The JNI boundary is batch-grained even though Kafka's Java API requires one heap
/// array per record.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_encodeKafkaBatch<'local>(
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
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_encodeFormatSupported<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    format: jint,
) -> jboolean {
    crate::bridge::jni_guard(env, move |_env| u8::from(encode_format_compiled(format)))
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
        FORMAT_AVRO | FORMAT_AVRO_CONFLUENT => true,
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

/// Serializes projected key/value rows together so an upsert batch crosses JNI once. Null values
/// are Kafka tombstones for DELETE and UPDATE_BEFORE, matching Flink's upsert-kafka schema.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_encodeKafkaRecords<'local>(
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
        let records = encode_records(
            &batch,
            &options,
            &key_options,
            &logical_types,
            &field_names,
            &key_fields,
            &value_fields,
            upsert != 0,
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

/// Drains one pending per-partition body batch, writes `[partition, nextOffset]` into `splitMeta`, and
/// the topic into `outTopic[0]`, so the JVM can form the split id and advance its checkpoint offset.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_drainKafkaSplit<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    split_meta: JLongArray<'local>,
    out_topic: JObjectArray<'local>,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jint {
    crate::bridge::jni_guard(env, move |env| {
        let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
        let (topic, partition, next_offset, batch, bytes, records, high_watermark) =
            reader.pending.pop_front().expect("drainKafkaSplit called with no pending batch");
        let rows = batch.num_rows() as jint;
        let metadata = [partition as i64, next_offset, bytes, records, high_watermark];
        let metadata_len = env
            .get_array_length(&split_meta)
            .expect("failed to read split meta length") as usize;
        env.set_long_array_region(&split_meta, 0, &metadata[..metadata_len.min(metadata.len())])
            .expect("failed to write split meta");
        let topic_jstr = env.new_string(&topic).expect("failed to make topic string");
        env.set_object_array_element(&out_topic, 0, &topic_jstr)
            .expect("failed to write topic");
        export_record_batch(batch, out_array_address, out_schema_address);
        rows
    })
}

/// Releases a native Kafka split reader, dropping the rdkafka consumer (which closes its connections).
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_closeKafkaConsumer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<KafkaSplitReader>(handle));
        }
    })
}

/// Benchmark-only: drive the **production** split reader (poll + inline decode) over a
/// whole topic and count the decoded rows **entirely in Rust** — the decoded Arrow batches are consumed
/// in Rust and never exported to the JVM, exactly as they would feed a downstream native operator in a
/// fused pipeline. This is the honest "fastest way to get Arrow batches in Rust" measurement: it
/// excludes the per-batch JVM export that the FLIP-27 DataStream wrapper forces. Returns the row count.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_benchmarkNativeConsume<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    topic: JString<'local>,
    format: jint,
    schema_array_address: jlong,
    schema_address: jlong,
    avro_schema: JString<'local>,
    schema_id: jint,
    max_messages: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let keys = read_string_array(&mut env, &config_keys);
        let values = read_string_array(&mut env, &config_values);
        let config: Vec<(String, String)> = keys.into_iter().zip(values).collect();
        let topic: String = env.get_string(&topic).expect("failed to read topic").into();
        let _ = (format, schema_array_address, schema_address, avro_schema, schema_id);
        let mut reader =
            KafkaSplitReader::open(&config).expect("failed to create kafka consumer");
        reader.assign_splits(&[topic], &[0], &[-2], &[i64::MIN]); // partition 0, earliest

        let timeout = std::time::Duration::from_millis(250);
        let mut rows: i64 = 0;
        let mut idle = 0;
        // The topic holds exactly `max_messages`; loop until we've decoded them all. A generous idle guard
        // (≈10s of empty polls) only trips if the broker truly stops delivering, avoiding a hang.
        // Poll cap from SF env via JVM? Keep it simple: an experiment knob compiled in — the production
        // reader is driven with the same generous cap the SQL source uses.
        let poll_cap: usize = std::env::var("SF_KAFKA_POLL_CAP")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(65536);
        while rows < max_messages && idle < 40 {
            let count = reader.poll(poll_cap, timeout).expect("failed to poll Kafka");
            if count == 0 {
                idle += 1;
                continue;
            }
            idle = 0;
            for (_topic, _partition, _next_offset, batch, _bytes, _records, _high) in
                reader.pending.drain(..)
            {
                rows += batch.num_rows() as i64; // consumed in Rust; no JVM export
            }
        }
        rows
    })
}

/// Benchmark-only: measure librdkafka's raw delivery rate — batch-consume the whole topic and count
/// messages with NO decode and no decode thread, isolating the consumer from everything downstream.
/// Compared against the Java client's raw poll to answer "is librdkafka delivery actually slower here".
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_benchmarkConsumeOnly<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    topic: JString<'local>,
    max_messages: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        use rdkafka::bindings as rdsys;
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{BaseConsumer, Consumer};
        use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

        let keys = read_string_array(&mut env, &config_keys);
        let values = read_string_array(&mut env, &config_values);
        let topic: String = env.get_string(&topic).expect("failed to read topic").into();
        let mut client = ClientConfig::new();
        for (key, value) in keys.iter().zip(&values) {
            client.set(key, value);
        }
        let consumer: BaseConsumer = client.create().expect("failed to create kafka consumer");
        // Assign every partition at the beginning — librdkafka fetches them all (one FetchRequest per
        // broker) and merges them onto the single consumer queue this loop drains.
        let metadata = consumer
            .fetch_metadata(Some(&topic), std::time::Duration::from_secs(10))
            .expect("fetch metadata");
        let partitions = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .expect("topic in metadata")
            .partitions();
        let mut tpl = TopicPartitionList::new();
        for partition in partitions {
            tpl.add_partition_offset(&topic, partition.id(), Offset::Beginning).expect("add partition");
        }
        consumer.assign(&tpl).expect("assign");
        let queue = unsafe { rdsys::rd_kafka_queue_get_consumer(consumer.client().native_ptr()) };

        // Drain with the callback API instead of `rd_kafka_consume_batch_queue`: the batch call locks
        // and unlocks the queue mutex PER MESSAGE (contending with the broker thread's enqueue), while
        // the callback path bulk-moves the whole queued backlog under ONE lock and dispatches lock-free
        // (librdkafka destroys each op after the callback returns).
        struct CountCtx {
            count: i64,
        }
        unsafe extern "C" fn count_message(
            message: *mut rdsys::rd_kafka_message_t,
            opaque: *mut std::os::raw::c_void,
        ) {
            let context = &mut *(opaque as *mut CountCtx);
            let message = &*message;
            if message.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
                && !message.payload.is_null()
            {
                context.count += 1; // no decode — raw delivery only
            }
        }
        let mut context = CountCtx { count: 0 };
        let mut idle = 0;
        while context.count < max_messages && idle < 40 {
            let served = unsafe {
                rdsys::rd_kafka_consume_callback_queue(
                    queue,
                    250,
                    Some(count_message),
                    &mut context as *mut CountCtx as *mut std::os::raw::c_void,
                )
            };
            if served <= 0 {
                idle += 1;
            } else {
                idle = 0;
            }
        }
        unsafe { rdsys::rd_kafka_queue_destroy(queue) };
        context.count
    })
}

/// Benchmark-only: a hand-rolled raw-consume loop with none of the split-reader machinery (no
/// per-partition bucketing, no offset tracking, no pending queue). Kept for comparisons with the Java
/// client; format decode is now deliberately owned by a separate format DSO.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_benchmarkNativeConsumeSerial<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    topic: JString<'local>,
    format: jint,
    schema_array_address: jlong,
    schema_address: jlong,
    avro_schema: JString<'local>,
    schema_id: jint,
    max_messages: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        use rdkafka::bindings as rdsys;
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{BaseConsumer, Consumer};
        use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

        let keys = read_string_array(&mut env, &config_keys);
        let values = read_string_array(&mut env, &config_values);
        let topic: String = env.get_string(&topic).expect("failed to read topic").into();
        let _ = (format, schema_array_address, schema_address, avro_schema, schema_id);

        let mut client = ClientConfig::new();
        for (key, value) in keys.iter().zip(&values) {
            client.set(key, value);
        }
        let consumer: BaseConsumer = client.create().expect("failed to create kafka consumer");
        let metadata = consumer
            .fetch_metadata(Some(&topic), std::time::Duration::from_secs(10))
            .expect("fetch metadata");
        let mut tpl = TopicPartitionList::new();
        for partition in metadata.topics().iter().find(|t| t.name() == topic).expect("topic").partitions() {
            tpl.add_partition_offset(&topic, partition.id(), Offset::Beginning).expect("add partition");
        }
        consumer.assign(&tpl).expect("assign");
        let queue = unsafe { rdsys::rd_kafka_queue_get_consumer(consumer.client().native_ptr()) };

        // Callback drain (one queue lock per poll, not per message — see benchmarkConsumeOnly). No payload
        // copy occurs here: this benchmark measures the connector DSO's raw delivery floor.
        struct SerialCtx {
            appended: i64,
        }
        unsafe extern "C" fn append_payload(
            message: *mut rdsys::rd_kafka_message_t,
            opaque: *mut std::os::raw::c_void,
        ) {
            let context = &mut *(opaque as *mut SerialCtx);
            let message = &*message;
            if message.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
                && !message.payload.is_null()
            {
                context.appended += 1;
            }
        }
        let mut rows: i64 = 0;
        let mut idle = 0;
        while rows < max_messages && idle < 40 {
            let mut context = SerialCtx { appended: 0 };
            let served = unsafe {
                rdsys::rd_kafka_consume_callback_queue(
                    queue,
                    250,
                    Some(append_payload),
                    &mut context as *mut SerialCtx as *mut std::os::raw::c_void,
                )
            };
            if served <= 0 || context.appended == 0 {
                idle += if served <= 0 { 1 } else { 0 };
                continue;
            }
            idle = 0;
            rows += context.appended;
        }
        unsafe { rdsys::rd_kafka_queue_destroy(queue) };
        rows
    })
}

/// Benchmark-only: consume an entire topic with a native (rdkafka) consumer and decode it to typed
/// Arrow, all in Rust — message payloads go straight from librdkafka into an Arrow binary builder (one
/// copy, no JVM heap byte[] and no per-record JNI crossing), then through the same `JsonDecoder` the
/// shallow path uses. Returns the decoded row count; the JVM times this single call to compare native
/// consume+decode against the shallow path. This is the fast path's measurement, not the production
/// FLIP-27 source (remaining source tails: https://github.com/datafusion-contrib/StreamFusion/issues/16).
#[cfg(feature = "kafka-bench")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_benchmarkKafkaConsume<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    brokers: JString<'local>,
    topic: JString<'local>,
    schema_array_address: jlong,
    schema_address: jlong,
    max_messages: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |env| {
        use arrow::array::BinaryBuilder;
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{BaseConsumer, Consumer};
        use rdkafka::message::Message;

        let brokers: String = env.get_string(&brokers).expect("failed to read brokers").into();
        let topic: String = env.get_string(&topic).expect("failed to read topic").into();
        let decoder = JsonDecoder::new(
            import_record_batch(schema_array_address, schema_address).schema(),
            crate::json::JsonEnv::default(),
        );

        // A fresh group reading from the beginning each run; offsets are not committed (the consumer is
        // throwaway). This mirrors the manual, non-committing consumption the production source would do.
        // Unique group per call so each timed run re-reads the whole topic from the beginning (a fixed
        // group would leave the warm-up run's position at the end and the timed run would read nothing).
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let group = format!("streamfusion-bench-{}-{}", std::process::id(), nonce);
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", &group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("failed to create kafka consumer");
        consumer.subscribe(&[&topic]).expect("failed to subscribe");

        let body_field = Field::new("body", DataType::Binary, true);
        let body_schema = Arc::new(Schema::new(vec![body_field]));
        let mut builder = BinaryBuilder::new();
        let mut buffered = 0usize;
        let mut seen: i64 = 0;
        let mut rows: i64 = 0;
        let mut decode = |builder: &mut BinaryBuilder| -> i64 {
            let batch = RecordBatch::try_new(body_schema.clone(), vec![Arc::new(builder.finish())])
                .expect("failed to build kafka body batch");
            decoder.decode(&batch).num_rows() as i64
        };

        while seen < max_messages {
            match consumer.poll(std::time::Duration::from_secs(5)) {
                Some(Ok(message)) => {
                    builder.append_value(message.payload().unwrap_or(&[]));
                    buffered += 1;
                    seen += 1;
                    if buffered >= 8192 {
                        rows += decode(&mut builder);
                        buffered = 0;
                    }
                }
                Some(Err(error)) => panic!("kafka consume error: {error}"),
                None => break, // poll timeout: the produced messages are exhausted
            }
        }
        if buffered > 0 {
            rows += decode(&mut builder);
        }
        rows
    })
}

/// The exactly-once producer hand-off (Phase 0A spike): the native side owns only the data plane of
/// a Kafka transaction — init, begin, produce, flush — and surfaces the broker-assigned producer
/// identity. The commit is deliberately absent. A Kafka transaction's identity is just
/// (transactional.id, producer id, epoch), and the coordinator accepts EndTxn from any connection
/// presenting that tuple, so the Flink Java committer finishes (or fences) the transaction after
/// the checkpoint completes. Destroying this producer sends neither commit nor abort — the broker
/// keeps the transaction ONGOING — which is exactly what the hand-off relies on.
///
/// The producer identity is sourced authoritatively from the transaction coordinator by the Java
/// host (describeTransactions) after `init_transactions` returns. librdkafka's statistics callback
/// (`statistics.interval.ms`) supplies an advisory copy used purely as an epoch-bump cross-check —
/// nothing correctness-bearing waits on the timer-driven tick.
#[cfg(feature = "kafka")]
pub(crate) struct KafkaTransactionalProducer {
    producer: rdkafka::producer::ThreadedProducer<TxnProducerContext>,
    shared: Arc<TxnProducerShared>,
    max_block: std::time::Duration,
    max_request_size: usize,
}

#[cfg(feature = "kafka")]
#[derive(Default)]
struct TxnProducerShared {
    /// Latest (producer id, epoch) seen in an eos statistics tick; None until the first tick after
    /// `init_transactions` assigns one.
    identity: Mutex<Option<(i64, i64)>>,
    /// First asynchronous per-record delivery failure; a flushed epoch with any failed delivery
    /// must never become a committable.
    delivery_error: Mutex<Option<String>>,
    records: std::sync::atomic::AtomicU64,
    bytes: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "kafka")]
struct TxnProducerContext {
    shared: Arc<TxnProducerShared>,
}

#[cfg(feature = "kafka")]
impl rdkafka::ClientContext for TxnProducerContext {
    fn stats(&self, statistics: rdkafka::Statistics) {
        if let Some(eos) = statistics.eos {
            if eos.producer_id >= 0 {
                *self.shared.identity.lock().unwrap() =
                    Some((eos.producer_id, eos.producer_epoch));
            }
        }
    }
}

#[cfg(feature = "kafka")]
impl rdkafka::producer::ProducerContext for TxnProducerContext {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &rdkafka::message::DeliveryResult<'_>,
        _: Self::DeliveryOpaque,
    ) {
        if let Err((error, _)) = delivery_result {
            let mut slot = self.shared.delivery_error.lock().unwrap();
            if slot.is_none() {
                *slot = Some(error.to_string());
            }
        }
    }
}

#[cfg(feature = "kafka")]
impl KafkaTransactionalProducer {
    fn open(
        config: &[(String, String)],
        max_block: std::time::Duration,
        max_request_size: usize,
    ) -> Result<KafkaTransactionalProducer, String> {
        use rdkafka::config::ClientConfig;

        let mut client = ClientConfig::new();
        for (key, value) in config {
            client.set(key, value);
        }
        let shared = Arc::new(TxnProducerShared::default());
        let producer = client
            .create_with_context(TxnProducerContext { shared: Arc::clone(&shared) })
            .map_err(|error| format!("failed to create Kafka transactional producer: {error}"))?;
        Ok(KafkaTransactionalProducer {
            producer,
            shared,
            max_block,
            max_request_size,
        })
    }

    /// Runs `init_transactions` and returns the identity if a statistics tick has already carried
    /// it, else `(-1, -1)`. The identity here is advisory: the authoritative source is the
    /// transaction coordinator itself, which the Java host queries (describeTransactions) after
    /// this returns — so nothing blocks on the timer-driven statistics callback.
    fn init_transactions(&self, timeout: std::time::Duration) -> Result<(i64, i64), String> {
        use rdkafka::producer::Producer;

        self.producer
            .init_transactions(timeout)
            .map_err(|error| format!("init_transactions failed: {error}"))?;
        Ok(self.shared.identity.lock().unwrap().unwrap_or((-1, -1)))
    }

    fn begin_transaction(&self) -> Result<(), String> {
        use rdkafka::producer::Producer;

        *self.shared.delivery_error.lock().unwrap() = None;
        self.shared
            .records
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.shared
            .bytes
            .store(0, std::sync::atomic::Ordering::Relaxed);
        self.producer
            .begin_transaction()
            .map_err(|error| format!("begin_transaction failed: {error}"))
    }

    fn produce(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<(), String> {
        use rdkafka::error::KafkaError;
        use rdkafka::producer::BaseRecord;
        use rdkafka::types::RDKafkaErrorCode;

        let record_bytes = key.map_or(0, <[u8]>::len) + value.map_or(0, <[u8]>::len);
        if record_bytes > self.max_request_size {
            return Err(format!(
                "Kafka record is {record_bytes} bytes, exceeding max.request.size={}",
                self.max_request_size
            ));
        }
        let mut record = BaseRecord::<[u8], [u8]>::to(topic);
        if let Some(key) = key {
            record = record.key(key);
        }
        if let Some(value) = value {
            record = record.payload(value);
        }
        let deadline = std::time::Instant::now() + self.max_block;
        loop {
            if let Some(error) = self.shared.delivery_error.lock().unwrap().clone() {
                return Err(format!("record delivery failed: {error}"));
            }
            match self.producer.send(record) {
                Ok(()) => {
                    self.shared
                        .records
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.shared
                        .bytes
                        .fetch_add(record_bytes as u64, std::sync::atomic::Ordering::Relaxed);
                    return Ok(());
                }
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned)) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(format!(
                            "Kafka producer queue remained full for max.block.ms={}ms",
                            self.max_block.as_millis()
                        ));
                    }
                    // The wait must be fine-grained: at 10ms the producing thread spent ~80% of a
                    // full-queue epoch asleep (q15 differential profile, 2026-07-19); 1ms keeps
                    // the retry close to the queue's actual drain granularity.
                    std::thread::sleep(std::time::Duration::from_millis(1));
                    record = returned;
                }
                Err((error, _)) => return Err(format!("produce to {topic} failed: {error}")),
            }
        }
    }

    /// Flushes every buffered record and returns the latest statistics-observed identity, or
    /// `(-1, -1)` if no tick has carried one yet. Fails if any delivery failed — such an epoch
    /// must never become a committable. The identity is an advisory cross-check: the Java host
    /// compares it against the coordinator-authoritative identity captured at warm-up, and an
    /// epoch bump additionally surfaces as an abortable transaction error on this flush.
    fn flush(&self, timeout: std::time::Duration) -> Result<(i64, i64), String> {
        use rdkafka::producer::Producer;

        self.producer
            .flush(timeout)
            .map_err(|error| format!("flush failed: {error}"))?;
        if let Some(error) = self.shared.delivery_error.lock().unwrap().clone() {
            return Err(format!("record delivery failed: {error}"));
        }
        Ok(self.shared.identity.lock().unwrap().unwrap_or((-1, -1)))
    }

    fn byte_count(&self) -> u64 {
        self.shared.bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn abort_transaction(&self, timeout: std::time::Duration) -> Result<(), String> {
        use rdkafka::producer::Producer;

        self.producer
            .abort_transaction(timeout)
            .map_err(|error| format!("abort_transaction failed: {error}"))
    }
}

#[cfg(feature = "kafka")]
fn producer_timeout(timeout_millis: jlong) -> std::time::Duration {
    std::time::Duration::from_millis(timeout_millis.max(0) as u64)
}

/// Writes an identity into the caller-allocated `[producerId, epoch]` array, following the
/// caller-owned out-array convention of `drainKafkaSplit`.
#[cfg(feature = "kafka")]
fn write_identity(
    env: &JNIEnv,
    out_identity: &JLongArray,
    identity: (i64, i64),
) -> Result<(), String> {
    let values = [identity.0, identity.1];
    let length = env
        .get_array_length(out_identity)
        .map_err(|error| format!("failed to read producer identity array length: {error}"))?
        as usize;
    env.set_long_array_region(out_identity, 0, &values[..length.min(values.len())])
        .map_err(|error| format!("failed to write producer identity: {error}"))
}

/// Opens a native transactional producer and returns an opaque handle, released with
/// `closeKafkaProducer`. `configKeys`/`configValues` are librdkafka config applied verbatim; it
/// must include `transactional.id` and `statistics.interval.ms`.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_openTransactionalKafkaProducer<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_version: jint,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    transactional_id: JString<'local>,
    max_block_millis: jlong,
    max_request_size: jint,
) -> jlong {
    kafka_jni(&mut env, 0, |env| {
        if config_version != 1 {
            return Err(format!("unsupported Kafka producer config ABI {config_version}"));
        }
        let keys = read_string_array(env, &config_keys);
        let values = read_string_array(env, &config_values);
        if keys.len() != values.len() {
            return Err("Kafka producer config arrays have different lengths".to_string());
        }
        let mut seen = std::collections::HashSet::with_capacity(keys.len());
        for key in &keys {
            if !seen.insert(key) {
                return Err(format!("duplicate Kafka producer config key {key}"));
            }
            if key == "transactional.id" {
                return Err("transactional.id is runtime-owned".to_string());
            }
        }
        let transactional_id: String = env
            .get_string(&transactional_id)
            .map_err(|error| format!("failed to read transactional id: {error}"))?
            .into();
        if transactional_id.is_empty() {
            return Err("transactional id must not be empty".to_string());
        }
        let mut config: Vec<(String, String)> = keys.into_iter().zip(values).collect();
        config.push(("transactional.id".to_string(), transactional_id));
        Ok(into_handle(KafkaTransactionalProducer::open(
            &config,
            producer_timeout(max_block_millis),
            max_request_size.max(1) as usize,
        )?))
    })
}

/// Runs `init_transactions` and writes the broker-assigned `[producerId, epoch]` into
/// `outIdentity`.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_initKafkaTransactions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    timeout_millis: jlong,
    out_identity: JLongArray<'local>,
) {
    kafka_jni(&mut env, (), |env| {
        let producer = unsafe { &*(handle as *const KafkaTransactionalProducer) };
        let identity = producer.init_transactions(producer_timeout(timeout_millis))?;
        write_identity(env, &out_identity, identity)
    });
}

#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_beginKafkaTransaction<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    kafka_jni(&mut env, (), |_env| {
        let producer = unsafe { &*(handle as *const KafkaTransactionalProducer) };
        producer.begin_transaction()
    });
}

/// Produces one record into the open transaction. A null `key` produces an unkeyed record.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_produceKafkaRecord<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topic: JString<'local>,
    key: JByteArray<'local>,
    value: JByteArray<'local>,
) {
    kafka_jni(&mut env, (), |env| {
        let producer = unsafe { &*(handle as *const KafkaTransactionalProducer) };
        let topic: String = env
            .get_string(&topic)
            .map_err(|error| format!("failed to read topic: {error}"))?
            .into();
        let key = if key.is_null() {
            None
        } else {
            Some(
                env.convert_byte_array(&key)
                    .map_err(|error| format!("failed to read record key: {error}"))?,
            )
        };
        let value = if value.is_null() {
            None
        } else {
            Some(
                env.convert_byte_array(&value)
                    .map_err(|error| format!("failed to read record value: {error}"))?,
            )
        };
        producer.produce(&topic, key.as_deref(), value.as_deref())
    });
}

/// Imports, JSON-encodes, and produces a complete Arrow batch without constructing per-row JNI
/// objects. Returns the total key+value payload bytes enqueued for producer metrics.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_produceKafkaBatch<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topic: JString<'local>,
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
) -> jlong {
    kafka_jni(&mut env, 0, |env| {
        let producer = unsafe { &*(handle as *const KafkaTransactionalProducer) };
        let topic: String = env
            .get_string(&topic)
            .map_err(|error| format!("failed to read topic: {error}"))?
            .into();
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
        let records = encode_records(
            &batch,
            &options,
            &key_options,
            &logical_types,
            &field_names,
            &key_fields,
            &value_fields,
            upsert != 0,
        )?;
        let before = producer.byte_count();
        for index in 0..records.len() {
            producer.produce(&topic, records.key(index), records.value(index))?;
        }
        Ok((producer.byte_count() - before) as jlong)
    })
}

/// Flushes the open transaction's records and writes the `[producerId, epoch]` it runs under into
/// `outIdentity`. After this returns the transaction is fully materialized on the broker (still
/// ONGOING) and the producer can be closed without losing it.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_flushKafkaProducer<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    timeout_millis: jlong,
    out_identity: JLongArray<'local>,
) {
    kafka_jni(&mut env, (), |env| {
        let producer = unsafe { &*(handle as *const KafkaTransactionalProducer) };
        let identity = producer.flush(producer_timeout(timeout_millis))?;
        write_identity(env, &out_identity, identity)
    });
}

#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_abortKafkaTransaction<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    timeout_millis: jlong,
) {
    kafka_jni(&mut env, (), |_env| {
        let producer = unsafe { &*(handle as *const KafkaTransactionalProducer) };
        producer.abort_transaction(producer_timeout(timeout_millis))
    });
}

/// Destroys the producer WITHOUT committing or aborting: librdkafka sends nothing on destroy, so an
/// open flushed transaction stays ONGOING on the broker for the Java committer to finish.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_closeKafkaProducer<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        drop(unsafe { from_handle::<KafkaTransactionalProducer>(handle) });
    })
}

/// Diagnostic-only (producer throughput probe): produces `count` copies of one record inside a
/// single JNI call, isolating librdkafka's drain rate from per-record JNI crossing cost.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_kafka_NativeKafka_produceKafkaRecordRepeated<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topic: JString<'local>,
    value: JByteArray<'local>,
    count: jlong,
) {
    kafka_jni(&mut env, (), |env| {
        let producer = unsafe { &*(handle as *const KafkaTransactionalProducer) };
        let topic: String = env
            .get_string(&topic)
            .map_err(|error| format!("failed to read topic: {error}"))?
            .into();
        let value = env
            .convert_byte_array(&value)
            .map_err(|error| format!("failed to read record value: {error}"))?;
        for _ in 0..count {
            producer.produce(&topic, None, Some(&value))?;
        }
        Ok(())
    });
}
