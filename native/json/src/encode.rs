use crate::*;
/// One JSON format instance's encode-affecting options — Flink configures the Jackson mapper and
/// converter family per format instance from the `json.*` option set, so the native encoder takes
/// the same set wherever a format instance would exist. The defaults are the json format
/// factory's own.
pub struct JsonEncodeOptions {
    pub ignore_null_fields: bool,
    pub iso_8601: bool,
    pub decimal_as_plain_number: bool,
    pub legacy_decimal_nodes: bool,
    pub map_null_key_mode: MapNullKeyMode,
    pub map_null_key_literal: String,
}

impl Default for JsonEncodeOptions {
    fn default() -> JsonEncodeOptions {
        JsonEncodeOptions {
            ignore_null_fields: false,
            iso_8601: false,
            decimal_as_plain_number: false,
            legacy_decimal_nodes: false,
            map_null_key_mode: MapNullKeyMode::Fail,
            map_null_key_literal: "null".to_string(),
        }
    }
}

/// Flink's `json.map-null-key.mode`: what a serialized map does with a null key.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MapNullKeyMode {
    Fail,
    Drop,
    Literal,
}

/// Parses one format instance's `EncodeFormat` option lines. Only options the planner has resolved
/// reach here — anything unsupported already fell back — so an unknown key is a wiring bug.
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
            "legacy-decimal-nodes" => options.legacy_decimal_nodes = value == "true",
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
/// The CDC JSON dialect whose envelope wraps each encoded row on the sink side.
#[derive(Clone, Copy)]
pub enum CdcEnvelope {
    /// `{"before", "after", "op"}`, op ∈ {`c`, `d`} — `DebeziumJsonSerializationSchema`.
    Debezium,
    /// `{"before", "after", "op_type"}`, op ∈ {`I`, `D`} — `OggJsonSerializationSchema`.
    Ogg,
    /// `{"data", "type"}`, type ∈ {`insert`, `delete`} — `MaxwellJsonSerializationSchema`.
    Maxwell,
    /// `{"data": [row], "type"}`, type ∈ {`INSERT`, `DELETE`} — `CanalJsonSerializationSchema`.
    Canal,
}

pub fn encode_json_batch(
    batch: &RecordBatch,
    options: &JsonEncodeOptions,
    logical_types: &[String],
    field_names: &[String],
) -> Result<EncodedLines, String> {
    let batch = annotate_flink_types(batch, logical_types, field_names)?;
    let encoder_options = json_encoder_options(options);
    encode_json_rows(&batch, &encoder_options)
}

fn json_encoder_options(options: &JsonEncodeOptions) -> arrow::json::writer::EncoderOptions {
    let mut encoder_options = arrow::json::writer::EncoderOptions::default()
        .with_explicit_nulls(!options.ignore_null_fields)
        .with_time_format("%H:%M:%S".to_string())
        .with_encoder_factory(Arc::new(FlinkJsonEncoderFactory {
            iso_8601: options.iso_8601,
            decimal_as_plain_number: options.decimal_as_plain_number,
            legacy_decimal_nodes: options.legacy_decimal_nodes,
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
/// its format/factory configuration and the Flink logical-type annotation plan are task-local.
struct PreparedJsonEncoder {
    encoder_options: arrow::json::writer::EncoderOptions,
    annotation: std::cell::RefCell<Option<BatchAnnotationPlan>>,
}

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
#[derive(Debug)]
struct FlinkJsonEncoderFactory {
    iso_8601: bool,
    decimal_as_plain_number: bool,
    legacy_decimal_nodes: bool,
    map_null_key_mode: MapNullKeyMode,
    /// The `map-null-key.literal` pre-rendered as a quoted, escaped JSON field name.
    map_null_key_literal: Vec<u8>,
}

impl arrow::json::writer::EncoderFactory for FlinkJsonEncoderFactory {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        options: &'a arrow::json::writer::EncoderOptions,
    ) -> Result<Option<arrow::json::writer::NullableEncoder<'a>>, arrow::error::ArrowError> {
        use arrow::array::cast::AsArray;
        use arrow::datatypes::Decimal128Type;
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
                legacy_decimal_nodes: self.legacy_decimal_nodes,
            })),
            // TIMESTAMP_LTZ is an instant Flink renders at UTC with a 'Z' designator; plain
            // TIMESTAMP is the same wall-clock digit layout without any zone. Both trim the
            // fraction to its shortest form (appendFraction(NANO_OF_SECOND, 0, 9, true)). The
            // boundary marks LTZ leaves with a UTC timezone (`BatchAnnotationPlan`), so the Arrow type
            // alone selects the designator — at any nesting depth, since arrow-json's container
            // encoders consult this factory recursively.
            data_type if streamfusion_bridge::timestamp::is_timestamp(data_type) => {
                Some(Box::new(FlinkTimestampEncoder {
                    array: streamfusion_bridge::timestamp::TimestampColumn::try_new(array)?,
                    iso_8601: self.iso_8601,
                    zulu: streamfusion_bridge::timestamp::timestamp_timezone(data_type).is_some(),
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
struct FlinkDoubleEncoder<'a> {
    array: &'a arrow::array::Float64Array,
}

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
struct FlinkFloatEncoder<'a> {
    array: &'a arrow::array::Float32Array,
}

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
struct FlinkDateEncoder<'a> {
    array: &'a arrow::array::Date32Array,
}

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
struct FlinkMapEncoder<'a> {
    offsets: arrow::buffer::OffsetBuffer<i32>,
    keys: arrow::json::writer::NullableEncoder<'a>,
    values: arrow::json::writer::NullableEncoder<'a>,
    null_key_mode: MapNullKeyMode,
    /// Pre-rendered quoted, escaped `map-null-key.literal`.
    null_key_literal: Vec<u8>,
    scratch: Vec<u8>,
}

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
struct FlinkStringEncoder<'a, O: arrow::array::OffsetSizeTrait> {
    array: &'a arrow::array::GenericStringArray<O>,
}

impl<O: arrow::array::OffsetSizeTrait> arrow::json::writer::Encoder for FlinkStringEncoder<'_, O> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        encode_json_string_value(self.array.value(index).as_bytes(), output);
    }
}

/// One quoted, Jackson-escaped JSON string value.
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

struct FlinkBinaryEncoder<'a, O: arrow::array::OffsetSizeTrait> {
    array: &'a arrow::array::GenericBinaryArray<O>,
}

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

struct FlinkFixedSizeBinaryEncoder<'a> {
    array: &'a arrow::array::FixedSizeBinaryArray,
}

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

struct FlinkDecimal128Encoder<'a> {
    array: &'a arrow::array::Decimal128Array,
    scale: i8,
    plain: bool,
    legacy_decimal_nodes: bool,
}

impl arrow::json::writer::Encoder for FlinkDecimal128Encoder<'_> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        if self.legacy_decimal_nodes {
            encode_legacy_decimal_node(self.array.value(index), self.scale, self.plain, output);
        } else {
            encode_java_big_decimal(self.array.value(index), self.scale, self.plain, output);
        }
    }
}

struct FlinkTimestampEncoder<'a> {
    array: streamfusion_bridge::timestamp::TimestampColumn<'a>,
    iso_8601: bool,
    zulu: bool,
}

impl arrow::json::writer::Encoder for FlinkTimestampEncoder<'_> {
    fn encode(&mut self, index: usize, output: &mut Vec<u8>) {
        // Flink's formatters never consult the column's declared precision: the fraction is
        // whatever the value's nanoseconds carry, trimmed of trailing zeros, so the full
        // nine-digit width is always offered.
        encode_flink_timestamp_value(
            self.array.value(index).expect("timestamp components"),
            9,
            self.iso_8601,
            self.zulu,
            output,
        );
    }
}

pub struct JsonMessageEncoder {
    options: JsonEncodeOptions,
    envelope: Option<CdcEnvelope>,
    prepared: PreparedJsonEncoder,
}
impl MessageEncoder for JsonMessageEncoder {
    fn encode(
        &self,
        batch: &RecordBatch,
        kinds: Option<&Int8Array>,
        types: &[String],
        names: &[String],
    ) -> Result<EncodedLines, String> {
        let rows = self.prepared.encode(batch, types, names)?;
        match self.envelope {
            None => Ok(rows),
            Some(dialect) => {
                wrap_cdc_envelopes(&rows, kinds, dialect, self.options.ignore_null_fields)
            }
        }
    }
}
pub fn supports_encoder(format: i32) -> bool {
    matches!(
        format,
        FORMAT_JSON
            | FORMAT_DEBEZIUM_JSON
            | FORMAT_OGG_JSON
            | FORMAT_MAXWELL_JSON
            | FORMAT_CANAL_JSON
    )
}
pub fn create_encoder(format: i32, options: &str) -> Result<Box<dyn MessageEncoder>, String> {
    let envelope = match format {
        FORMAT_JSON => None,
        FORMAT_DEBEZIUM_JSON => Some(CdcEnvelope::Debezium),
        FORMAT_OGG_JSON => Some(CdcEnvelope::Ogg),
        FORMAT_MAXWELL_JSON => Some(CdcEnvelope::Maxwell),
        FORMAT_CANAL_JSON => Some(CdcEnvelope::Canal),
        other => return Err(format!("JSON library does not encode format {other}")),
    };
    let options = parse_json_encode_options(options)?;
    let prepared = PreparedJsonEncoder::new(&options);
    Ok(Box::new(JsonMessageEncoder {
        options,
        envelope,
        prepared,
    }))
}
