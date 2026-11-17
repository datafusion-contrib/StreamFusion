use crate::*;

#[derive(Clone)]
pub struct CsvOptions {
    pub delimiter: u8,
    /// `None` is `disable-quote-character` — a quote char is ordinary field content.
    pub quote: Option<u8>,
    pub comments: bool,
    /// `null-literal`: a field exactly equal to this (post-unquoting, pre-trim) decodes to NULL,
    /// for every column type.
    pub null_literal: Option<String>,
}

impl Default for CsvOptions {
    fn default() -> CsvOptions {
        CsvOptions {
            delimiter: b',',
            quote: Some(b'"'),
            comments: false,
            null_literal: None,
        }
    }
}

/// The decode-relevant format options the JVM plumbs through as `key=value` lines (one per line,
/// split on the first `=`): the table's `csv.*` Jackson knobs, the JSON family's
/// `timestamp-format.standard`, `raw.endianness`, and a keyed table's Avro key schemas. Only
/// options the planner has vetted reach here — anything unsupported already fell back — so an
/// unknown key is a wiring bug, not user input.
pub struct FormatOptions {
    pub csv: CsvOptions,
    pub timestamp_mode: flink_text::TimestampMode,
    pub json_reject_array_roots: bool,
    /// A JSON decode reports the null rows Flink 2.x collects for null array elements, which its
    /// key/value merge trips over ([`crate::NullRows`]); the keyed composition sets it for a JSON
    /// key and for a strict JSON value.
    pub json_null_rows: bool,
    pub raw_little_endian: bool,
    /// An Avro key decoder's writer schema (a bare `avro` key, derived from the key's row type) and
    /// reader schema (an `avro-confluent` key, whose writer schemas come from the registry). A
    /// value decoder receives its schemas as call arguments instead.
    pub avro_writer_schema: String,
    pub avro_reader_schema: String,
    pub keyed: Option<KeyedSpec>,
    /// The Kafka connector metadata columns the decode fills: each output position and its key.
    pub metadata: Vec<(usize, KafkaMetadata)>,
}

/// A keyed table's decode composition (Flink's `key.format` on the source side): the key format,
/// which physical positions its fields fill (in `key.fields` order), which positions the value
/// decode fills (its row type is the physical schema projected to them), the `key.fields-prefix`
/// the key format's field names drop, and the key format's own option lines and skip mode. Rides
/// the same option lines as the format options so the connector needs no new JNI surface.
#[derive(Clone)]
pub struct KeyedSpec {
    pub key_format: i32,
    pub key_positions: Vec<usize>,
    pub value_positions: Vec<usize>,
    pub key_prefix: String,
    pub key_skip_errors: bool,
    pub key_options: String,
}

pub fn parse_format_options(encoded: &str) -> FormatOptions {
    let mut csv = CsvOptions::default();
    let mut timestamp_mode = flink_text::TimestampMode::default();
    let mut raw_little_endian = false;
    let mut json_reject_array_roots = false;
    let mut json_null_rows = false;
    let mut avro_writer_schema = String::new();
    let mut avro_reader_schema = String::new();
    let mut keyed_key_format = None;
    let mut keyed_key_positions = None;
    let mut keyed_value_positions = None;
    let mut keyed_key_prefix = String::new();
    let mut keyed_key_skip_errors = false;
    let mut keyed_key_options = String::new();
    let mut metadata = Vec::new();
    for line in encoded.lines().filter(|l| !l.is_empty()) {
        let (key, value) = line
            .split_once('=')
            .expect("format option is not key=value");
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
            "json.reject-array-roots" => json_reject_array_roots = value == "true",
            "json.null-rows" => json_null_rows = value == "true",
            "timestamp-format" => {
                timestamp_mode = match value {
                    "ISO-8601" => flink_text::TimestampMode::Iso8601,
                    "SQL" => flink_text::TimestampMode::Sql,
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
            "avro.writer-schema" => avro_writer_schema = value.to_string(),
            "avro.reader-schema" => avro_reader_schema = value.to_string(),
            "keyed.key-format" => {
                keyed_key_format = Some(value.parse::<i32>().expect("keyed.key-format"))
            }
            "keyed.key-positions" => keyed_key_positions = Some(positions(value)),
            "keyed.value-positions" => keyed_value_positions = Some(positions(value)),
            "keyed.key-prefix" => keyed_key_prefix = value.to_string(),
            "keyed.key-skip-errors" => keyed_key_skip_errors = value == "true",
            key if key.starts_with("keyed.key.") => {
                keyed_key_options.push_str(&format!("{}={value}\n", &key["keyed.key.".len()..]))
            }
            "metadata.columns" => {
                metadata = value
                    .split(',')
                    .map(|column| {
                        let (position, key) = column
                            .split_once(':')
                            .expect("metadata column is not position:key");
                        (
                            position.parse::<usize>().expect("metadata column position"),
                            KafkaMetadata::parse(key),
                        )
                    })
                    .collect()
            }
            other => panic!("unknown format option {other}"),
        }
    }
    let keyed = keyed_key_format.map(|key_format| KeyedSpec {
        key_format,
        key_positions: keyed_key_positions.expect("keyed decode carries no key positions"),
        value_positions: keyed_value_positions.expect("keyed decode carries no value positions"),
        key_prefix: keyed_key_prefix,
        key_skip_errors: keyed_key_skip_errors,
        key_options: keyed_key_options,
    });
    FormatOptions {
        csv,
        timestamp_mode,
        json_reject_array_roots,
        json_null_rows,
        raw_little_endian,
        avro_writer_schema,
        avro_reader_schema,
        keyed,
        metadata,
    }
}

fn positions(value: &str) -> Vec<usize> {
    if value.is_empty() {
        return Vec::new();
    }
    value
        .split(',')
        .map(|position| position.parse::<usize>().expect("keyed position"))
        .collect()
}
