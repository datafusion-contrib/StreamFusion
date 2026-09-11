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
/// `timestamp-format.standard`, and `raw.endianness`. Only options the planner has vetted reach
/// here — anything unsupported already fell back — so an unknown key is a wiring bug, not user input.
pub struct FormatOptions {
    pub csv: CsvOptions,
    pub timestamp_mode: flink_text::TimestampMode,
    pub raw_little_endian: bool,
    pub keyed: Option<KeyedSpec>,
}

/// A keyed table's decode composition (Flink's `key.format` on the source side): which physical
/// position the raw-decoded Kafka key fills, which positions the value decode fills (its row type
/// is the physical schema projected to them), and the key's `key.raw.endianness`. Rides the same
/// option lines as the format options so the connector needs no new JNI surface.
#[derive(Clone)]
pub struct KeyedSpec {
    pub key_position: usize,
    pub value_positions: Vec<usize>,
    pub key_little_endian: bool,
}

pub fn parse_format_options(encoded: &str) -> FormatOptions {
    let mut csv = CsvOptions::default();
    let mut timestamp_mode = flink_text::TimestampMode::default();
    let mut raw_little_endian = false;
    let mut keyed_key_position = None;
    let mut keyed_value_positions = None;
    let mut keyed_key_little_endian = false;
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
    FormatOptions {
        csv,
        timestamp_mode,
        raw_little_endian,
        keyed,
    }
}
