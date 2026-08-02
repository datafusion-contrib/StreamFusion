//! Flink-exact CSV encode for the Kafka sink: one CSV record per row, byte-identical to
//! `CsvRowDataSerializationSchema` (Jackson CSV writing a converted `JsonNode` row).
//!
//! arrow-csv's writer is NOT used, for the encode-side mirror of the decode reasoning
//! (divergences/21): Jackson's record envelope cannot be configured into it. The load-bearing
//! quirks replicated here, all pinned by the Java referee test against Flink's serializer:
//!
//! - No trailing line separator (Flink builds the schema `withLineSeparator("")`).
//! - Only string-shaped values (strings, base64 binary, date/time/timestamp spellings, and the
//!   joined array/row value) go through the quote decision; numbers, booleans, and the
//!   null literal are always written raw — even when they contain the delimiter.
//! - Jackson's default "loose" quote check (`STRICT_CHECK_FOR_QUOTING` off): a value longer than
//!   24 UTF-16 units is always quoted; otherwise it is quoted when any char is `<=`
//!   `max(delimiter, quote)`, equals the configured escape character, or — with no escape
//!   configured — equals `'\'` (Jackson's fallback control-escape char). With the quote character
//!   disabled nothing is ever quoted or escaped.
//! - Inside quotes the quote char is doubled (Flink never enables
//!   `ESCAPE_QUOTE_CHAR_WITH_ESCAPE_CHAR`), and a configured escape char is itself doubled;
//!   nothing else is escaped, so delimiters and line breaks ride inside the quotes verbatim.
//! - A nested ROW or ARRAY is one CSV field: elements are rendered raw (null elements as the
//!   null literal) and joined by `array-element-delimiter`, and only the joined whole goes
//!   through the quote decision.
//! - DECIMAL defaults to `BigDecimal.toPlainString()` at the column's exact scale — the
//!   `write-bigdecimal-in-scientific-notation` option's declared default of true is dead in
//!   Flink's factory (`getOptional` never yields a ConfigOption default), so only an explicit
//!   true selects `stripTrailingZeros().toString()`.
//! - DATE is `ISO_LOCAL_DATE` (years past 9999 gain `+`, negative years `-`), TIME is
//!   `ISO_LOCAL_TIME` of the millisecond value (seconds always present, fraction trimmed of
//!   trailing zeros), TIMESTAMP is the SQL spelling with a value-trimmed nanosecond fraction,
//!   and TIMESTAMP_LTZ is the same wall-clock digits plus Flink's `'Z'` designator.
//!
//! FLOAT/DOUBLE spell through the legacy `Double.toString`/`Float.toString` port (`jdk_double`),
//! written raw like every Jackson number — NaN and the infinities included. The planner's
//! spelling probe only admits these columns while the host JVM (JDK ≤ 18) still spells them the
//! same way.

use crate::*;
use arrow::array::cast::AsArray;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{
    Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampNanosecondType,
};

/// One CSV format instance's encode-affecting options, defaults from Flink's `CsvSchema`.
pub(crate) struct CsvEncodeOptions {
    pub(crate) delimiter: u8,
    /// `None` is `disable-quote-character`: nothing is ever quoted.
    pub(crate) quote: Option<u8>,
    pub(crate) array_separator: u8,
    pub(crate) escape: Option<u8>,
    /// Written raw for every null field and null container element; empty by default.
    pub(crate) null_literal: Vec<u8>,
    /// `write-bigdecimal-in-scientific-notation`: strip trailing zeros and use Java's
    /// `BigDecimal.toString()`; the default keeps the column scale as `toPlainString()`.
    pub(crate) scientific_decimal: bool,
}

impl Default for CsvEncodeOptions {
    fn default() -> CsvEncodeOptions {
        CsvEncodeOptions {
            delimiter: b',',
            quote: Some(b'"'),
            array_separator: b';',
            escape: None,
            null_literal: Vec::new(),
            scientific_decimal: false,
        }
    }
}

/// Parses one format instance's `EncodeFormat` option lines (see `EncodeFormat.csv` on the Java
/// side, which admits only single ASCII characters for the character options). Only options the
/// planner has resolved reach here, so an unknown key is a wiring bug.
pub(crate) fn parse_csv_encode_options(encoded: &str) -> Result<CsvEncodeOptions, String> {
    let mut options = CsvEncodeOptions::default();
    for line in encoded.lines().filter(|line| !line.is_empty()) {
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("encode option is not key=value: {line}"))?;
        let single_byte = || {
            value
                .as_bytes()
                .first()
                .copied()
                .filter(|_| value.len() == 1)
                .ok_or_else(|| format!("CSV encode option {key} is not one byte: {value}"))
        };
        match key {
            "field-delimiter" => options.delimiter = single_byte()?,
            "quote-character" => options.quote = Some(single_byte()?),
            "disable-quote-character" => options.quote = None,
            "array-element-delimiter" => options.array_separator = single_byte()?,
            "escape-character" => options.escape = Some(single_byte()?),
            "null-literal" => options.null_literal = value.as_bytes().to_vec(),
            "write-bigdecimal-in-scientific-notation" => {
                options.scientific_decimal = value == "true"
            }
            other => return Err(format!("unknown CSV encode option {other}")),
        }
    }
    Ok(options)
}

pub(crate) fn encode_csv_batch(
    batch: &RecordBatch,
    options: &CsvEncodeOptions,
    logical_types: &[String],
    field_names: &[String],
) -> Result<EncodedLines, String> {
    let batch = annotate_flink_types(batch, logical_types, field_names)?;
    let mut bytes = Vec::new();
    let mut lines = Vec::with_capacity(batch.num_rows());
    let mut scratch = Vec::new();
    for row in 0..batch.num_rows() {
        let start = bytes.len();
        for (index, column) in batch.columns().iter().enumerate() {
            if index > 0 {
                bytes.push(options.delimiter);
            }
            encode_csv_field(column, row, options, &mut scratch, &mut bytes)?;
        }
        lines.push(start..bytes.len());
    }
    Ok(EncodedLines::new(bytes, lines))
}

fn encode_csv_field(
    column: &ArrayRef,
    row: usize,
    options: &CsvEncodeOptions,
    scratch: &mut Vec<u8>,
    out: &mut Vec<u8>,
) -> Result<(), String> {
    if column.is_null(row) {
        // Jackson's writeNull appends the schema's null value with no quote decision.
        out.extend_from_slice(&options.null_literal);
        return Ok(());
    }
    scratch.clear();
    match column.data_type() {
        DataType::List(_) => {
            let list = column.as_list::<i32>();
            join_elements(&list.value(row), options, scratch)?;
            write_csv_text(scratch, options, out);
        }
        DataType::LargeList(_) => {
            let list = column.as_list::<i64>();
            join_elements(&list.value(row), options, scratch)?;
            write_csv_text(scratch, options, out);
        }
        DataType::Struct(_) => {
            let entries = column.as_struct();
            for (index, child) in entries.columns().iter().enumerate() {
                if index > 0 {
                    scratch.push(options.array_separator);
                }
                render_element(child, row, options, scratch)?;
            }
            write_csv_text(scratch, options, out);
        }
        _ => match render_csv_scalar(column, row, options, scratch)? {
            // Numbers and booleans go through Jackson's raw number path: never quoted.
            CsvScalar::Raw => out.extend_from_slice(scratch),
            CsvScalar::Text => write_csv_text(scratch, options, out),
        },
    }
    Ok(())
}

/// Joins one ARRAY value: elements rendered raw into `scratch`, separated by
/// `array-element-delimiter` (Jackson's `_addToArray`), nulls as the null literal.
fn join_elements(
    values: &ArrayRef,
    options: &CsvEncodeOptions,
    scratch: &mut Vec<u8>,
) -> Result<(), String> {
    for index in 0..values.len() {
        if index > 0 {
            scratch.push(options.array_separator);
        }
        render_element(values, index, options, scratch)?;
    }
    Ok(())
}

fn render_element(
    values: &ArrayRef,
    index: usize,
    options: &CsvEncodeOptions,
    scratch: &mut Vec<u8>,
) -> Result<(), String> {
    if values.is_null(index) {
        scratch.extend_from_slice(&options.null_literal);
        return Ok(());
    }
    render_csv_scalar(values, index, options, scratch).map(|_| ())
}

enum CsvScalar {
    /// Jackson's number/boolean writes: appended raw, never quoted.
    Raw,
    /// A string-shaped value: subject to the quote decision (and raw inside a joined array).
    Text,
}

/// Renders one scalar the way Flink's `RowDataToCsvConverters` + Jackson spell it. The planner's
/// type gate admits exactly these types, so anything else is a wiring bug.
fn render_csv_scalar(
    column: &dyn Array,
    row: usize,
    options: &CsvEncodeOptions,
    out: &mut Vec<u8>,
) -> Result<CsvScalar, String> {
    use std::io::Write;

    match column.data_type() {
        DataType::Boolean => {
            let value: &[u8] = if column.as_boolean().value(row) { b"true" } else { b"false" };
            out.extend_from_slice(value);
            Ok(CsvScalar::Raw)
        }
        DataType::Int8 => {
            write!(out, "{}", column.as_primitive::<Int8Type>().value(row)).expect("int digits");
            Ok(CsvScalar::Raw)
        }
        DataType::Int16 => {
            write!(out, "{}", column.as_primitive::<Int16Type>().value(row)).expect("int digits");
            Ok(CsvScalar::Raw)
        }
        DataType::Int32 => {
            write!(out, "{}", column.as_primitive::<Int32Type>().value(row)).expect("int digits");
            Ok(CsvScalar::Raw)
        }
        DataType::Int64 => {
            write!(out, "{}", column.as_primitive::<Int64Type>().value(row)).expect("int digits");
            Ok(CsvScalar::Raw)
        }
        DataType::Float32 => {
            crate::jdk_double::jdk_float_to_string(
                column.as_primitive::<Float32Type>().value(row),
                out,
            );
            Ok(CsvScalar::Raw)
        }
        DataType::Float64 => {
            crate::jdk_double::jdk_double_to_string(
                column.as_primitive::<Float64Type>().value(row),
                out,
            );
            Ok(CsvScalar::Raw)
        }
        DataType::Decimal128(_, scale) => {
            encode_java_big_decimal(
                column.as_primitive::<Decimal128Type>().value(row),
                *scale,
                !options.scientific_decimal,
                out,
            );
            Ok(CsvScalar::Raw)
        }
        DataType::Utf8 => {
            out.extend_from_slice(column.as_string::<i32>().value(row).as_bytes());
            Ok(CsvScalar::Text)
        }
        DataType::LargeUtf8 => {
            out.extend_from_slice(column.as_string::<i64>().value(row).as_bytes());
            Ok(CsvScalar::Text)
        }
        DataType::Binary => {
            encode_base64(column.as_binary::<i32>().value(row), out);
            Ok(CsvScalar::Text)
        }
        DataType::LargeBinary => {
            encode_base64(column.as_binary::<i64>().value(row), out);
            Ok(CsvScalar::Text)
        }
        DataType::Date32 => {
            iso_local_date(i64::from(column.as_primitive::<arrow::datatypes::Date32Type>().value(row)), out);
            Ok(CsvScalar::Text)
        }
        DataType::Time32(arrow::datatypes::TimeUnit::Second) => {
            iso_local_time(i64::from(column.as_primitive::<Time32SecondType>().value(row)) * 1_000, out);
            Ok(CsvScalar::Text)
        }
        DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => {
            iso_local_time(i64::from(column.as_primitive::<Time32MillisecondType>().value(row)), out);
            Ok(CsvScalar::Text)
        }
        DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
            iso_local_time(column.as_primitive::<Time64MicrosecondType>().value(row) / 1_000, out);
            Ok(CsvScalar::Text)
        }
        DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond) => {
            iso_local_time(column.as_primitive::<Time64NanosecondType>().value(row) / 1_000_000, out);
            Ok(CsvScalar::Text)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, timezone) => {
            sql_timestamp(
                column.as_primitive::<TimestampNanosecondType>().value(row),
                timezone.is_some(),
                out,
            );
            Ok(CsvScalar::Text)
        }
        other => Err(format!("CSV encode does not support column type {other}")),
    }
}

fn encode_base64(input: &[u8], out: &mut Vec<u8>) {
    use base64::Engine;
    let start = out.len();
    let encoded_len = base64::encoded_len(input.len(), true).expect("base64 output length");
    out.resize(start + encoded_len, 0);
    base64::engine::general_purpose::STANDARD
        .encode_slice(input, &mut out[start..])
        .expect("sized base64 output");
}

/// The Jackson CSV quote-and-escape write for one string-shaped value (see the module docs for
/// the decision rules).
fn write_csv_text(value: &[u8], options: &CsvEncodeOptions, out: &mut Vec<u8>) {
    let Some(quote) = options.quote else {
        out.extend_from_slice(value);
        return;
    };
    let quoted = utf16_units(value) > 24 || {
        let min_safe = options.delimiter.max(quote) + 1;
        match options.escape {
            Some(escape) => value.iter().any(|&byte| byte < min_safe || byte == escape),
            None => value.iter().any(|&byte| byte < min_safe || byte == b'\\'),
        }
    };
    if !quoted {
        out.extend_from_slice(value);
        return;
    }
    out.push(quote);
    for &byte in value {
        if byte == quote {
            out.push(quote);
        } else if options.escape == Some(byte) {
            out.push(byte);
        }
        out.push(byte);
    }
    out.push(quote);
}

/// Jackson's 24-char always-quote threshold counts Java chars, i.e. UTF-16 code units: one per
/// UTF-8 sequence plus one more for each supplementary (4-byte) character.
fn utf16_units(value: &[u8]) -> usize {
    value
        .iter()
        .map(|&byte| usize::from((byte & 0xC0) != 0x80) + usize::from(byte >= 0xF0))
        .sum()
}

/// `DateTimeFormatter.ISO_LOCAL_TIME` over a millisecond-of-day value: seconds always present
/// (the optional section prints whenever the field exists, which it always does), fraction
/// trimmed of trailing zeros and omitted at zero.
fn iso_local_time(millis: i64, out: &mut Vec<u8>) {
    let second_of_day = (millis / 1_000) as u32;
    push_two_digits(out, second_of_day / 3_600);
    out.push(b':');
    push_two_digits(out, (second_of_day / 60) % 60);
    out.push(b':');
    push_two_digits(out, second_of_day % 60);
    push_trimmed_fraction(out, (millis % 1_000) as u32 * 1_000_000);
}

/// Flink's SQL timestamp spelling: ISO date, a space, ISO time with the value-trimmed nanosecond
/// fraction, and — for TIMESTAMP_LTZ — the `'Z'` designator
/// (`TimeFormats.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT`).
fn sql_timestamp(nanos: i64, zulu: bool, out: &mut Vec<u8>) {
    let seconds = nanos.div_euclid(1_000_000_000);
    let nano_of_second = nanos.rem_euclid(1_000_000_000) as u32;
    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400) as u32;
    iso_local_date(days, out);
    out.push(b' ');
    push_two_digits(out, second_of_day / 3_600);
    out.push(b':');
    push_two_digits(out, (second_of_day / 60) % 60);
    out.push(b':');
    push_two_digits(out, second_of_day % 60);
    push_trimmed_fraction(out, nano_of_second);
    if zulu {
        out.push(b'Z');
    }
}

/// `appendFraction(NANO_OF_SECOND, 0, 9, true)`: the nine-digit expansion with trailing zeros
/// trimmed, nothing at zero.
fn push_trimmed_fraction(out: &mut Vec<u8>, nano_of_second: u32) {
    if nano_of_second == 0 {
        return;
    }
    let mut fraction = nano_of_second;
    let mut width = 9;
    while fraction % 10 == 0 {
        fraction /= 10;
        width -= 1;
    }
    out.push(b'.');
    let mut divisor = 10_u32.pow(width - 1);
    while divisor != 0 {
        out.push(b'0' + ((fraction / divisor) % 10) as u8);
        divisor /= 10;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Date32Array, Decimal128Array, Int32Array, ListArray, StringArray,
        StructArray, Time32MillisecondArray, TimestampNanosecondArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Fields;

    fn encode(columns: Vec<(&str, ArrayRef)>, options: &CsvEncodeOptions) -> Vec<String> {
        let batch = RecordBatch::try_from_iter_with_nullable(
            columns.into_iter().map(|(name, array)| (name, array, true)),
        )
        .unwrap();
        let lines = encode_csv_batch(&batch, options, &[], &[]).unwrap();
        (0..lines.len())
            .map(|index| String::from_utf8(lines.line(index).to_vec()).unwrap())
            .collect()
    }

    fn strings(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(values.iter().copied().collect::<StringArray>())
    }

    #[test]
    fn writes_rows_without_line_separators() {
        let rows = encode(
            vec![
                ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
                ("name", strings(&[Some("alice"), Some("bob")])),
                ("ok", Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef),
            ],
            &CsvEncodeOptions::default(),
        );
        assert_eq!(rows, vec!["1,alice,true", "2,bob,false"]);
    }

    #[test]
    fn quotes_by_jacksons_loose_check() {
        let rows = encode(
            vec![(
                "value",
                strings(&[
                    Some("plain-text"),                // nothing at or below ',' (44)
                    Some("with,comma"),                // delimiter
                    Some("with space"),                // ' ' (32) below the safe threshold
                    Some("bang!"),                     // '!' (33) below the safe threshold
                    Some("slash/ok"),                  // '/' (47) is safe
                    Some("back\\slash"),               // Jackson's fallback escape char
                    Some("has\"quote"),                // doubled inside quotes
                    Some("line\nbreak"),               // rides raw inside quotes
                    Some("abcdefghijklmnopqrstuvwx"),  // 24 units: content decides
                    Some("abcdefghijklmnopqrstuvwxy"), // 25 units: always quoted
                    Some(""),
                ]),
            )],
            &CsvEncodeOptions::default(),
        );
        assert_eq!(
            rows,
            vec![
                "plain-text",
                "\"with,comma\"",
                "\"with space\"",
                "\"bang!\"",
                "slash/ok",
                "\"back\\slash\"",
                "\"has\"\"quote\"",
                "\"line\nbreak\"",
                "abcdefghijklmnopqrstuvwx",
                "\"abcdefghijklmnopqrstuvwxy\"",
                "",
            ]
        );
    }

    #[test]
    fn escape_character_replaces_the_backslash_rule_and_doubles_itself() {
        let options = CsvEncodeOptions { escape: Some(b'|'), ..CsvEncodeOptions::default() };
        let rows = encode(
            vec![(
                "value",
                strings(&[Some("back\\slash"), Some("pipe|here"), Some("q\"uote")]),
            )],
            &options,
        );
        assert_eq!(rows, vec!["back\\slash", "\"pipe||here\"", "\"q\"\"uote\""]);
    }

    #[test]
    fn disabled_quote_character_writes_everything_raw() {
        let options = CsvEncodeOptions { quote: None, ..CsvEncodeOptions::default() };
        let rows = encode(
            vec![("value", strings(&[Some("with,comma"), Some("q\"uote\nline")]))],
            &options,
        );
        assert_eq!(rows, vec!["with,comma", "q\"uote\nline"]);
    }

    #[test]
    fn null_literal_is_raw_even_with_a_delimiter_inside() {
        let options = CsvEncodeOptions {
            null_literal: b"n,a".to_vec(),
            ..CsvEncodeOptions::default()
        };
        let rows = encode(
            vec![
                ("a", strings(&[None])),
                ("b", Arc::new(Int32Array::from(vec![None::<i32>])) as ArrayRef),
            ],
            &options,
        );
        assert_eq!(rows, vec!["n,a,n,a"]);
    }

    #[test]
    fn decimal_defaults_to_plain_scale_and_opts_into_scientific() {
        let column: ArrayRef = Arc::new(
            Decimal128Array::from(vec![10000_i128, 12345, 0])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let plain = encode(vec![("d", column.clone())], &CsvEncodeOptions::default());
        assert_eq!(plain, vec!["100.00", "123.45", "0.00"]);
        let scientific = CsvEncodeOptions {
            scientific_decimal: true,
            ..CsvEncodeOptions::default()
        };
        assert_eq!(
            encode(vec![("d", column)], &scientific),
            vec!["1E+2", "123.45", "0"]
        );
    }

    #[test]
    fn temporal_spellings_follow_flinks_formatters() {
        let date: ArrayRef = Arc::new(Date32Array::from(vec![18321, -1, 2_932_897]));
        let time: ArrayRef =
            Arc::new(Time32MillisecondArray::from(vec![45_240_000, 45_296_789, 500]));
        let ts = TimestampNanosecondArray::from(vec![
            1_577_934_245_000_000_000,
            1_577_934_245_120_000_000,
            123_456_789,
        ]);
        let plain: ArrayRef = Arc::new(ts.clone());
        let instant: ArrayRef = Arc::new(ts.with_timezone("UTC"));
        let rows = encode(
            vec![("d", date), ("t", time), ("ts", plain), ("ltz", instant)],
            &CsvEncodeOptions::default(),
        );
        // Seconds are never elided; fractions trim to the value; LTZ carries Flink's 'Z'. Every
        // SQL timestamp is quoted — its space separator sits below Jackson's safe threshold — as
        // is a five-digit year's '+' sign.
        assert_eq!(
            rows,
            vec![
                "2020-02-29,12:34:00,\"2020-01-02 03:04:05\",\"2020-01-02 03:04:05Z\"",
                "1969-12-31,12:34:56.789,\"2020-01-02 03:04:05.12\",\"2020-01-02 03:04:05.12Z\"",
                "\"+10000-01-01\",00:00:00.5,\"1970-01-01 00:00:00.123456789\",\"1970-01-01 00:00:00.123456789Z\"",
            ]
        );
    }

    #[test]
    fn arrays_and_nested_rows_join_into_one_quotable_field() {
        let items = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0, 3, 3].into()),
            strings(&[Some("a"), None, Some("b,c")]),
            None,
        );
        let nested = StructArray::new(
            Fields::from(vec![
                Field::new("x", DataType::Int32, true),
                Field::new("y", DataType::Utf8, true),
            ]),
            vec![
                Arc::new(Int32Array::from(vec![Some(7), None])) as ArrayRef,
                strings(&[Some("ok"), Some("q\"uote")]),
            ],
            None,
        );
        let options = CsvEncodeOptions {
            null_literal: b"N/A".to_vec(),
            ..CsvEncodeOptions::default()
        };
        let rows = encode(
            vec![
                ("items", Arc::new(items) as ArrayRef),
                ("nested", Arc::new(nested) as ArrayRef),
            ],
            &options,
        );
        // Element strings are raw inside the joined value; the joined whole is one CSV field, so
        // the embedded comma and quote force quoting of that field alone.
        assert_eq!(rows, vec!["\"a;N/A;b,c\",7;ok", ",\"N/A;q\"\"uote\""]);
    }

    #[test]
    fn custom_delimiter_raises_the_quote_threshold() {
        // Jackson's loose check quotes anything at or below max(delimiter, quote): with a '|'
        // delimiter almost every letter falls below it.
        let options = CsvEncodeOptions { delimiter: b'|', ..CsvEncodeOptions::default() };
        let rows = encode(
            vec![
                ("a", strings(&[Some("abc")])),
                ("b", Arc::new(Int32Array::from(vec![12])) as ArrayRef),
            ],
            &options,
        );
        assert_eq!(rows, vec!["\"abc\"|12"]);
    }
}
