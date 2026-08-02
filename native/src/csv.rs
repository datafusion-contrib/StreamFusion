//! Flink-exact CSV decode: one Kafka message → at most one row, matching Flink's `csv` format
//! (Jackson CSV for record structure + `CsvToRowDataConverters` for values) byte for byte.
//!
//! arrow-csv's reader is NOT used: its value envelope is fixed and diverges from Flink's on valid
//! data — an empty string field decodes to null where Flink produces `""`, extra decimal fraction
//! digits truncate where Flink rounds HALF_UP (and overflow errors where Flink nulls), padded
//! numbers (`" 12 "`) error where Flink trims, and none of the delimiter/quote/comment/
//! null-literal options can reproduce Jackson's semantics. So the decode splits records with
//! `csv-core` (configured exactly like Flink's `CsvSchema`) and converts each field with the
//! shared Flink-exact text parsers (`flink_text`).
//!
//! Row semantics mirror `CsvRowDataDeserializationSchema.deserialize` + Jackson:
//! - only the FIRST record of a message is read (Jackson's `readValue`), the rest is ignored;
//! - a message with no record (empty, or only comment/blank lines under `allow-comments`) is an
//!   error;
//! - Jackson never skips a blank line on its own: a blank first line is a record with a single
//!   empty field (so it hits the row-arity check like any other short row);
//! - `allow-comments` skips `#` lines AND blank lines AND spaces (0x20 only) before the record,
//!   exactly Jackson's `_skipCommentLines`;
//! - invalid UTF-8 is a message-level error (Jackson's `CharConversionException` escapes the
//!   per-field handling), but only inside the byte range Jackson actually decodes: through the
//!   first record's terminator plus one lookahead character — extended, under `allow-comments`,
//!   across the comment/blank run that follows the record. Its `UTF8Reader` defers an error hit
//!   mid-buffer and the parser never reads again once the record and lookahead are in hand, so
//!   garbage beyond that window is invisible to Flink;
//! - more fields than columns is a record-level error; fewer is a row-arity error by default;
//! - `ignore-parse-errors` reproduces Flink's granularity: a record-level error (including the
//!   UTF-8 window check) drops the row, a short row null-pads (`validateArity` goes silent), and
//!   a field conversion error nulls just that field (`createNullableConverter`'s catch).

use crate::flink_text::*;
use crate::*;
use arrow::array::{Date32Array, Float64Array};

/// The Jackson `CsvSchema` knobs Flink's `csv` format exposes for decoding.
pub(crate) struct CsvOptions {
    pub(crate) delimiter: u8,
    /// `None` is `disable-quote-character` — a quote char is ordinary field content.
    pub(crate) quote: Option<u8>,
    pub(crate) comments: bool,
    /// `null-literal`: a field exactly equal to this (post-unquoting, pre-trim) decodes to NULL,
    /// for every column type.
    pub(crate) null_literal: Option<String>,
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

pub(crate) struct CsvDecoder {
    schema: SchemaRef,
    options: CsvOptions,
    /// Flink's `ignore-parse-errors` for CSV — handled here rather than by the generic
    /// per-message retry, because Flink's skip granularity is per-field, not per-message.
    skip_errors: bool,
}

enum Converted {
    Null,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Str(String),
    Date(i32),
    Timestamp(i64),
    Decimal(i128),
}

impl CsvDecoder {
    pub(crate) fn new(schema: SchemaRef, options: CsvOptions, skip_errors: bool) -> CsvDecoder {
        for field in schema.fields() {
            match field.data_type() {
                DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::Date32
                | DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
                | DataType::Decimal128(_, _) => {}
                other => panic!("CSV decode does not support column type {other}"),
            }
        }
        CsvDecoder { schema, options, skip_errors }
    }

    pub(crate) fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        let column = bodies.column(0);
        let arity = self.schema.fields().len();
        let mut rows: Vec<Vec<Converted>> = Vec::with_capacity(bodies.num_rows());
        let mut fields_buf: Vec<u8> = Vec::new();
        let mut ends: Vec<usize> = Vec::new();
        for row in 0..bodies.num_rows() {
            // A null message (tombstone) produces no row, like Flink's null-message guard.
            let Some(bytes) = binary_body(column, row) else { continue };
            let count = match self.read_first_record(bytes, &mut fields_buf, &mut ends) {
                Err(()) => {
                    // Invalid UTF-8 escapes Jackson's record handling: the whole row fails (or
                    // drops under ignore-parse-errors), never a single field.
                    if self.skip_errors {
                        continue;
                    }
                    panic!(
                        "CSV message is not valid UTF-8: {:?}",
                        String::from_utf8_lossy(bytes)
                    );
                }
                Ok(None) => {
                    if self.skip_errors {
                        continue;
                    }
                    panic!(
                        "CSV message contains no record: {:?}",
                        String::from_utf8_lossy(bytes)
                    );
                }
                Ok(Some(count)) => count,
            };
            if count > arity {
                // Jackson fails the whole record on extra columns; under ignore-parse-errors the
                // row is dropped (Flink's testDeserializeMoreColumnsThanExpected).
                if self.skip_errors {
                    continue;
                }
                panic!("CSV record has {count} fields, the schema expects {arity}");
            }
            if count < arity && !self.skip_errors {
                // Flink's validateArity: a short row throws unless ignore-parse-errors pads it.
                panic!("Row length mismatch. {arity} fields expected but was {count}.");
            }
            let mut converted: Vec<Converted> = Vec::with_capacity(arity);
            let mut start = 0;
            for (i, field) in self.schema.fields().iter().enumerate() {
                if i >= count {
                    converted.push(Converted::Null);
                    continue;
                }
                let end = ends[i];
                let text = std::str::from_utf8(&fields_buf[start..end])
                    .expect("record bytes were validated by the UTF-8 window check");
                start = end;
                if self.options.null_literal.as_deref() == Some(text) {
                    converted.push(Converted::Null);
                    continue;
                }
                match convert(field.data_type(), text) {
                    Some(value) => converted.push(value),
                    None if self.skip_errors => converted.push(Converted::Null),
                    None => {
                        panic!("Fail to deserialize at field: {} from \"{text}\".", field.name())
                    }
                }
            }
            rows.push(converted);
        }
        let columns: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| build_column(field.data_type(), &rows, i))
            .collect();
        RecordBatch::try_new(self.schema.clone(), columns).expect("failed to build CSV batch")
    }

    /// Reads the FIRST record of `input` like Jackson's `readValue`: skips the comment/blank
    /// prefix when `allow-comments` is set, treats a blank first line as a one-empty-field record
    /// otherwise, and captures the record's unquoted fields into `fields_buf`, with `ends[i]` the
    /// end offset of field `i`. `Ok(None)` when the input holds no record at all; `Err` when the
    /// bytes Jackson would decode are not valid UTF-8 (a message-level failure).
    fn read_first_record(
        &self,
        input: &[u8],
        fields_buf: &mut Vec<u8>,
        ends: &mut Vec<usize>,
    ) -> Result<Option<usize>, ()> {
        let prefix = if self.options.comments { skip_comment_lines(input, 0) } else { 0 };
        let body = &input[prefix..];
        let (count, consumed) = if body.is_empty() {
            (None, 0)
        } else if !self.options.comments && (body[0] == b'\n' || body[0] == b'\r') {
            // Without allow-comments Jackson never skips a blank line: it parses as a record
            // with a single empty field. csv-core would silently read past it.
            if ends.is_empty() {
                ends.push(0);
            }
            ends[0] = 0;
            let crlf = body[0] == b'\r' && body.get(1) == Some(&b'\n');
            (Some(1), if crlf { 2 } else { 1 })
        } else {
            let (count, consumed) = self.split_record(body, fields_buf, ends);
            (count, consumed)
        };
        let window = self.decoded_window(input, prefix + consumed);
        if std::str::from_utf8(&input[..window]).is_err() {
            return Err(());
        }
        Ok(count)
    }

    /// Splits the first record of `body` with csv-core (configured like Flink's `CsvSchema`),
    /// returning the field count and the bytes consumed through the record terminator.
    fn split_record(
        &self,
        body: &[u8],
        fields_buf: &mut Vec<u8>,
        ends: &mut Vec<usize>,
    ) -> (Option<usize>, usize) {
        use csv_core::{ReadRecordResult, ReaderBuilder};
        let mut builder = ReaderBuilder::new();
        builder.delimiter(self.options.delimiter);
        match self.options.quote {
            Some(quote) => builder.quote(quote),
            None => builder.quoting(false),
        };
        let mut reader = builder.build();
        fields_buf.resize(std::cmp::max(body.len(), 64), 0);
        ends.resize(std::cmp::max(self.schema.fields().len(), 8), 0);
        let mut consumed = 0;
        let mut written = 0;
        let mut end_count = 0;
        loop {
            // Once the input is drained, the empty slice signals EOF and flushes a final
            // unterminated record.
            let (result, nin, nout, nend) = reader.read_record(
                &body[consumed..],
                &mut fields_buf[written..],
                &mut ends[end_count..],
            );
            consumed += nin;
            // csv-core's ends are already record-absolute ("as if there was a single contiguous
            // buffer"), so they index fields_buf directly across calls.
            written += nout;
            end_count += nend;
            match result {
                ReadRecordResult::Record => return (Some(end_count), consumed),
                ReadRecordResult::End => return (None, consumed),
                ReadRecordResult::InputEmpty => {}
                ReadRecordResult::OutputFull => {
                    let len = fields_buf.len();
                    fields_buf.resize(len * 2, 0);
                }
                ReadRecordResult::OutputEndsFull => {
                    let len = ends.len();
                    ends.resize(len * 2, 0);
                }
            }
        }
    }

    /// End of the byte range Jackson decodes for one message: the first record plus one lookahead
    /// character (`startNewLine` peeks past the record's terminator) — with `allow-comments`, the
    /// peek first skips the comment/blank run after the record, decoding all of it. Bytes past
    /// this window are never decoded, so invalid UTF-8 there does not fail the message.
    fn decoded_window(&self, input: &[u8], record_end: usize) -> usize {
        let lookahead = if self.options.comments {
            skip_comment_lines(input, record_end)
        } else {
            record_end
        };
        match input.get(lookahead) {
            Some(&first) => std::cmp::min(lookahead + utf8_char_len(first), input.len()),
            None => input.len(),
        }
    }
}

/// Jackson's `_skipCommentLines`: from `pos`, consumes spaces (0x20 only), line ends, and whole
/// `#` comment lines, stopping at the first character that starts real content.
fn skip_comment_lines(input: &[u8], mut pos: usize) -> usize {
    while pos < input.len() {
        match input[pos] {
            b' ' | b'\r' | b'\n' => pos += 1,
            b'#' => {
                while pos < input.len() && input[pos] != b'\r' && input[pos] != b'\n' {
                    pos += 1;
                }
            }
            _ => break,
        }
    }
    pos
}

/// Length a UTF-8 sequence claims from its lead byte (1 for an invalid lead, which the window
/// validation will then reject).
fn utf8_char_len(lead: u8) -> usize {
    match lead {
        b if b < 0x80 => 1,
        b if b & 0xE0 == 0xC0 => 2,
        b if b & 0xF0 == 0xE0 => 3,
        b if b & 0xF8 == 0xF0 => 4,
        _ => 1,
    }
}

/// One field's conversion, `CsvToRowDataConverters` exactly: numbers and booleans trim, strings
/// don't, DATE is `java.sql.Date.valueOf`'s lenient form, TIMESTAMP is the SQL `TimeFormats`
/// pattern, DECIMAL is `BigDecimal` + HALF_UP-rescale-or-NULL. `None` is a conversion failure
/// (the host throws, or nulls the field under ignore-parse-errors).
fn convert(data_type: &DataType, text: &str) -> Option<Converted> {
    Some(match data_type {
        DataType::Boolean => Converted::Bool(parse_java_boolean(text.trim())),
        DataType::Int8 => Converted::I8(parse_java_integer(text.trim())?),
        DataType::Int16 => Converted::I16(parse_java_integer(text.trim())?),
        DataType::Int32 => Converted::I32(parse_java_integer(text.trim())?),
        DataType::Int64 => Converted::I64(parse_java_integer(text.trim())?),
        DataType::Float32 => Converted::F32(parse_java_float(text)?),
        DataType::Float64 => Converted::F64(parse_java_float(text)?),
        DataType::Utf8 => Converted::Str(text.to_string()),
        DataType::Date32 => Converted::Date(parse_java_sql_date(text)?),
        DataType::Timestamp(_, _) => {
            Converted::Timestamp(parse_flink_timestamp(text.trim(), TimestampMode::Sql)?)
        }
        DataType::Decimal128(p, s) => match parse_flink_decimal(text, *p, *s) {
            Err(()) => return None,
            // Precision overflow is a NULL, not an error (DecimalData.fromBigDecimal).
            Ok(None) => Converted::Null,
            Ok(Some(v)) => Converted::Decimal(v),
        },
        other => panic!("CSV decode does not support column type {other}"),
    })
}

fn build_column(data_type: &DataType, rows: &[Vec<Converted>], i: usize) -> ArrayRef {
    macro_rules! primitive {
        ($array:ty, $variant:ident) => {
            Arc::new(
                rows.iter()
                    .map(|r| match &r[i] {
                        Converted::$variant(v) => Some(*v),
                        _ => None,
                    })
                    .collect::<$array>(),
            ) as ArrayRef
        };
    }
    match data_type {
        DataType::Boolean => primitive!(BooleanArray, Bool),
        DataType::Int8 => primitive!(Int8Array, I8),
        DataType::Int16 => primitive!(Int16Array, I16),
        DataType::Int32 => primitive!(Int32Array, I32),
        DataType::Int64 => primitive!(Int64Array, I64),
        DataType::Float32 => primitive!(Float32Array, F32),
        DataType::Float64 => primitive!(Float64Array, F64),
        DataType::Date32 => primitive!(Date32Array, Date),
        DataType::Utf8 => Arc::new(
            rows.iter()
                .map(|r| match &r[i] {
                    Converted::Str(s) => Some(s.as_str()),
                    _ => None,
                })
                .collect::<StringArray>(),
        ) as ArrayRef,
        DataType::Timestamp(_, _) => {
            let values: TimestampNanosecondArray = rows
                .iter()
                .map(|r| match &r[i] {
                    Converted::Timestamp(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Arc::new(values.with_data_type(data_type.clone())) as ArrayRef
        }
        DataType::Decimal128(p, s) => {
            let values: Decimal128Array = rows
                .iter()
                .map(|r| match &r[i] {
                    Converted::Decimal(v) => Some(*v),
                    _ => None,
                })
                .collect();
            Arc::new(values.with_precision_and_scale(*p, *s).expect("declared decimal type"))
                as ArrayRef
        }
        other => panic!("CSV decode does not support column type {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]))
    }

    fn bodies(messages: &[Option<&str>]) -> RecordBatch {
        let column: StringArray = messages.iter().copied().collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)])),
            vec![Arc::new(column)],
        )
        .unwrap()
    }

    fn decoder(options: CsvOptions, skip: bool) -> CsvDecoder {
        CsvDecoder::new(schema(), options, skip)
    }

    #[test]
    fn decodes_one_row_per_message_first_record_only() {
        let d = decoder(CsvOptions::default(), false);
        let out = d.decode(&bodies(&[
            Some("1,alice,2.5"),
            Some("2,bob,3.5\n99,ignored,9.9"),
            None,
        ]));
        assert_eq!(out.num_rows(), 2);
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
    }

    #[test]
    fn trims_numbers_but_not_strings() {
        let d = decoder(CsvOptions::default(), false);
        let out = d.decode(&bodies(&[Some(" 7 , padded ,  1.5 ")]));
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let names = out.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let scores = out.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(ids.value(0), 7);
        assert_eq!(names.value(0), " padded ");
        assert_eq!(scores.value(0), 1.5);
    }

    #[test]
    fn empty_string_field_stays_empty_not_null() {
        let d = decoder(CsvOptions::default(), false);
        let out = d.decode(&bodies(&[Some("1,,2.5")]));
        let names = out.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(names.is_valid(0));
        assert_eq!(names.value(0), "");
    }

    #[test]
    #[should_panic(expected = "Fail to deserialize at field: id")]
    fn empty_numeric_field_fails_like_flink() {
        decoder(CsvOptions::default(), false).decode(&bodies(&[Some(",x,2.5")]));
    }

    #[test]
    #[should_panic(expected = "Row length mismatch. 3 fields expected but was 1.")]
    fn short_row_fails_by_default() {
        decoder(CsvOptions::default(), false).decode(&bodies(&[Some("1")]));
    }

    #[test]
    #[should_panic(expected = "CSV record has 4 fields")]
    fn long_row_fails_by_default() {
        decoder(CsvOptions::default(), false).decode(&bodies(&[Some("1,x,2.5,extra")]));
    }

    #[test]
    fn skip_mode_matches_flink_granularity() {
        let d = decoder(CsvOptions::default(), true);
        let out = d.decode(&bodies(&[
            Some("junk,ok,2.5"),   // field error → field null, row kept
            Some("1"),             // short row → null-padded
            Some("1,x,2.5,extra"), // long row → dropped
            Some("2,y,3.5"),
        ]));
        assert_eq!(out.num_rows(), 3);
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(ids.is_null(0));
        assert_eq!(ids.value(1), 1);
        assert!(out.column(1).as_any().downcast_ref::<StringArray>().unwrap().is_null(1));
        assert_eq!(ids.value(2), 2);
    }

    #[test]
    fn quoting_and_embedded_newlines() {
        let d = decoder(CsvOptions::default(), false);
        let out = d.decode(&bodies(&[Some("1,\"a,b\nc\"\"d\",2.5")]));
        let names = out.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "a,b\nc\"d");
    }

    #[test]
    fn custom_delimiter_quote_and_comments() {
        let options = CsvOptions {
            delimiter: b';',
            quote: Some(b'\''),
            comments: true,
            null_literal: None,
        };
        let d = decoder(options, true);
        let out = d.decode(&bodies(&[
            Some("1;'a;''b';2.5"),
            Some("#comment only"),        // comment-only message → no record → dropped (skip mode)
            Some("#lead\n2;x;3.5"), // comment line skipped, the next line is the record
        ]));
        assert_eq!(out.num_rows(), 2);
        let names = out.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "a;'b");
        assert_eq!(names.value(1), "x");
    }

    #[test]
    fn disabled_quote_character_is_literal() {
        let options = CsvOptions { quote: None, ..CsvOptions::default() };
        let out = decoder(options, false).decode(&bodies(&[Some("1,\"abc,2.5")]));
        let names = out.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "\"abc");
    }

    #[test]
    fn null_literal_applies_to_every_type_pre_trim() {
        let options = CsvOptions { null_literal: Some("N/A".to_string()), ..CsvOptions::default() };
        let out = decoder(options, false).decode(&bodies(&[Some("N/A,N/A,2.5"), Some("1, N/A ,N/A")]));
        assert!(out.column(0).is_null(0));
        assert!(out.column(1).is_null(0));
        // Pre-trim comparison: a padded " N/A " is NOT the literal — it stays a string, and for
        // numbers it would be a parse failure.
        let names = out.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(1), " N/A ");
        assert!(out.column(2).is_null(1));
    }

    #[test]
    fn temporal_and_decimal_columns_follow_flink() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("dec", DataType::Decimal128(5, 2), true),
        ]));
        let d = CsvDecoder::new(schema, CsvOptions::default(), false);
        let out = d.decode(&bodies(&[Some("2020-2-31,1970-01-02 00:00:00.5,1.235")]));
        let days = out.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(days.value(0), crate::flink_text::parse_iso_local_date("2020-03-02").unwrap());
        let ts = out.column(1).as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
        assert_eq!(ts.value(0), 86_400_000_000_000 + 500_000_000);
        let dec = out.column(2).as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec.value(0), 124); // HALF_UP, not arrow-csv's truncation

        let overflow = d.decode(&bodies(&[Some("2020-01-01,1970-01-01 00:00:00,12345.6")]));
        assert!(overflow.column(2).is_null(0)); // precision overflow → NULL, not error
    }

    #[test]
    #[should_panic(expected = "Fail to deserialize at field: ts")]
    fn iso_t_separator_is_rejected_like_flink_csv() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        )]));
        CsvDecoder::new(schema, CsvOptions::default(), false)
            .decode(&bodies(&[Some("1970-01-02T00:00:00")]));
    }

    #[test]
    #[should_panic(expected = "no record")]
    fn empty_message_fails_by_default() {
        decoder(CsvOptions::default(), false).decode(&bodies(&[Some("")]));
    }

    #[test]
    #[should_panic(expected = "Row length mismatch. 3 fields expected but was 1.")]
    fn blank_first_line_is_a_one_field_record() {
        decoder(CsvOptions::default(), false).decode(&bodies(&[Some("\n1,alice,2.5")]));
    }

    #[test]
    fn blank_first_line_pads_under_skip_errors() {
        let d = decoder(CsvOptions::default(), true);
        let out = d.decode(&bodies(&[Some("\n1,alice,2.5"), Some("\r\n"), Some("\r1,x,2.5")]));
        // Each blank line is the message's record: one empty field (a null INT under
        // skip-errors) padded out with nulls — the data line after it is never read.
        assert_eq!(out.num_rows(), 3);
        for row in 0..3 {
            assert!(out.column(0).is_null(row));
            assert!(out.column(1).is_null(row));
            assert!(out.column(2).is_null(row));
        }
    }

    #[test]
    fn comments_skip_blank_lines_and_spaces_before_the_record() {
        let options = CsvOptions { comments: true, ..CsvOptions::default() };
        let d = CsvDecoder::new(schema(), options, false);
        let out = d.decode(&bodies(&[Some("\n #c\n\n 2,x,3.5")]));
        assert_eq!(out.num_rows(), 1);
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.value(0), 2);
        let names = out.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "x");
    }

    #[test]
    #[should_panic(expected = "Row length mismatch")]
    fn comments_do_not_skip_tab_prefixed_lines() {
        let options = CsvOptions { comments: true, ..CsvOptions::default() };
        CsvDecoder::new(schema(), options, false).decode(&bodies(&[Some("\t#c\n1,x,2.5")]));
    }

    fn byte_bodies(messages: &[&[u8]]) -> RecordBatch {
        let column: arrow::array::BinaryArray = messages.iter().copied().map(Some).collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("body", DataType::Binary, true)])),
            vec![Arc::new(column)],
        )
        .unwrap()
    }

    #[test]
    #[should_panic(expected = "not valid UTF-8")]
    fn invalid_utf8_fails_the_message() {
        decoder(CsvOptions::default(), false).decode(&byte_bodies(&[b"1,a\xFFb,2.5"]));
    }

    #[test]
    fn invalid_utf8_drops_the_whole_row_under_skip_errors() {
        let d = decoder(CsvOptions::default(), true);
        let out = d.decode(&byte_bodies(&[b"1,a\xFFb,2.5", b"2,y,3.5"]));
        assert_eq!(out.num_rows(), 1);
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.value(0), 2);
    }

    #[test]
    fn utf8_is_checked_only_inside_jacksons_decode_window() {
        let d = decoder(CsvOptions::default(), false);
        // Garbage past the record terminator and one lookahead character is never decoded.
        let out = d.decode(&byte_bodies(&[b"1,x,2.5\nz\xFF", b"2,y,3.5\nzzz\xFF\xFE"]));
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    #[should_panic(expected = "not valid UTF-8")]
    fn invalid_lookahead_right_after_the_terminator_fails() {
        decoder(CsvOptions::default(), false).decode(&byte_bodies(&[b"1,x,2.5\n\xFF"]));
    }

    #[test]
    fn comment_run_after_the_record_extends_the_window() {
        let options = CsvOptions { comments: true, ..CsvOptions::default() };
        let d = CsvDecoder::new(schema(), options, false);
        let out = d.decode(&byte_bodies(&[b"1,x,2.5\n#c\nz\xFF"]));
        assert_eq!(out.num_rows(), 1);
    }

    #[test]
    #[should_panic(expected = "not valid UTF-8")]
    fn invalid_byte_in_a_trailing_comment_fails() {
        let options = CsvOptions { comments: true, ..CsvOptions::default() };
        CsvDecoder::new(schema(), options, false).decode(&byte_bodies(&[b"1,x,2.5\n#\xFF"]));
    }
}
