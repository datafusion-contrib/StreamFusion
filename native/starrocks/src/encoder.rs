//! Stream Load rows byte-identical to the released StarRocks connector's row serializers.
//!
//! The connector turns each `RowData` into an `Object[]` (`StarRocksTableRowTransformer`) and
//! then into one CSV line (`StarRocksCsvSerializer`) or one fastjson object
//! (`StarRocksJsonSerializer`); its stream-load SDK frames those rows with the format delimiter.
//! This encoder produces the same rows for an Arrow batch, back to back with their end offsets, so
//! the SDK still receives, frames, counts and buffers every row itself. Row kinds follow the
//! connector's `RowDataSerializationSchema`: update-before images are dropped unless the table
//! takes deletes and `sink.ignore.update-before` is false, deletes are dropped for tables without
//! upsert/delete support, and `__op` is 0 for an upsert and 1 for a delete.

use crate::{fastjson, java_text};
use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
};
use arrow::datatypes::DataType;
use streamfusion_bridge::changelog::{data_arity, row_kind_column};
use streamfusion_bridge::jdk_decimal;
use streamfusion_bridge::jdk_double::{jdk_double_to_string, jdk_float_to_string};
use streamfusion_bridge::timestamp::{is_timestamp, TimestampColumn};

const INSERT: i8 = 0;
const UPDATE_BEFORE: i8 = 1;
const UPDATE_AFTER: i8 = 2;
const DELETE: i8 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal,
    Text,
    Date,
    Timestamp,
    Binary,
}

impl Kind {
    /// Reads the leading type name of a Flink summary string such as `DECIMAL(10, 2) NOT NULL`.
    fn parse(descriptor: &str) -> Result<Kind, String> {
        let name = descriptor.split(['(', ' ']).next().unwrap_or(descriptor);
        Ok(match name {
            "BOOLEAN" => Kind::Boolean,
            "TINYINT" => Kind::TinyInt,
            "SMALLINT" => Kind::SmallInt,
            "INT" => Kind::Int,
            "BIGINT" => Kind::BigInt,
            "FLOAT" => Kind::Float,
            "DOUBLE" => Kind::Double,
            "DECIMAL" => Kind::Decimal,
            "CHAR" | "VARCHAR" | "STRING" => Kind::Text,
            "DATE" => Kind::Date,
            "TIMESTAMP" | "TIMESTAMP_LTZ" => Kind::Timestamp,
            "BINARY" => Kind::Binary,
            _ => {
                return Err(format!(
                    "the StarRocks encoder does not admit the Flink type {descriptor}"
                ))
            }
        })
    }
}

/// The row layout: CSV lines or JSON objects.
pub enum Format {
    Csv { column_separator: Vec<u8> },
    Json,
}

enum JsonEntry {
    Column { index: usize, key: Vec<u8> },
    Op { key: Vec<u8> },
}

/// A task-local encoding plan, parsed once from the planner's resolved sink options.
pub struct Encoder {
    format: Format,
    kinds: Vec<Kind>,
    json_entries: Vec<JsonEntry>,
    upsert_delete: bool,
    ignore_update_before: bool,
}

impl Encoder {
    /// `json_order` lists the JSON keys in the iteration order of the connector's row `HashMap`:
    /// a field index, or -1 for `__op`. It is ignored for CSV.
    pub fn new(
        format: Format,
        logical_types: &[String],
        field_names: &[String],
        json_order: &[i32],
        upsert_delete: bool,
        ignore_update_before: bool,
    ) -> Result<Encoder, String> {
        if logical_types.len() != field_names.len() {
            return Err(format!(
                "StarRocks encoder received {} types for {} fields",
                logical_types.len(),
                field_names.len()
            ));
        }
        let kinds = logical_types
            .iter()
            .map(|descriptor| Kind::parse(descriptor))
            .collect::<Result<Vec<_>, _>>()?;
        let json_entries = if matches!(format, Format::Json) {
            let expected = field_names.len() + usize::from(upsert_delete);
            if json_order.len() != expected {
                return Err(format!(
                    "StarRocks JSON key order has {} entries for {expected} keys",
                    json_order.len()
                ));
            }
            json_order
                .iter()
                .map(|&position| {
                    let name = if position < 0 {
                        "__op"
                    } else {
                        field_names.get(position as usize).ok_or_else(|| {
                            format!("StarRocks JSON key {position} is out of range")
                        })?
                    };
                    let mut key = Vec::with_capacity(name.len() + 3);
                    fastjson::write_string(name.as_bytes(), &mut key);
                    key.push(b':');
                    Ok(if position < 0 {
                        JsonEntry::Op { key }
                    } else {
                        JsonEntry::Column {
                            index: position as usize,
                            key,
                        }
                    })
                })
                .collect::<Result<Vec<_>, String>>()?
        } else {
            Vec::new()
        };
        Ok(Encoder {
            format,
            kinds,
            json_entries,
            upsert_delete,
            ignore_update_before,
        })
    }

    /// Checks that a batch carries the sink's columns in Arrow types the encoder reads.
    pub fn check(&self, batch: &RecordBatch) -> Result<(), String> {
        self.columns(batch).map(|_| ())
    }

    fn columns<'a>(&self, batch: &'a RecordBatch) -> Result<Vec<Column<'a>>, String> {
        let arity = data_arity(batch);
        if arity != self.kinds.len() {
            return Err(format!(
                "StarRocks encoder expected {} columns but the batch carries {arity}",
                self.kinds.len()
            ));
        }
        (0..arity)
            .map(|index| Column::new(batch.column(index), self.kinds[index]))
            .collect()
    }

    /// Appends the rows the connector would send, from row `start` on, to `output` back to back
    /// and pushes each row's end offset onto `ends`. Stops before the next row once `output` holds
    /// `limit` bytes or `ends` holds `max_rows` offsets, so a row larger than `limit` still arrives
    /// whole. Returns the row to resume from, which is the batch's row count once all are visited.
    pub fn encode(
        &self,
        batch: &RecordBatch,
        start: usize,
        limit: usize,
        max_rows: usize,
        output: &mut Vec<u8>,
        ends: &mut Vec<usize>,
    ) -> Result<usize, String> {
        let columns = self.columns(batch)?;
        let kinds = row_kind_column(batch);
        for row in start..batch.num_rows() {
            if output.len() >= limit || ends.len() >= max_rows {
                return Ok(row);
            }
            let kind = kinds.map_or(INSERT, |kinds| kinds.value(row));
            let op = match kind {
                INSERT | UPDATE_AFTER => b'0',
                UPDATE_BEFORE if self.upsert_delete && !self.ignore_update_before => b'1',
                DELETE if self.upsert_delete => b'1',
                UPDATE_BEFORE | DELETE => continue,
                other => return Err(format!("unknown Flink row kind {other}")),
            };
            match &self.format {
                Format::Csv { column_separator } => {
                    for (index, column) in columns.iter().enumerate() {
                        if index > 0 {
                            output.extend_from_slice(column_separator);
                        }
                        column.write_csv(row, output);
                    }
                    if self.upsert_delete {
                        output.extend_from_slice(column_separator);
                        output.push(op);
                    }
                }
                Format::Json => {
                    output.push(b'{');
                    let mut first = true;
                    for entry in &self.json_entries {
                        match entry {
                            JsonEntry::Column { index, key } => {
                                if columns[*index].is_null(row) {
                                    continue;
                                }
                                if !first {
                                    output.push(b',');
                                }
                                output.extend_from_slice(key);
                                columns[*index].write_json(row, output);
                            }
                            JsonEntry::Op { key } => {
                                if !first {
                                    output.push(b',');
                                }
                                output.extend_from_slice(key);
                                output.push(op);
                            }
                        }
                        first = false;
                    }
                    output.push(b'}');
                }
            }
            ends.push(output.len());
        }
        Ok(batch.num_rows())
    }
}

/// A borrowed, typed view of one sink column.
enum Column<'a> {
    Boolean(&'a BooleanArray),
    TinyInt(&'a Int8Array),
    SmallInt(&'a Int16Array),
    Int(&'a Int32Array),
    BigInt(&'a Int64Array),
    Float(&'a Float32Array),
    Double(&'a Float64Array),
    Decimal(&'a Decimal128Array, i64),
    Text(&'a StringArray),
    LargeText(&'a LargeStringArray),
    Date(&'a Date32Array),
    Timestamp(TimestampColumn<'a>),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    FixedBinary(&'a FixedSizeBinaryArray),
}

impl<'a> Column<'a> {
    fn new(array: &'a ArrayRef, kind: Kind) -> Result<Column<'a>, String> {
        let data_type = array.data_type();
        let column = match (kind, data_type) {
            (Kind::Boolean, DataType::Boolean) => Column::Boolean(array.as_boolean()),
            (Kind::TinyInt, DataType::Int8) => Column::TinyInt(array.as_primitive()),
            (Kind::SmallInt, DataType::Int16) => Column::SmallInt(array.as_primitive()),
            (Kind::Int, DataType::Int32) => Column::Int(array.as_primitive()),
            (Kind::BigInt, DataType::Int64) => Column::BigInt(array.as_primitive()),
            (Kind::Float, DataType::Float32) => Column::Float(array.as_primitive()),
            (Kind::Double, DataType::Float64) => Column::Double(array.as_primitive()),
            (Kind::Decimal, DataType::Decimal128(_, scale)) => {
                Column::Decimal(array.as_primitive(), i64::from(*scale))
            }
            (Kind::Text, DataType::Utf8) => Column::Text(array.as_string()),
            (Kind::Text, DataType::LargeUtf8) => Column::LargeText(array.as_string()),
            (Kind::Date, DataType::Date32) => Column::Date(array.as_primitive()),
            (Kind::Timestamp, data_type) if is_timestamp(data_type) => Column::Timestamp(
                TimestampColumn::try_new(array.as_ref()).map_err(|error| error.to_string())?,
            ),
            (Kind::Binary, DataType::Binary) => Column::Binary(array.as_binary()),
            (Kind::Binary, DataType::LargeBinary) => Column::LargeBinary(array.as_binary()),
            (Kind::Binary, DataType::FixedSizeBinary(_)) => {
                Column::FixedBinary(array.as_fixed_size_binary())
            }
            _ => {
                return Err(format!(
                    "StarRocks encoder cannot read {kind:?} from an Arrow {data_type} column"
                ))
            }
        };
        Ok(column)
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Column::Boolean(array) => array.is_null(row),
            Column::TinyInt(array) => array.is_null(row),
            Column::SmallInt(array) => array.is_null(row),
            Column::Int(array) => array.is_null(row),
            Column::BigInt(array) => array.is_null(row),
            Column::Float(array) => array.is_null(row),
            Column::Double(array) => array.is_null(row),
            Column::Decimal(array, _) => array.is_null(row),
            Column::Text(array) => array.is_null(row),
            Column::LargeText(array) => array.is_null(row),
            Column::Date(array) => array.is_null(row),
            Column::Timestamp(array) => array.is_null(row),
            Column::Binary(array) => array.is_null(row),
            Column::LargeBinary(array) => array.is_null(row),
            Column::FixedBinary(array) => array.is_null(row),
        }
    }

    fn text(&self, row: usize) -> &'a [u8] {
        match self {
            Column::Text(array) => array.value(row).as_bytes(),
            Column::LargeText(array) => array.value(row).as_bytes(),
            _ => unreachable!("only text columns carry text"),
        }
    }

    fn binary(&self, row: usize) -> &'a [u8] {
        match self {
            Column::Binary(array) => array.value(row),
            Column::LargeBinary(array) => array.value(row),
            Column::FixedBinary(array) => array.value(row),
            _ => unreachable!("only binary columns carry bytes"),
        }
    }

    /// One `StarRocksCsvSerializer` field: `\N` for NULL, otherwise `String.valueOf` of the
    /// transformer's value.
    fn write_csv(&self, row: usize, output: &mut Vec<u8>) {
        if self.is_null(row) {
            output.extend_from_slice(b"\\N");
            return;
        }
        match self {
            Column::Float(array) => jdk_float_to_string(array.value(row), output),
            Column::Double(array) => jdk_double_to_string(array.value(row), output),
            Column::Text(_) | Column::LargeText(_) => {
                java_text::write_java_utf8(self.text(row), output)
            }
            _ => self.write_scalar(row, output),
        }
    }

    /// One fastjson value for a non-NULL field.
    fn write_json(&self, row: usize, output: &mut Vec<u8>) {
        match self {
            Column::Float(array) => fastjson::write_float(array.value(row), output),
            Column::Double(array) => fastjson::write_double(array.value(row), output),
            Column::Text(_) | Column::LargeText(_) => {
                let text = self.text(row);
                if std::str::from_utf8(text).is_ok() {
                    fastjson::write_string(text, output);
                } else {
                    let mut decoded = Vec::with_capacity(text.len() + 8);
                    java_text::write_java_utf8(text, &mut decoded);
                    fastjson::write_string(&decoded, output);
                }
            }
            Column::Date(_) | Column::Timestamp(_) => {
                output.push(b'"');
                self.write_scalar(row, output);
                output.push(b'"');
            }
            _ => self.write_scalar(row, output),
        }
    }

    /// Spellings shared by both formats: integers, the BOOLEAN 1/0 and BINARY long the
    /// transformer produces, `BigDecimal.toString()`, and the DATE/TIMESTAMP strings.
    fn write_scalar(&self, row: usize, output: &mut Vec<u8>) {
        let mut digits = itoa::Buffer::new();
        let spelled: &str = match self {
            Column::Boolean(array) => {
                if array.value(row) {
                    "1"
                } else {
                    "0"
                }
            }
            Column::TinyInt(array) => digits.format(array.value(row)),
            Column::SmallInt(array) => digits.format(array.value(row)),
            Column::Int(array) => digits.format(array.value(row)),
            Column::BigInt(array) => digits.format(array.value(row)),
            Column::Binary(_) | Column::LargeBinary(_) | Column::FixedBinary(_) => {
                digits.format(java_text::binary_long(self.binary(row)))
            }
            Column::Decimal(array, scale) => {
                jdk_decimal::write_big_decimal_string(array.value(row), *scale, output);
                return;
            }
            Column::Date(array) => {
                java_text::write_sql_date(array.value(row), output);
                return;
            }
            Column::Timestamp(array) => {
                let value = array
                    .value(row)
                    .expect("timestamp seconds exceed Flink's millisecond range");
                java_text::write_local_date_time(value, output);
                return;
            }
            Column::Float(_) | Column::Double(_) | Column::Text(_) | Column::LargeText(_) => {
                unreachable!("format-specific spellings are handled by the caller")
            }
        };
        output.extend_from_slice(spelled.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int8Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch(columns: Vec<(&str, ArrayRef)>, kinds: Option<Vec<i8>>) -> RecordBatch {
        let mut fields = columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let mut arrays = columns
            .into_iter()
            .map(|(_, array)| array)
            .collect::<Vec<_>>();
        if let Some(kinds) = kinds {
            fields.push(Field::new("$row_kind$", DataType::Int8, false));
            arrays.push(Arc::new(Int8Array::from(kinds)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    /// Encodes a whole batch and splits the output at the reported row ends.
    fn rows(encoder: &Encoder, batch: &RecordBatch) -> Vec<String> {
        let (mut output, mut ends) = (Vec::new(), Vec::new());
        let resume = encoder
            .encode(batch, 0, usize::MAX, usize::MAX, &mut output, &mut ends)
            .unwrap();
        assert_eq!(resume, batch.num_rows());
        let mut start = 0;
        ends.iter()
            .map(|&end| {
                let row = String::from_utf8(output[start..end].to_vec()).unwrap();
                start = end;
                row
            })
            .collect()
    }

    fn csv() -> Format {
        Format::Csv {
            column_separator: b"\t".to_vec(),
        }
    }

    #[test]
    fn csv_rows_use_backslash_n_nulls_and_raw_values() {
        let encoder = Encoder::new(
            csv(),
            &["BIGINT".into(), "STRING".into(), "BOOLEAN".into()],
            &["id".into(), "name".into(), "flag".into()],
            &[],
            false,
            true,
        )
        .unwrap();
        let input = batch(
            vec![
                ("id", Arc::new(Int64Array::from(vec![Some(1), None]))),
                (
                    "name",
                    Arc::new(StringArray::from(vec![Some("a\tb"), Some("")])),
                ),
                ("flag", Arc::new(BooleanArray::from(vec![Some(true), None]))),
            ],
            None,
        );
        assert_eq!(rows(&encoder, &input), ["1\ta\tb\t1", "\\N\t\t\\N"]);
    }

    #[test]
    fn upsert_tables_append_op_and_follow_update_before_policy() {
        let types = ["INT".to_string()];
        let names = ["k".to_string()];
        let input = batch(
            vec![("k", Arc::new(Int32Array::from(vec![1, 2, 3, 4])))],
            Some(vec![INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE]),
        );
        let ignoring = Encoder::new(csv(), &types, &names, &[], true, true).unwrap();
        assert_eq!(rows(&ignoring, &input), ["1\t0", "3\t0", "4\t1"]);
        let keeping = Encoder::new(csv(), &types, &names, &[], true, false).unwrap();
        assert_eq!(rows(&keeping, &input), ["1\t0", "2\t1", "3\t0", "4\t1"]);
        let append = Encoder::new(csv(), &types, &names, &[], false, false).unwrap();
        assert_eq!(rows(&append, &input), ["1", "3"]);
    }

    #[test]
    fn slabs_stop_at_byte_and_row_limits_and_resume_without_losing_rows() {
        let encoder =
            Encoder::new(csv(), &["STRING".into()], &["v".into()], &[], true, true).unwrap();
        let input = batch(
            vec![(
                "v",
                Arc::new(StringArray::from(vec!["aaaa", "b", "cc", "ddd"])),
            )],
            Some(vec![INSERT, UPDATE_BEFORE, INSERT, INSERT]),
        );
        let (mut output, mut ends) = (Vec::new(), Vec::new());

        let resume = encoder
            .encode(&input, 0, 1, 10, &mut output, &mut ends)
            .unwrap();
        assert_eq!(
            (resume, output.as_slice(), ends.as_slice()),
            (1, &b"aaaa\t0"[..], &[6][..])
        );

        output.clear();
        ends.clear();
        let resume = encoder
            .encode(&input, resume, 100, 1, &mut output, &mut ends)
            .unwrap();
        assert_eq!(
            (resume, output.as_slice(), ends.as_slice()),
            (3, &b"cc\t0"[..], &[4][..])
        );

        output.clear();
        ends.clear();
        let resume = encoder
            .encode(&input, resume, 100, 10, &mut output, &mut ends)
            .unwrap();
        assert_eq!(
            (resume, output.as_slice(), ends.as_slice()),
            (4, &b"ddd\t0"[..], &[5][..])
        );
    }

    #[test]
    fn json_rows_follow_hash_map_order_and_omit_nulls() {
        let encoder = Encoder::new(
            Format::Json,
            &["INT".into(), "STRING".into(), "DOUBLE".into()],
            &["a".into(), "b".into(), "c".into()],
            &[2, -1, 0, 1],
            true,
            true,
        )
        .unwrap();
        let input = batch(
            vec![
                ("a", Arc::new(Int32Array::from(vec![Some(1), None]))),
                ("b", Arc::new(StringArray::from(vec![Some("x\"y"), None]))),
                (
                    "c",
                    Arc::new(Float64Array::from(vec![Some(f64::NAN), Some(1e7)])),
                ),
            ],
            Some(vec![INSERT, DELETE]),
        );
        assert_eq!(
            rows(&encoder, &input),
            [
                r#"{"c":null,"__op":0,"a":1,"b":"x\"y"}"#,
                r#"{"c":1.0E7,"__op":1}"#
            ]
        );
    }

    #[test]
    fn decimals_dates_timestamps_and_binary_use_java_spellings() {
        let encoder = Encoder::new(
            csv(),
            &[
                "DECIMAL(10, 7)".into(),
                "DATE".into(),
                "TIMESTAMP(6)".into(),
                "BINARY(2)".into(),
            ],
            &["d".into(), "day".into(), "ts".into(), "bin".into()],
            &[],
            false,
            true,
        )
        .unwrap();
        let input = batch(
            vec![
                (
                    "d",
                    Arc::new(
                        Decimal128Array::from(vec![0, 1_234_567])
                            .with_precision_and_scale(10, 7)
                            .unwrap(),
                    ),
                ),
                ("day", Arc::new(Date32Array::from(vec![0, -719_528]))),
                (
                    "ts",
                    Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                        0, 1_500_001,
                    ])),
                ),
                (
                    "bin",
                    Arc::new(FixedSizeBinaryArray::from(vec![
                        &[1u8, 2][..],
                        &[0xff, 0xff],
                    ])),
                ),
            ],
            None,
        );
        assert_eq!(
            rows(&encoder, &input),
            [
                "0E-7\t1970-01-01\t1970-01-01T00:00\t258",
                "0.1234567\t0001-01-01\t1970-01-01T00:00:01.500001\t65535"
            ]
        );
    }

    #[test]
    fn schema_mismatches_and_unknown_types_fail_clearly() {
        assert!(Encoder::new(csv(), &["TIME(0)".into()], &["t".into()], &[], false, true).is_err());
        let encoder =
            Encoder::new(csv(), &["INT".into()], &["i".into()], &[], false, true).unwrap();
        let input = batch(vec![("i", Arc::new(Int64Array::from(vec![1])))], None);
        assert!(encoder.check(&input).is_err());
        assert!(encoder
            .encode(
                &input,
                0,
                usize::MAX,
                usize::MAX,
                &mut Vec::new(),
                &mut Vec::new()
            )
            .is_err());
        assert!(Encoder::new(
            Format::Json,
            &["INT".into()],
            &["i".into()],
            &[0, -1],
            false,
            true
        )
        .is_err());
    }
}
