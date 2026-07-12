use crate::*;
use arrow::array::{
    BinaryArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, MapArray, StringArray, StructArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::TimeUnit;

const DEFAULT_SEED: u32 = 42;
const MAX_INLINE_BYTES: usize = 7;

/// The logical shape of one key field. Arrow supplies the physical type tree; the parallel
/// precision stream supplies the one detail Arrow loses: Flink timestamp precision.
struct KeyTypeSchema {
    timestamp_precision: i32,
    children: Vec<KeyTypeSchema>,
}

fn key_type_schema(data_type: &DataType, precisions: &[i32], cursor: &mut usize) -> KeyTypeSchema {
    let timestamp_precision = *precisions
        .get(*cursor)
        .expect("missing Flink key type descriptor");
    *cursor += 1;
    let children = match data_type {
        DataType::List(field) | DataType::LargeList(field) => {
            vec![key_type_schema(field.data_type(), precisions, cursor)]
        }
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| key_type_schema(field.data_type(), precisions, cursor))
            .collect(),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) => fields
                .iter()
                .map(|field| key_type_schema(field.data_type(), precisions, cursor))
                .collect(),
            other => panic!("Arrow map entries must be a struct, got {other:?}"),
        },
        _ => Vec::new(),
    };
    KeyTypeSchema {
        timestamp_precision,
        children,
    }
}

/// The byte layout written by Flink's `BinaryRowWriter`, for the part of a row that affects
/// `BinaryRowData.hashCode()`. The format is intentionally local: this module is the native port of
/// the writer/hash pair, not a second general-purpose RowData representation.
struct BinaryRowWriter {
    bytes: Vec<u8>,
    null_bits_size: usize,
    fixed_size: usize,
    cursor: usize,
}

impl BinaryRowWriter {
    fn new(arity: usize) -> Self {
        let null_bits_size = ((arity + 63 + 8) / 64) * 8;
        let fixed_size = null_bits_size + 8 * arity;
        Self {
            bytes: vec![0; fixed_size],
            null_bits_size,
            fixed_size,
            cursor: fixed_size,
        }
    }

    /// Rewinds for the next row, keeping the allocation. The fixed section must be re-zeroed:
    /// null bits are OR-ed in and short values assume zeroed slot padding.
    fn reset(&mut self) {
        self.bytes.truncate(self.fixed_size);
        self.bytes.fill(0);
        self.cursor = self.fixed_size;
    }

    /// The finished row without consuming the writer; every write keeps `bytes` exactly
    /// `cursor` long, so the whole buffer is the row.
    fn row_bytes(&self) -> &[u8] {
        debug_assert_eq!(self.bytes.len(), self.cursor);
        &self.bytes
    }

    fn field_offset(&self, pos: usize) -> usize {
        self.null_bits_size + 8 * pos
    }

    fn set_null(&mut self, pos: usize) {
        let bit = pos + 8; // BinaryRowData.HEADER_SIZE_IN_BITS
        self.bytes[bit / 8] |= 1 << (bit % 8);
    }

    fn write_fixed(&mut self, pos: usize, value: &[u8]) {
        let offset = self.field_offset(pos);
        self.bytes[offset..offset + value.len()].copy_from_slice(value);
    }

    fn write_i32(&mut self, pos: usize, value: i32) {
        self.write_fixed(pos, &value.to_ne_bytes());
    }

    fn write_i64(&mut self, pos: usize, value: i64) {
        self.write_fixed(pos, &value.to_ne_bytes());
    }

    fn write_bytes(&mut self, pos: usize, value: &[u8]) {
        if value.len() <= MAX_INLINE_BYTES {
            let offset = self.field_offset(pos);
            self.bytes[offset..offset + 8].fill(0);
            if cfg!(target_endian = "little") {
                self.bytes[offset..offset + value.len()].copy_from_slice(value);
                self.bytes[offset + 7] = (value.len() as u8) | 0x80;
            } else {
                self.bytes[offset + 1..offset + 1 + value.len()].copy_from_slice(value);
                self.bytes[offset] = (value.len() as u8) | 0x80;
            }
            return;
        }
        self.write_variable(pos, value, round_to_word(value.len()));
    }

    fn write_variable(&mut self, pos: usize, value: &[u8], reserved: usize) {
        let offset = self.cursor;
        self.bytes.resize(offset + reserved, 0);
        self.bytes[offset..offset + value.len()].copy_from_slice(value);
        let offset_and_size = ((offset as u64) << 32) | value.len() as u64;
        self.write_i64(pos, offset_and_size as i64);
        self.cursor += reserved;
    }

    fn write_decimal(&mut self, pos: usize, value: i128, precision: u8) {
        if precision <= 18 {
            self.write_i64(pos, value as i64);
            return;
        }
        let bytes = unscaled_decimal_bytes(value);
        // Flink reserves a fixed 16-byte variable section for non-compact decimals.
        self.write_variable(pos, &bytes, 16);
    }

    fn write_timestamp(&mut self, pos: usize, nanos: i64, precision: i32) {
        let millis = nanos.div_euclid(1_000_000);
        if precision <= 3 {
            self.write_i64(pos, millis);
            return;
        }
        let nanos_of_milli = nanos.rem_euclid(1_000_000);
        let offset = self.cursor;
        self.bytes.resize(offset + 8, 0);
        self.bytes[offset..offset + 8].copy_from_slice(&millis.to_ne_bytes());
        let offset_and_nanos = ((offset as u64) << 32) | nanos_of_milli as u64;
        self.write_i64(pos, offset_and_nanos as i64);
        self.cursor += 8;
    }

    fn finish(mut self) -> Vec<u8> {
        self.bytes.truncate(self.cursor);
        self.bytes
    }
}

/// Flink's `BinaryArrayData` layout: size, null bits aligned to four bytes, fixed-width values,
/// then a word-aligned variable section. Composite values are carried in the variable section.
struct BinaryArrayWriter {
    bytes: Vec<u8>,
    element_offset: usize,
    element_size: usize,
    cursor: usize,
}

impl BinaryArrayWriter {
    fn new(len: usize, element_size: usize) -> Self {
        let header = 4 + ((len + 31) / 32) * 4;
        let fixed_size = round_to_word(header + len * element_size);
        let mut bytes = vec![0; fixed_size];
        bytes[..4].copy_from_slice(&(len as i32).to_ne_bytes());
        Self {
            bytes,
            element_offset: header,
            element_size,
            cursor: fixed_size,
        }
    }

    fn element_offset(&self, pos: usize) -> usize {
        self.element_offset + pos * self.element_size
    }

    fn set_null(&mut self, pos: usize) {
        let bit = pos;
        let byte = 4 + bit / 8;
        self.bytes[byte] |= 1 << (bit % 8);
    }

    fn write_fixed(&mut self, pos: usize, value: &[u8]) {
        let offset = self.element_offset(pos);
        self.bytes[offset..offset + value.len()].copy_from_slice(value);
    }

    fn write_bytes(&mut self, pos: usize, value: &[u8]) {
        if value.len() <= MAX_INLINE_BYTES {
            let offset = self.element_offset(pos);
            self.bytes[offset..offset + 8].fill(0);
            if cfg!(target_endian = "little") {
                self.bytes[offset..offset + value.len()].copy_from_slice(value);
                self.bytes[offset + 7] = (value.len() as u8) | 0x80;
            } else {
                self.bytes[offset + 1..offset + 1 + value.len()].copy_from_slice(value);
                self.bytes[offset] = (value.len() as u8) | 0x80;
            }
            return;
        }
        self.write_variable(pos, value, round_to_word(value.len()));
    }

    fn write_variable(&mut self, pos: usize, value: &[u8], reserved: usize) {
        let offset = self.cursor;
        self.bytes.resize(offset + reserved, 0);
        self.bytes[offset..offset + value.len()].copy_from_slice(value);
        let offset_and_size = ((offset as u64) << 32) | value.len() as u64;
        self.write_fixed(pos, &(offset_and_size as i64).to_ne_bytes());
        self.cursor += reserved;
    }

    fn finish(mut self) -> Vec<u8> {
        self.bytes.truncate(self.cursor);
        self.bytes
    }
}

fn round_to_word(size: usize) -> usize {
    (size + 7) & !7
}

fn unscaled_decimal_bytes(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let sign = if value < 0 { 0xff } else { 0 };
    let mut first = 0;
    while first < bytes.len() - 1
        && bytes[first] == sign
        && (bytes[first + 1] & 0x80 == 0) == (sign == 0)
    {
        first += 1;
    }
    bytes[first..].to_vec()
}

fn timestamp_nanos(array: &ArrayRef, row: usize) -> i64 {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .expect("timestamp second")
            .value(row)
            .checked_mul(1_000_000_000)
            .expect("timestamp nanoseconds overflow"),
        DataType::Timestamp(TimeUnit::Millisecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp millisecond")
            .value(row)
            .checked_mul(1_000_000)
            .expect("timestamp nanoseconds overflow"),
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp microsecond")
            .value(row)
            .checked_mul(1_000)
            .expect("timestamp nanoseconds overflow"),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("timestamp nanosecond")
            .value(row),
        other => panic!("not a timestamp: {other:?}"),
    }
}

fn time_millis(array: &ArrayRef, row: usize) -> i32 {
    match array.data_type() {
        DataType::Time32(TimeUnit::Second) => {
            array
                .as_any()
                .downcast_ref::<Time32SecondArray>()
                .expect("time second")
                .value(row)
                * 1_000
        }
        DataType::Time32(TimeUnit::Millisecond) => array
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .expect("time millisecond")
            .value(row),
        DataType::Time64(TimeUnit::Microsecond) => {
            (array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .expect("time microsecond")
                .value(row)
                / 1_000) as i32
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            (array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .expect("time nanosecond")
                .value(row)
                / 1_000_000) as i32
        }
        other => panic!("not a time: {other:?}"),
    }
}

fn array_element_size(data_type: &DataType) -> usize {
    match data_type {
        DataType::Boolean | DataType::Int8 => 1,
        DataType::Int16 => 2,
        DataType::Int32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Float32 => 4,
        _ => 8,
    }
}

fn encode_array(array: &ArrayRef, schema: &KeyTypeSchema) -> Vec<u8> {
    let mut writer = BinaryArrayWriter::new(array.len(), array_element_size(array.data_type()));
    for row in 0..array.len() {
        write_array_value(&mut writer, row, array, row, schema);
    }
    writer.finish()
}

fn encode_struct(array: &StructArray, row: usize, schema: &KeyTypeSchema) -> Vec<u8> {
    assert_eq!(array.num_columns(), schema.children.len());
    let mut writer = BinaryRowWriter::new(array.num_columns());
    for (pos, child_schema) in schema.children.iter().enumerate() {
        write_value(&mut writer, pos, array.column(pos), row, child_schema);
    }
    writer.finish()
}

fn encode_map(array: &MapArray, row: usize, schema: &KeyTypeSchema) -> Vec<u8> {
    assert_eq!(schema.children.len(), 2);
    let entries = array.value(row);
    let keys = encode_array(entries.column(0), &schema.children[0]);
    let values = encode_array(entries.column(1), &schema.children[1]);
    let mut bytes = Vec::with_capacity(4 + keys.len() + values.len());
    bytes.extend_from_slice(&(keys.len() as i32).to_ne_bytes());
    bytes.extend_from_slice(&keys);
    bytes.extend_from_slice(&values);
    bytes
}

fn write_array_value(
    writer: &mut BinaryArrayWriter,
    pos: usize,
    array: &ArrayRef,
    row: usize,
    schema: &KeyTypeSchema,
) {
    if array.is_null(row) {
        writer.set_null(pos);
        return;
    }
    match array.data_type() {
        DataType::Boolean => writer.write_fixed(
            pos,
            &[array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean")
                .value(row) as u8],
        ),
        DataType::Int8 => writer.write_fixed(
            pos,
            &[array
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("int8")
                .value(row) as u8],
        ),
        DataType::Int16 => writer.write_fixed(
            pos,
            &array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("int16")
                .value(row)
                .to_ne_bytes(),
        ),
        DataType::Int32 | DataType::Date32 => {
            let value = if matches!(array.data_type(), DataType::Date32) {
                array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .expect("date32")
                    .value(row)
            } else {
                array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("int32")
                    .value(row)
            };
            writer.write_fixed(pos, &value.to_ne_bytes());
        }
        DataType::Int64 => writer.write_fixed(
            pos,
            &array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64")
                .value(row)
                .to_ne_bytes(),
        ),
        DataType::Float32 => writer.write_fixed(
            pos,
            &array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("float32")
                .value(row)
                .to_ne_bytes(),
        ),
        DataType::Float64 => writer.write_fixed(
            pos,
            &array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("float64")
                .value(row)
                .to_ne_bytes(),
        ),
        DataType::Time32(_) | DataType::Time64(_) => {
            writer.write_fixed(pos, &time_millis(array, row).to_ne_bytes())
        }
        DataType::Utf8 => writer.write_bytes(
            pos,
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8")
                .value(row)
                .as_bytes(),
        ),
        DataType::LargeUtf8 => writer.write_bytes(
            pos,
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("large utf8")
                .value(row)
                .as_bytes(),
        ),
        DataType::Binary => {
            let value = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("binary")
                .value(row);
            writer.write_bytes(pos, value);
        }
        DataType::LargeBinary => {
            let value = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("large binary")
                .value(row);
            writer.write_bytes(pos, value);
        }
        DataType::FixedSizeBinary(_) => {
            let value = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("fixed binary")
                .value(row);
            writer.write_bytes(pos, value);
        }
        DataType::Decimal128(precision, _) => {
            let value = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("decimal128")
                .value(row);
            if *precision <= 18 {
                writer.write_fixed(pos, &(value as i64).to_ne_bytes());
            } else {
                writer.write_variable(pos, &unscaled_decimal_bytes(value), 16);
            }
        }
        DataType::Timestamp(_, _) => {
            let nanos = timestamp_nanos(array, row);
            let millis = nanos.div_euclid(1_000_000);
            if schema.timestamp_precision <= 3 {
                writer.write_fixed(pos, &millis.to_ne_bytes());
            } else {
                let nanos_of_milli = nanos.rem_euclid(1_000_000);
                let offset = writer.cursor;
                writer.bytes.resize(offset + 8, 0);
                writer.bytes[offset..offset + 8].copy_from_slice(&millis.to_ne_bytes());
                let offset_and_nanos = ((offset as u64) << 32) | nanos_of_milli as u64;
                writer.write_fixed(pos, &(offset_and_nanos as i64).to_ne_bytes());
                writer.cursor += 8;
            }
        }
        DataType::List(_) => {
            let value = array
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("list")
                .value(row);
            let bytes = encode_array(&value, &schema.children[0]);
            writer.write_variable(pos, &bytes, round_to_word(bytes.len()));
        }
        DataType::LargeList(_) => {
            let value = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .expect("large list")
                .value(row);
            let bytes = encode_array(&value, &schema.children[0]);
            writer.write_variable(pos, &bytes, round_to_word(bytes.len()));
        }
        DataType::Map(_, _) => {
            let bytes = encode_map(
                array.as_any().downcast_ref::<MapArray>().expect("map"),
                row,
                schema,
            );
            writer.write_variable(pos, &bytes, round_to_word(bytes.len()));
        }
        DataType::Struct(_) => {
            let bytes = encode_struct(
                array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("struct"),
                row,
                schema,
            );
            writer.write_variable(pos, &bytes, round_to_word(bytes.len()));
        }
        DataType::Null => writer.set_null(pos),
        other => panic!("Flink BinaryRow array encoding is not implemented for {other:?}"),
    }
}

fn write_value(
    writer: &mut BinaryRowWriter,
    pos: usize,
    array: &ArrayRef,
    row: usize,
    schema: &KeyTypeSchema,
) {
    if array.is_null(row) {
        writer.set_null(pos);
        return;
    }
    match array.data_type() {
        DataType::Boolean => writer.write_fixed(
            pos,
            &[array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean")
                .value(row) as u8],
        ),
        DataType::Int8 => writer.write_fixed(
            pos,
            &[array
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("int8")
                .value(row) as u8],
        ),
        DataType::Int16 => writer.write_fixed(
            pos,
            &array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("int16")
                .value(row)
                .to_ne_bytes(),
        ),
        DataType::Int32 | DataType::Date32 => {
            let value = if matches!(array.data_type(), DataType::Date32) {
                array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .expect("date32")
                    .value(row)
            } else {
                array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("int32")
                    .value(row)
            };
            writer.write_i32(pos, value);
        }
        DataType::Int64 => writer.write_i64(
            pos,
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64")
                .value(row),
        ),
        DataType::Float32 => writer.write_fixed(
            pos,
            &array
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("float32")
                .value(row)
                .to_ne_bytes(),
        ),
        DataType::Float64 => writer.write_fixed(
            pos,
            &array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("float64")
                .value(row)
                .to_ne_bytes(),
        ),
        DataType::Utf8 => writer.write_bytes(
            pos,
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8")
                .value(row)
                .as_bytes(),
        ),
        DataType::LargeUtf8 => writer.write_bytes(
            pos,
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("large utf8")
                .value(row)
                .as_bytes(),
        ),
        DataType::Binary => writer.write_bytes(
            pos,
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("binary")
                .value(row),
        ),
        DataType::LargeBinary => writer.write_bytes(
            pos,
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .expect("large binary")
                .value(row),
        ),
        DataType::FixedSizeBinary(_) => writer.write_bytes(
            pos,
            array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("fixed binary")
                .value(row),
        ),
        DataType::Decimal128(precision, _) => writer.write_decimal(
            pos,
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("decimal128")
                .value(row),
            *precision,
        ),
        DataType::Time32(_) | DataType::Time64(_) => writer.write_i32(pos, time_millis(array, row)),
        DataType::Timestamp(_, _) => {
            writer.write_timestamp(pos, timestamp_nanos(array, row), schema.timestamp_precision)
        }
        DataType::List(_) => {
            let value = array
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("list")
                .value(row);
            let bytes = encode_array(&value, &schema.children[0]);
            writer.write_variable(pos, &bytes, round_to_word(bytes.len()));
        }
        DataType::LargeList(_) => {
            let value = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .expect("large list")
                .value(row);
            let bytes = encode_array(&value, &schema.children[0]);
            writer.write_variable(pos, &bytes, round_to_word(bytes.len()));
        }
        DataType::Map(_, _) => {
            let bytes = encode_map(
                array.as_any().downcast_ref::<MapArray>().expect("map"),
                row,
                schema,
            );
            writer.write_variable(pos, &bytes, round_to_word(bytes.len()));
        }
        DataType::Struct(_) => {
            let bytes = encode_struct(
                array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("struct"),
                row,
                schema,
            );
            writer.write_variable(pos, &bytes, round_to_word(bytes.len()));
        }
        DataType::Null => writer.set_null(pos),
        other => panic!("Flink BinaryRow key encoding is not implemented for {other:?}"),
    }
}

/// Encodes one batch's rows, projected to the given key columns, as Flink `BinaryRowData` bytes.
/// `timestamp_precisions` is a pre-order logical-type stream (one entry per type node); `-1` means
/// the node is not a timestamp. Arrow does not retain Flink timestamp precision, including inside a
/// nested key, so the JVM supplies this compact schema sidecar.
///
/// The schema walk, column handles, and row buffer are set up once per batch and every row is
/// written into the same reused buffer: the per-row variant of this path was almost entirely
/// allocator traffic (a q17 profile put it at ~12% of the whole job before batching).
pub(crate) struct BinaryRowBatchEncoder<'a> {
    columns: Vec<&'a ArrayRef>,
    schemas: Vec<KeyTypeSchema>,
    writer: BinaryRowWriter,
}

impl<'a> BinaryRowBatchEncoder<'a> {
    pub(crate) fn new(
        batch: &'a RecordBatch,
        key_columns: &[usize],
        timestamp_precisions: &[i32],
    ) -> Self {
        let mut cursor = 0;
        let schema = batch.schema_ref();
        let schemas: Vec<KeyTypeSchema> = key_columns
            .iter()
            .map(|&column| {
                key_type_schema(
                    schema.field(column).data_type(),
                    timestamp_precisions,
                    &mut cursor,
                )
            })
            .collect();
        assert_eq!(
            cursor,
            timestamp_precisions.len(),
            "extra Flink key type descriptors"
        );
        Self {
            columns: key_columns.iter().map(|&column| batch.column(column)).collect(),
            schemas,
            writer: BinaryRowWriter::new(key_columns.len()),
        }
    }

    /// The encoded key of one row, valid until the next call.
    pub(crate) fn encode(&mut self, row: usize) -> &[u8] {
        self.writer.reset();
        for (pos, (column, schema)) in self.columns.iter().zip(&self.schemas).enumerate() {
            write_value(&mut self.writer, pos, column, row, schema);
        }
        self.writer.row_bytes()
    }

    /// `BinaryRowData.hashCode()` of one row's key.
    pub(crate) fn hash(&mut self, row: usize) -> i32 {
        hash_bytes_by_words(self.encode(row))
    }
}

/// One-row convenience over the batch encoder, for the checkpoint-time key-group routing paths
/// where per-row setup is not hot. Per-row processing paths should hold an encoder instead.
pub(crate) fn binary_row_hash(
    batch: &RecordBatch,
    key_columns: &[usize],
    row: usize,
    timestamp_precisions: &[i32],
) -> i32 {
    BinaryRowBatchEncoder::new(batch, key_columns, timestamp_precisions).hash(row)
}

/// Flink's `MurmurHashUtils.hashBytesByWords`: BinaryRow byte lengths are always word-aligned.
pub(crate) fn hash_bytes_by_words(bytes: &[u8]) -> i32 {
    assert_eq!(bytes.len() % 4, 0, "BinaryRow bytes must be word aligned");
    let mut hash = DEFAULT_SEED;
    for word in bytes.chunks_exact(4) {
        let mut key = u32::from_ne_bytes(word.try_into().expect("word"));
        key = key
            .wrapping_mul(0xcc9e2d51)
            .rotate_left(15)
            .wrapping_mul(0x1b873593);
        hash ^= key;
        hash = hash
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe6546b64);
    }
    hash ^= bytes.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    (hash ^ (hash >> 16)) as i32
}

/// Flink's `MathUtils.murmurHash(int)`, including its non-negative result normalization. This is
/// deliberately separate from the BinaryRow byte hash above: Flink hashes the serialized key first,
/// then mixes that resulting `hashCode()` to choose the key group.
pub(crate) fn flink_murmur_hash(code: i32) -> i32 {
    let mut mixed = (code as u32)
        .wrapping_mul(0xcc9e2d51)
        .rotate_left(15)
        .wrapping_mul(0x1b873593);
    mixed = mixed
        .rotate_left(13)
        .wrapping_mul(5)
        .wrapping_add(0xe6546b64);
    mixed ^= 4;
    mixed ^= mixed >> 16;
    mixed = mixed.wrapping_mul(0x85ebca6b);
    mixed ^= mixed >> 13;
    mixed = mixed.wrapping_mul(0xc2b2ae35);
    mixed ^= mixed >> 16;
    let mixed = mixed as i32;
    if mixed >= 0 {
        mixed
    } else if mixed == i32::MIN {
        0
    } else {
        -mixed
    }
}

/// Flink's `KeyGroupRangeAssignment.computeKeyGroupForKeyHash` for a BinaryRow key.
pub(crate) fn flink_key_group(binary_row_hash: i32, max_parallelism: usize) -> usize {
    assert!(max_parallelism > 0, "max parallelism must be positive");
    flink_murmur_hash(binary_row_hash) as usize % max_parallelism
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flinkBinaryRowHashes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    key_columns: JIntArray<'local>,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jintArray {
    let batch = import_record_batch(in_array_address, in_schema_address);
    let columns: Vec<usize> = read_int_array(&env, &key_columns)
        .into_iter()
        .map(|index| index as usize)
        .collect();
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let mut encoder = BinaryRowBatchEncoder::new(&batch, &columns, &precisions);
    let hashes: Vec<jint> = (0..batch.num_rows()).map(|row| encoder.hash(row)).collect();
    let output = env
        .new_int_array(hashes.len() as i32)
        .expect("allocate BinaryRow hash result");
    env.set_int_array_region(&output, 0, &hashes)
        .expect("write BinaryRow hash result");
    output.into_raw()
}
