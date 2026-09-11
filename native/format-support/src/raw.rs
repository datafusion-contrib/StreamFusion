use crate::*;

/// Decodes Flink's `raw` format: each message body is the single column's value verbatim. Strings
/// and bytes pass through (the plan gate admits only UTF-8 `raw.charset` values, so string bodies are
/// already in the column's encoding); BOOLEAN reads one byte as `!= 0`; the fixed-width numerics read
/// an exact-length buffer with the table's `raw.endianness`, failing the job on a wrong-length
/// message with Flink's own error text. 1:1 with the input rows — a null body stays a null field.
pub struct RawDecoder {
    schema: SchemaRef,
    little_endian: bool,
}

impl RawDecoder {
    pub fn new(schema: SchemaRef, little_endian: bool) -> RawDecoder {
        RawDecoder {
            schema,
            little_endian,
        }
    }

    pub fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        use arrow::datatypes::{
            Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        };
        let body = bodies.column(0);
        let column = match self.schema.field(0).data_type() {
            DataType::Boolean => self.decode_booleans(body),
            DataType::Int8 => {
                self.fixed::<Int8Type, 1>(body, "TINYINT", i8::from_be_bytes, i8::from_le_bytes)
            }
            DataType::Int16 => {
                self.fixed::<Int16Type, 2>(body, "SMALLINT", i16::from_be_bytes, i16::from_le_bytes)
            }
            DataType::Int32 => {
                self.fixed::<Int32Type, 4>(body, "INT", i32::from_be_bytes, i32::from_le_bytes)
            }
            DataType::Int64 => {
                self.fixed::<Int64Type, 8>(body, "BIGINT", i64::from_be_bytes, i64::from_le_bytes)
            }
            DataType::Float32 => {
                self.fixed::<Float32Type, 4>(body, "FLOAT", f32::from_be_bytes, f32::from_le_bytes)
            }
            DataType::Float64 => {
                self.fixed::<Float64Type, 8>(body, "DOUBLE", f64::from_be_bytes, f64::from_le_bytes)
            }
            // Strings must be validated: Flink passes the bytes through unvalidated
            // (StringData.fromBytes) but Arrow strings cannot hold invalid UTF-8, so the recorded
            // divergence (docs/coverage-and-fallbacks.md) is a loud decode failure — never the
            // silent NULL a safe cast would produce.
            target @ DataType::Utf8 => {
                let strict = arrow::compute::CastOptions {
                    safe: false,
                    ..arrow::compute::CastOptions::default()
                };
                arrow::compute::cast_with_options(body, target, &strict).unwrap_or_else(|e| {
                    panic!("raw format STRING message is not valid UTF-8 ({e})")
                })
            }
            target => arrow::compute::cast(body, target).expect("failed to cast raw column"),
        };
        RecordBatch::try_new(self.schema.clone(), vec![column]).expect("failed to build raw batch")
    }

    fn decode_booleans(&self, body: &ArrayRef) -> ArrayRef {
        let mut builder = arrow::array::BooleanBuilder::with_capacity(body.len());
        for row in 0..body.len() {
            match binary_body(body, row) {
                None => builder.append_null(),
                Some(bytes) => builder.append_value(exact::<1>(bytes, "BOOLEAN")[0] != 0),
            }
        }
        Arc::new(builder.finish())
    }

    fn fixed<T: arrow::datatypes::ArrowPrimitiveType, const N: usize>(
        &self,
        body: &ArrayRef,
        type_name: &str,
        from_be: fn([u8; N]) -> T::Native,
        from_le: fn([u8; N]) -> T::Native,
    ) -> ArrayRef {
        let convert = if self.little_endian { from_le } else { from_be };
        let mut builder = arrow::array::PrimitiveBuilder::<T>::with_capacity(body.len());
        for row in 0..body.len() {
            match binary_body(body, row) {
                None => builder.append_null(),
                Some(bytes) => builder.append_value(convert(exact::<N>(bytes, type_name))),
            }
        }
        Arc::new(builder.finish())
    }
}

/// Flink's raw-format length check with its exact `DeserializationException` text: a fixed-width
/// value must arrive as exactly `N` bytes or the job fails.
fn exact<const N: usize>(bytes: &[u8], type_name: &str) -> [u8; N] {
    bytes.try_into().unwrap_or_else(|_| {
        panic!("Size of data received for deserializing {type_name} type is not {N}.")
    })
}

/// Reads row `row` of a binary "body" column as bytes, or `None` if the column is null there. Shared by
/// the JSON/CSV decoders, which accept a Binary, LargeBinary, or Utf8 body column.
pub fn binary_body(column: &ArrayRef, row: usize) -> Option<&[u8]> {
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

impl Decoder for RawDecoder {
    fn decode(&self, body: &RecordBatch) -> RecordBatch {
        self.decode(body)
    }
}
