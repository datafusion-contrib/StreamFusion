use arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, StringArray};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, Time32MillisecondType, TimeUnit,
};
use datafusion::common::{cast::as_primitive_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;
use streamfusion_bridge::timestamp::{is_timestamp, TimestampColumn};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(HashCode {
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct HashCode {
    signature: Signature,
}

impl ScalarUDFImpl for HashCode {
    fn name(&self) -> &str {
        "flink_hash_code"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        self.return_type(types)?;
        Ok(types.to_vec())
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Time32(TimeUnit::Millisecond)] => Ok(DataType::Int32),
            [t] if is_timestamp(t) => Ok(DataType::Int32),
            _ => exec_err!("HASH_CODE requires one supported scalar operand"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(hash_code, vec![])(&args.args)
    }
}

fn hash_code(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("HASH_CODE expects one argument");
    };
    macro_rules! primitive {
        ($ty:ty, $map:expr) => {
            Arc::new(as_primitive_array::<$ty>(input)?.unary::<_, Int32Type>($map)) as ArrayRef
        };
    }
    Ok(match input.data_type() {
        DataType::Boolean => {
            let values = input.as_any().downcast_ref::<BooleanArray>().unwrap();
            Arc::new(Int32Array::from_iter(
                values
                    .iter()
                    .map(|v| v.map(|v| if v { 1231 } else { 1237 })),
            ))
        }
        DataType::Int8 => primitive!(Int8Type, i32::from),
        DataType::Int16 => primitive!(Int16Type, i32::from),
        DataType::Int32 => primitive!(Int32Type, |v| v),
        DataType::Int64 => primitive!(Int64Type, long_hash),
        DataType::Date32 => primitive!(Date32Type, |v| v),
        DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondType, |v| v),
        DataType::Float32 => primitive!(Float32Type, |v: f32| if v.is_nan() {
            0x7fc00000
        } else {
            v.to_bits() as i32
        }),
        DataType::Float64 => primitive!(Float64Type, |v: f64| long_hash(if v.is_nan() {
            0x7ff8000000000000
        } else {
            v.to_bits() as i64
        })),
        DataType::Decimal128(_, scale) => primitive!(Decimal128Type, |v| decimal_hash(v, *scale)),
        DataType::Utf8 => {
            let values = input.as_any().downcast_ref::<StringArray>().unwrap();
            Arc::new(Int32Array::from_iter(
                values.iter().map(|v| v.map(string_hash)),
            ))
        }
        t if is_timestamp(t) => {
            let values = TimestampColumn::try_new(input.as_ref())?;
            let hashes = (0..values.len())
                .map(|row| {
                    if values.is_null(row) {
                        return Ok(None);
                    }
                    let v = values.value(row)?;
                    Ok(Some(
                        long_hash(v.millis())
                            .wrapping_mul(31)
                            .wrapping_add(v.nano_of_milli() as i32),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(Int32Array::from(hashes))
        }
        other => return exec_err!("Unsupported HASH_CODE input {other}"),
    })
}

fn long_hash(value: i64) -> i32 {
    (value ^ (value >> 32)) as i32
}

fn string_hash(value: &str) -> i32 {
    value
        .encode_utf16()
        .fold(0i32, |hash, unit| {
            hash.wrapping_mul(31).wrapping_add(i32::from(unit))
        })
        .wrapping_abs()
}

fn decimal_hash(value: i128, scale: i8) -> i32 {
    // BigDecimal hashes the signed BigInteger magnitude, then includes the declared scale.
    let magnitude = value.unsigned_abs();
    let mut hash = 0i32;
    for shift in [96, 64, 32, 0] {
        hash = hash
            .wrapping_mul(31)
            .wrapping_add((magnitude >> shift) as i32);
    }
    if value < 0 {
        hash = hash.wrapping_neg();
    }
    hash.wrapping_mul(31).wrapping_add(i32::from(scale))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Decimal128Array;

    #[test]
    fn java_string_hash_uses_utf16_and_wrapping_abs() {
        assert_eq!(string_hash(""), 0);
        assert_eq!(string_hash("abc"), 96354);
        assert_eq!(string_hash("\u{1f600}"), 1772899);
        assert_eq!(string_hash("polygenelubricants"), i32::MIN);
    }

    #[test]
    fn decimal_hash_keeps_scale_sign_and_all_magnitude_words() {
        assert_eq!(decimal_hash(123456, 3), 3827139);
        assert_eq!(decimal_hash(-123456, 3), -3827133);
        assert_eq!(decimal_hash(0, 18), 18);
        assert_eq!(decimal_hash(1i128 << 96, 0), 923521);
        assert_eq!(decimal_hash(-(1i128 << 96), 0), -923521);
    }

    #[test]
    fn slices_nulls_and_empty_arrays_preserve_shape() {
        let values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(1), None, Some(-123456)])
                .with_precision_and_scale(38, 3)
                .unwrap(),
        );
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(-3827133)]));
        assert_eq!(&hash_code(&[values.slice(1, 2)]).unwrap(), &expected);
        assert_eq!(hash_code(&[values.slice(0, 0)]).unwrap().len(), 0);
    }
}
