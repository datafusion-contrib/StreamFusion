use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Decimal128Type, Int16Type, Int32Type, Int64Type, Int8Type};
use datafusion::common::{cast::as_primitive_array, exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(FlinkAbs {
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FlinkAbs {
    signature: Signature,
}

impl ScalarUDFImpl for FlinkAbs {
    fn name(&self) -> &str {
        "flink_exact_abs"
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
            [t @ (DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _))] => Ok(t.clone()),
            _ => exec_err!("Exact ABS requires one signed integer or DECIMAL operand"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke(&args.args)
    }
}

fn invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [input] = args else {
        return exec_err!("Exact ABS requires one operand");
    };
    Ok(match input {
        ColumnarValue::Array(input) => ColumnarValue::Array(abs(input)?),
        ColumnarValue::Scalar(input) => ColumnarValue::Scalar(match input {
            ScalarValue::Int8(v) => ScalarValue::Int8(v.map(i8::wrapping_abs)),
            ScalarValue::Int16(v) => ScalarValue::Int16(v.map(i16::wrapping_abs)),
            ScalarValue::Int32(v) => ScalarValue::Int32(v.map(i32::wrapping_abs)),
            ScalarValue::Int64(v) => ScalarValue::Int64(v.map(i64::wrapping_abs)),
            ScalarValue::Decimal128(v, p, s) => {
                ScalarValue::Decimal128(v.map(i128::wrapping_abs), *p, *s)
            }
            other => return exec_err!("Unsupported exact ABS input {other:?}"),
        }),
    })
}

fn abs(input: &ArrayRef) -> Result<ArrayRef> {
    // Flink retains the minimum negative value instead of reporting overflow.
    macro_rules! primitive {
        ($ty:ty) => {
            Arc::new(as_primitive_array::<$ty>(input)?.unary::<_, $ty>(|v| v.wrapping_abs()))
                as ArrayRef
        };
    }
    Ok(match input.data_type() {
        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::Decimal128(_, _) => Arc::new(
            as_primitive_array::<Decimal128Type>(input)?
                .unary::<_, Decimal128Type>(i128::wrapping_abs)
                .with_data_type(input.data_type().clone()),
        ),
        other => return exec_err!("Unsupported exact ABS input {other}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array};

    #[test]
    fn decimal_precision_scale_scalar_and_sliced_arrays_are_preserved() {
        for precision in 1..=38 {
            let max = 10i128.pow(u32::from(precision)) - 1;
            for scale in 0..=precision as i8 {
                let ty = DataType::Decimal128(precision, scale);
                let input = Decimal128Array::from(vec![
                    Some(42),
                    Some(-max),
                    None,
                    Some(-1),
                    Some(0),
                    Some(max),
                    Some(42),
                ])
                .with_data_type(ty.clone())
                .slice(1, 5);
                let result = abs(&(Arc::new(input.clone()) as ArrayRef)).unwrap();
                let result = as_primitive_array::<Decimal128Type>(&result).unwrap();
                assert_eq!(result.data_type(), &ty);
                assert_eq!(result.nulls(), input.nulls());
                assert_eq!(
                    result.iter().collect::<Vec<_>>(),
                    vec![Some(max), None, Some(1), Some(0), Some(max)]
                );
                assert_eq!(input.value(0), -max);
                for values in [
                    input.slice(2, 0),
                    Decimal128Array::from(vec![None; 3]).with_data_type(ty.clone()),
                ] {
                    let result = abs(&(Arc::new(values.clone()) as ArrayRef)).unwrap();
                    assert_eq!(result.data_type(), &ty);
                    assert_eq!(result.len(), values.len());
                    assert_eq!(result.null_count(), values.null_count());
                }
                for (input, expected) in [(Some(-max), Some(max)), (Some(0), Some(0)), (None, None)]
                {
                    let output = invoke(&[ColumnarValue::Scalar(ScalarValue::Decimal128(
                        input, precision, scale,
                    ))])
                    .unwrap();
                    let ColumnarValue::Scalar(output) = output else {
                        panic!("Scalar decimal ABS returned an array");
                    };
                    assert_eq!(output, ScalarValue::Decimal128(expected, precision, scale));
                }
            }
        }
    }

    #[test]
    fn widths_minima_nulls_and_sliced_validity() {
        macro_rules! check {
            ($array:ty, $native:ty) => {{
                let values = vec![
                    Some(42),
                    Some(<$native>::MIN),
                    Some(<$native>::MIN + 1),
                    None,
                    Some(-1),
                    Some(0),
                    Some(1),
                    Some(<$native>::MAX),
                    Some(-42),
                ];
                let input = <$array>::from(values).slice(1, 7);
                let output = abs(&(Arc::new(input.clone()) as ArrayRef)).unwrap();
                let output = output.as_any().downcast_ref::<$array>().unwrap();
                assert_eq!(output.data_type(), input.data_type());
                assert_eq!(output.nulls(), input.nulls());
                assert_eq!(
                    output.iter().collect::<Vec<_>>(),
                    vec![
                        Some(<$native>::MIN),
                        Some(<$native>::MAX),
                        None,
                        Some(1),
                        Some(0),
                        Some(1),
                        Some(<$native>::MAX),
                    ]
                );
                assert_eq!(input.value(0), <$native>::MIN);
                for empty in [input.slice(3, 0), <$array>::from(vec![None; 3])] {
                    let result = abs(&(Arc::new(empty.clone()) as ArrayRef)).unwrap();
                    assert_eq!(result.len(), empty.len());
                    assert_eq!(result.null_count(), empty.null_count());
                }
            }};
        }
        check!(Int8Array, i8);
        check!(Int16Array, i16);
        check!(Int32Array, i32);
        check!(Int64Array, i64);
    }

    #[test]
    fn scalar_literals_keep_their_declared_type_and_overflow() {
        for scalar in [
            ScalarValue::Int8(Some(i8::MIN)),
            ScalarValue::Int8(None),
            ScalarValue::Int16(Some(i16::MIN)),
            ScalarValue::Int16(None),
            ScalarValue::Int32(Some(i32::MIN)),
            ScalarValue::Int32(None),
            ScalarValue::Int64(Some(i64::MIN)),
            ScalarValue::Int64(None),
        ] {
            let args = [ColumnarValue::Scalar(scalar.clone())];
            let output = invoke(&args).unwrap();
            let ColumnarValue::Scalar(output) = output else {
                panic!("Scalar ABS returned an array");
            };
            assert_eq!(output, scalar);
        }
    }

    #[test]
    fn rejects_unsupported_widths_and_arities() {
        let f = function();
        for types in [
            vec![],
            vec![DataType::Float64],
            vec![DataType::Decimal256(38, 3)],
            vec![DataType::Int32, DataType::Int64],
        ] {
            assert!(f.return_type(&types).is_err());
        }
        assert!(invoke(&[]).is_err());
    }
}
