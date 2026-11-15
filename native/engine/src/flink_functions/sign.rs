use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array};
use arrow::datatypes::{DataType, Decimal128Type, Float32Type, Float64Type, Int32Type, Int64Type};
use datafusion::common::{cast::as_primitive_array, exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(FlinkSign {
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct FlinkSign {
    signature: Signature,
}

impl ScalarUDFImpl for FlinkSign {
    fn name(&self) -> &str {
        "flink_sign"
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
            [t @ (DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64)] => {
                Ok(t.clone())
            }
            [t @ DataType::Decimal128(p, s)] if native_decimal(*p, *s) => Ok(t.clone()),
            _ => {
                exec_err!("SIGN requires one verified signed integer, floating or DECIMAL operand")
            }
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke(&args.args)
    }
}

fn invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [input] = args else {
        return exec_err!("SIGN requires one operand");
    };
    match input {
        ColumnarValue::Array(array)
            if matches!(
                array.data_type(),
                DataType::Int32 | DataType::Int64 | DataType::Decimal128(_, _)
            ) =>
        {
            Ok(ColumnarValue::Array(sign(std::slice::from_ref(array))?))
        }
        ColumnarValue::Scalar(ScalarValue::Int32(v)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Int32(v.map(i32::signum)),
        )),
        ColumnarValue::Scalar(ScalarValue::Int64(v)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Int64(v.map(i64::signum)),
        )),
        ColumnarValue::Scalar(ScalarValue::Decimal128(value, p, s)) if native_decimal(*p, *s) => {
            let value = if *p as i8 == *s {
                value.filter(|v| *v == 0)
            } else {
                value.map(|v| v.signum() * 10i128.pow(*s as u32))
            };
            Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                value, *p, *s,
            )))
        }
        _ => datafusion::functions::utils::make_scalar_function(sign, vec![])(args),
    }
}

fn native_decimal(precision: u8, scale: i8) -> bool {
    (1..=38).contains(&precision)
        && scale >= 0
        && scale <= precision as i8
        && (scale < precision as i8 || precision > 18)
}

fn sign(args: &[ArrayRef]) -> Result<ArrayRef> {
    macro_rules! integer {
        ($t:ty) => {
            Arc::new(as_primitive_array::<$t>(&args[0])?.unary::<_, $t>(|v| v.signum()))
        };
    }
    macro_rules! sign {
        ($t:ty) => {
            Arc::new(as_primitive_array::<$t>(&args[0])?.unary::<_, $t>(|value| {
                if value == 0.0 || value.is_nan() {
                    value
                } else {
                    value.signum()
                }
            }))
        };
    }
    Ok(match args[0].data_type() {
        DataType::Float32 => sign!(Float32Type),
        DataType::Float64 => sign!(Float64Type),
        DataType::Int32 => integer!(Int32Type),
        DataType::Int64 => integer!(Int64Type),
        DataType::Decimal128(p, s) if native_decimal(*p, *s) => {
            let input = as_primitive_array::<Decimal128Type>(&args[0])?;
            let output = if *p as i8 == *s {
                // Flink's noncompact DecimalData rejects +/-1 at DECIMAL(p,p).
                Decimal128Array::from_iter(input.iter().map(|v| v.filter(|v| *v == 0)))
            } else {
                let unit = 10i128.pow(*s as u32);
                input.unary::<_, Decimal128Type>(|v| v.signum() * unit)
            };
            Arc::new(output.with_data_type(input.data_type().clone()))
        }
        other => return exec_err!("SIGN expected a verified numeric input, got {other}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, Int64Array};

    #[test]
    fn integer_widths_minima_nulls_and_sliced_validity() {
        macro_rules! check {
            ($array:ty, $native:ty) => {{
                let input = <$array>::from(vec![
                    Some(42),
                    Some(<$native>::MIN),
                    Some(-1),
                    None,
                    Some(0),
                    Some(1),
                    Some(<$native>::MAX),
                    Some(-42),
                ])
                .slice(1, 6);
                let output = sign(&[Arc::new(input.clone())]).unwrap();
                let output = output.as_any().downcast_ref::<$array>().unwrap();
                assert_eq!(output.data_type(), input.data_type());
                assert_eq!(output.nulls(), input.nulls());
                assert_eq!(
                    output.iter().collect::<Vec<_>>(),
                    vec![Some(-1), Some(-1), None, Some(0), Some(1), Some(1)]
                );
                assert_eq!(input.value(0), <$native>::MIN);
                for empty in [input.slice(3, 0), <$array>::from(vec![None; 3])] {
                    let result = sign(&[Arc::new(empty.clone())]).unwrap();
                    assert_eq!(result.len(), empty.len());
                    assert_eq!(result.null_count(), empty.null_count());
                }
            }};
        }
        check!(Int32Array, i32);
        check!(Int64Array, i64);
    }

    #[test]
    fn integer_scalars_keep_their_declared_width() {
        macro_rules! check {
            ($variant:ident, $native:ty) => {
                for (input, expected) in [
                    (Some(<$native>::MIN), Some(-1)),
                    (Some(-1), Some(-1)),
                    (Some(0), Some(0)),
                    (Some(<$native>::MAX), Some(1)),
                    (None, None),
                ] {
                    let output =
                        invoke(&[ColumnarValue::Scalar(ScalarValue::$variant(input))]).unwrap();
                    let ColumnarValue::Scalar(output) = output else {
                        panic!("Scalar SIGN returned an array");
                    };
                    assert_eq!(output, ScalarValue::$variant(expected));
                }
            };
        }
        check!(Int32, i32);
        check!(Int64, i64);
    }

    #[test]
    fn decimal_values_metadata_nulls_and_slices_match_flink() {
        let f = function();
        for p in 1..=38 {
            let max = 10i128.pow(u32::from(p)) - 1;
            for s in 0..=p as i8 {
                let ty = DataType::Decimal128(p, s);
                if p <= 18 && s == p as i8 {
                    assert!(f.return_type(std::slice::from_ref(&ty)).is_err());
                    continue;
                }
                assert_eq!(
                    f.coerce_types(std::slice::from_ref(&ty)).unwrap(),
                    vec![ty.clone()]
                );
                let expected = |v: i128| {
                    if s == p as i8 && v != 0 {
                        None
                    } else {
                        Some(v.signum() * 10i128.pow(s as u32))
                    }
                };
                let input = Decimal128Array::from(vec![
                    Some(0),
                    Some(-max),
                    Some(-1),
                    None,
                    Some(0),
                    Some(1),
                    Some(max),
                    Some(0),
                ])
                .with_data_type(ty.clone())
                .slice(1, 6);
                let output = sign(&[Arc::new(input.clone())]).unwrap();
                let output = as_primitive_array::<Decimal128Type>(&output).unwrap();
                assert_eq!(output.data_type(), &ty);
                assert_eq!(
                    output.iter().collect::<Vec<_>>(),
                    vec![
                        expected(-max),
                        expected(-1),
                        None,
                        expected(0),
                        expected(1),
                        expected(max)
                    ]
                );
                assert_eq!(input.value(0), -max);
                for empty in [
                    input.slice(2, 0),
                    Decimal128Array::from(vec![None; 3]).with_data_type(ty.clone()),
                ] {
                    let output = sign(&[Arc::new(empty.clone())]).unwrap();
                    assert_eq!(output.data_type(), &ty);
                    assert_eq!(output.len(), empty.len());
                    assert_eq!(output.null_count(), empty.null_count());
                }
                for value in [Some(-max), Some(-1), Some(0), Some(1), Some(max), None] {
                    let output =
                        invoke(&[ColumnarValue::Scalar(ScalarValue::Decimal128(value, p, s))])
                            .unwrap();
                    let ColumnarValue::Scalar(output) = output else {
                        panic!("Scalar decimal SIGN returned an array");
                    };
                    assert_eq!(
                        output,
                        ScalarValue::Decimal128(value.and_then(expected), p, s)
                    );
                }
            }
        }
    }

    #[test]
    fn rejects_unsupported_types_and_arities() {
        let f = function();
        for types in [
            vec![],
            vec![DataType::UInt32],
            vec![DataType::Decimal128(18, 18)],
            vec![DataType::Decimal128(0, 0)],
            vec![DataType::Decimal128(39, 0)],
            vec![DataType::Decimal128(18, -1)],
            vec![DataType::Decimal128(18, 19)],
            vec![DataType::Decimal256(38, 3)],
            vec![DataType::Int32, DataType::Int64],
        ] {
            assert!(f.return_type(&types).is_err());
        }
        assert!(invoke(&[]).is_err());
    }
}
