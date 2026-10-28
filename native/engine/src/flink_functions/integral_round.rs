use std::sync::Arc;

use arrow::array::{new_null_array, ArrayRef};
use arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type};
use datafusion::common::{cast::as_primitive_array, exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(super) fn function(truncate: bool) -> ScalarUDF {
    ScalarUDF::new_from_impl(IntegralRound {
        signature: Signature::user_defined(Volatility::Immutable),
        truncate,
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct IntegralRound {
    signature: Signature,
    truncate: bool,
}

impl ScalarUDFImpl for IntegralRound {
    fn name(&self) -> &str {
        if self.truncate {
            "flink_integral_truncate"
        } else {
            "flink_integral_round"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        self.return_type(types)?;
        Ok(types.to_vec())
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if self.truncate && !matches!(types.first(), Some(DataType::Int32 | DataType::Int64)) {
            return exec_err!("Integer TRUNCATE requires INT or BIGINT input");
        }
        match types {
            [t @ (DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64), DataType::Int32] => {
                Ok(t.clone())
            }
            _ => exec_err!("Integer ROUND requires a signed integer and an INT position"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        invoke(&args.args, self.truncate)
    }
}

fn invoke(args: &[ColumnarValue], truncate: bool) -> Result<ColumnarValue> {
    let [input, ColumnarValue::Scalar(ScalarValue::Int32(position))] = args else {
        return exec_err!("Integer rounding requires an integer and a literal INT position");
    };
    if position.is_some_and(|p| !(-38..=38).contains(&p)) {
        return exec_err!("Integer rounding position is outside the verified -38..38 range");
    }
    if truncate && !matches!(input.data_type(), DataType::Int32 | DataType::Int64) {
        return exec_err!("Integer TRUNCATE requires INT or BIGINT input");
    }
    match input {
        ColumnarValue::Array(input) => array(input, *position, truncate).map(ColumnarValue::Array),
        ColumnarValue::Scalar(input) => {
            macro_rules! scalar {
                ($value:expr, $variant:ident, $native:ty) => {
                    ScalarValue::$variant($value.and_then(|value| {
                        position.map(|position| {
                            scalar_round(value as i128, position, truncate) as $native
                        })
                    }))
                };
            }
            Ok(ColumnarValue::Scalar(match input {
                ScalarValue::Int8(v) => scalar!(v, Int8, i8),
                ScalarValue::Int16(v) => scalar!(v, Int16, i16),
                ScalarValue::Int32(v) => scalar!(v, Int32, i32),
                ScalarValue::Int64(v) => scalar!(v, Int64, i64),
                other => return exec_err!("Unsupported integer ROUND input {other:?}"),
            }))
        }
    }
}

fn scalar_round(value: i128, position: i32, truncate: bool) -> i128 {
    if position >= 0 {
        return value;
    }
    let power = 10i128.pow((-position) as u32);
    let remainder = value % power;
    if truncate {
        return value - remainder;
    }
    value - remainder
        + if remainder >= power / 2 {
            power
        } else if remainder <= -power / 2 {
            -power
        } else {
            0
        }
}

fn array(input: &ArrayRef, position: Option<i32>, truncate: bool) -> Result<ArrayRef> {
    if !matches!(
        input.data_type(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) {
        return exec_err!("Unsupported integer ROUND input {}", input.data_type());
    }
    let Some(position) = position else {
        return Ok(new_null_array(input.data_type(), input.len()));
    };
    if position >= 0 {
        return Ok(input.clone());
    }
    macro_rules! integer {
        ($type:ty, $native:ty) => {{
            let input = as_primitive_array::<$type>(input)?;
            if let Some(power) = (10 as $native).checked_pow((-position) as u32) {
                if truncate {
                    return Ok(Arc::new(
                        input.unary::<_, $type>(|value| value - value % power),
                    ));
                }
                let half = power / 2;
                Arc::new(input.unary::<_, $type>(|value| {
                    let remainder = value % power;
                    let truncated = value.wrapping_sub(remainder);
                    if remainder >= half {
                        truncated.wrapping_add(power)
                    } else if remainder <= -half {
                        truncated.wrapping_sub(power)
                    } else {
                        truncated
                    }
                })) as ArrayRef
            } else {
                if truncate {
                    return Ok(Arc::new(input.unary::<_, $type>(|_| 0)));
                }
                // Like Comet, compare in i128 when the power exceeds the input width.
                let power = 10i128.pow((-position) as u32);
                let half = power / 2;
                Arc::new(input.unary::<_, $type>(|value| {
                    let value = value as i128;
                    if value >= half {
                        power as $native
                    } else if value <= -half {
                        (-power) as $native
                    } else {
                        0
                    }
                })) as ArrayRef
            }
        }};
    }
    Ok(match input.data_type() {
        DataType::Int8 => integer!(Int8Type, i8),
        DataType::Int16 => integer!(Int16Type, i16),
        DataType::Int32 => integer!(Int32Type, i32),
        DataType::Int64 => integer!(Int64Type, i64),
        _ => unreachable!("validated integer input"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int16Array, Int32Array, Int64Array, Int8Array};

    fn expected(value: i128, position: i32, truncate: bool) -> i128 {
        if position >= 0 {
            return value;
        }
        let power = 10i128.pow((-position) as u32);
        let magnitude = value.abs();
        let quotient =
            magnitude / power + i128::from(!truncate && (magnitude % power) * 2 >= power);
        quotient * power * value.signum()
    }

    #[test]
    fn widths_ties_overflow_nulls_and_sliced_ownership() {
        macro_rules! check {
            ($array:ty, $native:ty, $variant:ident, $truncate:expr) => {{
                let mut values = vec![Some(<$native>::MIN), Some(<$native>::MAX), None];
                values.extend((-180..=180).map(|v| Some(v as $native)));
                let mut random = 0x1234_5678_9abc_def0u64;
                for _ in 0..1000 {
                    random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                    values.push(Some(random as $native));
                }
                let input = <$array>::from(values).slice(0, 1364);
                let input: ArrayRef = Arc::new(input);
                for position in -38..=38 {
                    let output = array(&input, Some(position), $truncate).unwrap();
                    let output = output.as_any().downcast_ref::<$array>().unwrap();
                    let source = input.as_any().downcast_ref::<$array>().unwrap();
                    assert_eq!(output.data_type(), input.data_type());
                    assert_eq!(output.nulls(), input.nulls());
                    for (actual, value) in output.iter().zip(source.iter()) {
                        assert_eq!(
                            actual,
                            value.map(|v| expected(v as i128, position, $truncate) as $native)
                        );
                    }
                    assert_eq!(source.value(0), <$native>::MIN);
                    if position >= 0 {
                        assert_eq!(output.values().as_ptr(), source.values().as_ptr());
                    }
                    for value in [Some(<$native>::MIN), Some(<$native>::MAX), Some(-15), None] {
                        let output = invoke(
                            &[
                                ColumnarValue::Scalar(ScalarValue::$variant(value)),
                                ColumnarValue::Scalar(ScalarValue::Int32(Some(position))),
                            ],
                            $truncate,
                        )
                        .unwrap();
                        let ColumnarValue::Scalar(output) = output else {
                            panic!("scalar ROUND returned array")
                        };
                        assert_eq!(
                            output,
                            ScalarValue::$variant(
                                value.map(|v| expected(v as i128, position, $truncate) as $native)
                            )
                        );
                    }
                    let sliced: ArrayRef = Arc::new(source.slice(1, 17));
                    let output = array(&sliced, Some(position), $truncate).unwrap();
                    assert_eq!(output.nulls(), sliced.nulls());
                    for value in [source.slice(1, 0), <$array>::from(vec![None; 3])] {
                        let output = array(
                            &(Arc::new(value.clone()) as ArrayRef),
                            Some(position),
                            $truncate,
                        )
                        .unwrap();
                        assert_eq!(output.len(), value.len());
                        assert_eq!(output.null_count(), value.null_count());
                    }
                }
                let output = array(&input, None, $truncate).unwrap();
                assert_eq!(output.null_count(), input.len());
                assert_eq!(output.data_type(), input.data_type());
                let output = invoke(
                    &[
                        ColumnarValue::Scalar(ScalarValue::$variant(Some(<$native>::MIN))),
                        ColumnarValue::Scalar(ScalarValue::Int32(None)),
                    ],
                    $truncate,
                )
                .unwrap();
                let ColumnarValue::Scalar(output) = output else {
                    panic!("scalar ROUND returned array")
                };
                assert_eq!(output, ScalarValue::$variant(None));
            }};
        }
        check!(Int8Array, i8, Int8, false);
        check!(Int16Array, i16, Int16, false);
        check!(Int32Array, i32, Int32, false);
        check!(Int64Array, i64, Int64, false);
        check!(Int32Array, i32, Int32, true);
        check!(Int64Array, i64, Int64, true);
    }

    #[test]
    fn rejects_unverified_positions_types_and_arities() {
        let f = function(false);
        for types in [
            vec![],
            vec![DataType::Int32],
            vec![DataType::Float64, DataType::Int32],
            vec![DataType::Int64, DataType::Int64],
        ] {
            assert!(f.return_type(&types).is_err());
        }
        for position in [-39, 39, i32::MIN, i32::MAX] {
            assert!(invoke(
                &[
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(position))),
                ],
                false
            )
            .is_err());
        }
        assert!(invoke(&[], false).is_err());
        assert!(invoke(
            &[
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
                ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(-1)]))),
            ],
            false
        )
        .is_err());
        let truncate = function(true);
        for input in [DataType::Int8, DataType::Int16, DataType::Float64] {
            assert!(truncate.return_type(&[input, DataType::Int32]).is_err());
        }
        assert!(invoke(
            &[
                ColumnarValue::Scalar(ScalarValue::Int16(Some(1))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(-1))),
            ],
            true
        )
        .is_err());
    }
}
