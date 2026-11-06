use std::sync::Arc;

use arrow::array::{new_null_array, Array, ArrayRef};
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
    if let [input, ColumnarValue::Array(position)] = args {
        if truncate && !matches!(input.data_type(), DataType::Int32 | DataType::Int64) {
            return exec_err!("Integer TRUNCATE requires INT or BIGINT input");
        }
        let input = input.clone().into_array(position.len())?;
        return positioned_array(&input, position, truncate).map(ColumnarValue::Array);
    }
    let [input, ColumnarValue::Scalar(ScalarValue::Int32(position))] = args else {
        return exec_err!("Integer rounding requires an integer and an INT position");
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

fn positioned_array(input: &ArrayRef, position: &ArrayRef, truncate: bool) -> Result<ArrayRef> {
    if input.len() != position.len() {
        return exec_err!("Integer rounding inputs must have equal lengths");
    }
    let position = as_primitive_array::<Int32Type>(position)?;
    macro_rules! integer {
        ($type:ty, $native:ty) => {{
            let input = as_primitive_array::<$type>(input)?;
            let mut values = Vec::with_capacity(input.len());
            for row in 0..input.len() {
                if input.is_null(row) || position.is_null(row) {
                    values.push(None);
                    continue;
                }
                let digits = position.value(row);
                if !(-38..=38).contains(&digits) {
                    return exec_err!(
                        "Integer rounding position is outside the verified -38..38 range"
                    );
                }
                values.push(Some(
                    positioned_round(input.value(row) as i64, digits, truncate) as $native,
                ));
            }
            Arc::new(arrow::array::PrimitiveArray::<$type>::from_iter(values)) as ArrayRef
        }};
    }
    Ok(match input.data_type() {
        DataType::Int8 => integer!(Int8Type, i8),
        DataType::Int16 => integer!(Int16Type, i16),
        DataType::Int32 => integer!(Int32Type, i32),
        DataType::Int64 => integer!(Int64Type, i64),
        other => return exec_err!("Unsupported integer ROUND input {other}"),
    })
}

fn positioned_round(value: i64, position: i32, truncate: bool) -> i64 {
    if position >= 0 {
        return value;
    }
    const POWERS: [i64; 19] = [
        1,
        10,
        100,
        1_000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
        1_000_000_000,
        10_000_000_000,
        100_000_000_000,
        1_000_000_000_000,
        10_000_000_000_000,
        100_000_000_000_000,
        1_000_000_000_000_000,
        10_000_000_000_000_000,
        100_000_000_000_000_000,
        1_000_000_000_000_000_000,
    ];
    let digits = (-position) as usize;
    if digits >= POWERS.len() {
        // Only 10^19 can round a signed i64 to a nonzero value outside its width.
        return if !truncate && digits == 19 && value >= 5_000_000_000_000_000_000 {
            10_000_000_000_000_000_000u64 as i64
        } else if !truncate && digits == 19 && value <= -5_000_000_000_000_000_000 {
            (10_000_000_000_000_000_000u64 as i64).wrapping_neg()
        } else {
            0
        };
    }
    let power = POWERS[digits];
    let remainder = value % power;
    let rounded = value.wrapping_sub(remainder);
    if truncate {
        rounded
    } else if remainder >= power / 2 {
        rounded.wrapping_add(power)
    } else if remainder <= -power / 2 {
        rounded.wrapping_sub(power)
    } else {
        rounded
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
    fn runtime_positions_preserve_widths_nulls_slices_and_source_ownership() {
        macro_rules! check {
            ($array:ty, $native:ty, $truncate:expr) => {{
                let mut values = Vec::new();
                let mut positions = Vec::new();
                for digits in -38..=38 {
                    for value in [
                        Some(<$native>::MIN),
                        Some(<$native>::MAX),
                        Some((-150i32) as $native),
                        Some(-15),
                        Some(-5),
                        Some(0),
                        Some(5),
                        Some(15),
                        Some(150i32 as $native),
                        Some(4_999_999_999_999_999_999i64 as $native),
                        Some(5_000_000_000_000_000_000i64 as $native),
                        Some(5_000_000_000_000_000_001i64 as $native),
                        Some(-4_999_999_999_999_999_999i64 as $native),
                        Some(-5_000_000_000_000_000_000i64 as $native),
                        Some(-5_000_000_000_000_000_001i64 as $native),
                        None,
                    ] {
                        values.push(value);
                        positions.push(Some(digits));
                        values.push(value);
                        positions.push(None);
                    }
                }
                let original: ArrayRef = Arc::new(<$array>::from(values.clone()));
                let original_positions: ArrayRef = Arc::new(Int32Array::from(positions.clone()));
                let input: ArrayRef = Arc::new(<$array>::from(values));
                let positions: ArrayRef = Arc::new(Int32Array::from(positions));
                for (input, positions) in [
                    (input.clone(), positions.clone()),
                    (
                        input.slice(1, input.len() - 2),
                        positions.slice(1, positions.len() - 2),
                    ),
                    (input.slice(0, 0), positions.slice(0, 0)),
                ] {
                    let output = positioned_array(&input, &positions, $truncate).unwrap();
                    let output = output.as_any().downcast_ref::<$array>().unwrap();
                    let input = input.as_any().downcast_ref::<$array>().unwrap();
                    let positions = positions.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(output.len(), input.len());
                    for row in 0..input.len() {
                        let expected = if input.is_null(row) || positions.is_null(row) {
                            None
                        } else {
                            Some(
                                expected(input.value(row) as i128, positions.value(row), $truncate)
                                    as $native,
                            )
                        };
                        assert_eq!(output.is_null(row), expected.is_none());
                        if let Some(expected) = expected {
                            assert_eq!(output.value(row), expected);
                        }
                    }
                }
                assert_eq!(input.to_data(), original.to_data());
                assert_eq!(positions.to_data(), original_positions.to_data());
            }};
        }
        check!(Int8Array, i8, false);
        check!(Int16Array, i16, false);
        check!(Int32Array, i32, false);
        check!(Int64Array, i64, false);
        check!(Int32Array, i32, true);
        check!(Int64Array, i64, true);
    }

    #[test]
    fn runtime_positions_broadcast_scalar_values_and_reject_invalid_shapes() {
        let positions: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(-1),
            Some(0),
            None,
            Some(-38),
            Some(38),
        ]));
        let output = invoke(
            &[
                ColumnarValue::Scalar(ScalarValue::Int64(Some(15))),
                ColumnarValue::Array(positions.clone()),
            ],
            false,
        )
        .unwrap()
        .into_array(5)
        .unwrap();
        assert_eq!(
            output.as_ref(),
            &Int64Array::from(vec![Some(20), Some(15), None, Some(0), Some(15)])
        );
        let output = invoke(
            &[
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
                ColumnarValue::Array(positions.clone()),
            ],
            false,
        )
        .unwrap()
        .into_array(5)
        .unwrap();
        assert_eq!(output.null_count(), 5);
        assert!(invoke(
            &[
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![1, 2]))),
                ColumnarValue::Array(positions.clone()),
            ],
            false
        )
        .is_err());
        assert!(invoke(
            &[
                ColumnarValue::Scalar(ScalarValue::Int16(Some(15))),
                ColumnarValue::Array(positions),
            ],
            true
        )
        .is_err());
        for position in [-39, 39, i32::MIN, i32::MAX] {
            assert!(invoke(
                &[
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(15))),
                    ColumnarValue::Array(Arc::new(Int32Array::from(vec![position]))),
                ],
                false
            )
            .is_err());
        }
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
                ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(-39)]))),
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
