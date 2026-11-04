use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;
use streamfusion_bridge::timestamp::{
    is_timestamp, timestamp_array, timestamp_type, TimestampBuilder, TimestampColumn,
    TimestampValue,
};

pub(crate) fn function() -> ScalarUDF {
    ScalarUDF::new_from_impl(TemporalRound {
        signature: Signature::any(3, Volatility::Immutable),
    })
}

pub(super) fn utc_ltz_ceil() -> ScalarUDF {
    ScalarUDF::new_from_impl(UtcLtzCeil {
        signature: Signature::user_defined(Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct UtcLtzCeil {
    signature: Signature,
}

impl ScalarUDFImpl for UtcLtzCeil {
    fn name(&self) -> &str {
        "flink_utc_ltz_ceil"
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
            [timestamp, DataType::Int64] if is_timestamp(timestamp) => Ok(timestamp_type()),
            _ => exec_err!("UTC LTZ CEIL requires a timestamp and a BIGINT width"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(ceil_utc_ltz, vec![])(&args.args)
    }
}

fn ceil_utc_ltz(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let timestamps = TimestampColumn::try_new(arrays[0].as_ref())?;
    let widths = arrays[1].as_any().downcast_ref::<Int64Array>().unwrap();
    let mut result = Vec::with_capacity(timestamps.len());
    for row in 0..timestamps.len() {
        if timestamps.is_null(row) || widths.is_null(row) {
            result.push(None);
            continue;
        }
        let millis = timestamps.value(row)?.millis();
        let width = widths.value(row);
        if width <= 0 {
            return exec_err!("Temporal rounding width must be positive");
        }
        // LTZ ignores nanos and rounds the original signed remainder before Java overflow.
        let remainder = millis % width;
        let rounded = millis
            .wrapping_sub(remainder)
            .wrapping_add(if remainder > 0 { width } else { 0 });
        result.push(Some(TimestampValue::from_millis(rounded)));
    }
    Ok(Arc::new(timestamp_array(result)))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct TemporalRound {
    signature: Signature,
}

impl ScalarUDFImpl for TemporalRound {
    fn name(&self) -> &str {
        "flink_temporal_round"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(timestamp_type())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Fixed SQL widths expose constant division without broadcasting two scalar columns.
        if let [ColumnarValue::Array(values), ColumnarValue::Scalar(ScalarValue::Int64(Some(width))), ColumnarValue::Scalar(ScalarValue::Boolean(Some(ceiling)))] =
            args.args.as_slice()
        {
            let result = match *width {
                1 => Some(round_fixed::<1>(values, *ceiling)),
                1_000 => Some(round_fixed::<1_000>(values, *ceiling)),
                60_000 => Some(round_fixed::<60_000>(values, *ceiling)),
                3_600_000 => Some(round_fixed::<3_600_000>(values, *ceiling)),
                86_400_000 => Some(round_fixed::<86_400_000>(values, *ceiling)),
                _ => None,
            };
            if let Some(result) = result {
                return result.map(ColumnarValue::Array);
            }
        }
        datafusion::functions::utils::make_scalar_function(round, vec![])(&args.args)
    }
}

fn round_fixed<const WIDTH: i64>(values: &ArrayRef, ceiling: bool) -> Result<ArrayRef> {
    if ceiling {
        round_fixed_values::<WIDTH, true>(values)
    } else {
        round_fixed_values::<WIDTH, false>(values)
    }
}

fn round_fixed_values<const WIDTH: i64, const CEILING: bool>(
    values: &ArrayRef,
) -> Result<ArrayRef> {
    let timestamps = TimestampColumn::try_new(values.as_ref())?;
    let mut result = TimestampBuilder::with_capacity(timestamps.len());
    for row in 0..timestamps.len() {
        if timestamps.is_null(row) {
            result.append_null();
        } else {
            result.append_value(TimestampValue::from_millis(rounded_millis(
                timestamps.value(row)?,
                WIDTH,
                CEILING,
            )));
        }
    }
    Ok(Arc::new(result.finish()))
}

#[inline]
fn rounded_millis(value: TimestampValue, width: i64, ceiling: bool) -> i64 {
    let millis = value.millis();
    if width == 1 {
        millis.wrapping_add(i64::from(ceiling && value.nano_of_milli() != 0))
    } else {
        // Released plain TIMESTAMP CEIL adds before floor, with Java long overflow.
        let value = if ceiling {
            millis.wrapping_add(width - 1)
        } else {
            millis
        };
        value.wrapping_sub(value.rem_euclid(width))
    }
}

fn round(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let timestamps = TimestampColumn::try_new(arrays[0].as_ref())?;
    let widths = arrays[1].as_any().downcast_ref::<Int64Array>().unwrap();
    let ceilings = arrays[2].as_any().downcast_ref::<BooleanArray>().unwrap();
    let mut result = TimestampBuilder::with_capacity(timestamps.len());
    for row in 0..timestamps.len() {
        if timestamps.is_null(row) || widths.is_null(row) || ceilings.is_null(row) {
            result.append_null();
            continue;
        }
        let value = timestamps.value(row)?;
        let width = widths.value(row);
        if width <= 0 {
            return exec_err!("Temporal rounding width must be positive");
        }
        result.append_value(TimestampValue::from_millis(rounded_millis(
            value,
            width,
            ceilings.value(row),
        )));
    }
    Ok(Arc::new(result.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion::common::{config::ConfigOptions, ScalarValue};

    #[test]
    fn fixed_scalar_dispatch_matches_array_rounding_at_extrema_nulls_and_slices() {
        let millis = [i64::MIN, i64::MAX, -1001, -1, 0, 1, 999, 1000, 1001];
        let input: ArrayRef = Arc::new(timestamp_array((0..137).map(|i| {
            (i % 7 != 0).then(|| TimestampValue::new(millis[i % millis.len()], 999_999).unwrap())
        })));
        for offset in [0, 1, 7, 65] {
            for len in [0, 1, 65] {
                let input = input.slice(offset, len);
                for width in [
                    None,
                    Some(1),
                    Some(1000),
                    Some(60_000),
                    Some(3_600_000),
                    Some(86_400_000),
                    Some(17),
                ] {
                    for ceiling in [None, Some(false), Some(true)] {
                        let expected = round(&[
                            input.clone(),
                            Arc::new(Int64Array::from(vec![width; len])),
                            Arc::new(BooleanArray::from(vec![ceiling; len])),
                        ])
                        .unwrap();
                        let actual = function()
                            .invoke_with_args(ScalarFunctionArgs {
                                args: vec![
                                    ColumnarValue::Array(input.clone()),
                                    ColumnarValue::Scalar(ScalarValue::Int64(width)),
                                    ColumnarValue::Scalar(ScalarValue::Boolean(ceiling)),
                                ],
                                number_rows: len,
                                return_field: Arc::new(Field::new(
                                    "result",
                                    timestamp_type(),
                                    true,
                                )),
                                arg_fields: vec![],
                                config_options: Arc::new(ConfigOptions::default()),
                            })
                            .unwrap()
                            .into_array(len)
                            .unwrap();
                        assert_eq!(actual.as_ref(), expected.as_ref());
                    }
                }
            }
        }
    }

    #[test]
    fn fixed_round_builder_keeps_sliced_parent_nulls_and_fraction_rules() {
        let millis = [-1, 0, 1, 999, 1000, 1001];
        for ceiling in [false, true] {
            for width in [1, 1000] {
                let expected = match (ceiling, width) {
                    (false, 1) => [-1, 0, 1, 999, 1000, 1001],
                    (true, 1) => [0, 1, 2, 1000, 1001, 1002],
                    (false, _) => [-1000, 0, 0, 0, 1000, 1000],
                    (true, _) => [0, 0, 1000, 1000, 1000, 2000],
                };
                let input =
                    timestamp_array((0..137).map(|i| {
                        (i % 7 != 0).then(|| TimestampValue::new(millis[i % 6], 1).unwrap())
                    }));
                let widths =
                    Int64Array::from_iter((0..137).map(|i| (i % 11 != 0).then_some(width)));
                let ceilings =
                    BooleanArray::from_iter((0..137).map(|i| (i % 13 != 0).then_some(ceiling)));
                for offset in [0, 1, 7, 65] {
                    for len in [0, 1, 65] {
                        let result = round(&[
                            Arc::new(input.slice(offset, len)),
                            Arc::new(widths.slice(offset, len)),
                            Arc::new(ceilings.slice(offset, len)),
                        ])
                        .unwrap();
                        assert_eq!(result.data_type(), &timestamp_type());
                        let result = TimestampColumn::try_new(result.as_ref()).unwrap();
                        for row in 0..len {
                            let i = row + offset;
                            let null = i % 7 == 0 || i % 11 == 0 || i % 13 == 0;
                            assert_eq!(result.is_null(row), null);
                            if !null {
                                assert_eq!(
                                    result.value(row).unwrap(),
                                    TimestampValue::from_millis(expected[i % 6])
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn utc_ltz_ceil_accepts_component_and_primitive_timestamp_inputs_without_coercion() {
        use arrow::array::{
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray,
        };
        let function = utc_ltz_ceil();
        for input in [
            Arc::new(TimestampSecondArray::from(vec![Some(-1), None, Some(1)])) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(-1),
                None,
                Some(1),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(-1),
                None,
                Some(1),
            ])),
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(-1),
                None,
                Some(1),
            ])),
        ] {
            let types = vec![input.data_type().clone(), DataType::Int64];
            assert_eq!(function.coerce_types(&types).unwrap(), types);
            let output = ceil_utc_ltz(&[input, Arc::new(Int64Array::from(vec![1000; 3]))]).unwrap();
            let output = TimestampColumn::try_new(output.as_ref()).unwrap();
            assert_eq!(
                output.value(0).unwrap(),
                TimestampValue::from_millis(
                    if matches!(
                        types[0],
                        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _)
                    ) {
                        -1000
                    } else {
                        0
                    }
                )
            );
            assert!(output.is_null(1));
            assert_eq!(
                output.value(2).unwrap(),
                TimestampValue::from_millis(
                    if matches!(
                        types[0],
                        DataType::Timestamp(
                            arrow::datatypes::TimeUnit::Second
                                | arrow::datatypes::TimeUnit::Millisecond,
                            _
                        )
                    ) {
                        1000
                    } else {
                        0
                    }
                )
            );
        }
        assert_eq!(
            function
                .coerce_types(&[timestamp_type(), DataType::Int64])
                .unwrap(),
            vec![timestamp_type(), DataType::Int64]
        );
        for types in [
            vec![],
            vec![DataType::Int64, DataType::Int64],
            vec![timestamp_type(), DataType::Int32],
        ] {
            assert!(function.coerce_types(&types).is_err());
        }
    }

    #[test]
    fn utc_ltz_ceil_preserves_full_range_nulls_fractions_and_slices() {
        let mut values = vec![None, Some(i64::MIN), Some(i64::MIN + 1)];
        values.extend((-1001..=1001).map(Some));
        values.extend([Some(i64::MAX - 1), Some(i64::MAX), None]);
        let input = timestamp_array(values.iter().enumerate().map(|(i, millis)| {
            millis.map(|millis| TimestampValue::new(millis, (i * 7919 % 1_000_000) as u32).unwrap())
        }));
        for input in [input.clone(), input.slice(1, input.len() - 2)] {
            let source = TimestampColumn::try_new(&input).unwrap();
            for width in [1i64, 1000, 60_000, 3_600_000, 86_400_000] {
                let output = ceil_utc_ltz(&[
                    Arc::new(input.clone()),
                    Arc::new(Int64Array::from(vec![width; input.len()])),
                ])
                .unwrap();
                let output = TimestampColumn::try_new(output.as_ref()).unwrap();
                assert_eq!(output.len(), source.len());
                for row in 0..source.len() {
                    assert_eq!(output.is_null(row), source.is_null(row));
                    if source.is_null(row) {
                        continue;
                    }
                    // Round with a wide intermediate, then narrow to Java's final long bits.
                    let expected =
                        ((i128::from(source.value(row).unwrap().millis()) + i128::from(width) - 1)
                            .div_euclid(i128::from(width))
                            * i128::from(width)) as i64;
                    assert_eq!(
                        output.value(row).unwrap(),
                        TimestampValue::from_millis(expected)
                    );
                }
            }
        }
        assert_eq!(
            input,
            timestamp_array(values.iter().enumerate().map(|(i, millis)| {
                millis.map(|millis| {
                    TimestampValue::new(millis, (i * 7919 % 1_000_000) as u32).unwrap()
                })
            }))
        );
    }

    #[test]
    fn utc_ltz_ceil_keeps_its_distinct_fraction_and_overflow_contract() {
        for (millis, width, expected) in [
            (-1, 1, -1),
            (0, 1, 0),
            (i64::MAX, 1, i64::MAX),
            (i64::MAX, 1000, -9_223_372_036_854_775_616),
            (i64::MIN, 1000, -9_223_372_036_854_775_000),
        ] {
            let input: ArrayRef = Arc::new(timestamp_array([Some(
                TimestampValue::new(millis, 999_999).unwrap(),
            )]));
            let widths: ArrayRef = Arc::new(Int64Array::from(vec![width]));
            let output = ceil_utc_ltz(&[input.clone(), widths.clone()]).unwrap();
            let output = TimestampColumn::try_new(output.as_ref()).unwrap();
            assert_eq!(
                output.value(0).unwrap(),
                TimestampValue::from_millis(expected)
            );
            if width == 1 {
                let plain =
                    round(&[input, widths, Arc::new(BooleanArray::from(vec![true]))]).unwrap();
                assert_ne!(
                    output.value(0).unwrap(),
                    TimestampColumn::try_new(plain.as_ref())
                        .unwrap()
                        .value(0)
                        .unwrap()
                );
            }
        }
    }

    #[test]
    fn utc_ltz_ceil_broadcasts_scalars_and_preserves_null_widths_and_empty_batches() {
        let input: ArrayRef =
            Arc::new(timestamp_array([Some(TimestampValue::new(-1, 1).unwrap())]));
        for width in [Some(1), Some(1000), None] {
            let output = utc_ltz_ceil()
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::try_from_array(&input, 0).unwrap()),
                        ColumnarValue::Scalar(ScalarValue::Int64(width)),
                    ],
                    number_rows: 7,
                    return_field: Arc::new(Field::new("result", timestamp_type(), true)),
                    arg_fields: vec![],
                    config_options: Arc::new(ConfigOptions::default()),
                })
                .unwrap()
                .into_array(7)
                .unwrap();
            let output = TimestampColumn::try_new(output.as_ref()).unwrap();
            for row in 0..7 {
                assert_eq!(output.is_null(row), width.is_none());
                if let Some(width) = width {
                    assert_eq!(
                        output.value(row).unwrap().millis(),
                        if width == 1 { -1 } else { 0 }
                    );
                    assert_eq!(output.value(row).unwrap().nano_of_milli(), 0);
                }
            }
        }
        let output = ceil_utc_ltz(&[
            input.slice(0, 0),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ])
        .unwrap();
        assert_eq!(output.len(), 0);
        let output = ceil_utc_ltz(&[
            Arc::new(timestamp_array([None, None])),
            Arc::new(Int64Array::from(vec![0, -1])),
        ])
        .unwrap();
        assert_eq!(output.null_count(), 2);
        for width in [0, -1] {
            assert!(
                ceil_utc_ltz(&[input.clone(), Arc::new(Int64Array::from(vec![width]))]).is_err()
            );
        }
    }
}
