use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;
use streamfusion_bridge::timestamp::{
    is_timestamp, timestamp_array, timestamp_type, TimestampColumn, TimestampValue,
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
        datafusion::functions::utils::make_scalar_function(round, vec![])(&args.args)
    }
}

fn round(arrays: &[ArrayRef]) -> Result<ArrayRef> {
    let timestamps = TimestampColumn::try_new(arrays[0].as_ref())?;
    let widths = arrays[1].as_any().downcast_ref::<Int64Array>().unwrap();
    let ceilings = arrays[2].as_any().downcast_ref::<BooleanArray>().unwrap();
    let mut result = Vec::with_capacity(timestamps.len());
    for row in 0..timestamps.len() {
        if timestamps.is_null(row) || widths.is_null(row) || ceilings.is_null(row) {
            result.push(None);
            continue;
        }
        let value = timestamps.value(row)?;
        let width = widths.value(row);
        if width <= 0 {
            return exec_err!("Temporal rounding width must be positive");
        }
        let millis = value.millis();
        let ceiling = ceilings.value(row);
        let rounded = if width == 1 {
            millis.wrapping_add(i64::from(ceiling && value.nano_of_milli() != 0))
        } else {
            // SqlFunctionUtils.ceil(long, long) adds width - 1 before floor, with Java
            // overflow. Fractions are ignored except when rounding to MILLISECOND itself.
            let value = if ceiling {
                millis.wrapping_add(width - 1)
            } else {
                millis
            };
            value.wrapping_sub(value.rem_euclid(width))
        };
        result.push(Some(TimestampValue::from_millis(rounded)));
    }
    Ok(Arc::new(timestamp_array(result)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion::common::{config::ConfigOptions, ScalarValue};

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
