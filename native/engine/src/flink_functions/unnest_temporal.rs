use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Int32Array, Int64Array, NullBufferBuilder, StructArray,
    Time32MillisecondArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::{DataType, Date32Type, Time32MillisecondType, TimeUnit};
use datafusion::common::{cast::as_primitive_array, exec_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use streamfusion_bridge::timestamp::timestamp_type;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(super) enum Operation {
    EpochMillis,
    EpochSeconds,
    EpochPrecision,
    UtcDate,
    UtcTime,
    ZonedDate,
    ZonedTime,
}

pub(super) fn function(operation: Operation) -> ScalarUDF {
    ScalarUDF::new_from_impl(UnnestTemporal {
        operation,
        signature: if operation == Operation::EpochPrecision {
            Signature::variadic_any(Volatility::Immutable)
        } else {
            Signature::any(
                match operation {
                    Operation::ZonedTime => 3,
                    Operation::UtcTime | Operation::ZonedDate => 2,
                    _ => 1,
                },
                Volatility::Immutable,
            )
        },
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct UnnestTemporal {
    operation: Operation,
    signature: Signature,
}

impl ScalarUDFImpl for UnnestTemporal {
    fn name(&self) -> &str {
        match self.operation {
            Operation::EpochMillis => "flink_unnest_epoch_millis",
            Operation::EpochSeconds => "flink_epoch_seconds",
            Operation::EpochPrecision => "flink_epoch_precision",
            Operation::UtcDate => "flink_unnest_utc_date",
            Operation::UtcTime => "flink_unnest_utc_time",
            Operation::ZonedDate => "flink_unnest_zoned_date",
            Operation::ZonedTime => "flink_unnest_zoned_time",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match (self.operation, types) {
            (Operation::EpochMillis | Operation::EpochSeconds, [DataType::Int64]) => {
                Ok(timestamp_type())
            }
            (Operation::EpochPrecision, types)
                if types.len() >= 4
                    && types.len() % 2 == 0
                    && types[0] == DataType::Int64
                    && types.last() == Some(&DataType::Int32)
                    && types[1..types.len() - 1]
                        .chunks_exact(2)
                        .all(|pair| pair == [DataType::Boolean, DataType::Int32]) =>
            {
                Ok(timestamp_type())
            }
            (Operation::UtcDate, [ty]) if streamfusion_bridge::timestamp::is_timestamp(ty) => {
                Ok(DataType::Date32)
            }
            (Operation::UtcTime, [ty, DataType::Int32])
                if streamfusion_bridge::timestamp::is_timestamp(ty) =>
            {
                Ok(DataType::Time32(TimeUnit::Millisecond))
            }
            (Operation::ZonedDate, [ty, DataType::Int32])
                if streamfusion_bridge::timestamp::is_timestamp(ty) =>
            {
                Ok(DataType::Date32)
            }
            (Operation::ZonedTime, [ty, DataType::Int32, DataType::Int32])
                if streamfusion_bridge::timestamp::is_timestamp(ty) =>
            {
                Ok(DataType::Time32(TimeUnit::Millisecond))
            }
            _ => exec_err!("{}: unexpected operand types {types:?}", self.name()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.return_type(
            &args
                .args
                .iter()
                .map(ColumnarValue::data_type)
                .collect::<Vec<_>>(),
        )?;
        let input = args.args[0].clone().into_array(args.number_rows)?;
        let output: ArrayRef = match self.operation {
            Operation::EpochMillis | Operation::EpochSeconds => {
                let values = as_primitive_array::<arrow::datatypes::Int64Type>(&input)?;
                if self.operation == Operation::EpochSeconds {
                    epoch_timestamp::<true>(values)
                } else {
                    epoch_timestamp::<false>(values)
                }
            }
            Operation::EpochPrecision => {
                let values = as_primitive_array::<arrow::datatypes::Int64Type>(&input)?;
                epoch_timestamp_with_precision(values, &args.args[1..])?
            }
            Operation::UtcDate => {
                let millis: Int64Array = super::map_timestamp_millis(&input, |value| value)?;
                Arc::new(millis.unary::<_, Date32Type>(utc_date))
            }
            Operation::UtcTime => {
                let factor = time_factor(&args.args[1])?;
                let millis: Int64Array = super::map_timestamp_millis(&input, |value| value)?;
                Arc::new(millis.unary::<_, Time32MillisecondType>(|value| {
                    (value.rem_euclid(86_400_000) as i32 / factor) * factor
                }))
            }
            Operation::ZonedDate | Operation::ZonedTime => {
                let millis = super::map_timestamp_millis(&input, |value| value)?;
                let offsets = args.args[1].clone().into_array(args.number_rows)?;
                let offsets = as_primitive_array::<arrow::datatypes::Int32Type>(&offsets)?;
                let local = |row| {
                    if millis.is_null(row) || offsets.is_null(row) {
                        return None;
                    }
                    let value = millis.value(row);
                    let within_day =
                        value.rem_euclid(86_400_000) + i64::from(offsets.value(row)) * 1000;
                    Some((
                        value.div_euclid(86_400_000) + within_day.div_euclid(86_400_000),
                        within_day.rem_euclid(86_400_000) as i32,
                    ))
                };
                if self.operation == Operation::ZonedDate {
                    Arc::new(Date32Array::from_iter(
                        (0..millis.len()).map(|row| local(row).map(|(day, _)| flink_date(day))),
                    ))
                } else {
                    let factor = time_factor(&args.args[2])?;
                    Arc::new(Time32MillisecondArray::from_iter(
                        (0..millis.len())
                            .map(|row| local(row).map(|(_, time)| time / factor * factor)),
                    ))
                }
            }
        };
        Ok(ColumnarValue::Array(output))
    }
}

pub(super) fn epoch_timestamp<const SECONDS: bool>(values: &Int64Array) -> ArrayRef {
    let bounds = if SECONDS {
        -62_167_219_200..=253_402_300_799
    } else {
        -62_167_219_200_000..=253_402_300_799_999
    };
    let range = NullBuffer::new(BooleanBuffer::collect_bool(values.len(), |row| {
        bounds.contains(&values.value(row))
    }));
    let nulls =
        NullBuffer::union(values.nulls(), Some(&range)).filter(|nulls| nulls.null_count() != 0);
    let millis = if SECONDS {
        // Range validity is checked before scaling; invalid payloads remain masked.
        let (_, buffer, _) = values
            .unary::<_, arrow::datatypes::Int64Type>(|value| value.wrapping_mul(1000))
            .into_parts();
        buffer
    } else {
        values.values().clone()
    };
    let DataType::Struct(fields) = timestamp_type() else {
        unreachable!()
    };
    // The parent owns row validity; children only hold their immutable component buffers.
    Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(Int64Array::new(millis, None)),
            Arc::new(Int32Array::from(vec![0; values.len()])),
        ],
        nulls,
    ))
}

fn epoch_timestamp_with_precision(values: &Int64Array, args: &[ColumnarValue]) -> Result<ArrayRef> {
    use super::round_position::{Position, Predicate};
    if args
        .iter()
        .any(|arg| matches!(arg, ColumnarValue::Array(a) if a.len() != values.len()))
    {
        return exec_err!("Epoch values and precision branches must have equal lengths");
    }
    let branches = args[..args.len() - 1]
        .chunks_exact(2)
        .map(|pair| (Predicate::new(&pair[0]), Position::new(&pair[1])))
        .collect::<Vec<_>>();
    let otherwise = Position::new(args.last().unwrap());
    let pick = |row| {
        for (condition, precision) in &branches {
            if condition.at(row) {
                return precision.at(row);
            }
        }
        otherwise.at(row)
    };
    let scalar_precisions = branches
        .iter()
        .map(|(_, p)| p)
        .chain(std::iter::once(&otherwise))
        .all(|p| matches!(p, Position::Constant(None | Some(0 | 3))));
    let (millis, nulls) = if scalar_precisions {
        let len = values.len();
        let mut remaining = BooleanBuffer::new_set(len);
        let mut seconds = BooleanBuffer::new_unset(len);
        let mut milliseconds = BooleanBuffer::new_unset(len);
        for (condition, precision) in &branches {
            let condition = match condition {
                Predicate::Constant(true) => BooleanBuffer::new_set(len),
                Predicate::Constant(false) => continue,
                Predicate::Column(array) => match array.nulls() {
                    Some(nulls) => array.values() & nulls.inner(),
                    None => array.values().clone(),
                },
            };
            let selected = &remaining & &condition;
            match precision {
                Position::Constant(Some(0)) => seconds |= &selected,
                Position::Constant(Some(3)) => milliseconds |= &selected,
                _ => {}
            }
            remaining &= &!&condition;
        }
        match otherwise {
            Position::Constant(Some(0)) => seconds |= &remaining,
            Position::Constant(Some(3)) => milliseconds |= &remaining,
            _ => {}
        }
        let has_seconds = seconds.count_set_bits() != 0;
        if has_seconds {
            seconds &= &BooleanBuffer::collect_bool(len, |row| {
                (-62_167_219_200..=253_402_300_799).contains(&values.value(row))
            });
        }
        if milliseconds.count_set_bits() != 0 {
            milliseconds &= &BooleanBuffer::collect_bool(len, |row| {
                (-62_167_219_200_000..=253_402_300_799_999).contains(&values.value(row))
            });
        }
        let millis = if has_seconds {
            let mut scaled = values.values().to_vec();
            for row in seconds.set_indices() {
                scaled[row] *= 1000;
            }
            scaled.into()
        } else {
            // Millisecond-or-NULL selection changes parent validity, not the immutable values.
            values.values().clone()
        };
        let range = NullBuffer::new(&seconds | &milliseconds);
        (
            millis,
            NullBuffer::union(values.nulls(), Some(&range)).filter(|nulls| nulls.null_count() != 0),
        )
    } else {
        let mut millis = Vec::with_capacity(values.len());
        let mut nulls = NullBufferBuilder::new(values.len());
        for row in 0..values.len() {
            let value = values.value(row);
            let valid = values.is_valid(row);
            let (scaled, in_range) = match pick(row) {
                Some(0) => (
                    value.wrapping_mul(1000),
                    (-62_167_219_200..=253_402_300_799).contains(&value),
                ),
                Some(3) => (
                    value,
                    (-62_167_219_200_000..=253_402_300_799_999).contains(&value),
                ),
                None => (0, false),
                _ if !valid => (0, false),
                _ => return exec_err!("Epoch precision must be 0 or 3"),
            };
            millis.push(scaled);
            nulls.append(valid && in_range);
        }
        (millis.into(), nulls.finish())
    };
    let DataType::Struct(fields) = timestamp_type() else {
        unreachable!()
    };
    Ok(Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(Int64Array::new(millis, None)),
            Arc::new(Int32Array::from(vec![0; values.len()])),
        ],
        nulls,
    )))
}

fn utc_date(millis: i64) -> i32 {
    flink_date(millis.div_euclid(86_400_000))
}

fn flink_date(days: i64) -> i32 {
    if days >= -719_528 {
        return days as i32;
    }
    // Flink converts LocalDate through signed-int Julian arithmetic. Negative adjusted
    // Julian years use truncated divisions rather than Gregorian floor division.
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400 + 4800;
    let correction = if year < 0 {
        i32::from(year % 4 != 0) - i32::from(year % 100 != 0) + i32::from(year % 400 != 0)
    } else {
        0
    };
    (days as i32).wrapping_add(correction)
}

fn time_factor(value: &ColumnarValue) -> Result<i32> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(factor)))
            if matches!(factor, 1 | 10 | 100 | 1000) =>
        {
            Ok(*factor)
        }
        _ => exec_err!("TIME requires a constant precision factor"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Date32Array, Time32MillisecondArray};
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use streamfusion_bridge::timestamp::{timestamp_array, TimestampColumn, TimestampValue};

    fn evaluate(operation: Operation, args: Vec<ColumnarValue>, rows: usize) -> ArrayRef {
        let function = function(operation);
        let data_type = function
            .return_type(
                &args
                    .iter()
                    .map(ColumnarValue::data_type)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        function
            .invoke_with_args(ScalarFunctionArgs {
                args,
                number_rows: rows,
                return_field: Arc::new(Field::new("result", data_type, true)),
                arg_fields: vec![],
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap()
            .into_array(rows)
            .unwrap()
    }

    #[test]
    fn epoch_millis_obey_released_sql_bounds_and_sliced_nulls() {
        let values = vec![
            Some(0),
            Some(i64::MIN),
            Some(-62_167_219_200_001),
            Some(-62_167_219_200_000),
            Some(-1),
            None,
            Some(253_402_300_799_999),
            Some(253_402_300_800_000),
            Some(i64::MAX),
        ];
        let input = Int64Array::from(values.clone()).slice(1, values.len() - 1);
        let output = evaluate(
            Operation::EpochMillis,
            vec![ColumnarValue::Array(Arc::new(input))],
            values.len() - 1,
        );
        let actual = TimestampColumn::try_new(output.as_ref()).unwrap();
        for (row, value) in values.into_iter().skip(1).enumerate() {
            let expected =
                value.filter(|value| (-62_167_219_200_000..=253_402_300_799_999).contains(value));
            assert_eq!(actual.is_null(row), expected.is_none());
            if let Some(value) = expected {
                assert_eq!(
                    actual.value(row).unwrap(),
                    TimestampValue::from_millis(value)
                );
            }
        }
        output.to_data().validate_full().unwrap();
    }

    #[test]
    fn epoch_millis_share_sliced_values_and_keep_nulls_on_the_parent() {
        let input =
            Int64Array::from(vec![Some(7), None, Some(-1), Some(i64::MAX), Some(1)]).slice(1, 4);
        let pointer = input.values().as_ptr();
        let output = evaluate(
            Operation::EpochMillis,
            vec![ColumnarValue::Array(Arc::new(input))],
            4,
        );
        let components = output.as_any().downcast_ref::<StructArray>().unwrap();
        let millis = components
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(pointer, millis.values().as_ptr());
        assert_eq!(millis.null_count(), 0);
        assert_eq!(components.column(1).null_count(), 0);
        let timestamp = TimestampColumn::try_new(output.as_ref()).unwrap();
        assert!(timestamp.is_null(0));
        assert_eq!(timestamp.value(1).unwrap(), TimestampValue::from_millis(-1));
        assert!(timestamp.is_null(2));
        assert_eq!(timestamp.value(3).unwrap(), TimestampValue::from_millis(1));
        output.to_data().validate_full().unwrap();
    }

    #[test]
    fn epoch_millis_keep_scalar_and_empty_results() {
        for value in [
            None,
            Some(-62_167_219_200_001),
            Some(-62_167_219_200_000),
            Some(-1),
            Some(253_402_300_799_999),
            Some(253_402_300_800_000),
        ] {
            let output = evaluate(
                Operation::EpochMillis,
                vec![ColumnarValue::Scalar(ScalarValue::Int64(value))],
                1,
            );
            let timestamp = TimestampColumn::try_new(output.as_ref()).unwrap();
            let expected =
                value.filter(|value| (-62_167_219_200_000..=253_402_300_799_999).contains(value));
            assert_eq!(timestamp.is_null(0), expected.is_none());
            if let Some(value) = expected {
                assert_eq!(
                    timestamp.value(0).unwrap(),
                    TimestampValue::from_millis(value)
                );
            }
            output.to_data().validate_full().unwrap();
        }
        let output = evaluate(
            Operation::EpochMillis,
            vec![ColumnarValue::Array(Arc::new(Int64Array::from(
                Vec::<i64>::new(),
            )))],
            0,
        );
        assert_eq!(output.len(), 0);
        output.to_data().validate_full().unwrap();
    }

    #[test]
    fn utc_casts_floor_negative_dates_and_apply_the_released_time_factor() {
        let values = [
            Some(-1),
            None,
            Some(86_400_001),
            Some(i64::MIN),
            Some(i64::MAX),
        ];
        let input: ArrayRef = Arc::new(timestamp_array(
            values.map(|value| value.map(TimestampValue::from_millis)),
        ));
        let dates = evaluate(
            Operation::UtcDate,
            vec![ColumnarValue::Array(input.clone())],
            values.len(),
        );
        assert_eq!(
            dates.as_ref(),
            &Date32Array::from(vec![
                Some(-1),
                None,
                Some(1),
                Some(622_191_233),
                Some(-622_191_233)
            ])
        );
        for factor in [1, 10, 100, 1000] {
            let times = evaluate(
                Operation::UtcTime,
                vec![
                    ColumnarValue::Array(input.clone()),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(factor))),
                ],
                values.len(),
            );
            assert_eq!(
                times.as_ref(),
                &Time32MillisecondArray::from_iter(values.map(|value| {
                    value.map(|value| value.rem_euclid(86_400_000) as i32 / factor * factor)
                }))
            );
            times.to_data().validate_full().unwrap();
        }
    }

    #[test]
    fn epoch_seconds_mask_before_scaling_and_keep_nonnull_components() {
        let values = [
            Some(7),
            None,
            Some(i64::MIN),
            Some(i64::MAX),
            Some(i64::MIN / 1000 - 1),
            Some(i64::MAX / 1000 + 1),
            Some(-62_167_219_201),
            Some(-62_167_219_200),
            Some(-1),
            Some(0),
            Some(253_402_300_799),
            Some(253_402_300_800),
        ];
        let input = Int64Array::from(values.to_vec()).slice(1, values.len() - 1);
        let output = evaluate(
            Operation::EpochSeconds,
            vec![ColumnarValue::Array(Arc::new(input.clone()))],
            input.len(),
        );
        let timestamp = TimestampColumn::try_new(output.as_ref()).unwrap();
        for (row, value) in values.into_iter().skip(1).enumerate() {
            let expected = value.filter(|v| (-62_167_219_200..=253_402_300_799).contains(v));
            assert_eq!(timestamp.is_null(row), expected.is_none());
            if let Some(value) = expected {
                assert_eq!(
                    timestamp.value(row).unwrap(),
                    TimestampValue::from_millis(value * 1000)
                );
            }
        }
        let components = output.as_any().downcast_ref::<StructArray>().unwrap();
        for child in components.columns() {
            assert_eq!(child.null_count(), 0);
        }
        assert_eq!(input.value(1), i64::MIN);
        output.to_data().validate_full().unwrap();
    }

    #[test]
    fn epoch_seconds_keep_scalar_null_and_empty_results() {
        for value in [
            None,
            Some(-62_167_219_201),
            Some(-62_167_219_200),
            Some(-1),
            Some(0),
            Some(253_402_300_799),
            Some(253_402_300_800),
        ] {
            let output = evaluate(
                Operation::EpochSeconds,
                vec![ColumnarValue::Scalar(ScalarValue::Int64(value))],
                1,
            );
            let timestamp = TimestampColumn::try_new(output.as_ref()).unwrap();
            let expected = value.filter(|v| (-62_167_219_200..=253_402_300_799).contains(v));
            assert_eq!(timestamp.is_null(0), expected.is_none());
            if let Some(value) = expected {
                assert_eq!(
                    timestamp.value(0).unwrap(),
                    TimestampValue::from_millis(value * 1000)
                );
            }
            output.to_data().validate_full().unwrap();
        }
        for input in [
            Int64Array::from(Vec::<i64>::new()),
            Int64Array::from(vec![None; 3]),
        ] {
            let output = evaluate(
                Operation::EpochSeconds,
                vec![ColumnarValue::Array(Arc::new(input.clone()))],
                input.len(),
            );
            assert_eq!(output.len(), input.len());
            assert_eq!(output.null_count(), input.len());
            output.to_data().validate_full().unwrap();
        }
    }

    #[test]
    fn runtime_epoch_precision_keeps_original_unit_bounds_and_sliced_validity() {
        let epochs = [
            None,
            Some(i64::MIN),
            Some(i64::MAX),
            Some(i64::MIN / 1000 - 1),
            Some(i64::MAX / 1000 + 1),
            Some(-62_167_219_201),
            Some(-62_167_219_200),
            Some(253_402_300_799),
            Some(253_402_300_800),
            Some(-62_167_219_200_001),
            Some(-62_167_219_200_000),
            Some(253_402_300_799_999),
            Some(253_402_300_800_000),
            Some(-1),
            Some(0),
            Some(1),
        ];
        let values = Int64Array::from_iter(epochs.into_iter().flat_map(|v| [v; 3]));
        let precision =
            Int32Array::from_iter(epochs.into_iter().flat_map(|_| [Some(0), Some(3), None]));
        for offset in [0, 1, 5, values.len()] {
            let len = values.len() - offset;
            let input = values.slice(offset, len);
            let units = precision.slice(offset, len);
            let output = evaluate(
                Operation::EpochPrecision,
                vec![
                    ColumnarValue::Array(Arc::new(input.clone())),
                    ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
                    ColumnarValue::Array(Arc::new(units.clone())),
                    ColumnarValue::Scalar(ScalarValue::Int32(None)),
                ],
                len,
            );
            let timestamp = TimestampColumn::try_new(output.as_ref()).unwrap();
            for row in 0..len {
                let expected = if input.is_null(row) || units.is_null(row) {
                    None
                } else {
                    let value = input.value(row);
                    if units.value(row) == 0 {
                        (-62_167_219_200..=253_402_300_799)
                            .contains(&value)
                            .then(|| value * 1000)
                    } else {
                        (-62_167_219_200_000..=253_402_300_799_999)
                            .contains(&value)
                            .then_some(value)
                    }
                };
                assert_eq!(timestamp.is_null(row), expected.is_none());
                if let Some(value) = expected {
                    assert_eq!(
                        timestamp.value(row).unwrap(),
                        TimestampValue::from_millis(value)
                    );
                }
            }
            let components = output.as_any().downcast_ref::<StructArray>().unwrap();
            assert!(components
                .columns()
                .iter()
                .all(|child| child.null_count() == 0));
            output.to_data().validate_full().unwrap();
            assert_eq!(input.to_data(), values.slice(offset, len).to_data());
            assert_eq!(units.to_data(), precision.slice(offset, len).to_data());
        }
    }

    #[test]
    fn runtime_epoch_precision_accepts_scalar_and_array_operands() {
        for precision in [None, Some(0), Some(3)] {
            for value in [None, Some(-1), Some(i64::MIN), Some(i64::MAX)] {
                for scalar_value in [false, true] {
                    for scalar_precision in [false, true] {
                        let values = ColumnarValue::Scalar(ScalarValue::Int64(value));
                        let units = ColumnarValue::Scalar(ScalarValue::Int32(precision));
                        let output = evaluate(
                            Operation::EpochPrecision,
                            vec![
                                if scalar_value {
                                    values
                                } else {
                                    ColumnarValue::Array(values.into_array(3).unwrap())
                                },
                                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
                                if scalar_precision {
                                    units
                                } else {
                                    ColumnarValue::Array(units.into_array(3).unwrap())
                                },
                                ColumnarValue::Scalar(ScalarValue::Int32(None)),
                            ],
                            3,
                        );
                        let timestamp = TimestampColumn::try_new(output.as_ref()).unwrap();
                        let expected = value.zip(precision).and_then(|(v, p)| {
                            if p == 0 {
                                (-62_167_219_200..=253_402_300_799)
                                    .contains(&v)
                                    .then(|| v * 1000)
                            } else {
                                (-62_167_219_200_000..=253_402_300_799_999)
                                    .contains(&v)
                                    .then_some(v)
                            }
                        });
                        for row in 0..3 {
                            assert_eq!(timestamp.is_null(row), expected.is_none());
                            if let Some(v) = expected {
                                assert_eq!(
                                    timestamp.value(row).unwrap(),
                                    TimestampValue::from_millis(v)
                                );
                            }
                        }
                        output.to_data().validate_full().unwrap();
                    }
                }
            }
        }
    }

    #[test]
    fn runtime_epoch_precision_rejects_unproven_units_and_mismatched_lengths() {
        assert!(epoch_timestamp_with_precision(
            &Int64Array::from(vec![1]),
            &[
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                ColumnarValue::Scalar(ScalarValue::Int32(None))
            ]
        )
        .is_err());
        assert!(epoch_timestamp_with_precision(
            &Int64Array::from(vec![1]),
            &[
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
                ColumnarValue::Array(Arc::new(Int32Array::from(vec![0, 3]))),
                ColumnarValue::Scalar(ScalarValue::Int32(None))
            ]
        )
        .is_err());
        assert!(function(Operation::EpochPrecision)
            .return_type(&[DataType::Int64])
            .is_err());
        assert!(function(Operation::EpochPrecision)
            .return_type(&[DataType::Int64, DataType::Int64])
            .is_err());
    }

    #[test]
    fn runtime_millis_or_null_shares_sliced_values_and_owns_validity() {
        use arrow::array::BooleanArray;
        let input =
            Int64Array::from(vec![Some(7), None, Some(-1), Some(i64::MAX), Some(1)]).slice(1, 4);
        let pointer = input.values().as_ptr();
        let conditions =
            BooleanArray::from(vec![Some(false), Some(true), None, Some(false), Some(true)])
                .slice(1, 4);
        let output = evaluate(
            Operation::EpochPrecision,
            vec![
                ColumnarValue::Array(Arc::new(input.clone())),
                ColumnarValue::Array(Arc::new(conditions)),
                ColumnarValue::Scalar(ScalarValue::Int32(None)),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(3))),
            ],
            4,
        );
        assert_eq!(
            input,
            Int64Array::from(vec![None, Some(-1), Some(i64::MAX), Some(1)])
        );
        drop(input);
        let components = output.as_any().downcast_ref::<StructArray>().unwrap();
        let millis = components
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(pointer, millis.values().as_ptr());
        assert_eq!(millis.null_count(), 0);
        let actual = TimestampColumn::try_new(output.as_ref()).unwrap();
        assert!(actual.is_null(0));
        assert_eq!(actual.value(1).unwrap(), TimestampValue::from_millis(-1));
        assert!(actual.is_null(2));
        assert!(actual.is_null(3));
        output.to_data().validate_full().unwrap();
    }

    #[test]
    fn runtime_epoch_precision_selects_first_true_and_treats_null_conditions_as_false() {
        use arrow::array::BooleanArray;
        let output = evaluate(
            Operation::EpochPrecision,
            vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
                ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
                    Some(true),
                    None,
                    Some(false),
                    None,
                ]))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(0))),
                ColumnarValue::Array(Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(true),
                    None,
                    Some(false),
                ]))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(3))),
                ColumnarValue::Scalar(ScalarValue::Int32(None)),
            ],
            4,
        );
        let actual = TimestampColumn::try_new(output.as_ref()).unwrap();
        assert_eq!(actual.value(0).unwrap(), TimestampValue::from_millis(-1000));
        assert_eq!(actual.value(1).unwrap(), TimestampValue::from_millis(-1));
        assert!(actual.is_null(2));
        assert!(actual.is_null(3));
        output.to_data().validate_full().unwrap();
    }

    #[test]
    fn runtime_epoch_scalar_branches_preserve_bitmap_boundaries_and_first_true_nulls() {
        use arrow::array::BooleanArray;
        let epochs = [
            None,
            Some(i64::MIN),
            Some(i64::MAX),
            Some(-62_167_219_201),
            Some(-62_167_219_200),
            Some(253_402_300_799),
            Some(253_402_300_800),
            Some(-62_167_219_200_001),
            Some(-62_167_219_200_000),
            Some(253_402_300_799_999),
            Some(253_402_300_800_000),
            Some(-1),
            Some(0),
            Some(1),
        ];
        let values = Int64Array::from_iter((0..137).map(|row| epochs[row % epochs.len()]));
        let conditions = [None, Some(false), Some(true)];
        let first = BooleanArray::from_iter((0..139).map(|row| conditions[row % 3]));
        let second = BooleanArray::from_iter((0..141).map(|row| conditions[(row / 3) % 3]));
        for units in [
            [Some(0), Some(3), None],
            [Some(3), None, Some(3)],
            [None; 3],
        ] {
            for offset in [0, 1, 5, 63, 64, 65, 136, 137] {
                let len = values.len() - offset;
                let input = values.slice(offset, len);
                let a = first.slice(offset + 1, len);
                let b = second.slice(offset + 2, len);
                let output = evaluate(
                    Operation::EpochPrecision,
                    vec![
                        ColumnarValue::Array(Arc::new(input.clone())),
                        ColumnarValue::Array(Arc::new(a.clone())),
                        ColumnarValue::Scalar(ScalarValue::Int32(units[0])),
                        ColumnarValue::Array(Arc::new(b.clone())),
                        ColumnarValue::Scalar(ScalarValue::Int32(units[1])),
                        ColumnarValue::Scalar(ScalarValue::Int32(units[2])),
                    ],
                    len,
                );
                let timestamp = TimestampColumn::try_new(output.as_ref()).unwrap();
                for row in 0..len {
                    let precision = if a.is_valid(row) && a.value(row) {
                        units[0]
                    } else if b.is_valid(row) && b.value(row) {
                        units[1]
                    } else {
                        units[2]
                    };
                    let expected = epochs[(row + offset) % epochs.len()]
                        .zip(precision)
                        .and_then(|(v, p)| {
                            if p == 0 {
                                (-62_167_219_200..=253_402_300_799)
                                    .contains(&v)
                                    .then(|| v * 1000)
                            } else {
                                (-62_167_219_200_000..=253_402_300_799_999)
                                    .contains(&v)
                                    .then_some(v)
                            }
                        });
                    assert_eq!(timestamp.is_null(row), expected.is_none());
                    if let Some(v) = expected {
                        assert_eq!(
                            timestamp.value(row).unwrap(),
                            TimestampValue::from_millis(v)
                        );
                    }
                }
                output.to_data().validate_full().unwrap();
                assert_eq!(input.to_data(), values.slice(offset, len).to_data());
            }
        }
    }

    #[test]
    fn invalid_types_and_arities_are_rejected() {
        assert!(function(Operation::EpochMillis)
            .return_type(&[DataType::Utf8])
            .is_err());
        assert!(function(Operation::EpochSeconds)
            .return_type(&[DataType::Int32])
            .is_err());
        assert!(function(Operation::UtcDate)
            .return_type(&[DataType::Int64])
            .is_err());
        assert!(function(Operation::UtcTime)
            .return_type(&[timestamp_type()])
            .is_err());
    }

    #[test]
    fn zoned_casts_apply_offsets_without_overflow_and_propagate_nulls() {
        let input: ArrayRef = Arc::new(timestamp_array([
            Some(TimestampValue::from_millis(i64::MIN)),
            Some(TimestampValue::from_millis(i64::MAX)),
            Some(TimestampValue::from_millis(-1)),
            Some(TimestampValue::from_millis(1)),
            None,
            Some(TimestampValue::from_millis(0)),
        ]));
        let offsets: ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![
            Some(3600),
            Some(-3600),
            Some(1),
            Some(-1),
            Some(0),
            None,
        ]));
        let dates = evaluate(
            Operation::ZonedDate,
            vec![
                ColumnarValue::Array(input.clone()),
                ColumnarValue::Array(offsets.clone()),
            ],
            6,
        );
        assert_eq!(
            dates.as_ref(),
            &Date32Array::from(vec![
                Some(622_191_233),
                Some(-622_191_233),
                Some(0),
                Some(-1),
                None,
                None
            ])
        );
        let times = evaluate(
            Operation::ZonedTime,
            vec![
                ColumnarValue::Array(input),
                ColumnarValue::Array(offsets),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(1000))),
            ],
            6,
        );
        let times = times
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap();
        assert_eq!(times.value(2), 0);
        assert_eq!(times.value(3), 86_399_000);
        assert!(times.is_null(4));
        assert!(times.is_null(5));
        times.to_data().validate_full().unwrap();
    }
}
