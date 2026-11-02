use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Int32Array, Int64Array, StructArray, Time32MillisecondArray,
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
    UtcDate,
    UtcTime,
    ZonedDate,
    ZonedTime,
}

pub(super) fn function(operation: Operation) -> ScalarUDF {
    ScalarUDF::new_from_impl(UnnestTemporal {
        operation,
        signature: Signature::any(
            match operation {
                Operation::ZonedTime => 3,
                Operation::UtcTime | Operation::ZonedDate => 2,
                _ => 1,
            },
            Volatility::Immutable,
        ),
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
            (Operation::EpochMillis, [DataType::Int64]) => Ok(timestamp_type()),
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
            Operation::EpochMillis => {
                let millis = as_primitive_array::<arrow::datatypes::Int64Type>(&input)?;
                let range = NullBuffer::new(BooleanBuffer::collect_bool(millis.len(), |row| {
                    (-62_167_219_200_000..=253_402_300_799_999).contains(&millis.value(row))
                }));
                let nulls = NullBuffer::union(millis.nulls(), Some(&range))
                    .filter(|nulls| nulls.null_count() != 0);
                let DataType::Struct(fields) = timestamp_type() else {
                    unreachable!()
                };
                // The parent owns row validity; its non-null component shares immutable values.
                Arc::new(StructArray::new(
                    fields,
                    vec![
                        Arc::new(Int64Array::new(millis.values().clone(), None)),
                        Arc::new(Int32Array::from(vec![0; millis.len()])),
                    ],
                    nulls,
                ))
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
    fn invalid_types_and_arities_are_rejected() {
        assert!(function(Operation::EpochMillis)
            .return_type(&[DataType::Utf8])
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
