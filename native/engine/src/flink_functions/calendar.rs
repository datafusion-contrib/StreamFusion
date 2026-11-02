use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Date32Type, Int64Type, Time32MillisecondType, TimeUnit};
use datafusion::common::{cast::as_primitive_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) enum Field {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Quarter,
    Week,
    DayOfYear,
    DayOfWeek,
}

pub(crate) fn function(field: Field) -> ScalarUDF {
    ScalarUDF::new_from_impl(CalendarField {
        field,
        offsets: None,
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CalendarField {
    field: Field,
    offsets: Option<(i64, Vec<(i64, i64)>)>,
    signature: Signature,
}

pub(crate) fn zoned(encoded: &str) -> Result<ScalarUDF> {
    let (unit, history) = encoded.split_once('|').ok_or_else(|| {
        datafusion::common::DataFusionError::Plan("Invalid calendar zone history".into())
    })?;
    let field = match unit {
        "YEAR" => Field::Year,
        "MONTH" => Field::Month,
        "DAY" => Field::Day,
        "HOUR" => Field::Hour,
        "MINUTE" => Field::Minute,
        "SECOND" => Field::Second,
        "QUARTER" => Field::Quarter,
        "WEEK" => Field::Week,
        "DOY" => Field::DayOfYear,
        "DOW" => Field::DayOfWeek,
        _ => return exec_err!("Unsupported calendar unit {unit}"),
    };
    Ok(ScalarUDF::new_from_impl(CalendarField {
        field,
        offsets: Some(super::from_unixtime::decode_offsets(history)?),
        signature: Signature::any(1, Volatility::Immutable),
    }))
}

impl ScalarUDFImpl for CalendarField {
    fn name(&self) -> &str {
        match self.field {
            Field::Year => "flink_year",
            Field::Month => "flink_month",
            Field::Day => "flink_day",
            Field::Hour => "flink_hour",
            Field::Minute => "flink_minute",
            Field::Second => "flink_second",
            Field::Quarter => "flink_quarter",
            Field::Week => "flink_week",
            Field::DayOfYear => "flink_dayofyear",
            Field::DayOfWeek => "flink_dayofweek",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [data_type]
                if *data_type == DataType::Date32
                    || *data_type == DataType::Time32(TimeUnit::Millisecond)
                    || streamfusion_bridge::timestamp::is_timestamp(data_type) =>
            {
                Ok(DataType::Int64)
            }
            _ => exec_err!("Calendar field expects one DATE or plain TIMESTAMP"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(
            |arrays: &[ArrayRef]| {
                let [input] = arrays else {
                    return exec_err!("Calendar field expects one argument");
                };
                let output = if streamfusion_bridge::timestamp::is_timestamp(input.data_type()) {
                    // Flink divides epoch milliseconds towards zero when extracting calendar fields.
                    // A timestamp just before 1970-01-01 therefore uses that day's calendar fields.
                    super::map_timestamp_millis(input, |value| {
                        let offset = self.offsets.as_ref().map_or(0, |(initial, transitions)| {
                            let index = transitions.partition_point(|(millis, _)| *millis <= value);
                            if index == 0 {
                                *initial
                            } else {
                                transitions[index - 1].1
                            }
                        });
                        extract_millis(value.wrapping_add(offset), self.field)
                    })?
                } else if input.data_type() == &DataType::Time32(TimeUnit::Millisecond) {
                    as_primitive_array::<Time32MillisecondType>(input)?.unary::<_, Int64Type>(
                        |millis| extract_millis(i64::from(millis), self.field),
                    )
                } else {
                    as_primitive_array::<Date32Type>(input)?
                        .unary::<_, Int64Type>(|days| i64::from(extract(days, self.field)))
                };
                Ok(Arc::new(output) as ArrayRef)
            },
            vec![],
        )(&args.args)
    }
}

// DateTimeUtils.julianExtract uses integer arithmetic even outside chrono's year range.
// Preserve its signed division and Java int overflow for the full DATE range.
fn extract(days: i32, field: Field) -> i32 {
    let julian = days.wrapping_add(2_440_588);
    match field {
        Field::Year => calendar_date(julian).0,
        Field::Month => calendar_date(julian).1,
        Field::Day => calendar_date(julian).2,
        // Released DATE clock extraction uses the physical integer as milliseconds.
        Field::Hour => extract_millis(i64::from(days), field) as i32,
        Field::Minute | Field::Second => 0,
        Field::DayOfWeek => julian.wrapping_add(1).rem_euclid(7) + 1,
        Field::Quarter => (calendar_date(julian).1 + 2) / 3,
        Field::Week => {
            let (year, month, day) = calendar_date(julian);
            iso_week(julian, year, month, day)
        }
        Field::DayOfYear => julian
            .wrapping_sub(january_first(calendar_date(julian).0))
            .wrapping_add(1),
    }
}

fn extract_millis(millis: i64, field: Field) -> i64 {
    match field {
        // DateTimeUtils uses signed remainder, including timestamps before the epoch.
        Field::Hour => (millis % 86_400_000) / 3_600_000,
        Field::Minute => (millis % 3_600_000) / 60_000,
        Field::Second => (millis % 60_000) / 1_000,
        _ => i64::from(extract((millis / 86_400_000) as i32, field)),
    }
}

pub(super) fn round_date(days: i32, year_unit: bool, ceiling: bool) -> i32 {
    let julian = days.wrapping_add(2_440_588);
    let (b, c) = if julian > 2_299_160 {
        let a = julian.wrapping_add(32_044);
        let b = a.wrapping_mul(4).wrapping_add(3) / 146_097;
        (b, a.wrapping_sub(b.wrapping_mul(146_097) / 4))
    } else {
        (0, julian.wrapping_add(32_082))
    };
    let d = c.wrapping_mul(4).wrapping_add(3) / 1461;
    let e = c.wrapping_sub(1461_i32.wrapping_mul(d) / 4);
    let m = e.wrapping_mul(5).wrapping_add(2) / 153;
    let day = e
        .wrapping_sub(m.wrapping_mul(153).wrapping_add(2) / 5)
        .wrapping_add(1);
    let mut month = m.wrapping_add(3).wrapping_sub(12_i32.wrapping_mul(m / 10));
    let mut year = b
        .wrapping_mul(100)
        .wrapping_add(d)
        .wrapping_sub(4800)
        .wrapping_add(m / 10);
    if year_unit {
        if ceiling && (month > 1 || day > 1) {
            year = year.wrapping_add(1);
        }
        month = 1;
    } else if ceiling && day > 1 {
        month = month.wrapping_add(1);
    }
    let a = (14 - i64::from(month)) / 12;
    let y = i64::from(year) + 4800 - a;
    let m = i64::from(month) + 12 * a - 3;
    let julian = (1 + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045) as i32;
    julian.wrapping_sub(2_440_588)
}

fn calendar_date(julian: i32) -> (i32, i32, i32) {
    let j = julian.wrapping_add(32_044);
    let g = j / 146_097;
    let dg = j % 146_097;
    let c = (dg / 36_524 + 1) * 3 / 4;
    let dc = dg - c * 36_524;
    let b = dc / 1461;
    let db = dc % 1461;
    let a = (db / 365 + 1) * 3 / 4;
    let da = db - a * 365;
    let y = g * 400 + c * 100 + b * 4 + a;
    let m = (da * 5 + 308) / 153 - 2;
    let day = da - (m + 4) * 153 / 5 + 123;
    let year = y - 4800 + (m + 2) / 12;
    let month = (m + 2) % 12 + 1;
    (year, month, day)
}

/// Flink DateTimeUtils.addMonths, including its Java integer overflow and month-end clamping.
pub(super) fn add_months(timestamp: i64, months: i32) -> i64 {
    const DAY: i64 = 86_400_000;
    let time = timestamp.rem_euclid(DAY);
    let days = (timestamp.wrapping_sub(time) / DAY) as i32;
    let (mut year, month, mut day) = calendar_date(days.wrapping_add(2_440_588));
    let month = month.wrapping_add(months);
    year += month.div_euclid(12);
    let mut month = month.rem_euclid(12);
    if month == 0 {
        year -= 1;
        month = 12;
    }
    let last_day = match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    day = day.min(last_day);
    let a = (14 - month) / 12;
    let y = i64::from(year) + 4800 - i64::from(a);
    let m = i64::from(month + 12 * a - 3);
    let julian =
        (i64::from(day) + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32_045) as i32;
    i64::from(julian.wrapping_sub(2_440_588)) * DAY + time
}

fn january_first(year: i32) -> i32 {
    let y = i64::from(year) + 4799;
    (1 + (153 * 10 + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32_045) as i32
}

fn first_monday(year: i32) -> i64 {
    let first = i64::from(january_first(year));
    first + (11 - (first + 1).rem_euclid(7)) % 7 - 3
}

fn iso_week(julian: i32, year: i32, month: i32, day: i32) -> i32 {
    let weekday = julian.rem_euclid(7) + 1;
    let first = if month == 12 && day > 28 {
        if !(31 - day + 4 > 7 - weekday && 31 - day + weekday >= 4) {
            return 1;
        }
        first_monday(year)
    } else if month == 1 && day < 5 {
        if 4 - day <= 7 - weekday && day - weekday >= -3 {
            return 1;
        }
        first_monday(year - 1)
    } else {
        first_monday(year)
    };
    (i64::from(julian) - first) as i32 / 7 + 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Date32Array, Int64Array};
    use arrow::datatypes::Field as ArrowField;
    use datafusion::config::ConfigOptions;

    fn assert_date_clock_field(field: Field, expected: Vec<Option<i64>>) {
        use datafusion::common::ScalarValue;
        let values = vec![
            Some(i32::MIN + 1),
            Some(-719162),
            Some(-60000),
            Some(-1000),
            None,
            Some(0),
            Some(19753),
            Some(i32::MAX),
        ];
        let invoke = |input, rows| {
            function(field)
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![input],
                    number_rows: rows,
                    return_field: Arc::new(ArrowField::new("v", DataType::Int64, true)),
                    config_options: Arc::new(ConfigOptions::default()),
                    arg_fields: vec![],
                })
                .unwrap()
        };
        for (value, expected) in values.iter().zip(&expected) {
            let ColumnarValue::Scalar(output) =
                invoke(ColumnarValue::Scalar(ScalarValue::Date32(*value)), 1024)
            else {
                panic!("expected scalar");
            };
            assert_eq!(output, ScalarValue::Int64(*expected));
        }
        let input = Date32Array::from([vec![Some(42)], values, vec![Some(43)]].concat())
            .slice(1, expected.len());
        let output = invoke(ColumnarValue::Array(Arc::new(input)), expected.len())
            .into_array(expected.len())
            .unwrap();
        output.to_data().validate_full().unwrap();
        assert_eq!(output.as_ref(), &Int64Array::from(expected));
        let output = invoke(
            ColumnarValue::Array(Arc::new(Date32Array::from(Vec::<Option<i32>>::new()))),
            0,
        )
        .into_array(0)
        .unwrap();
        assert_eq!(output.len(), 0);
        assert_eq!(output.data_type(), &DataType::Int64);
    }

    #[test]
    fn date_hour_keeps_released_signed_physical_integer_arithmetic() {
        assert_date_clock_field(
            Field::Hour,
            vec![
                Some(-20),
                Some(0),
                Some(0),
                Some(0),
                None,
                Some(0),
                Some(0),
                Some(20),
            ],
        );
    }

    #[test]
    fn fields_keep_iso_week_sunday_origin_nulls_and_sliced_offsets() {
        let input = Date32Array::from(vec![Some(0), Some(18628), None]).slice(1, 2);
        for (field, expected) in [
            (Field::Quarter, 1),
            (Field::Week, 53),
            (Field::DayOfYear, 1),
            (Field::DayOfWeek, 6),
        ] {
            let output = function(field)
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(Arc::new(input.clone()))],
                    number_rows: 2,
                    return_field: Arc::new(ArrowField::new("v", DataType::Int64, true)),
                    config_options: Arc::new(ConfigOptions::default()),
                    arg_fields: vec![],
                })
                .unwrap()
                .into_array(2)
                .unwrap();
            let output = output.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(output.value(0), expected);
            assert!(output.is_null(1));
        }
    }

    #[test]
    fn extraction_uses_flink_signed_arithmetic_and_full_date_range() {
        for (field, expected) in [(Field::Year, 1970), (Field::Month, 1), (Field::Day, 1)] {
            assert_eq!(extract_millis(-1, field), expected);
        }
        assert_eq!(extract_millis(-3_661_999, Field::Hour), -1);
        assert_eq!(extract_millis(-3_661_999, Field::Minute), -1);
        assert_eq!(extract_millis(-3_661_999, Field::Second), -1);
        for days in [i32::MIN, i32::MAX, -719162, 2932896] {
            for field in [Field::Year, Field::Month, Field::Day] {
                let _ = extract(days, field);
            }
        }
    }
}
