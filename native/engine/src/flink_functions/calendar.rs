use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Date32Type, Int64Type};
use datafusion::common::{cast::as_primitive_array, exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) enum Field {
    Quarter,
    Week,
    DayOfYear,
    DayOfWeek,
}

pub(crate) fn function(field: Field) -> ScalarUDF {
    ScalarUDF::new_from_impl(CalendarField {
        field,
        signature: Signature::any(1, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CalendarField {
    field: Field,
    signature: Signature,
}

impl ScalarUDFImpl for CalendarField {
    fn name(&self) -> &str {
        match self.field {
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
            [DataType::Date32 | DataType::Timestamp(_, None)] => Ok(DataType::Int64),
            _ => exec_err!("Calendar field expects one DATE or plain TIMESTAMP"),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::functions::utils::make_scalar_function(
            |arrays: &[ArrayRef]| {
                let [input] = arrays else {
                    return exec_err!("Calendar field expects one argument");
                };
                let output = if matches!(input.data_type(), DataType::Timestamp(_, _)) {
                    // Flink divides epoch milliseconds towards zero when extracting calendar fields.
                    // A timestamp just before 1970-01-01 therefore uses that day's calendar fields.
                    super::map_timestamp_millis(input, |value| {
                        i64::from(extract((value / 86_400_000) as i32, self.field))
                    })?
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
}
