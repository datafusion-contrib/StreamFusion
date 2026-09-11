use arrow::array::{Array, ArrayRef, Date32Array};
use arrow::datatypes::DataType;
use chrono::{Datelike, NaiveDate};
use datafusion::common::{cast::as_string_array, exec_err, Result};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

pub(crate) fn function() -> ScalarUDF {
    super::udf(
        "flink_to_date",
        vec![DataType::Utf8],
        DataType::Date32,
        to_date,
    )
}

fn to_date(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("TO_DATE expects one argument");
    };
    let input = as_string_array(input)?;
    let mut output = Date32Array::builder(input.len());
    for value in input.iter() {
        output.append_option(value.map(parse).transpose()?.flatten());
    }
    Ok(Arc::new(output.finish()))
}

fn parse(value: &str) -> Result<Option<i32>> {
    // Flink discards a timestamp suffix at the first ASCII space, before trimming fields.
    let value = match value.find(' ') {
        Some(index) if index > 0 => &value[..index],
        _ => value,
    };
    let mut parts = value.splitn(3, '-');
    let mut fields = [0, 1, 1];
    for (index, part) in parts.by_ref().enumerate() {
        let part = part.trim_matches(|ch| ch <= '\u{20}');
        if part.is_empty() || !part.bytes().all(|ch| ch.is_ascii_digit()) {
            return Ok(None);
        }
        fields[index] = part.parse::<i32>().map_err(|_| {
            datafusion::common::exec_datafusion_err!("TO_DATE numeric field exceeds INTEGER range")
        })?;
    }
    let [year, month, day] = fields;
    if !(0..=9999).contains(&year) {
        return Ok(None);
    }
    Ok(NaiveDate::from_ymd_opt(year, month as u32, day as u32)
        .map(|date| date.num_days_from_ce() - 719163))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_date_keeps_flink_partial_dates_and_error_envelope() {
        assert_eq!(parse("1970").unwrap(), Some(0));
        assert_eq!(parse("1970-1-2 ignored").unwrap(), Some(1));
        assert_eq!(parse("1970-02-30").unwrap(), None);
        assert_eq!(parse(" 1970-1-2 ").unwrap(), Some(1));
        assert_eq!(parse("1970- 1-2").unwrap(), None);
        assert_eq!(parse("+1970-1-2").unwrap(), None);
        assert!(parse("2147483648").is_err());
        assert!(to_date(&[]).is_err());
    }
}
