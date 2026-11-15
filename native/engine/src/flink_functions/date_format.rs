//! Numeric DateTimeFormatter patterns over the full millisecond timestamp range.

use datafusion::common::{exec_err, Result};
use std::fmt::Write;

#[derive(Default)]
pub(crate) struct TimestampFormat {
    pattern: Option<String>,
    segments: Vec<Segment>,
}

enum Segment {
    Literal(String),
    Field(char),
}

impl TimestampFormat {
    pub(crate) fn format_into(
        &mut self,
        output: &mut String,
        millis: i64,
        pattern: &str,
    ) -> Result<()> {
        if self.pattern.as_deref() != Some(pattern) {
            self.segments = compile(pattern)?;
            self.pattern = Some(pattern.to_owned());
        }
        let (year, month, day) = calendar_date(millis.div_euclid(86_400_000));
        let year_of_era = if year <= 0 { 1 - year } else { year } as u32;
        let time = millis.rem_euclid(86_400_000) as u32;
        output.clear();
        for segment in &self.segments {
            match segment {
                Segment::Literal(text) => output.push_str(text),
                Segment::Field('Y') => {
                    if year_of_era > 9999 {
                        output.push('+');
                        write!(output, "{year_of_era}").unwrap();
                    } else {
                        push2(output, year_of_era / 100);
                        push2(output, year_of_era % 100);
                    }
                }
                Segment::Field(field) => push2(
                    output,
                    match field {
                        'y' => year_of_era % 100,
                        'm' => month as u32,
                        'd' => day as u32,
                        'H' => time / 3_600_000,
                        'M' => time / 60_000 % 60,
                        'S' => time / 1000 % 60,
                        _ => unreachable!("validated numeric field"),
                    },
                ),
            }
        }
        Ok(())
    }
}

fn compile(pattern: &str) -> Result<Vec<Segment>> {
    let mut chars = pattern.chars();
    let mut literal = String::new();
    let mut segments = Vec::new();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            literal.push(ch);
            continue;
        }
        match chars.next() {
            Some('%') => literal.push('%'),
            Some(field @ ('Y' | 'y' | 'm' | 'd' | 'H' | 'M' | 'S')) => {
                if !literal.is_empty() {
                    segments.push(Segment::Literal(std::mem::take(&mut literal)));
                }
                segments.push(Segment::Field(field));
            }
            _ => return exec_err!("Unsupported numeric DATE_FORMAT pattern {pattern}"),
        }
    }
    if !literal.is_empty() {
        segments.push(Segment::Literal(literal));
    }
    Ok(segments)
}

fn push2(output: &mut String, value: u32) {
    debug_assert!(value < 100);
    output.push((b'0' + (value / 10) as u8) as char);
    output.push((b'0' + (value % 10) as u8) as char);
}

fn calendar_date(days: i64) -> (i64, i64, i64) {
    // Proleptic Gregorian 400-year cycles, including negative days. DateTimeFormatter has
    // no Julian cutover, unlike Flink's SimpleDateFormat-based FROM_UNIXTIME.
    let shifted = days + 2_440_588 + 32_044;
    let century = (4 * shifted + 3).div_euclid(146_097);
    let remaining = shifted - (146_097 * century).div_euclid(4);
    let years = (4 * remaining + 3).div_euclid(1461);
    let day_of_year = remaining - (1461 * years).div_euclid(4);
    let month = (5 * day_of_year + 2) / 153;
    (
        100 * century + years - 4800 + month / 10,
        month + 3 - 12 * (month / 10),
        day_of_year - (153 * month + 2) / 5 + 1,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn proleptic_dates_match_complete_gregorian_cycles() {
        for base in [-1000000, -719528, -146097, 0, 1000000] {
            for days in base..base + 146097 {
                let date = chrono::DateTime::from_timestamp(days * 86400, 0)
                    .unwrap()
                    .date_naive();
                assert_eq!(
                    calendar_date(days),
                    (
                        i64::from(date.year()),
                        i64::from(date.month()),
                        i64::from(date.day())
                    )
                );
            }
        }
    }

    #[test]
    fn java_year_of_era_and_signed_millisecond_endpoints() {
        let mut formatter = TimestampFormat::default();
        let mut output = String::new();
        for (millis, expected) in [
            (-62167219200000, "0001-01-01 00:00:00 01"),
            (-62198755200000, "0002-01-01 00:00:00 02"),
            (253402300800000, "+10000-01-01 00:00:00 00"),
            (-1, "1969-12-31 23:59:59 69"),
            (i64::MIN, "+292275056-05-16 16:47:04 56"),
            (i64::MAX, "+292278994-08-17 07:12:55 94"),
        ] {
            formatter
                .format_into(&mut output, millis, "%Y-%m-%d %H:%M:%S %y")
                .unwrap();
            assert_eq!(output, expected, "millis={millis}");
        }
        formatter.format_into(&mut output, 0, "%% %y").unwrap();
        assert_eq!(output, "% 70");
    }
}
