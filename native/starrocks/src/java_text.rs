//! Java text spellings the released StarRocks connector reaches through `java.time`,
//! `java.sql.Date` with `SimpleDateFormat`, and its BINARY-to-long fold.
//!
//! The connector formats DATE with a `SimpleDateFormat("yyyy-MM-dd")` over
//! `java.sql.Date.valueOf(LocalDate.ofEpochDay(day))`. `java.util.Date` reads the proleptic
//! Gregorian fields in the hybrid Julian/Gregorian calendar and the formatter prints them back in
//! that calendar with the year of era, so only the ten days the 1582 cutover skipped and years
//! before 1 change spelling. The planner's spelling probe covers the default time zone and locale
//! those classes read.

use streamfusion_bridge::timestamp::TimestampValue;

const DAYS_PER_CYCLE: i64 = 146_097;
const DAYS_0000_TO_1970: i64 = DAYS_PER_CYCLE * 5 - (30 * 365 + 7);
/// `GregorianCalendar`'s default cutover instant, 1582-10-15, in epoch days.
const GREGORIAN_CUTOVER_DAY: i64 = -141_427;
/// Days from the proleptic Julian 0000-03-01 to 1970-01-01.
const JULIAN_MARCH_ZERO_TO_EPOCH: i64 = 719_470;
const MILLIS_PER_DAY: i64 = 86_400_000;

/// `LocalDate.ofEpochDay`: proleptic Gregorian fields, ported from `java.time`.
pub(crate) fn local_date(epoch_day: i64) -> (i64, u32, u32) {
    let mut zero_day = epoch_day + DAYS_0000_TO_1970 - 60;
    let mut adjust = 0;
    if zero_day < 0 {
        let adjust_cycles = (zero_day + 1) / DAYS_PER_CYCLE - 1;
        adjust = adjust_cycles * 400;
        zero_day += -adjust_cycles * DAYS_PER_CYCLE;
    }
    let mut year = (400 * zero_day + 591) / DAYS_PER_CYCLE;
    let mut day_of_year = zero_day - (365 * year + year / 4 - year / 100 + year / 400);
    if day_of_year < 0 {
        year -= 1;
        day_of_year = zero_day - (365 * year + year / 4 - year / 100 + year / 400);
    }
    year += adjust;
    let march_month = (day_of_year * 5 + 2) / 153;
    let month = (march_month + 2) % 12 + 1;
    let day = day_of_year - (march_month * 306 + 5) / 10 + 1;
    year += march_month / 10;
    (year, month as u32, day as u32)
}

fn julian_epoch_day(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(4);
    let year_of_era = year - era * 4;
    let march_month = (i64::from(month) + 9) % 12;
    let day_of_year = (153 * march_month + 2) / 5 + i64::from(day) - 1;
    era * 1461 + year_of_era * 365 + day_of_year - JULIAN_MARCH_ZERO_TO_EPOCH
}

fn two_digits(value: u32, output: &mut Vec<u8>) {
    output.push(b'0' + (value / 10) as u8);
    output.push(b'0' + (value % 10) as u8);
}

fn padded(value: i64, width: usize, output: &mut Vec<u8>) {
    let mut digits = itoa::Buffer::new();
    let digits = digits.format(value).as_bytes();
    output.resize(output.len() + width.saturating_sub(digits.len()), b'0');
    output.extend_from_slice(digits);
}

/// `SimpleDateFormat("yyyy-MM-dd").format(java.sql.Date.valueOf(LocalDate.ofEpochDay(day)))`.
pub(crate) fn write_sql_date(epoch_day: i32, output: &mut Vec<u8>) {
    let (mut year, mut month, mut day) = local_date(i64::from(epoch_day));
    if i64::from(epoch_day) < GREGORIAN_CUTOVER_DAY {
        // Before the cutover the same fields name a Julian date; only the skipped days reach it.
        let julian = julian_epoch_day(year, month, day);
        if julian >= GREGORIAN_CUTOVER_DAY {
            (year, month, day) = local_date(julian);
        }
    }
    padded(if year <= 0 { 1 - year } else { year }, 4, output);
    output.push(b'-');
    two_digits(month, output);
    output.push(b'-');
    two_digits(day, output);
}

/// `LocalDate.toString()`.
fn write_local_date(year: i64, month: u32, day: u32, output: &mut Vec<u8>) {
    if year.abs() < 1000 {
        if year < 0 {
            output.push(b'-');
        }
        padded(year.abs(), 4, output);
    } else {
        if year > 9999 {
            output.push(b'+');
        }
        output.extend_from_slice(itoa::Buffer::new().format(year).as_bytes());
    }
    output.push(b'-');
    two_digits(month, output);
    output.push(b'-');
    two_digits(day, output);
}

/// `TimestampData.toLocalDateTime().toString()`, including `LocalTime`'s omission of zero
/// seconds and its millisecond, microsecond or nanosecond fraction groups.
pub(crate) fn write_local_date_time(value: TimestampValue, output: &mut Vec<u8>) {
    let millis = value.millis();
    let mut date = (millis / MILLIS_PER_DAY) as i32;
    let mut time = (millis % MILLIS_PER_DAY) as i32;
    if time < 0 {
        date -= 1;
        time += MILLIS_PER_DAY as i32;
    }
    let (year, month, day) = local_date(i64::from(date));
    write_local_date(year, month, day, output);
    output.push(b'T');
    let nano_of_day = i64::from(time) * 1_000_000 + i64::from(value.nano_of_milli());
    let second_of_day = nano_of_day / 1_000_000_000;
    let nano = (nano_of_day % 1_000_000_000) as u32;
    let hour = (second_of_day / 3600) as u32;
    let minute = ((second_of_day / 60) % 60) as u32;
    let second = (second_of_day % 60) as u32;
    two_digits(hour, output);
    output.push(b':');
    two_digits(minute, output);
    if second > 0 || nano > 0 {
        output.push(b':');
        two_digits(second, output);
        if nano > 0 {
            output.push(b'.');
            if nano.is_multiple_of(1_000_000) {
                padded(i64::from(nano / 1_000_000), 3, output);
            } else if nano.is_multiple_of(1000) {
                padded(i64::from(nano / 1000), 6, output);
            } else {
                padded(i64::from(nano), 9, output);
            }
        }
    }
}

const REPLACEMENT: &[u8] = "\u{fffd}".as_bytes();

fn continuation(byte: u8) -> bool {
    byte & 0xc0 == 0x80
}

/// How many bytes of a malformed four-byte sequence the JDK replaces with one U+FFFD.
fn malformed_four(bytes: &[u8], at: usize) -> usize {
    let (b1, b2) = (bytes[at], bytes[at + 1]);
    if b1 > 0xf4
        || b1 == 0xf0 && !(0x90..=0xbf).contains(&b2)
        || b1 == 0xf4 && b2 & 0xf0 != 0x80
        || !continuation(b2)
    {
        1
    } else if !continuation(bytes[at + 2]) {
        2
    } else {
        3
    }
}

/// UTF-8 of `new String(bytes, UTF_8)`: Flink decodes malformed string bytes with the JDK's
/// `String` decoder, whose replacement spans differ from Rust's lossy decoding (one U+FFFD for an
/// encoded surrogate, for example). Ported from JDK 17's `String.decodeUTF8_UTF16`.
pub(crate) fn write_java_utf8(bytes: &[u8], output: &mut Vec<u8>) {
    if std::str::from_utf8(bytes).is_ok() {
        output.extend_from_slice(bytes);
        return;
    }
    let length = bytes.len();
    let mut at = 0;
    while at < length {
        let b1 = bytes[at];
        at += 1;
        if b1 < 0x80 {
            output.push(b1);
        } else if b1 & 0xe0 == 0xc0 && b1 & 0x1e != 0 {
            if at < length {
                if continuation(bytes[at]) {
                    output.extend_from_slice(&bytes[at - 1..=at]);
                    at += 1;
                } else {
                    output.extend_from_slice(REPLACEMENT);
                }
                continue;
            }
            output.extend_from_slice(REPLACEMENT);
            break;
        } else if b1 & 0xf0 == 0xe0 {
            let overlong = |b2: u8| b1 == 0xe0 && b2 & 0xe0 == 0x80;
            if at + 1 < length {
                let (b2, b3) = (bytes[at], bytes[at + 1]);
                if overlong(b2) || !continuation(b2) || !continuation(b3) {
                    output.extend_from_slice(REPLACEMENT);
                    at += usize::from(!overlong(b2) && continuation(b2));
                } else {
                    let code = (u32::from(b1 & 0x0f) << 12)
                        | (u32::from(b2 & 0x3f) << 6)
                        | u32::from(b3 & 0x3f);
                    if (0xd800..=0xdfff).contains(&code) {
                        output.extend_from_slice(REPLACEMENT);
                    } else {
                        output.extend_from_slice(&bytes[at - 1..at + 2]);
                    }
                    at += 2;
                }
                continue;
            }
            output.extend_from_slice(REPLACEMENT);
            if at < length && (overlong(bytes[at]) || !continuation(bytes[at])) {
                continue;
            }
            break;
        } else if b1 & 0xf8 == 0xf0 {
            if at + 2 < length {
                let (b2, b3, b4) = (bytes[at], bytes[at + 1], bytes[at + 2]);
                let code = (u32::from(b1 & 0x07) << 18)
                    | (u32::from(b2 & 0x3f) << 12)
                    | (u32::from(b3 & 0x3f) << 6)
                    | u32::from(b4 & 0x3f);
                if !continuation(b2)
                    || !continuation(b3)
                    || !continuation(b4)
                    || !(0x1_0000..=0x10_ffff).contains(&code)
                {
                    output.extend_from_slice(REPLACEMENT);
                    at += malformed_four(bytes, at - 1) - 1;
                } else {
                    output.extend_from_slice(&bytes[at - 1..at + 3]);
                    at += 3;
                }
                continue;
            }
            output.extend_from_slice(REPLACEMENT);
            let malformed_second = at < length
                && (b1 == 0xf0 && !(0x90..=0xbf).contains(&bytes[at])
                    || b1 == 0xf4 && bytes[at] & 0xf0 != 0x80
                    || !continuation(bytes[at]));
            if b1 > 0xf4 || malformed_second {
                continue;
            }
            at += 1;
            if at < length && !continuation(bytes[at]) {
                continue;
            }
            break;
        } else {
            output.extend_from_slice(REPLACEMENT);
        }
    }
}

/// The connector's BINARY conversion: bytes folded big-endian into a `long` with Java's
/// wrapping addition and shift-distance masking.
pub(crate) fn binary_long(bytes: &[u8]) -> i64 {
    let mut value = 0i64;
    for (position, byte) in bytes.iter().rev().enumerate() {
        value = value.wrapping_add(i64::from(*byte).wrapping_shl((8 * position) as u32));
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql_date(day: i32) -> String {
        let mut output = Vec::new();
        write_sql_date(day, &mut output);
        String::from_utf8(output).unwrap()
    }

    fn date_time(millis: i64, nanos: u32) -> String {
        let mut output = Vec::new();
        write_local_date_time(TimestampValue::new(millis, nanos).unwrap(), &mut output);
        String::from_utf8(output).unwrap()
    }

    #[test]
    fn local_date_matches_java_time_across_eras() {
        assert_eq!(local_date(0), (1970, 1, 1));
        assert_eq!(local_date(-1), (1969, 12, 31));
        assert_eq!(local_date(-719_528), (0, 1, 1));
        assert_eq!(local_date(-719_529), (-1, 12, 31));
        assert_eq!(local_date(2_932_896), (9999, 12, 31));
        assert_eq!(local_date(11_016), (2000, 2, 29));
    }

    #[test]
    fn sql_dates_follow_the_hybrid_calendar_and_year_of_era() {
        assert_eq!(sql_date(0), "1970-01-01");
        assert_eq!(sql_date(2_932_896), "9999-12-31");
        assert_eq!(sql_date(2_932_897), "10000-01-01");
        assert_eq!(sql_date(GREGORIAN_CUTOVER_DAY as i32), "1582-10-15");
        assert_eq!(sql_date(GREGORIAN_CUTOVER_DAY as i32 - 1), "1582-10-24");
        assert_eq!(sql_date(GREGORIAN_CUTOVER_DAY as i32 - 10), "1582-10-15");
        assert_eq!(sql_date(GREGORIAN_CUTOVER_DAY as i32 - 11), "1582-10-04");
        assert_eq!(sql_date(-141_714), "1582-01-01");
        assert_eq!(sql_date(-141_715), "1581-12-31");
        assert_eq!(sql_date(-719_528), "0001-01-01");
        assert_eq!(sql_date(-719_529), "0002-12-31");
        assert_eq!(sql_date(-719_163), "0001-12-31");
        assert_eq!(sql_date(-719_162), "0001-01-01");
    }

    #[test]
    fn local_date_times_trim_like_local_time() {
        assert_eq!(date_time(0, 0), "1970-01-01T00:00");
        assert_eq!(date_time(1_000, 0), "1970-01-01T00:00:01");
        assert_eq!(date_time(1, 0), "1970-01-01T00:00:00.001");
        assert_eq!(date_time(1, 1_000), "1970-01-01T00:00:00.001001");
        assert_eq!(date_time(1, 1), "1970-01-01T00:00:00.001000001");
        assert_eq!(date_time(-1, 0), "1969-12-31T23:59:59.999");
        assert_eq!(date_time(-1, 999_999), "1969-12-31T23:59:59.999999999");
        assert_eq!(date_time(-62_167_219_200_000, 0), "0000-01-01T00:00");
        assert_eq!(
            date_time(-62_167_219_200_001, 0),
            "-0001-12-31T23:59:59.999"
        );
        assert_eq!(date_time(253_402_300_800_000, 0), "+10000-01-01T00:00");
    }

    fn java_utf8(bytes: &[u8]) -> String {
        let mut output = Vec::new();
        write_java_utf8(bytes, &mut output);
        String::from_utf8(output).unwrap()
    }

    #[test]
    fn malformed_text_follows_the_jdk_string_decoder() {
        assert_eq!(java_utf8(b"plain \xc3\xa9"), "plain é");
        assert_eq!(java_utf8(b"\xed\xa0\x80x"), "\u{fffd}x");
        assert_eq!(java_utf8(b"\xe0\x80\x80"), "\u{fffd}\u{fffd}\u{fffd}");
        assert_eq!(java_utf8(b"\xe2\x82"), "\u{fffd}");
        assert_eq!(java_utf8(b"\xe2\x82x"), "\u{fffd}x");
        assert_eq!(java_utf8(b"\xf0\x9f\x98"), "\u{fffd}");
        assert_eq!(java_utf8(b"\xf0\x9f\x98A"), "\u{fffd}A");
        assert_eq!(java_utf8(b"\xf4\x90\x80\x80"), "\u{fffd}".repeat(4));
        assert_eq!(java_utf8(b"\xc0\x80\xff"), "\u{fffd}".repeat(3));
    }

    #[test]
    fn binary_folds_like_java_long_arithmetic() {
        assert_eq!(binary_long(&[]), 0);
        assert_eq!(binary_long(&[1, 2]), 0x0102);
        assert_eq!(binary_long(&[0xff; 8]), -1);
        assert_eq!(binary_long(&[1, 0, 0, 0, 0, 0, 0, 0, 2]), 3);
    }
}
