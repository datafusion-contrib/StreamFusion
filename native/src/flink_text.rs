//! Flink-exact text→value parsers, shared by the ingest decoders. Flink's JSON and CSV formats both
//! funnel string-positioned values through the same converter family (`JsonToRowDataConverters` /
//! `CsvToRowDataConverters`): Java `parseLong`/`parseDouble`/`parseBoolean` over trimmed text,
//! `BigDecimal` + `DecimalData.fromBigDecimal` for decimals, and the `TimeFormats` formatters for
//! temporals. These functions reproduce those envelopes — what parses, what fails, and the produced
//! value — so a native decode accepts and rejects exactly the strings the host would. Deliberate
//! residual leniencies (a trailing 'Z' accepted on any timestamp column, Java-only exotica like hex
//! float literals and expanded ISO years rejected) are documented in
//! `divergences/21-ingest-text-envelope.md`.

/// `Boolean.parseBoolean`: case-insensitive "true" is true, anything else — including numbers and
/// garbage — is false. Never fails. The caller trims first (both Flink converters do).
pub(crate) fn parse_java_boolean(s: &str) -> bool {
    s.eq_ignore_ascii_case("true")
}

/// `Integer.parseInt`-family grammar: an optional sign followed by radix-10 digits, overflow
/// checked. Rust's integer `FromStr` matches it exactly. The caller trims first.
pub(crate) fn parse_java_integer<T: std::str::FromStr>(s: &str) -> Option<T> {
    s.parse::<T>().ok()
}

/// `Double.parseDouble` / `Float.parseFloat`: Java trims whitespace itself, accepts the exact
/// spellings `NaN`/`Infinity` (optionally signed), an optional trailing `f`/`F`/`d`/`D` suffix, and
/// decimal/exponent literals. Hex float literals (`0x1.8p1`) are not reproduced (documented).
/// Parsed directly at the target width so a float result rounds once, like `parseFloat`.
pub(crate) fn parse_java_float<T: JavaFloat>(s: &str) -> Option<T> {
    let t = s.trim();
    parse_java_float_body::<T>(t).or_else(|| {
        let (head, suffix) = t.split_at(t.len().checked_sub(1)?);
        matches!(suffix, "f" | "F" | "d" | "D")
            .then(|| parse_java_float_body::<T>(head))
            .flatten()
    })
}

pub(crate) trait JavaFloat: std::str::FromStr + Copy {
    const NAN: Self;
    const INFINITY: Self;
    const NEG_INFINITY: Self;
}
impl JavaFloat for f32 {
    const NAN: f32 = f32::NAN;
    const INFINITY: f32 = f32::INFINITY;
    const NEG_INFINITY: f32 = f32::NEG_INFINITY;
}
impl JavaFloat for f64 {
    const NAN: f64 = f64::NAN;
    const INFINITY: f64 = f64::INFINITY;
    const NEG_INFINITY: f64 = f64::NEG_INFINITY;
}

fn parse_java_float_body<T: JavaFloat>(t: &str) -> Option<T> {
    match t {
        "NaN" | "+NaN" | "-NaN" => return Some(T::NAN),
        "Infinity" | "+Infinity" => return Some(T::INFINITY),
        "-Infinity" => return Some(T::NEG_INFINITY),
        _ => {}
    }
    // Restrict to Java's decimal grammar before handing to Rust's parser: Rust would otherwise
    // accept spellings Java rejects ("inf", "nan", "infinity").
    if t.is_empty()
        || !t
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'+' | b'-' | b'.' | b'e' | b'E'))
    {
        return None;
    }
    t.parse::<T>().ok()
}

/// `DateTimeFormatter.ISO_LOCAL_DATE` (strict): exactly `yyyy-MM-dd`, zero-padded, a real calendar
/// date. Returns days since the epoch. (Java's expanded years beyond 4 digits are not reproduced.)
pub(crate) fn parse_iso_local_date(s: &str) -> Option<i32> {
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let digits = |r: std::ops::Range<usize>| -> Option<u32> {
        let mut v = 0u32;
        for &c in &b[r] {
            if !c.is_ascii_digit() {
                return None;
            }
            v = v * 10 + (c - b'0') as u32;
        }
        Some(v)
    };
    let (y, m, d) = (digits(0..4)?, digits(5..7)?, digits(8..10)?);
    days_from_epoch(y as i32, m, d, false)
}

/// `java.sql.Date.valueOf` + `toLocalDate` (Flink's CSV DATE parse): `yyyy-[m]m-[d]d` with each
/// segment read by `parseInt` (so `+2020` passes), month 1–12 and day 1–31 range-checked, and the
/// day *leniently normalized* by the legacy calendar — `2020-02-31` becomes `2020-03-02`, exactly
/// as the host.
pub(crate) fn parse_java_sql_date(s: &str) -> Option<i32> {
    let first = s.find('-').filter(|&i| i > 0)?;
    let second = s[first + 1..].find('-').map(|i| first + 1 + i)?;
    if second + 1 >= s.len() {
        return None;
    }
    let year: i32 = s[..first].parse().ok()?;
    let month: u32 = s[first + 1..second].parse().ok()?;
    let day: u32 = s[second + 1..].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    days_from_epoch(year, month, day, true)
}

/// Days since 1970-01-01 for a calendar date. `lenient` normalizes a day past the month's end into
/// the following month(s) (the legacy `java.util.Date` behavior `Date.valueOf` inherits); strict
/// rejects it (the ISO resolver).
fn days_from_epoch(year: i32, month: u32, day: u32, lenient: bool) -> Option<i32> {
    let date = if lenient {
        chrono::NaiveDate::from_ymd_opt(year, month, 1)?
            .checked_add_days(chrono::Days::new(day as u64 - 1))?
    } else {
        chrono::NaiveDate::from_ymd_opt(year, month, day)?
    };
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    i32::try_from(date.signed_duration_since(epoch).num_days()).ok()
}

/// Whether a `timestamp-format.standard` string parse follows Flink's SQL or ISO-8601 formatter.
#[derive(Clone, Copy, PartialEq, Default)]
pub(crate) enum TimestampMode {
    /// `TimeFormats.SQL_TIMESTAMP_FORMAT`: `yyyy-MM-dd HH:mm:ss[.f{1,9}]` — space separator,
    /// seconds required.
    #[default]
    Sql,
    /// `TimeFormats.ISO8601_TIMESTAMP_FORMAT` (`DateTimeFormatter.ISO_LOCAL_DATE_TIME`):
    /// `yyyy-MM-dd'T'HH:mm[:ss[.f{1,9}]]` — 'T' separator, seconds optional.
    Iso8601,
}

/// A Flink timestamp string per `TimeFormats`, as nanoseconds since the epoch. One deliberate
/// leniency (documented): the trailing `Z` the `*_WITH_LOCAL_TIMEZONE` variants require is accepted
/// but not required here, and also tolerated on a plain-timestamp column — the boundary schema
/// carries no LTZ marker, and the produced value is identical either way. Everything else is
/// strict: the mode's separator, padded two-digit fields, a real calendar date, 1–9 fraction
/// digits after a mandatory '.', and full-string consumption.
pub(crate) fn parse_flink_timestamp(s: &str, mode: TimestampMode) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 11 {
        return None;
    }
    let days = parse_iso_local_date(&s[..10])? as i64;
    match (mode, b[10]) {
        (TimestampMode::Sql, b' ') | (TimestampMode::Iso8601, b'T') => {}
        _ => return None,
    }
    let time = s[11..].strip_suffix('Z').unwrap_or(&s[11..]);
    let nanos_of_day = parse_flink_time(time, mode == TimestampMode::Iso8601)?;
    days.checked_mul(86_400_000_000_000)?
        .checked_add(nanos_of_day)
}

/// A TIME column string per Flink's `SQL_TIME_FORMAT` (`HH:mm:ss[.f{0,9}]` — seconds required,
/// no zone), as the **whole seconds** of the day: Flink's converter does
/// `localTime.toSecondOfDay() * 1000`, silently discarding any parsed fraction regardless of the
/// column's declared precision, so the produced value never carries sub-second digits (the
/// fraction still has to *parse* — 1–9 digits after the point).
pub(crate) fn parse_sql_time_second_of_day(s: &str) -> Option<i64> {
    // SMART hour-24 is a whole day; with no date to roll into, LocalTime is midnight.
    parse_flink_time(s, false).map(|nanos| (nanos / 1_000_000_000) % 86_400)
}

/// `HH:mm[:ss][.f{1,9}]` — seconds optional only for ISO-8601 (`ISO_LOCAL_TIME`), fraction only
/// after seconds.
fn parse_flink_time(s: &str, seconds_optional: bool) -> Option<i64> {
    let b = s.as_bytes();
    let two = |i: usize| -> Option<u32> {
        let (a, c) = (*b.get(i)?, *b.get(i + 1)?);
        (a.is_ascii_digit() && c.is_ascii_digit())
            .then(|| (a - b'0') as u32 * 10 + (c - b'0') as u32)
    };
    let hour = two(0)?;
    if b.get(2) != Some(&b':') {
        return None;
    }
    let minute = two(3)?;
    if hour > 24 || minute > 59 {
        return None;
    }
    let (second, rest) = if b.len() == 5 {
        if !seconds_optional {
            return None;
        }
        (0, "")
    } else {
        if b.get(5) != Some(&b':') {
            return None;
        }
        (two(6).filter(|&v| v <= 59)?, &s[8..])
    };
    let nanos = if rest.is_empty() {
        0
    } else {
        // Java's appendFraction(0, 9, true) even accepts a bare trailing '.' (zero digits) —
        // pinned by the decode parity test against Flink's own deserializer.
        let digits = rest.strip_prefix('.')?;
        if digits.len() > 9 || !digits.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        if digits.is_empty() {
            0
        } else {
            digits.parse::<i64>().ok()? * 10i64.pow(9 - digits.len() as u32)
        }
    };
    if hour == 24 && (minute != 0 || second != 0 || nanos != 0) {
        // java.time's SMART resolver (the formatters' default) accepts hour 24 only at exactly
        // zero minutes/seconds/fraction — resolved as a full day, which the callers fold per
        // Flink's queries: a timestamp rolls to the next day's midnight, a bare TIME's LocalTime
        // query sees 00:00 (the excess day is never applied without a date).
        return None;
    }
    Some(((hour as i64 * 60 + minute as i64) * 60 + second as i64) * 1_000_000_000 + nanos)
}

/// How a base64 read failed, mirroring what Jackson's *streaming* decoder does to the parser:
/// most bad inputs throw with the string's closing quote unread (`Recoverable` — Flink's
/// per-field skip then nulls just the value), but a group truncated after one character or after
/// a single `=` makes Jackson read — and consume — the closing quote before throwing, corrupting
/// the parser so Flink's lenient mode drops the whole message (`QuoteConsumed`).
#[derive(Debug, PartialEq)]
pub(crate) enum Base64Error {
    Recoverable,
    QuoteConsumed,
}

/// Base64 exactly as Flink's BINARY/VARBINARY JSON columns consume it —
/// `JsonParser.getBinaryValue()` with Jackson's default variant (MIME-NO-LINEFEEDS): the standard
/// `A–Za–z0–9+/` alphabet, whitespace skipped between (never inside) four-character groups, and
/// padding **required** on a trailing partial group (jackson-core 2.12+ reads the MIME variants
/// as padding-required). The declared column length is not enforced, matching Flink.
pub(crate) fn parse_jackson_base64(s: &str) -> Result<Vec<u8>, Base64Error> {
    fn decode(b: u8) -> Result<u32, Base64Error> {
        match b {
            b'A'..=b'Z' => Ok((b - b'A') as u32),
            b'a'..=b'z' => Ok((b - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((b - b'0' + 52) as u32),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(Base64Error::Recoverable),
        }
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut i = 0;
    loop {
        while i < bytes.len() && bytes[i] <= 0x20 {
            i += 1;
        }
        if i == bytes.len() {
            return Ok(out);
        }
        let head = decode(bytes[i])?;
        // A group of exactly one character: Jackson reads the closing quote as the second char.
        let second = *bytes.get(i + 1).ok_or(Base64Error::QuoteConsumed)?;
        let mut data = (head << 6) | decode(second)?;
        // A two-character group must carry `==`; end-of-string here is the rewound (recoverable)
        // missing-padding error.
        let third = *bytes.get(i + 2).ok_or(Base64Error::Recoverable)?;
        i += 3;
        if third == b'=' {
            // After one `=` Jackson insists on the second and reads the quote to find it.
            if *bytes.get(i).ok_or(Base64Error::QuoteConsumed)? != b'=' {
                return Err(Base64Error::Recoverable);
            }
            i += 1;
            out.push((data >> 4) as u8);
            continue;
        }
        data = (data << 6) | decode(third)?;
        let fourth = *bytes.get(i).ok_or(Base64Error::Recoverable)?;
        i += 1;
        if fourth == b'=' {
            data >>= 2;
            out.extend_from_slice(&[(data >> 8) as u8, data as u8]);
            continue;
        }
        data = (data << 6) | decode(fourth)?;
        out.extend_from_slice(&[(data >> 16) as u8, (data >> 8) as u8, data as u8]);
    }
}

/// `new BigDecimal(String)`: an optional sign, digits with at most one decimal point (at least one
/// digit overall), and an optional exponent. No whitespace, no specials. Returns the unscaled
/// arbitrary-precision value and its scale (fraction digits minus exponent — negative scales are
/// how `1e5` comes out, exactly as `BigDecimal`).
pub(crate) fn parse_java_big_decimal(s: &str) -> Option<(num_bigint::BigInt, i64)> {
    let b = s.as_bytes();
    let mut i = 0;
    let negative = match b.first()? {
        b'+' => {
            i += 1;
            false
        }
        b'-' => {
            i += 1;
            true
        }
        _ => false,
    };
    let mut digits: Vec<u8> = Vec::with_capacity(b.len());
    let mut fraction_digits: Option<i64> = None;
    while let Some(&c) = b.get(i) {
        match c {
            b'0'..=b'9' => {
                digits.push(c);
                if let Some(f) = fraction_digits.as_mut() {
                    *f += 1;
                }
            }
            b'.' if fraction_digits.is_none() => fraction_digits = Some(0),
            b'e' | b'E' => break,
            _ => return None,
        }
        i += 1;
    }
    if digits.is_empty() {
        return None;
    }
    let exponent: i64 = if i < b.len() {
        // At 'e'/'E': BigDecimal requires a parsable integer exponent.
        s[i + 1..]
            .parse()
            .ok()
            .filter(|e: &i64| e.unsigned_abs() < i32::MAX as u64)?
    } else {
        0
    };
    let unscaled = num_bigint::BigInt::parse_bytes(&digits, 10)?;
    let unscaled = if negative { -unscaled } else { unscaled };
    Some((unscaled, fraction_digits.unwrap_or(0) - exponent))
}

/// The full Flink decimal string decode: `new BigDecimal(s)` then
/// `DecimalData.fromBigDecimal(bd, precision, scale)` — HALF_UP rescale, **null** (not an error)
/// when the result exceeds the declared precision. `Err(())` is an unparsable string (the host
/// fails the field); `Ok(None)` is the precision-overflow null.
pub(crate) fn parse_flink_decimal(s: &str, precision: u8, scale: i8) -> Result<Option<i128>, ()> {
    let (unscaled, source_scale) = parse_java_big_decimal(s).ok_or(())?;
    Ok(crate::expr::rescale_half_up(
        unscaled,
        source_scale,
        precision,
        scale,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_boolean_never_fails() {
        assert!(parse_java_boolean("true"));
        assert!(parse_java_boolean("TRUE"));
        assert!(!parse_java_boolean("false"));
        assert!(!parse_java_boolean("yes"));
        assert!(!parse_java_boolean("1"));
        assert!(!parse_java_boolean(""));
    }

    #[test]
    fn java_integer_grammar() {
        assert_eq!(parse_java_integer::<i64>("42"), Some(42));
        assert_eq!(parse_java_integer::<i64>("+42"), Some(42));
        assert_eq!(parse_java_integer::<i64>("-42"), Some(-42));
        assert_eq!(parse_java_integer::<i64>("1.5"), None);
        assert_eq!(parse_java_integer::<i64>(""), None);
        assert_eq!(parse_java_integer::<i8>("128"), None);
    }

    #[test]
    fn java_float_specials_and_suffixes() {
        assert_eq!(parse_java_float::<f64>("1.5"), Some(1.5));
        assert_eq!(parse_java_float::<f64>(" 1.5 "), Some(1.5));
        assert_eq!(parse_java_float::<f64>("1.5d"), Some(1.5));
        assert_eq!(parse_java_float::<f32>("1.5f"), Some(1.5));
        assert_eq!(parse_java_float::<f64>("Infinity"), Some(f64::INFINITY));
        assert_eq!(
            parse_java_float::<f64>("-Infinity"),
            Some(f64::NEG_INFINITY)
        );
        assert!(parse_java_float::<f64>("NaN").unwrap().is_nan());
        assert_eq!(parse_java_float::<f64>("1e999"), Some(f64::INFINITY));
        // Java rejects Rust-only spellings.
        assert_eq!(parse_java_float::<f64>("inf"), None);
        assert_eq!(parse_java_float::<f64>("nan"), None);
        assert_eq!(parse_java_float::<f64>("infinity"), None);
        assert_eq!(parse_java_float::<f64>(""), None);
    }

    #[test]
    fn iso_local_date_is_strict() {
        assert_eq!(parse_iso_local_date("1970-01-02"), Some(1));
        assert_eq!(parse_iso_local_date("1969-12-31"), Some(-1));
        assert_eq!(parse_iso_local_date("2020-1-1"), None);
        assert_eq!(parse_iso_local_date("2020-02-30"), None);
        assert_eq!(parse_iso_local_date("2020-01-01 "), None);
        assert_eq!(parse_iso_local_date("2020-01-01T00:00:00"), None);
    }

    #[test]
    fn java_sql_date_is_lenient() {
        assert_eq!(parse_java_sql_date("1970-01-02"), Some(1));
        assert_eq!(parse_java_sql_date("1970-1-2"), Some(1));
        assert_eq!(parse_java_sql_date("+1970-01-02"), Some(1));
        // Day overflow normalizes into the next month, like java.util.Date.
        assert_eq!(
            parse_java_sql_date("2020-02-31"),
            parse_iso_local_date("2020-03-02")
        );
        assert_eq!(parse_java_sql_date("2020-13-01"), None);
        assert_eq!(parse_java_sql_date("2020-01-32"), None);
        assert_eq!(parse_java_sql_date("2020-01-01x"), None);
        assert_eq!(parse_java_sql_date("-2020-01-01"), None);
    }

    #[test]
    fn sql_timestamps_require_space_and_seconds() {
        let base = 86_400_000_000_000i64;
        assert_eq!(
            parse_flink_timestamp("1970-01-02 00:00:00", TimestampMode::Sql),
            Some(base)
        );
        assert_eq!(
            parse_flink_timestamp("1970-01-02 00:00:00.5", TimestampMode::Sql),
            Some(base + 500_000_000)
        );
        assert_eq!(
            parse_flink_timestamp("1970-01-02 00:00:00.123456789", TimestampMode::Sql),
            Some(base + 123_456_789)
        );
        // The LTZ 'Z' is tolerated (documented leniency), same value.
        assert_eq!(
            parse_flink_timestamp("1970-01-02 00:00:00Z", TimestampMode::Sql),
            Some(base)
        );
        assert_eq!(
            parse_flink_timestamp("1970-01-02T00:00:00", TimestampMode::Sql),
            None
        );
        assert_eq!(
            parse_flink_timestamp("1970-01-02 00:00", TimestampMode::Sql),
            None
        );
        // Java's fraction parser tolerates the bare decimal point (parity-pinned).
        assert_eq!(
            parse_flink_timestamp("1970-01-02 00:00:00.", TimestampMode::Sql),
            Some(base)
        );
        // SMART hour-24 rolls a timestamp to the NEXT day's midnight (parity-pinned against
        // Flink's deserializer); anything nonzero behind the 24 is an error.
        assert_eq!(
            parse_flink_timestamp("1970-01-02 24:00:00", TimestampMode::Sql),
            Some(2 * base)
        );
        assert_eq!(
            parse_flink_timestamp("1970-01-02 24:00:00.5", TimestampMode::Sql),
            None
        );
        assert_eq!(
            parse_flink_timestamp("1970-01-02 00:00:00.1234567890", TimestampMode::Sql),
            None
        );
        assert_eq!(
            parse_flink_timestamp("1970-01-02 00:00:00+05:00", TimestampMode::Sql),
            None
        );
        assert_eq!(
            parse_flink_timestamp("1970-01-02", TimestampMode::Sql),
            None
        );
    }

    #[test]
    fn iso_timestamps_require_t_and_allow_missing_seconds() {
        let base = 86_400_000_000_000i64;
        assert_eq!(
            parse_flink_timestamp("1970-01-02T00:00:00", TimestampMode::Iso8601),
            Some(base)
        );
        assert_eq!(
            parse_flink_timestamp("1970-01-02T00:01", TimestampMode::Iso8601),
            Some(base + 60_000_000_000)
        );
        assert_eq!(
            parse_flink_timestamp("1970-01-02 00:00:00", TimestampMode::Iso8601),
            None
        );
        assert_eq!(
            parse_flink_timestamp("1970-01-02T00:01.5", TimestampMode::Iso8601),
            None
        );
    }

    #[test]
    fn sql_time_requires_seconds_and_discards_the_fraction() {
        assert_eq!(parse_sql_time_second_of_day("00:00:00"), Some(0));
        assert_eq!(parse_sql_time_second_of_day("12:34:56"), Some(45_296));
        // Sub-second digits parse but never reach the value (Flink's toSecondOfDay * 1000).
        assert_eq!(parse_sql_time_second_of_day("12:34:56.789"), Some(45_296));
        assert_eq!(
            parse_sql_time_second_of_day("12:34:56.123456789"),
            Some(45_296)
        );
        assert_eq!(parse_sql_time_second_of_day("12:34:56."), Some(45_296));
        assert_eq!(parse_sql_time_second_of_day("23:59:59"), Some(86_399));
        // Seconds are required (SQL_TIME_FORMAT, unlike ISO_LOCAL_TIME) and the shape is strict.
        assert_eq!(parse_sql_time_second_of_day("12:34"), None);
        // SMART resolution: hour 24 is midnight when everything else is zero, an error otherwise.
        assert_eq!(parse_sql_time_second_of_day("24:00:00"), Some(0));
        assert_eq!(parse_sql_time_second_of_day("24:00:00.000"), Some(0));
        assert_eq!(parse_sql_time_second_of_day("24:00:01"), None);
        assert_eq!(parse_sql_time_second_of_day("24:00:00.5"), None);
        assert_eq!(parse_sql_time_second_of_day("25:00:00"), None);
        assert_eq!(parse_sql_time_second_of_day("12:34:60"), None);
        assert_eq!(parse_sql_time_second_of_day("12:34:56.1234567890"), None);
        assert_eq!(parse_sql_time_second_of_day("12:34:56Z"), None);
        assert_eq!(parse_sql_time_second_of_day(" 12:34:56"), None);
        assert_eq!(parse_sql_time_second_of_day("123456"), None);
    }

    #[test]
    fn jackson_base64_requires_padding_and_rejects_inner_whitespace() {
        assert_eq!(parse_jackson_base64(""), Ok(vec![]));
        assert_eq!(parse_jackson_base64("AQID"), Ok(vec![1, 2, 3]));
        assert_eq!(parse_jackson_base64("AQ=="), Ok(vec![1]));
        assert_eq!(parse_jackson_base64("AQI="), Ok(vec![1, 2]));
        assert_eq!(parse_jackson_base64("AQIDBA=="), Ok(vec![1, 2, 3, 4]));
        // Whitespace between four-char groups is skipped; inside a group it is an error.
        assert_eq!(parse_jackson_base64(" AQID AQ== "), Ok(vec![1, 2, 3, 1]));
        assert_eq!(parse_jackson_base64("AQ ID"), Err(Base64Error::Recoverable));
        // A trailing partial group without padding fails (jackson-core 2.12+ MIME read behavior).
        assert_eq!(parse_jackson_base64("AQ"), Err(Base64Error::Recoverable));
        assert_eq!(parse_jackson_base64("AQI"), Err(Base64Error::Recoverable));
        assert_eq!(parse_jackson_base64("QQ=Q"), Err(Base64Error::Recoverable));
        // Outside the standard alphabet (url-safe chars included) is an error.
        assert_eq!(parse_jackson_base64("AQ*D"), Err(Base64Error::Recoverable));
        assert_eq!(parse_jackson_base64("AQ-_"), Err(Base64Error::Recoverable));
        // The quote-consuming shapes: a 1-char group, or a group cut after a single '='.
        assert_eq!(parse_jackson_base64("A"), Err(Base64Error::QuoteConsumed));
        assert_eq!(
            parse_jackson_base64("AQIDA"),
            Err(Base64Error::QuoteConsumed)
        );
        assert_eq!(parse_jackson_base64("AQ="), Err(Base64Error::QuoteConsumed));
    }

    #[test]
    fn big_decimal_grammar_and_rescale() {
        use num_bigint::BigInt;
        assert_eq!(parse_java_big_decimal("1.5"), Some((BigInt::from(15), 1)));
        assert_eq!(parse_java_big_decimal("-1.5"), Some((BigInt::from(-15), 1)));
        assert_eq!(parse_java_big_decimal(".5"), Some((BigInt::from(5), 1)));
        assert_eq!(parse_java_big_decimal("1."), Some((BigInt::from(1), 0)));
        assert_eq!(parse_java_big_decimal("1e5"), Some((BigInt::from(1), -5)));
        assert_eq!(
            parse_java_big_decimal("1.5e-2"),
            Some((BigInt::from(15), 3))
        );
        assert_eq!(parse_java_big_decimal(" 1.5"), None);
        assert_eq!(parse_java_big_decimal("."), None);
        assert_eq!(parse_java_big_decimal("1.5.5"), None);
        assert_eq!(parse_java_big_decimal("NaN"), None);

        // HALF_UP rescale + precision-overflow null, DecimalData.fromBigDecimal's contract.
        assert_eq!(parse_flink_decimal("1.235", 5, 2), Ok(Some(124)));
        assert_eq!(parse_flink_decimal("-1.235", 5, 2), Ok(Some(-124)));
        assert_eq!(parse_flink_decimal("1.234", 5, 2), Ok(Some(123)));
        assert_eq!(parse_flink_decimal("12345.6", 5, 2), Ok(None));
        assert_eq!(parse_flink_decimal("junk", 5, 2), Err(()));
    }
}
