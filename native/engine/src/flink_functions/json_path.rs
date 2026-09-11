//! Definite SQL/JSON paths and first-document parsing for Flink's Jackson/Jayway runtime.

use std::borrow::Cow;

mod simd;
pub(super) use simd::Reader;
mod runtime;
pub(super) use runtime::with_reader;

#[derive(Debug)]
pub(super) struct Path<'a> {
    pub lax: bool,
    steps: Vec<Step<'a>>,
    identifier: &'static regex::Regex,
    legacy_decimal_exponent: bool,
}

#[derive(Debug)]
enum Step<'a> {
    Member(&'a str),
    Index(usize),
}

impl<'a> Path<'a> {
    // The Java encoder normalizes the mode and admits this same definite-path grammar.
    pub fn parse(path: &'a str, unicode: &str) -> Option<Self> {
        static IDENTIFIERS: std::sync::LazyLock<[regex::Regex; 3]> =
            std::sync::LazyLock::new(|| {
                const CATEGORIES: &str =
                    r"[\p{L}\p{Nl}\p{Sc}\p{Pc}\p{Nd}\p{Mc}\p{Mn}\p{Cf}\x7f-\x9f]";
                ["13.0", "15.0", "16.0"].map(|version| {
                    regex::Regex::new(&format!(r"[{CATEGORIES}&&\p{{Age={version}}}]")).unwrap()
                })
            });
        let identifier = &IDENTIFIERS[match unicode {
            "13.0" => 0,
            "15.0" => 1,
            "16.0" => 2,
            _ => return None,
        }];
        let (lax, mut text) = if let Some(text) = path.strip_prefix("lax ") {
            (true, text)
        } else {
            (false, path.strip_prefix("strict ").unwrap_or(path))
        };
        text = text.strip_prefix('$')?;
        let mut steps = Vec::new();
        while !text.is_empty() {
            if let Some(rest) = text.strip_prefix('.') {
                let end = rest.find(['.', '[']).unwrap_or(rest.len());
                let name = &rest[..end];
                if !is_identifier(name) {
                    return None;
                }
                steps.push(Step::Member(name));
                text = &rest[end..];
            } else if let Some(rest) = text.strip_prefix("['") {
                let end = rest.find("']")?;
                let name = &rest[..end];
                if name.is_empty()
                    || !name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_ -".contains(&b))
                {
                    return None;
                }
                steps.push(Step::Member(name));
                text = &rest[end + 2..];
            } else {
                let rest = text.strip_prefix('[')?;
                let end = rest.find(']')?;
                let index = &rest[..end];
                if index.is_empty() || !index.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                let index: i32 = index.parse().ok()?;
                steps.push(Step::Index(index as usize));
                text = &rest[end + 1..];
            }
        }
        Some(Self {
            lax,
            steps,
            identifier,
            legacy_decimal_exponent: unicode == "13.0",
        })
    }

    #[cfg(test)]
    pub fn read<'s>(&self, input: &'s str) -> Result<Value<'s>, ()> {
        self.apply_policy(self.parse_with_buffer(input, 4000))
    }

    fn parse_with_buffer<'s>(
        &self,
        input: &'s str,
        buffer_size: usize,
    ) -> Result<(Value<'s>, bool), ()> {
        let mut parser = Parser {
            input,
            pos: 0,
            identifier: self.identifier,
            legacy_decimal_exponent: self.legacy_decimal_exponent,
            buffer_size,
        };
        parser.whitespace();
        let root_null = parser.remaining().starts_with("null");
        parser
            .value(Some(&self.steps), 0)
            .map(|value| (value, root_null))
    }

    fn apply_policy<'s>(&self, parsed: Result<(Value<'s>, bool), ()>) -> Result<Value<'s>, ()> {
        match parsed {
            Ok((_, true)) => Err(()), // Jayway cannot construct a context from Java null.
            Ok((Value::Missing | Value::Null, _)) if !self.lax => Err(()),
            Ok((value, _)) => Ok(value),
            Err(()) if self.lax => Ok(Value::Missing),
            Err(()) => Err(()),
        }
    }
}

fn is_identifier(name: &str) -> bool {
    name.as_bytes()
        .first()
        .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub(super) enum Value<'a> {
    Missing,
    Null,
    Container,
    String(&'a str),
    DecodedString(&'a str),
    Number(&'a str),
    Boolean(bool),
}

impl Value<'_> {
    pub fn text(&self) -> Option<Cow<'_, str>> {
        match self {
            Self::String(value) => Some(unescape(value)),
            Self::DecodedString(value) => Some(Cow::Borrowed(value)),
            Self::Number(value) => Some(number_text(value)),
            Self::Boolean(value) => Some(Cow::Borrowed(if *value { "true" } else { "false" })),
            _ => None,
        }
    }
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
    identifier: &'static regex::Regex,
    legacy_decimal_exponent: bool,
    buffer_size: usize,
}

impl<'a> Parser<'a> {
    fn remaining(&self) -> &'a str {
        &self.input[self.pos..]
    }

    fn whitespace(&mut self) {
        while self.peek().is_some_and(|b| b" \t\r\n".contains(&b)) {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.as_bytes().get(self.pos).copied()
    }

    fn consume(&mut self, byte: u8) -> bool {
        if self.peek() == Some(byte) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn value(&mut self, path: Option<&[Step<'_>]>, depth: usize) -> Result<Value<'a>, ()> {
        self.whitespace();
        let selected = path.is_some_and(|steps| steps.is_empty());
        let value = match self.peek().ok_or(())? {
            b'{' | b'[' => {
                if depth >= 1000 {
                    return Err(());
                }
                return self.container(path, depth + 1);
            }
            b'"' => Value::String(self.string(20_000_000)?),
            b't' => {
                self.literal("true")?;
                Value::Boolean(true)
            }
            b'f' => {
                self.literal("false")?;
                Value::Boolean(false)
            }
            b'n' => {
                self.literal("null")?;
                Value::Null
            }
            b'-' | b'0'..=b'9' => Value::Number(self.number(depth == 0)?),
            _ => return Err(()),
        };
        Ok(if selected { value } else { Value::Missing })
    }

    fn container(&mut self, path: Option<&[Step<'_>]>, depth: usize) -> Result<Value<'a>, ()> {
        let object = self.consume(b'{');
        if !object {
            self.pos += 1; // '[' was checked by value().
        }
        let end = if object { b'}' } else { b']' };
        let mut result = if path.is_some_and(|steps| steps.is_empty()) {
            Value::Container
        } else {
            Value::Missing
        };
        self.whitespace();
        if self.consume(end) {
            return Ok(result);
        }
        let mut index = 0;
        loop {
            self.whitespace();
            let matches = if object {
                let key = self.string(50_000)?;
                self.whitespace();
                if !self.consume(b':') {
                    return Err(());
                }
                match path.and_then(|steps| steps.first()) {
                    Some(Step::Member(name)) => key_matches(key, name),
                    _ => false,
                }
            } else {
                matches!(path.and_then(|steps| steps.first()), Some(Step::Index(n)) if *n == index)
            };
            let child = self.value(if matches { path.map(|p| &p[1..]) } else { None }, depth)?;
            if matches {
                // Jackson keeps the last duplicate member, including a replacement with no match.
                result = child;
            }
            self.whitespace();
            if self.consume(end) {
                return Ok(result);
            }
            if !self.consume(b',') {
                return Err(());
            }
            index += 1;
        }
    }

    fn string(&mut self, max_units: usize) -> Result<&'a str, ()> {
        if self.peek() != Some(b'"') {
            return Err(());
        }
        let start = self.pos + 1;
        // IgnoredAny uses serde_json's allocation-free string scanner and accepts lone
        // escaped surrogates. Deserializing a Rust String would reject those Flink inputs.
        let mut token = serde_json::Deserializer::from_str(self.remaining())
            .into_iter::<serde::de::IgnoredAny>();
        token.next().ok_or(())?.map_err(|_| ())?;
        self.pos += token.byte_offset();
        let raw = &self.input[start..self.pos - 1];
        if raw.len() > max_units && unescape(raw).encode_utf16().count() > max_units {
            return Err(());
        }
        Ok(raw)
    }

    fn literal(&mut self, literal: &str) -> Result<(), ()> {
        if !self.remaining().starts_with(literal) {
            return Err(());
        }
        self.pos += literal.len();
        if self
            .remaining()
            .chars()
            .next()
            .is_some_and(|ch| identifier_suffix(ch, self.identifier))
        {
            return Err(());
        }
        Ok(())
    }

    fn number(&mut self, root: bool) -> Result<&'a str, ()> {
        let start = self.pos;
        self.consume(b'-');
        let integer_start = self.pos;
        if !self.consume(b'0') && self.digits() == 0 {
            return Err(());
        }
        let integer = self.pos - integer_start;
        let fraction = if self.consume(b'.') {
            let count = self.digits();
            if count == 0 {
                return Err(());
            }
            count
        } else {
            0
        };
        let mut exponent_digits = 0;
        if self.consume(b'e') || self.consume(b'E') {
            let negative = self.consume(b'-');
            if !negative {
                self.consume(b'+');
            }
            let exponent_start = self.pos;
            exponent_digits = self.digits();
            if exponent_digits == 0 {
                return Err(());
            }
            let digits = self.input[exponent_start..self.pos].trim_start_matches('0');
            let exponent = if digits.is_empty() {
                0
            } else {
                digits.parse::<i64>().map_err(|_| ())?
            };
            // Below 500 chars Jackson uses the JDK constructor. JDK 17 caps the exponent
            // itself; later admitted JDKs and FastDoubleParser only require the scale to fit.
            if self.legacy_decimal_exponent
                && self.pos - start < 500
                && exponent > i64::from(i32::MAX)
            {
                return Err(());
            }
            let scale = (fraction as i64)
                .checked_sub(if negative { -exponent } else { exponent })
                .ok_or(())?;
            i32::try_from(scale).map_err(|_| ())?;
        }
        let number = &self.input[start..self.pos];
        if !self.number_length_valid(start, integer, fraction, exponent_digits)
            || (root && self.peek().is_some_and(|b| !b" \t\r\n".contains(&b)))
        {
            return Err(());
        }
        Ok(number)
    }

    fn number_length_valid(
        &self,
        start: usize,
        integer: usize,
        fraction: usize,
        exponent: usize,
    ) -> bool {
        let digits = integer + fraction + exponent;
        if digits <= 1000 {
            return true;
        }
        if (fraction == 0 && exponent == 0) || digits > 1002 {
            return false;
        }
        // ReaderBasedJsonParser._parseNumber2 counts an absent fraction/exponent as -1.
        // It is entered for a leading zero, EOF, or a number crossing the actual input buffer.
        let number = self.input[start..self.pos].trim_start_matches('-');
        let slow = number.starts_with('0') || self.pos == self.input.len() || {
            let units = self.input.encode_utf16().count();
            units > 32768
                && self.input[..start].encode_utf16().count() / self.buffer_size
                    != self.input[..self.pos].encode_utf16().count() / self.buffer_size
        };
        slow && digits - usize::from(fraction == 0) - usize::from(exponent == 0) <= 1000
    }

    fn digits(&mut self) -> usize {
        let start = self.pos;
        while self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.pos += 1;
        }
        self.pos - start
    }
}

fn identifier_suffix(ch: char, identifier: &regex::Regex) -> bool {
    // Jackson checks the next UTF-16 code unit, so a supplementary code point terminates a token.
    if ch > '\u{ffff}' || ch < '0' {
        return false;
    }
    if ch.is_ascii() {
        return ch.is_ascii_alphanumeric() || ch == '_' || ch == '\u{7f}';
    }
    identifier.is_match(ch.encode_utf8(&mut [0; 4]))
}

fn key_matches(raw: &str, name: &str) -> bool {
    if !raw.contains('\\') {
        return raw == name;
    }
    // Admitted member names are ASCII; surrogate replacement cannot alias one of them.
    unescape(raw) == name
}

fn unescape(raw: &str) -> Cow<'_, str> {
    if !raw.contains('\\') {
        return Cow::Borrowed(raw);
    }
    let mut units = Vec::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            units.extend_from_slice(ch.encode_utf16(&mut [0; 2]));
            continue;
        }
        let unit = match chars.next().unwrap() {
            '"' => b'"' as u16,
            '\\' => b'\\' as u16,
            '/' => b'/' as u16,
            'b' => 8,
            'f' => 12,
            'n' => 10,
            'r' => 13,
            't' => 9,
            'u' => (0..4).fold(0, |n, _| {
                n * 16 + chars.next().unwrap().to_digit(16).unwrap() as u16
            }),
            _ => unreachable!("validated JSON escape"),
        };
        units.push(unit);
    }
    Cow::Owned(
        char::decode_utf16(units)
            .map(|ch| ch.unwrap_or('?'))
            .collect(),
    )
}

fn number_text(raw: &str) -> Cow<'_, str> {
    if !raw.contains(['.', 'e', 'E']) {
        return Cow::Borrowed(if raw == "-0" { "0" } else { raw });
    }
    let (mantissa, exponent) = raw.split_once(['e', 'E']).unwrap_or((raw, "0"));
    let exponent = exponent.parse::<i64>().unwrap_or(0);
    let fraction = mantissa.split_once('.').map_or(0, |(_, part)| part.len());
    let scale = fraction as i64 - exponent;
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let significant = digits.trim_start_matches('0');
    let significant = if significant.is_empty() {
        "0"
    } else {
        significant
    };
    let adjusted = significant.len() as i64 - scale - 1;
    let mut output = String::new();
    if raw.starts_with('-') && significant != "0" {
        output.push('-');
    }
    if scale >= 0 && adjusted >= -6 {
        let point = significant.len() as i64 - scale;
        if point <= 0 {
            output.push_str("0.");
            output.extend(std::iter::repeat_n('0', (-point) as usize));
            output.push_str(significant);
        } else {
            let point = point as usize;
            output.push_str(&significant[..point]);
            if point < significant.len() {
                output.push('.');
                output.push_str(&significant[point..]);
            }
        }
    } else {
        output.push_str(&significant[..1]);
        if significant.len() > 1 {
            output.push('.');
            output.push_str(&significant[1..]);
        }
        use std::fmt::Write;
        write!(output, "E{adjusted:+}").unwrap();
    }
    Cow::Owned(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(text: &str) -> Path<'_> {
        Path::parse(text, "13.0").unwrap()
    }

    #[test]
    fn definite_path_grammar() {
        for text in [
            "$",
            "strict $.a.b[0]",
            "lax $['a b'][2147483647]",
            "$[01].a",
        ] {
            assert!(Path::parse(text, "13.0").is_some(), "{text}");
        }
        for text in [
            "",
            "a",
            "$.a.*",
            "$..a",
            "$[-1]",
            "$[2147483648]",
            "$['']",
            "$['a\\b']",
        ] {
            assert!(Path::parse(text, "13.0").is_none(), "{text}");
        }
    }

    #[test]
    fn duplicate_ancestors_replace_the_entire_selected_subtree() {
        let path = path("lax $.a.b[1]");
        assert_eq!(path.read(r#"{"a":{"b":[0,1]},"a":{}}"#), Ok(Value::Missing));
        assert_eq!(
            path.read(r#"{"a":{},"a":{"b":[0,1]}}"#),
            Ok(Value::Number("1"))
        );
        assert_eq!(
            path.read(r#"{"\u0061":{"b":[0,"ok"]}}"#),
            Ok(Value::String("ok"))
        );
        assert_eq!(
            path.read(r#"{"a":{"b":[0,1]},"other":[}"#),
            Ok(Value::Missing)
        );
    }

    #[test]
    fn strict_lax_and_document_null_are_distinct() {
        for document in ["{}", r#"{"a":null}"#, "bad", ""] {
            assert!(path("$.a").read(document).is_err());
            assert!(matches!(
                path("lax $.a").read(document),
                Ok(Value::Missing | Value::Null)
            ));
        }
        assert!(path("lax $.a").read("null").is_err());
        assert_eq!(path("lax $.a").read("nullx"), Ok(Value::Missing));
        assert_eq!(path("$.a").read(r#"{"a":[]}suffix"#), Ok(Value::Container));
    }

    #[test]
    fn validates_first_document_and_jackson_token_terminators() {
        for document in [
            "truex",
            "false_",
            "null\u{301}",
            "true\u{7f}",
            "1,",
            "01",
            "1e+",
            "[1,]",
            r#"{"a":"\q"}"#,
        ] {
            assert!(path("$").read(document).is_err(), "{document}");
        }
        for document in [
            "true,",
            "true\u{1f600}",
            "true\u{0}",
            "true false",
            "\"ok\"garbage",
            "[0]garbage",
        ] {
            assert!(path("$").read(document).is_ok(), "{document}");
        }
        assert!(path("$").read("true\u{870}").is_ok()); // Assigned in Unicode 14, after JDK 17.
        assert!(Path::parse("$", "15.0")
            .unwrap()
            .read("true\u{870}")
            .is_err());
    }

    #[test]
    fn scalar_text_preserves_big_decimals_and_java_surrogate_replacement() {
        for (document, expected) in [
            ("-0", "0"),
            ("-0.0", "0.0"),
            ("1.2300", "1.2300"),
            ("1e3", "1E+3"),
            ("100e-1", "10.0"),
            ("0.000001", "0.000001"),
            ("0.0000001", "1E-7"),
            ("1e2147483647", "1E+2147483647"),
            ("0e999999999", "0E+999999999"),
            (r#""\ud800x\udc00\ud83d\ude00""#, "?x?\u{1f600}"),
            (r#""a\u0000b""#, "a\0b"),
        ] {
            assert_eq!(
                path("$").read(document).unwrap().text().unwrap(),
                expected,
                "{document}"
            );
        }
        for document in ["1e2147483648", "1e-2147483648", "0.1e-2147483647"] {
            assert!(path("$").read(document).is_err());
        }
        for version in ["15.0", "16.0"] {
            assert_eq!(
                Path::parse("$", version)
                    .unwrap()
                    .read("1e2147483648")
                    .unwrap()
                    .text()
                    .unwrap(),
                "1E+2147483648"
            );
            assert!(Path::parse("$", version)
                .unwrap()
                .read("1e-2147483648")
                .is_err());
        }
        let long = format!("1.{}e2147483648", "0".repeat(490));
        assert!(path("$").read(&long).is_ok());
    }

    #[test]
    fn jackson_constraints_apply_to_unselected_values_too() {
        for depth in [1000, 1001] {
            let document = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
            assert_eq!(path("$").read(&document).is_ok(), depth == 1000);
        }
        assert!(path("$").read(&"1".repeat(1000)).is_ok());
        assert!(path("$").read(&format!("-{}", "1".repeat(1000))).is_ok());
        assert!(path("$").read(&"1".repeat(1001)).is_err());
        assert!(path("$").read(&format!("1.{}", "1".repeat(1000))).is_ok());
        assert!(path("$")
            .read(&format!("[1.{}]", "1".repeat(1000)))
            .is_err());
        assert!(path("$").read(&format!("[0.{}]", "1".repeat(1000))).is_ok());
        for len in [50_000, 50_001] {
            let document = format!(r#"{{"a":1,"{}":2}}"#, "k".repeat(len));
            assert_eq!(path("$.a").read(&document).is_ok(), len == 50_000);
        }
        let document = format!(r#"{{"a":1,"long":"{}"}}"#, "x".repeat(20_000_001));
        assert!(path("$.a").read(&document).is_err());
    }
}
