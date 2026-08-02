//! The Jackson-faithful retry for single JSON messages the simd fast parse rejects. Flink's
//! deserializer sits on a Jackson pull parser that tokenizes documents simd-json refuses —
//! out-of-range number literals stay raw text until a converter reads them, raw control
//! characters inside strings are allowed (`ALLOW_UNESCAPED_CONTROL_CHARS`), and anything trailing
//! the root document is simply never read — and its converters then walk the token stream with a
//! bare cursor. This module reproduces that walk over an owned token vector
//! (`JsonParserToRowDataConverters.createRowConverter` and friends, cursor drift included) and
//! REWRITES the message into sanitized single-row documents whose values the fast-path appenders
//! convert to exactly Flink's outcome. The coercion envelope therefore lives in one place (the
//! appenders in `json.rs`); the rewrite only chooses each slot's surviving token and spells it so
//! the appender's conversion is the one Flink performed:
//!
//! - a string / true / false / null token spells as itself (the `getText` branches match the
//!   appenders' string-positioned parses);
//! - a number token spells as a number when simd can reparse the literal, and as a STRING of the
//!   raw literal otherwise — the text parse then fails or coerces exactly like Jackson's
//!   `getIntValue`/`parseDouble` on the out-of-range literal;
//! - FLOAT columns and STRING columns always take the raw literal as a string, for the
//!   single-rounding `parseFloat` and the exact echo respectively;
//! - a container token consumed by a scalar converter spells as `getText`'s "{" / "[", and the
//!   cursor stays on it — the walk then drifts into the container exactly like Flink's.
//!
//! One divergence is deliberate: a walk that runs past the end of input makes Flink's object loop
//! spin forever (`nextToken()` keeps returning null and no END token ever arrives), hanging the
//! job. The retry fails the message in strict mode and drops it in skip mode instead
//! (divergences/21).

use crate::json::{make_json_appender, write_json_string, JsonEnv};
use crate::*;

#[derive(Debug, PartialEq)]
pub(crate) enum Token {
    StartObject,
    EndObject,
    StartArray,
    EndArray,
    FieldName(String),
    Str(String),
    Number { raw: String, float: bool },
    True,
    False,
    Null,
}

/// `JsonToken.asString()` — what Jackson's `getText` yields for each token, the universal
/// coercion input of Flink's scalar converters.
fn text_of(token: &Token) -> &str {
    match token {
        Token::StartObject => "{",
        Token::EndObject => "}",
        Token::StartArray => "[",
        Token::EndArray => "]",
        Token::FieldName(name) => name,
        Token::Str(value) => value,
        Token::Number { raw, .. } => raw,
        Token::True => "true",
        Token::False => "false",
        Token::Null => "null",
    }
}

/// The walk ran past the last token where Flink's converter loops would keep polling a null
/// token forever. Strict mode fails the message, skip mode drops it (divergences/21).
struct WalkSpins;

type Walk<T> = Result<T, WalkSpins>;

struct Cursor<'t> {
    tokens: &'t [Token],
    pos: usize,
}

impl Cursor<'_> {
    fn current(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) {
        self.pos += 1;
    }
}

/// Decodes one message Jackson's way and rewrites it into sanitized single-row documents for the
/// fast path to append (possibly none — every skip-mode drop returns an empty vec). Strict-mode
/// failures panic, like the appenders they mirror.
pub(crate) fn rewrite_message(bytes: &[u8], fields: &Fields, env: JsonEnv) -> Vec<String> {
    let tokens = match tokenize(bytes) {
        Ok(tokens) => tokens,
        Err(reason) => {
            assert!(env.lenient, "failed to decode JSON record: {reason}");
            return Vec::new();
        }
    };
    let mut cursor = Cursor { tokens: &tokens, pos: 0 };
    let rows = match cursor.current() {
        // Jackson found no token, or a root Flink's deserializer rejects outright (it only maps
        // object and array roots).
        None => {
            assert!(env.lenient, "failed to decode JSON record: no content to map due to end-of-input");
            return Vec::new();
        }
        Some(Token::StartObject) => row_object(&mut cursor, fields, env).map(|row| vec![row]),
        // Only the plain `json` format (top-level arrays fan out) retries — see the decode loop.
        Some(Token::StartArray) => fan_out(&mut cursor, fields, env),
        Some(_) => {
            assert!(env.lenient, "JSON body was not a single object");
            return Vec::new();
        }
    };
    match rows {
        Ok(rows) => rows,
        Err(WalkSpins) => {
            assert!(
                env.lenient,
                "failed to decode JSON record: the converter walk ran past end-of-input \
                 (Flink's parser loop would spin forever here) — divergences/21"
            );
            Vec::new()
        }
    }
}

/// `processArray`: one row per element, converted from wherever the cursor stands — a failing
/// element fails the whole message in strict mode; in skip mode its null row drops alone and the
/// walk continues from the element's own tokens (a container element drifts into its subtree).
fn fan_out(cursor: &mut Cursor, fields: &Fields, env: JsonEnv) -> Walk<Vec<String>> {
    let mut rows = Vec::new();
    loop {
        cursor.advance();
        match cursor.current() {
            None => return Err(WalkSpins),
            Some(Token::EndArray) => return Ok(rows),
            Some(Token::StartObject) => rows.push(row_object(cursor, fields, env)?),
            Some(other) => {
                assert!(env.lenient, "JSON array element was not an object: {other:?}");
            }
        }
    }
}

/// `createRowConverter`'s walk: field names read from whatever token the cursor reached, a field
/// counter every MATCHED occurrence advances (duplicates included) whose saturation skips the
/// remaining keys, and per-field conversions that leave the cursor wherever Jackson's would.
/// Returns the sanitized row object; the cursor ends on the END_OBJECT token the walk took for
/// the row's close — after a drift, not necessarily the matching one.
fn row_object(cursor: &mut Cursor, fields: &Fields, env: JsonEnv) -> Walk<String> {
    let arity = fields.len();
    let mut slots: Vec<Option<String>> = vec![None; arity];
    let mut cnt = 0;
    cursor.advance();
    loop {
        match cursor.current() {
            None => return Err(WalkSpins),
            Some(Token::EndObject) => break,
            Some(token) => {
                if cnt >= arity {
                    skip_to_next_field(cursor)?;
                    continue;
                }
                let name = text_of(token).to_string();
                cursor.advance();
                match fields.iter().position(|f| f.name() == &name) {
                    Some(idx) => {
                        let snippet = convert_value(cursor, fields[idx].data_type(), env, &name)?;
                        if !env.lenient {
                            validate(&snippet, fields[idx].data_type(), env, &name);
                        }
                        slots[idx] = Some(snippet);
                        cursor.advance();
                        cnt += 1;
                    }
                    None => skip_to_next_field(cursor)?,
                }
            }
        }
    }
    let mut row = String::from("{");
    for (field, slot) in fields.iter().zip(&slots) {
        if let Some(snippet) = slot {
            if row.len() > 1 {
                row.push(',');
            }
            write_json_string(&mut row, field.name());
            row.push(':');
            row.push_str(snippet);
        }
    }
    row.push('}');
    Ok(row)
}

/// `skipToNextField`: a container skips to its balanced end, anything else is a single token;
/// both end with one more advance.
fn skip_to_next_field(cursor: &mut Cursor) -> Walk<()> {
    if matches!(cursor.current(), Some(Token::StartObject | Token::StartArray)) {
        let mut depth = 1;
        while depth > 0 {
            cursor.advance();
            match cursor.current() {
                None => return Err(WalkSpins),
                Some(Token::StartObject | Token::StartArray) => depth += 1,
                Some(Token::EndObject | Token::EndArray) => depth -= 1,
                Some(_) => {}
            }
        }
    }
    cursor.advance();
    Ok(())
}

/// One nullable-wrapped converter (`wrapIntoNullableConverter`): a missing/null token is a null
/// value, a conversion failure nulls the field in skip mode and fails the message in strict mode
/// with Flink's row-converter error. The returned snippet is the JSON value the fast path
/// re-decodes; the cursor consumes exactly what Flink's converter would.
fn convert_value(cursor: &mut Cursor, target: &DataType, env: JsonEnv, field: &str) -> Walk<String> {
    let Some(token) = cursor.current() else {
        return Ok("null".into());
    };
    if matches!(token, Token::Null) {
        return Ok("null".into());
    }
    match target {
        DataType::Struct(fields) => match token {
            Token::StartObject => row_object(cursor, fields, env),
            _ => Ok(fail_field(env, field)),
        },
        DataType::List(element) => match token {
            Token::StartArray => array_value(cursor, element.data_type(), env, field),
            _ => Ok(fail_field(env, field)),
        },
        DataType::Map(entries, _) => match token {
            Token::StartObject => map_value(cursor, entries, env, field),
            _ => Ok(fail_field(env, field)),
        },
        DataType::Utf8 => match token {
            // `convertToString` reads a container through `readValueAsTree` — a clean, balanced
            // consume even mid-drift.
            Token::StartObject | Token::StartArray => echo_snippet(cursor, env),
            token => Ok(quoted(text_of(token))),
        },
        DataType::Binary => match token {
            Token::Str(value) => Ok(quoted(value)),
            // `getBinaryValue` reads only a VALUE_STRING token and consumes nothing else.
            _ => Ok(fail_field(env, field)),
        },
        _ => Ok(scalar_snippet(token, target)),
    }
}

/// A scalar column's snippet for a non-null, non-string-special token. Container and structural
/// tokens coerce through `getText` (the drift source — the cursor stays put); number tokens keep
/// Jackson's token-type split via the spelling rules in the module docs.
fn scalar_snippet(token: &Token, target: &DataType) -> String {
    match token {
        Token::Number { raw, .. } => match target {
            // parseFloat/parseDouble read the literal text — FLOAT gets its single rounding.
            DataType::Float32 | DataType::Float64 => quoted(raw),
            _ if reparses(raw.as_bytes()) => raw.clone(),
            // An out-of-range literal: the appender's text parse fails (or coerces) the field
            // exactly like Jackson's number accessors on the raw literal.
            _ => quoted(raw),
        },
        Token::True => "true".into(),
        Token::False => "false".into(),
        token => quoted(text_of(token)),
    }
}

/// Whether the fast path can reparse this literal as a number token (simd-json rejects literals
/// beyond i64/u64/f64).
fn reparses(literal: &[u8]) -> bool {
    simd_json::to_tape(&mut literal.to_vec()).is_ok()
}

/// A per-field failure: NULL under `ignore-parse-errors`, Flink's row-converter error otherwise.
fn fail_field(env: JsonEnv, field: &str) -> String {
    assert!(env.lenient, "Fail to deserialize at field: {field}.");
    "null".into()
}

/// Strict mode converts every matched occurrence in document order (an occurrence a later
/// duplicate overwrites can still fail the message), so each snippet replays eagerly through a
/// throwaway fast-path appender — the coercion envelope stays in one place — and a failure
/// resurfaces as Flink's row-converter error.
fn validate(snippet: &str, target: &DataType, env: JsonEnv, field: &str) {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let outcome = crate::formats::silence_expected_decode_panics(|| {
        catch_unwind(AssertUnwindSafe(|| {
            let mut bytes = snippet.as_bytes().to_vec();
            let tape = simd_json::to_tape(&mut bytes).expect("sanitized snippet reparses");
            make_json_appender(target, 1, env).append(Some(tape.as_value()));
        }))
    });
    assert!(outcome.is_ok(), "Fail to deserialize at field: {field}.");
}

/// `createArrayConverter`: elements convert from wherever `nextToken` lands until an END_ARRAY
/// token arrives — mid-drift that may not be this array's own end.
fn array_value(cursor: &mut Cursor, element: &DataType, env: JsonEnv, field: &str) -> Walk<String> {
    let mut out = String::from("[");
    loop {
        cursor.advance();
        match cursor.current() {
            None => return Err(WalkSpins),
            Some(Token::EndArray) => break,
            Some(_) => {
                let snippet = convert_value(cursor, element, env, field)?;
                if out.len() > 1 {
                    out.push(',');
                }
                out.push_str(&snippet);
            }
        }
    }
    out.push(']');
    Ok(out)
}

/// `createMapConverter`: key/value pairs until an END_OBJECT token, each key read through the
/// string converter (a container key consumes its subtree), entries accumulated java.util.Map
/// style — one entry per key, last value, first position.
fn map_value(cursor: &mut Cursor, entries: &FieldRef, env: JsonEnv, field: &str) -> Walk<String> {
    let value_type = match entries.data_type() {
        DataType::Struct(kv) if kv.len() == 2 => kv[1].data_type(),
        other => panic!("MAP entries must be a two-field struct, got {other}"),
    };
    let mut keys: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    loop {
        cursor.advance();
        match cursor.current() {
            None => return Err(WalkSpins),
            Some(Token::EndObject) => break,
            Some(token) => {
                let key = match token {
                    Token::StartObject | Token::StartArray => {
                        let echoed = echo_snippet(cursor, env)?;
                        // The echo snippet is a JSON string (or `null` when unechoable in skip
                        // mode) — the key is its content.
                        unquote(&echoed)
                    }
                    token => text_of(token).to_string(),
                };
                cursor.advance();
                let value = convert_value(cursor, value_type, env, field)?;
                if !env.lenient {
                    validate(&value, value_type, env, field);
                }
                match keys.iter().position(|k| k == &key) {
                    Some(at) => values[at] = value,
                    None => {
                        keys.push(key);
                        values.push(value);
                    }
                }
            }
        }
    }
    let mut out = String::from("{");
    for (i, (key, value)) in keys.iter().zip(&values).enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_json_string(&mut out, key);
        out.push(':');
        out.push_str(value);
    }
    out.push('}');
    Ok(out)
}

/// The content of a snippet produced by [`echo_snippet`] (a JSON string literal or `null`).
fn unquote(snippet: &str) -> String {
    let mut bytes = snippet.as_bytes().to_vec();
    let tape = simd_json::to_tape(&mut bytes).expect("echo snippet reparses");
    use simd_json::prelude::*;
    tape.as_value().as_str().unwrap_or("null").to_string()
}

/// `readValueAsTree().toString()` under a STRING column: the balanced subtree at the cursor
/// serialized to Jackson's compact tree form — insertion-ordered keys, duplicates keeping first
/// position and last value, int literals echoing canonically. A float token cannot be re-rendered
/// exactly (raw literal vs `Double.toString` — divergences/21): strict fails, skip mode nulls the
/// whole echoed value, exactly like the fast path. The cursor ends on the subtree's closing token.
fn echo_snippet(cursor: &mut Cursor, env: JsonEnv) -> Walk<String> {
    let end = subtree_end(cursor);
    let echoable = !cursor.tokens[cursor.pos..=end]
        .iter()
        .any(|t| matches!(t, Token::Number { float: true, .. }));
    if !echoable {
        cursor.pos = end;
        assert!(
            env.lenient,
            "a float literal under a STRING column cannot be echoed exactly (Flink's tree \
             rendering re-spells it) — divergences/21"
        );
        return Ok("null".into());
    }
    let mut echoed = String::new();
    let mut pos = cursor.pos;
    echo_tokens(cursor.tokens, &mut pos, &mut echoed);
    cursor.pos = end;
    Ok(quoted(&echoed))
}

fn subtree_end(cursor: &Cursor) -> usize {
    let mut depth = 0;
    let mut pos = cursor.pos;
    loop {
        match cursor.tokens[pos] {
            Token::StartObject | Token::StartArray => depth += 1,
            Token::EndObject | Token::EndArray => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            return pos;
        }
        pos += 1;
    }
}

fn echo_tokens(tokens: &[Token], pos: &mut usize, out: &mut String) {
    match &tokens[*pos] {
        Token::StartObject => {
            *pos += 1;
            let mut keys: Vec<&str> = Vec::new();
            let mut values: Vec<String> = Vec::new();
            while !matches!(tokens[*pos], Token::EndObject) {
                let Token::FieldName(name) = &tokens[*pos] else {
                    unreachable!("the tokenizer emits field names inside objects")
                };
                *pos += 1;
                let mut value = String::new();
                echo_tokens(tokens, pos, &mut value);
                match keys.iter().position(|k| *k == name.as_str()) {
                    Some(at) => values[at] = value,
                    None => {
                        keys.push(name);
                        values.push(value);
                    }
                }
            }
            *pos += 1;
            out.push('{');
            for (i, (key, value)) in keys.iter().zip(&values).enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(out, key);
                out.push(':');
                out.push_str(value);
            }
            out.push('}');
        }
        Token::StartArray => {
            *pos += 1;
            out.push('[');
            let mut first = true;
            while !matches!(tokens[*pos], Token::EndArray) {
                if !first {
                    out.push(',');
                }
                first = false;
                echo_tokens(tokens, pos, out);
            }
            *pos += 1;
            out.push(']');
        }
        Token::Str(value) => {
            write_json_string(out, value);
            *pos += 1;
        }
        Token::Number { raw, float } => {
            assert!(!float, "float tokens are pre-checked before echoing");
            out.push_str(raw);
            *pos += 1;
        }
        Token::True => {
            out.push_str("true");
            *pos += 1;
        }
        Token::False => {
            out.push_str("false");
            *pos += 1;
        }
        Token::Null => {
            out.push_str("null");
            *pos += 1;
        }
        Token::FieldName(_) | Token::EndObject | Token::EndArray => {
            unreachable!("echo starts on a value token")
        }
    }
}

fn quoted(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    write_json_string(&mut out, value);
    out
}

/// Tokenizes one root JSON value the way Flink's Jackson parser is configured: strict JSON
/// grammar plus raw control characters inside strings, out-of-range numbers kept as raw text,
/// Jackson's default nesting (1000) and number-length (1000) caps, and — like the pull parser
/// Flink never reads past the root on — anything after the root value left untouched.
pub(crate) fn tokenize(bytes: &[u8]) -> Result<Vec<Token>, String> {
    let mut reader = Reader { bytes, pos: 0 };
    reader.skip_whitespace();
    if reader.pos == bytes.len() {
        return Ok(Vec::new());
    }
    let mut tokens = Vec::new();
    reader.read_value(&mut tokens, 0)?;
    Ok(tokens)
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn skip_whitespace(&mut self) {
        while matches!(self.bytes.get(self.pos), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Result<u8, String> {
        self.bytes.get(self.pos).copied().ok_or_else(|| "unexpected end-of-input".to_string())
    }

    fn read_value(&mut self, tokens: &mut Vec<Token>, depth: usize) -> Result<(), String> {
        if depth > 1000 {
            return Err("document nesting exceeds Jackson's depth limit (1000)".to_string());
        }
        match self.peek()? {
            b'{' => {
                self.pos += 1;
                tokens.push(Token::StartObject);
                self.skip_whitespace();
                if self.peek()? == b'}' {
                    self.pos += 1;
                    tokens.push(Token::EndObject);
                    return Ok(());
                }
                loop {
                    self.skip_whitespace();
                    if self.peek()? != b'"' {
                        return Err("expected a field name".to_string());
                    }
                    tokens.push(Token::FieldName(self.read_string()?));
                    self.skip_whitespace();
                    if self.peek()? != b':' {
                        return Err("expected ':' after a field name".to_string());
                    }
                    self.pos += 1;
                    self.skip_whitespace();
                    self.read_value(tokens, depth + 1)?;
                    self.skip_whitespace();
                    match self.peek()? {
                        b',' => self.pos += 1,
                        b'}' => {
                            self.pos += 1;
                            tokens.push(Token::EndObject);
                            return Ok(());
                        }
                        other => return Err(format!("unexpected character '{}'", other as char)),
                    }
                }
            }
            b'[' => {
                self.pos += 1;
                tokens.push(Token::StartArray);
                self.skip_whitespace();
                if self.peek()? == b']' {
                    self.pos += 1;
                    tokens.push(Token::EndArray);
                    return Ok(());
                }
                loop {
                    self.skip_whitespace();
                    self.read_value(tokens, depth + 1)?;
                    self.skip_whitespace();
                    match self.peek()? {
                        b',' => self.pos += 1,
                        b']' => {
                            self.pos += 1;
                            tokens.push(Token::EndArray);
                            return Ok(());
                        }
                        other => return Err(format!("unexpected character '{}'", other as char)),
                    }
                }
            }
            b'"' => {
                let value = self.read_string()?;
                tokens.push(Token::Str(value));
                Ok(())
            }
            b't' => self.read_literal(b"true", Token::True, tokens),
            b'f' => self.read_literal(b"false", Token::False, tokens),
            b'n' => self.read_literal(b"null", Token::Null, tokens),
            b'-' | b'0'..=b'9' => {
                tokens.push(self.read_number()?);
                Ok(())
            }
            other => Err(format!("unexpected character '{}'", other as char)),
        }
    }

    fn read_literal(
        &mut self,
        literal: &[u8],
        token: Token,
        tokens: &mut Vec<Token>,
    ) -> Result<(), String> {
        if self.bytes[self.pos..].starts_with(literal) {
            self.pos += literal.len();
            tokens.push(token);
            Ok(())
        } else {
            Err("unrecognized token".to_string())
        }
    }

    /// One string, unescaped. Raw control characters pass through (Flink enables
    /// `ALLOW_UNESCAPED_CONTROL_CHARS`); invalid UTF-8 and an unpaired `\u` surrogate fail the
    /// document (Jackson carries a lone surrogate in its UTF-16 text where Rust cannot —
    /// divergences/21).
    fn read_string(&mut self) -> Result<String, String> {
        self.pos += 1; // opening quote
        let mut out = String::new();
        loop {
            let chunk_start = self.pos;
            while !matches!(self.peek()?, b'"' | b'\\') {
                self.pos += 1;
            }
            out.push_str(
                std::str::from_utf8(&self.bytes[chunk_start..self.pos])
                    .map_err(|_| "invalid UTF-8 in a string".to_string())?,
            );
            if self.peek()? == b'"' {
                self.pos += 1;
                return Ok(out);
            }
            self.pos += 1; // backslash
            match self.peek()? {
                b'"' => out.push('"'),
                b'\\' => out.push('\\'),
                b'/' => out.push('/'),
                b'b' => out.push('\u{8}'),
                b'f' => out.push('\u{c}'),
                b'n' => out.push('\n'),
                b'r' => out.push('\r'),
                b't' => out.push('\t'),
                b'u' => {
                    self.pos += 1;
                    let unit = self.read_hex_unit()?;
                    let c = match unit {
                        0xD800..=0xDBFF => {
                            if self.bytes.get(self.pos..self.pos + 2) != Some(b"\\u") {
                                return Err("unpaired surrogate escape".to_string());
                            }
                            self.pos += 2;
                            let low = self.read_hex_unit()?;
                            if !(0xDC00..=0xDFFF).contains(&low) {
                                return Err("unpaired surrogate escape".to_string());
                            }
                            let combined =
                                0x10000 + ((unit - 0xD800) << 10) + (low - 0xDC00);
                            char::from_u32(combined).expect("valid surrogate pair")
                        }
                        0xDC00..=0xDFFF => return Err("unpaired surrogate escape".to_string()),
                        unit => char::from_u32(unit).expect("a BMP code point"),
                    };
                    out.push(c);
                    continue; // read_hex_unit already advanced past the digits
                }
                other => return Err(format!("invalid escape '\\{}'", other as char)),
            }
            self.pos += 1;
        }
    }

    /// Four hex digits of a `\u` escape; leaves the position after them.
    fn read_hex_unit(&mut self) -> Result<u32, String> {
        let digits = self
            .bytes
            .get(self.pos..self.pos + 4)
            .ok_or_else(|| "unexpected end-of-input".to_string())?;
        let text = std::str::from_utf8(digits).map_err(|_| "invalid unicode escape".to_string())?;
        let unit =
            u32::from_str_radix(text, 16).map_err(|_| "invalid unicode escape".to_string())?;
        self.pos += 4;
        Ok(unit)
    }

    /// Jackson's number grammar: optional '-', no leading zeros, an optional fraction and
    /// exponent each requiring digits, capped at Jackson's default 1000 characters.
    fn read_number(&mut self) -> Result<Token, String> {
        let start = self.pos;
        if self.peek()? == b'-' {
            self.pos += 1;
        }
        match self.peek()? {
            b'0' => {
                self.pos += 1;
                if matches!(self.bytes.get(self.pos), Some(b'0'..=b'9')) {
                    return Err("leading zeroes are not allowed".to_string());
                }
            }
            b'1'..=b'9' => self.read_digits()?,
            _ => return Err("a digit must follow '-'".to_string()),
        }
        let mut float = false;
        if self.bytes.get(self.pos) == Some(&b'.') {
            float = true;
            self.pos += 1;
            if !matches!(self.peek()?, b'0'..=b'9') {
                return Err("a digit must follow '.'".to_string());
            }
            self.read_digits()?;
        }
        if matches!(self.bytes.get(self.pos), Some(b'e' | b'E')) {
            float = true;
            self.pos += 1;
            if matches!(self.bytes.get(self.pos), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !matches!(self.peek()?, b'0'..=b'9') {
                return Err("a digit must follow the exponent".to_string());
            }
            self.read_digits()?;
        }
        if self.pos - start > 1000 {
            return Err("number length exceeds Jackson's limit (1000)".to_string());
        }
        let raw = std::str::from_utf8(&self.bytes[start..self.pos])
            .expect("a number literal is ASCII")
            .to_string();
        Ok(Token::Number { raw, float })
    }

    fn read_digits(&mut self) -> Result<(), String> {
        while matches!(self.bytes.get(self.pos), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
        Ok(())
    }
}
