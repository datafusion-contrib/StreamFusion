use crate::*;
use jni::objects::{GlobalRef, JBooleanArray, JValue};
use jni::JavaVM;

/// Which of Canal's two include options a verdict answers.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum IncludeField {
    Database,
    Table,
}

impl IncludeField {
    fn key(self) -> &'static str {
        match self {
            IncludeField::Database => "database",
            IncludeField::Table => "table",
        }
    }
}

/// The value an include pattern is matched against. Flink matches `root.get(key).asText()`:
/// strings, null, booleans and containers have a fixed `asText()` and arrive as that text, while a
/// number's text depends on Flink's ObjectMapper configuration (BigDecimal floats only when the
/// row type nests a DECIMAL), so it arrives as its raw JSON literal for the JVM to spell. So does
/// a string simd-json can't spell: it writes a lone high surrogate escape as U+0000, where
/// Jackson keeps the surrogate.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum IncludeToken {
    Text(String),
    Literal(Vec<u8>),
}

/// Decides values against one field's pattern with `java.util.regex` semantics: every value of a
/// batch the cache cannot answer, in one call.
pub(crate) trait IncludeVerdicts {
    fn matches(&self, field: IncludeField, tokens: &[IncludeToken]) -> Vec<bool>;
}

/// A message's fate under the filters.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Admission {
    Keep,
    Drop,
    /// The filter cannot read the message, exactly where Flink's deserializer throws (and
    /// `ignore-parse-errors` skips).
    Corrupt,
}

/// Canal's `database.include` / `table.include`. Flink applies them after parsing a message and
/// before converting it: a rejected message is dropped without ever failing on bad data, while an
/// unparseable message, a non-object root or a filtered key missing from the root is corrupt. The
/// database is checked first, and only a message it admits has its table read. Each distinct
/// value is decided once and cached, so a steady stream makes no JVM calls, and a batch's
/// undecided values go to the JVM together; the caches are bounded so value churn cannot grow
/// them without limit.
pub(crate) struct CanalIncludeFilter {
    fields: Vec<IncludeField>,
    verdicts: Box<dyn IncludeVerdicts>,
    state: std::cell::RefCell<FilterState>,
}

#[derive(Default)]
struct VerdictCache {
    texts: HashMap<String, bool>,
    literals: HashMap<Vec<u8>, bool>,
}

impl VerdictCache {
    fn get(&self, token: Token, arena: &[u8]) -> Option<bool> {
        match token.kind {
            TokenKind::Text => self.texts.get(token.text(arena)).copied(),
            TokenKind::Literal => self.literals.get(token.bytes(arena)).copied(),
            TokenKind::Missing => unreachable!("a missing value is never looked up"),
        }
    }

    fn len(&self) -> usize {
        self.texts.len() + self.literals.len()
    }

    fn insert(&mut self, token: IncludeToken, verdict: bool) {
        match token {
            IncludeToken::Text(text) => self.texts.insert(text, verdict),
            IncludeToken::Literal(literal) => self.literals.insert(literal, verdict),
        };
    }
}

const CACHED_VALUES: usize = 4096;

#[derive(Clone, Copy, PartialEq, Eq)]
enum TokenKind {
    Missing,
    Text,
    Literal,
}

/// A message's value for one field, as a span of the batch's token arena.
#[derive(Clone, Copy)]
struct Token {
    kind: TokenKind,
    start: usize,
    end: usize,
}

impl Token {
    const MISSING: Token = Token {
        kind: TokenKind::Missing,
        start: 0,
        end: 0,
    };

    fn bytes(self, arena: &[u8]) -> &[u8] {
        &arena[self.start..self.end]
    }

    fn text(self, arena: &[u8]) -> &str {
        std::str::from_utf8(self.bytes(arena)).expect("simd-json strings are UTF-8")
    }

    fn owned(self, arena: &[u8]) -> IncludeToken {
        match self.kind {
            TokenKind::Text => IncludeToken::Text(self.text(arena).to_owned()),
            TokenKind::Literal => IncludeToken::Literal(self.bytes(arena).to_vec()),
            TokenKind::Missing => unreachable!("a missing value is never decided"),
        }
    }
}

struct FilterState {
    caches: [VerdictCache; 2],
    scratch: Vec<u8>,
    buffers: simd_json::Buffers,
    tape: Option<simd_json::tape::Tape<'static>>,
    /// The batch's filtered values, back to back.
    arena: Vec<u8>,
}

impl CanalIncludeFilter {
    pub(crate) fn new(
        database: bool,
        table: bool,
        verdicts: Box<dyn IncludeVerdicts>,
    ) -> CanalIncludeFilter {
        let mut fields = Vec::new();
        if database {
            fields.push(IncludeField::Database);
        }
        if table {
            fields.push(IncludeField::Table);
        }
        CanalIncludeFilter {
            fields,
            verdicts,
            state: std::cell::RefCell::new(FilterState {
                caches: [VerdictCache::default(), VerdictCache::default()],
                scratch: Vec::new(),
                buffers: simd_json::Buffers::default(),
                tape: Some(simd_json::tape::Tape::null()),
                arena: Vec::new(),
            }),
        }
    }

    /// Each message's admission, in order. `messages` holds no tombstones.
    pub(crate) fn admit(&self, messages: &ArrayRef) -> Vec<Admission> {
        let mut state = self.state.borrow_mut();
        let state = &mut *state;
        state.arena.clear();
        let count = messages.len();
        let mut tokens: Vec<[Token; 2]> = Vec::with_capacity(count);
        let mut admissions = vec![Admission::Keep; count];
        for (row, admission) in admissions.iter_mut().enumerate() {
            let bytes = binary_body(messages, row).expect("tombstones are stripped first");
            state.scratch.clear();
            state.scratch.extend_from_slice(bytes);
            let mut tape = state
                .tape
                .take()
                .unwrap_or_else(simd_json::tape::Tape::null)
                .reset();
            let mut values = [Token::MISSING; 2];
            match simd_json::fill_tape(&mut state.scratch, &mut state.buffers, &mut tape) {
                Ok(()) => match tape.as_value().as_object() {
                    Some(object) => {
                        for (slot, &field) in self.fields.iter().enumerate() {
                            values[slot] = read_token(&object, field, bytes, &mut state.arena);
                        }
                    }
                    None => *admission = Admission::Corrupt,
                },
                Err(_) => *admission = Admission::Corrupt,
            }
            state.tape = Some(tape.reset());
            tokens.push(values);
        }
        for (slot, &field) in self.fields.iter().enumerate() {
            self.decide(field, slot, &tokens, &mut admissions, state);
        }
        admissions
    }

    /// Settles `field` for the messages every earlier filter kept, as Flink checks the database
    /// before reading the table: cached values directly, the rest in one JVM call.
    fn decide(
        &self,
        field: IncludeField,
        slot: usize,
        tokens: &[[Token; 2]],
        admissions: &mut [Admission],
        state: &mut FilterState,
    ) {
        let arena = &state.arena;
        let cache = &mut state.caches[field as usize];
        let mut pending: Vec<Token> = Vec::new();
        let mut pending_index: HashMap<(bool, &[u8]), usize> = HashMap::default();
        // Per message: its cached verdict, or its index among the pending values.
        let mut lookups: Vec<Result<bool, usize>> = vec![Ok(true); admissions.len()];
        for (row, admission) in admissions.iter_mut().enumerate() {
            if *admission != Admission::Keep {
                continue;
            }
            let token = tokens[row][slot];
            if token.kind == TokenKind::Missing {
                *admission = Admission::Corrupt;
                continue;
            }
            lookups[row] = match cache.get(token, arena) {
                Some(verdict) => Ok(verdict),
                None => Err(*pending_index
                    .entry((token.kind == TokenKind::Literal, token.bytes(arena)))
                    .or_insert_with(|| {
                        pending.push(token);
                        pending.len() - 1
                    })),
            };
        }
        if pending.is_empty() {
            for (admission, lookup) in admissions.iter_mut().zip(&lookups) {
                if *admission == Admission::Keep && *lookup == Ok(false) {
                    *admission = Admission::Drop;
                }
            }
            return;
        }
        let owned: Vec<IncludeToken> = pending.iter().map(|token| token.owned(arena)).collect();
        let verdicts = self.verdicts.matches(field, &owned);
        assert_eq!(verdicts.len(), owned.len(), "one verdict per value");
        for (admission, lookup) in admissions.iter_mut().zip(&lookups) {
            let verdict = match *lookup {
                Ok(verdict) => verdict,
                Err(index) => verdicts[index],
            };
            if *admission == Admission::Keep && !verdict {
                *admission = Admission::Drop;
            }
        }
        if cache.len() + owned.len() > CACHED_VALUES {
            *cache = VerdictCache::default();
        }
        for (token, verdict) in owned.into_iter().zip(verdicts).take(CACHED_VALUES) {
            cache.insert(token, verdict);
        }
    }
}

/// A field's value in `object` as Jackson's tree keeps it (the last of repeated keys), appended
/// to `arena` as the text Flink matches — or, for a number or a string simd-json can't spell, its
/// raw literal.
fn read_token(
    object: &simd_json::tape::Object<'_, '_>,
    field: IncludeField,
    message: &[u8],
    arena: &mut Vec<u8>,
) -> Token {
    use simd_json::prelude::*;
    let Some(value) = object
        .iter()
        .filter(|(key, _)| *key == field.key())
        .last()
        .map(|(_, value)| value)
    else {
        return Token::MISSING;
    };
    let start = arena.len();
    let kind = match value.value_type() {
        simd_json::ValueType::String => {
            let text = value.as_str().expect("string value");
            if text.contains('\0') {
                arena.extend_from_slice(
                    last_top_level_value(message, field.key())
                        .expect("validated message lost its string literal"),
                );
                TokenKind::Literal
            } else {
                arena.extend_from_slice(text.as_bytes());
                TokenKind::Text
            }
        }
        simd_json::ValueType::Null => {
            arena.extend_from_slice(b"null");
            TokenKind::Text
        }
        simd_json::ValueType::Bool => {
            let text: &[u8] = if value.as_bool().expect("bool value") {
                b"true"
            } else {
                b"false"
            };
            arena.extend_from_slice(text);
            TokenKind::Text
        }
        simd_json::ValueType::Object | simd_json::ValueType::Array => TokenKind::Text,
        _ => {
            arena.extend_from_slice(
                last_top_level_value(message, field.key())
                    .expect("validated message lost its number literal"),
            );
            TokenKind::Literal
        }
    };
    Token {
        kind,
        start,
        end: arena.len(),
    }
}

/// The raw bytes of the last top-level value under `key` in an already validated JSON object —
/// the literal Jackson's tree keeps for a repeated key.
fn last_top_level_value<'a>(message: &'a [u8], key: &str) -> Option<&'a [u8]> {
    let mut at = skip_whitespace(message, 0);
    if message.get(at) != Some(&b'{') {
        return None;
    }
    at += 1;
    let mut found = None;
    loop {
        at = skip_whitespace(message, at);
        if message.get(at) != Some(&b'"') {
            return found;
        }
        let end = skip_string(message, at);
        let name = unescape_json_string(&message[at + 1..end - 1]);
        at = skip_whitespace(message, end) + 1;
        let start = skip_whitespace(message, at);
        at = skip_value(message, start);
        if name == key {
            found = Some(&message[start..at]);
        }
        at = skip_whitespace(message, at);
        if message.get(at) == Some(&b',') {
            at += 1;
        }
    }
}

fn skip_whitespace(message: &[u8], mut at: usize) -> usize {
    while at < message.len() && matches!(message[at], b' ' | b'\t' | b'\n' | b'\r') {
        at += 1;
    }
    at
}

/// The index just past the string literal starting at `at` (its opening quote).
fn skip_string(message: &[u8], mut at: usize) -> usize {
    at += 1;
    while at < message.len() {
        match message[at] {
            b'\\' => at += 2,
            b'"' => return at + 1,
            _ => at += 1,
        }
    }
    at
}

fn skip_value(message: &[u8], at: usize) -> usize {
    match message.get(at) {
        Some(b'"') => skip_string(message, at),
        Some(b'{') | Some(b'[') => {
            let mut depth = 0usize;
            let mut cursor = at;
            while cursor < message.len() {
                match message[cursor] {
                    b'"' => {
                        cursor = skip_string(message, cursor);
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return cursor + 1;
                        }
                    }
                    _ => {}
                }
                cursor += 1;
            }
            cursor
        }
        _ => {
            let mut cursor = at;
            while cursor < message.len()
                && !matches!(
                    message[cursor],
                    b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r'
                )
            {
                cursor += 1;
            }
            cursor
        }
    }
}

/// A key's text after JSON unescaping; keys rarely carry escapes, so the plain case borrows.
fn unescape_json_string(raw: &[u8]) -> std::borrow::Cow<'_, str> {
    if !raw.contains(&b'\\') {
        return String::from_utf8_lossy(raw);
    }
    let quoted = [b"\"".as_slice(), raw, b"\"".as_slice()].concat();
    std::borrow::Cow::Owned(
        serde_json::from_slice::<String>(&quoted)
            .unwrap_or_else(|_| String::from_utf8_lossy(raw).into_owned()),
    )
}

/// Verdicts from the JVM: `java.util.regex` and, for literals, Flink's ObjectMapper spelling. The
/// decode runs on a JVM thread inside a JNI call, so the thread is already attached. The values go
/// over as one byte array with end offsets, and a literal flag each.
pub(crate) struct JvmIncludeVerdicts {
    pub(crate) vm: JavaVM,
    pub(crate) filter: GlobalRef,
}

impl IncludeVerdicts for JvmIncludeVerdicts {
    fn matches(&self, field: IncludeField, tokens: &[IncludeToken]) -> Vec<bool> {
        let mut env = self
            .vm
            .get_env()
            .expect("Canal include verdicts are requested on a JVM thread");
        let mut bytes = Vec::new();
        let mut ends = Vec::with_capacity(tokens.len());
        let mut literals = Vec::with_capacity(tokens.len());
        for token in tokens {
            match token {
                IncludeToken::Text(text) => {
                    bytes.extend_from_slice(text.as_bytes());
                    literals.push(0_u8);
                }
                IncludeToken::Literal(literal) => {
                    bytes.extend_from_slice(literal);
                    literals.push(1_u8);
                }
            }
            ends.push(i32::try_from(bytes.len()).expect("Canal include values exceed 2 GiB"));
        }
        let count = tokens.len() as i32;
        let outcome = env.with_local_frame(8, |env| -> jni::errors::Result<Vec<bool>> {
            let values = env.byte_array_from_slice(&bytes)?;
            let value_ends = env.new_int_array(count)?;
            env.set_int_array_region(&value_ends, 0, &ends)?;
            let literal_flags = env.new_boolean_array(count)?;
            env.set_boolean_array_region(&literal_flags, 0, &literals)?;
            let result = env
                .call_method(
                    &self.filter,
                    "matches",
                    "(I[B[I[Z)[Z",
                    &[
                        JValue::Int(field as i32),
                        JValue::Object(&values),
                        JValue::Object(&value_ends),
                        JValue::Object(&literal_flags),
                    ],
                )?
                .l()?;
            let result = JBooleanArray::from(result);
            let mut verdicts = vec![0_u8; tokens.len()];
            env.get_boolean_array_region(&result, 0, &mut verdicts)?;
            Ok(verdicts.into_iter().map(|verdict| verdict != 0).collect())
        });
        match outcome {
            Ok(verdicts) => verdicts,
            Err(error) => {
                // Its own frame, so a skip-mode retry that fails message after message doesn't
                // pile local references into the native call.
                let detail = env
                    .with_local_frame(4, |env| -> jni::errors::Result<String> {
                        if !env.exception_check()? {
                            return Ok(error.to_string());
                        }
                        let thrown = env.exception_occurred()?;
                        env.exception_clear()?;
                        let text = env
                            .call_method(&thrown, "toString", "()Ljava/lang/String;", &[])?
                            .l()?;
                        Ok(env.get_string(&text.into())?.into())
                    })
                    .unwrap_or_else(|_| error.to_string());
                let _ = env.exception_clear();
                panic!("Canal include filter failed: {detail}")
            }
        }
    }
}
