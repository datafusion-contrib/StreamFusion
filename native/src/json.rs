use crate::flink_text;
use crate::*;

/// Decodes a column of raw JSON message bodies — one complete document per row, as a source hands
/// them off untouched — into a typed Arrow batch matching `schema`. This replaces Flink's per-record
/// `byte[] -> tree -> RowData` materialization with a single batched decode straight to columnar
/// form, so the row representation never exists on the hot ingest path. The body column may arrive as
/// binary or string (whichever the source-edge transpose produced for the message bytes).
/// One column's JSON→Arrow appender in the simd-json decode path: a schema-driven walk of the parse
/// tape appending straight into a typed builder. `None` (a field absent from the object) and an
/// explicit JSON null both append SQL NULL. The per-type semantics replicate Flink's own JSON
/// converters (`JsonParserToRowDataConverters` — string-encoded numbers parse with a trim, floats
/// truncate toward zero into INT/BIGINT columns (the narrow integers reject float tokens),
/// booleans never fail, temporals follow the table's `timestamp-format.standard`), pinned by a
/// per-message parity test against Flink's deserializer; the deliberate residual leniencies live
/// in divergences/21.
pub(crate) trait JsonAppend {
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>);
    /// Appends a JSON object key (always a raw string): scalar targets parse it exactly like a
    /// string-positioned value. Only map key columns reach this.
    fn append_key(&mut self, key: &str);
    fn finish(&mut self) -> ArrayRef;
}

/// The decode envelope every appender follows: the table's `timestamp-format.standard`, and
/// whether `ignore-parse-errors` is on. Flink's skip mode is per-FIELD at every nesting level
/// (each converter is wrapped in a catch that nulls just that value — `wrapIntoNullableConverter`),
/// so under `lenient` an appender never fails: it appends SQL NULL where the strict mode would
/// fail the job. That also guarantees builders never see a partial container append.
#[derive(Clone, Copy, Default)]
pub(crate) struct JsonEnv {
    pub(crate) mode: flink_text::TimestampMode,
    pub(crate) lenient: bool,
    /// How duplicate keys inside one row object bind to fields. Plain `json` runs Flink's
    /// parser-path row converter, whose field counter saturates at the arity and skips the
    /// remaining keys (the default, `false`); the CDC dialects run Flink's tree-based
    /// deserializer (`readTree` + `JsonToRowDataConverters`), where the object is rebuilt as a
    /// map first — the last occurrence simply wins and no counter exists (`true`).
    pub(crate) tree_duplicates: bool,
}

/// Integer columns: number tokens convert through `NumCast` (a float truncates toward zero, out of
/// range fails — Jackson's `getIntValue` family), strings parse per Java's `parseInt` over trimmed
/// text. INT and BIGINT accept float tokens (`convertToInt`/`convertToLong` truncate); TINYINT and
/// SMALLINT do not — Flink's `convertToByte`/`convertToShort` fall through to `parseByte` over the
/// raw literal, which no float literal survives.
pub(crate) struct PrimitiveJsonAppender<T: ArrowPrimitiveType> {
    builder: PrimitiveBuilder<T>,
    data_type: DataType,
    float_tokens: bool,
    env: JsonEnv,
}

impl<T: ArrowPrimitiveType> PrimitiveJsonAppender<T> {
    fn new(
        data_type: &DataType,
        capacity: usize,
        float_tokens: bool,
        env: JsonEnv,
    ) -> PrimitiveJsonAppender<T> {
        PrimitiveJsonAppender {
            builder: PrimitiveBuilder::<T>::with_capacity(capacity)
                .with_data_type(data_type.clone()),
            data_type: data_type.clone(),
            float_tokens,
            env,
        }
    }
}

impl<T> JsonAppend for PrimitiveJsonAppender<T>
where
    T: ArrowPrimitiveType,
    T::Native: num_traits::NumCast + std::str::FromStr,
{
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>) {
        use num_traits::NumCast;
        use simd_json::prelude::*;
        let Some(v) = value else {
            self.builder.append_null();
            return;
        };
        let parsed: Option<T::Native> = match v.value_type() {
            simd_json::ValueType::Null => {
                self.builder.append_null();
                return;
            }
            simd_json::ValueType::String => {
                self.append_key(v.as_str().expect("string node"));
                return;
            }
            simd_json::ValueType::I64 => NumCast::from(v.as_i64().expect("i64 node")),
            simd_json::ValueType::U64 => NumCast::from(v.as_u64().expect("u64 node")),
            simd_json::ValueType::F64 if self.float_tokens => {
                NumCast::from(v.as_f64().expect("f64 node"))
            }
            simd_json::ValueType::F64 => None, // no float literal parses as a Java byte/short
            other if self.env.lenient => {
                let _ = other;
                None
            }
            other => panic!("failed to decode JSON {other:?} as {}", self.data_type),
        };
        match parsed {
            Some(parsed) => self.builder.append_value(parsed),
            None if self.env.lenient => self.builder.append_null(),
            None => panic!("JSON number out of range for {}", self.data_type),
        }
    }

    fn append_key(&mut self, key: &str) {
        match flink_text::parse_java_integer(key.trim()) {
            Some(parsed) => self.builder.append_value(parsed),
            None if self.env.lenient => self.builder.append_null(),
            None => panic!("failed to parse \"{key}\" as {}", self.data_type),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

/// Float columns: number tokens pass through, strings follow Java's `parseDouble`/`parseFloat`
/// envelope (`Infinity`/`NaN`, an `f`/`d` suffix, self-trimming). A number token rides the tape's
/// f64; for FLOAT columns the narrowing equals Jackson's direct-to-float literal parse everywhere
/// except exactly at an f32 rounding midpoint — the plain `json` format re-decodes those messages
/// through the token walk, which re-parses the raw literal at float width
/// ([`f32_parse_ambiguous`]).
pub(crate) struct FloatJsonAppender<T: ArrowPrimitiveType> {
    builder: PrimitiveBuilder<T>,
    data_type: DataType,
    env: JsonEnv,
}

impl<T: ArrowPrimitiveType> FloatJsonAppender<T> {
    fn new(data_type: &DataType, capacity: usize, env: JsonEnv) -> FloatJsonAppender<T> {
        FloatJsonAppender {
            builder: PrimitiveBuilder::<T>::with_capacity(capacity)
                .with_data_type(data_type.clone()),
            data_type: data_type.clone(),
            env,
        }
    }
}

impl<T> JsonAppend for FloatJsonAppender<T>
where
    T: ArrowPrimitiveType,
    T::Native: num_traits::NumCast + flink_text::JavaFloat,
{
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>) {
        use num_traits::NumCast;
        use simd_json::prelude::*;
        let Some(v) = value else {
            self.builder.append_null();
            return;
        };
        let parsed: Option<T::Native> = match v.value_type() {
            simd_json::ValueType::Null => {
                self.builder.append_null();
                return;
            }
            simd_json::ValueType::String => {
                self.append_key(v.as_str().expect("string node"));
                return;
            }
            simd_json::ValueType::I64 => NumCast::from(v.as_i64().expect("i64 node")),
            simd_json::ValueType::U64 => NumCast::from(v.as_u64().expect("u64 node")),
            simd_json::ValueType::F64 => NumCast::from(v.as_f64().expect("f64 node")),
            other if self.env.lenient => {
                let _ = other;
                None
            }
            other => panic!("failed to decode JSON {other:?} as {}", self.data_type),
        };
        match parsed {
            Some(parsed) => self.builder.append_value(parsed),
            None if self.env.lenient => self.builder.append_null(),
            None => panic!("JSON number out of range for {}", self.data_type),
        }
    }

    fn append_key(&mut self, key: &str) {
        match flink_text::parse_java_float::<T::Native>(key) {
            Some(parsed) => self.builder.append_value(parsed),
            None if self.env.lenient => self.builder.append_null(),
            None => panic!("failed to parse \"{key}\" as {}", self.data_type),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

/// DATE: strings parse per Java's strict `ISO_LOCAL_DATE` (`yyyy-MM-dd`, a real calendar date) —
/// Flink rejects everything else, bare numbers included (its converter renders the token to text
/// and hands it to the date formatter, which fails on digits).
pub(crate) struct DateJsonAppender {
    builder: PrimitiveBuilder<Date32Type>,
    env: JsonEnv,
}

impl JsonAppend for DateJsonAppender {
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>) {
        use simd_json::prelude::*;
        let Some(v) = value else {
            self.builder.append_null();
            return;
        };
        match v.value_type() {
            simd_json::ValueType::Null => self.builder.append_null(),
            simd_json::ValueType::String => self.append_key(v.as_str().expect("string node")),
            _ if self.env.lenient => self.builder.append_null(),
            other => panic!("failed to decode JSON {other:?} as DATE"),
        }
    }

    fn append_key(&mut self, key: &str) {
        match flink_text::parse_iso_local_date(key) {
            Some(days) => self.builder.append_value(days),
            None if self.env.lenient => self.builder.append_null(),
            None => panic!("failed to parse \"{key}\" as DATE"),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

/// TIMESTAMP / TIMESTAMP_LTZ (nanosecond): strings parse per the table's
/// `timestamp-format.standard` — Flink's SQL (`yyyy-MM-dd HH:mm:ss[.f]`) or ISO-8601
/// (`yyyy-MM-dd'T'HH:mm[:ss[.f]]`) formatter, nothing else. A bare number fails, as it does in
/// Flink (the converter renders the token to text and the formatter rejects digits). A trailing
/// 'Z' is tolerated either way (divergences/21 — the boundary schema carries no LTZ marker).
pub(crate) struct TimestampJsonAppender {
    builder: PrimitiveBuilder<TimestampNanosecondType>,
    data_type: DataType,
    env: JsonEnv,
}

impl TimestampJsonAppender {
    fn new(data_type: &DataType, capacity: usize, env: JsonEnv) -> TimestampJsonAppender {
        TimestampJsonAppender {
            builder: PrimitiveBuilder::with_capacity(capacity).with_data_type(data_type.clone()),
            data_type: data_type.clone(),
            env,
        }
    }
}

impl JsonAppend for TimestampJsonAppender {
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>) {
        use simd_json::prelude::*;
        let Some(v) = value else {
            self.builder.append_null();
            return;
        };
        match v.value_type() {
            simd_json::ValueType::Null => self.builder.append_null(),
            simd_json::ValueType::String => self.append_key(v.as_str().expect("string node")),
            _ if self.env.lenient => self.builder.append_null(),
            other => panic!("failed to decode JSON {other:?} as {}", self.data_type),
        }
    }

    fn append_key(&mut self, key: &str) {
        match flink_text::parse_flink_timestamp(key, self.env.mode) {
            Some(nanos) => self.builder.append_value(nanos),
            None if self.env.lenient => self.builder.append_null(),
            None => panic!("failed to parse \"{key}\" as {}", self.data_type),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

/// TIME: strings parse per Flink's `SQL_TIME_FORMAT` (`HH:mm:ss` + optional fraction), then the
/// fraction is DISCARDED — Flink stores `toSecondOfDay() * 1000` whatever the declared precision —
/// so the appended value is whole seconds scaled to the column's Arrow time unit. A bare number
/// fails, as in Flink (the formatter rejects digits).
pub(crate) struct TimeJsonAppender<T: ArrowPrimitiveType> {
    builder: PrimitiveBuilder<T>,
    data_type: DataType,
    per_second: i64,
    env: JsonEnv,
}

impl<T: ArrowPrimitiveType> TimeJsonAppender<T> {
    fn new(data_type: &DataType, capacity: usize, per_second: i64, env: JsonEnv) -> TimeJsonAppender<T> {
        TimeJsonAppender {
            builder: PrimitiveBuilder::<T>::with_capacity(capacity)
                .with_data_type(data_type.clone()),
            data_type: data_type.clone(),
            per_second,
            env,
        }
    }
}

impl<T> JsonAppend for TimeJsonAppender<T>
where
    T: ArrowPrimitiveType,
    T::Native: num_traits::NumCast,
{
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>) {
        use simd_json::prelude::*;
        let Some(v) = value else {
            self.builder.append_null();
            return;
        };
        match v.value_type() {
            simd_json::ValueType::Null => self.builder.append_null(),
            simd_json::ValueType::String => self.append_key(v.as_str().expect("string node")),
            _ if self.env.lenient => self.builder.append_null(),
            other => panic!("failed to decode JSON {other:?} as {}", self.data_type),
        }
    }

    fn append_key(&mut self, key: &str) {
        let value = flink_text::parse_sql_time_second_of_day(key)
            .and_then(|seconds| num_traits::NumCast::from(seconds * self.per_second));
        match value {
            Some(value) => self.builder.append_value(value),
            None if self.env.lenient => self.builder.append_null(),
            None => panic!("failed to parse \"{key}\" as {}", self.data_type),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

/// VARBINARY: strings base64-decode with Jackson's exact read (`JsonParser.getBinaryValue`);
/// declared length is not enforced, matching Flink. Every non-string token fails, as
/// `getBinaryValue` does. (BINARY is gated out at plan time — its fixed-size Arrow carriage
/// cannot hold an arbitrary-length decode.)
pub(crate) struct BinaryJsonAppender {
    builder: BinaryBuilder,
    env: JsonEnv,
}

impl JsonAppend for BinaryJsonAppender {
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>) {
        use simd_json::prelude::*;
        let Some(v) = value else {
            self.builder.append_null();
            return;
        };
        match v.value_type() {
            simd_json::ValueType::Null => self.builder.append_null(),
            simd_json::ValueType::String => self.append_key(v.as_str().expect("string node")),
            _ if self.env.lenient => self.builder.append_null(),
            other => panic!("failed to decode JSON {other:?} as VARBINARY"),
        }
    }

    fn append_key(&mut self, key: &str) {
        match flink_text::parse_jackson_base64(key) {
            Ok(bytes) => self.builder.append_value(bytes),
            // A quote-consuming shape never reaches a lenient appender — the message-level
            // pre-scan drops the whole document first, as Flink's corrupted parser does.
            Err(_) if self.env.lenient => self.builder.append_null(),
            Err(_) => panic!("failed to decode base64 \"{key}\" as VARBINARY"),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

pub(crate) struct BooleanJsonAppender {
    builder: BooleanBuilder,
    env: JsonEnv,
}

impl JsonAppend for BooleanJsonAppender {
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>) {
        use simd_json::prelude::*;
        let Some(v) = value else {
            self.builder.append_null();
            return;
        };
        // Flink's converter never fails a scalar here: a non-boolean token is rendered to text and
        // fed to Boolean.parseBoolean, so a number is simply false.
        match v.value_type() {
            simd_json::ValueType::Null => self.builder.append_null(),
            simd_json::ValueType::Bool => {
                self.builder.append_value(v.as_bool().expect("bool node"))
            }
            simd_json::ValueType::String => self.append_key(v.as_str().expect("string node")),
            simd_json::ValueType::I64 | simd_json::ValueType::U64 | simd_json::ValueType::F64 => {
                self.builder.append_value(false)
            }
            _ if self.env.lenient => self.builder.append_null(),
            other => panic!("failed to decode JSON {other:?} as BOOLEAN"),
        }
    }

    fn append_key(&mut self, key: &str) {
        self.builder.append_value(flink_text::parse_java_boolean(key.trim()));
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

pub(crate) struct StringJsonAppender {
    builder: StringBuilder,
    env: JsonEnv,
}

impl JsonAppend for StringJsonAppender {
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>) {
        use simd_json::prelude::*;
        let Some(v) = value else {
            self.builder.append_null();
            return;
        };
        // Flink coerces any value to text under a STRING column: scalars echo their literal,
        // containers serialize to compact JSON. Integer and boolean tokens echo exactly (a JSON
        // integer literal has one canonical form); a float token's raw literal is gone after the
        // tape parse (`1.50` and `1.5e0` are indistinguishable from `1.5`) and Flink's own two
        // decode paths already disagree on how to render it, so it fails loudly here instead of
        // silently diverging — divergences/21.
        match v.value_type() {
            simd_json::ValueType::Null => self.builder.append_null(),
            simd_json::ValueType::String => {
                self.builder.append_value(v.as_str().expect("string node"))
            }
            simd_json::ValueType::I64 => {
                self.builder.append_value(v.as_i64().expect("i64 node").to_string())
            }
            simd_json::ValueType::U64 => {
                self.builder.append_value(v.as_u64().expect("u64 node").to_string())
            }
            simd_json::ValueType::Bool => self
                .builder
                .append_value(if v.as_bool().expect("bool node") { "true" } else { "false" }),
            simd_json::ValueType::F64 if self.env.lenient => self.builder.append_null(),
            simd_json::ValueType::F64 => panic!(
                "a float literal under a STRING column cannot be echoed exactly (raw literal \
                 lost in the parse) — divergences/21"
            ),
            simd_json::ValueType::Object | simd_json::ValueType::Array
                if self.env.lenient && !json_echoable(v) =>
            {
                self.builder.append_null()
            }
            simd_json::ValueType::Object | simd_json::ValueType::Array => {
                let mut out = String::new();
                write_json_value(&mut out, v);
                self.builder.append_value(out);
            }
            other => panic!("failed to decode JSON {other:?} as VARCHAR"),
        }
    }

    fn append_key(&mut self, key: &str) {
        self.builder.append_value(key);
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

/// Whether a container subtree can be echoed exactly under a STRING column — i.e. it holds no
/// float token, whose raw literal the tape parse discards (divergences/21). The lenient mode
/// pre-checks this so a non-echoable value nulls the field instead of failing mid-serialization.
fn json_echoable(value: simd_json::tape::Value<'_, '_>) -> bool {
    use simd_json::prelude::*;
    match value.value_type() {
        simd_json::ValueType::F64 => false,
        simd_json::ValueType::Object => {
            value.as_object().expect("object node").iter().all(|(_, v)| json_echoable(v))
        }
        simd_json::ValueType::Array => {
            value.as_array().expect("array node").iter().all(json_echoable)
        }
        _ => true,
    }
}

/// Serializes a tape subtree back to the compact JSON text Jackson's `JsonNode.toString` produces
/// for a container under a STRING column: no whitespace, insertion-ordered keys with a duplicate
/// key keeping its first position and last value (Jackson's tree is a LinkedHashMap), standard
/// escaping, non-ASCII characters raw. Float tokens fail as in the scalar case (raw literal lost).
fn write_json_value(out: &mut String, value: simd_json::tape::Value<'_, '_>) {
    use simd_json::prelude::*;
    match value.value_type() {
        simd_json::ValueType::Null => out.push_str("null"),
        simd_json::ValueType::Bool => {
            out.push_str(if value.as_bool().expect("bool node") { "true" } else { "false" })
        }
        simd_json::ValueType::I64 => {
            out.push_str(&value.as_i64().expect("i64 node").to_string())
        }
        simd_json::ValueType::U64 => {
            out.push_str(&value.as_u64().expect("u64 node").to_string())
        }
        simd_json::ValueType::F64 => panic!(
            "a float literal inside a JSON value under a STRING column cannot be echoed exactly \
             (raw literal lost in the parse) — divergences/21"
        ),
        simd_json::ValueType::String => {
            write_json_string(out, value.as_str().expect("string node"))
        }
        simd_json::ValueType::Object => {
            let object = value.as_object().expect("object node");
            let mut keys: Vec<&str> = Vec::with_capacity(object.len());
            let mut values: Vec<simd_json::tape::Value> = Vec::with_capacity(object.len());
            for (key, entry) in &object {
                let existing = keys.iter().position(|k| *k == key);
                match existing {
                    Some(i) => values[i] = entry, // duplicate key: last value, first position
                    None => {
                        keys.push(key);
                        values.push(entry);
                    }
                }
            }
            out.push('{');
            for (i, (key, entry)) in keys.iter().zip(&values).enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(out, key);
                out.push(':');
                write_json_value(out, *entry);
            }
            out.push('}');
        }
        simd_json::ValueType::Array => {
            out.push('[');
            for (i, entry) in value.as_array().expect("array node").iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_value(out, entry);
            }
            out.push(']');
        }
        other => panic!("cannot serialize JSON {other:?}"),
    }
}

/// JSON string escaping matching Jackson's writer: quote/backslash escaped, the short control
/// escapes for \b \t \n \f \r, \u00XX for the other control characters, everything else raw.
pub(crate) fn write_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => {
                // Jackson writes the remaining control characters with uppercase hex.
                out.push_str(&format!("\\u{:04X}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

pub(crate) struct StructJsonAppender {
    fields: Fields,
    env: JsonEnv,
    children: Vec<Box<dyn JsonAppend>>,
    /// Name→child lookup above a linear-scan threshold (arrow-json's heuristic: a map only pays for
    /// itself on wide structs).
    index: Option<HashMap<String, usize>>,
    nulls: NullBufferBuilder,
}

impl StructJsonAppender {
    fn new(fields: &Fields, capacity: usize, env: JsonEnv) -> StructJsonAppender {
        let children =
            fields.iter().map(|f| make_json_appender(f.data_type(), capacity, env)).collect();
        let index = (fields.len() >= 16).then(|| {
            let mut map = HashMap::with_capacity_and_hasher(fields.len(), Default::default());
            for (i, field) in fields.iter().enumerate() {
                map.entry(field.name().clone()).or_insert(i);
            }
            map
        });
        StructJsonAppender {
            fields: fields.clone(),
            env,
            children,
            index,
            nulls: NullBufferBuilder::new(capacity),
        }
    }

    fn field_index(&self, name: &str) -> Option<usize> {
        match &self.index {
            Some(map) => map.get(name).copied(),
            None => self.fields.iter().position(|f| f.name() == name),
        }
    }

    /// Collects the last value per field first, then appends one value per child so every column
    /// stays row-aligned. Key matching follows the env's duplicate-key mode: on the parser path
    /// (`JsonParserToRowDataConverters.createRowConverter`, plain `json`) every matched key
    /// occurrence — a duplicate included — advances a field counter, and once it reaches the
    /// arity the remaining keys are SKIPPED, so a late duplicate never overwrites the earlier
    /// value; on the tree path (the CDC dialects) the last occurrence wins unconditionally.
    /// Unknown keys are ignored and never advance the counter.
    fn append_object(&mut self, object: &simd_json::tape::Object<'_, '_>) {
        let appended = self.try_append_object(object, false);
        debug_assert!(appended, "cursor drift is cleared at the message root");
    }

    /// Like [`Self::append_object`], but when `check_drift` is set and a collected slot would make
    /// Flink's pull-parser cursor drift ([`value_needs_token_walk`]), nothing is appended and `false`
    /// returns — the caller re-decodes the whole message through the Jackson-faithful walk. The
    /// check rides the collected slots, so a drift-free scalar row (the common case) pays one
    /// token-type test per slot; only container-valued slots re-walk their subtree.
    fn try_append_object(
        &mut self,
        object: &simd_json::tape::Object<'_, '_>,
        check_drift: bool,
    ) -> bool {
        const STACK_FIELDS: usize = 32;
        let count = self.children.len();
        let mut stack = [None; STACK_FIELDS];
        let mut heap = Vec::new();
        let slots: &mut [Option<simd_json::tape::Value>] = if count <= STACK_FIELDS {
            &mut stack[..count]
        } else {
            heap.resize(count, None);
            &mut heap
        };
        let mut matched = 0;
        for (key, value) in object {
            if matched == count {
                break;
            }
            if let Some(i) = self.field_index(key) {
                slots[i] = Some(value);
                if !self.env.tree_duplicates {
                    matched += 1;
                }
            }
        }
        if check_drift
            && self.fields.iter().zip(slots.iter()).any(|(field, slot)| {
                slot.is_some_and(|value| value_needs_token_walk(field.data_type(), value))
            })
        {
            return false;
        }
        for (child, slot) in self.children.iter_mut().zip(slots.iter()) {
            child.append(*slot);
        }
        true
    }

    fn finish_columns(&mut self) -> Vec<ArrayRef> {
        self.children.iter_mut().map(|c| c.finish()).collect()
    }
}

impl JsonAppend for StructJsonAppender {
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>) {
        use simd_json::prelude::*;
        let object = value.and_then(|v| match v.value_type() {
            simd_json::ValueType::Null => None,
            _ => match v.as_object() {
                Some(object) => Some(object),
                None if self.env.lenient => None,
                None => panic!("failed to decode JSON {:?} as ROW", v.value_type()),
            },
        });
        match object {
            None => {
                self.nulls.append_null();
                for child in &mut self.children {
                    child.append(None);
                }
            }
            Some(object) => {
                self.nulls.append_non_null();
                self.append_object(&object);
            }
        }
    }

    fn append_key(&mut self, key: &str) {
        panic!("failed to parse map key \"{key}\" as ROW");
    }

    fn finish(&mut self) -> ArrayRef {
        let columns = self.finish_columns();
        let nulls = self.nulls.finish();
        Arc::new(
            StructArray::try_new(self.fields.clone(), columns, nulls)
                .expect("failed to build JSON struct column"),
        )
    }
}

pub(crate) struct ListJsonAppender {
    field: FieldRef,
    env: JsonEnv,
    child: Box<dyn JsonAppend>,
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
}

impl ListJsonAppender {
    fn new(field: &FieldRef, capacity: usize, env: JsonEnv) -> ListJsonAppender {
        ListJsonAppender {
            field: field.clone(),
            env,
            child: make_json_appender(field.data_type(), capacity, env),
            offsets: vec![0],
            nulls: NullBufferBuilder::new(capacity),
        }
    }
}

impl JsonAppend for ListJsonAppender {
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>) {
        use simd_json::prelude::*;
        let array = value.and_then(|v| match v.value_type() {
            simd_json::ValueType::Null => None,
            _ => match v.as_array() {
                Some(array) => Some(array),
                None if self.env.lenient => None,
                None => panic!("failed to decode JSON {:?} as ARRAY", v.value_type()),
            },
        });
        let mut end = *self.offsets.last().expect("non-empty offsets");
        match array {
            None => self.nulls.append_null(),
            Some(array) => {
                self.nulls.append_non_null();
                for element in &array {
                    self.child.append(Some(element));
                    end = end.checked_add(1).expect("offset overflow decoding ARRAY");
                }
            }
        }
        self.offsets.push(end);
    }

    fn append_key(&mut self, key: &str) {
        panic!("failed to parse map key \"{key}\" as ARRAY");
    }

    fn finish(&mut self) -> ArrayRef {
        let values = self.child.finish();
        let offsets =
            OffsetBuffer::new(ScalarBuffer::from(std::mem::replace(&mut self.offsets, vec![0])));
        Arc::new(
            ListArray::try_new(self.field.clone(), offsets, values, self.nulls.finish())
                .expect("failed to build JSON array column"),
        )
    }
}

/// MAP (and MULTISET riding as `MAP<E, INT>`): a JSON object per row, each key parsed by the key
/// column's scalar appender and each value decoded normally.
pub(crate) struct MapJsonAppender {
    entries_field: FieldRef,
    env: JsonEnv,
    entry_fields: Fields,
    keys: Box<dyn JsonAppend>,
    values: Box<dyn JsonAppend>,
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
}

impl MapJsonAppender {
    fn new(entries_field: &FieldRef, capacity: usize, env: JsonEnv) -> MapJsonAppender {
        let entry_fields = match entries_field.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => fields.clone(),
            other => panic!("MAP entries must be a two-field struct, got {other}"),
        };
        MapJsonAppender {
            entries_field: entries_field.clone(),
            env,
            keys: make_json_appender(entry_fields[0].data_type(), capacity, env),
            values: make_json_appender(entry_fields[1].data_type(), capacity, env),
            entry_fields,
            offsets: vec![0],
            nulls: NullBufferBuilder::new(capacity),
        }
    }
}

impl JsonAppend for MapJsonAppender {
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>) {
        use simd_json::prelude::*;
        let object = value.and_then(|v| match v.value_type() {
            simd_json::ValueType::Null => None,
            _ => match v.as_object() {
                Some(object) => Some(object),
                None if self.env.lenient => None,
                None => panic!("failed to decode JSON {:?} as MAP", v.value_type()),
            },
        });
        let mut end = *self.offsets.last().expect("non-empty offsets");
        match object {
            None => self.nulls.append_null(),
            Some(object) => {
                self.nulls.append_non_null();
                // Duplicate keys collapse last-value-first-position: Flink's converter builds a
                // java.util.Map, so a repeated key holds one entry with the final value.
                let mut keys: Vec<&str> = Vec::with_capacity(object.len());
                let mut values: Vec<simd_json::tape::Value> = Vec::with_capacity(object.len());
                for (key, value) in &object {
                    let existing = keys.iter().position(|k| *k == key);
                    match existing {
                        Some(i) => values[i] = value,
                        None => {
                            keys.push(key);
                            values.push(value);
                        }
                    }
                }
                for (key, value) in keys.iter().zip(&values) {
                    self.keys.append_key(key);
                    self.values.append(Some(*value));
                    end = end.checked_add(1).expect("offset overflow decoding MAP");
                }
            }
        }
        self.offsets.push(end);
    }

    fn append_key(&mut self, key: &str) {
        panic!("failed to parse map key \"{key}\" as MAP");
    }

    fn finish(&mut self) -> ArrayRef {
        let entries = StructArray::try_new(
            self.entry_fields.clone(),
            vec![self.keys.finish(), self.values.finish()],
            None,
        )
        .expect("failed to build JSON map entries");
        let offsets =
            OffsetBuffer::new(ScalarBuffer::from(std::mem::replace(&mut self.offsets, vec![0])));
        Arc::new(
            MapArray::try_new(self.entries_field.clone(), offsets, entries, self.nulls.finish(), false)
                .expect("failed to build JSON map column"),
        )
    }
}

/// The types here are exactly the ones the format-owned plan gate admits
/// (`JsonFormatProvider.decodableColumns`, kept in lockstep with this dispatch) minus DECIMAL,
/// which `JsonDecoder` routes to the arrow-json path instead — anything else falls back to Flink
/// at plan time and can never reach a native decode.
pub(crate) fn make_json_appender(
    data_type: &DataType,
    capacity: usize,
    env: JsonEnv,
) -> Box<dyn JsonAppend> {
    use arrow::datatypes::TimeUnit;
    match data_type {
        DataType::Int8 => {
            Box::new(PrimitiveJsonAppender::<Int8Type>::new(data_type, capacity, false, env))
        }
        DataType::Int16 => {
            Box::new(PrimitiveJsonAppender::<Int16Type>::new(data_type, capacity, false, env))
        }
        DataType::Int32 => {
            Box::new(PrimitiveJsonAppender::<Int32Type>::new(data_type, capacity, true, env))
        }
        DataType::Int64 => {
            Box::new(PrimitiveJsonAppender::<Int64Type>::new(data_type, capacity, true, env))
        }
        DataType::Float32 => {
            Box::new(FloatJsonAppender::<Float32Type>::new(data_type, capacity, env))
        }
        DataType::Float64 => {
            Box::new(FloatJsonAppender::<Float64Type>::new(data_type, capacity, env))
        }
        DataType::Date32 => {
            Box::new(DateJsonAppender { builder: PrimitiveBuilder::with_capacity(capacity), env })
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            Box::new(TimestampJsonAppender::new(data_type, capacity, env))
        }
        // TIME(p)'s Arrow unit follows the declared precision; the value is always whole seconds.
        DataType::Time32(TimeUnit::Second) => {
            Box::new(TimeJsonAppender::<Time32SecondType>::new(data_type, capacity, 1, env))
        }
        DataType::Time32(TimeUnit::Millisecond) => Box::new(
            TimeJsonAppender::<Time32MillisecondType>::new(data_type, capacity, 1_000, env),
        ),
        DataType::Time64(TimeUnit::Microsecond) => Box::new(
            TimeJsonAppender::<Time64MicrosecondType>::new(data_type, capacity, 1_000_000, env),
        ),
        DataType::Time64(TimeUnit::Nanosecond) => Box::new(
            TimeJsonAppender::<Time64NanosecondType>::new(data_type, capacity, 1_000_000_000, env),
        ),
        DataType::Binary => Box::new(BinaryJsonAppender { builder: BinaryBuilder::new(), env }),
        DataType::Boolean => {
            Box::new(BooleanJsonAppender { builder: BooleanBuilder::new(), env })
        }
        DataType::Utf8 => Box::new(StringJsonAppender { builder: StringBuilder::new(), env }),
        DataType::Struct(fields) => Box::new(StructJsonAppender::new(fields, capacity, env)),
        DataType::List(field) => Box::new(ListJsonAppender::new(field, capacity, env)),
        DataType::Map(entries, false) => Box::new(MapJsonAppender::new(entries, capacity, env)),
        other => panic!("JSON decode does not support {other}"),
    }
}

/// Whether converting this value faithfully needs the Jackson-faithful token walk. Two causes:
///
/// - **Cursor drift.** A container token under a scalar (non-STRING) column coerces `getText`
///   ("{" / "[") WITHOUT consuming the container, and a container of the wrong kind throws with
///   the cursor still on its start token — either way the enclosing walk's `nextToken` then steps
///   INTO the container and the remaining fields convert from drifted positions, which the tree
///   walk cannot reproduce. STRING columns are safe (`convertToString` consumes a container
///   subtree cleanly), as are scalar tokens under container columns (the converter throws without
///   having consumed anything).
/// - **A FLOAT column needs its raw literal.** The tape parses number tokens f64-first, and
///   narrowing re-rounds; that equals Jackson's direct-to-float literal parse for every value
///   EXCEPT an f64 landing exactly on an f32 rounding midpoint, where the literal's true value
///   decides the side ([`f32_parse_ambiguous`]). The token walk re-parses the literal at float
///   width.
fn value_needs_token_walk(data_type: &DataType, value: simd_json::tape::Value<'_, '_>) -> bool {
    use simd_json::prelude::*;
    use simd_json::ValueType;
    let token = value.value_type();
    match data_type {
        DataType::Utf8 => false,
        DataType::Float32 => match token {
            ValueType::F64 => f32_parse_ambiguous(value.as_f64().expect("f64 node")),
            _ => matches!(token, ValueType::Object | ValueType::Array),
        },
        DataType::Struct(fields) => match token {
            ValueType::Object => {
                object_needs_token_walk(fields, &value.as_object().expect("object node"))
            }
            ValueType::Array => true,
            _ => false,
        },
        DataType::List(field) => match token {
            ValueType::Array => value
                .as_array()
                .expect("array node")
                .iter()
                .any(|element| value_needs_token_walk(field.data_type(), element)),
            ValueType::Object => true,
            _ => false,
        },
        DataType::Map(entries, _) => match token {
            ValueType::Object => {
                let value_type = match entries.data_type() {
                    DataType::Struct(kv) if kv.len() == 2 => kv[1].data_type(),
                    other => panic!("MAP entries must be a two-field struct, got {other}"),
                };
                value
                    .as_object()
                    .expect("object node")
                    .iter()
                    .any(|(_, entry)| value_needs_token_walk(value_type, entry))
            }
            ValueType::Array => true,
            _ => false,
        },
        _ => matches!(token, ValueType::Object | ValueType::Array),
    }
}

/// Whether the correctly-rounded f64 of a number literal cannot decide the literal's f32: exactly
/// at an f32 rounding midpoint the true decimal may sit on either side within the f64's half-ulp
/// (and `Float.parseFloat` rounds from the literal, not the f64). Adjacent-f32 midpoints — and the
/// overflow boundary `f32::MAX + 2^103` — are exactly representable in f64, so ambiguity is a
/// plain equality; everywhere else the f64 strictly separates from the midpoint and narrowing
/// rounds identically to the direct parse.
fn f32_parse_ambiguous(value: f64) -> bool {
    if !value.is_finite() || (value as f32) as f64 == value {
        return false;
    }
    let overflow_midpoint = f32::MAX as f64 + 2f64.powi(103);
    let narrowed = value as f32;
    if narrowed.is_infinite() {
        return value.abs() == overflow_midpoint;
    }
    let (below, above) = if (narrowed as f64) < value {
        (narrowed, narrowed.next_up())
    } else {
        (narrowed.next_down(), narrowed)
    };
    if below.is_infinite() || above.is_infinite() {
        return value.abs() == overflow_midpoint;
    }
    value == (below as f64 + above as f64) / 2.0
}

/// The nested-row drift scan, matching keys with the same saturating field counter as the append:
/// a value Flink's walk would SKIP (unknown key, or any key after saturation) is consumed cleanly
/// by `skipToNextField` and never drifts.
fn object_needs_token_walk(fields: &Fields, object: &simd_json::tape::Object<'_, '_>) -> bool {
    let mut matched = 0;
    for (key, value) in object {
        if matched == fields.len() {
            break;
        }
        if let Some(field) = fields.iter().find(|f| f.name() == key) {
            matched += 1;
            if value_needs_token_walk(field.data_type(), value) {
                return true;
            }
        }
    }
    false
}

/// Whether any (nested) leaf is DECIMAL. simd-json's tape parses numbers eagerly to i64/f64 and
/// drops the raw literal, so a decimal with more significant digits than an f64 carries would round;
/// arrow-json and Flink both parse the raw digit string exactly. Decimal-bearing schemas therefore
/// stay on the arrow-json path.
pub(crate) fn json_needs_raw_number_literals(data_type: &DataType) -> bool {
    match data_type {
        DataType::Decimal128(_, _) => true,
        DataType::Struct(fields) => {
            fields.iter().any(|f| json_needs_raw_number_literals(f.data_type()))
        }
        DataType::List(field) => json_needs_raw_number_literals(field.data_type()),
        DataType::Map(entries, _) => json_needs_raw_number_literals(entries.data_type()),
        _ => false,
    }
}

/// How a top-level JSON array root decodes, mirroring which Flink entry point the format funnels
/// its documents through.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArrayRootPolicy {
    /// Flink's plain `json` format: one row per element (`processArray`). A non-object element
    /// fails the whole message in strict mode and drops alone under `ignore-parse-errors` — see
    /// divergences/21 for how this settles the two Flink decode paths' disagreement.
    FanOut,
    /// Maxwell/Canal hand the root node straight to the tree converter: any array root is a
    /// corrupt message (job failure, or a whole-message drop under `ignore-parse-errors`).
    Corrupt,
    /// Debezium/OGG decode their envelope through the deprecated one-row `deserialize(byte[])`,
    /// which fans a top-level array out and unwraps the result when exactly one row came back. In
    /// default mode only `[{envelope}]` survives (a non-object element throws before the count is
    /// checked); under `ignore-parse-errors` failing elements are skipped inside the fan-out loop
    /// first, so the unwrap happens when exactly one OBJECT element remains — `[{envelope}, 1]`
    /// decodes, while `[]` and `[{a}, {b}]` drop the message.
    UnwrapSingle,
}

/// Whether this body skips the decode entirely: Flink's deserialize returns before parsing only
/// for a null or ZERO-LENGTH body. An all-whitespace document reaches Jackson, which finds no
/// token — "no content to map due to end-of-input" — so it fails the job in strict mode and
/// drops under `ignore-parse-errors`, never a silent skip.
fn skip_blank_body(bytes: &[u8], lenient: bool) -> bool {
    if bytes.is_empty() {
        return true;
    }
    if bytes.iter().all(u8::is_ascii_whitespace) {
        assert!(lenient, "failed to decode JSON record: no content to map due to end-of-input");
        return true;
    }
    false
}

/// Decodes one JSON document per body row into `schema` via a simd-json tape walk. A null or
/// zero-length body contributes no row; an all-whitespace one fails/drops (`skip_blank_body`). A
/// body whose root is an object decodes as one row; a top-level array follows `array_roots`
/// (a bad *value* inside a decoded object stays the appenders' per-field null, exactly as for a
/// single-object body). simd-json parses in place, so each body is copied into a reused scratch
/// buffer — the copy is part of the measured win over arrow-json.
pub(crate) fn decode_json_bodies_simd(
    schema: &SchemaRef,
    bodies: &RecordBatch,
    env: JsonEnv,
    array_roots: ArrayRootPolicy,
) -> RecordBatch {
    use simd_json::prelude::*;
    let column = bodies.column(0);
    let mut root = StructJsonAppender::new(schema.fields(), bodies.num_rows(), env);
    let mut scratch: Vec<u8> = Vec::new();
    let mut buffers = simd_json::Buffers::default();
    let scan_binary =
        env.lenient && schema.fields().iter().any(|f| contains_binary(f.data_type()));
    // Only the plain `json` format re-decodes a message through the Jackson-faithful walk (on a
    // failed fast parse or a cursor drift); the CDC envelopes keep the spec-strict fast parse —
    // their `old`-presence pre-scans mirror its skip conditions row for row.
    let retryable = array_roots == ArrayRootPolicy::FanOut;
    for row in 0..bodies.num_rows() {
        let Some(bytes) = binary_body(column, row) else { continue };
        if skip_blank_body(bytes, env.lenient) {
            continue;
        }
        scratch.clear();
        scratch.extend_from_slice(bytes);
        // A structurally bad document (or a root that is neither object nor fanned-out array)
        // fails the job like Flink's deserializer; under ignore-parse-errors it drops the whole
        // message (the value-level skips inside a good document are the appenders' per-field
        // nulls).
        let tape = match simd_json::to_tape_with_buffers(&mut scratch, &mut buffers) {
            Ok(tape) => tape,
            // simd-json is spec-strict where Jackson tokenizes more: out-of-range number
            // literals, raw control characters inside strings, content trailing the root
            // document. Retryable messages re-decode through the Jackson-faithful walk, which
            // rewrites them into sanitized rows this fast path appends.
            Err(_) if retryable => {
                append_rewritten(&mut root, bytes, schema.fields(), env, scan_binary);
                continue;
            }
            Err(_) if env.lenient => continue,
            Err(e) => panic!("failed to decode JSON record: {e}"),
        };
        let value = tape.as_value();
        match value.value_type() {
            simd_json::ValueType::Array if array_roots == ArrayRootPolicy::FanOut => {
                let array = value.as_array().expect("array node");
                // A drift anywhere re-decodes the MESSAGE: Flink's cursor drift crosses element
                // boundaries (a nested-array element drifts the element loop itself).
                if array.iter().any(|element| element_needs_token_walk(schema.fields(), element)) {
                    append_rewritten(&mut root, bytes, schema.fields(), env, scan_binary);
                    continue;
                }
                for element in &array {
                    match element.as_object() {
                        Some(object) => {
                            append_checked(&mut root, &object, schema.fields(), scan_binary)
                        }
                        None if env.lenient => {}
                        None => panic!(
                            "JSON array element was not an object: {:?}",
                            element.value_type()
                        ),
                    }
                }
            }
            simd_json::ValueType::Array if array_roots == ArrayRootPolicy::UnwrapSingle => {
                let array = value.as_array().expect("array node");
                if env.lenient {
                    let mut objects = array.iter().filter_map(|element| element.as_object());
                    if let (Some(object), None) = (objects.next(), objects.next()) {
                        append_checked(&mut root, &object, schema.fields(), scan_binary);
                    }
                    // else: 0 or 2+ decodable envelopes — the whole message drops.
                } else {
                    let mut elements = array.iter();
                    match (elements.next().and_then(|e| e.as_object()), elements.next()) {
                        (Some(object), None) => {
                            append_checked(&mut root, &object, schema.fields(), scan_binary)
                        }
                        _ => panic!("CDC message array root did not hold exactly one envelope"),
                    }
                }
            }
            _ => match value.as_object() {
                Some(object) => {
                    if scan_binary && object_poisoned(schema.fields(), &object) {
                        continue;
                    }
                    if !root.try_append_object(&object, retryable) {
                        append_rewritten(&mut root, bytes, schema.fields(), env, scan_binary);
                    }
                }
                None if env.lenient => continue,
                None => panic!("JSON body was not a single object"),
            },
        }
    }
    RecordBatch::try_new(schema.clone(), root.finish_columns())
        .expect("failed to build JSON batch")
}

/// Appends one decoded row object, first applying the lenient BINARY poison pre-scan (a
/// quote-consuming base64 shape drops the whole row, as Flink's corrupted parser does).
fn append_checked(
    root: &mut StructJsonAppender,
    object: &simd_json::tape::Object<'_, '_>,
    fields: &Fields,
    scan_binary: bool,
) {
    if scan_binary && object_poisoned(fields, object) {
        return;
    }
    root.append_object(object);
}

/// Re-decodes one message through the Jackson-faithful walk and appends the sanitized rows it
/// rewrote (none when the walk dropped the message in skip mode).
fn append_rewritten(
    root: &mut StructJsonAppender,
    bytes: &[u8],
    fields: &Fields,
    env: JsonEnv,
    scan_binary: bool,
) {
    use simd_json::prelude::*;
    for row in crate::json_retry::rewrite_message(bytes, fields, env) {
        let mut buf = row.into_bytes();
        let tape = simd_json::to_tape(&mut buf).expect("sanitized row reparses");
        let value = tape.as_value();
        append_checked(root, &value.as_object().expect("sanitized row object"), fields, scan_binary);
    }
}

/// A fanned-out element's cursor-drift check: a nested-array element makes Flink's element loop
/// itself drift into the array; scalar and null elements are consumed cleanly.
fn element_needs_token_walk(fields: &Fields, element: simd_json::tape::Value<'_, '_>) -> bool {
    use simd_json::prelude::*;
    match element.value_type() {
        simd_json::ValueType::Array => true,
        simd_json::ValueType::Object => {
            object_needs_token_walk(fields, &element.as_object().expect("object node"))
        }
        _ => false,
    }
}

fn contains_binary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Binary => true,
        DataType::Struct(fields) => fields.iter().any(|f| contains_binary(f.data_type())),
        DataType::List(field) => contains_binary(field.data_type()),
        DataType::Map(entries, _) => contains_binary(entries.data_type()),
        _ => false,
    }
}

/// Whether the document holds, under a BINARY-typed leaf, a base64 string whose failure makes
/// Jackson consume the closing quote (see [`flink_text::Base64Error::QuoteConsumed`]). Flink's
/// lenient mode drops the WHOLE message on those — the corrupted parser fails outside the
/// per-field catch — so the pre-scan reproduces that granularity before anything is appended.
fn object_poisoned(fields: &Fields, object: &simd_json::tape::Object<'_, '_>) -> bool {
    object.iter().any(|(key, value)| {
        fields
            .iter()
            .find(|f| f.name() == key)
            .is_some_and(|f| value_poisoned(f.data_type(), value))
    })
}

fn value_poisoned(data_type: &DataType, value: simd_json::tape::Value<'_, '_>) -> bool {
    use simd_json::prelude::*;
    if !contains_binary(data_type) {
        return false;
    }
    match (data_type, value.value_type()) {
        (DataType::Binary, simd_json::ValueType::String) => matches!(
            flink_text::parse_jackson_base64(value.as_str().expect("string node")),
            Err(flink_text::Base64Error::QuoteConsumed)
        ),
        (DataType::Struct(fields), simd_json::ValueType::Object) => {
            object_poisoned(fields, &value.as_object().expect("object node"))
        }
        (DataType::List(field), simd_json::ValueType::Array) => value
            .as_array()
            .expect("array node")
            .iter()
            .any(|element| value_poisoned(field.data_type(), element)),
        (DataType::Map(entries, _), simd_json::ValueType::Object) => {
            let value_type = match entries.data_type() {
                DataType::Struct(kv) if kv.len() == 2 => kv[1].data_type(),
                other => panic!("MAP entries must be a two-field struct, got {other}"),
            };
            value
                .as_object()
                .expect("object node")
                .iter()
                .any(|(_, entry)| value_poisoned(value_type, entry))
        }
        _ => false,
    }
}

pub(crate) struct JsonDecoder {
    pub(crate) schema: SchemaRef,
    /// DECIMAL columns need the raw number literal for exactness (see
    /// `json_needs_raw_number_literals`); those schemas decode via arrow-json, all others via the
    /// simd-json tape walk. The arrow-json path keeps its own (lenient) temporal/coercion
    /// envelope — an accept-where-Flink-rejects residual documented in divergences/21.
    raw_literals: bool,
    /// The table's `timestamp-format.standard` and skip mode.
    env: JsonEnv,
    /// Flink's plain `json` format fans a top-level array out into one row per element
    /// (`processArray`); the CDC dialects funnel their envelope through the same converters but
    /// never fan out — an array root is corrupt outright, or unwrapped when it holds exactly one
    /// envelope (see [`ArrayRootPolicy`]).
    array_roots: ArrayRootPolicy,
}

impl JsonDecoder {
    pub(crate) fn new(schema: SchemaRef, env: JsonEnv) -> JsonDecoder {
        JsonDecoder::build(schema, env, ArrayRootPolicy::FanOut)
    }

    /// The CDC-envelope shape: a top-level array is never a fan-out — `array_roots` picks the
    /// dialect's treatment ([`ArrayRootPolicy::Corrupt`] or [`ArrayRootPolicy::UnwrapSingle`]).
    pub(crate) fn single_object(
        schema: SchemaRef,
        env: JsonEnv,
        array_roots: ArrayRootPolicy,
    ) -> JsonDecoder {
        JsonDecoder::build(schema, env, array_roots)
    }

    fn build(schema: SchemaRef, env: JsonEnv, array_roots: ArrayRootPolicy) -> JsonDecoder {
        let raw_literals =
            schema.fields().iter().any(|f| json_needs_raw_number_literals(f.data_type()));
        JsonDecoder { schema, raw_literals, env, array_roots }
    }

    /// Decodes the single body column of `bodies` into a batch of the target schema. Each row is a
    /// complete document decoding to one row — or, for a fanned-out top-level array, one row per
    /// element; a null body contributes no row.
    pub(crate) fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        if self.raw_literals {
            return self.decode_raw_literals(bodies);
        }
        decode_json_bodies_simd(&self.schema, bodies, self.env, self.array_roots)
    }

    /// The arrow-json path for decimal-bearing schemas: its tape keeps each number's raw literal.
    /// DECIMAL columns are decoded as *text* (`coerce_primitive` writes a number token's raw
    /// literal) and converted here with Flink's exact semantics — `new BigDecimal(String)` then a
    /// HALF_UP rescale that goes NULL on precision overflow. arrow-json's own decimal parse
    /// truncates extra fraction digits and errors on overflow, which silently diverged from Flink
    /// on valid data. TIME and VARBINARY leaves sharing such a schema also ride as text and
    /// convert through the same Flink-exact parsers the simd path uses — arrow-json's own
    /// time/binary handling has a different envelope. Documents feed one at a time to keep the
    /// decoder's record boundaries aligned with the input rows.
    fn decode_raw_literals(&self, bodies: &RecordBatch) -> RecordBatch {
        let column = bodies.column(0);
        let text_schema = Arc::new(Schema::new(
            self.schema
                .fields()
                .iter()
                .map(|f| Arc::new(exact_leaves_as_text(f)))
                .collect::<Vec<FieldRef>>(),
        ));
        let build = |batch_size: usize| {
            arrow::json::ReaderBuilder::new(text_schema.clone())
                .with_batch_size(batch_size.max(1))
                .with_coerce_primitive(true)
                .build_decoder()
                .expect("failed to build JSON decoder")
        };
        // In skip mode each message decodes through its own decoder so a bad one drops alone
        // (arrow-json's decoder state is unusable after an error), and each fanned-out array
        // element through its own so a bad element drops alone — the same per-element granularity
        // as the simd path. Flink's skip on this path is otherwise approximated at message/element
        // granularity for non-decimal errors — divergences/21; the decimal cells themselves skip
        // per FIELD in restore_exact_leaves, like the host.
        let mut batches = Vec::new();
        if self.env.lenient {
            for row in 0..bodies.num_rows() {
                let Some(bytes) = binary_body(column, row) else { continue };
                if skip_blank_body(bytes, true) {
                    continue;
                }
                let elements = match self.array_body(bytes) {
                    ArrayBody::Not => vec![bytes],
                    ArrayBody::Elements(elements) => elements,
                    // A non-fanned (CDC-envelope) array root drops the whole message.
                    ArrayBody::Corrupt => continue,
                };
                for element in elements {
                    if !starts_with_object(element) {
                        continue; // a non-object array element drops alone, like the simd path
                    }
                    let mut decoder = build(1);
                    let decoded = decoder
                        .decode(element)
                        .ok()
                        .filter(|&consumed| consumed == element.len())
                        .and_then(|_| decoder.flush().ok().flatten());
                    if let Some(batch) = decoded {
                        batches.push(batch);
                    }
                }
            }
        } else {
            // Fanned-out arrays can exceed the input row count, and the decoder stops consuming
            // at its batch size — so gather every document first and size the decoder to fit.
            let mut documents: Vec<&[u8]> = Vec::with_capacity(bodies.num_rows());
            for row in 0..bodies.num_rows() {
                let Some(bytes) = binary_body(column, row) else { continue };
                if skip_blank_body(bytes, false) {
                    continue;
                }
                match self.array_body(bytes) {
                    ArrayBody::Not => documents.push(bytes),
                    ArrayBody::Elements(elements) => {
                        for element in elements {
                            assert!(
                                starts_with_object(element),
                                "JSON array element was not an object"
                            );
                            documents.push(element);
                        }
                    }
                    ArrayBody::Corrupt => panic!("JSON body was not a single object"),
                }
            }
            let mut decoder = build(documents.len());
            for document in documents {
                let consumed = decoder.decode(document).expect("failed to decode JSON record");
                assert_eq!(
                    consumed,
                    document.len(),
                    "JSON body was not a single complete document"
                );
            }
            if let Some(batch) = decoder.flush().expect("failed to flush JSON batch") {
                batches.push(batch);
            }
        }
        let decoded = match batches.len() {
            0 => RecordBatch::new_empty(text_schema),
            1 => batches.into_iter().next().expect("one batch"),
            _ => {
                let schema = batches[0].schema();
                concat_batches(&schema, &batches).expect("raw-literal batch concat failed")
            }
        };
        let columns = self
            .schema
            .fields()
            .iter()
            .zip(decoded.columns())
            .map(|(field, column)| {
                let column = restore_exact_leaves(column, field.data_type(), self.env.lenient);
                collapse_duplicate_map_keys(&column, field.data_type())
            })
            .collect();
        RecordBatch::try_new(self.schema.clone(), columns).expect("failed to build JSON batch")
    }

    /// Classifies a body for the raw-literals path: `Not` (the root is not an array — decode it as
    /// a single document), `Elements` (a fanned-out top-level array's raw element slices — or the
    /// single unwrapped envelope slice — exact literals intact), or `Corrupt` (an array root the
    /// policy rejects, so the message fails whole in strict mode and drops whole in skip mode). A
    /// malformed document behaves like the simd path: strict fails the job here, skip mode drops
    /// the whole message.
    fn array_body<'a>(&self, bytes: &'a [u8]) -> ArrayBody<'a> {
        if bytes.iter().find(|b| !b.is_ascii_whitespace()) != Some(&&b'[') {
            return ArrayBody::Not;
        }
        if self.array_roots == ArrayRootPolicy::Corrupt {
            return ArrayBody::Corrupt;
        }
        // Validate before scanning boundaries: the scanner assumes well-formed JSON, and a
        // malformed document must fail/drop whole, never element by element.
        let mut scratch = bytes.to_vec();
        match simd_json::to_tape(&mut scratch) {
            Ok(_) => {
                let elements = top_level_array_elements(bytes);
                match self.array_roots {
                    ArrayRootPolicy::FanOut => ArrayBody::Elements(elements),
                    ArrayRootPolicy::UnwrapSingle => {
                        unwrap_single_element(elements, self.env.lenient)
                    }
                    ArrayRootPolicy::Corrupt => unreachable!("returned above"),
                }
            }
            Err(_) if self.env.lenient => ArrayBody::Corrupt,
            Err(e) => panic!("failed to decode JSON record: {e}"),
        }
    }
}

enum ArrayBody<'a> {
    Not,
    Elements(Vec<&'a [u8]>),
    Corrupt,
}

/// [`ArrayRootPolicy::UnwrapSingle`] on the raw-literals path: the message decodes as its lone
/// envelope object exactly when the deprecated one-row Flink entry would have unwrapped it (in
/// default mode any junk element fails the message first; in skip mode junk is skipped and only
/// the surviving-row count matters).
fn unwrap_single_element(elements: Vec<&[u8]>, lenient: bool) -> ArrayBody<'_> {
    let objects: Vec<&[u8]> =
        elements.iter().copied().filter(|element| starts_with_object(element)).collect();
    if objects.len() == 1 && (lenient || elements.len() == 1) {
        ArrayBody::Elements(objects)
    } else {
        ArrayBody::Corrupt
    }
}

fn starts_with_object(element: &[u8]) -> bool {
    element.iter().find(|b| !b.is_ascii_whitespace()) == Some(&&b'{')
}

/// The raw element slices of a top-level JSON array (whitespace-trimmed only at the edges the
/// separators leave behind — each slice is the element's exact bytes, so raw number literals
/// survive for the decimal parse). The input must already be validated as well-formed JSON with an
/// array root; the scan then only needs string/escape state and container depth to find the
/// top-level commas.
fn top_level_array_elements(bytes: &[u8]) -> Vec<&[u8]> {
    let start = bytes.iter().position(|b| *b == b'[').expect("array root") + 1;
    let end = bytes.iter().rposition(|b| *b == b']').expect("array root");
    let mut elements = Vec::new();
    let (mut depth, mut in_string, mut escaped) = (0usize, false, false);
    let mut element_start = start;
    for i in start..end {
        let byte = bytes[i];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => depth += 1,
            b'}' | b']' => depth -= 1,
            b',' if depth == 0 => {
                elements.push(&bytes[element_start..i]);
                element_start = i + 1;
            }
            _ => {}
        }
    }
    let last = &bytes[element_start..end];
    if !last.iter().all(u8::is_ascii_whitespace) {
        elements.push(last);
    }
    elements
}

/// The leaves the raw-literals path decodes as Utf8 and converts with a Flink-exact parser after
/// arrow-json: DECIMAL (needs the raw number literal), TIME, and VARBINARY (arrow-json's own
/// envelopes differ from Flink's).
fn text_restored_leaf(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal128(_, _) | DataType::Time32(_) | DataType::Time64(_) | DataType::Binary
    )
}

/// Whether a (nested) leaf of this type is converted by [`restore_exact_leaves`].
fn needs_text_restore(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(fields) => fields.iter().any(|f| needs_text_restore(f.data_type())),
        DataType::List(field) => needs_text_restore(field.data_type()),
        DataType::Map(entries, _) => needs_text_restore(entries.data_type()),
        other => text_restored_leaf(other),
    }
}

/// The arrow-json decode schema for the raw-literals path: every (nested) DECIMAL/TIME/VARBINARY
/// leaf becomes Utf8, so `coerce_primitive` captures the exact literal for
/// [`restore_exact_leaves`] to convert.
fn exact_leaves_as_text(field: &Field) -> Field {
    let data_type = match field.data_type() {
        leaf if text_restored_leaf(leaf) => DataType::Utf8,
        DataType::Struct(fields) => DataType::Struct(
            fields.iter().map(|f| Arc::new(exact_leaves_as_text(f))).collect::<Fields>(),
        ),
        DataType::List(f) => DataType::List(Arc::new(exact_leaves_as_text(f))),
        DataType::Map(entries, sorted) => {
            DataType::Map(Arc::new(exact_leaves_as_text(entries)), *sorted)
        }
        other => other.clone(),
    };
    field.clone().with_data_type(data_type)
}

/// Converts a raw-literals column back to its declared type: a Utf8-decoded DECIMAL leaf parses
/// with Flink's `BigDecimal` + `DecimalData.fromBigDecimal` (HALF_UP, precision overflow → NULL,
/// garbage fails), TIME with the `SQL_TIME_FORMAT`-and-truncate rule, VARBINARY with Jackson's
/// base64 read; containers rebuild around their converted children; anything else is already its
/// declared type.
fn restore_exact_leaves(column: &ArrayRef, target: &DataType, lenient: bool) -> ArrayRef {
    if !needs_text_restore(target) {
        return column.clone();
    }
    match target {
        DataType::Time32(_) | DataType::Time64(_) => {
            let strings = column.as_any().downcast_ref::<StringArray>().expect("time text");
            let seconds: Vec<Option<i64>> = strings
                .iter()
                .map(|text| {
                    let text = text?;
                    match flink_text::parse_sql_time_second_of_day(text) {
                        Some(seconds) => Some(seconds),
                        None if lenient => None,
                        None => panic!("failed to parse \"{text}\" as {target}"),
                    }
                })
                .collect();
            time_array(target, &seconds)
        }
        DataType::Binary => {
            let strings = column.as_any().downcast_ref::<StringArray>().expect("binary text");
            let values: BinaryArray = strings
                .iter()
                .map(|text| {
                    let text = text?;
                    match flink_text::parse_jackson_base64(text) {
                        Ok(bytes) => Some(bytes),
                        // Message-drop granularity for a quote-consuming shape is not
                        // reproducible on this path (columns are already built) — the field
                        // nulls instead, a decimal-path residual noted in divergences/21.
                        Err(_) if lenient => None,
                        Err(_) => panic!("failed to decode base64 \"{text}\" as VARBINARY"),
                    }
                })
                .collect();
            Arc::new(values)
        }
        DataType::Decimal128(p, s) => {
            let strings = column.as_any().downcast_ref::<StringArray>().expect("decimal text");
            let values: Decimal128Array = strings
                .iter()
                .map(|text| {
                    let text = text?;
                    // Flink trims a string-positioned decimal; a number token can't carry spaces,
                    // so trimming both is exact.
                    match flink_text::parse_flink_decimal(text.trim(), *p, *s) {
                        Ok(value) => value,
                        Err(()) if lenient => None,
                        Err(()) => panic!("failed to parse \"{text}\" as DECIMAL({p}, {s})"),
                    }
                })
                .collect();
            Arc::new(values.with_precision_and_scale(*p, *s).expect("declared decimal type"))
        }
        DataType::Struct(fields) => {
            let source = column.as_any().downcast_ref::<StructArray>().expect("struct column");
            let children = fields
                .iter()
                .zip(source.columns())
                .map(|(field, child)| restore_exact_leaves(child, field.data_type(), lenient))
                .collect();
            Arc::new(
                StructArray::try_new(fields.clone(), children, source.nulls().cloned())
                    .expect("failed to rebuild struct column"),
            )
        }
        DataType::List(field) => {
            let source = column.as_any().downcast_ref::<ListArray>().expect("list column");
            let values = restore_exact_leaves(source.values(), field.data_type(), lenient);
            Arc::new(
                ListArray::try_new(
                    field.clone(),
                    source.offsets().clone(),
                    values,
                    source.nulls().cloned(),
                )
                .expect("failed to rebuild list column"),
            )
        }
        DataType::Map(entries_field, sorted) => {
            let source = column.as_any().downcast_ref::<MapArray>().expect("map column");
            let entries = restore_exact_leaves(
                &(Arc::new(source.entries().clone()) as ArrayRef),
                entries_field.data_type(),
                lenient,
            );
            let entries = entries.as_any().downcast_ref::<StructArray>().expect("map entries").clone();
            Arc::new(
                MapArray::try_new(
                    entries_field.clone(),
                    source.offsets().clone(),
                    entries,
                    source.nulls().cloned(),
                    *sorted,
                )
                .expect("failed to rebuild map column"),
            )
        }
        _ => column.clone(),
    }
}

/// Whether a (nested) leaf is a MAP — the raw-literals path must collapse its duplicate keys.
fn contains_map(data_type: &DataType) -> bool {
    match data_type {
        DataType::Map(_, _) => true,
        DataType::Struct(fields) => fields.iter().any(|f| contains_map(f.data_type())),
        DataType::List(field) => contains_map(field.data_type()),
        _ => false,
    }
}

/// arrow-json's map decoder keeps every entry of a duplicate-keyed JSON object, but Flink's
/// converter builds a `java.util.Map` — a repeated key holds ONE entry with the final value. This
/// collapses the arrow-json-built maps to the simd path's last-value-first-position rule (hash
/// order is not reproducible either way); a map-free column passes through untouched and an
/// already-unique column rebuilds nothing.
fn collapse_duplicate_map_keys(column: &ArrayRef, data_type: &DataType) -> ArrayRef {
    if !contains_map(data_type) {
        return column.clone();
    }
    match data_type {
        DataType::Struct(fields) => {
            let source = column.as_any().downcast_ref::<StructArray>().expect("struct column");
            let children = fields
                .iter()
                .zip(source.columns())
                .map(|(field, child)| collapse_duplicate_map_keys(child, field.data_type()))
                .collect();
            Arc::new(
                StructArray::try_new(fields.clone(), children, source.nulls().cloned())
                    .expect("failed to rebuild struct column"),
            )
        }
        DataType::List(field) => {
            let source = column.as_any().downcast_ref::<ListArray>().expect("list column");
            let values = collapse_duplicate_map_keys(source.values(), field.data_type());
            Arc::new(
                ListArray::try_new(
                    field.clone(),
                    source.offsets().clone(),
                    values,
                    source.nulls().cloned(),
                )
                .expect("failed to rebuild list column"),
            )
        }
        DataType::Map(entries_field, sorted) => {
            let source = column.as_any().downcast_ref::<MapArray>().expect("map column");
            let entry_fields = match entries_field.data_type() {
                DataType::Struct(kv) if kv.len() == 2 => kv.clone(),
                other => panic!("MAP entries must be a two-field struct, got {other}"),
            };
            let keys = source
                .entries()
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string map keys");
            let values =
                collapse_duplicate_map_keys(source.entries().column(1), entry_fields[1].data_type());
            let offsets = source.offsets();
            let mut surviving: Vec<u32> = Vec::with_capacity(keys.len());
            let mut new_offsets: Vec<i32> = Vec::with_capacity(offsets.len());
            new_offsets.push(0);
            let mut kept: Vec<u32> = Vec::new();
            for window in offsets.windows(2) {
                kept.clear();
                for entry in window[0] as usize..window[1] as usize {
                    let key = keys.value(entry);
                    match kept.iter().position(|&k| keys.value(k as usize) == key) {
                        Some(at) => kept[at] = entry as u32,
                        None => kept.push(entry as u32),
                    }
                }
                surviving.extend_from_slice(&kept);
                new_offsets.push(surviving.len() as i32);
            }
            if surviving.len() == keys.len() && Arc::ptr_eq(&values, source.entries().column(1)) {
                return column.clone();
            }
            let indices = UInt32Array::from(surviving);
            let entries = StructArray::try_new(
                entry_fields,
                vec![
                    arrow::compute::take(keys, &indices, None).expect("take map keys"),
                    arrow::compute::take(&values, &indices, None).expect("take map values"),
                ],
                None,
            )
            .expect("failed to rebuild map entries");
            Arc::new(
                MapArray::try_new(
                    entries_field.clone(),
                    OffsetBuffer::new(ScalarBuffer::from(new_offsets)),
                    entries,
                    source.nulls().cloned(),
                    *sorted,
                )
                .expect("failed to rebuild map column"),
            )
        }
        _ => column.clone(),
    }
}

/// Builds the declared Arrow time array from whole seconds of the day (the unit follows the
/// column's declared precision; the value is always whole seconds — see the TIME appender).
fn time_array(target: &DataType, seconds: &[Option<i64>]) -> ArrayRef {
    use arrow::datatypes::TimeUnit;
    fn collect<T>(seconds: &[Option<i64>], per_second: i64) -> ArrayRef
    where
        T: ArrowPrimitiveType,
        T::Native: num_traits::NumCast,
    {
        let values: PrimitiveArray<T> = seconds
            .iter()
            .map(|s| {
                s.map(|s| num_traits::NumCast::from(s * per_second).expect("a day fits the unit"))
            })
            .collect();
        Arc::new(values)
    }
    match target {
        DataType::Time32(TimeUnit::Second) => collect::<Time32SecondType>(seconds, 1),
        DataType::Time32(TimeUnit::Millisecond) => collect::<Time32MillisecondType>(seconds, 1_000),
        DataType::Time64(TimeUnit::Microsecond) => {
            collect::<Time64MicrosecondType>(seconds, 1_000_000)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            collect::<Time64NanosecondType>(seconds, 1_000_000_000)
        }
        other => panic!("not a TIME type: {other}"),
    }
}
