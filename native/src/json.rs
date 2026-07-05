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
/// truncate toward zero into integer columns, booleans never fail, temporals follow the table's
/// `timestamp-format.standard`), pinned by a per-message parity test against Flink's deserializer;
/// the deliberate residual leniencies live in divergences/21.
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
}

/// Integer columns: number tokens convert through `NumCast` (a float truncates toward zero, out of
/// range fails — Jackson's `getIntValue` family), strings parse per Java's `parseInt` over trimmed
/// text.
pub(crate) struct PrimitiveJsonAppender<T: ArrowPrimitiveType> {
    builder: PrimitiveBuilder<T>,
    data_type: DataType,
    env: JsonEnv,
}

impl<T: ArrowPrimitiveType> PrimitiveJsonAppender<T> {
    fn new(data_type: &DataType, capacity: usize, env: JsonEnv) -> PrimitiveJsonAppender<T> {
        PrimitiveJsonAppender {
            builder: PrimitiveBuilder::<T>::with_capacity(capacity)
                .with_data_type(data_type.clone()),
            data_type: data_type.clone(),
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
/// envelope (`Infinity`/`NaN`, an `f`/`d` suffix, self-trimming).
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
fn write_json_string(out: &mut String, s: &str) {
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

    /// Collects the last value per field first (duplicate keys: last wins, like arrow-json and
    /// Jackson; unknown keys are ignored), then appends one value per child so every column stays
    /// row-aligned.
    fn append_object(&mut self, object: &simd_json::tape::Object<'_, '_>) {
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
        for (key, value) in object {
            if let Some(i) = self.field_index(key) {
                slots[i] = Some(value);
            }
        }
        for (child, slot) in self.children.iter_mut().zip(slots.iter()) {
            child.append(*slot);
        }
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
                for (key, value) in &object {
                    self.keys.append_key(key);
                    self.values.append(Some(value));
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

/// The types here are exactly the ones the boundary type gate admits (see
/// `docs/coverage-and-fallbacks.md` §4) minus DECIMAL, which `JsonDecoder` routes to the arrow-json
/// path instead — anything else can never reach a native decode.
pub(crate) fn make_json_appender(
    data_type: &DataType,
    capacity: usize,
    env: JsonEnv,
) -> Box<dyn JsonAppend> {
    use arrow::datatypes::TimeUnit;
    match data_type {
        DataType::Int8 => {
            Box::new(PrimitiveJsonAppender::<Int8Type>::new(data_type, capacity, env))
        }
        DataType::Int16 => {
            Box::new(PrimitiveJsonAppender::<Int16Type>::new(data_type, capacity, env))
        }
        DataType::Int32 => {
            Box::new(PrimitiveJsonAppender::<Int32Type>::new(data_type, capacity, env))
        }
        DataType::Int64 => {
            Box::new(PrimitiveJsonAppender::<Int64Type>::new(data_type, capacity, env))
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

/// Decodes one JSON document per body row into `schema` via a simd-json tape walk. A null or
/// all-whitespace body contributes no row (exactly what feeding it to arrow-json did); each present
/// body must be a single complete object. simd-json parses in place, so each body is copied into a
/// reused scratch buffer — the copy is part of the measured win over arrow-json.
pub(crate) fn decode_json_bodies_simd(
    schema: &SchemaRef,
    bodies: &RecordBatch,
    env: JsonEnv,
) -> RecordBatch {
    let column = bodies.column(0);
    let mut root = StructJsonAppender::new(schema.fields(), bodies.num_rows(), env);
    let mut scratch: Vec<u8> = Vec::new();
    let mut buffers = simd_json::Buffers::default();
    for row in 0..bodies.num_rows() {
        let Some(bytes) = binary_body(column, row) else { continue };
        if bytes.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        scratch.clear();
        scratch.extend_from_slice(bytes);
        // A structurally bad document (or a non-object root) fails the job like Flink's
        // deserializer; under ignore-parse-errors it drops the whole message (the value-level
        // skips inside a good document are the appenders' per-field nulls).
        let tape = match simd_json::to_tape_with_buffers(&mut scratch, &mut buffers) {
            Ok(tape) => tape,
            Err(_) if env.lenient => continue,
            Err(e) => panic!("failed to decode JSON record: {e}"),
        };
        let value = tape.as_value();
        let object = match value.as_object() {
            Some(object) => object,
            None if env.lenient => continue,
            None => panic!("JSON body was not a single object"),
        };
        root.append_object(&object);
    }
    RecordBatch::try_new(schema.clone(), root.finish_columns())
        .expect("failed to build JSON batch")
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
}

impl JsonDecoder {
    pub(crate) fn new(schema: SchemaRef, env: JsonEnv) -> JsonDecoder {
        let raw_literals =
            schema.fields().iter().any(|f| json_needs_raw_number_literals(f.data_type()));
        JsonDecoder { schema, raw_literals, env }
    }

    /// Decodes the single body column of `bodies` into a batch of the target schema. Each row is a
    /// complete document; a null body contributes no row.
    pub(crate) fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        if self.raw_literals {
            return self.decode_raw_literals(bodies);
        }
        decode_json_bodies_simd(&self.schema, bodies, self.env)
    }

    /// The arrow-json path for decimal-bearing schemas: its tape keeps each number's raw literal.
    /// DECIMAL columns are decoded as *text* (`coerce_primitive` writes a number token's raw
    /// literal) and converted here with Flink's exact semantics — `new BigDecimal(String)` then a
    /// HALF_UP rescale that goes NULL on precision overflow. arrow-json's own decimal parse
    /// truncates extra fraction digits and errors on overflow, which silently diverged from Flink
    /// on valid data. Documents feed one at a time to keep the decoder's record boundaries aligned
    /// with the input rows.
    fn decode_raw_literals(&self, bodies: &RecordBatch) -> RecordBatch {
        let column = bodies.column(0);
        let text_schema = Arc::new(Schema::new(
            self.schema
                .fields()
                .iter()
                .map(|f| Arc::new(decimals_as_text(f)))
                .collect::<Vec<FieldRef>>(),
        ));
        let build = || {
            arrow::json::ReaderBuilder::new(text_schema.clone())
                .with_batch_size(bodies.num_rows().max(1))
                .with_coerce_primitive(true)
                .build_decoder()
                .expect("failed to build JSON decoder")
        };
        // In skip mode each message decodes through its own decoder so a bad one drops alone
        // (arrow-json's decoder state is unusable after an error). Flink's skip on this path is
        // approximated at message granularity for non-decimal errors — divergences/21; the decimal
        // cells themselves skip per FIELD in restore_decimals, like the host.
        let mut batches = Vec::new();
        if self.env.lenient {
            for row in 0..bodies.num_rows() {
                let Some(bytes) = binary_body(column, row) else { continue };
                let mut decoder = build();
                let decoded = decoder
                    .decode(bytes)
                    .ok()
                    .filter(|&consumed| consumed == bytes.len())
                    .and_then(|_| decoder.flush().ok().flatten());
                if let Some(batch) = decoded {
                    batches.push(batch);
                }
            }
        } else {
            let mut decoder = build();
            for row in 0..bodies.num_rows() {
                if let Some(bytes) = binary_body(column, row) {
                    let consumed = decoder.decode(bytes).expect("failed to decode JSON record");
                    assert_eq!(
                        consumed,
                        bytes.len(),
                        "JSON body was not a single complete document"
                    );
                }
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
            .map(|(field, column)| restore_decimals(column, field.data_type(), self.env.lenient))
            .collect();
        RecordBatch::try_new(self.schema.clone(), columns).expect("failed to build JSON batch")
    }
}

/// The arrow-json decode schema for the raw-literals path: every (nested) DECIMAL leaf becomes
/// Utf8, so `coerce_primitive` captures the exact raw literal for [`restore_decimals`] to convert.
fn decimals_as_text(field: &Field) -> Field {
    let data_type = match field.data_type() {
        DataType::Decimal128(_, _) => DataType::Utf8,
        DataType::Struct(fields) => DataType::Struct(
            fields.iter().map(|f| Arc::new(decimals_as_text(f))).collect::<Fields>(),
        ),
        DataType::List(f) => DataType::List(Arc::new(decimals_as_text(f))),
        DataType::Map(entries, sorted) => {
            DataType::Map(Arc::new(decimals_as_text(entries)), *sorted)
        }
        other => other.clone(),
    };
    field.clone().with_data_type(data_type)
}

/// Converts a raw-literals column back to its declared type: a Utf8-decoded DECIMAL leaf parses
/// with Flink's `BigDecimal` + `DecimalData.fromBigDecimal` (HALF_UP, precision overflow → NULL,
/// garbage fails); containers rebuild around their converted children; anything else is already
/// its declared type.
fn restore_decimals(column: &ArrayRef, target: &DataType, lenient: bool) -> ArrayRef {
    if !json_needs_raw_number_literals(target) {
        return column.clone();
    }
    match target {
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
                .map(|(field, child)| restore_decimals(child, field.data_type(), lenient))
                .collect();
            Arc::new(
                StructArray::try_new(fields.clone(), children, source.nulls().cloned())
                    .expect("failed to rebuild struct column"),
            )
        }
        DataType::List(field) => {
            let source = column.as_any().downcast_ref::<ListArray>().expect("list column");
            let values = restore_decimals(source.values(), field.data_type(), lenient);
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
            let entries = restore_decimals(
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
