use crate::*;

/// Decodes a column of raw JSON message bodies — one complete document per row, as a source hands
/// them off untouched — into a typed Arrow batch matching `schema`. This replaces Flink's per-record
/// `byte[] -> tree -> RowData` materialization with a single batched decode straight to columnar
/// form, so the row representation never exists on the hot ingest path. The body column may arrive as
/// binary or string (whichever the source-edge transpose produced for the message bytes).
/// One column's JSON→Arrow appender in the simd-json decode path: a schema-driven walk of the parse
/// tape appending straight into a typed builder. `None` (a field absent from the object) and an
/// explicit JSON null both append SQL NULL. The per-type semantics replicate arrow-json's reader —
/// which the Kafka JSON parity tests pin against Flink — so swapping the parser changes no output.
pub(crate) trait JsonAppend {
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>);
    /// Appends a JSON object key (always a raw string): scalar targets parse it exactly like a
    /// string-positioned value. Only map key columns reach this.
    fn append_key(&mut self, key: &str);
    fn finish(&mut self) -> ArrayRef;
}

/// Integers, floats, and dates: numbers convert through `NumCast` (a float for an integer column
/// truncates toward zero) and strings parse via arrow-cast's `Parser`, both exactly as arrow-json.
pub(crate) struct PrimitiveJsonAppender<T: ArrowPrimitiveType> {
    builder: PrimitiveBuilder<T>,
    data_type: DataType,
}

impl<T: ArrowPrimitiveType> PrimitiveJsonAppender<T> {
    fn new(data_type: &DataType, capacity: usize) -> PrimitiveJsonAppender<T> {
        PrimitiveJsonAppender {
            builder: PrimitiveBuilder::<T>::with_capacity(capacity)
                .with_data_type(data_type.clone()),
            data_type: data_type.clone(),
        }
    }
}

impl<T> JsonAppend for PrimitiveJsonAppender<T>
where
    T: ArrowPrimitiveType + Parser,
    T::Native: num_traits::NumCast,
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
            other => panic!("failed to decode JSON {other:?} as {}", self.data_type),
        };
        let parsed =
            parsed.unwrap_or_else(|| panic!("JSON number out of range for {}", self.data_type));
        self.builder.append_value(parsed);
    }

    fn append_key(&mut self, key: &str) {
        let parsed = T::parse(key)
            .unwrap_or_else(|| panic!("failed to parse \"{key}\" as {}", self.data_type));
        self.builder.append_value(parsed);
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

/// TIMESTAMP / TIMESTAMP_LTZ (nanosecond): strings parse through the same arrow-cast datetime parser
/// arrow-json uses (SQL `yyyy-MM-dd HH:mm:ss[.SSS]` and ISO-8601 forms); a bare number is taken as a
/// raw nanosecond epoch value, saturating like arrow-json's float fallback.
pub(crate) struct TimestampJsonAppender {
    builder: PrimitiveBuilder<TimestampNanosecondType>,
    data_type: DataType,
}

impl TimestampJsonAppender {
    fn new(data_type: &DataType, capacity: usize) -> TimestampJsonAppender {
        TimestampJsonAppender {
            builder: PrimitiveBuilder::with_capacity(capacity).with_data_type(data_type.clone()),
            data_type: data_type.clone(),
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
        let nanos = match v.value_type() {
            simd_json::ValueType::Null => {
                self.builder.append_null();
                return;
            }
            simd_json::ValueType::String => {
                self.append_key(v.as_str().expect("string node"));
                return;
            }
            simd_json::ValueType::I64 => v.as_i64().expect("i64 node"),
            simd_json::ValueType::U64 => {
                v.as_u64().expect("u64 node").min(i64::MAX as u64) as i64
            }
            simd_json::ValueType::F64 => v.as_f64().expect("f64 node") as i64,
            other => panic!("failed to decode JSON {other:?} as {}", self.data_type),
        };
        self.builder.append_value(nanos);
    }

    fn append_key(&mut self, key: &str) {
        let date = string_to_datetime(&chrono::Utc, key)
            .unwrap_or_else(|e| panic!("failed to parse \"{key}\" as {}: {e}", self.data_type));
        let nanos = date
            .timestamp_nanos_opt()
            .unwrap_or_else(|| panic!("\"{key}\" overflows 64-bit signed nanoseconds"));
        self.builder.append_value(nanos);
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

pub(crate) struct BooleanJsonAppender {
    builder: BooleanBuilder,
}

impl JsonAppend for BooleanJsonAppender {
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>) {
        use simd_json::prelude::*;
        let Some(v) = value else {
            self.builder.append_null();
            return;
        };
        match v.value_type() {
            simd_json::ValueType::Null => self.builder.append_null(),
            simd_json::ValueType::Bool => {
                self.builder.append_value(v.as_bool().expect("bool node"))
            }
            other => panic!("failed to decode JSON {other:?} as BOOLEAN"),
        }
    }

    fn append_key(&mut self, key: &str) {
        panic!("failed to parse map key \"{key}\" as BOOLEAN");
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}

pub(crate) struct StringJsonAppender {
    builder: StringBuilder,
}

impl JsonAppend for StringJsonAppender {
    fn append(&mut self, value: Option<simd_json::tape::Value<'_, '_>>) {
        use simd_json::prelude::*;
        let Some(v) = value else {
            self.builder.append_null();
            return;
        };
        match v.value_type() {
            simd_json::ValueType::Null => self.builder.append_null(),
            simd_json::ValueType::String => {
                self.builder.append_value(v.as_str().expect("string node"))
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

pub(crate) struct StructJsonAppender {
    fields: Fields,
    children: Vec<Box<dyn JsonAppend>>,
    /// Name→child lookup above a linear-scan threshold (arrow-json's heuristic: a map only pays for
    /// itself on wide structs).
    index: Option<HashMap<String, usize>>,
    nulls: NullBufferBuilder,
}

impl StructJsonAppender {
    fn new(fields: &Fields, capacity: usize) -> StructJsonAppender {
        let children =
            fields.iter().map(|f| make_json_appender(f.data_type(), capacity)).collect();
        let index = (fields.len() >= 16).then(|| {
            let mut map = HashMap::with_capacity_and_hasher(fields.len(), Default::default());
            for (i, field) in fields.iter().enumerate() {
                map.entry(field.name().clone()).or_insert(i);
            }
            map
        });
        StructJsonAppender {
            fields: fields.clone(),
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
            _ => Some(
                v.as_object()
                    .unwrap_or_else(|| panic!("failed to decode JSON {:?} as ROW", v.value_type())),
            ),
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
    child: Box<dyn JsonAppend>,
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
}

impl ListJsonAppender {
    fn new(field: &FieldRef, capacity: usize) -> ListJsonAppender {
        ListJsonAppender {
            field: field.clone(),
            child: make_json_appender(field.data_type(), capacity),
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
            _ => Some(v.as_array().unwrap_or_else(|| {
                panic!("failed to decode JSON {:?} as ARRAY", v.value_type())
            })),
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
    entry_fields: Fields,
    keys: Box<dyn JsonAppend>,
    values: Box<dyn JsonAppend>,
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
}

impl MapJsonAppender {
    fn new(entries_field: &FieldRef, capacity: usize) -> MapJsonAppender {
        let entry_fields = match entries_field.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => fields.clone(),
            other => panic!("MAP entries must be a two-field struct, got {other}"),
        };
        MapJsonAppender {
            entries_field: entries_field.clone(),
            keys: make_json_appender(entry_fields[0].data_type(), capacity),
            values: make_json_appender(entry_fields[1].data_type(), capacity),
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
            _ => Some(
                v.as_object()
                    .unwrap_or_else(|| panic!("failed to decode JSON {:?} as MAP", v.value_type())),
            ),
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
pub(crate) fn make_json_appender(data_type: &DataType, capacity: usize) -> Box<dyn JsonAppend> {
    use arrow::datatypes::TimeUnit;
    match data_type {
        DataType::Int8 => Box::new(PrimitiveJsonAppender::<Int8Type>::new(data_type, capacity)),
        DataType::Int16 => Box::new(PrimitiveJsonAppender::<Int16Type>::new(data_type, capacity)),
        DataType::Int32 => Box::new(PrimitiveJsonAppender::<Int32Type>::new(data_type, capacity)),
        DataType::Int64 => Box::new(PrimitiveJsonAppender::<Int64Type>::new(data_type, capacity)),
        DataType::Float32 => {
            Box::new(PrimitiveJsonAppender::<Float32Type>::new(data_type, capacity))
        }
        DataType::Float64 => {
            Box::new(PrimitiveJsonAppender::<Float64Type>::new(data_type, capacity))
        }
        DataType::Date32 => Box::new(PrimitiveJsonAppender::<Date32Type>::new(data_type, capacity)),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            Box::new(TimestampJsonAppender::new(data_type, capacity))
        }
        DataType::Boolean => Box::new(BooleanJsonAppender { builder: BooleanBuilder::new() }),
        DataType::Utf8 => Box::new(StringJsonAppender { builder: StringBuilder::new() }),
        DataType::Struct(fields) => Box::new(StructJsonAppender::new(fields, capacity)),
        DataType::List(field) => Box::new(ListJsonAppender::new(field, capacity)),
        DataType::Map(entries, false) => Box::new(MapJsonAppender::new(entries, capacity)),
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
pub(crate) fn decode_json_bodies_simd(schema: &SchemaRef, bodies: &RecordBatch) -> RecordBatch {
    let column = bodies.column(0);
    let mut root = StructJsonAppender::new(schema.fields(), bodies.num_rows());
    let mut scratch: Vec<u8> = Vec::new();
    let mut buffers = simd_json::Buffers::default();
    for row in 0..bodies.num_rows() {
        let Some(bytes) = binary_body(column, row) else { continue };
        if bytes.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        scratch.clear();
        scratch.extend_from_slice(bytes);
        let tape = simd_json::to_tape_with_buffers(&mut scratch, &mut buffers)
            .expect("failed to decode JSON record");
        let value = tape.as_value();
        let object = value.as_object().expect("JSON body was not a single object");
        root.append_object(&object);
    }
    RecordBatch::try_new(schema.clone(), root.finish_columns())
        .expect("failed to build JSON batch")
}

pub(crate) struct JsonDecoder {
    pub(crate) schema: SchemaRef,
    /// DECIMAL columns need the raw number literal for exactness (see
    /// `json_needs_raw_number_literals`); those schemas decode via arrow-json, all others via the
    /// simd-json tape walk.
    raw_literals: bool,
}

impl JsonDecoder {
    pub(crate) fn new(schema: SchemaRef) -> JsonDecoder {
        let raw_literals =
            schema.fields().iter().any(|f| json_needs_raw_number_literals(f.data_type()));
        JsonDecoder { schema, raw_literals }
    }

    /// Decodes the single body column of `bodies` into a batch of the target schema. Each row is a
    /// complete document; a null body contributes no row.
    pub(crate) fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        if self.raw_literals {
            return self.decode_raw_literals(bodies);
        }
        decode_json_bodies_simd(&self.schema, bodies)
    }

    /// The arrow-json path for decimal-bearing schemas: its tape keeps each number's raw literal, so
    /// decimals parse from the exact digit string. Documents feed one at a time to keep the decoder's
    /// record boundaries aligned with the input rows.
    fn decode_raw_literals(&self, bodies: &RecordBatch) -> RecordBatch {
        let column = bodies.column(0);
        let mut decoder = arrow::json::ReaderBuilder::new(self.schema.clone())
            .with_batch_size(bodies.num_rows().max(1))
            .build_decoder()
            .expect("failed to build JSON decoder");
        for row in 0..bodies.num_rows() {
            if let Some(bytes) = binary_body(column, row) {
                let consumed = decoder.decode(bytes).expect("failed to decode JSON record");
                assert_eq!(consumed, bytes.len(), "JSON body was not a single complete document");
            }
        }
        decoder
            .flush()
            .expect("failed to flush JSON batch")
            .unwrap_or_else(|| RecordBatch::new_empty(self.schema.clone()))
    }
}
