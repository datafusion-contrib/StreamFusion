use super::java_map_order::JavaMapOrder;
use crate::*;
use serde::{Deserialize, Serialize};
mod elements;
use elements::{DecimalElement, FloatElement, StringElement, TimestampElement};

pub(super) fn is_collection(kind: i64) -> bool {
    (48..=53).contains(&kind)
}

pub(super) fn result_type(kind: i64, element: &DataType) -> DataType {
    if kind <= 49 {
        DataType::Map(
            Arc::new(Field::new(
                "items",
                DataType::Struct(
                    vec![
                        Field::new("key", element.clone(), false),
                        Field::new("value", DataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    } else {
        DataType::List(Arc::new(Field::new("item", element.clone(), true)))
    }
}

pub(super) fn output_field(name: String, kind: i64, data_type: &DataType) -> Field {
    let field = Field::new(name, data_type.clone(), true);
    if (50..=53).contains(&kind) {
        field.with_metadata(std::collections::HashMap::from([(
            "streamfusion.array-copy".into(),
            "1".into(),
        )]))
    } else {
        field
    }
}

// Primitive boxed values use Java's value hash and comparison, including signed narrow integers.
fn hash(value: i64, ty: &DataType) -> i32 {
    let hash = match ty {
        DataType::Boolean => {
            if value != 0 {
                1231
            } else {
                1237
            }
        }
        DataType::Int64 => (value as u64 ^ ((value as u64) >> 32)) as u32,
        _ => value as u32,
    };
    (hash ^ (hash >> 16)) as i32
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum PrimitiveType {
    Boolean,
    I8,
    I16,
    I32,
    I64,
    Date,
    Time,
}
impl PrimitiveType {
    fn new(ty: &DataType) -> Self {
        match ty {
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::I8,
            DataType::Int16 => Self::I16,
            DataType::Int32 => Self::I32,
            DataType::Int64 => Self::I64,
            DataType::Date32 => Self::Date,
            DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => Self::Time,
            _ => unreachable!("verified collection element"),
        }
    }
    fn data_type(self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::I8 => DataType::Int8,
            Self::I16 => DataType::Int16,
            Self::I32 => DataType::Int32,
            Self::I64 => DataType::Int64,
            Self::Date => DataType::Date32,
            Self::Time => DataType::Time32(arrow::datatypes::TimeUnit::Millisecond),
        }
    }
    fn scalar(self, value: Option<i64>) -> ScalarValue {
        match self {
            Self::Boolean => ScalarValue::Boolean(value.map(|v| v != 0)),
            Self::I8 => ScalarValue::Int8(value.map(|v| v as i8)),
            Self::I16 => ScalarValue::Int16(value.map(|v| v as i16)),
            Self::I32 => ScalarValue::Int32(value.map(|v| v as i32)),
            Self::I64 => ScalarValue::Int64(value),
            Self::Date => ScalarValue::Date32(value.map(|v| v as i32)),
            Self::Time => ScalarValue::Time32Millisecond(value.map(|v| v as i32)),
        }
    }
}
fn primitive(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Boolean(v) => v.map(i64::from),
        ScalarValue::Int8(v) => v.map(i64::from),
        ScalarValue::Int16(v) => v.map(i64::from),
        ScalarValue::Int32(v) => v.map(i64::from),
        ScalarValue::Int64(v) => *v,
        ScalarValue::Date32(v) | ScalarValue::Time32Millisecond(v) => v.map(i64::from),
        _ => unreachable!("verified collection membership"),
    }
}

#[derive(Clone)]
enum CollectionStorage {
    Primitive(TypedCollectionState<PrimitiveType>),
    Float(TypedCollectionState<FloatElement>),
    Decimal(TypedCollectionState<DecimalElement>),
    Timestamp(TypedCollectionState<TimestampElement>),
    String(TypedCollectionState<StringElement>),
}

#[derive(Clone)]
pub(crate) struct CollectionState {
    storage: CollectionStorage,
}

macro_rules! with_storage {
    ($storage:expr, $state:ident, $body:expr) => {
        match $storage {
            CollectionStorage::Primitive($state) => $body,
            CollectionStorage::Float($state) => $body,
            CollectionStorage::Decimal($state) => $body,
            CollectionStorage::Timestamp($state) => $body,
            CollectionStorage::String($state) => $body,
        }
    };
}

impl CollectionState {
    pub(super) fn new(kind: i64, ty: &DataType) -> Self {
        let storage = match ty {
            DataType::Utf8 => {
                assert!(kind > 49, "character COLLECT retains the JVM handler");
                CollectionStorage::String(TypedCollectionState::new(kind, ty))
            }
            ty if streamfusion_bridge::timestamp::is_component_timestamp(ty) => {
                CollectionStorage::Timestamp(TypedCollectionState::new(kind, ty))
            }
            DataType::Float32 | DataType::Float64 => {
                CollectionStorage::Float(TypedCollectionState::new(kind, ty))
            }
            DataType::Decimal128(_, _) => {
                CollectionStorage::Decimal(TypedCollectionState::new(kind, ty))
            }
            _ => CollectionStorage::Primitive(TypedCollectionState::new(kind, ty)),
        };
        Self { storage }
    }

    pub(super) fn update(
        &mut self,
        column: &CollectionColumn<'_>,
        row: usize,
        retract: bool,
        ttl: StateTtl,
        append_distinct: bool,
    ) -> bool {
        use super::numeric_map_order::FloatingKey;
        match (&mut self.storage, column) {
            (CollectionStorage::String(state), CollectionColumn::String(column)) => state.update(
                (!column.is_null(row)).then(|| Arc::<str>::from(column.value(row))),
                retract,
                ttl,
                append_distinct,
            ),
            (CollectionStorage::Primitive(state), column) => {
                state.update(column.at(row), retract, ttl, append_distinct)
            }
            (CollectionStorage::Float(state), CollectionColumn::F32(column)) => state.update(
                (!column.is_null(row)).then(|| FloatingKey::from_f32(column.value(row))),
                retract,
                ttl,
                append_distinct,
            ),
            (CollectionStorage::Float(state), CollectionColumn::F64(column)) => state.update(
                (!column.is_null(row)).then(|| FloatingKey::from_f64(column.value(row))),
                retract,
                ttl,
                append_distinct,
            ),
            (CollectionStorage::Decimal(state), CollectionColumn::Decimal(column)) => state.update(
                (!column.is_null(row)).then(|| column.value(row)),
                retract,
                ttl,
                append_distinct,
            ),
            (CollectionStorage::Timestamp(state), CollectionColumn::Timestamp(column)) => state
                .update(
                    (!column.is_null(row)).then(|| {
                        column
                            .value(row)
                            .expect("timestamp collection value")
                            .nanos()
                    }),
                    retract,
                    ttl,
                    append_distinct,
                ),
            _ => unreachable!("collection column and codec must agree"),
        }
    }

    pub(super) fn expire(&mut self, ttl: StateTtl) -> bool {
        with_storage!(&mut self.storage, state, state.expire(ttl))
    }
    pub(super) fn accepts_null(&self) -> bool {
        with_storage!(&self.storage, state, state.accepts_null())
    }
    pub(super) fn forget_distinct(&mut self, value: &ScalarValue) {
        with_storage!(&mut self.storage, state, state.forget_distinct(value))
    }
    pub(super) fn distinct_count(&self, value: &ScalarValue) -> i64 {
        with_storage!(&self.storage, state, state.distinct_count(value))
    }
    pub(super) fn distinct_entries(&self) -> Vec<(ScalarValue, i64)> {
        with_storage!(&self.storage, state, state.distinct_entries())
    }
    pub(super) fn emit(&self, result: &DataType) -> ScalarValue {
        with_storage!(&self.storage, state, state.emit(result))
    }
    pub(super) fn snapshot(&self) -> ScalarValue {
        with_storage!(&self.storage, state, state.snapshot())
    }
    pub(super) fn restore(&mut self, value: ScalarValue) -> Result<(), DataFusionError> {
        with_storage!(&mut self.storage, state, state.restore(value))
    }
    pub(super) fn bytes(&self) -> usize {
        with_storage!(
            &self.storage,
            state,
            state.bytes() + std::mem::size_of::<Self>() - std::mem::size_of_val(state)
        )
    }
}

trait CollectionElement: Clone + PartialEq {
    type Value: Clone + Ord;
    const VARIABLE_SIZE: bool = false;
    fn payload_bytes(_: &Self::Value) -> usize {
        0
    }
    fn from_type(ty: &DataType) -> Self;
    fn scalar(&self, value: Option<Self::Value>) -> ScalarValue;
    fn value(&self, value: &ScalarValue) -> Option<Self::Value>;
    fn hash(&self, value: &Self::Value) -> i32;
    fn array(
        &self,
        values: impl IntoIterator<Item = Option<Self::Value>>,
        ty: &DataType,
    ) -> ArrayRef;
}

impl CollectionElement for PrimitiveType {
    type Value = i64;
    fn from_type(ty: &DataType) -> Self {
        Self::new(ty)
    }
    fn scalar(&self, value: Option<i64>) -> ScalarValue {
        (*self).scalar(value)
    }
    fn value(&self, value: &ScalarValue) -> Option<i64> {
        primitive(value)
    }
    fn hash(&self, value: &i64) -> i32 {
        hash(*value, &self.data_type())
    }
    fn array(&self, values: impl IntoIterator<Item = Option<i64>>, _: &DataType) -> ArrayRef {
        let ty = &self.data_type();
        match ty {
            DataType::Boolean => Arc::new(BooleanArray::from(
                values
                    .into_iter()
                    .map(|v| v.map(|v| v != 0))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int8 => Arc::new(Int8Array::from(
                values
                    .into_iter()
                    .map(|v| v.map(|v| v as i8))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int16 => Arc::new(Int16Array::from(
                values
                    .into_iter()
                    .map(|v| v.map(|v| v as i16))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int32 => Arc::new(Int32Array::from(
                values
                    .into_iter()
                    .map(|v| v.map(|v| v as i32))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int64 => Arc::new(Int64Array::from(values.into_iter().collect::<Vec<_>>())),
            DataType::Date32 => Arc::new(arrow::array::Date32Array::from(
                values
                    .into_iter()
                    .map(|v| v.map(|v| v as i32))
                    .collect::<Vec<_>>(),
            )),
            DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => {
                Arc::new(arrow::array::Time32MillisecondArray::from(
                    values
                        .into_iter()
                        .map(|v| v.map(|v| v as i32))
                        .collect::<Vec<_>>(),
                ))
            }
            _ => unreachable!("verified collection element type"),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(bound(
    serialize = "C: Serialize, C::Value: Serialize",
    deserialize = "C: Deserialize<'de>, C::Value: Deserialize<'de>"
))]
#[derive(Clone)]
struct TypedCollectionState<C: CollectionElement> {
    kind: i64,
    element: C,
    values: Vec<Option<C::Value>>,
    pending: Vec<Option<C::Value>>,
    counts: std::collections::BTreeMap<C::Value, i32>,
    #[serde(with = "entries")]
    distinct: std::collections::BTreeMap<Option<C::Value>, i64>,
    times: std::collections::BTreeMap<C::Value, i64>,
    order: JavaMapOrder<C::Value>,
}

impl<C> TypedCollectionState<C>
where
    C: CollectionElement + Serialize + serde::de::DeserializeOwned,
    C::Value: Serialize + serde::de::DeserializeOwned,
{
    pub(super) fn new(kind: i64, ty: &DataType) -> Self {
        Self {
            kind,
            element: C::from_type(ty),
            values: Vec::new(),
            pending: Vec::new(),
            counts: Default::default(),
            distinct: Default::default(),
            times: Default::default(),
            order: JavaMapOrder::with_capacity(16),
        }
    }

    pub(super) fn expire(&mut self, ttl: StateTtl) -> bool {
        if !ttl.enabled() {
            self.times.clear();
            return false;
        }
        let expired: Vec<_> = self
            .times
            .iter()
            .filter(|(_, time)| ttl.expired(**time))
            .map(|(v, _)| v.clone())
            .collect();
        for key in &expired {
            self.times.remove(key);
            self.counts.remove(key);
            self.order.remove(key.clone(), self.element.hash(key));
        }
        self.order.compact();
        !expired.is_empty()
    }

    pub(super) fn accepts_null(&self) -> bool {
        matches!(self.kind, 50 | 51)
    }

    pub(super) fn forget_distinct(&mut self, value: &ScalarValue) {
        self.distinct.remove(&self.element.value(value));
    }
    pub(super) fn distinct_count(&self, value: &ScalarValue) -> i64 {
        self.distinct
            .get(&self.element.value(value))
            .copied()
            .unwrap_or(0)
    }
    pub(super) fn distinct_entries(&self) -> Vec<(ScalarValue, i64)> {
        self.distinct
            .iter()
            .map(|(value, count)| (self.element.scalar(value.clone()), *count))
            .collect()
    }

    pub(super) fn update(
        &mut self,
        value: Option<C::Value>,
        retract: bool,
        ttl: StateTtl,
        append_distinct: bool,
    ) -> bool {
        let collect = self.kind <= 49;
        if value.is_none() && !self.accepts_null() {
            return false;
        }
        if matches!(self.kind, 49 | 51 | 53) {
            let old = self.distinct.get(&value).copied().unwrap_or(0);
            if append_distinct && old != 0 {
                return false;
            }
            let next = if append_distinct {
                1
            } else {
                old.wrapping_add(if retract { -1 } else { 1 })
            };
            if next == 0 {
                self.distinct.remove(&value);
            } else {
                self.distinct.insert(value.clone(), next);
            }
            if (retract && next != 0) || (!retract && next != 1) {
                return false;
            }
        }
        if collect {
            let value = value.unwrap();
            let old = self.counts.get(&value).copied();
            // Accumulate retains a zero after a negative count; retract removes only count one.
            if retract && old == Some(1) {
                self.counts.remove(&value);
                self.times.remove(&value);
                self.order.remove(value.clone(), self.element.hash(&value));
            } else {
                let next = old.unwrap_or(0).wrapping_add(if retract { -1 } else { 1 });
                self.counts.insert(value.clone(), next);
                if ttl.enabled() {
                    self.times.insert(value.clone(), ttl.now());
                }
                if old.is_none() {
                    self.order.insert(value.clone(), self.element.hash(&value));
                }
            }
        } else if retract {
            if let Some(index) = self.values.iter().position(|old| *old == value) {
                self.values.remove(index);
            } else {
                self.pending.push(value);
            }
        } else {
            self.values.push(value);
        }
        self.order.compact();
        true
    }

    pub(super) fn emit(&self, result: &DataType) -> ScalarValue {
        if self.kind <= 49 {
            // StateMapView.getMap materializes a fresh HashMap for each result.
            let capacity = (self.order.len().saturating_mul(4).div_ceil(3))
                .next_power_of_two()
                .max(16);
            let mut copy;
            let order = if self.order.copy_preserves_order(capacity) {
                &self.order
            } else {
                copy = JavaMapOrder::with_capacity(16);
                for key in self.order.iter() {
                    copy.insert(key.clone(), self.element.hash(&key));
                }
                &copy
            };
            let keys: Vec<_> = order.iter().collect();
            let counts = keys.iter().map(|key| self.counts[key]).collect::<Vec<_>>();
            let DataType::Map(field, _) = result else {
                unreachable!()
            };
            let DataType::Struct(fields) = field.data_type() else {
                unreachable!()
            };
            let entries = arrow::array::StructArray::new(
                fields.clone(),
                vec![
                    self.element
                        .array(keys.into_iter().map(Some), fields[0].data_type()),
                    Arc::new(Int32Array::from(counts)),
                ],
                None,
            );
            let map = arrow::array::MapArray::new(
                field.clone(),
                arrow::buffer::OffsetBuffer::from_lengths([entries.len()]),
                entries,
                None,
                false,
            );
            ScalarValue::Map(Arc::new(map))
        } else if self.values.is_empty() {
            null_scalar(result)
        } else {
            let DataType::List(field) = result else {
                unreachable!()
            };
            ScalarValue::List(Arc::new(arrow::array::ListArray::new(
                field.clone(),
                arrow::buffer::OffsetBuffer::from_lengths([self.values.len()]),
                self.element
                    .array(self.values.iter().cloned(), field.data_type()),
                None,
            )))
        }
    }

    pub(super) fn snapshot(&self) -> ScalarValue {
        ScalarValue::Binary(Some(
            serde_json::to_vec(&(1u8, self)).expect("collection state serialization"),
        ))
    }
    pub(super) fn restore(&mut self, value: ScalarValue) -> Result<(), DataFusionError> {
        let ScalarValue::Binary(Some(bytes)) = value else {
            return Err(DataFusionError::Execution(
                "missing native collection state; JVM savepoints require the JVM route".into(),
            ));
        };
        let (version, restored): (u8, Self) = serde_json::from_slice(&bytes)
            .map_err(|e| DataFusionError::Execution(format!("invalid collection state: {e}")))?;
        if version != 1 || restored.kind != self.kind || restored.element != self.element {
            return Err(DataFusionError::Execution(
                "incompatible collection state".into(),
            ));
        }
        *self = restored;
        Ok(())
    }
    pub(super) fn bytes(&self) -> usize {
        let payload = if C::VARIABLE_SIZE {
            self.values
                .iter()
                .chain(&self.pending)
                .flatten()
                .chain(self.counts.keys())
                .chain(self.distinct.keys().flatten())
                .chain(self.times.keys())
                .chain(self.order.retained_keys())
                .map(C::payload_bytes)
                .sum()
        } else {
            0
        };
        payload
            + std::mem::size_of::<Self>()
            + (self.values.capacity() + self.pending.capacity())
                * std::mem::size_of::<Option<C::Value>>()
            + (self.counts.len() + self.distinct.len() + self.times.len())
                * (40 + std::mem::size_of::<C::Value>())
            + self.order.bytes()
    }
}

pub(super) enum CollectionColumn<'a> {
    String(&'a StringArray),
    Timestamp(streamfusion_bridge::timestamp::TimestampColumn<'a>),
    F32(&'a arrow::array::Float32Array),
    F64(&'a arrow::array::Float64Array),
    Decimal(&'a arrow::array::Decimal128Array),
    Boolean(&'a BooleanArray),
    I8(&'a Int8Array),
    I16(&'a Int16Array),
    I32(&'a Int32Array),
    I64(&'a Int64Array),
    Date(&'a arrow::array::Date32Array),
    Time(&'a arrow::array::Time32MillisecondArray),
}
impl<'a> CollectionColumn<'a> {
    pub(super) fn new(array: &'a ArrayRef) -> Self {
        match array.data_type() {
            DataType::Utf8 => Self::String(array.as_any().downcast_ref().unwrap()),
            ty if streamfusion_bridge::timestamp::is_component_timestamp(ty) => Self::Timestamp(
                streamfusion_bridge::timestamp::TimestampColumn::try_new(array.as_ref())
                    .expect("timestamp collection column"),
            ),
            DataType::Float32 => Self::F32(array.as_any().downcast_ref().unwrap()),
            DataType::Float64 => Self::F64(array.as_any().downcast_ref().unwrap()),
            DataType::Decimal128(_, _) => Self::Decimal(array.as_any().downcast_ref().unwrap()),
            DataType::Boolean => Self::Boolean(array.as_any().downcast_ref().unwrap()),
            DataType::Int8 => Self::I8(array.as_any().downcast_ref().unwrap()),
            DataType::Int16 => Self::I16(array.as_any().downcast_ref().unwrap()),
            DataType::Int32 => Self::I32(array.as_any().downcast_ref().unwrap()),
            DataType::Int64 => Self::I64(array.as_any().downcast_ref().unwrap()),
            DataType::Date32 => Self::Date(array.as_any().downcast_ref().unwrap()),
            DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => {
                Self::Time(array.as_any().downcast_ref().unwrap())
            }
            _ => unreachable!("verified collection input"),
        }
    }
    pub(super) fn at(&self, row: usize) -> Option<i64> {
        macro_rules! read {
            ($a:expr) => {
                (!$a.is_null(row)).then(|| $a.value(row) as i64)
            };
        }
        match self {
            Self::Boolean(a) => read!(a),
            Self::I8(a) => read!(a),
            Self::I16(a) => read!(a),
            Self::I32(a) => read!(a),
            Self::I64(a) => read!(a),
            Self::Date(a) => read!(a),
            Self::Time(a) => read!(a),
            _ => unreachable!("primitive collection column"),
        }
    }
}

mod entries {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;
    pub fn serialize<K: Serialize, S: Serializer>(
        map: &BTreeMap<Option<K>, i64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }
    pub fn deserialize<'de, K: Ord + Deserialize<'de>, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<Option<K>, i64>, D::Error> {
        Ok(Vec::<(Option<K>, i64)>::deserialize(deserializer)?
            .into_iter()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type CollectionState = TypedCollectionState<PrimitiveType>;
    #[test]
    fn owned_string_arrays_keep_slices_outputs_and_versioned_state() {
        let wide = "é🙂\0".repeat(1024);
        for kind in 50..=53 {
            let input: ArrayRef = Arc::new(StringArray::from(vec![
                Some("unread"),
                Some(wide.as_str()),
                None,
                Some(""),
                Some(wide.as_str()),
                Some("tail"),
                Some("unread"),
            ]));
            let slice = input.slice(1, 5);
            let column = CollectionColumn::new(&slice);
            let mut state = super::CollectionState::new(kind, &DataType::Utf8);
            let empty_bytes = state.bytes();
            for row in 0..5 {
                state.update(&column, row, false, StateTtl::new(0, 0), false);
            }
            assert!(state.bytes() >= empty_bytes + wide.len());
            let result = result_type(kind, &DataType::Utf8);
            let old = state.emit(&result);
            let snapshot = state.snapshot();
            let mut restored = super::CollectionState::new(kind, &DataType::Utf8);
            restored.restore(snapshot.clone()).unwrap();
            assert_eq!(old, restored.emit(&result));
            let mut wrong = super::CollectionState::new(kind, &DataType::Int64);
            assert!(wrong.restore(snapshot).is_err());
            state.update(&column, 0, true, StateTtl::new(0, 0), false);
            restored.update(&column, 0, true, StateTtl::new(0, 0), false);
            assert_eq!(state.emit(&result), restored.emit(&result));
            drop(column);
            drop(slice);
            drop(input);
            let ScalarValue::List(old_array) = old else {
                unreachable!()
            };
            let old_values = old_array
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(old_values.value(0), wide);
            assert_eq!(state.emit(&result), restored.emit(&result));
            let missing: ArrayRef = Arc::new(StringArray::from(vec!["missing"]));
            state.update(
                &CollectionColumn::new(&missing),
                0,
                true,
                StateTtl::new(0, 0),
                false,
            );
            restored.restore(state.snapshot()).unwrap();
            assert_eq!(state.emit(&result), restored.emit(&result));
        }
    }

    #[test]
    fn owned_string_retractions_remove_the_first_equal_value() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("b"),
            Some("a"),
        ]));
        let column = CollectionColumn::new(&input);
        let mut state = super::CollectionState::new(50, &DataType::Utf8);
        for row in 0..4 {
            state.update(&column, row, false, StateTtl::new(0, 0), false);
        }
        let independent: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
        state.update(
            &CollectionColumn::new(&independent),
            0,
            true,
            StateTtl::new(0, 0),
            false,
        );
        let ScalarValue::List(result) = state.emit(&result_type(50, &DataType::Utf8)) else {
            unreachable!()
        };
        let actual = result
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(actual, &StringArray::from(vec![None, Some("b"), Some("a")]));
    }

    #[test]
    fn collection_retractions_nulls_and_versioned_restore() {
        for ty in [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Date32,
            DataType::Time32(arrow::datatypes::TimeUnit::Millisecond),
        ] {
            for kind in 48..=53 {
                let mut state = CollectionState::new(kind, &ty);
                let ttl = StateTtl::new(0, 0);
                for (value, retract) in [
                    (Some(1), false),
                    (None, false),
                    (Some(0), false),
                    (Some(1), false),
                    (Some(1), true),
                    (Some(1), false),
                    (Some(0), true),
                ] {
                    state.update(value, retract, ttl, false);
                }
                let result = result_type(kind, &ty);
                let old = state.emit(&result);
                let mut restored = CollectionState::new(kind, &ty);
                restored.restore(state.snapshot()).unwrap();
                assert_eq!(old, restored.emit(&result));
                let expected = format!("{old:?}");
                state.update(Some(1), true, ttl, false);
                assert_eq!(
                    expected,
                    format!("{old:?}"),
                    "outputs must own their buffers"
                );
                assert!(CollectionState::new(if kind == 48 { 49 } else { 48 }, &ty)
                    .restore(state.snapshot())
                    .is_err());
            }
        }
    }

    #[test]
    fn temporal_columns_keep_slices_and_distinct_membership_types() {
        for (ty, array) in [
            (
                DataType::Date32,
                Arc::new(arrow::array::Date32Array::from(vec![
                    Some(99),
                    Some(-719162),
                    None,
                    Some(2932896),
                ])) as ArrayRef,
            ),
            (
                DataType::Time32(arrow::datatypes::TimeUnit::Millisecond),
                Arc::new(arrow::array::Time32MillisecondArray::from(vec![
                    Some(99),
                    Some(0),
                    None,
                    Some(86399999),
                ])) as ArrayRef,
            ),
        ] {
            let input = array.slice(1, 3);
            let column = CollectionColumn::new(&input);
            let mut state = CollectionState::new(51, &ty);
            for row in 0..3 {
                let value = column.at(row);
                state.update(value, false, StateTtl::disabled(), false);
                state.update(value, false, StateTtl::disabled(), false);
                assert_eq!(state.distinct_count(&state.element.scalar(value)), 2);
            }
            let output = state.emit(&result_type(51, &ty));
            let ScalarValue::List(list) = output else {
                unreachable!()
            };
            assert_eq!(list.values().as_ref(), input.as_ref());
            let mut restored = CollectionState::new(51, &ty);
            restored.restore(state.snapshot()).unwrap();
            for (value, count) in restored.distinct_entries() {
                assert_eq!(value.data_type(), ty);
                assert_eq!(count, 2);
                restored.forget_distinct(&value);
                assert_eq!(restored.distinct_count(&value), 0);
            }
            assert!(CollectionState::new(51, &DataType::Int32)
                .restore(state.snapshot())
                .is_err());
        }
    }

    #[test]
    fn collect_count_overflow_and_repeated_reinsertion_keep_bounded_order_state() {
        let mut state = CollectionState::new(48, &DataType::Int32);
        state.update(Some(9), false, StateTtl::disabled(), false);
        state.counts.insert(9, i32::MAX);
        state.update(Some(9), false, StateTtl::disabled(), false);
        assert_eq!(state.counts[&9], i32::MIN);
        state.counts.insert(9, 1);
        for _ in 0..1000 {
            state.update(Some(9), true, StateTtl::disabled(), false);
            state.update(Some(9), false, StateTtl::disabled(), false);
        }
        assert!(state.order.bytes() < 16_384);
        let before = state.snapshot();
        let mut other = CollectionState::new(48, &DataType::Int64);
        assert!(other.restore(before.clone()).is_err());
        let ScalarValue::Binary(Some(mut bytes)) = before else {
            unreachable!()
        };
        bytes[1] = b'9';
        assert!(state.restore(ScalarValue::Binary(Some(bytes))).is_err());
    }

    #[test]
    fn array_removes_first_match_and_preserves_unmatched_retractions() {
        let mut state = CollectionState::new(50, &DataType::Int64);
        let ttl = StateTtl::new(0, 0);
        for value in [Some(1), None, Some(2), Some(1)] {
            state.update(value, false, ttl, false);
        }
        state.update(Some(1), true, ttl, false);
        assert_eq!(state.values, vec![None, Some(2), Some(1)]);
        state.update(Some(7), true, ttl, false);
        state.update(Some(7), false, ttl, false);
        assert_eq!(state.pending, vec![Some(7)]);
        assert_eq!(state.values.last(), Some(&Some(7)));
    }

    #[test]
    fn collect_keeps_signed_counts_including_zero_after_accumulation() {
        let mut state = CollectionState::new(48, &DataType::Int32);
        let ttl = StateTtl::new(0, 0);
        state.update(Some(9), true, ttl, false);
        state.update(Some(9), false, ttl, false);
        assert_eq!(state.counts.get(&9), Some(&0));
        assert_eq!(state.order.iter().collect::<Vec<_>>(), vec![9]);
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;

    fn payloads(array: &dyn Array) -> Vec<Option<u128>> {
        (0..array.len())
            .map(|row| {
                if array.is_null(row) {
                    return None;
                }
                Some(match array.data_type() {
                    ty if streamfusion_bridge::timestamp::is_component_timestamp(ty) => {
                        streamfusion_bridge::timestamp::TimestampColumn::try_new(array)
                            .unwrap()
                            .value(row)
                            .unwrap()
                            .nanos() as u128
                    }
                    DataType::Float32 => array
                        .as_any()
                        .downcast_ref::<arrow::array::Float32Array>()
                        .unwrap()
                        .value(row)
                        .to_bits() as u128,
                    DataType::Float64 => array
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .unwrap()
                        .value(row)
                        .to_bits() as u128,
                    DataType::Decimal128(_, _) => array
                        .as_any()
                        .downcast_ref::<arrow::array::Decimal128Array>()
                        .unwrap()
                        .value(row) as u128,
                    _ => unreachable!(),
                })
            })
            .collect()
    }

    fn samples() -> Vec<ArrayRef> {
        use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
        vec![
            Arc::new(timestamp_array([
                Some(TimestampValue::new(i64::MIN, 1).unwrap()),
                Some(TimestampValue::new(i64::MIN, 1).unwrap()),
                Some(TimestampValue::new(-1, 999999).unwrap()),
                Some(TimestampValue::new(i64::MAX, 999999).unwrap()),
                None,
            ])),
            Arc::new(arrow::array::Float32Array::from(vec![
                Some(f32::from_bits(0x7f800001)),
                Some(f32::from_bits(0xffc00002)),
                Some(-0.0),
                Some(0.0),
                None,
            ])),
            Arc::new(arrow::array::Float64Array::from(vec![
                Some(f64::from_bits(0x7ff0000000000001)),
                Some(f64::from_bits(0xfff8000000000002)),
                Some(-0.0),
                Some(0.0),
                None,
            ])),
            Arc::new(
                arrow::array::Decimal128Array::from(vec![
                    Some(12345),
                    Some(12345),
                    Some(-1),
                    Some(0),
                    None,
                ])
                .with_precision_and_scale(8, 2)
                .unwrap(),
            ),
            Arc::new(
                arrow::array::Decimal128Array::from(vec![
                    Some(10i128.pow(38) - 1),
                    Some(10i128.pow(38) - 1),
                    Some(1 - 10i128.pow(38)),
                    Some(0),
                    None,
                ])
                .with_precision_and_scale(38, 18)
                .unwrap(),
            ),
        ]
    }

    #[test]
    fn numeric_family_shares_typed_retractions_distinct_and_owned_restore() {
        for original in samples() {
            let input = original.slice(0, original.len());
            let column = CollectionColumn::new(&input);
            for kind in 48..=53 {
                let mut state = CollectionState::new(kind, input.data_type());
                for row in 0..input.len() {
                    state.update(&column, row, false, StateTtl::disabled(), false);
                }
                let result = result_type(kind, input.data_type());
                let output = state.emit(&result);
                let before = match &output {
                    ScalarValue::List(array) => {
                        let mut expected = payloads(input.as_ref());
                        if matches!(kind, 51 | 53) {
                            expected.remove(1);
                        }
                        if kind >= 52 {
                            expected.retain(Option::is_some);
                        }
                        assert_eq!(payloads(array.values().as_ref()), expected);
                        payloads(array.values().as_ref())
                    }
                    ScalarValue::Map(array) => {
                        let keys = payloads(array.keys().as_ref());
                        assert!(keys.contains(&payloads(input.as_ref())[0]));
                        assert_eq!(keys.len(), 3);
                        let counts = array
                            .values()
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap();
                        let repeated = keys
                            .iter()
                            .position(|key| *key == payloads(input.as_ref())[0])
                            .unwrap();
                        assert_eq!(counts.value(repeated), if kind == 49 { 1 } else { 2 });
                        keys
                    }
                    _ => unreachable!(),
                };
                let checkpoint = state.snapshot();
                let mut restored = CollectionState::new(kind, input.data_type());
                restored.restore(checkpoint.clone()).unwrap();
                assert_eq!(checkpoint, restored.snapshot());
                restored.update(&column, 1, true, StateTtl::disabled(), false);
                state.update(&column, 1, true, StateTtl::disabled(), false);
                assert_eq!(state.snapshot(), restored.snapshot());
                if kind == 50 {
                    let ScalarValue::List(array) = restored.emit(&result) else {
                        unreachable!()
                    };
                    assert_eq!(
                        payloads(array.values().as_ref()),
                        payloads(input.as_ref())[1..]
                    );
                }
                let retained = match &output {
                    ScalarValue::List(array) => payloads(array.values().as_ref()),
                    ScalarValue::Map(array) => payloads(array.keys().as_ref()),
                    _ => unreachable!(),
                };
                assert_eq!(
                    before, retained,
                    "later retractions must not mutate old output"
                );
                assert!(CollectionState::new(kind, &DataType::Int64)
                    .restore(checkpoint)
                    .is_err());
            }
        }
    }

    #[test]
    fn previous_primitive_checkpoints_keep_the_exact_version_one_format() {
        for (kind, fixture) in [
            (
                48,
                include_bytes!("../../../../src/test/resources/collection-state-v1/kind-48.json")
                    .as_slice(),
            ),
            (
                49,
                include_bytes!("../../../../src/test/resources/collection-state-v1/kind-49.json")
                    .as_slice(),
            ),
            (
                50,
                include_bytes!("../../../../src/test/resources/collection-state-v1/kind-50.json")
                    .as_slice(),
            ),
            (
                51,
                include_bytes!("../../../../src/test/resources/collection-state-v1/kind-51.json")
                    .as_slice(),
            ),
            (
                52,
                include_bytes!("../../../../src/test/resources/collection-state-v1/kind-52.json")
                    .as_slice(),
            ),
            (
                53,
                include_bytes!("../../../../src/test/resources/collection-state-v1/kind-53.json")
                    .as_slice(),
            ),
        ] {
            let mut state = CollectionState::new(kind, &DataType::Int64);
            let snapshot = ScalarValue::Binary(Some(fixture.to_vec()));
            state.restore(snapshot.clone()).unwrap();
            assert_eq!(snapshot, state.snapshot());
        }
    }

    #[test]
    fn numeric_codecs_keep_slices_and_reject_other_widths_or_decimal_scales() {
        for input in samples() {
            let slice = input.slice(1, 3);
            let column = CollectionColumn::new(&slice);
            let mut state = CollectionState::new(50, input.data_type());
            for row in 0..slice.len() {
                state.update(&column, row, false, StateTtl::disabled(), false);
            }
            let ScalarValue::List(output) = state.emit(&result_type(50, input.data_type())) else {
                unreachable!()
            };
            assert_eq!(payloads(output.values().as_ref()), payloads(slice.as_ref()));
            for other in [
                DataType::Float32,
                DataType::Float64,
                DataType::Decimal128(38, 0),
                DataType::Decimal128(38, 18),
            ] {
                if &other != input.data_type() {
                    assert!(CollectionState::new(50, &other)
                        .restore(state.snapshot())
                        .is_err());
                }
            }
        }
    }
}
