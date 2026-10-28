use crate::*;
use serde::{Deserialize, Serialize};
use streamfusion_bridge::java_map_order::JavaMapOrder;

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

#[derive(Serialize, Deserialize)]
pub(crate) struct CollectionState {
    kind: i64,
    element: PrimitiveType,
    values: Vec<Option<i64>>,
    pending: Vec<Option<i64>>,
    counts: std::collections::BTreeMap<i64, i32>,
    #[serde(with = "entries")]
    distinct: std::collections::BTreeMap<Option<i64>, i64>,
    times: std::collections::BTreeMap<i64, i64>,
    order: JavaMapOrder<i64>,
}

impl CollectionState {
    pub(super) fn new(kind: i64, ty: &DataType) -> Self {
        Self {
            kind,
            element: PrimitiveType::new(ty),
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
            .map(|(v, _)| *v)
            .collect();
        for key in &expired {
            self.times.remove(key);
            self.counts.remove(key);
            self.order
                .remove(*key, hash(*key, &self.element.data_type()));
        }
        self.order.compact();
        !expired.is_empty()
    }

    pub(super) fn accepts_null(&self) -> bool {
        matches!(self.kind, 50 | 51)
    }

    pub(super) fn forget_distinct(&mut self, value: &ScalarValue) {
        self.distinct.remove(&primitive(value));
    }
    pub(super) fn distinct_count(&self, value: &ScalarValue) -> i64 {
        self.distinct.get(&primitive(value)).copied().unwrap_or(0)
    }
    pub(super) fn distinct_entries(&self) -> Vec<(ScalarValue, i64)> {
        self.distinct
            .iter()
            .map(|(value, count)| (self.element.scalar(*value), *count))
            .collect()
    }

    pub(super) fn update(
        &mut self,
        value: Option<i64>,
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
                self.distinct.insert(value, next);
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
                self.order
                    .remove(value, hash(value, &self.element.data_type()));
            } else {
                let next = old.unwrap_or(0).wrapping_add(if retract { -1 } else { 1 });
                self.counts.insert(value, next);
                if ttl.enabled() {
                    self.times.insert(value, ttl.now());
                }
                if old.is_none() {
                    self.order
                        .insert(value, hash(value, &self.element.data_type()));
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
        fn array(values: impl IntoIterator<Item = Option<i64>>, ty: &DataType) -> ArrayRef {
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
                DataType::Int64 => {
                    Arc::new(Int64Array::from(values.into_iter().collect::<Vec<_>>()))
                }
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
        let ty = self.element.data_type();
        let ty = &ty;
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
                    copy.insert(key, hash(key, ty));
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
                    array(keys.into_iter().map(Some), ty),
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
                array(self.values.iter().copied(), ty),
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
        std::mem::size_of::<Self>()
            + (self.values.capacity() + self.pending.capacity())
                * std::mem::size_of::<Option<i64>>()
            + self.counts.len() * 48
            + self.distinct.len() * 48
            + self.times.len() * 48
            + self.order.bytes()
    }
}

pub(super) enum CollectionColumn<'a> {
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
        }
    }
}

mod entries {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;
    pub fn serialize<S: Serializer>(
        map: &BTreeMap<Option<i64>, i64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<Option<i64>, i64>, D::Error> {
        Ok(Vec::<(Option<i64>, i64)>::deserialize(deserializer)?
            .into_iter()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
