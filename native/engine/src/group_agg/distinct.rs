use super::*;
use std::borrow::Borrow;
use std::hash::Hash;

fn timestamp_scalar(nanos: i128) -> ScalarValue {
    ScalarValue::Struct(Arc::new(streamfusion_bridge::timestamp::timestamp_array([
        Some(
            streamfusion_bridge::timestamp::TimestampValue::from_nanos(nanos)
                .expect("timestamp range"),
        ),
    ])))
}

pub(super) enum DistinctColumn<'a> {
    I64(&'a Int64Array),
    I32(&'a Int32Array),
    I16(&'a arrow::array::Int16Array),
    I8(&'a Int8Array),
    Decimal(&'a Decimal128Array),
    Boolean(&'a BooleanArray),
    Timestamp(streamfusion_bridge::timestamp::TimestampColumn<'a>),
    String(&'a arrow::array::StringArray),
    Scalar(&'a ArrayRef),
}

impl<'a> DistinctColumn<'a> {
    pub(super) fn new(array: &'a ArrayRef) -> Self {
        match array.data_type() {
            DataType::Int64 => Self::I64(array.as_any().downcast_ref().unwrap()),
            DataType::Int32 => Self::I32(array.as_any().downcast_ref().unwrap()),
            DataType::Int16 => Self::I16(array.as_any().downcast_ref().unwrap()),
            DataType::Int8 => Self::I8(array.as_any().downcast_ref().unwrap()),
            DataType::Decimal128(_, _) => Self::Decimal(array.as_any().downcast_ref().unwrap()),
            DataType::Boolean => Self::Boolean(array.as_any().downcast_ref().unwrap()),
            DataType::Utf8 => Self::String(array.as_any().downcast_ref().unwrap()),
            ty if streamfusion_bridge::timestamp::is_component_timestamp(ty) => Self::Timestamp(
                streamfusion_bridge::timestamp::TimestampColumn::try_new(array.as_ref()).unwrap(),
            ),
            _ => Self::Scalar(array),
        }
    }

    fn change(&self, set: &mut DistinctSet, row: usize, retract: bool, count: i64) -> bool {
        let change = if retract {
            Change::Remove
        } else {
            Change::Add(count)
        };
        macro_rules! primitive {
            ($array:expr, $variant:ident, $scalar:ident) => {{
                let array = $array;
                if array.is_null(row) {
                    return false;
                }
                let value = array.value(row);
                match set {
                    DistinctSet::$variant(m) => m.change_owned(value, change),
                    _ => set.change_scalar(&ScalarValue::$scalar(Some(value)), change),
                }
            }};
        }
        match self {
            Self::I64(a) => primitive!(a, I64, Int64),
            Self::I32(a) => primitive!(a, I32, Int32),
            Self::I16(a) => primitive!(a, I16, Int16),
            Self::I8(a) => primitive!(a, I8, Int8),
            Self::Boolean(a) => primitive!(a, Boolean, Boolean),
            Self::Decimal(a) => {
                if a.is_null(row) {
                    return false;
                }
                let DataType::Decimal128(p, s) = a.data_type() else {
                    unreachable!()
                };
                match set {
                    DistinctSet::Decimal(m, mp, ms) if p == mp && s == ms => {
                        m.change_owned(a.value(row), change)
                    }
                    _ => set.change_scalar(
                        &ScalarValue::Decimal128(Some(a.value(row)), *p, *s),
                        change,
                    ),
                }
            }
            Self::Timestamp(a) => {
                if a.is_null(row) {
                    return false;
                }
                let value = a.value(row).expect("timestamp value").nanos();
                match set {
                    DistinctSet::Timestamp(m) => m.change_owned(value, change),
                    _ => set.change_scalar(&timestamp_scalar(value), change),
                }
            }
            Self::String(a) => !a.is_null(row) && set.change_string(a.value(row), retract, count),
            Self::Scalar(a) => {
                !a.is_null(row)
                    && set.change_value(
                        ScalarValue::try_from_array(a, row).expect("distinct value"),
                        change,
                    )
            }
        }
    }

    fn num(&self, row: usize) -> Num {
        match self {
            Self::I64(a) => Num::I64(a.value(row)),
            Self::I32(a) => Num::I32(a.value(row)),
            Self::I16(a) => Num::I16(a.value(row)),
            Self::I8(a) => Num::I8(a.value(row)),
            Self::Decimal(a) => Num::I128(a.value(row)),
            Self::Scalar(a) => {
                distinct_num(&ScalarValue::try_from_array(a, row).expect("distinct numeric value"))
            }
            _ => unreachable!("SUM/AVG require numeric input"),
        }
    }

    pub(super) fn update(&self, state: &mut GroupAggState, row: usize, retract: bool, count: i64) {
        let (set, live) = match state {
            GroupAggState::Distinct { set, live } => (set, live),
            GroupAggState::DistinctRunning { counts, live, .. } => (counts, live),
            _ => unreachable!("distinct state"),
        };
        if self.change(set, row, retract, count) {
            *live += if retract { -1 } else { 1 };
            if let GroupAggState::DistinctRunning { agg, .. } = state {
                if retract {
                    agg.retract(self.num(row));
                } else {
                    agg.fold(self.num(row));
                }
            }
        }
    }
}

// Build local membership columns directly instead of allocating a scalar and a temporary
// vector for every map. Ordered decimals append in the host's iteration order below.
pub(super) enum DistinctValues {
    I64(arrow::array::Int64Builder),
    I32(arrow::array::Int32Builder),
    I16(arrow::array::Int16Builder),
    I8(arrow::array::Int8Builder),
    Decimal(arrow::array::Decimal128Builder),
    Boolean(arrow::array::BooleanBuilder),
    Timestamp(streamfusion_bridge::timestamp::TimestampBuilder),
    String(arrow::array::StringBuilder),
    Scalar(Vec<ScalarValue>, DataType),
}

impl DistinctValues {
    pub(super) fn new(ty: &DataType) -> Self {
        match ty {
            DataType::Int64 => Self::I64(Default::default()),
            DataType::Int32 => Self::I32(Default::default()),
            DataType::Int16 => Self::I16(Default::default()),
            DataType::Int8 => Self::I8(Default::default()),
            DataType::Decimal128(_, _) => {
                Self::Decimal(arrow::array::Decimal128Builder::new().with_data_type(ty.clone()))
            }
            DataType::Boolean => Self::Boolean(Default::default()),
            DataType::Utf8 => Self::String(Default::default()),
            ty if streamfusion_bridge::timestamp::is_component_timestamp(ty) => {
                Self::Timestamp(streamfusion_bridge::timestamp::TimestampBuilder::with_capacity(0))
            }
            _ => Self::Scalar(Vec::new(), ty.clone()),
        }
    }

    pub(super) fn push(&mut self, scalar: ScalarValue) {
        match (self, scalar) {
            (Self::I64(b), ScalarValue::Int64(v)) => b.append_option(v),
            (Self::I32(b), ScalarValue::Int32(v)) => b.append_option(v),
            (Self::I16(b), ScalarValue::Int16(v)) => b.append_option(v),
            (Self::I8(b), ScalarValue::Int8(v)) => b.append_option(v),
            (Self::Decimal(b), ScalarValue::Decimal128(v, _, _)) => b.append_option(v),
            (Self::Boolean(b), ScalarValue::Boolean(v)) => b.append_option(v),
            (Self::String(b), ScalarValue::Utf8(v)) => b.append_option(v),
            (Self::Timestamp(b), ScalarValue::Struct(a)) => {
                let column = streamfusion_bridge::timestamp::TimestampColumn::try_new(a.as_ref())
                    .expect("timestamp view");
                if column.is_null(0) {
                    b.append_null();
                } else {
                    b.append_value(column.value(0).expect("timestamp value"));
                }
            }
            (Self::Scalar(values, _), scalar) => values.push(scalar),
            (builder, scalar) => {
                // A compact planner type may have promoted to a generic runtime key.
                // Retain the scalar reconstruction/cast used by the general view path.
                let previous =
                    std::mem::replace(builder, Self::Scalar(Vec::new(), DataType::Null)).finish();
                let mut values: Vec<_> = (0..previous.len())
                    .map(|row| {
                        ScalarValue::try_from_array(&previous, row).expect("distinct view value")
                    })
                    .collect();
                values.push(scalar);
                *builder = Self::Scalar(values, previous.data_type().clone());
            }
        }
    }

    pub(super) fn append(&mut self, set: &DistinctSet, counts: &mut Vec<i64>) {
        macro_rules! append {
            ($builder:expr, $map:expr) => {
                for (value, count) in &$map.counts {
                    $builder.append_value(*value);
                    counts.push(*count);
                }
            };
        }
        match (&mut *self, set) {
            (Self::I64(b), DistinctSet::I64(m)) => append!(b, m),
            (Self::I32(b), DistinctSet::I32(m)) => append!(b, m),
            (Self::I16(b), DistinctSet::I16(m)) => append!(b, m),
            (Self::I8(b), DistinctSet::I8(m)) => append!(b, m),
            (Self::Decimal(b), DistinctSet::Decimal(m, _, _)) => append!(b, m),
            (Self::Boolean(b), DistinctSet::Boolean(m)) => append!(b, m),
            (Self::String(b), DistinctSet::String(m)) => {
                for (value, count) in &m.counts {
                    b.append_value(value);
                    counts.push(*count);
                }
            }
            (Self::Timestamp(b), _) => set.append_timestamps(b, counts),
            _ => {
                for (value, count) in set.scalar_entries() {
                    self.push(value);
                    counts.push(count);
                }
            }
        }
    }

    pub(super) fn finish(self) -> ArrayRef {
        match self {
            Self::I64(mut b) => Arc::new(b.finish()),
            Self::I32(mut b) => Arc::new(b.finish()),
            Self::I16(mut b) => Arc::new(b.finish()),
            Self::I8(mut b) => Arc::new(b.finish()),
            Self::Decimal(mut b) => Arc::new(b.finish()),
            Self::Boolean(mut b) => Arc::new(b.finish()),
            Self::String(mut b) => Arc::new(b.finish()),
            Self::Timestamp(mut b) => Arc::new(b.finish()),
            Self::Scalar(values, ty) => scalars_to_array(values, &ty),
        }
    }
}

#[derive(Clone, Copy)]
enum Change {
    Add(i64),
    Remove,
    Restore(i64),
    Import(i64),
}

pub(crate) struct Multiplicities<K> {
    counts: ahash::HashMap<K, i64>,
    journal: Option<Box<ahash::HashSet<K>>>,
}

impl<K: Eq + Hash + Clone> Multiplicities<K> {
    fn new() -> Self {
        Self {
            counts: Default::default(),
            journal: None,
        }
    }

    fn capacity_bytes(&self) -> usize {
        let capacity = self.counts.capacity();
        if capacity == 0 {
            return 0;
        }
        // Include spare buckets/control bytes, including generic scalar keys wider than
        // the ordinary per-entry estimate. Cleared keys own no additional payload.
        capacity * MULTISET_ENTRY_BYTES.max(2 * (std::mem::size_of::<(K, i64)>() + 1)) + 16
    }

    fn change_owned(&mut self, key: K, change: Change) -> bool {
        if let Change::Add(n) = change {
            super::note(&mut self.journal, &key);
            let count = self.counts.entry(key).or_insert(0);
            *count += n;
            return *count == n;
        }
        if let Change::Restore(n) | Change::Import(n) = change {
            if matches!(change, Change::Import(_)) {
                super::note(&mut self.journal, &key);
            }
            self.counts.insert(key, n);
            return false;
        }
        self.change(&key, change)
    }

    fn change<Q>(&mut self, key: &Q, change: Change) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ToOwned<Owned = K> + ?Sized,
    {
        if !matches!(change, Change::Restore(_)) {
            if let Some(journal) = &mut self.journal {
                if !journal.contains(key) {
                    journal.insert(key.to_owned());
                }
            }
        }
        match change {
            Change::Add(n) => {
                if let Some(count) = self.counts.get_mut(key) {
                    *count += n;
                    *count == n
                } else {
                    self.counts.insert(key.to_owned(), n);
                    true
                }
            }
            Change::Remove => {
                if let Some(count) = self.counts.get_mut(key) {
                    *count -= 1;
                    if *count <= 0 {
                        self.counts.remove(key);
                        return true;
                    }
                }
                false
            }
            Change::Restore(n) | Change::Import(n) => {
                self.counts.insert(key.to_owned(), n);
                false
            }
        }
    }

    fn entries(&self, scalar: impl Fn(&K) -> ScalarValue) -> Vec<(ScalarValue, i64)> {
        self.counts.iter().map(|(k, v)| (scalar(k), *v)).collect()
    }

    #[cfg(feature = "rocksdb-state")]
    fn drain(&mut self, scalar: impl Fn(&K) -> ScalarValue) -> Vec<(ScalarValue, Option<i64>)> {
        self.journal.as_mut().map_or_else(Vec::new, |journal| {
            journal
                .drain()
                .map(|k| (scalar(&k), self.counts.get(&k).copied()))
                .collect()
        })
    }
}

// Fixed-schema keys keep value hashing independent of ScalarValue's type tag. Persistence still
// writes the original typed scalar entries; decimal precision and scale belong to the map.
pub(crate) enum DistinctSet {
    I64(Multiplicities<i64>),
    I32(Multiplicities<i32>),
    I16(Multiplicities<i16>),
    I8(Multiplicities<i8>),
    Decimal(Multiplicities<i128>, u8, i8),
    String(Multiplicities<String>),
    Boolean(Multiplicities<bool>),
    Timestamp(Multiplicities<i128>),
    Scalar(Multiplicities<ScalarValue>),
}

macro_rules! each_map {
    ($self:expr, $map:ident, $body:expr) => {
        match $self {
            DistinctSet::I64($map) => $body,
            DistinctSet::I32($map) => $body,
            DistinctSet::I16($map) => $body,
            DistinctSet::I8($map) => $body,
            DistinctSet::Decimal($map, _, _) => $body,
            DistinctSet::String($map) => $body,
            DistinctSet::Boolean($map) => $body,
            DistinctSet::Timestamp($map) => $body,
            DistinctSet::Scalar($map) => $body,
        }
    };
}

impl DistinctSet {
    pub(crate) fn new(value_type: &DataType) -> Self {
        match value_type {
            DataType::Int64 => Self::I64(Multiplicities::new()),
            DataType::Int32 => Self::I32(Multiplicities::new()),
            DataType::Int16 => Self::I16(Multiplicities::new()),
            DataType::Int8 => Self::I8(Multiplicities::new()),
            DataType::Decimal128(p, s) => Self::Decimal(Multiplicities::new(), *p, *s),
            DataType::Utf8 => Self::String(Multiplicities::new()),
            DataType::Boolean => Self::Boolean(Multiplicities::new()),
            ty if streamfusion_bridge::timestamp::is_component_timestamp(ty) => {
                Self::Timestamp(Multiplicities::new())
            }
            _ => Self::Scalar(Multiplicities::new()),
        }
    }
    pub(super) fn len(&self) -> usize {
        each_map!(self, m, m.counts.len())
    }
    pub(super) fn capacity_bytes(&self) -> usize {
        each_map!(self, m, m.capacity_bytes())
    }

    pub(super) fn clear(&mut self) {
        each_map!(self, m, {
            m.counts.clear();
            m.journal = None;
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(super) fn journaled(&self) -> bool {
        each_map!(self, m, m.journal.is_some())
    }
    #[cfg(feature = "rocksdb-state")]
    pub(super) fn arm_journal(&mut self) {
        each_map!(self, m, m.journal = Some(Box::default()))
    }

    pub(crate) fn scalar_entries(&self) -> Vec<(ScalarValue, i64)> {
        match self {
            Self::I64(m) => m.entries(|v| ScalarValue::Int64(Some(*v))),
            Self::I32(m) => m.entries(|v| ScalarValue::Int32(Some(*v))),
            Self::I16(m) => m.entries(|v| ScalarValue::Int16(Some(*v))),
            Self::I8(m) => m.entries(|v| ScalarValue::Int8(Some(*v))),
            Self::Decimal(m, p, s) => m.entries(|v| ScalarValue::Decimal128(Some(*v), *p, *s)),
            Self::String(m) => m.entries(|v| ScalarValue::Utf8(Some(v.clone()))),
            Self::Boolean(m) => m.entries(|v| ScalarValue::Boolean(Some(*v))),
            Self::Timestamp(m) => m.entries(|v| timestamp_scalar(*v)),
            Self::Scalar(m) => m.entries(Clone::clone),
        }
    }
    #[cfg(feature = "rocksdb-state")]
    pub(super) fn drain_journal(&mut self) -> Vec<(ScalarValue, Option<i64>)> {
        match self {
            Self::I64(m) => m.drain(|v| ScalarValue::Int64(Some(*v))),
            Self::I32(m) => m.drain(|v| ScalarValue::Int32(Some(*v))),
            Self::I16(m) => m.drain(|v| ScalarValue::Int16(Some(*v))),
            Self::I8(m) => m.drain(|v| ScalarValue::Int8(Some(*v))),
            Self::Decimal(m, p, s) => m.drain(|v| ScalarValue::Decimal128(Some(*v), *p, *s)),
            Self::String(m) => m.drain(|v| ScalarValue::Utf8(Some(v.clone()))),
            Self::Boolean(m) => m.drain(|v| ScalarValue::Boolean(Some(*v))),
            Self::Timestamp(m) => m.drain(|v| timestamp_scalar(*v)),
            Self::Scalar(m) => m.drain(Clone::clone),
        }
    }

    pub(super) fn append_timestamps(
        &self,
        builder: &mut streamfusion_bridge::timestamp::TimestampBuilder,
        counts: &mut Vec<i64>,
    ) {
        if let Self::Timestamp(map) = self {
            for (&nanos, &count) in &map.counts {
                builder.append_value(
                    streamfusion_bridge::timestamp::TimestampValue::from_nanos(nanos)
                        .expect("timestamp range"),
                );
                counts.push(count);
            }
        } else {
            for (scalar, count) in self.scalar_entries() {
                let array = scalar.to_array().expect("timestamp scalar");
                let column =
                    streamfusion_bridge::timestamp::TimestampColumn::try_new(array.as_ref())
                        .expect("timestamp view");
                builder.append_value(column.value(0).expect("timestamp value"));
                counts.push(count);
            }
        }
    }

    pub(super) fn decimal_count(&self, value: &ScalarValue) -> i64 {
        match (self, value) {
            (Self::Decimal(m, p, s), ScalarValue::Decimal128(Some(v), vp, vs))
                if p == vp && s == vs =>
            {
                m.counts.get(v).copied().unwrap_or(0)
            }
            (Self::Scalar(m), value) => m.counts.get(value).copied().unwrap_or(0),
            _ => 0,
        }
    }

    fn promote(&mut self) {
        let entries = self.scalar_entries();
        #[cfg(feature = "rocksdb-state")]
        let journal = if self.journaled() {
            Some(Box::new(
                self.drain_journal().into_iter().map(|(v, _)| v).collect(),
            ))
        } else {
            None
        };
        #[cfg(not(feature = "rocksdb-state"))]
        let journal = None;
        *self = Self::Scalar(Multiplicities {
            counts: entries.into_iter().collect(),
            journal,
        });
    }

    fn change_scalar(&mut self, value: &ScalarValue, change: Change) -> bool {
        match (self, value) {
            (Self::I64(m), ScalarValue::Int64(Some(v))) => m.change_owned(*v, change),
            (Self::I32(m), ScalarValue::Int32(Some(v))) => m.change_owned(*v, change),
            (Self::I16(m), ScalarValue::Int16(Some(v))) => m.change_owned(*v, change),
            (Self::I8(m), ScalarValue::Int8(Some(v))) => m.change_owned(*v, change),
            (Self::Decimal(m, p, s), ScalarValue::Decimal128(Some(v), vp, vs))
                if *p == *vp && *s == *vs =>
            {
                m.change_owned(*v, change)
            }
            (Self::String(m), ScalarValue::Utf8(Some(v))) => m.change(v.as_str(), change),
            (Self::Boolean(m), ScalarValue::Boolean(Some(v))) => m.change_owned(*v, change),
            (Self::Timestamp(m), ScalarValue::Struct(array))
                if streamfusion_bridge::timestamp::is_component_timestamp(array.data_type())
                    && !array.is_null(0) =>
            {
                let column =
                    streamfusion_bridge::timestamp::TimestampColumn::try_new(array.as_ref())
                        .expect("timestamp key");
                m.change_owned(column.value(0).expect("timestamp value").nanos(), change)
            }
            (Self::Scalar(m), v) => {
                let canonical = crate::flink_float::canonical_scalar(v);
                m.change(canonical.as_ref().unwrap_or(v), change)
            }
            (set, v) => {
                set.promote();
                set.change_scalar(v, change)
            }
        }
    }
    fn change_value(&mut self, value: ScalarValue, change: Change) -> bool {
        if let Self::Scalar(map) = self {
            let value = crate::flink_float::canonical_scalar(&value).unwrap_or(value);
            map.change_owned(value, change)
        } else {
            self.change_scalar(&value, change)
        }
    }

    pub(crate) fn add_scalar(&mut self, v: ScalarValue) -> bool {
        self.change_value(v, Change::Add(1))
    }
    pub(super) fn add_scalar_n(&mut self, v: ScalarValue, n: i64) -> bool {
        self.change_value(v, Change::Add(n))
    }
    pub(super) fn remove_scalar(&mut self, v: &ScalarValue) -> bool {
        self.change_scalar(v, Change::Remove)
    }
    pub(super) fn insert_restored(&mut self, v: ScalarValue, n: i64) {
        self.change_value(v, Change::Restore(n));
    }
    pub(super) fn insert_imported(&mut self, v: ScalarValue, n: i64) {
        self.change_value(v, Change::Import(n));
    }
    pub(super) fn change_string(&mut self, v: &str, retract: bool, n: i64) -> bool {
        let change = if retract {
            Change::Remove
        } else {
            Change::Add(n)
        };
        match self {
            Self::String(m) => m.change(v, change),
            _ => self.change_scalar(&ScalarValue::Utf8(Some(v.to_owned())), change),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples() -> Vec<ScalarValue> {
        vec![
            ScalarValue::Int64(Some(i64::MIN)),
            ScalarValue::Boolean(Some(false)),
            timestamp_scalar(i128::from(i64::MAX) * 1_000_000 + 999_999),
            ScalarValue::Int32(Some(i32::MIN)),
            ScalarValue::Int16(Some(i16::MIN)),
            ScalarValue::Int8(Some(i8::MIN)),
            ScalarValue::Decimal128(Some(-12345678901234567890), 20, 2),
            ScalarValue::Utf8(Some("a\0中😀".repeat(64))),
        ]
    }

    #[test]
    fn promoted_keys_keep_generic_view_casts() {
        let value = ScalarValue::LargeUtf8(Some("wide string".into()));
        let mut set = DistinctSet::new(&DataType::Utf8);
        set.add_scalar(value.clone());
        let mut view = DistinctValues::new(&DataType::Utf8);
        let mut counts = Vec::new();
        view.append(&set, &mut counts);
        assert_eq!(
            view.finish().to_data(),
            scalars_to_array(vec![value], &DataType::Utf8).to_data()
        );
        assert_eq!(counts, vec![1]);
    }

    #[test]
    fn component_timestamps_preserve_range_nanos_and_nulls_without_scalar_keys() {
        use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
        let values = [
            TimestampValue::new(i64::MIN, 0).unwrap(),
            TimestampValue::new(-1, 999_999).unwrap(),
            TimestampValue::new(0, 0).unwrap(),
            TimestampValue::new(0, 1).unwrap(),
            TimestampValue::new(i64::MAX, 999_999).unwrap(),
        ];
        let array: ArrayRef = Arc::new(timestamp_array(
            values.iter().copied().map(Some).chain([None]),
        ));
        let column = DistinctColumn::new(&array);
        let mut set = DistinctSet::new(array.data_type());
        for row in 0..values.len() {
            assert!(column.change(&mut set, row, false, 1));
            assert!(!column.change(&mut set, row, false, 2));
        }
        assert!(!column.change(&mut set, values.len(), false, 1));
        assert!(matches!(set, DistinctSet::Timestamp(_)));
        let mut restored = DistinctSet::new(array.data_type());
        for (value, count) in set.scalar_entries() {
            restored.insert_restored(value, count);
        }
        let mut builder = streamfusion_bridge::timestamp::TimestampBuilder::with_capacity(0);
        let mut counts = Vec::new();
        restored.append_timestamps(&mut builder, &mut counts);
        let output: ArrayRef = Arc::new(builder.finish());
        let output =
            streamfusion_bridge::timestamp::TimestampColumn::try_new(output.as_ref()).unwrap();
        let mut actual: Vec<_> = (0..counts.len())
            .map(|row| output.value(row).unwrap())
            .collect();
        actual.sort();
        assert_eq!(actual, values);
        assert_eq!(counts, vec![3; values.len()]);
        for row in 0..values.len() {
            assert!(!column.change(&mut restored, row, true, 1));
            assert!(!column.change(&mut restored, row, true, 1));
            assert!(column.change(&mut restored, row, true, 1));
        }
        assert!(restored.is_empty());
    }

    #[test]
    fn typed_maps_preserve_scalar_wire_entries_and_multiplicity() {
        for value in samples() {
            let mut set = DistinctSet::new(&value.data_type());
            assert!(set.add_scalar(value.clone()));
            assert!(!set.add_scalar_n(value.clone(), 2));
            assert_eq!(set.scalar_entries(), vec![(value.clone(), 3)]);
            let mut restored = DistinctSet::new(&value.data_type());
            restored.insert_restored(value.clone(), 3);
            for expected in [false, false, true, false] {
                assert_eq!(restored.remove_scalar(&value), expected);
            }
            assert!(restored.is_empty());
            assert!(restored.add_scalar(value.clone()));
            assert_eq!(restored.scalar_entries(), vec![(value, 1)]);
        }
    }

    #[cfg(feature = "rocksdb-state")]
    #[test]
    fn journals_preserve_restore_import_delete_and_promotion() {
        for value in samples() {
            let mut set = DistinctSet::new(&value.data_type());
            set.arm_journal();
            set.insert_restored(value.clone(), 2);
            assert!(set.drain_journal().is_empty());
            assert!(!set.remove_scalar(&value));
            assert!(set.remove_scalar(&value));
            assert_eq!(set.drain_journal(), vec![(value.clone(), None)]);
            set.insert_imported(value.clone(), 4);
            assert_eq!(set.drain_journal(), vec![(value.clone(), Some(4))]);
            set.remove_scalar(&value);
            let other = ScalarValue::Boolean(Some(true));
            set.add_scalar(other.clone());
            let journal = set.drain_journal();
            assert_eq!(journal.len(), 2);
            assert!(journal.contains(&(value, Some(3))));
            assert!(journal.contains(&(other, Some(1))));
        }
    }

    #[test]
    fn borrowed_strings_keep_exact_bytes_and_last_occurrence() {
        let mut set = DistinctSet::new(&DataType::Utf8);
        for value in ["", "é", "e\u{301}", "a\0b", "A", "a"] {
            assert!(set.change_string(value, false, 1));
            assert!(!set.change_string(value, false, 2));
            assert!(!set.change_string(value, true, 1));
            assert!(!set.change_string(value, true, 1));
            assert!(set.change_string(value, true, 1));
        }
        assert!(set.is_empty());
    }
}
