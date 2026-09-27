use super::*;
use std::borrow::Borrow;
use std::hash::Hash;

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
            _ => Self::Scalar(Multiplicities::new()),
        }
    }
    pub(super) fn len(&self) -> usize {
        each_map!(self, m, m.counts.len())
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
            Self::Scalar(m) => m.drain(Clone::clone),
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
    pub(super) fn add_i64(&mut self, v: i64) -> bool {
        self.add_i64_n(v, 1)
    }
    pub(super) fn add_i64_n(&mut self, v: i64, n: i64) -> bool {
        match self {
            Self::I64(m) => m.change_owned(v, Change::Add(n)),
            _ => self.change_scalar(&ScalarValue::Int64(Some(v)), Change::Add(n)),
        }
    }
    pub(super) fn remove_i64(&mut self, v: i64) -> bool {
        match self {
            Self::I64(m) => m.change(&v, Change::Remove),
            _ => self.change_scalar(&ScalarValue::Int64(Some(v)), Change::Remove),
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
            ScalarValue::Int32(Some(i32::MIN)),
            ScalarValue::Int16(Some(i16::MIN)),
            ScalarValue::Int8(Some(i8::MIN)),
            ScalarValue::Decimal128(Some(-12345678901234567890), 20, 2),
            ScalarValue::Utf8(Some("a\0中😀".repeat(64))),
        ]
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
