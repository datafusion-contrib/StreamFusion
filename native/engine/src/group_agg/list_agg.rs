use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub(super) fn is_list_agg(kind: i64) -> bool {
    matches!(kind, 24 | 25 | 32 | 33 | 46 | 47)
}

pub(super) fn is_merge(kind: i64) -> bool {
    matches!(kind, 46 | 47)
}

pub(super) fn partials<'a>(states: impl Iterator<Item = &'a ListAggState>) -> ArrayRef {
    let mut values = arrow::array::ListBuilder::new(arrow::array::StringBuilder::new());
    let mut retractions = arrow::array::ListBuilder::new(arrow::array::StringBuilder::new());
    let mut separators = arrow::array::StringBuilder::new();
    for state in states {
        for value in &state.values {
            values.values().append_value(value);
        }
        values.append(true);
        for value in &state.pending_retractions {
            retractions.values().append_value(value);
        }
        retractions.append(true);
        separators.append_value(state.separator.as_deref().unwrap_or(","));
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(values.finish()),
        Arc::new(retractions.finish()),
        Arc::new(separators.finish()),
    ];
    let fields: arrow::datatypes::Fields = ["values", "retractions", "separator"]
        .into_iter()
        .zip(&columns)
        .map(|(name, values)| Field::new(name, values.data_type().clone(), false))
        .collect();
    Arc::new(arrow::array::StructArray::new(fields, columns, None))
}

pub(super) fn partial_type() -> DataType {
    partials(std::iter::empty()).data_type().clone()
}

#[derive(Default, Serialize, Deserialize)]
struct ListViewTimestamps {
    values: VecDeque<i64>,
    pending_retractions: VecDeque<i64>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ListAggState {
    append_only: bool,
    prefix: Option<String>,
    separator: Option<String>,
    values: VecDeque<String>,
    pending_retractions: VecDeque<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    distinct: Option<Box<ahash::HashMap<String, i64>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timestamps: Option<Box<ListViewTimestamps>>,
    #[serde(skip)]
    distinct_bytes: usize,
    #[serde(skip)]
    element_bytes: usize,
}

impl ListAggState {
    pub(super) fn new(kind: i64) -> Self {
        Self {
            append_only: matches!(kind, 24 | 32),
            prefix: None,
            separator: None,
            values: VecDeque::new(),
            pending_retractions: VecDeque::new(),
            distinct: matches!(kind, 32 | 33).then(Box::default),
            timestamps: None,
            distinct_bytes: 0,
            element_bytes: 0,
        }
    }

    pub(super) fn from_partial(partial: &arrow::array::StructArray, row: usize) -> Self {
        let mut state = Self::new(25);
        for (index, target) in [(0, &mut state.values), (1, &mut state.pending_retractions)] {
            let list = partial
                .column(index)
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .expect("LISTAGG partial list");
            let strings = StringColumn::new(list.values());
            for index in list.value_offsets()[row] as usize..list.value_offsets()[row + 1] as usize
            {
                let value = strings
                    .at(index)
                    .expect("non-null LISTAGG element")
                    .to_owned();
                state.element_bytes += value.capacity();
                target.push_back(value);
            }
        }
        state.separator = StringColumn::new(partial.column(2))
            .at(row)
            .map(str::to_owned);
        state
    }

    pub(super) fn merge(&mut self, mut other: Self, skip_empty: bool, ttl: StateTtl) {
        if skip_empty && other.values.is_empty() && other.pending_retractions.is_empty() {
            return;
        }
        self.separator = other.separator.take();
        self.element_bytes += other.element_bytes;
        self.values.append(&mut other.values);
        self.pending_retractions
            .append(&mut other.pending_retractions);
        for pending in std::mem::take(&mut self.pending_retractions) {
            if let Some(index) = self.values.iter().position(|value| value == &pending) {
                self.element_bytes -=
                    self.values.remove(index).unwrap().capacity() + pending.capacity();
            } else {
                self.pending_retractions.push_back(pending);
            }
        }
        if ttl.enabled() {
            let times = self.timestamps.get_or_insert_with(Default::default);
            times.values.clear();
            times.values.resize(self.values.len(), ttl.now());
            times.pending_retractions.clear();
            times
                .pending_retractions
                .resize(self.pending_retractions.len(), ttl.now());
        } else {
            self.timestamps = None;
        }
    }

    pub(super) fn expire(&mut self, ttl: StateTtl, previous_write: i64) -> bool {
        if !ttl.enabled() {
            self.timestamps = None;
            return false;
        }
        if self.append_only {
            return false;
        }
        let times = self.timestamps.get_or_insert_with(|| {
            Box::new(ListViewTimestamps {
                values: VecDeque::from(vec![previous_write; self.values.len()]),
                pending_retractions: VecDeque::from(vec![
                    previous_write;
                    self.pending_retractions.len()
                ]),
            })
        });
        let mut changed = false;
        for (values, timestamps) in [
            (&mut self.values, &mut times.values),
            (
                &mut self.pending_retractions,
                &mut times.pending_retractions,
            ),
        ] {
            let mut old_times = std::mem::take(timestamps).into_iter();
            values.retain(|value| {
                let timestamp = old_times.next().expect("LISTAGG entry timestamp");
                if ttl.expired(timestamp) {
                    self.element_bytes -= value.capacity();
                    changed = true;
                    false
                } else {
                    timestamps.push_back(timestamp);
                    true
                }
            });
        }
        changed
    }

    pub(super) fn update_with_ttl(
        &mut self,
        value: Option<&str>,
        separator: &str,
        retract: bool,
        ttl: StateTtl,
    ) -> bool {
        let old_len = self.values.len();
        let old_pending = self.pending_retractions.len();
        let changed = self.update(value, separator, retract);
        self.update_timestamps(old_len, old_pending, ttl);
        changed
    }

    fn update_timestamps(&mut self, old_len: usize, old_pending: usize, ttl: StateTtl) {
        let Some(times) = &mut self.timestamps else {
            return;
        };
        if self.values.len() > old_len {
            times.values.push_back(ttl.now());
        } else if self.values.len() < old_len {
            // StateListView.remove rewrites the entire surviving list after a successful removal.
            times.values.clear();
            times.values.resize(self.values.len(), ttl.now());
        }
        if self.pending_retractions.len() > old_pending {
            times.pending_retractions.push_back(ttl.now());
        }
    }

    pub(super) fn merge_distinct(
        &mut self,
        value: Option<&str>,
        separator: &str,
        delta: i64,
        ttl: StateTtl,
    ) -> bool {
        let Some(value) = value else { return false };
        let counts = self.distinct.as_mut().expect("LISTAGG DISTINCT membership");
        let old = counts.get(value).copied().unwrap_or(0);
        let next = if self.append_only {
            i64::from(old != 0 || delta != 0)
        } else {
            old.wrapping_add(delta)
        };
        if next == 0 {
            if let Some((key, _)) = counts.remove_entry(value) {
                self.distinct_bytes -= key.capacity();
            }
        } else if let Some(count) = counts.get_mut(value) {
            *count = next;
        } else {
            let key = value.to_owned();
            self.distinct_bytes += key.capacity();
            counts.insert(key, next);
        }
        if (old > 0) == (next > 0) {
            return false;
        }
        let old_len = self.values.len();
        let old_pending = self.pending_retractions.len();
        self.update_value(value, separator, next <= 0);
        self.update_timestamps(old_len, old_pending, ttl);
        true
    }

    pub(super) fn update(&mut self, value: Option<&str>, separator: &str, retract: bool) -> bool {
        let Some(value) = value else { return false };
        if let Some(counts) = &mut self.distinct {
            assert!(
                !self.append_only || !retract,
                "insert-only LISTAGG cannot retract"
            );
            let next = if let Some(count) = counts.get_mut(value) {
                if self.append_only {
                    return false;
                }
                *count = count.wrapping_add(if retract { -1 } else { 1 });
                *count
            } else {
                let key = value.to_owned();
                self.distinct_bytes += key.capacity();
                let next = if retract { -1 } else { 1 };
                counts.insert(key, next);
                next
            };
            if next == 0 {
                let (key, _) = counts.remove_entry(value).expect("existing LISTAGG member");
                self.distinct_bytes -= key.capacity();
            }
            if (retract && next != 0) || (!retract && next != 1) {
                return false;
            }
        }
        self.update_value(value, separator, retract);
        true
    }

    fn update_value(&mut self, value: &str, separator: &str, retract: bool) {
        if self.separator.as_deref() != Some(separator) {
            self.separator = Some(separator.to_owned());
        }
        if self.append_only {
            assert!(!retract, "insert-only LISTAGG cannot retract");
            match &mut self.prefix {
                Some(prefix) => {
                    prefix.push_str(separator);
                    prefix.push_str(value);
                }
                None => self.prefix = Some(value.to_owned()),
            }
        } else if retract {
            if let Some(index) = self.values.iter().position(|old| old == value) {
                self.element_bytes -= self.values.remove(index).unwrap().capacity();
            } else {
                let value = value.to_owned();
                self.element_bytes += value.capacity();
                self.pending_retractions.push_back(value);
            }
        } else {
            let value = value.to_owned();
            self.element_bytes += value.capacity();
            self.values.push_back(value);
        }
    }

    pub(super) fn forget_distinct(&mut self, value: &ScalarValue) {
        if let ScalarValue::Utf8(Some(value)) = value {
            if let Some((key, _)) = self.distinct.as_mut().unwrap().remove_entry(value.as_str()) {
                self.distinct_bytes -= key.capacity();
            }
        }
    }

    pub(super) fn distinct_entries(&self) -> Vec<(ScalarValue, i64)> {
        self.distinct
            .as_ref()
            .unwrap()
            .iter()
            .map(|(value, count)| (ScalarValue::Utf8(Some(value.clone())), *count))
            .collect()
    }

    pub(super) fn distinct_count(&self, value: &ScalarValue) -> i64 {
        match value {
            ScalarValue::Utf8(Some(value)) => self
                .distinct
                .as_ref()
                .unwrap()
                .get(value.as_str())
                .copied()
                .unwrap_or(0),
            _ => 0,
        }
    }

    pub(super) fn update_local(&mut self, value: Option<&str>, separator: &str) {
        self.update_local_retract(value, separator, false);
    }

    pub(super) fn update_local_retract(
        &mut self,
        value: Option<&str>,
        separator: &str,
        retract: bool,
    ) {
        if self.separator.as_deref() != Some(separator) {
            self.separator = Some(separator.to_owned());
        }
        self.update(value, separator, retract);
    }

    pub(super) fn separator(&self) -> ScalarValue {
        ScalarValue::Utf8(Some(self.separator.as_deref().unwrap_or(",").to_owned()))
    }

    pub(super) fn emit(&self) -> ScalarValue {
        let value = if self.append_only {
            self.prefix.clone()
        } else if self.values.is_empty() {
            None
        } else {
            let separator = self.separator.as_deref().unwrap_or(",");
            let mut result = String::new();
            for (index, value) in self.values.iter().enumerate() {
                if index != 0 {
                    result.push_str(separator);
                }
                result.push_str(value);
            }
            Some(result)
        };
        ScalarValue::Utf8(value)
    }

    pub(super) fn snapshot(&self) -> ScalarValue {
        ScalarValue::Binary(Some(
            serde_json::to_vec(&(1_u8, self)).expect("serialize LISTAGG state"),
        ))
    }

    pub(super) fn restore(&mut self, value: ScalarValue) -> Result<(), DataFusionError> {
        let ScalarValue::Binary(Some(bytes)) = value else {
            return Err(DataFusionError::Execution("missing LISTAGG state".into()));
        };
        let (version, mut restored): (u8, Self) =
            serde_json::from_slice(&bytes).map_err(|error| {
                DataFusionError::Execution(format!("invalid LISTAGG state: {error}"))
            })?;
        if version != 1
            || restored.append_only != self.append_only
            || restored.distinct.is_some() != self.distinct.is_some()
        {
            return Err(DataFusionError::Execution(
                "incompatible LISTAGG state".into(),
            ));
        }
        restored.element_bytes = restored
            .values
            .iter()
            .chain(&restored.pending_retractions)
            .map(String::capacity)
            .sum();
        restored.distinct_bytes = restored
            .distinct
            .as_ref()
            .map_or(0, |counts| counts.keys().map(String::capacity).sum());
        *self = restored;
        Ok(())
    }

    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.timestamps.as_ref().map_or(0, |times| {
                std::mem::size_of::<ListViewTimestamps>()
                    + (times.values.capacity() + times.pending_retractions.capacity())
                        * std::mem::size_of::<i64>()
            })
            + self.distinct_bytes
            + self.distinct.as_ref().map_or(0, |counts| {
                std::mem::size_of_val(counts.as_ref())
                    + counts.capacity() * (std::mem::size_of::<(String, i64)>() + 1)
            })
            + self.element_bytes
            + self.prefix.as_ref().map_or(0, String::capacity)
            + self.separator.as_ref().map_or(0, String::capacity)
            + (self.values.capacity() + self.pending_retractions.capacity())
                * std::mem::size_of::<String>()
    }
}

pub(super) enum StringColumn<'a> {
    Utf8(&'a StringArray),
    Large(&'a arrow::array::LargeStringArray),
    View(&'a arrow::array::StringViewArray),
}

impl<'a> StringColumn<'a> {
    pub(super) fn new(array: &'a ArrayRef) -> Self {
        match array.data_type() {
            DataType::Utf8 => Self::Utf8(array.as_any().downcast_ref().unwrap()),
            DataType::LargeUtf8 => Self::Large(array.as_any().downcast_ref().unwrap()),
            DataType::Utf8View => Self::View(array.as_any().downcast_ref().unwrap()),
            other => panic!("LISTAGG requires a string column, got {other}"),
        }
    }
    pub(super) fn at(&self, row: usize) -> Option<&str> {
        match self {
            Self::Utf8(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Large(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::View(array) => (!array.is_null(row)).then(|| array.value(row)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_partial_counts_replay_only_positive_membership_crossings() {
        let mut state = ListAggState::new(33);
        let ttl = StateTtl::new(1000, 1000);
        state.expire(ttl, 0);
        assert!(!state.merge_distinct(Some("a"), "::", -3, ttl));
        assert!(!state.merge_distinct(Some("a"), "::", 2, ttl));
        assert!(state.merge_distinct(Some("a"), "::", 5, ttl));
        assert!(state.merge_distinct(Some("b"), "::", 2, ttl));
        assert!(!state.merge_distinct(Some("a"), "::", -2, ttl));
        assert_eq!(state.emit(), ScalarValue::Utf8(Some("a::b".into())));
        assert!(state.merge_distinct(Some("a"), "::", -4, StateTtl::new(1000, 1500)));
        assert_eq!(state.emit(), ScalarValue::Utf8(Some("b".into())));
        assert!(!state.expire(StateTtl::new(1000, 2000), 1500));
        let mut restored = ListAggState::new(33);
        restored.restore(state.snapshot()).unwrap();
        assert!(restored.merge_distinct(Some("a"), "::", 3, StateTtl::new(1000, 2100)));
        assert_eq!(restored.emit(), ScalarValue::Utf8(Some("b::a".into())));
        assert_eq!(
            restored.distinct_count(&ScalarValue::Utf8(Some("a".into()))),
            1
        );
        assert!(restored.expire(StateTtl::new(1000, 2500), 2100));
        assert_eq!(restored.emit(), ScalarValue::Utf8(Some("a".into())));
    }

    #[test]
    fn append_distinct_partials_merge_membership_bits_without_repeating_values() {
        let mut state = ListAggState::new(32);
        let ttl = StateTtl::new(0, 0);
        assert!(state.merge_distinct(Some(""), "é🚀", 1, ttl));
        assert!(!state.merge_distinct(Some(""), "é🚀", 1, ttl));
        assert!(!state.merge_distinct(None, "é🚀", 1, ttl));
        assert!(state.merge_distinct(Some("a"), "é🚀", 1, ttl));
        assert_eq!(state.emit(), ScalarValue::Utf8(Some("é🚀a".into())));
        assert_eq!(
            state.distinct_count(&ScalarValue::Utf8(Some("a".into()))),
            1
        );
    }

    #[test]
    fn a_single_local_partial_normalizes_pending_retractions_before_retained_values() {
        let mut local = ListAggState::new(25);
        local.update(Some("a"), "::", true);
        local.update(Some("a"), "::", false);
        let partial = partials(std::iter::once(&local));
        let incoming = ListAggState::from_partial(partial.as_any().downcast_ref().unwrap(), 0);
        let mut temporary = ListAggState::new(25);
        temporary.merge(incoming, true, StateTtl::new(0, 0));
        let mut retained = ListAggState::new(25);
        retained.update(Some("a"), "::", false);
        retained.update(Some("b"), "::", false);
        retained.merge(temporary, true, StateTtl::new(0, 0));
        assert_eq!(retained.emit(), ScalarValue::Utf8(Some("a::b".into())));
        assert_eq!(retained.element_bytes, 2);
    }

    #[test]
    fn partials_preserve_pending_retractions_and_bundle_cancellation_before_retained_merge() {
        let mut retained = ListAggState::new(25);
        retained.expire(StateTtl::new(1000, 1000), 0);
        retained.update_with_ttl(Some("a"), "::", false, StateTtl::new(1000, 1000));
        retained.update_with_ttl(Some("b"), "::", false, StateTtl::new(1000, 1500));
        let mut first = ListAggState::new(25);
        first.update(Some("a"), "::", true);
        let partial = partials(std::iter::once(&first));
        let mut buffer = ListAggState::from_partial(partial.as_any().downcast_ref().unwrap(), 0);
        let mut second = ListAggState::new(25);
        second.update(Some("a"), "::", false);
        buffer.merge(second, true, StateTtl::new(0, 1700));
        assert!(buffer.values.is_empty() && buffer.pending_retractions.is_empty());
        retained.merge(buffer, true, StateTtl::new(1000, 1700));
        assert!(retained.expire(StateTtl::new(1000, 2000), 1700));
        assert_eq!(retained.emit(), ScalarValue::Utf8(Some("b".into())));
    }

    #[test]
    fn default_separator_empty_merges_refresh_survivors_and_rewrite_pending_clocks() {
        let mut retained = ListAggState::new(25);
        retained.expire(StateTtl::new(1000, 1000), 0);
        retained.update_with_ttl(Some("a"), ",", false, StateTtl::new(1000, 1000));
        retained.update_with_ttl(Some("missing"), ",", true, StateTtl::new(1000, 1100));
        retained.merge(ListAggState::new(25), false, StateTtl::new(1000, 1500));
        assert!(!retained.expire(StateTtl::new(1000, 2000), 1500));
        let mut restored = ListAggState::new(46);
        restored.restore(retained.snapshot()).unwrap();
        assert!(restored.expire(StateTtl::new(1000, 2500), 1500));
        assert!(restored.values.is_empty() && restored.pending_retractions.is_empty());
        assert_eq!(restored.element_bytes, 0);
    }

    #[test]
    fn clockless_list_membership_restores_with_separate_separator_arguments() {
        use super::super::GroupAggregator;
        for kind in [32, 33] {
            let make = |retained: bool| {
                let flags = if retained {
                    super::super::distinct_retention::RETAINED_DISTINCT
                        | if kind == 32 {
                            super::super::distinct_retention::APPEND_DISTINCT
                        } else {
                            0
                        }
                } else {
                    0
                };
                let mut state = GroupAggregator::new(
                    vec![kind | flags, 7 | flags],
                    vec![3, 3],
                    vec![1, 1],
                    vec![0],
                    true,
                )
                .with_count_columns(vec![2, -1]);
                state.ttl_ms = if retained { 1000 } else { 0 };
                state
            };
            let batch = |value: Option<&str>| {
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("k", DataType::Int64, false),
                        Field::new("v", DataType::Utf8, true),
                        Field::new("sep", DataType::Utf8, false),
                    ])),
                    vec![
                        Arc::new(Int64Array::from(vec![1])),
                        Arc::new(StringArray::from(vec![value])),
                        Arc::new(StringArray::from(vec!["::"])),
                    ],
                )
                .unwrap()
            };
            let mut old = make(false);
            old.update(&batch(Some("a")), 1000).unwrap();
            old.update(&batch(None), 1900).unwrap();
            let mut restored = make(true);
            restored.load_snapshot(&old.snapshot(), 2000);
            restored.update(&batch(None), 2500).unwrap();
            let output = restored.update(&batch(Some("a")), 3000).unwrap();
            let row = output.num_rows() - 1;
            assert_eq!(
                ScalarValue::try_from_array(output.column(1), row).unwrap(),
                ScalarValue::Utf8(Some(if kind == 32 { "a::a" } else { "a" }.into()))
            );
            assert_eq!(
                ScalarValue::try_from_array(output.column(2), row).unwrap(),
                ScalarValue::Int64(Some(2))
            );
        }
    }

    #[test]
    fn forgotten_distinct_membership_preserves_cached_result_and_reinsertion() {
        for kind in [32, 33] {
            let mut state = ListAggState::new(kind);
            assert!(state.update(Some("é🚀"), "::", false));
            assert!(!state.update(Some("é🚀"), "::", false));
            let value = ScalarValue::Utf8(Some("é🚀".into()));
            assert_eq!(state.distinct_count(&value), if kind == 32 { 1 } else { 2 });
            state.forget_distinct(&value);
            assert_eq!(state.distinct_bytes, 0);
            assert!(state.distinct_entries().is_empty());
            assert_eq!(state.emit(), value);
            let mut restored = ListAggState::new(kind);
            restored.restore(state.snapshot()).unwrap();
            assert!(restored.update(Some("é🚀"), "::", false));
            assert_eq!(restored.emit(), ScalarValue::Utf8(Some("é🚀::é🚀".into())));
        }
    }

    #[test]
    fn retained_lists_expire_independently_and_successful_removal_refreshes_survivors() {
        let mut state = ListAggState::new(25);
        for (now, value, retract) in [(1000, "a", false), (1500, "b", false), (1700, "b", true)] {
            let ttl = StateTtl::new(1000, now);
            state.expire(ttl, 0);
            state.update_with_ttl(Some(value), "::", retract, ttl);
        }
        let mut restored = ListAggState::new(25);
        restored.restore(state.snapshot()).unwrap();
        for state in [&mut state, &mut restored] {
            assert!(!state.expire(StateTtl::new(1000, 2699), 1700));
            assert_eq!(state.emit(), ScalarValue::Utf8(Some("a".into())));
            assert!(state.expire(StateTtl::new(1000, 2700), 1700));
            assert_eq!(state.emit(), ScalarValue::Utf8(None));
            state.update_with_ttl(Some("missing"), "::", true, StateTtl::new(1000, 2800));
            assert_eq!(state.pending_retractions.len(), 1);
            assert!(state.expire(StateTtl::new(1000, 3800), 2800));
            assert!(state.pending_retractions.is_empty());
            assert_eq!(state.element_bytes, 0);
        }
    }

    #[test]
    fn absent_retraction_does_not_refresh_surviving_list_or_other_pending_entries() {
        let mut state = ListAggState::new(25);
        for (now, value, retract) in [
            (1000, "a", false),
            (1500, "missing", true),
            (1900, "another", true),
        ] {
            let ttl = StateTtl::new(1000, now);
            state.expire(ttl, 0);
            state.update_with_ttl(Some(value), ",", retract, ttl);
        }
        assert!(state.expire(StateTtl::new(1000, 2000), 1900));
        assert!(state.values.is_empty());
        assert_eq!(state.pending_retractions.len(), 2);
        assert!(state.expire(StateTtl::new(1000, 2500), 2000));
        assert_eq!(
            state
                .pending_retractions
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["another"]
        );
        state.expire(StateTtl::new(0, 2600), 2500);
        assert!(state.timestamps.is_none());
    }

    #[test]
    fn distinct_duplicates_retractions_reinsertion_and_restore_keep_arrival_order() {
        for kind in [32, 33] {
            let mut state = ListAggState::new(kind);
            for value in [
                None,
                Some(""),
                Some("a"),
                Some("b"),
                Some("a"),
                Some("é🚀\0"),
            ] {
                state.update(value, "::", false);
            }
            assert_eq!(
                state.emit(),
                ScalarValue::Utf8(Some("::a::b::é🚀\0".into()))
            );
            let mut restored = ListAggState::new(kind);
            restored.restore(state.snapshot()).unwrap();
            assert_eq!(state.emit(), restored.emit());
            assert_eq!(
                restored.distinct_bytes,
                restored
                    .distinct
                    .as_ref()
                    .unwrap()
                    .keys()
                    .map(String::capacity)
                    .sum::<usize>()
            );
            if kind == 33 {
                restored.update(Some("a"), "::", true);
                assert_eq!(state.emit(), restored.emit());
                restored.update(Some("a"), "::", true);
                restored.update(Some("a"), "::", false);
                assert_eq!(
                    restored.emit(),
                    ScalarValue::Utf8(Some("::b::é🚀\0::a".into()))
                );
                let mut second = ListAggState::new(kind);
                second.restore(restored.snapshot()).unwrap();
                for value in ["", "b", "é🚀\0", "a"] {
                    second.update(Some(value), "::", true);
                }
                assert_eq!(second.emit(), ScalarValue::Utf8(None));
                assert_eq!(second.distinct_bytes, 0);
            }
            assert!(ListAggState::new(if kind == 32 { 24 } else { 25 })
                .restore(state.snapshot())
                .is_err());
        }
    }

    #[test]
    fn distinct_negative_multiplicity_does_not_replay_a_pending_retraction() {
        let mut state = ListAggState::new(33);
        state.update(Some("a"), ",", true);
        let mut restored = ListAggState::new(33);
        restored.restore(state.snapshot()).unwrap();
        restored.update(Some("a"), ",", false);
        assert_eq!(restored.emit(), ScalarValue::Utf8(None));
        restored.update(Some("a"), ",", false);
        assert_eq!(restored.emit(), ScalarValue::Utf8(Some("a".into())));
    }

    #[test]
    fn append_preserves_empty_values_nulls_and_separators() {
        for separator in ["", ",", "::", "é🚀", "'"] {
            let mut state = ListAggState::new(24);
            assert_eq!(state.emit(), ScalarValue::Utf8(None));
            state.update(None, separator, false);
            for value in ["", "a", "a", "\0\n🚀"] {
                state.update(Some(value), separator, false);
            }
            assert_eq!(
                state.emit(),
                ScalarValue::Utf8(Some(["", "a", "a", "\0\n🚀"].join(separator)))
            );
            let mut restored = ListAggState::new(24);
            restored.restore(state.snapshot()).unwrap();
            assert_eq!(state.emit(), restored.emit());
            assert!(restored.bytes() >= std::mem::size_of::<ListAggState>());
        }
    }

    #[test]
    fn retract_removes_first_equal_occurrence_and_retains_pending_retractions() {
        let mut state = ListAggState::new(25);
        for value in ["a", "b", "a", "c"] {
            state.update(Some(value), "::", false);
        }
        state.update(Some("a"), "::", true);
        state.update(Some("absent"), "::", true);
        state.update(Some("absent"), "::", false);
        assert_eq!(
            state.emit(),
            ScalarValue::Utf8(Some("b::a::c::absent".into()))
        );
        let mut restored = ListAggState::new(25);
        restored.restore(state.snapshot()).unwrap();
        assert_eq!(restored.pending_retractions, ["absent"]);
        assert_eq!(restored.emit(), state.emit());
        for value in ["b", "a", "c", "absent"] {
            restored.update(Some(value), "::", true);
        }
        assert_eq!(restored.emit(), ScalarValue::Utf8(None));
        assert_eq!(restored.element_bytes, "absent".len());
    }
}
