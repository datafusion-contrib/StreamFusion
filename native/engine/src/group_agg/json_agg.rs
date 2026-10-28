use crate::*;
use arrow::array::ArrayBuilder;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use streamfusion_bridge::java_map_order::JavaMapOrder;

#[derive(Default, Serialize, Deserialize)]
struct JsonViewTimestamps {
    values: VecDeque<i64>,
    fields: HashMap<String, i64>,
    null_key: Option<i64>,
}

pub(super) fn is_json_agg(kind: i64) -> bool {
    (26..=31).contains(&kind) || is_object_merge(kind)
}

pub(super) fn is_object_merge(kind: i64) -> bool {
    (42..=45).contains(&kind)
}

pub(super) fn object_partial_type() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, false),
            ]
            .into(),
        ),
        false,
    )))
}

struct LocalObjectOrder {
    keys: Vec<(Option<String>, i32)>,
    payload: usize,
    order: JavaMapOrder<usize>,
}
impl LocalObjectOrder {
    fn new() -> Self {
        Self {
            keys: Vec::new(),
            payload: 0,
            order: JavaMapOrder::with_capacity(16),
        }
    }
    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.order.bytes()
            + self.keys.capacity() * std::mem::size_of::<(Option<String>, i32)>()
            + self.payload
    }
}

fn string_map_hash(key: Option<&str>) -> i32 {
    crate::flink_key::string_distinct_map_hash(key, None)
}

pub(super) fn object_partials<'a>(states: impl Iterator<Item = &'a JsonAggState>) -> ArrayRef {
    let mut offsets = vec![0_i32];
    let mut keys = arrow::array::StringBuilder::new();
    let mut values = arrow::array::StringBuilder::new();
    for state in states {
        if let Some(order) = &state.local_order {
            let legacy_null = state.kind >= 30 && state.legacy_null_key.is_some();
            let mut capacity = 16;
            while order.order.len() > capacity * 3 / 4 {
                capacity *= 2;
            }
            let mut empty_alias: Option<(usize, usize)> = None;
            let copied = (legacy_null || !order.order.copy_preserves_order(capacity)).then(|| {
                let mut copied = JavaMapOrder::with_capacity(16);
                for index in order.order.iter() {
                    let key = order.keys[index].0.as_deref();
                    let hash = if legacy_null && (key.is_none() || key == Some("")) {
                        if let Some((_, last)) = &mut empty_alias {
                            *last = index;
                            continue;
                        }
                        empty_alias = Some((index, index));
                        string_map_hash(Some(""))
                    } else {
                        order.keys[index].1
                    };
                    copied.insert(index, hash);
                }
                copied
            });
            for index in copied.as_ref().unwrap_or(&order.order).iter() {
                let key = order.keys[index].0.as_deref();
                // Released 1.18's non-null STRING MapView getter turns a heap NULL key
                // into an empty key; rebuilding its HashMap also collapses that alias.
                keys.append_option(if state.kind >= 30 {
                    Some(key.unwrap_or(""))
                } else {
                    key
                });
                let value_index = empty_alias
                    .filter(|(first, _)| *first == index)
                    .map_or(index, |(_, last)| last);
                let value_key = order.keys[value_index].0.as_deref();
                values.append_value(value_key.map_or_else(
                    || state.legacy_null_key.as_ref().unwrap(),
                    |key| &state.fields[key],
                ));
            }
        }
        offsets.push(keys.len() as i32);
    }
    let DataType::List(item) = object_partial_type() else {
        unreachable!()
    };
    let DataType::Struct(fields) = item.data_type() else {
        unreachable!()
    };
    let entries = arrow::array::StructArray::new(
        fields.clone(),
        vec![Arc::new(keys.finish()), Arc::new(values.finish())],
        None,
    );
    Arc::new(arrow::array::ListArray::new(
        item,
        arrow::buffer::OffsetBuffer::new(offsets.into()),
        Arc::new(entries),
        None,
    ))
}

#[derive(Debug)]
pub(super) enum JsonAggregateError {
    Duplicate(String),
    NullKey,
    NullDuplicate,
    LocalNullDuplicate,
    Serialization,
}

impl std::fmt::Display for JsonAggregateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Duplicate(key) => write!(f, "Key '{key}' is already present. Duplicate keys are not allowed in JSON_OBJECTAGG. Please ensure that keys are unique."),
            Self::LocalNullDuplicate => f.write_str("Duplicate NULL JSON object key in a local partial"),
            Self::NullDuplicate => f.write_str("Duplicate NULL JSON object key"),
            Self::NullKey => f.write_str("Map views don't support null keys."),
            Self::Serialization => f.write_str("The accumulator state could not be serialized."),
        }
    }
}

impl std::error::Error for JsonAggregateError {}

impl JsonAggregateError {
    pub(super) fn method(&self) -> &'static str {
        match self {
            Self::NullKey => "jsonNullKey",
            Self::LocalNullDuplicate => "jsonLocalDuplicateNullKey",
            Self::NullDuplicate => "jsonDuplicateNullKey",
            Self::Duplicate(_) => "jsonAggregate",
            Self::Serialization => "jsonSerialization",
        }
    }

    fn execution(self) -> DataFusionError {
        DataFusionError::External(Box::new(self))
    }
}

/// Values have already passed through Flink's JSON_STRING expression. Keeping those
/// lexemes preserves decimal scale and avoids parsing and re-encoding each value.
#[derive(Serialize, Deserialize)]
pub(crate) struct JsonAggState {
    kind: i64,
    #[serde(skip)]
    local_order: Option<Box<LocalObjectOrder>>,
    values: VecDeque<String>,
    fields: HashMap<String, String>,
    legacy_null_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timestamps: Option<Box<JsonViewTimestamps>>,
    #[serde(skip)]
    element_bytes: usize,
}

impl JsonAggState {
    pub(super) fn new(kind: i64) -> Self {
        Self {
            kind: if is_object_merge(kind) {
                kind - 14
            } else {
                kind
            },
            local_order: None,
            values: VecDeque::new(),
            fields: HashMap::new(),
            legacy_null_key: None,
            timestamps: None,
            element_bytes: 0,
        }
    }

    pub(super) fn update_local(
        &mut self,
        key: Option<&str>,
        value: Option<&str>,
        retract: bool,
    ) -> Result<(), DataFusionError> {
        let present = key.map_or(self.legacy_null_key.is_some(), |key| {
            self.fields.contains_key(key)
        });
        let kind = self.kind;
        self.kind = if kind < 30 { kind + 2 } else { kind };
        let result = self.update(key, value, retract);
        self.kind = kind;
        if matches!(&result, Err(DataFusionError::External(error)) if error.downcast_ref::<JsonAggregateError>().is_some_and(|error| matches!(error, JsonAggregateError::NullDuplicate)))
        {
            return Err(JsonAggregateError::LocalNullDuplicate.execution());
        }
        result?;
        let order = self
            .local_order
            .get_or_insert_with(|| Box::new(LocalObjectOrder::new()));
        let hash = string_map_hash(key);
        if retract && present {
            let index = order
                .order
                .iter()
                .find(|&index| order.keys[index].0.as_deref() == key)
                .unwrap();
            order.order.remove(index, hash);
            if let Some(key) = order.keys[index].0.take() {
                order.payload -= key.capacity();
            }
        } else if !retract && (value.is_some() || self.kind % 2 != 0) {
            let index = order.keys.len();
            let key = key.map(str::to_owned);
            order.payload += key.as_ref().map_or(0, String::capacity);
            order.keys.push((key, hash));
            order.order.insert(index, hash);
        }
        Ok(())
    }

    pub(super) fn merge_partial(
        &mut self,
        partial: &arrow::array::ListArray,
        row: usize,
        ttl: StateTtl,
    ) -> Result<(), DataFusionError> {
        let entries = partial
            .values()
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("JSON object entries");
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("JSON object keys");
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("JSON object values");
        for index in
            partial.value_offsets()[row] as usize..partial.value_offsets()[row + 1] as usize
        {
            self.update_with_ttl(
                (!keys.is_null(index)).then(|| keys.value(index)),
                Some(values.value(index)),
                false,
                ttl,
            )?;
        }
        Ok(())
    }

    pub(super) fn expire(&mut self, ttl: StateTtl, previous_write_ms: i64) -> bool {
        if !ttl.enabled() {
            return false;
        }
        let times = self.timestamps.get_or_insert_with(|| {
            Box::new(JsonViewTimestamps {
                values: VecDeque::from(vec![previous_write_ms; self.values.len()]),
                fields: self
                    .fields
                    .keys()
                    .map(|key| (key.clone(), previous_write_ms))
                    .collect(),
                null_key: self.legacy_null_key.as_ref().map(|_| previous_write_ms),
            })
        });
        let mut changed = false;
        let mut value_times = std::mem::take(&mut times.values).into_iter();
        self.values.retain(|value| {
            let ts = value_times.next().expect("JSON value timestamp");
            if ttl.expired(ts) {
                self.element_bytes -= value.capacity();
                changed = true;
                false
            } else {
                times.values.push_back(ts);
                true
            }
        });
        times.fields.retain(|key, ts| {
            if !ttl.expired(*ts) {
                return true;
            }
            if let Some((key, value)) = self.fields.remove_entry(key) {
                self.element_bytes -= key.capacity() + value.capacity();
                changed = true;
            }
            false
        });
        if times.null_key.is_some_and(|ts| ttl.expired(ts)) {
            if let Some(value) = self.legacy_null_key.take() {
                self.element_bytes -= value.capacity();
                changed = true;
            }
            times.null_key = None;
        }
        changed
    }

    pub(super) fn update_with_ttl(
        &mut self,
        key: Option<&str>,
        value: Option<&str>,
        retract: bool,
        ttl: StateTtl,
    ) -> Result<(), DataFusionError> {
        let old_len = self.values.len();
        self.update(key, value, retract)?;
        let Some(times) = &mut self.timestamps else {
            return Ok(());
        };
        if self.kind <= 27 {
            if self.values.len() > old_len {
                times.values.push_back(ttl.now());
            } else if self.values.len() < old_len {
                // StateListView.remove rewrites all survivors only when removal succeeds.
                times.values.clear();
                times.values.resize(self.values.len(), ttl.now());
            }
        } else if let Some(key) = key {
            if retract {
                times.fields.remove(key);
            } else if self.fields.contains_key(key) {
                times.fields.insert(key.to_owned(), ttl.now());
            }
        } else {
            times.null_key = self.legacy_null_key.as_ref().map(|_| ttl.now());
        }
        Ok(())
    }

    pub(super) fn update(
        &mut self,
        key: Option<&str>,
        value: Option<&str>,
        retract: bool,
    ) -> Result<(), DataFusionError> {
        let absent = self.kind % 2 == 0;
        if self.kind <= 27 {
            if retract {
                let value = value.unwrap_or("null");
                if let Some(index) = self.values.iter().position(|old| old == value) {
                    self.element_bytes -= self.values.remove(index).unwrap().capacity();
                }
            } else if value.is_some() || !absent {
                let value = value.unwrap_or("null").to_owned();
                self.element_bytes += value.capacity();
                self.values.push_back(value);
            }
            return Ok(());
        }
        let Some(key) = key else {
            if self.kind < 30 {
                return Err(JsonAggregateError::NullKey.execution());
            }
            if retract {
                if let Some(value) = self.legacy_null_key.take() {
                    self.element_bytes -= value.capacity();
                }
            } else {
                if self.legacy_null_key.is_some() {
                    return Err(JsonAggregateError::NullDuplicate.execution());
                }
                if value.is_some() || !absent {
                    let value = value.unwrap_or("null").to_owned();
                    self.element_bytes += value.capacity();
                    self.legacy_null_key = Some(value);
                }
            }
            return Ok(());
        };
        if retract {
            if let Some((key, value)) = self.fields.remove_entry(key) {
                self.element_bytes -= key.capacity() + value.capacity();
            }
        } else {
            // Released Flink checks duplicates before applying ABSENT ON NULL.
            if self.fields.contains_key(key) {
                return Err(JsonAggregateError::Duplicate(key.to_owned()).execution());
            }
            if value.is_some() || !absent {
                let (key, value) = (key.to_owned(), value.unwrap_or("null").to_owned());
                self.element_bytes += key.capacity() + value.capacity();
                self.fields.insert(key, value);
            }
        }
        Ok(())
    }

    pub(super) fn emit(&self) -> Result<ScalarValue, DataFusionError> {
        if self.legacy_null_key.is_some() {
            return Err(JsonAggregateError::Serialization.execution());
        }
        let mut result = String::new();
        if self.kind <= 27 {
            result.push('[');
            for (i, value) in self.values.iter().enumerate() {
                if i != 0 {
                    result.push(',');
                }
                result.push_str(value);
            }
            result.push(']');
        } else {
            let mut fields: Vec<_> = self.fields.iter().collect();
            fields.sort_unstable_by(|(a, _), (b, _)| a.encode_utf16().cmp(b.encode_utf16()));
            result.push('{');
            for (i, (key, value)) in fields.into_iter().enumerate() {
                if i != 0 {
                    result.push(',');
                }
                crate::json_string::write_json_string(&mut result, key).expect("writing a string");
                result.push(':');
                result.push_str(value);
            }
            result.push('}');
        }
        Ok(ScalarValue::Utf8(Some(result)))
    }

    pub(super) fn snapshot(&self) -> ScalarValue {
        ScalarValue::Binary(Some(
            serde_json::to_vec(&(1_u8, self)).expect("serialize JSON aggregate state"),
        ))
    }

    pub(super) fn restore(&mut self, value: ScalarValue) -> Result<(), DataFusionError> {
        let ScalarValue::Binary(Some(bytes)) = value else {
            return Err(DataFusionError::Execution(
                "missing JSON aggregate state".into(),
            ));
        };
        let (version, mut restored): (u8, Self) = serde_json::from_slice(&bytes).map_err(|e| {
            DataFusionError::Execution(format!("invalid JSON aggregate state: {e}"))
        })?;
        if version != 1 || restored.kind != self.kind {
            return Err(DataFusionError::Execution(
                "incompatible JSON aggregate state".into(),
            ));
        }
        restored.element_bytes = restored.values.iter().map(String::capacity).sum::<usize>()
            + restored
                .fields
                .iter()
                .map(|(k, v)| k.capacity() + v.capacity())
                .sum::<usize>()
            + restored
                .legacy_null_key
                .as_ref()
                .map_or(0, String::capacity);
        *self = restored;
        Ok(())
    }

    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.local_order.as_ref().map_or(0, |order| order.bytes())
            + self.element_bytes
            + self.values.capacity() * std::mem::size_of::<String>()
            + self.fields.capacity() * (2 * std::mem::size_of::<String>() + 1)
            + self.timestamps.as_ref().map_or(0, |times| {
                std::mem::size_of_val(times.as_ref())
                    + times.values.capacity() * std::mem::size_of::<i64>()
                    + times.fields.capacity() * (std::mem::size_of::<(String, i64)>() + 1)
                    + times.fields.keys().map(String::capacity).sum::<usize>()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_partials_merge_live_heap_entries_and_expire_only_written_fields() {
        let mut local = JsonAggState::new(42);
        local.update_local(Some("old"), Some("1"), false).unwrap();
        local.update_local(Some("old"), None, true).unwrap();
        local.update_local(Some("a"), Some("2"), false).unwrap();
        local.update_local(Some("b"), None, false).unwrap();
        let partial = object_partials(std::iter::once(&local));
        let partial = partial
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        let mut global = JsonAggState::new(42);
        global.expire(StateTtl::new(1000, 1000), 0);
        global
            .merge_partial(partial, 0, StateTtl::new(1000, 1000))
            .unwrap();
        assert_eq!(
            global.emit().unwrap(),
            ScalarValue::Utf8(Some("{\"a\":2}".into()))
        );
        assert!(global
            .merge_partial(partial, 0, StateTtl::new(1000, 1500))
            .is_err());
        global.expire(StateTtl::new(1000, 2000), 1500);
        global
            .merge_partial(partial, 0, StateTtl::new(1000, 2000))
            .unwrap();
        let mut restored = JsonAggState::new(42);
        restored.restore(global.snapshot()).unwrap();
        assert_eq!(restored.emit().unwrap(), global.emit().unwrap());
    }

    #[test]
    fn legacy_map_transport_collapses_null_and_empty_keys_in_released_iteration_order() {
        for empty_first in [false, true] {
            let mut local = JsonAggState::new(45);
            let keys = if empty_first {
                [Some(""), None]
            } else {
                [None, Some("")]
            };
            for (index, key) in keys.into_iter().enumerate() {
                local
                    .update_local(key, Some(if index == 0 { "1" } else { "2" }), false)
                    .unwrap();
            }
            let partial = object_partials(std::iter::once(&local));
            let partial = partial
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .unwrap();
            assert_eq!(partial.value_length(0), 1);
            let mut global = JsonAggState::new(45);
            global
                .merge_partial(partial, 0, StateTtl::new(0, 0))
                .unwrap();
            assert_eq!(
                global.emit().unwrap(),
                ScalarValue::Utf8(Some(format!(
                    "{{\"\":{}}}",
                    if empty_first { 1 } else { 2 }
                )))
            );
        }
    }

    #[test]
    fn local_null_keys_are_heap_entries_until_the_retained_map_is_accessed() {
        let mut local = JsonAggState::new(43);
        local.update_local(None, None, false).unwrap();
        assert!(local.update_local(None, Some("1"), false).is_err());
        let partial = object_partials(std::iter::once(&local));
        let partial = partial
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        let mut current = JsonAggState::new(43);
        assert!(current
            .merge_partial(partial, 0, StateTtl::new(0, 0))
            .is_err());
        let mut legacy_local = JsonAggState::new(45);
        legacy_local.update_local(None, None, false).unwrap();
        let legacy_partial = object_partials(std::iter::once(&legacy_local));
        let legacy_partial = legacy_partial
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        let mut legacy = JsonAggState::new(45);
        legacy
            .merge_partial(legacy_partial, 0, StateTtl::new(0, 0))
            .unwrap();
        assert_eq!(
            legacy.emit().unwrap(),
            ScalarValue::Utf8(Some("{\"\":null}".into()))
        );
    }

    #[test]
    fn list_entry_expiry_and_successful_removal_have_separate_write_clocks() {
        let mut state = JsonAggState::new(26);
        for (now, value, retract) in [
            (5000, Some("1"), false),
            (5500, Some("2"), false),
            (5900, None, false),
            (6000, Some("3"), false),
        ] {
            let ttl = StateTtl::new(1000, now);
            state.expire(ttl, 0);
            state.update_with_ttl(None, value, retract, ttl).unwrap();
        }
        assert_eq!(
            state.emit().unwrap(),
            ScalarValue::Utf8(Some("[2,3]".into()))
        );
        state
            .update_with_ttl(None, Some("2"), true, StateTtl::new(1000, 6400))
            .unwrap();
        assert!(!state.expire(StateTtl::new(1000, 7399), 0));
        assert!(state.expire(StateTtl::new(1000, 7400), 0));
        assert_eq!(state.emit().unwrap(), ScalarValue::Utf8(Some("[]".into())));
    }

    #[test]
    fn object_entry_expiry_survives_restore_and_allows_key_reuse() {
        let mut state = JsonAggState::new(28);
        for (now, key, value) in [(5000, "a", "1"), (5500, "b", "2")] {
            let ttl = StateTtl::new(1000, now);
            state.expire(ttl, 0);
            state
                .update_with_ttl(Some(key), Some(value), false, ttl)
                .unwrap();
        }
        let mut restored = JsonAggState::new(28);
        restored.restore(state.snapshot()).unwrap();
        assert!(restored.expire(StateTtl::new(1000, 6000), 0));
        restored
            .update_with_ttl(Some("a"), Some("3"), false, StateTtl::new(1000, 6000))
            .unwrap();
        assert_eq!(
            restored.emit().unwrap(),
            ScalarValue::Utf8(Some("{\"a\":3,\"b\":2}".into()))
        );
        assert!(restored.expire(StateTtl::new(1000, 6500), 0));
        assert_eq!(
            restored.emit().unwrap(),
            ScalarValue::Utf8(Some("{\"a\":3}".into()))
        );
    }

    #[test]
    fn list_expiry_handles_a_backwards_clock_without_hiding_expired_members() {
        let mut state = JsonAggState::new(26);
        for now in [5000, 4500] {
            let ttl = StateTtl::new(1000, now);
            state.expire(ttl, 0);
            state.update_with_ttl(None, Some("1"), false, ttl).unwrap();
        }
        assert!(state.expire(StateTtl::new(1000, 5500), 0));
        assert_eq!(state.emit().unwrap(), ScalarValue::Utf8(Some("[1]".into())));
    }

    #[test]
    fn arrays_retain_lexemes_order_null_policy_and_first_duplicate_retraction() {
        for kind in [26, 27] {
            let mut state = JsonAggState::new(kind);
            for value in [Some("1.00"), Some("\"a\""), None, Some("1.00")] {
                state.update(None, value, false).unwrap();
            }
            state.update(None, Some("1.00"), true).unwrap();
            let expected = if kind == 26 {
                "[\"a\",1.00]"
            } else {
                "[\"a\",null,1.00]"
            };
            assert_eq!(
                state.emit().unwrap(),
                ScalarValue::Utf8(Some(expected.into()))
            );
            let mut restored = JsonAggState::new(kind);
            restored.restore(state.snapshot()).unwrap();
            assert_eq!(restored.emit().unwrap(), state.emit().unwrap());
            restored.update(None, Some("\"a\""), true).unwrap();
            state.update(None, Some("\"a\""), true).unwrap();
            assert_eq!(restored.emit().unwrap(), state.emit().unwrap());
        }
    }

    #[test]
    fn objects_sort_utf16_keys_check_duplicates_before_omission_and_retract_by_key() {
        for kind in [28, 29, 30, 31] {
            let mut state = JsonAggState::new(kind);
            for key in ["\u{e000}", "🚀", "a"] {
                state.update(Some(key), Some("1.00"), false).unwrap();
            }
            assert_eq!(
                state.emit().unwrap(),
                ScalarValue::Utf8(Some("{\"a\":1.00,\"🚀\":1.00,\"\u{e000}\":1.00}".into()))
            );
            let error = state.update(Some("a"), None, false).unwrap_err();
            assert!(matches!(error, DataFusionError::External(_)));
            assert!(error.to_string().contains("Duplicate keys are not allowed"));
            let mut restored = JsonAggState::new(kind);
            restored.restore(state.snapshot()).unwrap();
            restored.update(Some("a"), Some("different"), true).unwrap();
            restored.update(Some("a"), Some("false"), false).unwrap();
            assert!(restored.emit().unwrap().to_string().contains("\"a\":false"));
        }
    }

    #[test]
    fn null_key_failures_keep_accumulation_and_materialization_separate() {
        assert!(JsonAggState::new(28)
            .update(None, None, false)
            .unwrap_err()
            .to_string()
            .contains("Map views"));
        let mut absent = JsonAggState::new(30);
        absent.update(None, None, false).unwrap();
        assert_eq!(absent.emit().unwrap(), ScalarValue::Utf8(Some("{}".into())));
        absent.update(None, Some("0"), false).unwrap();
        assert!(absent
            .emit()
            .unwrap_err()
            .to_string()
            .contains("could not be serialized"));
        let mut restored = JsonAggState::new(30);
        restored.restore(absent.snapshot()).unwrap();
        restored.update(None, None, true).unwrap();
        assert_eq!(
            restored.emit().unwrap(),
            ScalarValue::Utf8(Some("{}".into()))
        );
    }
}
