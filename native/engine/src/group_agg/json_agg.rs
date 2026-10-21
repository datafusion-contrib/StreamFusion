use crate::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

#[derive(Default, Serialize, Deserialize)]
struct JsonViewTimestamps {
    values: VecDeque<i64>,
    fields: HashMap<String, i64>,
    null_key: Option<i64>,
}

pub(super) fn is_json_agg(kind: i64) -> bool {
    (26..=31).contains(&kind)
}

#[derive(Debug)]
pub(super) enum JsonAggregateError {
    Duplicate(String),
    NullKey,
    NullDuplicate,
    Serialization,
}

impl std::fmt::Display for JsonAggregateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Duplicate(key) => write!(f, "Key '{key}' is already present. Duplicate keys are not allowed in JSON_OBJECTAGG. Please ensure that keys are unique."),
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
            kind,
            values: VecDeque::new(),
            fields: HashMap::new(),
            legacy_null_key: None,
            timestamps: None,
            element_bytes: 0,
        }
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
