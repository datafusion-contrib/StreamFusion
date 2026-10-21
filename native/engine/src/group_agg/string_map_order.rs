use super::java_map_order::JavaMapOrder;
use crate::flink_key::string_distinct_map_hash;
use std::sync::Arc;

pub(super) struct StringMapOrder {
    order: JavaMapOrder<usize>,
    keys: Vec<Option<Arc<str>>>,
    indices: ahash::HashMap<Arc<str>, usize>,
    separator: Option<String>,
    payload: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_and_composite_views_match_released_hashes_copies_and_removals() {
        let cases: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../src/test/resources/string-map-order.json"
        ))
        .unwrap();
        for case in cases.as_array().unwrap() {
            let separator = case["separator"].as_str();
            let mut order = StringMapOrder::new();
            order.set_separator(separator);
            for (index, operation) in case["operations"].as_array().unwrap().iter().enumerate() {
                if case["copy_after"].as_i64() == Some(index as i64) {
                    if let Some(copied) = order.copy_if_reordered() {
                        order = copied;
                    }
                }
                let value = operation["value"].as_str();
                assert_eq!(
                    string_distinct_map_hash(value, separator) as i64,
                    case["hashes"][index].as_i64().unwrap(),
                    "{case}"
                );
                if operation["remove"].as_bool().unwrap() {
                    order.remove(value);
                } else {
                    order.insert(value);
                }
            }
            let expected: Vec<_> = case["order"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str())
                .collect();
            assert_eq!(order.iter().collect::<Vec<_>>(), expected, "{case}");
        }
    }
}

impl StringMapOrder {
    pub(super) fn new() -> Self {
        Self::with_capacity(16, None)
    }

    fn with_capacity(capacity: usize, separator: Option<String>) -> Self {
        Self {
            order: JavaMapOrder::with_capacity(capacity),
            keys: vec![None],
            indices: ahash::HashMap::default(),
            separator,
            payload: 0,
        }
    }

    pub(super) fn set_separator(&mut self, separator: Option<&str>) {
        if self.separator.as_deref() != separator {
            assert_eq!(
                self.order.len(),
                0,
                "DISTINCT arguments changed within a view"
            );
            self.separator = separator.map(str::to_owned);
        }
    }

    pub(super) fn insert(&mut self, value: Option<&str>) {
        let hash = string_distinct_map_hash(value, self.separator.as_deref());
        let index = match value {
            None => 0,
            Some(value) => match self.indices.get(value) {
                Some(&index) => index,
                None => {
                    let index = self.keys.len();
                    let key: Arc<str> = Arc::from(value);
                    self.payload += value.len();
                    self.keys.push(Some(key.clone()));
                    self.indices.insert(key, index);
                    index
                }
            },
        };
        self.order.insert(index, hash);
    }

    pub(super) fn remove(&mut self, value: Option<&str>) {
        let hash = string_distinct_map_hash(value, self.separator.as_deref());
        let index = match value {
            None => Some(0),
            Some(value) => self.indices.get(value).copied(),
        };
        if let Some(index) = index {
            self.order.remove(index, hash);
        }
    }

    pub(super) fn copy_if_reordered(&self) -> Option<Self> {
        let size = self.order.len();
        let required = if size <= 2 {
            size + 1
        } else {
            ((size as f32) / 0.75).ceil() as usize
        };
        let capacity = required.next_power_of_two().min(1 << 30);
        if self.order.copy_preserves_order(capacity) {
            return None;
        }
        let mut copied = Self::with_capacity(capacity, self.separator.clone());
        for value in self.iter() {
            copied.insert(value);
        }
        Some(copied)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = Option<&str>> {
        self.order.iter().map(|index| self.keys[index].as_deref())
    }

    pub(super) fn len(&self) -> usize {
        self.order.len()
    }

    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.order.bytes()
            + self.keys.capacity() * std::mem::size_of::<Option<Arc<str>>>()
            + self.indices.capacity() * (std::mem::size_of::<(Arc<str>, usize)>() + 1)
            + self.payload
            + self.indices.len() * 2 * std::mem::size_of::<usize>()
            + self.separator.as_ref().map_or(0, String::capacity)
    }
}
