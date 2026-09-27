//! Iteration order of insert-only, same-scale decimal membership in a Java HashMap.

use super::java_map_order::JavaMapOrder;

pub(super) struct DecimalMapOrder {
    scale: i8,
    order: JavaMapOrder<Option<i128>>,
}

fn decimal_hash(value: i128, scale: i8) -> i32 {
    let magnitude = value.unsigned_abs();
    let mut hash = 0u32;
    for shift in [96, 64, 32, 0] {
        hash = hash
            .wrapping_mul(31)
            .wrapping_add((magnitude >> shift) as u32);
    }
    if value < 0 {
        hash = hash.wrapping_neg();
    }
    hash = hash.wrapping_mul(31).wrapping_add(scale as u32);
    (hash ^ (hash >> 16)) as i32
}

impl DecimalMapOrder {
    pub(super) fn new(scale: i8) -> Self {
        Self {
            scale,
            order: JavaMapOrder::with_capacity(16),
        }
    }

    fn copied_capacity(&self) -> usize {
        let size = self.order.len();
        let required = if size <= 2 {
            size + 1
        } else {
            ((size as f32) / 0.75).ceil() as usize
        };
        required.next_power_of_two().min(1 << 30)
    }

    pub(super) fn copy_if_reordered(&self) -> Option<Self> {
        // Equal bucket counts preserve short list-bin insertion order. Tree insertion can move
        // a root to the front, including when copying a long list left behind by a resize.
        (!self.order.copy_preserves_order(self.copied_capacity())).then(|| self.copied())
    }

    pub(super) fn copied(&self) -> Self {
        let mut copy = Self {
            scale: self.scale,
            order: JavaMapOrder::with_capacity(self.copied_capacity()),
        };
        for key in self.iter() {
            copy.insert_optional(key);
        }
        copy
    }

    pub(super) fn insert_optional(&mut self, key: Option<i128>) -> bool {
        self.order
            .insert(key, key.map_or(0, |key| decimal_hash(key, self.scale)))
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = Option<i128>> + '_ {
        self.order.iter()
    }

    pub(super) fn len(&self) -> usize {
        self.order.len()
    }

    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.order.bytes()
            - std::mem::size_of::<JavaMapOrder<Option<i128>>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_copy_handles_long_lists_left_by_resize() {
        let mut map = DecimalMapOrder::new(0);
        for value in (0..10000)
            .filter(|&value| decimal_hash(value, 0) & 63 == 0)
            .take(10)
        {
            map.insert_optional(Some(value));
        }
        for value in (0..10000)
            .filter(|&value| decimal_hash(value, 0) & 63 != 0)
            .take(23)
        {
            map.insert_optional(Some(value));
        }
        let copy = map
            .copy_if_reordered()
            .expect("copy can treeify the long bin");
        assert_eq!(
            copy.iter().collect::<Vec<_>>(),
            map.copied().iter().collect::<Vec<_>>()
        );
        let mut ordinary = DecimalMapOrder::new(2);
        for value in 0..16 {
            ordinary.insert_optional(Some(value));
        }
        assert!(ordinary.copy_if_reordered().is_none());
    }

    #[test]
    fn matches_released_jdk_decimal_map_fixtures() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../src/test/resources/decimal-map-order.json"
        ))
        .unwrap();
        for case in fixtures.as_array().unwrap() {
            let mut map = DecimalMapOrder::new(case["scale"].as_i64().unwrap() as i8);
            let keys = case["keys"].as_array().unwrap();
            for position in 0..=keys.len() {
                if case["copy_after"].as_i64().unwrap() == position as i64 {
                    map = map.copied();
                }
                let full_copy = map.copied();
                let optimized_copy = map.copy_if_reordered();
                assert_eq!(
                    optimized_copy
                        .as_ref()
                        .unwrap_or(&map)
                        .iter()
                        .collect::<Vec<_>>(),
                    full_copy.iter().collect::<Vec<_>>(),
                    "{} at prefix {position}",
                    case["name"]
                );
                if position < keys.len() {
                    map.insert_optional(keys[position].as_str().map(|key| key.parse().unwrap()));
                    map.order.assert_structure();
                }
            }
            let expected: Vec<Option<i128>> = case["order"]
                .as_array()
                .unwrap()
                .iter()
                .map(|key| key.as_str().map(|key| key.parse().unwrap()))
                .collect();
            assert_eq!(map.iter().collect::<Vec<_>>(), expected, "{}", case["name"]);
            assert!(map.bytes() >= std::mem::size_of::<DecimalMapOrder>());
        }
    }
}
