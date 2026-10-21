use super::{decimal_map_order::DecimalMapOrder, java_map_order::JavaMapOrder, OrdF64};
use crate::*;

enum Order {
    Decimal(DecimalMapOrder),
    Floating(JavaMapOrder<Option<FloatingKey>>),
}

#[derive(Clone, Copy)]
struct FloatingKey {
    value: OrdF64,
    // Widening FLOAT to DOUBLE quiets signaling NaNs; transport retains the original key bits.
    bits: u64,
}

impl PartialEq for FloatingKey {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for FloatingKey {}

impl PartialOrd for FloatingKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FloatingKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

pub(super) struct NumericMapOrder {
    data_type: DataType,
    order: Order,
    null_counts: Vec<i64>,
}

impl NumericMapOrder {
    pub(super) fn new(data_type: &DataType) -> Self {
        Self {
            data_type: data_type.clone(),
            null_counts: Vec::new(),
            order: match data_type {
                DataType::Decimal128(_, scale) => Order::Decimal(DecimalMapOrder::new(*scale)),
                DataType::Float32 | DataType::Float64 => {
                    Order::Floating(JavaMapOrder::with_capacity(16))
                }
                other => panic!("unsupported ordered numeric view: {other:?}"),
            },
        }
    }

    pub(super) fn insert(&mut self, value: &ScalarValue) {
        match (&mut self.order, value) {
            (Order::Decimal(order), ScalarValue::Decimal128(value, _, _)) => {
                order.insert_optional(*value);
            }
            (Order::Floating(order), ScalarValue::Float32(value)) => {
                let key = value.map(|v| FloatingKey {
                    value: OrdF64::new(v as f64),
                    bits: v.to_bits() as u64,
                });
                let hash = key.map_or(0, |v| (v.value.canonical() as f32).to_bits());
                order.insert(key, (hash ^ (hash >> 16)) as i32);
            }
            (Order::Floating(order), ScalarValue::Float64(value)) => {
                let key = value.map(|v| FloatingKey {
                    value: OrdF64::new(v),
                    bits: v.to_bits(),
                });
                let bits = key.map_or(0, |v| v.value.canonical().to_bits());
                let hash = (bits ^ (bits >> 32)) as u32;
                order.insert(key, (hash ^ (hash >> 16)) as i32);
            }
            _ => panic!("numeric view key does not match its type"),
        }
    }

    pub(super) fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub(super) fn remove(&mut self, value: &ScalarValue) {
        match (&mut self.order, value) {
            (Order::Decimal(order), ScalarValue::Decimal128(value, _, _)) => {
                order.remove_optional(*value);
            }
            (Order::Floating(order), ScalarValue::Float32(value)) => {
                let key = value.map(|v| FloatingKey {
                    value: OrdF64::new(v as f64),
                    bits: v.to_bits() as u64,
                });
                let hash = key.map_or(0, |v| (v.value.canonical() as f32).to_bits());
                order.remove(key, (hash ^ (hash >> 16)) as i32);
            }
            (Order::Floating(order), ScalarValue::Float64(value)) => {
                let key = value.map(|v| FloatingKey {
                    value: OrdF64::new(v),
                    bits: v.to_bits(),
                });
                let bits = key.map_or(0, |v| v.value.canonical().to_bits());
                let hash = (bits ^ (bits >> 32)) as u32;
                order.remove(key, (hash ^ (hash >> 16)) as i32);
            }
            _ => panic!("numeric view key does not match its type"),
        }
    }

    pub(super) fn add_null_count(&mut self, source: usize, delta: i64) {
        if self.null_counts.len() <= source {
            self.null_counts.resize(source + 1, 0);
        }
        self.null_counts[source] = self.null_counts[source].wrapping_add(delta);
    }

    pub(super) fn null_count(&self, source: usize) -> i64 {
        self.null_counts.get(source).copied().unwrap_or(0)
    }

    pub(super) fn has_null_membership(&self) -> bool {
        self.null_counts.iter().any(|count| *count != 0)
    }

    pub(super) fn copy_if_reordered(&self) -> Option<Self> {
        let order = match &self.order {
            Order::Decimal(order) => Order::Decimal(order.copy_if_reordered()?),
            Order::Floating(order) => {
                let size = order.len();
                let required = if size <= 2 {
                    size + 1
                } else {
                    ((size as f32) / 0.75).ceil() as usize
                };
                let capacity = required.next_power_of_two().min(1 << 30);
                if order.copy_preserves_order(capacity) {
                    return None;
                }
                let mut copy = Self {
                    data_type: self.data_type.clone(),
                    null_counts: self.null_counts.clone(),
                    order: Order::Floating(JavaMapOrder::with_capacity(capacity)),
                };
                for value in self.iter() {
                    copy.insert(&value);
                }
                return Some(copy);
            }
        };
        Some(Self {
            data_type: self.data_type.clone(),
            null_counts: self.null_counts.clone(),
            order,
        })
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = ScalarValue> + '_ {
        let decimal = match &self.order {
            Order::Decimal(order) => Some(order.iter()),
            _ => None,
        };
        let floating = match &self.order {
            Order::Floating(order) => Some(order.iter()),
            _ => None,
        };
        decimal
            .into_iter()
            .flatten()
            .map(|value| {
                let DataType::Decimal128(precision, scale) = self.data_type else {
                    unreachable!()
                };
                ScalarValue::Decimal128(value, precision, scale)
            })
            .chain(
                floating
                    .into_iter()
                    .flatten()
                    .map(|value| match self.data_type {
                        DataType::Float32 => {
                            ScalarValue::Float32(value.map(|v| f32::from_bits(v.bits as u32)))
                        }
                        DataType::Float64 => {
                            ScalarValue::Float64(value.map(|v| f64::from_bits(v.bits)))
                        }
                        _ => unreachable!(),
                    }),
            )
    }

    pub(super) fn len(&self) -> usize {
        match &self.order {
            Order::Decimal(order) => order.len(),
            Order::Floating(order) => order.len(),
        }
    }

    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.null_counts.capacity() * std::mem::size_of::<i64>()
            + match &self.order {
                Order::Decimal(order) => order.bytes() - std::mem::size_of::<DecimalMapOrder>(),
                Order::Floating(order) => {
                    order.bytes() - std::mem::size_of::<JavaMapOrder<Option<FloatingKey>>>()
                }
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floating_views_match_jdk_hashmap_copy_resize_and_tree_order() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../src/test/resources/floating-map-order.json"
        ))
        .unwrap();
        for case in fixtures.as_array().unwrap() {
            let single = case["type"] == "FLOAT";
            let data_type = if single {
                DataType::Float32
            } else {
                DataType::Float64
            };
            let mut map = NumericMapOrder::new(&data_type);
            let keys = case["keys"].as_array().unwrap();
            for position in 0..=keys.len() {
                if case["copy_after"].as_i64().unwrap() == position as i64 {
                    if let Some(copied) = map.copy_if_reordered() {
                        map = copied;
                    }
                }
                if position < keys.len() {
                    let bits = keys[position]
                        .as_str()
                        .map(|bits| u64::from_str_radix(bits, 16).unwrap());
                    let value = if single {
                        ScalarValue::Float32(bits.map(|bits| f32::from_bits(bits as u32)))
                    } else {
                        ScalarValue::Float64(bits.map(f64::from_bits))
                    };
                    map.insert(&value);
                }
            }
            let actual: Vec<_> = map
                .iter()
                .map(|value| match value {
                    ScalarValue::Float32(value) => {
                        value.map(|value| format!("{:x}", value.to_bits()))
                    }
                    ScalarValue::Float64(value) => {
                        value.map(|value| format!("{:x}", value.to_bits()))
                    }
                    _ => unreachable!(),
                })
                .collect();
            let expected: Vec<_> = case["order"]
                .as_array()
                .unwrap()
                .iter()
                .map(|bits| bits.as_str().map(str::to_owned))
                .collect();
            assert_eq!(
                actual, expected,
                "{} copy_after={}",
                case["type"], case["copy_after"]
            );
            assert!(map.bytes() >= std::mem::size_of::<NumericMapOrder>());
        }
    }
}
