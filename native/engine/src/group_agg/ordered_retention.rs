use crate::*;
use std::collections::{BTreeMap, VecDeque};
use std::hash::{Hash, Hasher};

#[derive(Clone)]
struct ValueKey(ScalarValue);

impl PartialEq for ValueKey {
    fn eq(&self, other: &Self) -> bool {
        crate::flink_float::boxed_equal(&self.0, &other.0)
    }
}
impl Eq for ValueKey {}
impl Hash for ValueKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0 {
            ScalarValue::Float32(Some(value)) => crate::flink_float::canonical_f32(*value)
                .to_bits()
                .hash(state),
            ScalarValue::Float64(Some(value)) => crate::flink_float::canonical_f64(*value)
                .to_bits()
                .hash(state),
            // Type buckets let borrowed Flink equality compare nested values without
            // allocating a canonical copy. Collisions affect lookup cost, not membership.
            ScalarValue::List(_)
            | ScalarValue::LargeList(_)
            | ScalarValue::FixedSizeList(_)
            | ScalarValue::Struct(_)
            | ScalarValue::Map(_) => self.0.data_type().hash(state),
            value => value.hash(state),
        }
    }
}

struct TimedOrders {
    orders: VecDeque<i64>,
    written: i64,
}
impl TimedOrders {
    fn bytes(&self, key: &ValueKey) -> usize {
        key.0.size() + self.orders.capacity() * std::mem::size_of::<i64>()
    }
}

struct TimedValues {
    values: VecDeque<ScalarValue>,
    written: i64,
    payload: usize,
}
impl TimedValues {
    fn bytes(&self) -> usize {
        self.payload
            + (self.values.capacity() - self.values.len()) * std::mem::size_of::<ScalarValue>()
    }
}

pub(super) struct RetainedOrderedValues {
    selected_order: Option<i64>,
    value_orders: HashMap<ValueKey, TimedOrders>,
    order_values: BTreeMap<i64, TimedValues>,
    entry_bytes: usize,
    last_sweep: i64,
}

impl RetainedOrderedValues {
    pub(super) fn new() -> Self {
        Self {
            selected_order: None,
            value_orders: HashMap::default(),
            order_values: BTreeMap::new(),
            entry_bytes: 0,
            last_sweep: 0,
        }
    }

    fn remove_value(&mut self, key: &ValueKey) {
        if let Some((key, orders)) = self.value_orders.remove_entry(key) {
            self.entry_bytes -= orders.bytes(&key);
        }
    }

    fn remove_order(&mut self, order: i64) {
        if let Some(values) = self.order_values.remove(&order) {
            self.entry_bytes -= values.bytes();
        }
    }

    fn sweep(&mut self, ttl: StateTtl) {
        let bytes = &mut self.entry_bytes;
        self.value_orders.retain(|key, orders| {
            if ttl.expired(orders.written) {
                *bytes -= orders.bytes(key);
                false
            } else {
                true
            }
        });
        self.order_values.retain(|_, values| {
            if ttl.expired(values.written) {
                *bytes -= values.bytes();
                false
            } else {
                true
            }
        });
        self.last_sweep = ttl.now();
    }

    pub(super) fn update(
        &mut self,
        selected: &mut ScalarValue,
        value: ScalarValue,
        retract: bool,
        last: bool,
        ttl: StateTtl,
    ) -> bool {
        if value.is_null() {
            return false;
        }
        if retract
            && matches!(
                value,
                ScalarValue::Binary(_)
                    | ScalarValue::LargeBinary(_)
                    | ScalarValue::FixedSizeBinary(..)
            )
        {
            return false;
        }
        if ttl.expired(self.last_sweep) {
            self.sweep(ttl);
        }
        let key = ValueKey(value.clone());
        if self
            .value_orders
            .get(&key)
            .is_some_and(|orders| ttl.expired(orders.written))
        {
            self.remove_value(&key);
        }
        if !retract {
            let orders = self.value_orders.entry(key.clone()).or_insert_with(|| {
                self.entry_bytes += key.0.size();
                TimedOrders {
                    orders: VecDeque::new(),
                    written: ttl.now(),
                }
            });
            let capacity = orders.orders.capacity();
            orders.orders.push_back(ttl.now());
            self.entry_bytes += (orders.orders.capacity() - capacity) * std::mem::size_of::<i64>();
            orders.written = ttl.now();
            if self.selected_order.is_none_or(|order| {
                if last {
                    order <= ttl.now()
                } else {
                    order > ttl.now()
                }
            }) {
                *selected = value.clone();
                self.selected_order = Some(ttl.now());
            }
            if self
                .order_values
                .get(&ttl.now())
                .is_some_and(|values| ttl.expired(values.written))
            {
                self.remove_order(ttl.now());
            }
            let values = self
                .order_values
                .entry(ttl.now())
                .or_insert_with(|| TimedValues {
                    values: VecDeque::new(),
                    written: ttl.now(),
                    payload: 0,
                });
            let before = values.bytes();
            values.payload += value.size();
            values.values.push_back(value);
            values.written = ttl.now();
            self.entry_bytes += values.bytes() - before;
            return true;
        }
        let Some(orders) = self.value_orders.get_mut(&key) else {
            return false;
        };
        let order = orders
            .orders
            .pop_front()
            .expect("retained order list is nonempty");
        if orders.orders.is_empty() {
            self.remove_value(&key);
        } else {
            orders.written = ttl.now();
        }
        if self
            .order_values
            .get(&order)
            .is_some_and(|values| ttl.expired(values.written))
        {
            self.remove_order(order);
        }
        let Some(values) = self.order_values.get_mut(&order) else {
            return false;
        };
        let before = values.bytes();
        if let Some(position) = values
            .values
            .iter()
            .position(|old| crate::flink_float::boxed_equal(old, &value))
        {
            values.payload -= values.values.remove(position).unwrap().size();
            values.written = ttl.now();
        }
        self.entry_bytes = self.entry_bytes - before + values.bytes();
        if values.values.is_empty() {
            self.remove_order(order);
        }
        if crate::flink_float::boxed_equal(&value, selected) {
            self.sweep(ttl);
            let bound = self
                .selected_order
                .expect("selected non-null value has an order");
            let next = if last {
                self.order_values.range(..=bound).next_back()
            } else {
                self.order_values.range(bound..).next()
            };
            if let Some((&order, values)) = next {
                *selected = if last {
                    values.values.back()
                } else {
                    values.values.front()
                }
                .unwrap()
                .clone();
                self.selected_order = Some(order);
            } else {
                *selected = null_scalar(&selected.data_type());
                self.selected_order = None;
            }
        }
        true
    }

    pub(super) fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.entry_bytes
            + self.value_orders.capacity() * (std::mem::size_of::<(ValueKey, TimedOrders)>() + 16)
            + self.order_values.len() * (std::mem::size_of::<(i64, TimedValues)>() + 64)
    }

    pub(super) fn snapshot(&self, data_type: &DataType) -> Vec<u8> {
        let header = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "selected_order",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![self.selected_order]))],
        )
        .unwrap();
        let mut keys = Vec::new();
        let mut orders = Vec::new();
        let mut timestamps = Vec::new();
        for (key, entry) in &self.value_orders {
            for &order in &entry.orders {
                keys.push(key.0.clone());
                orders.push(order);
                timestamps.push(entry.written);
            }
        }
        let values = retained_batch(data_type, keys, orders, timestamps);
        let mut keys = Vec::new();
        let mut orders = Vec::new();
        let mut timestamps = Vec::new();
        for (&order, entry) in &self.order_values {
            for value in &entry.values {
                keys.push(value.clone());
                orders.push(order);
                timestamps.push(entry.written);
            }
        }
        write_framed(&[
            header,
            values,
            retained_batch(data_type, keys, orders, timestamps),
        ])
    }

    pub(super) fn restore(bytes: &[u8]) -> Self {
        let batches = read_framed(bytes);
        assert_eq!(batches.len(), 3, "ordered retention snapshot");
        let mut state = Self::new();
        let selected = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        state.selected_order = (!selected.is_null(0)).then(|| selected.value(0));
        for (index, batch) in batches.iter().enumerate().skip(1) {
            let orders = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let timestamps = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let value = ScalarValue::try_from_array(batch.column(0), row).unwrap();
                if index == 1 {
                    state
                        .value_orders
                        .entry(ValueKey(value))
                        .or_insert_with(|| TimedOrders {
                            orders: VecDeque::new(),
                            written: timestamps.value(row),
                        })
                        .orders
                        .push_back(orders.value(row));
                } else {
                    let entry = state
                        .order_values
                        .entry(orders.value(row))
                        .or_insert_with(|| TimedValues {
                            values: VecDeque::new(),
                            written: timestamps.value(row),
                            payload: 0,
                        });
                    entry.payload += value.size();
                    entry.values.push_back(value);
                }
            }
        }
        state.entry_bytes = state
            .value_orders
            .iter()
            .map(|(key, entry)| entry.bytes(key))
            .sum::<usize>()
            + state
                .order_values
                .values()
                .map(TimedValues::bytes)
                .sum::<usize>();
        state
    }
}

fn retained_batch(
    data_type: &DataType,
    values: Vec<ScalarValue>,
    orders: Vec<i64>,
    timestamps: Vec<i64>,
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("value", data_type.clone(), true),
            Field::new("order", DataType::Int64, false),
            Field::new("written", DataType::Int64, false),
        ])),
        vec![
            scalars_to_array(values, data_type),
            Arc::new(Int64Array::from(orders)),
            Arc::new(Int64Array::from(timestamps)),
        ],
    )
    .unwrap()
}
