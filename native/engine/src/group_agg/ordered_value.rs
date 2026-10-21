use super::DistinctSet;
use crate::*;
use std::collections::VecDeque;

pub(super) const SINGLE_VALUE_ERROR: &str =
    "SingleValueAggFunction received more than one element.";

pub(super) fn is_ordered_value(kind: i64) -> bool {
    matches!(kind, 12..=16 | 19..=21)
}

pub(super) struct OrderedValueColumn<'a> {
    column: &'a ArrayRef,
    timestamp: Option<streamfusion_bridge::timestamp::TimestampColumn<'a>>,
}

impl<'a> OrderedValueColumn<'a> {
    pub(super) fn new(column: &'a ArrayRef) -> Self {
        let timestamp = streamfusion_bridge::timestamp::is_component_timestamp(column.data_type())
            .then(|| {
                streamfusion_bridge::timestamp::TimestampColumn::try_new(column.as_ref())
                    .expect("ordered timestamp column")
            });
        Self { column, timestamp }
    }

    pub(super) fn is_null(&self, row: usize) -> bool {
        self.column.is_null(row)
    }

    pub(super) fn value(&self, row: usize) -> Result<ScalarValue, DataFusionError> {
        if let Some(values) = &self.timestamp {
            return Ok(ScalarValue::Decimal128(
                (!self.column.is_null(row))
                    .then(|| values.value(row).expect("timestamp value").nanos()),
                38,
                0,
            ));
        }
        ScalarValue::try_from_array(self.column, row)
    }
}

pub(super) fn accumulator_type(kind: i64, value_type: &DataType) -> DataType {
    if is_ordered_value(kind) && streamfusion_bridge::timestamp::is_component_timestamp(value_type)
    {
        DataType::Decimal128(38, 0)
    } else {
        value_type.clone()
    }
}

fn compact_value(value: ScalarValue) -> ScalarValue {
    if let ScalarValue::Struct(array) = &value {
        if streamfusion_bridge::timestamp::is_component_timestamp(array.data_type()) {
            let values = streamfusion_bridge::timestamp::TimestampColumn::try_new(array.as_ref())
                .expect("ordered timestamp scalar");
            return ScalarValue::Decimal128(
                (!array.is_null(0)).then(|| values.value(0).expect("timestamp value").nanos()),
                38,
                0,
            );
        }
    }
    value
}

/// Append-only first/last retain one scalar; retractable first/last retain arrival order.
/// SINGLE_VALUE counts every element, including NULL, and can fail during accumulation.
pub(crate) struct OrderedValueState {
    kind: i64,
    value: ScalarValue,
    count: i32,
    values: VecDeque<ScalarValue>,
    value_bytes: usize,
    distinct: Option<Box<DistinctSet>>,
}

impl OrderedValueState {
    pub(super) fn new(kind: i64, value_type: &DataType) -> Self {
        Self {
            kind,
            value: if streamfusion_bridge::timestamp::is_component_timestamp(value_type) {
                ScalarValue::Decimal128(None, 38, 0)
            } else {
                null_scalar(value_type)
            },
            count: 0,
            values: VecDeque::new(),
            value_bytes: 0,
            distinct: matches!(kind, 19 | 20)
                .then(|| Box::new(DistinctSet::new(&accumulator_type(kind, value_type)))),
        }
    }

    pub(super) fn skips_value(&self, is_null: bool) -> bool {
        self.kind == 12 && !self.value.is_null() || is_null && !matches!(self.kind, 14 | 21)
    }

    /// False guarantees that the selected value and its payload are unchanged.
    pub(super) fn update(
        &mut self,
        value: ScalarValue,
        retract: bool,
    ) -> Result<bool, DataFusionError> {
        let value = compact_value(value);
        if let Some(counts) = &mut self.distinct {
            if value.is_null() {
                return Ok(false);
            }
            if retract {
                if !counts.remove_scalar(&value) {
                    return Ok(false);
                }
                let index = self
                    .values
                    .iter()
                    .position(|existing| crate::flink_float::boxed_equal(existing, &value))
                    .expect("distinct membership retains arrival order");
                self.value_bytes -= self.values.remove(index).unwrap().size();
            } else {
                if counts.add_scalar_ref(&value) {
                    self.value_bytes += value.size();
                    self.values.push_back(value);
                } else {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        if matches!(self.kind, 14 | 21) {
            if (retract && !matches!(self.count, 0 | 1)) || (!retract && self.count > 0) {
                return Err(DataFusionError::Execution(SINGLE_VALUE_ERROR.into()));
            }
            if retract {
                if self.kind != 21 {
                    self.value = null_scalar(&self.value.data_type());
                }
                self.count = self.count.wrapping_sub(1);
            } else {
                if self.kind != 21 {
                    self.value = value;
                }
                self.count = self.count.wrapping_add(1);
            }
        } else if !value.is_null() {
            match self.kind {
                12 | 13 => {
                    assert!(!retract, "append-only first/last cannot retract");
                    if self.kind == 13 || self.value.is_null() {
                        self.value = value;
                    }
                }
                15 | 16 => {
                    if retract {
                        // Released generic binary MapView keys use byte-array identity.
                        if matches!(
                            value,
                            ScalarValue::Binary(_)
                                | ScalarValue::LargeBinary(_)
                                | ScalarValue::FixedSizeBinary(..)
                        ) {
                            return Ok(false);
                        }
                        if let Some(index) = self
                            .values
                            .iter()
                            .position(|existing| crate::flink_float::boxed_equal(existing, &value))
                        {
                            self.value_bytes -= self.values.remove(index).unwrap().size();
                        }
                    } else {
                        self.append_restored(value);
                    }
                }
                _ => unreachable!("ordered aggregate kind"),
            }
        }
        Ok(true)
    }

    pub(super) fn merge_single(
        &mut self,
        value: ScalarValue,
        count: i32,
    ) -> Result<(), DataFusionError> {
        let value = compact_value(value);
        let total = self.count.wrapping_add(count);
        if total > 1 {
            return Err(DataFusionError::Execution(SINGLE_VALUE_ERROR.into()));
        }
        self.value = if total == 0 && self.count != 0 && count <= 0 {
            null_scalar(&self.value.data_type())
        } else {
            value
        };
        self.count = total;
        Ok(())
    }

    pub(super) fn emit(&self) -> ScalarValue {
        match self.kind {
            21 => ScalarValue::Int32(Some(self.count)),
            15 | 19 => self.values.front().unwrap_or(&self.value).clone(),
            16 | 20 => self.values.back().unwrap_or(&self.value).clone(),
            _ => self.value.clone(),
        }
    }

    pub(super) fn snapshot_value(&self) -> ScalarValue {
        self.value.clone()
    }

    pub(super) fn count(&self) -> i64 {
        self.count as i64
    }

    pub(super) fn restore_value(&mut self, value: ScalarValue, count: i64) {
        self.value = compact_value(value);
        self.count = count as i32;
    }

    pub(super) fn entries(&self) -> impl Iterator<Item = (&ScalarValue, i64)> {
        self.values.iter().map(|value| {
            (
                value,
                self.distinct
                    .as_ref()
                    .map_or(1, |counts| counts.scalar_count(value)),
            )
        })
    }

    pub(super) fn restore_entry(&mut self, value: ScalarValue, count: i64) {
        let value = compact_value(value);
        if let Some(counts) = &mut self.distinct {
            counts.insert_restored(value.clone(), count);
        }
        self.append_restored(value);
    }

    pub(super) fn append_restored(&mut self, value: ScalarValue) {
        let value = compact_value(value);
        self.value_bytes += value.size();
        self.values.push_back(value);
    }

    pub(super) fn bytes(&self) -> usize {
        let distinct_bytes = self.distinct.as_ref().map_or(0, |counts| {
            let payload = self.value_bytes - self.values.len() * std::mem::size_of::<ScalarValue>();
            std::mem::size_of_val(counts.as_ref()) + payload + counts.capacity_bytes()
        });
        distinct_bytes
            + std::mem::size_of::<Self>()
            + self.value.size()
            + self.value_bytes
            + (self.values.capacity() - self.values.len()) * std::mem::size_of::<ScalarValue>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_floating_order_uses_boxed_membership_and_original_payloads() {
        for values in [
            vec![
                ScalarValue::Float32(Some(-0.0)),
                ScalarValue::Float32(Some(0.0)),
                ScalarValue::Float32(Some(f32::from_bits(0x7fc00001))),
                ScalarValue::Float32(Some(f32::from_bits(0xffc00002))),
            ],
            vec![
                ScalarValue::Float64(Some(-0.0)),
                ScalarValue::Float64(Some(0.0)),
                ScalarValue::Float64(Some(f64::from_bits(0x7ff8000000000001))),
                ScalarValue::Float64(Some(f64::from_bits(0xfff8000000000002))),
            ],
        ] {
            let bits = |value: &ScalarValue| match value {
                ScalarValue::Float32(Some(value)) => u64::from(value.to_bits()),
                ScalarValue::Float64(Some(value)) => value.to_bits(),
                _ => unreachable!(),
            };
            for kind in [19, 20] {
                let mut state = OrderedValueState::new(kind, &values[0].data_type());
                for value in [&values[0], &values[1], &values[0], &values[2], &values[3]] {
                    state.update(value.clone(), false).unwrap();
                }
                assert_eq!(state.values.len(), 3);
                let selected = if kind == 19 { &values[0] } else { &values[2] };
                assert_eq!(bits(&state.emit()), bits(selected));
                let mut restored = OrderedValueState::new(kind, &values[0].data_type());
                for (value, count) in state.entries() {
                    restored.restore_entry(value.clone(), count);
                }
                assert_eq!(bits(&restored.emit()), bits(selected));
                for state in [&mut state, &mut restored] {
                    state.update(values[3].clone(), true).unwrap();
                    assert_eq!(state.values.len(), 3);
                    state.update(values[2].clone(), true).unwrap();
                    assert_eq!(state.values.len(), 2);
                    state.update(values[0].clone(), true).unwrap();
                    assert_eq!(state.values.len(), 2);
                    state.update(values[0].clone(), true).unwrap();
                    assert_eq!(bits(&state.emit()), bits(&values[1]));
                    state.update(values[1].clone(), true).unwrap();
                    assert!(state.emit().is_null());
                }
            }
        }
    }

    #[test]
    fn distinct_timestamp_membership_survives_arrow_checkpoint_values() {
        use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
        let input: ArrayRef = Arc::new(timestamp_array([
            Some(TimestampValue::new(-1, 999999).unwrap()),
            Some(TimestampValue::new(0, 1).unwrap()),
        ]));
        for kind in [19, 20] {
            let column = OrderedValueColumn::new(&input);
            let mut state = OrderedValueState::new(kind, input.data_type());
            for row in [0, 1, 0] {
                state.update(column.value(row).unwrap(), false).unwrap();
            }
            let entries: Vec<_> = state.entries().collect();
            assert_eq!(
                entries.iter().map(|(_, count)| *count).collect::<Vec<_>>(),
                vec![2, 1]
            );
            let wire = scalars_to_array(
                entries.iter().map(|(v, _)| (*v).clone()).collect(),
                input.data_type(),
            );
            assert_eq!(wire.data_type(), input.data_type());
            let mut restored = OrderedValueState::new(kind, input.data_type());
            for (row, (_, count)) in entries.into_iter().enumerate() {
                restored.restore_entry(ScalarValue::try_from_array(&wire, row).unwrap(), count);
            }
            for state in [&mut state, &mut restored] {
                state.update(column.value(0).unwrap(), true).unwrap();
                assert_eq!(
                    state.entries().map(|(_, count)| count).collect::<Vec<_>>(),
                    vec![1, 1]
                );
                state.update(column.value(0).unwrap(), true).unwrap();
                assert_eq!(state.emit(), column.value(1).unwrap());
                state.update(column.value(0).unwrap(), false).unwrap();
                assert_eq!(
                    state.emit(),
                    column.value(if kind == 19 { 1 } else { 0 }).unwrap()
                );
            }
        }
    }

    #[test]
    fn timestamp_values_remain_inline_until_arrow_output_and_restore() {
        use streamfusion_bridge::timestamp::{timestamp_array, TimestampColumn, TimestampValue};
        let values: ArrayRef = Arc::new(timestamp_array([
            Some(TimestampValue::new(-1, 999999).unwrap()),
            None,
        ]));
        let mut state = OrderedValueState::new(14, values.data_type());
        assert!(matches!(state.value, ScalarValue::Decimal128(None, 38, 0)));
        state
            .update(OrderedValueColumn::new(&values).value(0).unwrap(), false)
            .unwrap();
        assert!(matches!(
            state.value,
            ScalarValue::Decimal128(Some(-1), 38, 0)
        ));
        let output = scalars_to_array(vec![state.emit()], values.data_type());
        assert_eq!(
            TimestampColumn::try_new(output.as_ref())
                .unwrap()
                .value(0)
                .unwrap(),
            TimestampValue::new(-1, 999999).unwrap()
        );
        let mut restored = OrderedValueState::new(14, values.data_type());
        restored.restore_value(ScalarValue::try_from_array(&output, 0).unwrap(), 1);
        assert!(matches!(
            restored.value,
            ScalarValue::Decimal128(Some(-1), 38, 0)
        ));
        restored
            .update(OrderedValueColumn::new(&values).value(0).unwrap(), true)
            .unwrap();
        restored
            .update(OrderedValueColumn::new(&values).value(1).unwrap(), false)
            .unwrap();
        assert!(restored.emit().is_null());
        assert!(restored
            .update(OrderedValueColumn::new(&values).value(0).unwrap(), false)
            .is_err());

        let mut count = OrderedValueState::new(21, values.data_type());
        count.update(ScalarValue::Null, false).unwrap();
        assert_eq!(count.emit(), ScalarValue::Int32(Some(1)));
        assert!(matches!(count.value, ScalarValue::Decimal128(None, 38, 0)));
    }

    #[test]
    fn duplicate_retractions_preserve_arrival_order() {
        for kind in [15, 16] {
            let mut state = OrderedValueState::new(kind, &DataType::Utf8);
            let scalar = |s: &str| ScalarValue::Utf8(Some(s.to_owned()));
            for value in ["a", "b", "a", "c"] {
                state.update(scalar(value), false).unwrap();
            }
            state.update(ScalarValue::Utf8(None), false).unwrap();
            state.update(scalar("a"), true).unwrap();
            assert_eq!(state.emit(), scalar(if kind == 15 { "b" } else { "c" }));
            state.update(scalar("c"), true).unwrap();
            assert_eq!(state.emit(), scalar(if kind == 15 { "b" } else { "a" }));
            state.update(scalar("a"), true).unwrap();
            state.update(scalar("absent"), true).unwrap();
            assert_eq!(state.emit(), scalar("b"));
            state.update(scalar("b"), true).unwrap();
            assert!(state.emit().is_null());
            assert_eq!(state.value_bytes, 0);
        }
    }

    #[test]
    fn single_value_merge_preserves_zero_and_negative_count_rules() {
        let value = |s: &str| ScalarValue::Utf8(Some(s.into()));
        let mut state = OrderedValueState::new(14, &DataType::Utf8);
        state.merge_single(value("zero"), 0).unwrap();
        assert_eq!(state.emit(), value("zero"));
        state.merge_single(value("one"), 1).unwrap();
        state.merge_single(ScalarValue::Utf8(None), 0).unwrap();
        assert!(state.emit().is_null());
        state.merge_single(value("removed"), -1).unwrap();
        assert!(state.emit().is_null());
        state.merge_single(value("negative"), -1).unwrap();
        state.merge_single(value("replacement"), 1).unwrap();
        assert_eq!(state.count(), 0);
        assert_eq!(state.emit(), value("replacement"));
    }

    #[test]
    fn single_value_counts_null_and_preserves_retraction_underflow() {
        let mut state = OrderedValueState::new(14, &DataType::Int64);
        state.update(ScalarValue::Int64(None), false).unwrap();
        assert_eq!(state.count(), 1);
        assert!(state.update(ScalarValue::Int64(None), false).is_err());
        state.update(ScalarValue::Int64(None), true).unwrap();
        state.update(ScalarValue::Int64(None), true).unwrap();
        assert_eq!(state.count(), -1);
        assert!(state.update(ScalarValue::Int64(None), true).is_err());
        state.update(ScalarValue::Int64(Some(7)), false).unwrap();
        assert_eq!(state.count(), 0);
        assert_eq!(state.emit(), ScalarValue::Int64(Some(7)));
        state.update(ScalarValue::Int64(Some(8)), false).unwrap();
        assert_eq!(state.count(), 1);
        assert_eq!(state.emit(), ScalarValue::Int64(Some(8)));
    }
}
