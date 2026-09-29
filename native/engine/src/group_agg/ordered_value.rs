use crate::*;
use std::collections::VecDeque;

pub(super) const SINGLE_VALUE_ERROR: &str =
    "SingleValueAggFunction received more than one element.";

pub(super) fn is_ordered_value(kind: i64) -> bool {
    matches!(kind, 12..=16 | 19 | 20)
}

/// Append-only first/last retain one scalar; retractable first/last retain arrival order.
/// SINGLE_VALUE counts every element, including NULL, and can fail during accumulation.
pub(crate) struct OrderedValueState {
    kind: i64,
    value: ScalarValue,
    count: i32,
    values: VecDeque<ScalarValue>,
    value_bytes: usize,
    distinct: Option<Box<ahash::HashMap<ScalarValue, i64>>>,
}

impl OrderedValueState {
    pub(super) fn new(kind: i64, value_type: &DataType) -> Self {
        Self {
            kind,
            value: null_scalar(value_type),
            count: 0,
            values: VecDeque::new(),
            value_bytes: 0,
            distinct: matches!(kind, 19 | 20).then(Box::default),
        }
    }

    pub(super) fn update(
        &mut self,
        value: ScalarValue,
        retract: bool,
    ) -> Result<(), DataFusionError> {
        if let Some(counts) = &mut self.distinct {
            if value.is_null() {
                return Ok(());
            }
            if retract {
                let Some(count) = counts.get_mut(&value) else {
                    return Ok(());
                };
                *count -= 1;
                if *count != 0 {
                    return Ok(());
                }
                counts.remove(&value);
                let index = self
                    .values
                    .iter()
                    .position(|existing| *existing == value)
                    .expect("distinct membership retains arrival order");
                self.value_bytes -= self.values.remove(index).unwrap().size();
            } else {
                let count = counts.entry(value.clone()).or_insert(0);
                *count += 1;
                if *count == 1 {
                    self.value_bytes += value.size();
                    self.values.push_back(value);
                }
            }
            return Ok(());
        }
        if self.kind == 14 {
            if (retract && !matches!(self.count, 0 | 1)) || (!retract && self.count > 0) {
                return Err(DataFusionError::Execution(SINGLE_VALUE_ERROR.into()));
            }
            if retract {
                self.value = null_scalar(&self.value.data_type());
                self.count = self.count.wrapping_sub(1);
            } else {
                self.value = value;
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
                        if let Some(index) =
                            self.values.iter().position(|existing| *existing == value)
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
        Ok(())
    }

    pub(super) fn emit(&self) -> ScalarValue {
        match self.kind {
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
        self.value = value;
        self.count = count as i32;
    }

    pub(super) fn entries(&self) -> impl Iterator<Item = (&ScalarValue, i64)> {
        self.values.iter().map(|value| {
            (
                value,
                self.distinct.as_ref().map_or(1, |counts| counts[value]),
            )
        })
    }

    pub(super) fn restore_entry(&mut self, value: ScalarValue, count: i64) {
        if let Some(counts) = &mut self.distinct {
            counts.insert(value.clone(), count);
        }
        self.append_restored(value);
    }

    pub(super) fn append_restored(&mut self, value: ScalarValue) {
        self.value_bytes += value.size();
        self.values.push_back(value);
    }

    pub(super) fn bytes(&self) -> usize {
        let distinct_bytes = self.distinct.as_ref().map_or(0, |counts| {
            std::mem::size_of_val(counts.as_ref())
                + self.value_bytes
                + counts.len() * std::mem::size_of::<i64>()
                + (counts.capacity() - counts.len()) * std::mem::size_of::<(ScalarValue, i64)>()
                + counts.capacity()
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
