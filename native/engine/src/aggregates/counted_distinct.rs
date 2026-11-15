use super::*;
use crate::group_agg::DistinctSet;
use datafusion::common::cast::{as_int64_array, as_list_array};

pub(crate) struct CountedDistinctAccumulator {
    value_type: DataType,
    values: DistinctSet,
    count: i64,
}

impl std::fmt::Debug for CountedDistinctAccumulator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CountedDistinctAccumulator")
            .field("value_type", &self.value_type)
            .field("count", &self.count)
            .finish()
    }
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("counted DISTINCT: {message}"))
}

impl CountedDistinctAccumulator {
    pub(crate) fn new(value_type: &DataType) -> Self {
        Self {
            value_type: value_type.clone(),
            values: DistinctSet::new(value_type),
            count: 0,
        }
    }

    pub(crate) fn fields(value_type: &DataType) -> Vec<Field> {
        vec![
            Field::new(
                "distinct_values",
                DataType::List(Arc::new(Field::new("item", value_type.clone(), true))),
                false,
            ),
            Field::new(
                "distinct_counts",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                false,
            ),
            Field::new("distinct_cardinality", DataType::Int64, false),
        ]
    }

    pub(crate) fn entries(&self) -> Vec<(ScalarValue, i64)> {
        self.values.scalar_entries()
    }

    pub(crate) fn set_cardinality(&mut self, count: i64) {
        self.count = count;
    }

    pub(crate) fn import_entry(
        &mut self,
        value: ScalarValue,
        count: i64,
    ) -> Result<(), DataFusionError> {
        if value.is_null()
            || value.data_type() != self.value_type
            || count == 0
            || self.values.scalar_count(&value) != 0
        {
            return Err(invalid("invalid or duplicate membership counter"));
        }
        self.values.insert_restored(value, count);
        Ok(())
    }

    pub(crate) fn restore_exact(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.validate(states)?;
        if states[0].len() != 1 {
            return Err(invalid("snapshot must contain one accumulator"));
        }
        let values = as_list_array(&states[0])?.value(0);
        let counts = as_list_array(&states[1])?.value(0);
        let counts = as_int64_array(&counts)?;
        if values.len() != counts.len() || counts.null_count() != 0 {
            return Err(invalid("membership/count lengths differ"));
        }
        let mut restored = Self::new(&self.value_type);
        for row in 0..values.len() {
            restored.import_entry(
                ScalarValue::try_from_array(&values, row)?,
                counts.value(row),
            )?;
        }
        restored.count = as_int64_array(&states[2])?.value(0);
        *self = restored;
        Ok(())
    }

    fn validate(&self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        if states.len() != 3 {
            return Err(invalid("expected values, counters and cardinality"));
        }
        for (state, field) in states.iter().zip(Self::fields(&self.value_type)) {
            if state.len() != states[0].len()
                || state.data_type() != field.data_type()
                || state.null_count() != 0
            {
                return Err(invalid("invalid accumulator state schema"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

impl Accumulator for CountedDistinctAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        if !(1..=2).contains(&values.len()) || values[0].data_type() != &self.value_type {
            return Err(invalid("invalid input schema"));
        }
        let changes = values
            .get(1)
            .map(|changes| {
                changes
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .filter(|changes| {
                        changes.len() == values[0].len()
                            && changes.null_count() == 0
                            && changes.values().iter().all(|kind| (0..=3).contains(kind))
                    })
                    .ok_or_else(|| invalid("invalid row-kind sidecar"))
            })
            .transpose()?;
        for row in 0..values[0].len() {
            if values[0].is_null(row) {
                continue;
            }
            let value = ScalarValue::try_from_array(&values[0], row)?;
            let retract = changes.is_some_and(|changes| matches!(changes.value(row), 1 | 3));
            self.values
                .merge_scalar_delta(&value, if retract { -1 } else { 1 });
            let count = self.values.scalar_count(&value);
            if retract && count == 0 {
                self.count = self.count.wrapping_sub(1);
            } else if !retract && count == 1 {
                self.count = self.count.wrapping_add(1);
            }
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        if values.len() != 1 || values[0].data_type() != &self.value_type {
            return Err(invalid("invalid retraction schema"));
        }
        for row in 0..values[0].len() {
            if values[0].is_null(row) {
                continue;
            }
            let value = ScalarValue::try_from_array(&values[0], row)?;
            self.values.merge_scalar_delta(&value, -1);
            if self.values.scalar_count(&value) == 0 {
                self.count = self.count.wrapping_sub(1);
            }
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.validate(states)?;
        let values = as_list_array(&states[0])?;
        let counts = as_list_array(&states[1])?;
        for row in 0..values.len() {
            let values = values.value(row);
            let counters = counts.value(row);
            let counters = as_int64_array(&counters)?;
            if values.len() != counters.len() || counters.null_count() != 0 {
                return Err(invalid("invalid membership counters"));
            }
            for entry in 0..values.len() {
                let value = ScalarValue::try_from_array(&values, entry)?;
                if value.is_null() || counters.value(entry) == 0 {
                    return Err(invalid("empty membership"));
                }
                let before = self.values.scalar_count(&value);
                self.values
                    .merge_scalar_delta(&value, counters.value(entry));
                let after = self.values.scalar_count(&value);
                if before <= 0 && after > 0 {
                    self.count = self.count.wrapping_add(1);
                } else if before > 0 && after <= 0 {
                    self.count = self.count.wrapping_sub(1);
                }
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError> {
        let entries = self.entries();
        let values = entries
            .iter()
            .map(|(value, _)| value.clone())
            .collect::<Vec<_>>();
        let counts = entries
            .iter()
            .map(|(_, count)| ScalarValue::Int64(Some(*count)))
            .collect::<Vec<_>>();
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list_nullable(&values, &self.value_type)),
            ScalarValue::List(ScalarValue::new_list_nullable(&counts, &DataType::Int64)),
            ScalarValue::Int64(Some(self.count)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.values.capacity_bytes()
            + self
                .entries()
                .iter()
                .map(|(value, _)| value.size())
                .sum::<usize>()
    }
}
