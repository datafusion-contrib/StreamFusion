use super::*;
use crate::group_agg::MappedExtreme;

pub(crate) const RETRACT_MIN: i64 = 23;
pub(crate) const RETRACT_MAX: i64 = 24;

pub(crate) struct RetractingExtremeAccumulator {
    state: MappedExtreme,
    minimum: bool,
    value_type: DataType,
}

impl std::fmt::Debug for RetractingExtremeAccumulator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetractingExtremeAccumulator")
            .field("minimum", &self.minimum)
            .field("value_type", &self.value_type)
            .finish_non_exhaustive()
    }
}

impl RetractingExtremeAccumulator {
    pub(crate) fn merge_combined(&mut self, other: &MappedExtreme) {
        self.state.merge(other, StateTtl::new(0, 0), 0);
    }

    pub(super) fn new(minimum: bool, value_type: &DataType) -> Self {
        Self {
            state: MappedExtreme::new(minimum),
            minimum,
            value_type: value_type.clone(),
        }
    }
}

impl Accumulator for RetractingExtremeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let changes = values
            .get(1)
            .map(|kinds| kinds.as_any().downcast_ref::<Int8Array>().unwrap());
        for row in 0..values[0].len() {
            if !values[0].is_null(row) {
                let value = ScalarValue::try_from_array(&values[0], row)?;
                self.state.update(
                    value,
                    changes.is_some_and(|kinds| matches!(kinds.value(row), 1 | 3)),
                );
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        for row in 0..states[0].len() {
            let incoming = MappedExtreme::from_partial(&states[0], row, self.minimum);
            self.state.merge(&incoming, StateTtl::new(0, 0), 0);
        }
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![self.state.partial(&self.value_type)])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(self.state.emit(&self.value_type))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.state.bytes() - std::mem::size_of::<MappedExtreme>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(values: Vec<Option<i64>>, changes: Vec<i8>) -> Vec<ArrayRef> {
        vec![
            Arc::new(Int64Array::from(values)),
            Arc::new(Int8Array::from(changes)),
        ]
    }

    #[test]
    fn signed_partials_preserve_cached_extremes_negative_entries_and_restore() {
        for minimum in [false, true] {
            let (pending, winner, survivor) = if minimum { (1, 3, 5) } else { (9, 7, 5) };
            let mut local = RetractingExtremeAccumulator::new(minimum, &DataType::Int64);
            local
                .update_batch(&input(
                    vec![Some(pending), Some(winner), Some(survivor), None],
                    vec![3, 0, 0, 0],
                ))
                .unwrap();
            let partials: Vec<_> = local
                .state()
                .unwrap()
                .into_iter()
                .map(|value| value.to_array().unwrap())
                .collect();
            let mut global = RetractingExtremeAccumulator::new(minimum, &DataType::Int64);
            global.merge_batch(&partials).unwrap();
            assert_eq!(global.evaluate().unwrap(), ScalarValue::Int64(Some(winner)));
            global
                .update_batch(&input(vec![Some(winner)], vec![3]))
                .unwrap();
            assert_eq!(
                global.evaluate().unwrap(),
                ScalarValue::Int64(Some(pending))
            );
            let snapshot: Vec<_> = global
                .state()
                .unwrap()
                .into_iter()
                .map(|value| value.to_array().unwrap())
                .collect();
            let mut restored = RetractingExtremeAccumulator::new(minimum, &DataType::Int64);
            restored.merge_batch(&snapshot).unwrap();
            assert_eq!(restored.evaluate().unwrap(), global.evaluate().unwrap());
            restored
                .update_batch(&input(vec![Some(survivor)], vec![3]))
                .unwrap();
            assert_eq!(restored.evaluate().unwrap(), ScalarValue::Int64(None));
        }
    }

    #[test]
    fn structured_string_partials_keep_duplicates_nulls_and_unicode() {
        let mut local = RetractingExtremeAccumulator::new(true, &DataType::Utf8);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("é🚀"),
            Some("a"),
            Some("a"),
            None,
        ]));
        local.update_batch(&[values]).unwrap();
        let mut global = RetractingExtremeAccumulator::new(true, &DataType::Utf8);
        let state = local.state().unwrap()[0].to_array().unwrap();
        global.merge_batch(&[state]).unwrap();
        let value: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
        let kinds: ArrayRef = Arc::new(Int8Array::from(vec![3]));
        global
            .update_batch(&[value.clone(), kinds.clone()])
            .unwrap();
        assert_eq!(
            global.evaluate().unwrap(),
            ScalarValue::Utf8(Some("a".into()))
        );
        global.update_batch(&[value, kinds]).unwrap();
        assert_eq!(
            global.evaluate().unwrap(),
            ScalarValue::Utf8(Some("é🚀".into()))
        );
    }
}
