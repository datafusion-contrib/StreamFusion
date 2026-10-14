use crate::*;

/// The same scalar COUNT state as DataFusion, with an explicit partial-count merge.
#[derive(Debug, Default)]
pub(crate) struct WindowCountAccumulator {
    count: i64,
}

impl WindowCountAccumulator {
    pub(crate) fn count(&self) -> i64 {
        self.count
    }

    pub(crate) fn merge_count(&mut self, count: i64) {
        self.count += count;
    }
}

impl Accumulator for WindowCountAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let nulls = values
            .iter()
            .map(|value| value.logical_nulls())
            .fold(None, |total, next| {
                arrow::buffer::NullBuffer::union(total.as_ref(), next.as_ref())
            });
        self.merge_count((values[0].len() - nulls.map_or(0, |nulls| nulls.null_count())) as i64);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let counts = states[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("COUNT partial");
        for count in counts.iter().flatten() {
            self.merge_count(count);
        }
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_count_matches_datafusion_nulls_partials_and_state() {
        let mut reference = crate::aggregates::build_aggregates(&[3], &[0])
            .remove(0)
            .create_accumulator();
        let mut scalar = WindowCountAccumulator::default();
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), None, Some(7), Some(9)]));
        let kinds: ArrayRef = Arc::new(Int8Array::from(vec![Some(0), Some(0), None, Some(0)]));
        for arrays in [vec![values.clone()], vec![values, kinds]] {
            reference.update_batch(&arrays).unwrap();
            scalar.update_batch(&arrays).unwrap();
            assert_eq!(reference.state().unwrap(), scalar.state().unwrap());
        }
        let partial: ArrayRef = Arc::new(Int64Array::from(vec![Some(5), None, Some(2)]));
        reference
            .merge_batch(std::slice::from_ref(&partial))
            .unwrap();
        scalar.merge_count(7);
        assert_eq!(reference.evaluate().unwrap(), scalar.evaluate().unwrap());
        assert_eq!(reference.size(), scalar.size());
    }
}
