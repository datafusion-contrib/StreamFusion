use super::*;

/// Flink's nullable SUM buffer assigns the first value and then adds in arrival order.
#[derive(Debug)]
pub(super) struct FloatingSumAccumulator<T: arrow::datatypes::ArrowPrimitiveType> {
    sum: Option<T::Native>,
    scalar: fn(Option<T::Native>) -> ScalarValue,
}

impl<T: arrow::datatypes::ArrowPrimitiveType> FloatingSumAccumulator<T> {
    pub(super) fn new(scalar: fn(Option<T::Native>) -> ScalarValue) -> Self {
        Self { sum: None, scalar }
    }
}

impl<T: arrow::datatypes::ArrowPrimitiveType> Accumulator for FloatingSumAccumulator<T>
where
    T: std::fmt::Debug,
    T::Native: std::ops::Add<Output = T::Native>,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let values = values[0]
            .as_any()
            .downcast_ref::<arrow::array::PrimitiveArray<T>>()
            .expect("floating SUM value type");
        for value in values.iter().flatten() {
            self.sum = Some(self.sum.map_or(value, |sum| sum + value));
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![(self.scalar)(self.sum)])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok((self.scalar)(self.sum))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bits(value: ScalarValue) -> Option<u64> {
        match value {
            ScalarValue::Float32(value) => value.map(|value| u64::from(value.to_bits())),
            ScalarValue::Float64(value) => value.map(f64::to_bits),
            other => panic!("floating SUM value: {other:?}"),
        }
    }

    #[test]
    fn nullable_first_values_survive_partial_merge_and_checkpoint_rebuild() {
        for data_type in [DataType::Float32, DataType::Float64] {
            let aggregate = WindowAggregate::new(0, &data_type);
            assert!(!aggregate.state_requires_restore());
            for initial in [
                None,
                Some(-0.0),
                Some(0.0),
                Some(f64::NEG_INFINITY),
                Some(f64::from_bits(0x7ff8_0000_0000_0042)),
            ] {
                let input = arrow::compute::cast(
                    &Float64Array::from(vec![None, initial, None]),
                    &data_type,
                )
                .unwrap();
                let expected = bits(ScalarValue::try_from_array(&input, 1).unwrap());
                let mut live = aggregate.create_accumulator();
                live.update_batch(&[input]).unwrap();
                for _ in 0..3 {
                    assert_eq!(bits(live.evaluate().unwrap()), expected);
                    let state =
                        snapshot_accumulator_state(&mut live, || aggregate.create_accumulator());
                    assert_eq!(bits(state[0].clone()), expected);
                    let mut restored = aggregate.create_accumulator();
                    restore_accumulator_state(&mut restored, &[state[0].to_array().unwrap()])
                        .unwrap();
                    assert_eq!(bits(restored.evaluate().unwrap()), expected);
                    live = restored;
                }
            }
        }
    }

    #[test]
    fn physical_batch_boundaries_do_not_reorder_the_nullable_fold() {
        for (data_type, large) in [
            (DataType::Float32, 16_777_216.0),
            (DataType::Float64, 9_007_199_254_740_992.0),
        ] {
            let input = arrow::compute::cast(
                &Float64Array::from(vec![None, Some(large), Some(1.0), Some(-large), None]),
                &data_type,
            )
            .unwrap();
            for single in [false, true] {
                let mut live = WindowAggregate::new(0, &data_type).create_accumulator();
                if single {
                    for row in 0..input.len() {
                        live.update_batch(&[input.slice(row, 1)]).unwrap();
                    }
                } else {
                    live.update_batch(&[input.clone()]).unwrap();
                }
                assert_eq!(bits(live.evaluate().unwrap()), Some(0));
            }
        }
    }
}
