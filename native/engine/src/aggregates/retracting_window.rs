use super::decimal_sum_add;
use crate::*;

pub(crate) const RETRACT_SUM: i64 = crate::expression_protocol::window_aggregate_kind::RETRACT_SUM;
pub(crate) const RETRACT_COUNT: i64 =
    crate::expression_protocol::window_aggregate_kind::RETRACT_COUNT;
pub(crate) const LIVE_COUNT: i64 = crate::expression_protocol::window_aggregate_kind::LIVE_COUNT;
pub(crate) const HIDDEN_LIVE_COUNT: i64 =
    crate::expression_protocol::window_aggregate_kind::HIDDEN_LIVE_COUNT;

#[derive(Debug)]
pub(super) struct RetractingDecimalSumAccumulator {
    sum: DecimalSumAccumulator,
    count: i64,
}

impl RetractingDecimalSumAccumulator {
    pub(super) fn new(scale: i8) -> Self {
        Self {
            sum: DecimalSumAccumulator { sum: None, scale },
            count: 0,
        }
    }
}

impl Accumulator for RetractingDecimalSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let input = values[0]
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal SUM value type");
        let changes = values
            .get(1)
            .map(|kinds| kinds.as_any().downcast_ref::<Int8Array>().unwrap());
        for (row, value) in input.iter().enumerate() {
            if let Some(value) = value {
                let retract = changes.is_some_and(|kinds| matches!(kinds.value(row), 1 | 3));
                self.sum.sum = decimal_sum_add(self.sum.sum, if retract { -value } else { value });
                self.count = self.count.wrapping_add(if retract { -1 } else { 1 });
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        self.sum.merge_batch(&states[..1])?;
        for count in states[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
        {
            self.count = self.count.wrapping_add(*count);
        }
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![
            self.sum.evaluate()?,
            ScalarValue::Int64(Some(self.count)),
        ])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(ScalarValue::Decimal128(
            if self.count == 0 { None } else { self.sum.sum },
            38,
            self.sum.scale,
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[derive(Debug)]
pub(super) struct RetractingFloatingSumAccumulator<T: arrow::datatypes::ArrowPrimitiveType> {
    sum: Option<T::Native>,
    count: i64,
    scalar: fn(Option<T::Native>) -> ScalarValue,
}

impl<T: arrow::datatypes::ArrowPrimitiveType> RetractingFloatingSumAccumulator<T> {
    pub(super) fn new(scalar: fn(Option<T::Native>) -> ScalarValue) -> Self {
        Self {
            sum: None,
            count: 0,
            scalar,
        }
    }
}

impl<T: arrow::datatypes::ArrowPrimitiveType> Accumulator for RetractingFloatingSumAccumulator<T>
where
    T: std::fmt::Debug,
    T::Native: Default + std::ops::Add<Output = T::Native> + std::ops::Sub<Output = T::Native>,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let input = values[0]
            .as_any()
            .downcast_ref::<arrow::array::PrimitiveArray<T>>()
            .expect("floating SUM value type");
        let changes = values
            .get(1)
            .map(|kinds| kinds.as_any().downcast_ref::<Int8Array>().unwrap());
        for (row, value) in input.iter().enumerate() {
            if let Some(value) = value {
                let retract = changes.is_some_and(|kinds| matches!(kinds.value(row), 1 | 3));
                self.sum = Some(if retract {
                    self.sum.unwrap_or_default() - value
                } else {
                    // Assign the first value directly: adding +0 would lose an initial -0.
                    self.sum.map_or(value, |sum| sum + value)
                });
                self.count = self.count.wrapping_add(if retract { -1 } else { 1 });
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let sums = states[0]
            .as_any()
            .downcast_ref::<arrow::array::PrimitiveArray<T>>()
            .expect("floating SUM partial type");
        let counts = states[1].as_any().downcast_ref::<Int64Array>().unwrap();
        for (row, value) in sums.iter().enumerate() {
            if let Some(value) = value {
                self.sum = Some(self.sum.map_or(value, |sum| sum + value));
            }
            self.count = self.count.wrapping_add(counts.value(row));
        }
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![
            (self.scalar)(self.sum),
            ScalarValue::Int64(Some(self.count)),
        ])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok((self.scalar)(if self.count == 0 { None } else { self.sum }))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[derive(Debug)]
pub(super) struct RetractingWindowAccumulator {
    sum_type: Option<DataType>,
    sum: Option<i64>,
    count: i64,
}

impl RetractingWindowAccumulator {
    pub(super) fn new(sum_type: Option<DataType>) -> Self {
        Self {
            sum_type,
            sum: None,
            count: 0,
        }
    }

    fn sum_scalar(&self, value: Option<i64>) -> ScalarValue {
        match self.sum_type.as_ref().expect("sum accumulator") {
            DataType::Int8 => ScalarValue::Int8(value.map(|v| v as i8)),
            DataType::Int16 => ScalarValue::Int16(value.map(|v| v as i16)),
            DataType::Int32 => ScalarValue::Int32(value.map(|v| v as i32)),
            DataType::Int64 => ScalarValue::Int64(value),
            other => unreachable!("unsupported retracting SUM: {other}"),
        }
    }

    fn integer(array: &ArrayRef, row: usize) -> i64 {
        match array.data_type() {
            DataType::Int8 => array
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(row) as i64,
            DataType::Int16 => array
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row) as i64,
            DataType::Int32 => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row) as i64,
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
            other => unreachable!("unsupported retracting SUM column: {other}"),
        }
    }
}

impl Accumulator for RetractingWindowAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let input = &values[0];
        let changes = values
            .get(1)
            .map(|v| v.as_any().downcast_ref::<Int8Array>().unwrap());
        for row in 0..input.len() {
            if input.is_null(row) {
                continue;
            }
            let retract = changes.is_some_and(|kinds| matches!(kinds.value(row), 1 | 3));
            if self.sum_type.is_some() {
                let value = Self::integer(input, row);
                let sum = self.sum.unwrap_or(0);
                self.sum = Some(if retract {
                    sum.wrapping_sub(value)
                } else {
                    sum.wrapping_add(value)
                });
            }
            self.count = if retract {
                self.count.wrapping_sub(1)
            } else {
                self.count.wrapping_add(1)
            };
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let count_column = usize::from(self.sum_type.is_some());
        let counts = states[count_column]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..counts.len() {
            if self.sum_type.is_some() && !states[0].is_null(row) {
                self.sum = Some(
                    self.sum
                        .unwrap_or(0)
                        .wrapping_add(Self::integer(&states[0], row)),
                );
            }
            self.count = self.count.wrapping_add(counts.value(row));
        }
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        let count = ScalarValue::Int64(Some(self.count));
        Ok(if self.sum_type.is_some() {
            vec![self.sum_scalar(self.sum), count]
        } else {
            vec![count]
        })
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(if self.sum_type.is_some() {
            // Flink tests zero, not positivity: unmatched deletes have a negative count and sum.
            self.sum_scalar(if self.count == 0 { None } else { self.sum })
        } else {
            ScalarValue::Int64(Some(self.count))
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregates::{build_aggregates, WindowAggregate};

    fn decimals(values: Vec<Option<i128>>, precision: u8, scale: i8) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        )
    }

    #[test]
    fn decimal_average_retains_zero_count_residuals_and_signed_division() {
        for (precision, scale) in [(12, 2), (38, 18)] {
            let aggregates = build_aggregates(
                &[4, HIDDEN_LIVE_COUNT],
                &[2000 + precision * 100 + scale, 0],
            );
            let mut avg = aggregates[0].create_accumulator();
            avg.update_batch(&[
                decimals(
                    vec![Some(150), Some(100), None],
                    precision as u8,
                    scale as i8,
                ),
                Arc::new(Int8Array::from(vec![0, 1, 3])),
            ])
            .unwrap();
            assert!(avg.evaluate().unwrap().is_null());
            let saved = avg.state().unwrap();
            assert_eq!(saved[0], ScalarValue::Decimal128(Some(50), 38, scale as i8));
            let state = saved
                .into_iter()
                .map(|value| value.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>();
            let mut restored = aggregates[0].create_accumulator();
            restored.merge_batch(&state).unwrap();
            restored
                .update_batch(&[
                    decimals(vec![Some(25)], precision as u8, scale as i8),
                    Arc::new(Int8Array::from(vec![3])),
                ])
                .unwrap();
            let result_scale = (scale as i8).max(6);
            assert_eq!(
                restored.evaluate().unwrap(),
                ScalarValue::Decimal128(
                    Some(-25 * 10_i128.pow((result_scale - scale as i8) as u32)),
                    38,
                    result_scale
                )
            );
            assert_eq!(restored.state().unwrap()[1], ScalarValue::Int64(Some(-1)));
        }
    }

    #[test]
    fn decimal_average_overflow_stays_null_across_zero_count_restore() {
        let aggregates = build_aggregates(&[4, HIDDEN_LIVE_COUNT], &[5802, 0]);
        for sign in [1, -1] {
            let mut avg = aggregates[0].create_accumulator();
            avg.update_batch(&[
                decimals(
                    vec![
                        Some(sign * (DECIMAL128_MAX - 1)),
                        Some(sign),
                        Some(0),
                        Some(0),
                    ],
                    38,
                    2,
                ),
                Arc::new(Int8Array::from(vec![0, 0, 1, 3])),
            ])
            .unwrap();
            let saved = avg.state().unwrap();
            assert_eq!(
                saved,
                vec![
                    ScalarValue::Decimal128(None, 38, 2),
                    ScalarValue::Int64(Some(0))
                ]
            );
            let state = saved
                .into_iter()
                .map(|value| value.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>();
            let mut restored = aggregates[0].create_accumulator();
            restored.merge_batch(&state).unwrap();
            restored
                .update_batch(&[
                    decimals(vec![Some(250)], 38, 2),
                    Arc::new(Int8Array::from(vec![2])),
                ])
                .unwrap();
            assert_eq!(
                restored.evaluate().unwrap(),
                ScalarValue::Decimal128(None, 38, 6)
            );
            assert_eq!(restored.state().unwrap()[1], ScalarValue::Int64(Some(1)));
        }
    }

    #[test]
    fn decimal_average_empty_partials_and_append_checkpoints_keep_their_contracts() {
        let aggregates = build_aggregates(&[4, LIVE_COUNT], &[3202, 0]);
        let mut avg = aggregates[0].create_accumulator();
        let empty = avg.state().unwrap();
        assert_eq!(empty[0], ScalarValue::Decimal128(Some(0), 38, 2));
        avg.merge_batch(&[
            decimals(vec![Some(0), Some(150), Some(0)], 38, 2),
            Arc::new(Int64Array::from(vec![0, i64::MAX, 1])),
        ])
        .unwrap();
        assert_eq!(avg.state().unwrap()[1], ScalarValue::Int64(Some(i64::MIN)));
        let mut append = WindowAggregate::new(4, &DataType::Decimal128(12, 2)).create_accumulator();
        assert_eq!(
            append.state().unwrap()[0],
            ScalarValue::Decimal128(None, 38, 2)
        );
        append
            .merge_batch(&[
                decimals(vec![None, Some(150)], 38, 2),
                Arc::new(Int64Array::from(vec![0, 1])),
            ])
            .unwrap();
        assert_eq!(
            append.evaluate().unwrap(),
            ScalarValue::Decimal128(Some(1_500_000), 38, 6)
        );
    }

    #[test]
    fn decimal_sum_widens_precision_and_restores_signed_counts_and_residuals() {
        for (precision, scale) in [(12, 2), (38, 18)] {
            let aggregate =
                WindowAggregate::new(RETRACT_SUM, &DataType::Decimal128(precision, scale));
            assert_eq!(aggregate.result_type(), DataType::Decimal128(38, scale));
            assert_eq!(
                aggregate.state_fields()[0].data_type(),
                &DataType::Decimal128(38, scale)
            );
            let mut sum = aggregate.create_accumulator();
            sum.update_batch(&[
                decimals(vec![Some(150), Some(100), None], precision, scale),
                Arc::new(Int8Array::from(vec![0, 1, 3])),
            ])
            .unwrap();
            assert!(sum.evaluate().unwrap().is_null());
            let state = sum
                .state()
                .unwrap()
                .into_iter()
                .map(|value| value.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>();
            let mut restored = aggregate.create_accumulator();
            restored.merge_batch(&state).unwrap();
            restored
                .update_batch(&[
                    decimals(vec![Some(25)], precision, scale),
                    Arc::new(Int8Array::from(vec![3])),
                ])
                .unwrap();
            assert_eq!(
                restored.evaluate().unwrap(),
                ScalarValue::Decimal128(Some(25), 38, scale)
            );
            assert_eq!(restored.state().unwrap()[1], ScalarValue::Int64(Some(-1)));
        }
    }

    #[test]
    fn decimal_sum_overflow_restores_as_null_and_restarts_with_either_sign() {
        for kind in [0, 1, 2, 3] {
            let aggregate = WindowAggregate::new(RETRACT_SUM, &DataType::Decimal128(38, 2));
            let mut sum = aggregate.create_accumulator();
            sum.update_batch(&[
                decimals(vec![Some(DECIMAL128_MAX - 1), Some(1)], 38, 2),
                Arc::new(Int8Array::from(vec![kind; 2])),
            ])
            .unwrap();
            assert!(sum.evaluate().unwrap().is_null());
            let state = sum
                .state()
                .unwrap()
                .into_iter()
                .map(|value| value.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>();
            let mut restored = aggregate.create_accumulator();
            restored.merge_batch(&state).unwrap();
            restored
                .update_batch(&[
                    decimals(vec![Some(25)], 38, 2),
                    Arc::new(Int8Array::from(vec![kind])),
                ])
                .unwrap();
            let sign = if kind == 1 || kind == 3 { -1 } else { 1 };
            assert_eq!(
                restored.evaluate().unwrap(),
                ScalarValue::Decimal128(Some(25 * sign), 38, 2)
            );
            assert_eq!(
                restored.state().unwrap()[1],
                ScalarValue::Int64(Some(3 * sign as i64))
            );
        }
    }

    #[test]
    fn decimal_sum_merges_null_overflow_partials_and_wraps_the_count() {
        let aggregate = WindowAggregate::new(RETRACT_SUM, &DataType::Decimal128(38, 2));
        let mut sum = aggregate.create_accumulator();
        sum.merge_batch(&[
            decimals(
                vec![Some(DECIMAL128_MAX - 1), Some(1), Some(25), None],
                38,
                2,
            ),
            Arc::new(Int64Array::from(vec![i64::MAX, 1, 0, 2])),
        ])
        .unwrap();
        assert_eq!(
            sum.evaluate().unwrap(),
            ScalarValue::Decimal128(Some(25), 38, 2)
        );
        assert_eq!(
            sum.state().unwrap()[1],
            ScalarValue::Int64(Some(i64::MIN + 2))
        );
    }

    #[test]
    fn floating_sums_restore_initial_negative_zero_without_adding_positive_zero() {
        for datatype in [DataType::Float32, DataType::Float64] {
            let aggregate = WindowAggregate::new(RETRACT_SUM, &datatype);
            let mut sum = aggregate.create_accumulator();
            sum.update_batch(&[arrow::compute::cast(
                &arrow::array::Float64Array::from(vec![-0.0]),
                &datatype,
            )
            .unwrap()])
                .unwrap();
            let states = sum
                .state()
                .unwrap()
                .into_iter()
                .map(|value| value.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>();
            let mut restored = aggregate.create_accumulator();
            restored.merge_batch(&states).unwrap();
            for value in [sum.evaluate().unwrap(), restored.evaluate().unwrap()] {
                match value {
                    ScalarValue::Float32(Some(value)) => {
                        assert_eq!(value.to_bits(), (-0.0_f32).to_bits())
                    }
                    ScalarValue::Float64(Some(value)) => {
                        assert_eq!(value.to_bits(), (-0.0_f64).to_bits())
                    }
                    other => panic!("expected negative zero, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn floating_sums_round_each_update_at_the_declared_width_and_merge_in_order() {
        for datatype in [DataType::Float32, DataType::Float64] {
            let values = |values: Vec<f64>| {
                arrow::compute::cast(&arrow::array::Float64Array::from(values), &datatype).unwrap()
            };
            let aggregate = WindowAggregate::new(RETRACT_SUM, &datatype);
            let mut sum = aggregate.create_accumulator();
            sum.update_batch(&[
                values(vec![16777216.0, 1.0, 16777216.0]),
                Arc::new(Int8Array::from(vec![0, 2, 1])),
            ])
            .unwrap();
            let expected = if datatype == DataType::Float32 {
                0.0
            } else {
                1.0
            };
            assert_eq!(
                sum.evaluate().unwrap(),
                ScalarValue::try_from_array(&values(vec![expected]), 0).unwrap()
            );
            let mut merge = aggregate.create_accumulator();
            merge
                .merge_batch(&[
                    values(vec![2_f64.powi(54)]),
                    Arc::new(Int64Array::from(vec![1])),
                ])
                .unwrap();
            merge
                .merge_batch(&[
                    values(vec![-2_f64.powi(54), 1.0]),
                    Arc::new(Int64Array::from(vec![-1, 1])),
                ])
                .unwrap();
            assert_eq!(
                merge.evaluate().unwrap(),
                ScalarValue::try_from_array(&values(vec![1.0]), 0).unwrap()
            );
        }
    }

    #[test]
    fn floating_sums_restore_zero_count_finite_and_nonfinite_partials() {
        for datatype in [DataType::Float32, DataType::Float64] {
            for first in [2.0, f64::INFINITY] {
                let values = |values: Vec<f64>| {
                    arrow::compute::cast(&arrow::array::Float64Array::from(values), &datatype)
                        .unwrap()
                };
                let aggregate = WindowAggregate::new(RETRACT_SUM, &datatype);
                let mut sum = aggregate.create_accumulator();
                sum.update_batch(&[
                    values(vec![first, if first.is_finite() { 1.0 } else { first }]),
                    Arc::new(Int8Array::from(vec![0, 3])),
                ])
                .unwrap();
                assert!(sum.evaluate().unwrap().is_null());
                let states = sum
                    .state()
                    .unwrap()
                    .into_iter()
                    .map(|value| value.to_array_of_size(1).unwrap())
                    .collect::<Vec<_>>();
                let mut restored = aggregate.create_accumulator();
                restored.merge_batch(&states).unwrap();
                restored.update_batch(&[values(vec![3.0])]).unwrap();
                if first.is_finite() {
                    assert_eq!(
                        restored.evaluate().unwrap(),
                        ScalarValue::try_from_array(&values(vec![4.0]), 0).unwrap()
                    );
                } else {
                    match restored.evaluate().unwrap() {
                        ScalarValue::Float32(Some(value)) => assert!(value.is_nan()),
                        ScalarValue::Float64(Some(value)) => assert!(value.is_nan()),
                        other => panic!("expected NaN, got {other:?}"),
                    }
                }
            }
        }
    }

    #[test]
    fn integer_retract_buffers_wrap_at_every_declared_width_and_keep_zero_count_null() {
        for (data_type, max) in [
            (DataType::Int8, i8::MAX as i64),
            (DataType::Int16, i16::MAX as i64),
            (DataType::Int32, i32::MAX as i64),
            (DataType::Int64, i64::MAX),
        ] {
            let values = |values: Vec<Option<i64>>| {
                arrow::compute::cast(&Int64Array::from(values), &data_type).unwrap()
            };
            let mut first = RetractingWindowAccumulator::new(Some(data_type.clone()));
            first
                .update_batch(&[values(vec![Some(max), Some(1), None])])
                .unwrap();
            let state = first
                .state()
                .unwrap()
                .into_iter()
                .map(|v| v.to_array_of_size(1).unwrap())
                .collect::<Vec<_>>();
            let mut restored = RetractingWindowAccumulator::new(Some(data_type.clone()));
            restored.merge_batch(&state).unwrap();
            restored
                .update_batch(&[
                    values(vec![Some(max), None]),
                    Arc::new(Int8Array::from(vec![3, 3])),
                ])
                .unwrap();
            assert_eq!(restored.evaluate().unwrap(), restored.sum_scalar(Some(1)));
            restored
                .update_batch(&[values(vec![Some(1)]), Arc::new(Int8Array::from(vec![1]))])
                .unwrap();
            assert_eq!(restored.evaluate().unwrap(), restored.sum_scalar(None));
            assert_eq!(restored.state().unwrap()[1], ScalarValue::Int64(Some(0)));
            restored
                .update_batch(&[values(vec![Some(3)]), Arc::new(Int8Array::from(vec![3]))])
                .unwrap();
            assert_eq!(restored.evaluate().unwrap(), restored.sum_scalar(Some(-3)));
            assert_eq!(restored.state().unwrap()[1], ScalarValue::Int64(Some(-1)));
        }
    }
}
