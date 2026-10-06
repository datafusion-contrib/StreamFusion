use super::*;

/// Flink probes and eagerly cleans the opposite cache once per arriving row. A batch joins
/// against the original cache, then masks pairs that an earlier row's cleanup would have removed.
pub(super) struct IntervalProbePlan {
    enabled: Vec<bool>,
    first_probe: Vec<usize>,
    first_by_key: HashMap<Vec<u8>, usize>,
    expiration: i64,
}

impl IntervalProbePlan {
    pub(super) fn new(
        batch: &RecordBatch,
        key_columns: &[usize],
        key_precisions: &[i32],
        time_column: usize,
        horizon_offset: i64,
        expiration: i64,
        previous_expiration: &mut i64,
    ) -> Self {
        let times = rt_to_millis(batch.column(time_column));
        let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, key_precisions);
        let mut enabled = Vec::with_capacity(batch.num_rows());
        let mut first_probe = Vec::with_capacity(batch.num_rows());
        let mut first_by_key: HashMap<Vec<u8>, usize> = HashMap::default();
        for (row, time) in times.iter().enumerate() {
            let probes =
                time.is_some_and(|time| time.wrapping_add(horizon_offset) > *previous_expiration);
            enabled.push(probes);
            if probes {
                *previous_expiration = expiration;
                let key = encoder.encode(row);
                let first = if let Some(&first) = first_by_key.get(key) {
                    first
                } else {
                    first_by_key.insert(key.to_vec(), row);
                    row
                };
                first_probe.push(first);
            } else {
                first_probe.push(usize::MAX);
            }
        }
        Self {
            enabled,
            first_probe,
            first_by_key,
            expiration,
        }
    }

    pub(super) fn first_probe(&self, key: &[u8]) -> Option<usize> {
        self.first_by_key.get(key).copied()
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.first_by_key.keys().map(Vec::as_slice)
    }

    pub(super) fn expiration(&self) -> i64 {
        self.expiration
    }

    pub(super) fn pair_mask(
        &self,
        own_ids: &Int64Array,
        base: i64,
        opposite_times: &Int64Array,
    ) -> BooleanArray {
        assert_eq!(own_ids.len(), opposite_times.len());
        (0..own_ids.len())
            .map(|pair| {
                let row = usize::try_from(own_ids.value(pair) - base)
                    .expect("interval pair's input ordinal");
                Some(
                    self.enabled[row]
                        && (opposite_times.value(pair) > self.expiration
                            || row == self.first_probe[row]),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(keys: &[i64], times: &[i64]) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(keys.to_vec())) as ArrayRef),
            ("rt", Arc::new(Int64Array::from(times.to_vec())) as ArrayRef),
        ])
        .unwrap()
    }

    #[test]
    fn expired_cached_rows_match_only_the_first_probe_of_each_key() {
        let batch = input(&[1, 1, 2, 2], &[90, 90, 90, 90]);
        let mut previous = 0;
        let plan = IntervalProbePlan::new(&batch, &[0], &[-1], 1, 20, 94, &mut previous);
        let mask = plan.pair_mask(
            &Int64Array::from(vec![100, 101, 102, 103, 101, 103]),
            100,
            &Int64Array::from(vec![90, 90, 90, 90, 95, 95]),
        );
        assert_eq!(
            mask,
            BooleanArray::from(vec![true, false, true, false, true, true])
        );
        assert_eq!(previous, 94);
        assert_eq!(plan.keys().count(), 2);
    }

    #[test]
    fn global_expiration_gates_even_a_key_not_previously_cleaned() {
        let batch = input(&[1, 2, 1], &[100, 90, 105]);
        let mut previous = 0;
        let plan = IntervalProbePlan::new(&batch, &[0], &[-1], 1, 0, 94, &mut previous);
        let mask = plan.pair_mask(
            &Int64Array::from(vec![0, 1, 2]),
            0,
            &Int64Array::from(vec![90, 90, 90]),
        );
        assert_eq!(mask, BooleanArray::from(vec![true, false, false]));
        let mut encoder = BinaryRowBatchEncoder::new(&batch, &[0], &[-1]);
        assert_eq!(plan.first_probe(encoder.encode(0)), Some(0));
        assert_eq!(plan.first_probe(encoder.encode(1)), None);
    }

    #[test]
    fn initial_expiration_can_move_backwards_before_the_first_watermark() {
        let batch = input(&[1, 1, 2], &[1, -50, 1]);
        let mut previous = 0;
        let plan = IntervalProbePlan::new(&batch, &[0], &[-1], 1, 0, -101, &mut previous);
        let mask = plan.pair_mask(
            &Int64Array::from(vec![0, 1, 2]),
            0,
            &Int64Array::from(vec![0, -75, 0]),
        );
        assert_eq!(mask, BooleanArray::from(vec![true, true, true]));
        assert_eq!(previous, -101);
    }
}
