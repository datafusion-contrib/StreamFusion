use super::*;
use crate::aggregates::RetractingExtremeAccumulator;
use crate::group_agg::MappedExtreme;

pub(super) struct PendingWindowExtremes {
    start: i64,
    values: Vec<Option<MappedExtreme>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canceled_partial_bundles_leave_retained_extrema_and_release_reservations() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        {
            let mut global = TumblingAggregator::new(1000, 1000, false, vec![0, 0], vec![23, 12])
                .with_memory_pool(&pool)
                .unwrap();
            let seed = RecordBatch::try_from_iter(vec![
                ("ts", Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef),
                (
                    "value0",
                    Arc::new(Int64Array::from(vec![1, 5, 9])) as ArrayRef,
                ),
                (
                    "value1",
                    Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef,
                ),
                (
                    ROW_KIND_COLUMN,
                    Arc::new(Int8Array::from(vec![3, 0, 0])) as ArrayRef,
                ),
            ])
            .unwrap();
            global.update(&seed).unwrap();
            for retract in [false, true] {
                let mut partial = MappedExtreme::new(true);
                partial.update(ScalarValue::Int64(Some(3)), retract);
                let batch = RecordBatch::try_from_iter(vec![
                    (
                        "partial0",
                        partial.partial(&DataType::Int64).to_array().unwrap(),
                    ),
                    (
                        "partial1",
                        Arc::new(Int64Array::from(vec![if retract { -1 } else { 1 }])) as ArrayRef,
                    ),
                    (
                        "slice_end",
                        Arc::new(Int64Array::from(vec![1000])) as ArrayRef,
                    ),
                ])
                .unwrap();
                global.update_partial(&batch).unwrap();
                assert_eq!(pool.reserved(), global.computed_state_bytes());
            }
            assert_eq!(global.pending_extremes.len(), 1);
            let result = global.flush(1000).unwrap();
            assert_eq!(column_i64(&result, "result0").values(), &[5]);
            assert_eq!(pool.reserved(), 0);
        }
        assert_eq!(pool.reserved(), 0);
    }
}

impl PendingWindowExtremes {
    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.values.capacity() * std::mem::size_of::<Option<MappedExtreme>>()
            + self
                .values
                .iter()
                .flatten()
                .map(|value| value.bytes() - std::mem::size_of::<MappedExtreme>())
                .sum::<usize>()
    }
}

impl TumblingAggregator {
    pub(super) fn pending_extreme_bytes(&self) -> usize {
        self.pending_extremes
            .iter()
            .map(|((_, key), values)| {
                std::mem::size_of::<(i64, OwnedRow)>() + owned_row_bytes(key) + values.bytes()
            })
            .sum()
    }

    pub(super) fn stage_extreme_partial(
        &mut self,
        start: i64,
        end: i64,
        key: &OwnedRow,
        aggregate: usize,
        column: &ArrayRef,
        row: usize,
    ) -> bool {
        let WindowAggregate::RetractingExtreme { minimum, .. } = self.aggregates[aggregate] else {
            return false;
        };
        let incoming = MappedExtreme::from_partial(column, row, minimum);
        let identity = (end, key.clone());
        let before = self.pending_extremes.get(&identity).map_or(0, |entry| {
            std::mem::size_of::<(i64, OwnedRow)>() + owned_row_bytes(key) + entry.bytes()
        });
        let entry =
            self.pending_extremes
                .entry(identity)
                .or_insert_with(|| PendingWindowExtremes {
                    start,
                    values: (0..self.aggregates.len()).map(|_| None).collect(),
                });
        entry.values[aggregate]
            .get_or_insert_with(|| MappedExtreme::new(minimum))
            .merge(&incoming, StateTtl::new(0, 0), 0);
        let after = std::mem::size_of::<(i64, OwnedRow)>() + owned_row_bytes(key) + entry.bytes();
        if self.memory.tracking() {
            self.memory.record(after as isize - before as isize);
        }
        true
    }

    pub(super) fn commit_extreme_partials(&mut self) -> Result<(), DataFusionError> {
        if self.pending_extremes.is_empty() {
            return Ok(());
        }
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            let keys: Vec<_> = self
                .pending_extremes
                .keys()
                .map(|(_, key)| key.clone())
                .collect();
            let columns = decode_keys(self.key_converter.as_ref(), &keys, &self.key_types);
            let batch = RecordBatch::try_new_with_options(
                Arc::new(Schema::new(key_fields(&self.key_types))),
                columns,
                &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(keys.len())),
            )?;
            let touched: Vec<_> = self
                .pending_extremes
                .iter()
                .enumerate()
                .map(|(row, ((end, _), entry))| (entry.start, *end, keys[row].row(), row as u32))
                .collect();
            self.hydrate_store_groups(&batch, &touched)?;
        }
        let pending = std::mem::take(&mut self.pending_extremes);
        for ((end, key), entry) in pending {
            let staged_bytes =
                std::mem::size_of::<(i64, OwnedRow)>() + owned_row_bytes(&key) + entry.bytes();
            let track = self.tracks_window_state();
            let accumulators = self.accumulators(entry.start, end, key);
            let before = if track {
                accumulators_bytes(accumulators)
            } else {
                0
            };
            for (accumulator, partial) in accumulators.iter_mut().zip(&entry.values) {
                if let Some(partial) = partial {
                    (accumulator.as_mut() as &mut dyn std::any::Any)
                        .downcast_mut::<RetractingExtremeAccumulator>()
                        .expect("retracting window extreme")
                        .merge_combined(partial);
                }
            }
            let after = if track {
                accumulators_bytes(accumulators)
            } else {
                0
            };
            if self.memory.tracking() {
                self.memory
                    .record(after as isize - before as isize - staged_bytes as isize);
            }
        }
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            self.write_through_store_with_timers(self.shared_slices)?;
        }
        self.memory.account()
    }
}
