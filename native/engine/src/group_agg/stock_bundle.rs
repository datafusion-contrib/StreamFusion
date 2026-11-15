use super::*;

impl<S: KeyedStateStore<GroupKeyState>> GroupAggregator<S> {
    pub(super) fn stock_retracting_bundle(&self) -> bool {
        self.mini_batch && self.stock_append_only == Some(false) && !self.stock_global_merge
    }

    pub(super) fn drain_stock_retracting_bundle(
        &mut self,
        batches: &[(RecordBatch, usize)],
        now: i64,
    ) -> Result<(), DataFusionError> {
        let rows = batches
            .iter()
            .map(|(batch, _)| batch.num_rows())
            .sum::<usize>();
        let key_bytes = self.deferred_keys.keys().map(ByteKey::len).sum::<usize>();
        let largest = batches
            .iter()
            .map(|(batch, _)| batch.get_array_memory_size())
            .max()
            .unwrap_or(0);
        let mut work = self.memory.temporary_reservation();
        if let Some(work) = &mut work {
            work.try_resize(
                rows.saturating_mul(96)
                    .saturating_add(self.deferred_keys.len().saturating_mul(512))
                    .saturating_add(key_bytes.saturating_mul(4))
                    .saturating_add(self.stock_bundle_capacity.saturating_mul(64))
                    .saturating_add(largest.saturating_mul(2)),
            )?;
        }
        let mut groups = Vec::<(ByteKey, Vec<(usize, Vec<u32>)>)>::new();
        let mut lookup = HashMap::<ByteKey, usize>::default();
        let mut order = JavaMapOrder::with_capacity(self.stock_bundle_capacity);
        for (batch_index, (batch, _)) in batches.iter().enumerate() {
            let mut keys = BinaryRowBatchEncoder::new(
                batch,
                &self.key_columns,
                &self.key_timestamp_precisions,
            );
            let mut hashes = BinaryRowBatchEncoder::new(
                batch,
                &self.key_columns,
                &self.key_timestamp_precisions,
            )
            .projected();
            for row in 0..batch.num_rows() {
                let key = keys.encode(row);
                let index = match lookup.get(key) {
                    Some(&index) => index,
                    None => {
                        let index = groups.len();
                        let owned = ByteKey::from(key);
                        lookup.insert(owned.clone(), index);
                        groups.push((owned, Vec::new()));
                        let hash = hashes.hash(row) as u32;
                        order.insert(index, (hash ^ (hash >> 16)) as i32);
                        index
                    }
                };
                let slices = &mut groups[index].1;
                if slices.last().is_none_or(|(index, _)| *index != batch_index) {
                    slices.push((batch_index, Vec::new()));
                }
                slices.last_mut().unwrap().1.push(row as u32);
            }
        }
        self.stock_bundle_capacity = order.capacity();
        for index in order.iter() {
            let (key, slices) = &groups[index];
            for (batch, rows) in slices {
                let selected = arrow::compute::take_record_batch(
                    &batches[*batch].0,
                    &UInt32Array::from(rows.clone()),
                )?;
                self.update(&selected, now)?;
            }
            // Released 1.18/2.2 returns from finishBundle when this key contains only orphan
            // retractions. Later keys must remain untouched, including their retained views.
            if !self.staged_changes.contains_key(key.0.as_ref()) {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner() -> GroupAggregator {
        let mut owner = GroupAggregator::new(vec![3, 0], vec![0; 2], vec![-1, 1], vec![0], true)
            .with_key_timestamp_precisions(vec![i32::MIN, -1])
            .with_mini_batch();
        owner.stock_append_only = Some(false);
        owner
    }

    fn input(keys: Vec<Option<i64>>, kinds: Vec<i8>) -> RecordBatch {
        let rows = keys.len();
        RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(keys)) as ArrayRef),
            ("v", Arc::new(Int64Array::from(vec![7; rows]))),
            (ROW_KIND_COLUMN, Arc::new(Int8Array::from(kinds))),
        ])
        .unwrap()
    }

    #[test]
    fn orphan_retract_drops_later_keys_and_the_next_bundle_starts_fresh() {
        let mut owner = owner();
        let batch = input(vec![Some(99), Some(1), Some(1), None], vec![3, 0, 0, 0]);
        owner.update(&batch, 100).unwrap();
        assert!(owner.staged_keys() > 0);
        assert_eq!(owner.flush_mini_batch().unwrap().num_rows(), 0);
        assert_eq!(owner.store.iter().count(), 0);
        assert_eq!(owner.staged_keys(), 0);
        owner
            .update(&input(vec![Some(1), None], vec![0, 0]), 200)
            .unwrap();
        assert_eq!(owner.flush_mini_batch().unwrap().num_rows(), 2);
        assert_eq!(owner.store.iter().count(), 2);
    }

    #[test]
    fn later_accumulation_for_an_orphan_key_keeps_the_whole_bundle() {
        let mut owner = owner();
        owner
            .update(&input(vec![Some(99), Some(1), None], vec![3, 0, 0]), 100)
            .unwrap();
        owner.update(&input(vec![Some(99)], vec![0]), 100).unwrap();
        assert_eq!(owner.flush_mini_batch().unwrap().num_rows(), 3);
        assert_eq!(owner.store.iter().count(), 3);
    }
}
