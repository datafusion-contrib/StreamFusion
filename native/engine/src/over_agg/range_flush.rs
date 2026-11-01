use super::*;

impl OverWindowAggregator {
    pub(crate) fn flush(
        &mut self,
        watermark: i64,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        self.ensure_canonical_distinct_ready()?;
        let Some(offset) = self.range_offset() else {
            return self.flush_inner(watermark, now_ms);
        };
        let mut outputs = Vec::new();
        loop {
            // Normally cleanup is the last timer for a key, so the bulk firing path suffices.
            // Java long overflow can move cleanup before data, or leave data at the deadline.
            let next = self
                .cleanup_state
                .iter()
                .filter(|(key, deadline)| {
                    **deadline <= watermark
                        && (self.range_timers.get(*key).is_some_and(|timers| {
                            timers.armed.last().is_some_and(|time| time > *deadline)
                        }) || deadline
                            .checked_add(offset)
                            .and_then(|time| time.checked_add(1))
                            .is_none())
                })
                .map(|(_, deadline)| *deadline)
                .min();
            let Some(deadline) = next else { break };
            if let Some(before) = deadline.checked_sub(1) {
                outputs.push(self.flush_inner(before, now_ms)?);
            }
            let keys = self
                .cleanup_state
                .iter()
                .filter(|(_, time)| **time == deadline)
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            for key in keys {
                self.clear_range_pending(&key.0)?;
                let remaining = self.range_timers.remove(&key).map(|mut timers| {
                    self.retention_bytes -= byte_key_bytes(&key.0) + timers.bytes();
                    timers.armed.retain(|time| *time > deadline);
                    timers.last = None;
                    timers
                });
                self.clear_key(&key.0);
                // Flink clears state without cancelling other timers for this key. Later empty
                // callbacks still advance its last-triggering timestamp and affect admission.
                if let Some(timers) = remaining.filter(|timers| !timers.armed.is_empty()) {
                    self.retention_bytes += byte_key_bytes(&key.0) + timers.bytes();
                    self.range_timers.insert(key, timers);
                }
            }
        }
        outputs.push(self.flush_inner(watermark, now_ms)?);
        let batches = outputs
            .iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect::<Vec<_>>();
        if batches.is_empty() {
            return Ok(outputs.pop().unwrap());
        }
        let output = concat_batches(&batches[0].schema(), batches.iter().copied())?;
        let times = rt_to_millis(output.column(self.rt_column));
        let mut order = (0..output.num_rows()).collect::<Vec<_>>();
        order.sort_by_key(|&row| times.value(row));
        let indices = UInt32Array::from_iter_values(order.into_iter().map(|row| row as u32));
        Ok(arrow::compute::take_record_batch(&output, &indices)?)
    }

    fn clear_range_pending(&mut self, key: &[u8]) -> Result<(), DataFusionError> {
        let columns = &self.key_columns;
        let types = self.cleanup_key_types();
        let mut converter = Some(key_row_converter_from_types(&types));
        let matches = |batch: &RecordBatch| {
            let arrays = columns.iter().map(|&i| batch.column(i)).collect::<Vec<_>>();
            let keys = encode_keys(&mut converter, &arrays, batch.num_rows());
            (0..batch.num_rows())
                .map(|row| keys.row(row).data() == key)
                .collect::<Vec<_>>()
        };
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &mut self.store {
            return store.delete_pending_matching(matches);
        }
        let mut matches = matches;
        let mut pending = Vec::new();
        for batch in &self.buffered {
            let keep = BooleanArray::from(
                matches(batch)
                    .into_iter()
                    .map(|value| !value)
                    .collect::<Vec<_>>(),
            );
            let retained = filter_record_batch(batch, &keep)?;
            if retained.num_rows() > 0 {
                pending.push(retained);
            }
        }
        self.buffered = pending;
        Ok(())
    }
}
