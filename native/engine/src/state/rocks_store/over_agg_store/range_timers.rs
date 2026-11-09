use super::*;

impl RocksOverAggStore {
    pub(crate) fn delete_pending_matching(
        &mut self,
        mut matches: impl FnMut(&RecordBatch) -> Vec<bool>,
    ) -> Result<(), DataFusionError> {
        let mut keys = Vec::new();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, _) = entry.map_err(re)?;
            if key.len() == PENDING_KEY_LEN && key[4] == PENDING_TABLE {
                keys.push(key);
            }
        }
        let mut position = 0;
        let mut deletes = Vec::new();
        self.visit_pending(1024, |batch| {
            for matched in matches(&batch) {
                if matched {
                    deletes.push(keys[position].clone());
                }
                position += 1;
            }
            Ok(())
        })?;
        assert_eq!(position, keys.len(), "pending OVER rows and keys disagree");
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for key in deletes {
            writes.delete(key)?;
        }
        writes.finish()
    }

    pub(crate) fn range_timer_key(&self, group: i32, key: &[u8]) -> Vec<u8> {
        let mut out = group.to_be_bytes().to_vec();
        out.push(RANGE_TIMERS_TABLE);
        // Unbounded DISTINCT shares table numbers; Arrow keys never start with this marker.
        if self.frames_converter.is_none() {
            out.push(0xff);
        }
        out.extend_from_slice(key);
        out
    }

    pub(super) fn range_timer_prefix_len(&self) -> usize {
        KEY_PREFIX_LEN + usize::from(self.frames_converter.is_none())
    }

    pub(super) fn is_range_timer_key(&self, key: &[u8]) -> bool {
        key.len() >= self.range_timer_prefix_len()
            && key[4] == RANGE_TIMERS_TABLE
            && (self.frames_converter.is_some() || key[KEY_PREFIX_LEN] == 0xff)
    }

    pub(super) fn upgrade_polling_ledger(&self) -> Result<(), DataFusionError> {
        if self.frames_converter.is_some() || self.distinct_element_types.len() > 1 {
            return Ok(());
        }
        self.db.visit_snapshot_batches(
            1024,
            |key| {
                key.len() > KEY_PREFIX_LEN
                    && key[4] == RANGE_TIMERS_TABLE
                    && key[KEY_PREFIX_LEN] != 0xff
            },
            |rows| {
                let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
                for (key, value) in rows {
                    let mut tagged = key[..KEY_PREFIX_LEN].to_vec();
                    tagged.push(0xff);
                    tagged.extend_from_slice(&key[KEY_PREFIX_LEN..]);
                    writes.put(tagged, value)?;
                    writes.delete(key)?;
                }
                writes.finish()
            },
        )
    }

    pub(crate) fn scan_range_timers(&self) -> Result<Vec<(Box<[u8]>, Vec<u8>)>, DataFusionError> {
        let mut entries = Vec::new();
        self.visit_range_timers(1024, |key, value| {
            entries.push((key.into(), value.to_vec()));
            Ok(())
        })?;
        Ok(entries)
    }

    pub(crate) fn write_metadata(
        &self,
        entries: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (key, value) in entries {
            if let Some(value) = value {
                writes.put(key, value)?;
            } else {
                writes.delete(key)?;
            }
        }
        writes.finish()
    }

    pub(crate) fn clear_metadata(
        &self,
        timers: bool,
        counters: bool,
    ) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, _) = entry.map_err(re)?;
            if key.len() >= KEY_PREFIX_LEN
                && (key[4] == STAMPS_TABLE
                    || timers && key[4] == RANGE_TIMERS_TABLE
                    || counters && key[4] == PROC_ROWS_TABLE)
            {
                writes.delete(key)?;
            }
        }
        writes.finish()
    }
}
