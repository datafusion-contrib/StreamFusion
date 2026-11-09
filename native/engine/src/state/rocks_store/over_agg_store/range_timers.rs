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
        out.extend_from_slice(key);
        out
    }

    pub(crate) fn scan_range_timers(&self) -> Result<Vec<(Box<[u8]>, Vec<u8>)>, DataFusionError> {
        let mut entries = Vec::new();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.map_err(re)?;
            if key.len() >= KEY_PREFIX_LEN && key[4] == RANGE_TIMERS_TABLE {
                entries.push((key[KEY_PREFIX_LEN..].into(), value.to_vec()));
            }
        }
        Ok(entries)
    }

    pub(crate) fn write_range_timers(
        &mut self,
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

    pub(crate) fn replace_range_timers(
        &mut self,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, _) = entry.map_err(re)?;
            if key.len() >= KEY_PREFIX_LEN && key[4] == RANGE_TIMERS_TABLE {
                writes.delete(key)?;
            }
        }
        for (key, value) in entries {
            writes.put(key, value)?;
        }
        writes.finish()
    }
}
