use super::*;

impl RocksOverAggStore {
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
