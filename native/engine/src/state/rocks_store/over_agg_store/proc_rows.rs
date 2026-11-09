use super::*;

impl RocksOverAggStore {
    pub(super) fn frame_clock_key(frame_key: &[u8]) -> Vec<u8> {
        let mut key = frame_key.to_vec();
        key[4] = FRAME_CLOCKS_TABLE;
        key
    }

    pub(crate) fn frame_clock_prefix(&self, group: i32, key: &[u8]) -> Vec<u8> {
        Self::frame_clock_key(&self.frame_prefix(group, key))
    }

    pub(super) fn read_frame_clock(
        &self,
        frame_key: &[u8],
    ) -> Result<Option<i64>, DataFusionError> {
        self.db
            .get(Self::frame_clock_key(frame_key))
            .map_err(re)?
            .map(|bytes| {
                bytes
                    .as_slice()
                    .try_into()
                    .map(i64::from_le_bytes)
                    .map_err(|_| {
                        DataFusionError::Execution("invalid OVER frame processing clock".into())
                    })
            })
            .transpose()
    }

    pub(crate) fn write_frame_clocks(
        &mut self,
        entries: &[(Vec<u8>, i64, u64)],
        clocks: &[Option<i64>],
    ) -> Result<(), DataFusionError> {
        if entries.len() != clocks.len() {
            return Err(DataFusionError::Execution(
                "OVER frame clock length mismatch".into(),
            ));
        }
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for ((prefix, rt, seq), clock) in entries.iter().zip(clocks) {
            let key = Self::frame_clock_key(&Self::frame_db_key(prefix, *rt, *seq));
            if let Some(clock) = clock {
                writes.put(key, clock.to_le_bytes())?;
            } else {
                writes.delete(key)?;
            }
        }
        writes.finish()
    }

    pub(crate) fn proc_rows_key(&self, group: i32, key: &[u8]) -> Vec<u8> {
        let mut bytes = group.to_be_bytes().to_vec();
        bytes.push(PROC_ROWS_TABLE);
        bytes.extend_from_slice(key);
        bytes
    }

    pub(crate) fn scan_proc_rows(&self) -> Result<Vec<(Box<[u8]>, Vec<u8>)>, DataFusionError> {
        let mut entries = Vec::new();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.map_err(re)?;
            if key.len() >= KEY_PREFIX_LEN && key[4] == PROC_ROWS_TABLE {
                entries.push((key[KEY_PREFIX_LEN..].into(), value.to_vec()));
            }
        }
        Ok(entries)
    }
}
