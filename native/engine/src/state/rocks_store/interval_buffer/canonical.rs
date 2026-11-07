use super::*;

impl RocksIntervalBuffer {
    pub(crate) fn visit_canonical_rows(
        &self,
        left: bool,
        schema: &SchemaRef,
        chunk: usize,
        mut work: Option<MemoryReservation>,
        mut accept: impl FnMut(RecordBatch, Vec<bool>) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let mut rows = Vec::with_capacity(chunk);
        let mut bytes = 0;
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.map_err(re)?;
            if key.len() != KEY_LEN || key[4] != Self::table(left) {
                continue;
            }
            if value.len() < VALUE_PREFIX_LEN || value[8] > 1 {
                return Err(DataFusionError::Execution(
                    "invalid canonical interval-join row".into(),
                ));
            }
            bytes += value.len() + std::mem::size_of::<BufferedIntervalRow>();
            if let Some(work) = &mut work {
                work.try_resize(bytes)?;
            }
            rows.push(Self::buffered_row(&key, &value));
            if rows.len() == chunk {
                let refs = rows.iter().collect::<Vec<_>>();
                let batch = self.decode(left, schema, &refs)?;
                if let Some(work) = &mut work {
                    work.try_resize(bytes + batch.get_array_memory_size())?;
                }
                accept(batch, rows.iter().map(|row| row.matched).collect())?;
                rows.clear();
                bytes = 0;
                if let Some(work) = &mut work {
                    work.resize(0);
                }
            }
        }
        if !rows.is_empty() {
            let refs = rows.iter().collect::<Vec<_>>();
            let batch = self.decode(left, schema, &refs)?;
            if let Some(work) = &mut work {
                work.try_resize(bytes + batch.get_array_memory_size())?;
            }
            accept(batch, rows.iter().map(|row| row.matched).collect())?;
        }
        Ok(())
    }

    pub(crate) fn visit_canonical_timers(
        &self,
        left: bool,
        mut accept: impl FnMut(&[u8], i64) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let table = CLEANUP_TIMER_TABLES[Self::table(left) as usize];
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.map_err(re)?;
            if key.len() >= 5 && key[4] == table {
                accept(&key[5..], Self::cleanup_deadline(&value)?)?;
            }
        }
        Ok(())
    }
}
