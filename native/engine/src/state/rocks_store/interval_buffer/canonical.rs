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
        self.db.visit_snapshot_batches(
            chunk,
            |key| key.len() == KEY_LEN && key[4] == Self::table(left),
            |entries| {
                if entries
                    .iter()
                    .any(|(_, value)| value.len() < VALUE_PREFIX_LEN || value[8] > 1)
                {
                    return Err(DataFusionError::Execution(
                        "invalid canonical interval-join row".into(),
                    ));
                }
                let bytes = entries
                    .iter()
                    .map(|(_, value)| 2 * value.len() + std::mem::size_of::<BufferedIntervalRow>())
                    .sum::<usize>();
                if let Some(work) = &mut work {
                    work.try_resize(bytes)?;
                }
                let rows = entries
                    .iter()
                    .map(|(key, value)| Self::buffered_row(key, value))
                    .collect::<Vec<_>>();
                let refs = rows.iter().collect::<Vec<_>>();
                let batch = self.decode(left, schema, &refs)?;
                if let Some(work) = &mut work {
                    work.try_resize(bytes + batch.get_array_memory_size())?;
                }
                accept(batch, rows.iter().map(|row| row.matched).collect())?;
                if let Some(work) = &mut work {
                    work.resize(0);
                }
                Ok(())
            },
        )
    }

    pub(crate) fn visit_canonical_timers(
        &self,
        left: bool,
        mut accept: impl FnMut(&[u8], i64) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let table = CLEANUP_TIMER_TABLES[Self::table(left) as usize];
        self.db.visit_snapshot_batches(
            1024,
            |key| key.len() >= 5 && key[4] == table,
            |rows| {
                for (key, value) in rows {
                    accept(&key[5..], Self::cleanup_deadline(&value)?)?;
                }
                Ok(())
            },
        )
    }
}
