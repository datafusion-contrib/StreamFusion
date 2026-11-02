use super::*;

impl RocksWindowBuffer {
    /// Group-major scans preserve each key/window list's original arrival order.
    pub(crate) fn visit_canonical_rows(
        &self,
        left: bool,
        schema: &SchemaRef,
        chunk: usize,
        mut work: Option<MemoryReservation>,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let mut rows: Vec<Box<[u8]>> = Vec::with_capacity(chunk);
        let mut bytes = 0;
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() != KEY_LEN || key[4] != Self::table(left) {
                continue;
            }
            if value.len() < 8 {
                return Err(DataFusionError::Execution(
                    "truncated window-join row".into(),
                ));
            }
            bytes += value.len() - 8;
            if let Some(work) = &mut work {
                work.try_resize(bytes)?;
            }
            rows.push(value[8..].into());
            if rows.len() == chunk {
                let batch = self.decode(left, schema, rows.iter().map(AsRef::as_ref))?;
                if let Some(work) = &mut work {
                    work.try_resize(bytes + batch.get_array_memory_size())?;
                }
                accept(batch)?;
                rows.clear();
                bytes = 0;
                if let Some(work) = &mut work {
                    work.resize(0);
                }
            }
        }
        if !rows.is_empty() {
            let batch = self.decode(left, schema, rows.iter().map(AsRef::as_ref))?;
            if let Some(work) = &mut work {
                work.try_resize(bytes + batch.get_array_memory_size())?;
            }
            accept(batch)?;
        }
        Ok(())
    }
}
