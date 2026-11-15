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
        self.db.visit_snapshot_batches(
            chunk,
            |key| key.len() == KEY_LEN && key[4] == Self::table(left),
            |rows| {
                if rows.iter().any(|(_, value)| value.len() < 8) {
                    return Err(DataFusionError::Execution(
                        "truncated window-join row".into(),
                    ));
                }
                let bytes = rows.iter().map(|(_, value)| value.len()).sum::<usize>();
                if let Some(work) = &mut work {
                    work.try_resize(bytes)?;
                }
                let batch = self.decode(left, schema, rows.iter().map(|(_, value)| &value[8..]))?;
                if let Some(work) = &mut work {
                    work.try_resize(bytes + batch.get_array_memory_size())?;
                }
                accept(batch)?;
                if let Some(work) = &mut work {
                    work.resize(0);
                }
                Ok(())
            },
        )
    }
}
