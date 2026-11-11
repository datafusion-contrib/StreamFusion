use super::*;
use datafusion::execution::memory_pool::MemoryReservation;

impl RocksTemporalJoinStore {
    pub(crate) fn visit_canonical_rows(
        &self,
        left: bool,
        schema: &SchemaRef,
        chunk: usize,
        mut work: Option<MemoryReservation>,
        mut accept: impl FnMut(RecordBatch) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let table = if left { LEFT_TABLE } else { RIGHT_TABLE };
        let prefix = if left {
            LEFT_VALUE_PREFIX_LEN
        } else {
            RIGHT_VALUE_PREFIX_LEN
        };
        self.db.visit_snapshot_batches(
            chunk,
            |key| key.len() >= KEY_PREFIX_LEN + SUFFIX_LEN && key[4] == table,
            |rows| {
                let mut indices = Vec::with_capacity(rows.len());
                let mut kinds = Vec::with_capacity(rows.len());
                for (key, value) in &rows {
                    if value.len() < prefix || left && value[8] & 0x80 == 0 {
                        return Err(DataFusionError::Execution(
                            "invalid temporal canonical retained row".into(),
                        ));
                    }
                    let kind = value[if left { 8 } else { 0 }] & 0x7f;
                    if kind > 3 {
                        return Err(DataFusionError::Execution(
                            "invalid temporal canonical row kind".into(),
                        ));
                    }
                    indices.push(Self::unflip_ts(&key[key.len() - SUFFIX_LEN..]));
                    kinds.push(kind as i8);
                }
                let bytes = rows
                    .iter()
                    .map(|(_, value)| value.len() + 17)
                    .sum::<usize>();
                if let Some(work) = &mut work {
                    work.try_resize(bytes)?;
                }
                let data =
                    self.decode(left, schema, rows.iter().map(|(_, value)| &value[prefix..]))?;
                let mut fields = data.schema().fields().to_vec();
                fields.push(Arc::new(Field::new(
                    "__canonical_index__",
                    DataType::Int64,
                    false,
                )));
                fields.push(Arc::new(Field::new(
                    "__canonical_kind__",
                    DataType::Int8,
                    false,
                )));
                let mut columns = data.columns().to_vec();
                columns.push(Arc::new(Int64Array::from(indices)));
                columns.push(Arc::new(Int8Array::from(kinds)));
                let data = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
                if let Some(work) = &mut work {
                    work.try_resize(bytes + data.get_array_memory_size())?;
                }
                accept(data)?;
                if let Some(work) = &mut work {
                    work.resize(0);
                }
                Ok(())
            },
        )
    }

    pub(crate) fn validate_canonical_row_keys(
        &self,
        left: bool,
        keys: &[ByteKey],
        indices: &Int64Array,
    ) -> Result<(), DataFusionError> {
        let table = if left { LEFT_TABLE } else { RIGHT_TABLE };
        for (row, key) in keys.iter().enumerate() {
            let suffix = Self::flip_ts(indices.value(row));
            if self
                .db
                .get(Self::db_key(key, table, &suffix))
                .map_err(re)?
                .is_some()
            {
                return Err(DataFusionError::Execution(
                    "temporal retained row already imported".into(),
                ));
            }
        }
        Ok(())
    }
}
