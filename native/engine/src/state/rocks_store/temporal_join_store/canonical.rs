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
        let mut rows = Vec::<Box<[u8]>>::with_capacity(chunk);
        let mut indices = Vec::with_capacity(chunk);
        let mut kinds = Vec::with_capacity(chunk);
        let mut bytes = 0;
        let mut flush = |rows: &mut Vec<Box<[u8]>>,
                         indices: &mut Vec<i64>,
                         kinds: &mut Vec<i8>,
                         bytes: &mut usize,
                         work: &mut Option<MemoryReservation>|
         -> Result<(), DataFusionError> {
            if rows.is_empty() {
                return Ok(());
            }
            let data = self.decode(left, schema, rows.iter().map(AsRef::as_ref))?;
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
            columns.push(Arc::new(Int64Array::from(std::mem::take(indices))));
            columns.push(Arc::new(Int8Array::from(std::mem::take(kinds))));
            let data = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
            if let Some(work) = work.as_mut() {
                work.try_resize(*bytes + data.get_array_memory_size())?;
            }
            accept(data)?;
            rows.clear();
            *bytes = 0;
            if let Some(work) = work.as_mut() {
                work.resize(0);
            }
            Ok(())
        };
        let table = if left { LEFT_TABLE } else { RIGHT_TABLE };
        let prefix = if left {
            LEFT_VALUE_PREFIX_LEN
        } else {
            RIGHT_VALUE_PREFIX_LEN
        };
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.map_err(re)?;
            if key.len() < KEY_PREFIX_LEN + SUFFIX_LEN || key[4] != table {
                continue;
            }
            if value.len() < prefix || left && value[8] & 0x80 == 0 {
                return Err(DataFusionError::Execution(
                    "invalid temporal canonical retained row".into(),
                ));
            }
            let index = Self::unflip_ts(&key[key.len() - SUFFIX_LEN..]);
            let kind = value[if left { 8 } else { 0 }] & 0x7f;
            if kind > 3 {
                return Err(DataFusionError::Execution(
                    "invalid temporal canonical row kind".into(),
                ));
            }
            bytes += value.len() + 17;
            if let Some(work) = work.as_mut() {
                work.try_resize(bytes)?;
            }
            rows.push(value[prefix..].into());
            indices.push(index);
            kinds.push(kind as i8);
            if rows.len() == chunk {
                flush(&mut rows, &mut indices, &mut kinds, &mut bytes, &mut work)?;
            }
        }
        flush(&mut rows, &mut indices, &mut kinds, &mut bytes, &mut work)
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
