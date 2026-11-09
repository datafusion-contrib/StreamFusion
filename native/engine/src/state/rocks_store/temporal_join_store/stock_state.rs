use super::*;
use crate::temporal_join::stock_meta::StockTemporalMeta;

fn invalid(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

const META_TABLE: u8 = 3;
pub(super) const META_FORMAT_KEY: &[u8] = b"\xff\xff\xff\xffstreamfusion-temporal-metadata";
const STOCK_INDEX_FLAG: u8 = 0x80;

fn footprint(key: &ByteKey, value: &StockTemporalMeta) -> usize {
    key.0.len()
        + GROUP_ENTRY_OVERHEAD
        + std::mem::size_of::<StockTemporalMeta>()
        + value.callbacks.len() * 32
}

impl RocksTemporalJoinStore {
    pub(crate) fn visit_canonical_metadata(
        &self,
        chunk_rows: usize,
        mut accept: impl FnMut(
            Vec<(ByteKey, Option<Box<[u8]>>, Option<i64>)>,
        ) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        for table in [META_TABLE, DEADLINE_TABLE] {
            self.db.visit_snapshot_batches(
                chunk_rows,
                |key| key.len() > KEY_PREFIX_LEN && key[4] == table,
                |rows| {
                    let mut decoded = Vec::with_capacity(rows.len());
                    for (db_key, value) in rows {
                        let key = Self::store_key(&db_key, db_key.len());
                        if table == DEADLINE_TABLE
                            && self
                                .db
                                .get(Self::table_prefix(&key, META_TABLE))
                                .map_err(re)?
                                .is_some()
                        {
                            continue;
                        }
                        let deadline = if table == DEADLINE_TABLE {
                            Some(value.as_ref())
                        } else {
                            None
                        };
                        let stored = if table == META_TABLE {
                            self.db
                                .get(Self::table_prefix(&key, DEADLINE_TABLE))
                                .map_err(re)?
                        } else {
                            None
                        };
                        let deadline = deadline
                            .or(stored.as_deref())
                            .map(|bytes| {
                                let bytes = bytes
                                    .try_into()
                                    .map_err(|_| invalid("invalid temporal cleanup deadline"))?;
                                Ok::<_, DataFusionError>(i64::from_le_bytes(bytes))
                            })
                            .transpose()?;
                        decoded.push((key, (table == META_TABLE).then_some(value), deadline));
                    }
                    if !decoded.is_empty() {
                        accept(decoded)?;
                    }
                    Ok(())
                },
            )?;
        }
        Ok(())
    }

    pub(crate) fn metadata_bytes(&self) -> usize {
        self.metadata
            .iter()
            .map(|(key, value)| footprint(key, value))
            .sum()
    }

    pub(crate) fn take_metadata_delta(&mut self) -> isize {
        std::mem::take(&mut self.metadata_delta)
    }

    pub(crate) fn all_metadata(&self) -> &HashMap<ByteKey, StockTemporalMeta> {
        &self.metadata
    }
    pub(crate) fn set_cleaning_mode(&mut self, cleaning: bool) {
        self.current_cleaning = cleaning;
    }
    pub(crate) fn source_cleaning(&self) -> Option<bool> {
        self.source_cleaning
    }

    fn touch_metadata(&mut self, keys: &[ByteKey], times: &Int64Array, left: bool) -> Vec<i64> {
        let mut indices = Vec::with_capacity(keys.len());
        for (row, key) in keys.iter().enumerate() {
            let before = self
                .metadata
                .get(key)
                .map_or(0, |value| footprint(key, value));
            let value = self.metadata.entry(key.clone()).or_default();
            indices.push(if left {
                value.probe(times.value(row))
            } else {
                value.build(times.value(row));
                0
            });
            self.metadata_delta += footprint(key, value) as isize - before as isize;
        }
        indices
    }

    fn persist_metadata(&self, keys: &[ByteKey]) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        let mut seen = HashSet::default();
        for key in keys {
            if !seen.insert(key) {
                continue;
            }
            let db_key = Self::table_prefix(key, META_TABLE);
            if let Some(value) = self.metadata.get(key) {
                writes.put(db_key, value.encode())?;
            } else {
                writes.delete(db_key)?;
            }
        }
        writes.finish()
    }

    fn edit_metadata(
        &mut self,
        key: &ByteKey,
        edit: impl FnOnce(&mut StockTemporalMeta),
    ) -> Result<(), DataFusionError> {
        let before = self
            .metadata
            .get(key)
            .map_or(0, |value| footprint(key, value));
        let value = self.metadata.entry(key.clone()).or_default();
        edit(value);
        let after = if value.next_index.is_none()
            && value.registered.is_none()
            && value.callbacks.is_empty()
        {
            0
        } else {
            footprint(key, value)
        };
        if after == 0 {
            self.metadata.remove(key);
        }
        self.metadata_delta += after as isize - before as isize;
        self.persist_metadata(std::slice::from_ref(key))
    }

    pub(crate) fn cleanup_metadata(&mut self, key: &ByteKey) -> Result<(), DataFusionError> {
        self.edit_metadata(key, StockTemporalMeta::cleanup)
    }

    pub(crate) fn take_due_callbacks(
        &mut self,
        watermark: i64,
    ) -> Result<Vec<ByteKey>, DataFusionError> {
        let due = self
            .metadata
            .iter()
            .filter(|(_, value)| {
                value
                    .callbacks
                    .first()
                    .is_some_and(|time| *time <= watermark)
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in &due {
            self.edit_metadata(key, |value| {
                value.take_due(watermark);
            })?;
        }
        Ok(due)
    }

    pub(crate) fn after_callback(
        &mut self,
        key: &ByteKey,
        last: i64,
        right: bool,
        cleaning: bool,
    ) -> Result<bool, DataFusionError> {
        let mut retained = false;
        self.edit_metadata(key, |value| {
            retained = value.after_event(last, right, cleaning);
        })?;
        Ok(retained)
    }

    pub(crate) fn import_metadata(
        &mut self,
        batch: &RecordBatch,
        precisions: &[i32],
    ) -> Result<(), DataFusionError> {
        let count = self.key_types.len();
        let columns = (0..count).collect::<Vec<_>>();
        let keys = self.entry_keys(batch, &columns, precisions);
        let values = batch
            .column(count)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("invalid temporal metadata column"))?;
        let mut decoded = Vec::with_capacity(keys.len());
        let mut seen = HashSet::default();
        for (row, key) in keys.iter().enumerate() {
            if self.metadata.contains_key(key) || !seen.insert(key) {
                return Err(invalid("duplicate temporal metadata key"));
            }
            if values.is_null(row) {
                return Err(invalid("NULL temporal metadata"));
            }
            decoded.push(
                StockTemporalMeta::decode(values.value(row)).map_err(DataFusionError::Execution)?,
            );
        }
        for (key, value) in keys.iter().zip(decoded) {
            self.metadata_delta += footprint(key, &value) as isize;
            self.metadata.insert(key.clone(), value);
        }
        self.persist_metadata(&keys)
    }

    pub(crate) fn set_source_cleaning(&mut self, value: bool) -> Result<(), DataFusionError> {
        if self
            .source_cleaning
            .is_some_and(|existing| existing != value)
        {
            return Err(invalid("restore mixes temporal retention modes"));
        }
        self.source_cleaning = Some(value);
        Ok(())
    }

    fn load_metadata_rows(&mut self) -> Result<(), DataFusionError> {
        let before = self.metadata_bytes();
        self.metadata.clear();
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.map_err(re)?;
            if key.len() > KEY_PREFIX_LEN && key[4] == META_TABLE {
                let state =
                    StockTemporalMeta::decode(&value).map_err(DataFusionError::Execution)?;
                self.metadata
                    .insert(Self::store_key(&key, key.len()), state);
            }
        }
        self.metadata_delta += self.metadata_bytes() as isize - before as isize;
        Ok(())
    }

    fn upgrade_legacy_metadata(&mut self) -> Result<(), DataFusionError> {
        let before = self.metadata_bytes();
        self.metadata.clear();
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry.map_err(re)?;
            if key.len() < KEY_PREFIX_LEN + SUFFIX_LEN {
                continue;
            }
            if key[4] == LEFT_TABLE {
                if value.len() < LEFT_VALUE_PREFIX_LEN || value[8] & STOCK_INDEX_FLAG != 0 {
                    return Err(invalid("unversioned temporal probe metadata"));
                }
                let entry_key = Self::store_key(&key, key.len() - SUFFIX_LEN);
                let time = i64::from_le_bytes(value[..8].try_into().unwrap());
                let index = self
                    .metadata
                    .entry(entry_key.clone())
                    .or_default()
                    .probe(time);
                let mut bytes = value.to_vec();
                bytes[8] |= STOCK_INDEX_FLAG;
                writes.delete(key)?;
                writes.put(
                    Self::db_key(
                        &entry_key,
                        LEFT_TABLE,
                        &((index as u64) ^ TS_SIGN_FLIP).to_be_bytes(),
                    ),
                    bytes,
                )?;
            } else if key[4] == RIGHT_TABLE {
                let entry_key = Self::store_key(&key, key.len() - SUFFIX_LEN);
                let time = Self::unflip_ts(&key[key.len() - SUFFIX_LEN..]);
                self.metadata.entry(entry_key).or_default().build(time);
            }
        }
        writes.finish()?;
        self.metadata_delta += self.metadata_bytes() as isize - before as isize;
        self.persist_metadata(&self.metadata.keys().cloned().collect::<Vec<_>>())
    }

    pub(super) fn load_or_upgrade_metadata(&mut self) -> Result<(), DataFusionError> {
        let header = self.db.get(META_FORMAT_KEY).map_err(re)?;
        if let Some(header) = header {
            if header.len() != 2 || header[0] != 1 || header[1] > 1 {
                return Err(invalid("invalid temporal metadata version"));
            }
            self.source_cleaning = Some(header[1] != 0);
            if !self.db.is_frozen() {
                self.load_metadata_rows()?;
            }
        } else {
            if self.db.is_frozen() {
                return Err(invalid(
                    "canonical reader requires initialized temporal metadata",
                ));
            }
            self.upgrade_legacy_metadata()?;
            self.db.put(META_FORMAT_KEY, [1, 0]).map_err(re)?;
        }
        Ok(())
    }

    pub(super) fn write_left(
        &mut self,
        batch: &RecordBatch,
        keys: &[ByteKey],
        times: &Int64Array,
        kinds: Option<&Int8Array>,
        sequences: Option<&Int64Array>,
    ) -> Result<(), DataFusionError> {
        let indices = sequences
            .map(|values| values.values().to_vec())
            .unwrap_or_else(|| self.touch_metadata(keys, times, true));
        let arity = self.row_arities[0];
        let rows = self
            .converters
            .0
            .convert_columns(&batch.columns()[..arity])?;
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (index, row) in rows.iter().enumerate() {
            let mut value = Vec::with_capacity(LEFT_VALUE_PREFIX_LEN + row.data().len());
            value.extend_from_slice(&times.value(index).to_le_bytes());
            value.push(kinds.map_or(0, |values| values.value(index) as u8) | STOCK_INDEX_FLAG);
            value.extend_from_slice(row.data());
            writes.put(
                Self::db_key(
                    &keys[index],
                    LEFT_TABLE,
                    &((indices[index] as u64) ^ TS_SIGN_FLIP).to_be_bytes(),
                ),
                value,
            )?;
        }
        writes.finish()?;
        if sequences.is_none() {
            self.persist_metadata(keys)?;
        }
        Ok(())
    }

    pub(super) fn write_right(
        &mut self,
        batch: &RecordBatch,
        keys: &[ByteKey],
        times: &Int64Array,
        kinds: Option<&Int8Array>,
        touch: bool,
    ) -> Result<(), DataFusionError> {
        if touch {
            self.touch_metadata(keys, times, false);
        }
        let arity = self.row_arities[1];
        let rows = self
            .converters
            .1
            .convert_columns(&batch.columns()[..arity])?;
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (index, row) in rows.iter().enumerate() {
            let mut value = Vec::with_capacity(RIGHT_VALUE_PREFIX_LEN + row.data().len());
            value.push(kinds.map_or(0, |values| values.value(index)) as u8);
            value.extend_from_slice(row.data());
            writes.put(
                Self::db_key(
                    &keys[index],
                    RIGHT_TABLE,
                    &Self::flip_ts(times.value(index)),
                ),
                value,
            )?;
        }
        writes.finish()?;
        if touch {
            self.persist_metadata(keys)?;
        }
        Ok(())
    }
}

impl RocksTemporalJoinStore {
    pub(super) fn restore_merged_metadata(
        &mut self,
        headers: &[Option<Vec<u8>>],
    ) -> Result<(), DataFusionError> {
        if headers.is_empty() {
            return Ok(());
        }
        if headers.iter().any(|header| header != &headers[0]) {
            return Err(invalid(
                "restore mixes temporal metadata versions or retention modes",
            ));
        }
        if let Some(header) = &headers[0] {
            if header.len() != 2 || header[0] != 1 || header[1] > 1 {
                return Err(invalid("invalid temporal metadata version"));
            }
            self.source_cleaning = Some(header[1] != 0);
            self.load_metadata_rows()?;
            self.db.put(META_FORMAT_KEY, header).map_err(re)?;
        } else {
            self.source_cleaning = None;
            self.upgrade_legacy_metadata()?;
            self.db.put(META_FORMAT_KEY, [1, 0]).map_err(re)?;
        }
        Ok(())
    }
}
