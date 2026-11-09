use super::*;

pub(crate) type CanonicalMultisetEntry = (ByteKey, ScalarValue, i64);
pub(crate) type CanonicalTimedMultisetEntry = (ByteKey, ScalarValue, i64, i64);

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical RocksDB transfer: {message}"))
}

impl<C: RocksStateCodec> RocksStore<C> {
    pub(crate) fn canonical_multiset_position(&self, table: u8) -> Option<usize> {
        self.multisets.iter().position(|entry| entry.table == table)
    }

    /// Visits main rows without hydrating companion tables or changing persisted TTL ages.
    pub(crate) fn for_each_canonical_header_batch(
        &mut self,
        chunk_rows: usize,
        mut consume: impl FnMut(&[(ByteKey, C::Value)]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if chunk_rows == 0 {
            return Err(invalid("zero header chunk size"));
        }
        self.checkpoint("")?;
        let prefix = self.key_prefix_len();
        self.db.visit_snapshot_batches(
            chunk_rows,
            |key| {
                key.len() >= prefix
                    && !self.table.is_some_and(|table| key[4] != table)
                    && i32::from_be_bytes(key[..4].try_into().unwrap()) >= 0
            },
            |entries| {
                let values = entries
                    .iter()
                    .map(|(_, value)| value.as_ref())
                    .collect::<Vec<_>>();
                let decoded = self.decode_values_with_visibility(&values, false)?;
                let rows = entries
                    .into_iter()
                    .zip(decoded)
                    .filter_map(|((key, _), value)| {
                        value.map(|value| (ByteKey::from(&key[prefix..]), value))
                    })
                    .collect::<Vec<_>>();
                if !rows.is_empty() {
                    consume(&rows)?;
                }
                Ok(())
            },
        )
    }

    /// Visits raw persisted values without reconstructing their execution containers.
    pub(crate) fn for_each_canonical_raw(
        &mut self,
        work: Option<datafusion::execution::memory_pool::MemoryReservation>,
        mut consume: impl FnMut(&[u8], &[u8]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if !self.codec.raw() {
            return Err(invalid("raw canonical scan requires a raw codec"));
        }
        self.checkpoint("")?;
        let prefix = self.key_prefix_len();
        let value_prefix = self.value_prefix_len();
        self.db.visit_snapshot_raw(
            work,
            |key| {
                key.len() >= prefix
                    && !self.table.is_some_and(|table| key[4] != table)
                    && i32::from_be_bytes(key[..4].try_into().unwrap()) >= 0
            },
            |key, value| {
                let bytes = value
                    .get(value_prefix..)
                    .ok_or_else(|| invalid("truncated raw value prefix"))?;
                consume(&key[prefix..], bytes)?;
                Ok(())
            },
        )
    }

    /// Streams one companion table by its main-row keys, retaining bounded decoded frames.
    pub(crate) fn for_each_canonical_multiset_batch(
        &mut self,
        position: usize,
        chunk_rows: usize,
        consume: impl FnMut(&[CanonicalMultisetEntry]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if position < self.multisets.len() && self.codec.multiset_has_timestamps(position) {
            return Err(invalid(
                "timestamped entries require an age-preserving visitor",
            ));
        }
        self.visit_canonical_multiset(
            position,
            chunk_rows,
            |key, value, count, _| (key, value, count),
            consume,
        )
    }

    pub(crate) fn for_each_canonical_timed_multiset_batch(
        &mut self,
        position: usize,
        chunk_rows: usize,
        consume: impl FnMut(&[CanonicalTimedMultisetEntry]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.visit_canonical_multiset(
            position,
            chunk_rows,
            |key, value, count, written| (key, value, count, written),
            consume,
        )
    }

    fn visit_canonical_multiset<T>(
        &mut self,
        position: usize,
        chunk_rows: usize,
        record: impl Fn(ByteKey, ScalarValue, i64, i64) -> T,
        mut consume: impl FnMut(&[T]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if chunk_rows == 0 {
            return Err(invalid("zero element chunk size"));
        }
        if position >= self.multisets.len() {
            return Err(invalid("unknown companion table"));
        }
        self.checkpoint("")?;
        let table = &self.multisets[position];
        let db = self.db.read_snapshot();
        let main_prefix = self.key_prefix_len();
        let mut batch = Vec::with_capacity(chunk_rows);
        let mut bytes = 0usize;
        for entry in db.iterator(IteratorMode::Start) {
            database::check_cancelled()?;
            let (key, value) = entry.map_err(re)?;
            if key.len() < main_prefix
                || self.table.is_some_and(|table| key[4] != table)
                || i32::from_be_bytes(key[..4].try_into().unwrap()) < 0
            {
                continue;
            }
            if self
                .decode_values_with_visibility(&[value.as_ref()], false)?
                .pop()
                .flatten()
                .is_none()
            {
                continue;
            }
            let owner = ByteKey::from(&key[main_prefix..]);
            let prefix = self.multiset_prefix(table.table, &owner.0);
            for entry in db.iterator(IteratorMode::From(&prefix, Direction::Forward)) {
                database::check_cancelled()?;
                let (key, value) = entry.map_err(re)?;
                if !key.starts_with(&prefix) {
                    break;
                }
                let (count, written) =
                    decode_companion_count(&value, self.codec.multiset_has_timestamps(position))?;
                let element = table.decode_element(&key[prefix.len()..])?;
                bytes = bytes
                    .saturating_add(owner.0.len())
                    .saturating_add(key.len())
                    .saturating_add(value.len());
                batch.push(record(owner.clone(), element, count, written.unwrap_or(0)));
                if batch.len() == chunk_rows || bytes >= 4 * 1024 * 1024 {
                    consume(&batch)?;
                    batch.clear();
                    bytes = 0;
                }
            }
        }
        if !batch.is_empty() {
            consume(&batch)?;
        }
        Ok(())
    }

    /// Installs fresh companion entries directly after their validated main rows.
    pub(crate) fn import_canonical_multiset_batch(
        &mut self,
        position: usize,
        entries: &[CanonicalMultisetEntry],
    ) -> Result<(), DataFusionError> {
        if position < self.multisets.len() && self.codec.multiset_has_timestamps(position) {
            return Err(invalid(
                "timestamped entries require an age-preserving import",
            ));
        }
        let entries = entries
            .iter()
            .map(|(key, value, count)| (key.clone(), value.clone(), *count, 0))
            .collect::<Vec<_>>();
        self.import_canonical_timed_multiset_batch(position, &entries)
    }

    pub(crate) fn import_canonical_timed_multiset_batch(
        &mut self,
        position: usize,
        entries: &[CanonicalTimedMultisetEntry],
    ) -> Result<(), DataFusionError> {
        self.transfer_canonical_timed_multiset_batch(position, entries, true, true)
    }

    pub(crate) fn validate_canonical_timed_multiset_batch(
        &mut self,
        position: usize,
        entries: &[CanonicalTimedMultisetEntry],
        require_header: bool,
    ) -> Result<(), DataFusionError> {
        self.transfer_canonical_timed_multiset_batch(position, entries, false, require_header)
    }

    fn transfer_canonical_timed_multiset_batch(
        &mut self,
        position: usize,
        entries: &[CanonicalTimedMultisetEntry],
        apply: bool,
        require_header: bool,
    ) -> Result<(), DataFusionError> {
        if entries.len() > 1024 {
            return Err(invalid("oversized element frame"));
        }
        if position >= self.multisets.len() {
            return Err(invalid("unknown companion table"));
        }
        self.checkpoint("")?;
        let table = &self.multisets[position];
        let mut scalars = Vec::with_capacity(entries.len());
        for (key, value, _, written) in entries {
            if !self.codec.multiset_has_timestamps(position) && *written != 0 {
                return Err(invalid("untimestamped companion carries an age"));
            }
            if value.data_type() != table.element_type {
                return Err(invalid("invalid companion element type"));
            }
            let main = self.db.get(self.db_key(&key.0)).map_err(re)?;
            if require_header && main.is_none() {
                return Err(invalid("orphan companion entry"));
            }
            if let Some(main) = main {
                if self
                    .decode_values_with_visibility(&[main.as_ref()], false)?
                    .pop()
                    .flatten()
                    .is_none()
                {
                    return Err(invalid("companion entry has no main state"));
                }
            }
            scalars
                .push(crate::flink_float::canonical_scalar(value).unwrap_or_else(|| value.clone()));
        }
        let values = scalars_to_array(scalars, &table.element_type);
        let rows = table
            .converter
            .convert_columns(&[values])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut seen = ahash::HashSet::default();
        let mut writes = WriteBatch::default();
        for (row, (owner, _, count, written)) in rows.iter().zip(entries) {
            let mut key = self.multiset_prefix(table.table, &owner.0);
            key.extend_from_slice(row.data());
            if !seen.insert(key.clone()) || self.db.get(&key).map_err(re)?.is_some() {
                return Err(invalid("duplicate companion entry"));
            }
            writes.put(
                key,
                encode_companion_count(
                    *count,
                    self.codec
                        .multiset_has_timestamps(position)
                        .then_some(*written),
                ),
            );
        }
        if apply && !writes.is_empty() {
            self.db
                .write_opt(writes, &flink_write_options())
                .map_err(re)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
