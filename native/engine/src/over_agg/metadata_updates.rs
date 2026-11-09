use super::*;

#[derive(Default)]
pub(super) struct MetadataUpdates {
    keys: ahash::HashSet<ByteKey>,
    pub(super) bytes: usize,
}

impl MetadataUpdates {
    pub(super) fn mark(&mut self, key: &[u8]) {
        if self.keys.insert(ByteKey::from(key)) {
            self.bytes += byte_key_bytes(key);
        }
    }
}

#[derive(Clone, Copy)]
struct MetadataTables {
    cleanup: bool,
    timers: bool,
    counters: bool,
}

impl OverWindowAggregator {
    pub(super) fn mark_store_metadata(&mut self, key: &[u8]) {
        if self.store.is_none() {
            return;
        }
        let tables = self.metadata_tables();
        if tables.cleanup || tables.timers || tables.counters {
            self.metadata_updates.mark(key);
        }
    }

    fn metadata_tables(&self) -> MetadataTables {
        MetadataTables {
            cleanup: self.retention_scheme() == RetentionScheme::Deadline
                || self.range_offset().is_some(),
            timers: self.range_offset().is_some() || self.event_polling,
            counters: self.processing_rows(),
        }
    }

    fn metadata_entries(
        &self,
        key: &[u8],
        tables: MetadataTables,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let store = self.store.as_ref().expect("OVER store");
        let group = self.store_fold_key_group(key);
        let mut rows = Vec::with_capacity(3);
        if tables.cleanup {
            let value = self.cleanup_state.get(key).map(|stamp| {
                let mut bytes = stamp.to_le_bytes().to_vec();
                if self.event_cleanup_unarmed.contains(key) {
                    bytes.push(0);
                }
                bytes
            });
            rows.push((store.stamp_key(group, key), value));
        }
        if tables.timers {
            rows.push((
                store.range_timer_key(group, key),
                self.range_timers.get(key).map(RangeTimers::encode),
            ));
        }
        if tables.counters {
            rows.push((
                store.proc_rows_key(group, key),
                self.proc_rows.get(key).map(ProcRowsState::encode),
            ));
        }
        rows
    }

    fn persist_metadata_keys<'a>(
        &self,
        keys: impl Iterator<Item = &'a [u8]>,
        tables: MetadataTables,
    ) -> Result<(), DataFusionError> {
        let store = self.store.as_ref().expect("OVER store");
        let mut entries = Vec::new();
        let mut bytes = 0usize;
        let mut work = self.memory.temporary_reservation();
        for key in keys {
            for entry in self.metadata_entries(key, tables) {
                bytes = bytes.saturating_add(entry.0.len() + entry.1.as_ref().map_or(0, Vec::len));
                let capacity = if entries.len() == entries.capacity() {
                    (entries.len() + 1).next_power_of_two().max(4)
                } else {
                    entries.capacity()
                };
                if let Some(work) = &mut work {
                    work.try_resize(bytes + capacity * std::mem::size_of_val(&entry))?;
                }
                if capacity > entries.capacity() {
                    entries.reserve_exact(capacity - entries.len());
                }
                entries.push(entry);
                if entries.len() == 1024 || bytes >= 4 * 1024 * 1024 {
                    store.write_metadata(&entries)?;
                    entries.clear();
                    bytes = 0;
                    if let Some(work) = &mut work {
                        work.resize(
                            entries.capacity() * std::mem::size_of::<(Vec<u8>, Option<Vec<u8>>)>(),
                        );
                    }
                }
            }
        }
        store.write_metadata(&entries)
    }

    pub(super) fn flush_metadata_updates(&mut self) -> Result<(), DataFusionError> {
        if self.metadata_updates.keys.is_empty() {
            return Ok(());
        }
        if self.memory.tracking() {
            self.memory
                .set(self.retained_state_bytes() + self.metadata_updates.bytes);
            self.memory.account()?;
        }
        let updates = std::mem::take(&mut self.metadata_updates);
        let result = self.persist_metadata_keys(
            updates.keys.iter().map(|key| key.0.as_ref()),
            self.metadata_tables(),
        );
        match result {
            Err(error) => {
                self.metadata_updates = updates;
                Err(error)
            }
            Ok(()) => {
                drop(updates);
                if self.memory.tracking() {
                    self.memory.set(self.retained_state_bytes());
                    self.memory.account()?;
                }
                Ok(())
            }
        }
    }

    /// Native restore migrates old metadata once; subsequent checkpoints only flush changed keys.
    pub(super) fn reconcile_store_metadata(&mut self) -> Result<(), DataFusionError> {
        self.metadata_updates = MetadataUpdates::default();
        let mut tables = self.metadata_tables();
        // A V1 constructor binds its persisted polling ledger after ordinary retention hydration.
        tables.timers = self.range_offset().is_some();
        self.store
            .as_ref()
            .unwrap()
            .clear_metadata(tables.timers, tables.counters)?;
        self.persist_metadata_keys(
            self.cleanup_state
                .keys()
                .chain(self.range_timers.keys())
                .chain(self.proc_rows.keys())
                .map(|key| key.0.as_ref()),
            tables,
        )
    }

    pub(super) fn persist_restored_protocol(
        &self,
        keys: &arrow::row::Rows,
    ) -> Result<(), DataFusionError> {
        let mut tables = self.metadata_tables();
        tables.cleanup = false;
        self.persist_metadata_keys(keys.iter().map(|key| key.data()), tables)
    }
}

#[cfg(test)]
mod tests;
