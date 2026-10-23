use super::*;

/// Record-level tables with the shared Flink RocksDB options, pool and checkpoint lifecycle.
pub(crate) struct RocksRawStore {
    pub(crate) db: Arc<DB>,
    _cache: Option<Cache>,
    generation: i64,
    write_batch_size: usize,
}

impl RocksRawStore {
    pub(crate) fn open(
        config: RocksStoreConfig,
        sources: &[(String, i64)],
        groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        if aligned && sources.len() == 1 {
            copy_checkpoint_db(&sources[0].0, &config.table_dir)?;
        }
        let opened = open_shared_db(&config, &[(None, 0)])?;
        let store = Self {
            db: opened.db,
            _cache: opened.cache,
            generation: sources.iter().map(|s| s.1).max().unwrap_or(0),
            write_batch_size: opened.write_batch_size,
        };
        if !(aligned && sources.len() == 1) {
            let mut writes = FlinkWriteBatch::new(&store.db, store.write_batch_size);
            for (source, _) in sources {
                let db = DB::open_for_read_only(&Options::default(), source, false).map_err(re)?;
                for item in db.iterator(IteratorMode::Start) {
                    let (key, value) = item.map_err(re)?;
                    if key.len() >= 4 {
                        let group = i32::from_be_bytes(key[..4].try_into().unwrap());
                        if group == -1 || groups.contains(&group) {
                            writes.put(key, value)?;
                        }
                    }
                }
            }
            writes.finish()?;
        }
        Ok(store)
    }

    pub(crate) fn apply(
        &self,
        changes: impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    ) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (key, value) in changes {
            if let Some(value) = value {
                writes.put(key, value)?;
            } else {
                writes.delete(key)?;
            }
        }
        writes.finish()
    }

    pub(crate) fn prefix_end(prefix: &[u8]) -> Vec<u8> {
        let mut end = prefix.to_vec();
        let mut i = end.len();
        while i > 0 {
            i -= 1;
            if end[i] != 255 {
                end[i] += 1;
                end.truncate(i + 1);
                break;
            }
        }
        end
    }

    pub(crate) fn clear_prefix(&self, prefix: &[u8]) -> Result<(), DataFusionError> {
        let end = Self::prefix_end(prefix);
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        writes.delete_range(prefix, &end)?;
        writes.finish()
    }

    pub(crate) fn checkpoint(
        &mut self,
        directory: &str,
    ) -> Result<RocksCheckpointManifest, DataFusionError> {
        if directory.is_empty() {
            return Ok(RocksCheckpointManifest::absent());
        }
        self.generation += 1;
        checkpoint_files(&self.db, directory, self.generation)
    }
}
