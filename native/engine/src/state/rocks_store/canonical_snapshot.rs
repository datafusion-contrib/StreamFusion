use super::*;
/// An owned MVCC view with bounded forward reads in native key-group order.
pub(crate) struct RocksReadSnapshot {
    view: Arc<database::ReadView>,
    after: Option<Vec<u8>>,
    finished: bool,
}

impl RocksReadSnapshot {
    pub(crate) fn new(db: &StateDb) -> Self {
        Self {
            view: db.pin(),
            after: None,
            finished: false,
        }
    }

    pub(crate) fn next_batch(
        &mut self,
        accept: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>, DataFusionError> {
        self.next_batch_limited(1024, 4 * 1024 * 1024, accept)
    }

    pub(crate) fn next_batch_limited(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        accept: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>, DataFusionError> {
        if max_rows == 0 || max_bytes == 0 {
            return Err(DataFusionError::Execution(
                "canonical scan requires positive bounds".into(),
            ));
        }
        if self.finished {
            return Ok(Vec::new());
        }
        let after = self.after.clone();
        let mode = after.as_ref().map_or(IteratorMode::Start, |key| {
            IteratorMode::From(key, Direction::Forward)
        });
        let mut rows = Vec::new();
        let mut bytes = 0;
        for entry in self.view.iterator(mode) {
            database::check_cancelled()?;
            let (key, value) = entry.map_err(re)?;
            if after
                .as_ref()
                .is_some_and(|previous| key.as_ref() <= previous.as_slice())
            {
                continue;
            }
            if !accept(&key) {
                continue;
            }
            bytes += key.len() + value.len();
            self.after = Some(key.to_vec());
            rows.push((key, value));
            if rows.len() == max_rows || bytes >= max_bytes {
                break;
            }
        }
        self.finished = rows.is_empty();
        Ok(rows)
    }
}

/// Captures decoder configuration without copying the live operator's state or caches.
pub(crate) struct RocksCanonicalSnapshot<C: RocksStateCodec> {
    snapshot: RocksReadSnapshot,
    decoder: RocksStore<C>,
}

impl<C: RocksStateCodec + Clone> RocksStore<C> {
    pub(crate) fn canonical_snapshot(
        &mut self,
    ) -> Result<RocksCanonicalSnapshot<C>, DataFusionError> {
        self.checkpoint("")?;
        let opened = OpenedDb {
            db: self.db.clone(),
            cache: self._cache.clone(),
            clock: Arc::clone(&self.clock),
            write_batch_size: self.write_batch_size,
        };
        let decoder = Self::attach(&opened, self.config.clone(), self.codec.clone(), self.table)?;
        Ok(RocksCanonicalSnapshot {
            snapshot: RocksReadSnapshot::new(&self.db),
            decoder,
        })
    }
}

impl<C: RocksStateCodec> RocksCanonicalSnapshot<C> {
    pub(crate) fn next_batch(&mut self) -> Result<Vec<(ByteKey, C::Value)>, DataFusionError> {
        let prefix = self.decoder.key_prefix_len();
        let table = self.decoder.table;
        let entries = self.snapshot.next_batch(|key| {
            key.len() >= prefix
                && i32::from_be_bytes(key[..4].try_into().unwrap()) >= 0
                && !table.is_some_and(|table| key[4] != table)
        })?;
        let values: Vec<_> = entries.iter().map(|(_, value)| value.as_ref()).collect();
        let decoded = self.decoder.decode_values_with_visibility(&values, false)?;
        Ok(entries
            .into_iter()
            .zip(decoded)
            .filter_map(|((key, _), value)| {
                value.map(|value| (ByteKey::from(&key[prefix..]), value))
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_compaction_cannot_remove_a_captured_version() {
        let directory = tempfile::tempdir().unwrap();
        let config = RocksStoreConfig {
            table_dir: directory
                .path()
                .join("state")
                .to_string_lossy()
                .into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 1000,
            shared_resources: 0,
        };
        let opened = open_shared_db(&config, &[(None, 1000)]).unwrap();
        let key = 7i32.to_be_bytes();
        let mut value = 1500i64.to_le_bytes().to_vec();
        value.extend(2i32.to_le_bytes());
        opened.clock.store(1500, Ordering::Release);
        opened.db.put(key, &value).unwrap();
        opened.db.flush().unwrap();
        let mut snapshot = RocksReadSnapshot::new(&opened.db);
        opened.clock.store(10_000, Ordering::Release);
        opened.db.compact_range(None::<&[u8]>, None::<&[u8]>);
        assert_eq!(
            snapshot.next_batch(|_| true).unwrap()[0].1.as_ref(),
            value.as_slice()
        );
        drop(snapshot);
        opened.db.compact_range(None::<&[u8]>, None::<&[u8]>);
        assert!(
            opened.db.get(key).unwrap().is_none(),
            "TTL compaction resumes after the last reader closes"
        );
    }

    #[test]
    fn pinned_chunks_survive_updates_deletes_compaction_and_owner_close() {
        let directory = tempfile::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        let raw = Arc::new(
            DB::open_cf_with_opts(&options, directory.path(), [("default", options.clone())])
                .unwrap(),
        );
        let db = StateDb::live(
            raw,
            None,
            Arc::new(StateClock::new(0)),
            1024,
            directory.path(),
        );
        for index in 0u32..2051 {
            db.put(index.to_be_bytes(), index.to_le_bytes()).unwrap();
        }
        db.flush().unwrap();
        let mut snapshot = RocksReadSnapshot::new(&db);
        for index in 0u32..2051 {
            if index % 2 == 0 {
                db.delete(index.to_be_bytes()).unwrap();
            } else {
                db.put(index.to_be_bytes(), b"replacement").unwrap();
            }
        }
        db.flush().unwrap();
        db.compact_range(None::<&[u8]>, None::<&[u8]>);
        drop(db);
        let mut count = 0u32;
        loop {
            let chunk = snapshot.next_batch(|_| true).unwrap();
            assert!(chunk.len() <= 1024);
            if chunk.is_empty() {
                break;
            }
            for (key, value) in chunk {
                assert_eq!(key.as_ref(), count.to_be_bytes());
                assert_eq!(value.as_ref(), count.to_le_bytes());
                count += 1;
            }
        }
        assert_eq!(count, 2051);
        drop(snapshot);
        let reopened = DB::open(&options, directory.path()).unwrap();
        assert_eq!(
            reopened.get(1u32.to_be_bytes()).unwrap().as_deref(),
            Some(b"replacement".as_slice())
        );
        assert!(reopened.get(0u32.to_be_bytes()).unwrap().is_none());
    }
}
