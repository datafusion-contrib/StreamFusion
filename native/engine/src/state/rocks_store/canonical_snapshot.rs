use super::*;

const RAW_ENTRY_BYTES: usize = std::mem::size_of::<(Box<[u8]>, Box<[u8]>)>();
/// An owned MVCC view with bounded forward reads in native key-group order.
pub(crate) struct RocksReadSnapshot {
    view: Arc<database::ReadView>,
    after: Option<Vec<u8>>,
    finished: bool,
    work: Option<MemoryReservation>,
    decoded_per_row: usize,
    decoded_multiplier: usize,
}

impl RocksReadSnapshot {
    pub(crate) fn new(db: &StateDb) -> Self {
        Self {
            view: db.pin(),
            after: None,
            finished: false,
            work: crate::state::canonical_context::temporary_reservation(),
            decoded_per_row: 0,
            decoded_multiplier: 1,
        }
    }

    pub(crate) fn with_memory(mut self, work: Option<MemoryReservation>) -> Self {
        if work.is_some() {
            self.work = work;
        }
        self
    }

    fn with_decoding_cost(mut self, per_row: usize, multiplier: usize) -> Self {
        self.decoded_per_row = per_row;
        self.decoded_multiplier = multiplier;
        self
    }

    pub(crate) fn release_batch_memory(&mut self) {
        if let Some(work) = &mut self.work {
            work.resize(self.after.as_ref().map_or(0, Vec::capacity));
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
        self.release_batch_memory();
        if self.finished {
            return Ok(Vec::new());
        }
        let result = self.read_batch(max_rows, max_bytes, accept);
        if result.is_err() {
            self.release_batch_memory();
        }
        result
    }

    fn read_batch(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        accept: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>, DataFusionError> {
        let mut reader = self.view.raw_iterator();
        if let Some(after) = &self.after {
            reader.seek(after);
            if reader.key().is_some_and(|key| key == after) {
                reader.next();
            }
        } else {
            reader.seek_to_first();
        }
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        while reader.valid() {
            database::check_cancelled()?;
            let key = reader.key().unwrap();
            if accept(key) {
                let value = reader.value().unwrap();
                let next = bytes.saturating_add(key.len()).saturating_add(value.len());
                if !rows.is_empty() && next > max_bytes {
                    break;
                }
                let capacity = if rows.len() == rows.capacity() {
                    rows.capacity().saturating_mul(2).max(4).min(max_rows)
                } else {
                    rows.capacity()
                };
                if let Some(work) = &mut self.work {
                    let cursor = self.after.as_ref().map_or(0, Vec::capacity);
                    work.try_resize(
                        next.saturating_mul(self.decoded_multiplier)
                            .saturating_add((rows.len() + 1).saturating_mul(self.decoded_per_row))
                            .saturating_add(capacity.saturating_mul(RAW_ENTRY_BYTES))
                            .saturating_add(cursor)
                            .saturating_add(key.len()),
                    )?;
                }
                if capacity > rows.capacity() {
                    rows.try_reserve_exact(capacity - rows.len())
                        .map_err(|error| DataFusionError::ResourcesExhausted(error.to_string()))?;
                }
                rows.push((Box::<[u8]>::from(key), Box::<[u8]>::from(value)));
                bytes = next;
                if rows.len() == max_rows || bytes >= max_bytes {
                    break;
                }
            }
            reader.next();
        }
        reader.status().map_err(re)?;
        if let Some((key, _)) = rows.last() {
            self.after = Some(key.to_vec());
        } else {
            self.finished = true;
        }
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
        work: Option<MemoryReservation>,
    ) -> Result<RocksCanonicalSnapshot<C>, DataFusionError> {
        self.checkpoint("")?;
        let opened = OpenedDb {
            db: self.db.clone(),
            cache: self._cache.clone(),
            clock: Arc::clone(&self.clock),
            write_batch_size: self.write_batch_size,
        };
        let decoder = Self::attach(&opened, self.config.clone(), self.codec.clone(), self.table)?;
        let decoded_per_row = std::mem::size_of::<(ByteKey, C::Value)>().saturating_add(
            self.codec
                .value_fields()
                .len()
                .saturating_mul(2 * std::mem::size_of::<ScalarValue>()),
        );
        Ok(RocksCanonicalSnapshot {
            snapshot: RocksReadSnapshot::new(&self.db)
                .with_memory(work)
                .with_decoding_cost(decoded_per_row, 4),
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

#[cfg(test)]
mod reservation_tests {
    use super::*;

    #[derive(Clone)]
    struct DecodeProbe(Arc<std::sync::atomic::AtomicUsize>);

    impl RocksStateCodec for DecodeProbe {
        type Value = i64;
        fn supported(&self) -> bool {
            true
        }
        fn value_fields(&self) -> Vec<(String, DataType)> {
            vec![("value".into(), DataType::Int64)]
        }
        fn value_bytes(&self, _: &i64) -> usize {
            8
        }
        fn raw(&self) -> bool {
            true
        }
        fn raw_write(&self, value: &i64, out: &mut Vec<u8>) {
            out.extend_from_slice(&value.to_le_bytes());
        }
        fn from_raw(&self, bytes: &[u8]) -> i64 {
            self.0.fetch_add(1, Ordering::Relaxed);
            i64::from_le_bytes(bytes.try_into().unwrap())
        }
    }

    #[test]
    fn direct_cursor_refuses_before_decode_and_retries_without_skipping_entries() {
        use crate::state::KeyedStateStore;
        let directory = tempfile::tempdir().unwrap();
        let decoded = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut source = RocksStore::create(
            RocksStoreConfig {
                table_dir: directory.path().to_string_lossy().into_owned(),
                max_parallelism: 128,
                options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 0,
                shared_resources: 0,
            },
            DecodeProbe(Arc::clone(&decoded)),
        )
        .unwrap();
        for value in 0..3i64 {
            source.insert(ByteKey::from(value.to_le_bytes().as_slice()), value);
        }
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
        let mut blocker = MemoryConsumer::new("competing work").register(&pool);
        blocker.try_resize(4000).unwrap();
        let mut captured = source
            .canonical_snapshot(Some(MemoryConsumer::new("direct cursor").register(&pool)))
            .unwrap();
        assert!(matches!(
            captured.next_batch(),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(decoded.load(Ordering::Relaxed), 0);
        assert_eq!(pool.reserved(), 4000);
        drop(blocker);
        let rows = captured.next_batch().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(decoded.load(Ordering::Relaxed), 3);
        assert!(pool.reserved() > 0);
        drop(rows);
        drop(captured);
        assert_eq!(pool.reserved(), 0);
    }
}
