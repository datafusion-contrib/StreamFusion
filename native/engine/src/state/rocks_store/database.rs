use super::*;
use rocksdb::{
    DBIteratorWithThreadMode, DBRawIteratorWithThreadMode, ReadOptions, SnapshotWithThreadMode,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, Weak};

pub(super) struct StateClock {
    now: AtomicI64,
    pins: std::sync::atomic::AtomicUsize,
}

impl StateClock {
    pub(super) fn new(now: i64) -> Self {
        Self {
            now: AtomicI64::new(now),
            pins: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    pub(super) fn pinned(&self) -> bool {
        self.pins.load(Ordering::Acquire) != 0
    }
}

impl std::ops::Deref for StateClock {
    type Target = AtomicI64;
    fn deref(&self) -> &AtomicI64 {
        &self.now
    }
}

struct SnapshotPin(Arc<StateClock>);
impl SnapshotPin {
    fn new(clock: &Arc<StateClock>) -> Self {
        clock.pins.fetch_add(1, Ordering::AcqRel);
        Self(Arc::clone(clock))
    }
}
impl Drop for SnapshotPin {
    fn drop(&mut self) {
        self.0.pins.fetch_sub(1, Ordering::AcqRel);
    }
}

struct Database {
    db: Arc<DB>,
    cache: Option<Cache>,
    clock: Arc<StateClock>,
    write_batch_size: usize,
    path: PathBuf,
    #[cfg(test)]
    scans: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    write_batches: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    fail_write: std::sync::atomic::AtomicBool,
}

static DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<Database>>>> = OnceLock::new();
fn databases() -> &'static Mutex<HashMap<PathBuf, Weak<Database>>> {
    DATABASES.get_or_init(|| Mutex::new(HashMap::new()))
}

impl Drop for Database {
    fn drop(&mut self) {
        let mut registered = databases().lock().unwrap_or_else(|e| e.into_inner());
        if registered
            .get(&self.path)
            .is_some_and(|entry| entry.strong_count() == 0)
        {
            registered.remove(&self.path);
        }
    }
}

pub(super) struct ReadView {
    // The stable Arc allocation outlives its snapshot. Fields drop in declaration order.
    snapshot: SnapshotWithThreadMode<'static, DB>,
    database: Arc<Database>,
    clock: i64,
    _pin: SnapshotPin,
}

impl ReadView {
    fn new(database: Arc<Database>) -> Arc<Self> {
        let pin = SnapshotPin::new(&database.clock);
        let clock = database.clock.load(Ordering::Acquire);
        let snapshot = unsafe {
            std::mem::transmute::<SnapshotWithThreadMode<'_, DB>, SnapshotWithThreadMode<'static, DB>>(
                database.db.snapshot(),
            )
        };
        Arc::new(Self {
            snapshot,
            database,
            clock,
            _pin: pin,
        })
    }

    pub(super) fn raw_iterator(&self) -> DBRawIteratorWithThreadMode<'_, DB> {
        #[cfg(test)]
        self.database.scans.fetch_add(1, Ordering::Relaxed);
        self.snapshot.raw_iterator()
    }
}

/// Live writes and immutable canonical reads share the same store implementation.
#[derive(Clone)]
pub(crate) struct StateDb {
    database: Arc<Database>,
    view: Option<Arc<ReadView>>,
}

impl std::ops::Deref for StateDb {
    type Target = DB;
    fn deref(&self) -> &DB {
        &self.database.db
    }
}

impl StateDb {
    pub(super) fn live(
        db: Arc<DB>,
        cache: Option<Cache>,
        clock: Arc<StateClock>,
        write_batch_size: usize,
        path: &Path,
    ) -> Self {
        let path = path
            .canonicalize()
            .expect("opened state database directory");
        let database = Arc::new(Database {
            db,
            cache,
            clock,
            write_batch_size,
            path: path.clone(),
            #[cfg(test)]
            scans: Default::default(),
            #[cfg(test)]
            write_batches: Default::default(),
            #[cfg(test)]
            fail_write: Default::default(),
        });
        databases()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(path, Arc::downgrade(&database));
        Self {
            database,
            view: None,
        }
    }

    pub(super) fn pin(&self) -> Arc<ReadView> {
        self.view
            .clone()
            .unwrap_or_else(|| ReadView::new(Arc::clone(&self.database)))
    }

    pub(super) fn read_snapshot(&self) -> Self {
        Self {
            database: Arc::clone(&self.database),
            view: Some(self.pin()),
        }
    }

    #[cfg(test)]
    pub(crate) fn io_counts(&self) -> (usize, usize) {
        (
            self.database.scans.load(Ordering::Relaxed),
            self.database.write_batches.load(Ordering::Relaxed),
        )
    }

    #[cfg(test)]
    pub(crate) fn fail_next_write(&self) {
        self.database.fail_write.store(true, Ordering::Relaxed);
    }

    pub(super) fn write_batch_size(&self) -> usize {
        self.database.write_batch_size
    }

    pub(crate) fn is_frozen(&self) -> bool {
        self.view.is_some()
    }

    pub(crate) fn visit_snapshot_batches(
        &self,
        chunk_rows: usize,
        accept: impl Fn(&[u8]) -> bool,
        consume: impl FnMut(Vec<(Box<[u8]>, Box<[u8]>)>) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        self.visit_snapshot_batches_charged(chunk_rows, None, accept, consume)
    }

    pub(crate) fn visit_snapshot_batches_charged(
        &self,
        chunk_rows: usize,
        work: Option<MemoryReservation>,
        accept: impl Fn(&[u8]) -> bool,
        mut consume: impl FnMut(Vec<(Box<[u8]>, Box<[u8]>)>) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let mut reader = super::canonical_snapshot::RocksReadSnapshot::new(self).with_memory(work);
        loop {
            let rows = reader.next_batch_limited(chunk_rows, 4 * 1024 * 1024, &accept)?;
            if rows.is_empty() {
                return Ok(());
            }
            consume(rows)?;
            reader.release_batch_memory();
        }
    }

    pub(crate) fn visit_snapshot_raw(
        &self,
        mut work: Option<MemoryReservation>,
        accept: impl Fn(&[u8]) -> bool,
        mut consume: impl FnMut(&[u8], &[u8]) -> Result<(), DataFusionError>,
    ) -> Result<(), DataFusionError> {
        if work.is_none() {
            work = crate::state::canonical_context::temporary_reservation();
        }
        let view = self.pin();
        let mut reader = view.snapshot.raw_iterator();
        reader.seek_to_first();
        while reader.valid() {
            check_cancelled()?;
            let key = reader.key().unwrap();
            if accept(key) {
                let value = reader.value().unwrap();
                if let Some(work) = &mut work {
                    work.try_resize(key.len().saturating_add(value.len()))?;
                }
                consume(key, value)?;
                if let Some(work) = &mut work {
                    work.try_resize(0)?;
                }
            }
            reader.next();
        }
        reader.status().map_err(re)
    }

    fn options(&self) -> ReadOptions {
        let mut options = ReadOptions::default();
        if let Some(view) = &self.view {
            options.set_snapshot(&view.snapshot);
        }
        options
    }

    pub(crate) fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        self.database.db.get_opt(key, &self.options())
    }

    pub(crate) fn get_pinned<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> Result<Option<DBPinnableSlice<'_>>, rocksdb::Error> {
        self.database.db.get_pinned_opt(key, &self.options())
    }

    pub(crate) fn iterator(&self, mode: IteratorMode<'_>) -> DBIteratorWithThreadMode<'_, DB> {
        #[cfg(test)]
        self.database.scans.fetch_add(1, Ordering::Relaxed);
        self.database.db.iterator_opt(mode, self.options())
    }

    pub(crate) fn raw_iterator(&self) -> DBRawIteratorWithThreadMode<'_, DB> {
        #[cfg(test)]
        self.database.scans.fetch_add(1, Ordering::Relaxed);
        self.database.db.raw_iterator_opt(self.options())
    }

    pub(super) fn multi_get_pinned<'a, K: AsRef<[u8]> + 'a + ?Sized>(
        &self,
        keys: impl IntoIterator<Item = &'a K>,
        sorted: bool,
    ) -> Vec<Result<Option<DBPinnableSlice<'_>>, rocksdb::Error>> {
        self.database.db.batched_multi_get_cf_opt(
            self.database
                .db
                .cf_handle("default")
                .expect("registered default column family"),
            keys,
            sorted,
            &self.options(),
        )
    }

    pub(crate) fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> Result<(), DataFusionError> {
        if self.allow_put(key.as_ref(), value.as_ref())? {
            self.database.db.put(key, value).map_err(re)
        } else {
            Ok(())
        }
    }

    fn allow_put(&self, key: &[u8], value: &[u8]) -> Result<bool, DataFusionError> {
        if self.view.is_none() {
            return Ok(true);
        }
        if key.starts_with(&[0xff; 4]) || self.get(key).map_err(re)?.as_deref() == Some(value) {
            return Ok(false);
        }
        Err(DataFusionError::Execution(
            "canonical read view cannot change state".to_owned(),
        ))
    }
}

pub(super) trait StateWrite {
    fn allow_put(&self, _key: &[u8], _value: &[u8]) -> Result<bool, DataFusionError> {
        Ok(true)
    }
    fn allow_delete(&self, _key: &[u8]) -> Result<bool, DataFusionError> {
        Ok(true)
    }
    fn allow_delete_range(&self) -> Result<(), DataFusionError> {
        Ok(())
    }
    fn write_batch(&self, batch: WriteBatch, options: &WriteOptions)
        -> Result<(), DataFusionError>;
}

impl StateWrite for DB {
    fn write_batch(
        &self,
        batch: WriteBatch,
        options: &WriteOptions,
    ) -> Result<(), DataFusionError> {
        self.write_opt(batch, options).map_err(re)
    }
}

impl StateWrite for StateDb {
    fn allow_put(&self, key: &[u8], value: &[u8]) -> Result<bool, DataFusionError> {
        self.allow_put(key, value)
    }
    fn allow_delete(&self, key: &[u8]) -> Result<bool, DataFusionError> {
        if self.view.is_none() {
            return Ok(true);
        }
        if self.get(key).map_err(re)?.is_none() {
            return Ok(false);
        }
        Err(DataFusionError::Execution(
            "canonical read view cannot delete state".to_owned(),
        ))
    }
    fn allow_delete_range(&self) -> Result<(), DataFusionError> {
        if self.view.is_some() {
            return Err(DataFusionError::Execution(
                "canonical read view cannot delete state".to_owned(),
            ));
        }
        Ok(())
    }
    fn write_batch(
        &self,
        batch: WriteBatch,
        options: &WriteOptions,
    ) -> Result<(), DataFusionError> {
        if self.view.is_some() {
            return Err(DataFusionError::Execution(
                "canonical read view cannot write state".to_owned(),
            ));
        }
        #[cfg(test)]
        {
            if self.database.fail_write.swap(false, Ordering::Relaxed) {
                return Err(DataFusionError::Execution(
                    "injected state write failure".into(),
                ));
            }
            self.database.write_batches.fetch_add(1, Ordering::Relaxed);
        }
        self.database.db.write_opt(batch, options).map_err(re)
    }
}

pub(crate) use crate::state::canonical_context::{check_cancelled, in_frozen_scope};
pub(crate) type FrozenDatabases = crate::state::canonical_context::CanonicalContext;

impl FrozenDatabases {
    #[cfg(test)]
    pub(crate) fn capture(directory: &Path) -> Result<Arc<Self>, DataFusionError> {
        Self::capture_with_budget(directory, -1)
    }

    pub(crate) fn capture_with_budget(
        directory: &Path,
        budget: i64,
    ) -> Result<Arc<Self>, DataFusionError> {
        let directory = directory.canonicalize().map_err(ioe)?;
        let registered = databases().lock().unwrap_or_else(|e| e.into_inner());
        let mut views = HashMap::new();
        for (path, database) in registered
            .iter()
            .filter(|(path, _)| path.starts_with(&directory))
        {
            if let Some(database) = database.upgrade() {
                views.insert(
                    path.clone(),
                    ReadView::new(database) as crate::state::canonical_context::CapturedView,
                );
            }
        }
        if views.is_empty() {
            return Err(DataFusionError::Execution(
                "native state has no open database to pin".into(),
            ));
        }
        Self::with_budget(views, budget)
    }
}

pub(super) fn frozen_open(path: &Path) -> Result<Option<OpenedDb>, DataFusionError> {
    if !in_frozen_scope() {
        return Ok(None);
    }
    let path = path.canonicalize().map_err(ioe)?;
    let view = crate::state::canonical_context::view(&path)?.expect("active canonical scope");
    let view = view.downcast::<ReadView>().expect("RocksDB capture type");
    let database = Arc::clone(&view.database);
    let clock = Arc::new(StateClock::new(view.clock));
    Ok(Some(OpenedDb {
        cache: database.cache.clone(),
        clock,
        write_batch_size: database.write_batch_size,
        db: StateDb {
            database,
            view: Some(view),
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_database() -> (tempfile::TempDir, StateDb, Arc<StateClock>) {
        let directory = tempfile::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = Arc::new(
            DB::open_cf_with_opts(&options, directory.path(), [("default", options.clone())])
                .unwrap(),
        );
        let clock = Arc::new(StateClock::new(100));
        let live = StateDb::live(db, None, Arc::clone(&clock), 1024, directory.path());
        (directory, live, clock)
    }

    #[test]
    fn bounded_snapshot_visitors_keep_their_version_through_live_writes_and_compaction() {
        let (_directory, live, clock) = test_database();
        for value in 0..2051 {
            live.put(format!("key-{value:04}"), b"before").unwrap();
        }
        let mut count = 0;
        live.visit_snapshot_batches(
            17,
            |_| true,
            |rows| {
                assert!(rows.len() <= 17);
                if count == 0 {
                    live.put(b"key-2000", b"after").unwrap();
                    live.database.db.delete(b"key-2001").unwrap();
                    live.database.db.flush().unwrap();
                    live.database.db.compact_range(None::<&[u8]>, None::<&[u8]>);
                }
                for (_, value) in rows {
                    assert_eq!(value.as_ref(), b"before");
                    count += 1;
                }
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(count, 2051);
        assert!(!clock.pinned());
        assert_eq!(live.get(b"key-2000").unwrap().unwrap(), b"after");
        assert!(live.get(b"key-2001").unwrap().is_none());
    }

    #[test]
    fn borrowed_raw_snapshot_keeps_version_and_releases_memory_on_failure() {
        let (_directory, live, clock) = test_database();
        for value in 0..2051 {
            live.put(format!("key-{value:04}"), b"before").unwrap();
        }
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128));
        let mut count = 0;
        live.visit_snapshot_raw(
            Some(MemoryConsumer::new("raw canonical").register(&pool)),
            |_| true,
            |key, value| {
                assert!(pool.reserved() >= key.len() + value.len());
                if count == 0 {
                    live.put(b"key-2000", b"after").unwrap();
                    live.delete(b"key-2001").unwrap();
                    live.flush().unwrap();
                    live.compact_range(None::<&[u8]>, None::<&[u8]>);
                }
                assert_eq!(value, b"before");
                count += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(count, 2051);
        assert!(!clock.pinned());
        assert_eq!(pool.reserved(), 0);
        let result = live.visit_snapshot_raw(
            Some(MemoryConsumer::new("failed canonical").register(&pool)),
            |_| true,
            |_, _| Err(DataFusionError::Execution("failed consumer".into())),
        );
        assert!(result.unwrap_err().to_string().contains("failed consumer"));
        assert!(!clock.pinned());
        assert_eq!(pool.reserved(), 0);
        let tiny: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let result = live.visit_snapshot_raw(
            Some(MemoryConsumer::new("denied raw").register(&tiny)),
            |_| true,
            |_, _| panic!("denied before conversion"),
        );
        assert!(matches!(
            result,
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert!(!clock.pinned());
        assert_eq!(tiny.reserved(), 0);
    }

    #[test]
    fn snapshot_visitors_bound_bytes_and_release_on_failure_and_cancellation() {
        let (_directory, live, clock) = test_database();
        let value = vec![7; 2 * 1024 * 1024];
        for key in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
            live.put(key, &value).unwrap();
        }
        let mut pages = Vec::new();
        live.visit_snapshot_batches(
            1024,
            |_| true,
            |rows| {
                assert!(
                    rows.len() <= 2,
                    "the byte bound must split before the row bound"
                );
                pages.push(rows.len());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(pages.iter().sum::<usize>(), 3);
        assert_eq!(pages.len(), 3);
        assert!(live
            .visit_snapshot_batches(0, |_| true, |_| Ok(()))
            .is_err());
        assert!(live
            .visit_snapshot_batches(
                1,
                |_| true,
                |_| Err(DataFusionError::Execution("receiver stopped".into()))
            )
            .is_err());
        assert!(!clock.pinned());
        let scope = FrozenDatabases::new(HashMap::new());
        let result = scope.with(|| {
            live.visit_snapshot_batches(
                1,
                |_| true,
                |_| {
                    scope.cancel();
                    Ok(())
                },
            )
        });
        assert!(result.is_err());
        assert!(!clock.pinned());
        assert!(
            check_cancelled().is_ok(),
            "cancel scope must not escape its reader"
        );
    }

    #[test]
    fn denied_scan_batch_keeps_its_cursor_and_releases_partial_work_before_retry() {
        let (_directory, live, clock) = test_database();
        for key in [b"a", b"b", b"c"] {
            live.put(key, vec![7; 100]).unwrap();
        }
        live.put(b"z", vec![8; 1 << 20]).unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        let mut blocker = MemoryConsumer::new("other work").register(&pool);
        blocker.try_resize(700).unwrap();
        let mut reader = super::super::RocksReadSnapshot::new(&live)
            .with_memory(Some(MemoryConsumer::new("copied scan").register(&pool)));
        assert!(matches!(
            reader.next_batch(|key| key != b"z"),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(pool.reserved(), 700);
        drop(blocker);
        let rows = reader.next_batch(|key| key != b"z").unwrap();
        assert_eq!(
            rows.len(),
            3,
            "the refused frame must not advance its cursor"
        );
        assert_eq!(rows[0].0.as_ref(), b"a");
        assert_eq!(rows[2].0.as_ref(), b"c");
        assert!(pool.reserved() >= rows.iter().map(|(k, v)| k.len() + v.len()).sum());
        drop(rows);
        assert!(
            reader.next_batch(|_| true).is_err(),
            "the oversized next record exceeds the pool"
        );
        drop(reader);
        assert_eq!(pool.reserved(), 0);
        assert!(!clock.pinned());
    }

    #[test]
    fn default_visitors_share_captured_memory_and_release_after_failure_or_cancellation() {
        let (_directory, live, clock) = test_database();
        for key in [b"a", b"b", b"c"] {
            live.put(key, vec![7; 100]).unwrap();
        }
        for borrowed in [false, true] {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
            let mut memory = OperatorMemory::unaccounted();
            memory.attach_pool("captured conversion", &pool, 0).unwrap();
            let scope = FrozenDatabases::with_memory(HashMap::new(), memory);
            let failed = scope.with(|| {
                if borrowed {
                    live.visit_snapshot_raw(
                        None,
                        |_| true,
                        |_, _| {
                            assert!(pool.reserved() > 0);
                            Err(DataFusionError::Execution("refused receiver".into()))
                        },
                    )
                } else {
                    live.visit_snapshot_batches(
                        1,
                        |_| true,
                        |_| {
                            assert!(pool.reserved() > 0);
                            Err(DataFusionError::Execution("refused receiver".into()))
                        },
                    )
                }
            });
            assert!(failed.unwrap_err().to_string().contains("refused receiver"));
            assert_eq!(pool.reserved(), 0);
            let cancelled = scope.with(|| {
                live.visit_snapshot_batches(
                    1,
                    |_| true,
                    |_| {
                        assert!(pool.reserved() > 0);
                        scope.cancel();
                        Ok(())
                    },
                )
            });
            assert!(cancelled.unwrap_err().to_string().contains("cancelled"));
            assert_eq!(pool.reserved(), 0);
            assert!(!clock.pinned());
            assert!(crate::state::canonical_context::temporary_reservation().is_none());
        }
    }

    #[test]
    fn all_store_readers_observe_the_same_pinned_version() {
        let directory = tempfile::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        let raw = Arc::new(
            DB::open_cf_with_opts(&options, directory.path(), [("default", options.clone())])
                .unwrap(),
        );
        let clock = Arc::new(StateClock::new(100));
        let live = StateDb::live(raw, None, Arc::clone(&clock), 1024, directory.path());
        live.put(b"key", b"before").unwrap();
        let frozen = FrozenDatabases::capture(directory.path()).unwrap();
        live.put(b"key", b"after").unwrap();
        clock.store(200, Ordering::Release);
        let captured = frozen.with(|| frozen_open(directory.path()).unwrap().unwrap());
        assert_eq!(
            captured.clock.load(Ordering::Acquire),
            100,
            "a lazily opened reader uses the clock from capture"
        );
        captured.clock.store(1, Ordering::Release);
        assert_eq!(
            clock.load(Ordering::Acquire),
            200,
            "a decoder must not rewind the live TTL clock"
        );
        let captured = captured.db;
        assert_eq!(
            captured.get(b"key").unwrap().as_deref(),
            Some(b"before".as_slice())
        );
        assert_eq!(
            captured.get_pinned(b"key").unwrap().unwrap().as_ref(),
            b"before"
        );
        assert_eq!(
            captured.multi_get_pinned([b"key"], true)[0]
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap()
                .as_ref(),
            b"before"
        );
        assert_eq!(
            captured
                .iterator(IteratorMode::Start)
                .next()
                .unwrap()
                .unwrap()
                .1
                .as_ref(),
            b"before"
        );
        let mut raw = captured.raw_iterator();
        raw.seek_to_first();
        assert_eq!(raw.value(), Some(b"before".as_slice()));
        drop(raw);
        assert!(captured.put(b"key", b"forbidden").is_err());
        captured.put(b"key", b"before").unwrap();
        assert_eq!(
            live.get(b"key").unwrap().as_deref(),
            Some(b"after".as_slice())
        );
        frozen.cancel();
        assert!(frozen.with(check_cancelled).is_err());
        assert!(check_cancelled().is_ok(), "scope must be cleared on return");
        drop(live);
        drop(frozen);
        assert_eq!(
            captured.get(b"key").unwrap().as_deref(),
            Some(b"before".as_slice())
        );
        drop(captured);
        let reopened = DB::open(&options, directory.path()).unwrap();
        assert_eq!(
            reopened.get(b"key").unwrap().as_deref(),
            Some(b"after".as_slice())
        );
    }
}
