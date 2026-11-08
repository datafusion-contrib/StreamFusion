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
    _pin: SnapshotPin,
}

impl ReadView {
    fn new(database: Arc<Database>) -> Arc<Self> {
        let pin = SnapshotPin::new(&database.clock);
        let snapshot = unsafe {
            std::mem::transmute::<SnapshotWithThreadMode<'_, DB>, SnapshotWithThreadMode<'static, DB>>(
                database.db.snapshot(),
            )
        };
        Arc::new(Self {
            snapshot,
            database,
            _pin: pin,
        })
    }

    pub(super) fn iterator(&self, mode: IteratorMode<'_>) -> DBIteratorWithThreadMode<'_, DB> {
        self.snapshot.iterator(mode)
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
        self.database.db.iterator_opt(mode, self.options())
    }

    pub(crate) fn raw_iterator(&self) -> DBRawIteratorWithThreadMode<'_, DB> {
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
        self.database.db.write_opt(batch, options).map_err(re)
    }
}

pub(crate) use crate::state::canonical_context::{check_cancelled, in_frozen_scope};
pub(crate) type FrozenDatabases = crate::state::canonical_context::CanonicalContext;

impl FrozenDatabases {
    pub(crate) fn capture(directory: &Path) -> Result<Arc<Self>, DataFusionError> {
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
        Ok(Self::new(views))
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
    Ok(Some(OpenedDb {
        cache: database.cache.clone(),
        clock: Arc::new(StateClock::new(database.clock.load(Ordering::Acquire))),
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
        let captured = frozen.with(|| frozen_open(directory.path()).unwrap().unwrap());
        captured.clock.store(1, Ordering::Release);
        assert_eq!(
            clock.load(Ordering::Acquire),
            100,
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
