//! Rust-owned RocksDB state, on Flink's write path: dirty entries are written through to the
//! RocksDB memtable at every bundle boundary, so RocksDB's own write buffers are the only write
//! buffer and its background threads own all flushing and compaction. Committed entries are keyed
//! by Flink key group plus BinaryRow bytes and are read directly through RocksDB without a
//! Java/JNI data-plane round trip. Values travel as compact arrow-row bytes, encoded and decoded
//! for a whole bundle's working set in one columnar conversion; a state-TTL value carries its
//! last-write timestamp as a fixed 8-byte prefix so the compaction filter never parses the row.

use crate::*;
use arrow::row::{RowConverter, SortField};
use rocksdb::checkpoint::Checkpoint;
use rocksdb::{Cache, CompactionDecision, IteratorMode, Options, WriteBatch, WriteOptions, DB};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

const SNAPSHOT_TIMER_KEY: &[u8] = b"\xff\xff\xff\xffstreamfusion-timer";

pub(crate) trait RocksStateCodec {
    type Value;
    fn supported(&self) -> bool;
    fn value_fields(&self) -> Vec<(String, DataType)>;
    fn encode(&self, value: &Self::Value) -> Vec<ScalarValue>;
    fn decode(&self, scalars: &[ScalarValue]) -> Self::Value;
    fn value_bytes(&self, value: &Self::Value) -> usize;
    fn write_ms(&self, _value: &Self::Value) -> i64 {
        0
    }
    fn stamp_write_ms(&self, _value: &mut Self::Value, _ts_ms: i64) {}

    /// A codec whose value already carries a self-contained byte payload (an arrow-row the
    /// operator encoded) can persist its own layout directly, skipping the store's columnar
    /// conversion in both directions. A raw codec implements all three methods; `encode`/`decode`
    /// are then unused. `raw_write` appends into the store's value buffer (after any TTL prefix),
    /// so a composite layout costs no intermediate copy.
    fn raw(&self) -> bool {
        false
    }
    fn raw_write(&self, _value: &Self::Value, _out: &mut Vec<u8>) {
        unreachable!("not a raw codec")
    }
    fn from_raw(&self, _bytes: &[u8]) -> Self::Value {
        unreachable!("not a raw codec")
    }
}

pub(crate) fn rocks_row_supported(types: &[DataType]) -> bool {
    types.iter().all(|data_type| {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::Binary
                | DataType::Date32
                | DataType::Decimal128(_, _)
                | DataType::Timestamp(_, None)
        )
    })
}

pub(crate) fn rocks_group_supported(kinds: &[i64], state_types: &[DataType]) -> bool {
    group_kinds_persistable(kinds) && rocks_row_supported(state_types)
}

#[derive(Clone)]
pub(crate) struct RocksStoreConfig {
    pub table_dir: String,
    pub max_parallelism: usize,
    pub options_json: String,
    pub ttl_ms: i64,
    /// Borrowed pointer to the slot's [`RocksSharedResources`], 0 when the job runs without a
    /// shared pool. Java's shared-resource lease outlives every store opened under it.
    pub shared_resources: i64,
}

impl RocksStoreConfig {
    fn shared(&self) -> Option<&'static crate::state::rocks_config::RocksSharedResources> {
        (self.shared_resources != 0)
            .then(|| unsafe { &*(self.shared_resources as *const _) })
    }
}

#[derive(serde::Serialize)]
pub(crate) struct RocksCheckpointManifest {
    pub snapshot_id: i64,
    pub data_files: Vec<String>,
    pub meta_files: Vec<String>,
}

impl RocksCheckpointManifest {
    pub(crate) fn absent() -> Self {
        Self {
            snapshot_id: -1,
            data_files: Vec::new(),
            meta_files: Vec::new(),
        }
    }
}

enum Slot<V> {
    Present { state: V, dirty: bool },
    Absent { dirty: bool },
}

/// Flink's write-batch size is a memory bound, not a semantic transaction boundary. Apply the
/// same cap while restoring or committing buffered native state, with WAL disabled on every
/// physical write just like Flink's `RocksDBWriteBatchWrapper`.
struct FlinkWriteBatch<'a> {
    db: &'a DB,
    options: WriteOptions,
    max_bytes: usize,
    batch: WriteBatch,
}

impl<'a> FlinkWriteBatch<'a> {
    fn new(db: &'a DB, max_bytes: usize) -> Self {
        Self {
            db,
            options: flink_write_options(),
            max_bytes,
            batch: WriteBatch::default(),
        }
    }

    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), DataFusionError> {
        self.batch.put(key, value);
        self.flush_if_full()
    }

    fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), DataFusionError> {
        self.batch.delete(key);
        self.flush_if_full()
    }

    fn flush_if_full(&mut self) -> Result<(), DataFusionError> {
        if self.max_bytes > 0 && self.batch.size_in_bytes() >= self.max_bytes {
            self.flush()?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(), DataFusionError> {
        self.flush()
    }

    fn flush(&mut self) -> Result<(), DataFusionError> {
        if !self.batch.is_empty() {
            let batch = std::mem::take(&mut self.batch);
            self.db.write_opt(batch, &self.options).map_err(re)?;
        }
        Ok(())
    }
}

pub(crate) struct RocksStore<C: RocksStateCodec> {
    db: DB,
    _cache: Option<Cache>,
    config: RocksStoreConfig,
    codec: C,
    value_fields: Vec<Field>,
    converter: RowConverter,
    now_ms: i64,
    clock: Arc<AtomicI64>,
    generation: i64,
    write_batch_size: usize,
    working: ahash::HashMap<ByteKey, Slot<C::Value>>,
    footprint: isize,
}

impl<C: RocksStateCodec> RocksStore<C> {
    const SLOT_OVERHEAD: usize = std::mem::size_of::<Slot<C::Value>>() + GROUP_ENTRY_OVERHEAD;

    pub(crate) fn create(config: RocksStoreConfig, codec: C) -> Result<Self, DataFusionError> {
        Self::open_db(config, codec)
    }

    pub(crate) fn open_merged(
        config: RocksStoreConfig,
        codec: C,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
        now_ms: i64,
    ) -> Result<Self, DataFusionError> {
        if aligned && sources.len() == 1 {
            copy_checkpoint_db(&sources[0].0, &config.table_dir)?;
            let mut store = Self::open_db(config, codec)?;
            store.now_ms = now_ms;
            store.clock.store(now_ms, Ordering::Relaxed);
            store.generation = sources[0].1;
            return Ok(store);
        }
        let mut store = Self::open_db(config, codec)?;
        store.now_ms = now_ms;
        let mut writes = FlinkWriteBatch::new(&store.db, store.write_batch_size);
        for (source, _) in sources {
            let source_db =
                DB::open_for_read_only(&Options::default(), source, false).map_err(re)?;
            for row in source_db.iterator(IteratorMode::Start) {
                let (key, value) = row.map_err(re)?;
                if key.len() >= 4 {
                    let kg = i32::from_be_bytes(key[0..4].try_into().expect("key group prefix"));
                    if key_groups.contains(&kg) {
                        writes.put(key, value)?;
                    }
                }
            }
        }
        writes.finish()?;
        Ok(store)
    }

    fn open_db(config: RocksStoreConfig, codec: C) -> Result<Self, DataFusionError> {
        if !codec.supported() {
            return Err(DataFusionError::Plan(
                "state shape not supported by RocksDB".into(),
            ));
        }
        std::fs::create_dir_all(&config.table_dir).map_err(ioe)?;
        let resolved =
            crate::state::rocks_config::FlinkRocksOptions::from_json(&config.options_json)
                .map_err(DataFusionError::Plan)?;
        let (mut options, cache) = resolved.build(config.shared()).map_err(DataFusionError::Plan)?;
        let write_batch_size = resolved.write_batch_size;
        let clock = Arc::new(AtomicI64::new(0));
        if config.ttl_ms > 0 {
            let filter_clock = Arc::clone(&clock);
            let ttl_ms = config.ttl_ms;
            let refresh_after = resolved
                .compaction_filter_query_time_after_num_entries
                .max(1);
            let mut remaining = 0u64;
            let mut now = 0i64;
            options.set_compaction_filter("streamfusion-state-ttl", move |_level, _key, value| {
                if remaining == 0 {
                    now = filter_clock.load(Ordering::Relaxed);
                    remaining = refresh_after;
                }
                remaining -= 1;
                match persisted_write_ms(value) {
                    Some(written) if now >= written.saturating_add(ttl_ms) => {
                        CompactionDecision::Remove
                    }
                    _ => CompactionDecision::Keep,
                }
            });
        }
        let db = DB::open(&options, &config.table_dir).map_err(re)?;
        let value_fields: Vec<_> = codec
            .value_fields()
            .into_iter()
            .map(|(n, t)| Field::new(n, t, true))
            .collect();
        let converter = RowConverter::new(
            value_fields
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            db,
            _cache: cache,
            config,
            codec,
            value_fields,
            converter,
            now_ms: 0,
            clock,
            generation: 0,
            write_batch_size,
            working: ahash::HashMap::default(),
            footprint: 0,
        })
    }

    pub(crate) fn set_clock(&mut self, now_ms: i64) {
        self.now_ms = now_ms;
        self.clock.store(now_ms, Ordering::Relaxed);
    }
    pub(crate) fn staging_bytes(&self) -> usize {
        self.working.len() * Self::SLOT_OVERHEAD
    }
    pub(crate) fn staged_keys(&self) -> usize {
        self.working.len()
    }
    pub(crate) fn metric_entry_count(&self) -> usize {
        self.working.len()
    }

    fn db_key(&self, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + key.len());
        let key_group =
            flink_key_group(hash_bytes_by_words(key), self.config.max_parallelism) as i32;
        out.extend_from_slice(&key_group.to_be_bytes());
        out.extend_from_slice(key);
        out
    }

    /// The fixed prefix a persisted value carries ahead of its arrow-row bytes: the last-write
    /// timestamp when TTL is on, nothing when it is off (matching the raw snapshots' convention
    /// that a TTL-off format carries no timestamp).
    fn value_prefix_len(&self) -> usize {
        if self.config.ttl_ms > 0 {
            8
        } else {
            0
        }
    }

    /// Writes every dirty working-set entry through to RocksDB in one columnar conversion —
    /// Flink's write path, amortized to one memtable write per touched key per bundle.
    fn write_dirty(&mut self) -> Result<(), DataFusionError> {
        let mut keys = Vec::new();
        let mut states = Vec::new();
        let mut deletes = Vec::new();
        for (key, slot) in &self.working {
            match slot {
                Slot::Present { state, dirty: true } => {
                    keys.push(key);
                    states.push(state);
                }
                Slot::Absent { dirty: true } => deletes.push(key),
                _ => {}
            }
        }
        if keys.is_empty() && deletes.is_empty() {
            return Ok(());
        }
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        let ttl = self.config.ttl_ms > 0;
        if !keys.is_empty() && self.codec.raw() {
            for (key, state) in keys.iter().zip(&states) {
                let mut value = Vec::with_capacity(self.value_prefix_len());
                if ttl {
                    value.extend_from_slice(&self.codec.write_ms(state).to_le_bytes());
                }
                self.codec.raw_write(state, &mut value);
                writes.put(self.db_key(&key.0), value)?;
            }
        } else if !keys.is_empty() {
            let mut columns: Vec<Vec<ScalarValue>> =
                vec![Vec::with_capacity(states.len()); self.value_fields.len()];
            for state in &states {
                for (column, scalar) in columns.iter_mut().zip(self.codec.encode(state)) {
                    column.push(scalar);
                }
            }
            let arrays: Vec<_> = columns
                .into_iter()
                .zip(&self.value_fields)
                .map(|(scalars, field)| scalars_to_array(scalars, field.data_type()))
                .collect();
            let rows = self
                .converter
                .convert_columns(&arrays)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            for ((key, state), row) in keys.iter().zip(&states).zip(rows.iter()) {
                let row = row.data();
                let mut value = Vec::with_capacity(self.value_prefix_len() + row.len());
                if ttl {
                    value.extend_from_slice(&self.codec.write_ms(state).to_le_bytes());
                }
                value.extend_from_slice(row);
                writes.put(self.db_key(&key.0), value)?;
            }
        }
        for key in deletes {
            writes.delete(self.db_key(&key.0))?;
        }
        writes.finish()
    }

    /// Decodes a set of persisted values in one columnar conversion; `None` marks an entry whose
    /// TTL has lapsed (Flink's `NeverReturnExpired`).
    fn decode_values(&self, values: &[&[u8]]) -> Result<Vec<Option<C::Value>>, DataFusionError> {
        let prefix = self.value_prefix_len();
        let live_ts = |value: &&[u8]| {
            let ts = (prefix > 0)
                .then(|| i64::from_le_bytes(value[..8].try_into().expect("ttl prefix")));
            match ts {
                Some(t) if self.now_ms >= t.saturating_add(self.config.ttl_ms) => Err(()),
                other => Ok(other),
            }
        };
        if self.codec.raw() {
            return Ok(values
                .iter()
                .map(|value| {
                    live_ts(value).ok().map(|ts| {
                        let mut state = self.codec.from_raw(&value[prefix..]);
                        if let Some(ts) = ts {
                            self.codec.stamp_write_ms(&mut state, ts);
                        }
                        state
                    })
                })
                .collect());
        }
        let parser = self.converter.parser();
        let rows: Vec<_> = values.iter().map(|v| parser.parse(&v[prefix..])).collect();
        let columns = self
            .converter
            .convert_rows(rows)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut out = Vec::with_capacity(values.len());
        let mut scalars = vec![ScalarValue::Null; columns.len()];
        for (row, value) in values.iter().enumerate() {
            let Ok(ts) = live_ts(value) else {
                out.push(None);
                continue;
            };
            for (slot, column) in scalars.iter_mut().zip(&columns) {
                *slot = ScalarValue::try_from_array(column, row)?;
            }
            let mut state = self.codec.decode(&scalars);
            if let Some(ts) = ts {
                self.codec.stamp_write_ms(&mut state, ts);
            }
            out.push(Some(state));
        }
        Ok(out)
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<Option<C::Value>, DataFusionError> {
        Ok(self.decode_values(&[bytes])?.pop().flatten())
    }

    pub(crate) fn checkpoint(
        &mut self,
        snapshot_dir: &str,
    ) -> Result<RocksCheckpointManifest, DataFusionError> {
        self.write_dirty()?;
        self.working.clear();
        if snapshot_dir.is_empty() {
            return Ok(RocksCheckpointManifest::absent());
        }
        self.generation += 1;
        checkpoint_files(&self.db, snapshot_dir, self.generation)
    }

    /// Commits the working set and decodes the complete logical table for a canonical
    /// savepoint. This intentionally walks RocksDB only for the portable full-snapshot path.
    pub(crate) fn canonical_keys_by_group(
        &mut self,
    ) -> Result<std::collections::BTreeMap<i32, Vec<ByteKey>>, DataFusionError> {
        self.checkpoint("")?;
        let mut keys = std::collections::BTreeMap::<i32, Vec<ByteKey>>::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (db_key, value) = row.map_err(re)?;
            if db_key.len() < 4 || db_key.as_ref() == SNAPSHOT_TIMER_KEY {
                continue;
            }
            let key_group = i32::from_be_bytes(db_key[..4].try_into().unwrap());
            if let Some(state) = self.decode_value(&value)? {
                let key = ByteKey::from(&db_key[4..]);
                self.working.insert(
                    key.clone(),
                    Slot::Present {
                        state,
                        dirty: false,
                    },
                );
                keys.entry(key_group).or_default().push(key);
            }
        }
        Ok(keys)
    }

    pub(crate) fn finish_canonical_scan(&mut self) {
        self.working.clear();
    }
}

impl<C: RocksStateCodec> KeyedStateStore<C::Value> for RocksStore<C> {
    fn contains(&self, key: &[u8]) -> bool {
        matches!(self.working.get(key), Some(Slot::Present { .. }))
    }
    fn get(&self, key: &[u8]) -> Option<&C::Value> {
        match self.working.get(key) {
            Some(Slot::Present { state, .. }) => Some(state),
            _ => None,
        }
    }
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut C::Value> {
        match self.working.get_mut(key) {
            Some(Slot::Present { state, dirty }) => {
                *dirty = true;
                Some(state)
            }
            _ => None,
        }
    }
    fn insert(&mut self, key: ByteKey, value: C::Value) -> &mut C::Value {
        match self
            .working
            .entry(key)
            .insert_entry(Slot::Present {
                state: value,
                dirty: true,
            })
            .into_mut()
        {
            Slot::Present { state, .. } => state,
            _ => unreachable!(),
        }
    }
    fn remove(&mut self, key: &[u8]) {
        self.working
            .insert(ByteKey::from(key), Slot::Absent { dirty: true });
    }
    fn begin_batch(
        &mut self,
        batch: &RecordBatch,
        key_columns: &[usize],
        precisions: &[i32],
    ) -> Result<(), DataFusionError> {
        let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, precisions);
        let mut missing = Vec::new();
        let mut seen = ahash::HashSet::default();
        for row in 0..batch.num_rows() {
            let key = ByteKey::from(encoder.encode(row));
            if !self.working.contains_key(&key) && seen.insert(key.clone()) {
                missing.push(key);
            }
        }
        if missing.is_empty() {
            return Ok(());
        }
        let db_keys: Vec<_> = missing.iter().map(|key| self.db_key(&key.0)).collect();
        let fetched = self.db.multi_get(&db_keys);
        let mut hit_keys = Vec::new();
        let mut hit_values = Vec::new();
        for (key, value) in missing.iter().zip(&fetched) {
            match value {
                Ok(Some(bytes)) => {
                    hit_keys.push(key.clone());
                    hit_values.push(bytes.as_slice());
                }
                Ok(None) => {
                    self.working
                        .insert(key.clone(), Slot::Absent { dirty: false });
                }
                Err(error) => return Err(re(error.clone())),
            }
        }
        for (key, state) in hit_keys.into_iter().zip(self.decode_values(&hit_values)?) {
            let slot = match state {
                Some(state) => Slot::Present {
                    state,
                    dirty: false,
                },
                None => Slot::Absent { dirty: true },
            };
            self.working.insert(key, slot);
        }
        Ok(())
    }
    fn end_bundle(&mut self) -> Result<(), DataFusionError> {
        self.write_dirty()?;
        self.working.clear();
        Ok(())
    }
    fn footprint_delta(&mut self) -> isize {
        std::mem::take(&mut self.footprint)
    }
}

fn re(error: rocksdb::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
fn ioe(error: std::io::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Flink deliberately disables the WAL for keyed state: completed checkpoints, rather than the
/// local database log, are the durability boundary. Keep the Rust-owned instance on that same
/// write path so barriers flush memtables to SSTs without uploading transient WAL files.
fn flink_write_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.disable_wal(true);
    options
}

/// A TTL value's last-write timestamp, read from the fixed 8-byte prefix ahead of the row bytes.
/// Only installed as a compaction-filter probe when the store's TTL is on, so a TTL-off value
/// (which carries no prefix) is never inspected.
fn persisted_write_ms(bytes: &[u8]) -> Option<i64> {
    bytes
        .get(..8)
        .map(|prefix| i64::from_le_bytes(prefix.try_into().expect("ttl prefix")))
}

/// Compatibility store for native operators whose existing state engine still snapshots by key
/// group. RocksDB owns the durable local image and compaction/checkpoint files; operators can move
/// to typed read-through stores independently without retaining a second Java-side backend.
pub(crate) struct RocksSnapshotStore {
    db: DB,
    _cache: Option<Cache>,
    generation: i64,
    timer_deadline: i64,
    write_batch_size: usize,
}

impl RocksSnapshotStore {
    pub(crate) fn open_merged(
        config: RocksStoreConfig,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        if aligned && sources.len() == 1 {
            copy_checkpoint_db(&sources[0].0, &config.table_dir)?;
            let resolved =
                crate::state::rocks_config::FlinkRocksOptions::from_json(&config.options_json)
                    .map_err(DataFusionError::Plan)?;
            let (options, cache) = resolved.build(config.shared()).map_err(DataFusionError::Plan)?;
            let write_batch_size = resolved.write_batch_size;
            let db = DB::open(&options, &config.table_dir).map_err(re)?;
            let timer_deadline = db
                .get(SNAPSHOT_TIMER_KEY)
                .map_err(re)?
                .filter(|bytes| bytes.len() == 8)
                .map(|bytes| i64::from_be_bytes(bytes[..8].try_into().unwrap()))
                .unwrap_or(i64::MIN);
            return Ok(Self {
                db,
                _cache: cache,
                generation: sources[0].1,
                timer_deadline,
                write_batch_size,
            });
        }
        std::fs::create_dir_all(&config.table_dir).map_err(ioe)?;
        let resolved =
            crate::state::rocks_config::FlinkRocksOptions::from_json(&config.options_json)
                .map_err(DataFusionError::Plan)?;
        let (options, cache) = resolved.build(config.shared()).map_err(DataFusionError::Plan)?;
        let write_batch_size = resolved.write_batch_size;
        let db = DB::open(&options, &config.table_dir).map_err(re)?;
        let mut timer_deadline = i64::MIN;
        let mut writes = FlinkWriteBatch::new(&db, write_batch_size);
        for (source, _) in sources {
            let source_db =
                DB::open_for_read_only(&Options::default(), source, false).map_err(re)?;
            for row in source_db.iterator(IteratorMode::Start) {
                let (key, value) = row.map_err(re)?;
                if key.as_ref() == SNAPSHOT_TIMER_KEY {
                    if value.len() == 8 {
                        timer_deadline =
                            timer_deadline.max(i64::from_be_bytes(value[..8].try_into().unwrap()));
                    }
                } else if key.len() >= 4 {
                    let kg = i32::from_be_bytes(key[..4].try_into().unwrap());
                    if key_groups.contains(&kg) {
                        writes.put(key, value)?;
                    }
                }
            }
        }
        if timer_deadline != i64::MIN {
            writes.put(SNAPSHOT_TIMER_KEY, timer_deadline.to_be_bytes())?;
        }
        writes.finish()?;
        Ok(Self {
            db,
            _cache: cache,
            generation: 0,
            timer_deadline,
            write_batch_size,
        })
    }

    pub(crate) fn partitions(&self) -> Result<Vec<Vec<u8>>, DataFusionError> {
        let mut out = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.as_ref() != SNAPSHOT_TIMER_KEY {
                out.push(value.to_vec());
            }
        }
        Ok(out)
    }

    pub(crate) fn timer_deadline(&self) -> i64 {
        self.timer_deadline
    }

    pub(crate) fn checkpoint(
        &mut self,
        partitions: &[Vec<u8>],
        timer_deadline: i64,
        snapshot_dir: &str,
    ) -> Result<RocksCheckpointManifest, DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, _) = row.map_err(re)?;
            writes.delete(key)?;
        }
        for partition in partitions {
            if partition.len() < 4 {
                return Err(DataFusionError::Execution(
                    "native state partition has no key-group prefix".into(),
                ));
            }
            writes.put(&partition[..4], &partition[4..])?;
        }
        if timer_deadline != i64::MIN {
            writes.put(SNAPSHOT_TIMER_KEY, timer_deadline.to_be_bytes())?;
        }
        writes.finish()?;
        self.timer_deadline = timer_deadline;
        if snapshot_dir.is_empty() {
            return Ok(RocksCheckpointManifest::absent());
        }
        self.generation += 1;
        checkpoint_files(&self.db, snapshot_dir, self.generation)
    }
}

fn copy_checkpoint_db(source: &str, destination: &str) -> Result<(), DataFusionError> {
    std::fs::create_dir_all(destination).map_err(ioe)?;
    for entry in std::fs::read_dir(source).map_err(ioe)? {
        let entry = entry.map_err(ioe)?;
        if entry.file_type().map_err(ioe)?.is_file() {
            std::fs::copy(
                entry.path(),
                std::path::Path::new(destination).join(entry.file_name()),
            )
            .map_err(ioe)?;
        }
    }
    Ok(())
}

/// RocksDB's native checkpoint flushes live memtables itself before hard-linking the immutable
/// files (there is no WAL to carry them), so a barrier needs no explicit flush call — matching
/// Flink's incremental snapshot strategy.
fn checkpoint_files(
    db: &DB,
    snapshot_dir: &str,
    generation: i64,
) -> Result<RocksCheckpointManifest, DataFusionError> {
    let snapshot_path = std::path::Path::new(snapshot_dir);
    if snapshot_path.exists() {
        std::fs::remove_dir_all(snapshot_path).map_err(ioe)?;
    }
    if let Some(parent) = snapshot_path.parent() {
        std::fs::create_dir_all(parent).map_err(ioe)?;
    }
    Checkpoint::new(db)
        .map_err(re)?
        .create_checkpoint(snapshot_path)
        .map_err(re)?;
    let mut data_files = Vec::new();
    let mut meta_files = Vec::new();
    for entry in std::fs::read_dir(snapshot_path).map_err(ioe)? {
        let entry = entry.map_err(ioe)?;
        if !entry.file_type().map_err(ioe)?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "LOCK" || name.starts_with("LOG") {
            continue;
        }
        if name.ends_with(".sst") {
            data_files.push(name);
        } else {
            meta_files.push(name);
        }
    }
    data_files.sort();
    meta_files.sort();
    Ok(RocksCheckpointManifest {
        snapshot_id: generation,
        data_files,
        meta_files,
    })
}
