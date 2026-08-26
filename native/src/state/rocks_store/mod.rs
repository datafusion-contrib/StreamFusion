//! Rust-owned RocksDB state, on Flink's write path: dirty entries are written through to the
//! RocksDB memtable at every bundle boundary, so RocksDB's own write buffers are the only write
//! buffer and its background threads own all flushing and compaction. Committed entries are keyed
//! by Flink key group (plus a table byte when two stores share one DB) plus BinaryRow bytes and
//! are read directly through RocksDB without a
//! Java/JNI data-plane round trip. Values travel as compact arrow-row bytes, encoded and decoded
//! for a whole bundle's working set in one columnar conversion; a state-TTL value carries its
//! last-write timestamp as a fixed 8-byte prefix so the compaction filter never parses the row.

pub(crate) mod interval_buffer;
pub(crate) mod keep_first_dedup_store;
pub(crate) mod over_agg_store;
pub(crate) mod session_agg_store;
pub(crate) mod temporal_join_store;
pub(crate) mod temporal_sort_buffer;
pub(crate) mod window_agg_store;
pub(crate) mod window_buffer;
pub(crate) mod window_rank_store;
pub(crate) use interval_buffer::{BufferedIntervalRow, RocksIntervalBuffer};
pub(crate) use keep_first_dedup_store::{RocksKeepFirstDedupStore, StoredCandidate};
pub(crate) use over_agg_store::RocksOverAggStore;
pub(crate) use session_agg_store::RocksSessionAggStore;
pub(crate) use temporal_join_store::RocksTemporalJoinStore;
pub(crate) use temporal_sort_buffer::RocksTemporalSortBuffer;
pub(crate) use window_agg_store::RocksWindowAggStore;
pub(crate) use window_buffer::RocksWindowBuffer;
pub(crate) use window_rank_store::RocksWindowRankStore;

use crate::*;
use arrow::row::{RowConverter, SortField};
use rocksdb::checkpoint::Checkpoint;
use rocksdb::{
    Cache, CompactionDecision, Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB,
};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

const SNAPSHOT_TIMER_KEY: &[u8] = b"\xff\xff\xff\xffstreamfusion-timer";

/// The operator's single processing-time timer deadline, persisted by a typed store at checkpoint
/// under a reserved key whose leading bytes can never be a subtask's key group (the snapshot
/// store's convention) — a proctime operator re-arms its firing timer from it after recovery.
pub(super) const TIMER_DEADLINE_KEY: &[u8] = b"\xff\xff\xff\xffstreamfusion-timer-deadline";

/// Reads a store's persisted timer deadline; `i64::MIN` when none was written.
pub(super) fn stored_timer_deadline(db: &DB) -> Result<i64, DataFusionError> {
    Ok(db
        .get(TIMER_DEADLINE_KEY)
        .map_err(re)?
        .filter(|bytes| bytes.len() == 8)
        .map(|bytes| i64::from_be_bytes(bytes[..8].try_into().unwrap()))
        .unwrap_or(i64::MIN))
}

/// Appends the timer deadline to a checkpoint's write batch (`i64::MIN` = no timer, not written —
/// the snapshot store's convention, keeping timer-less checkpoints byte-identical).
pub(super) fn write_timer_deadline(
    writes: &mut FlinkWriteBatch,
    timer_deadline: i64,
) -> Result<(), DataFusionError> {
    if timer_deadline != i64::MIN {
        writes.put(TIMER_DEADLINE_KEY, timer_deadline.to_be_bytes())?;
    }
    Ok(())
}

/// Folds one merged source's copy of the timer deadline into the running max (the multi-source
/// clip merge's convention for reserved keys).
pub(super) fn merged_timer_deadline(current: i64, value: &[u8]) -> i64 {
    if value.len() == 8 {
        current.max(i64::from_be_bytes(value[..8].try_into().unwrap()))
    } else {
        current
    }
}

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

    /// A codec whose value keeps per-key multisets (a MIN/MAX retraction multiset, a DISTINCT
    /// value set) declares one companion element table per multiset: `(table byte, element type)`.
    /// The main row then keys as `[key_group][0][key]` and each element as
    /// `[key_group][table][key][element arrow-row bytes]` → `[count i64 LE]`. The resident
    /// multiset is a PARTIAL view: a bundle point-reads exactly the (key, element) pairs its
    /// batches name (never the whole set), the running value each emit needs travels in the main
    /// row, changes write through as element-level deltas via the value's armed change journal,
    /// and only canonical savepoints materialize a full set. Companion values carry no TTL prefix;
    /// their lifetime follows the main row.
    fn multiset_tables(&self) -> Vec<(u8, DataType)> {
        Vec::new()
    }
    fn arm_multisets(&self, _value: &mut Self::Value) {}
    fn restore_multiset_entry(
        &self,
        _value: &mut Self::Value,
        _table: usize,
        _element: ScalarValue,
        _count: i64,
    ) {
        unreachable!("codec has no multiset tables")
    }
    fn drain_multiset_changes(
        &self,
        _value: &mut Self::Value,
        _table: usize,
    ) -> Vec<(ScalarValue, Option<i64>)> {
        unreachable!("codec has no multiset tables")
    }
    /// The elements one input batch can touch in a companion table, as (elements array, input row
    /// per element): the values the batch folds into the table's aggregate. `u32::MAX` marks an
    /// element outside every row (a sliced list child); `None` means nothing to probe.
    fn multiset_batch_elements(
        &self,
        _batch: &RecordBatch,
        _table: usize,
    ) -> Option<(ArrayRef, Vec<u32>)> {
        unreachable!("codec has no multiset tables")
    }
    /// Whether a companion table's aggregate seeks its minimum (true) or maximum element.
    fn multiset_extreme_is_min(&self, _table: usize) -> bool {
        unreachable!("codec has no multiset tables")
    }
    /// Whether a retraction this bundle removed the table's current extreme (needs a reseek).
    fn multiset_extreme_stale(&self, _value: &Self::Value, _table: usize) -> bool {
        false
    }
    /// Re-establishes a killed extreme from `committed`, which yields the table's committed
    /// elements in extreme order (the value's resident entries override their committed rows).
    fn resolve_multiset_extreme(
        &self,
        _value: &mut Self::Value,
        _table: usize,
        _committed: &mut dyn FnMut() -> Result<Option<ScalarValue>, DataFusionError>,
    ) -> Result<(), DataFusionError> {
        unreachable!("codec has no multiset tables")
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

/// Group-aggregate admission: every main-row state scalar must be a supported row type, and every
/// multiset element type too — a MIN/MAX extreme is its state scalar (covered by `state_types`), a
/// DISTINCT element is the value itself (kind 7's opaque-coded values map to `DataType::Null`,
/// which stays unsupported here and falls back to the snapshot-blob path).
pub(crate) fn rocks_group_supported(
    kinds: &[i64],
    value_types: &[DataType],
    state_types: &[DataType],
) -> bool {
    rocks_row_supported(state_types)
        && kinds.iter().zip(value_types).all(|(&kind, value_type)| {
            !matches!(kind, 7 | 9) || rocks_row_supported(std::slice::from_ref(value_type))
        })
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
        (self.shared_resources != 0).then(|| unsafe { &*(self.shared_resources as *const _) })
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

    fn delete_range<K: AsRef<[u8]>>(&mut self, from: K, to: K) -> Result<(), DataFusionError> {
        self.batch.delete_range(from, to);
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

/// One companion element table of a multiset codec: its key byte after the key group, and the
/// arrow-row conversion for its element bytes.
struct MultisetTable {
    table: u8,
    element_type: DataType,
    converter: RowConverter,
}

impl MultisetTable {
    fn decode_element(&self, bytes: &[u8]) -> Result<ScalarValue, DataFusionError> {
        let parser = self.converter.parser();
        let columns = self
            .converter
            .convert_rows(vec![parser.parse(bytes)])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(ScalarValue::try_from_array(&columns[0], 0)?)
    }
}

/// The smallest byte string greater than every key beginning with `prefix` — the exclusive upper
/// bound of the prefix's range. A key-group prefix starts below 0xff, so the carry terminates.
fn prefix_successor(prefix: &[u8]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.last_mut() {
        if *last == 0xff {
            out.pop();
        } else {
            *last += 1;
            return out;
        }
    }
    unreachable!("a key-group prefix has a byte below 0xff")
}

pub(crate) struct RocksStore<C: RocksStateCodec> {
    db: Arc<DB>,
    _cache: Option<Cache>,
    config: RocksStoreConfig,
    codec: C,
    /// Table prefix byte after the key-group prefix, for stores sharing one DB (see
    /// [`RocksStore::create_pair`]) and for multiset codecs (main rows under
    /// [`MULTISET_MAIN_TABLE`]); `None` keeps the single-table layout `[key_group][key]`.
    table: Option<u8>,
    multisets: Vec<MultisetTable>,
    /// Composite companion keys already point-probed this bundle — each (key, element) is read
    /// from the committed table at most once, and a probe never clobbers a resident update.
    probed_multiset_keys: ahash::HashSet<Vec<u8>>,
    /// Keys removed this bundle: their committed companion ranges are deleted ahead of the
    /// bundle's element puts (so a re-created key keeps its new elements), and later probes must
    /// not hydrate their stale committed rows.
    removed_multiset_keys: ahash::HashSet<ByteKey>,
    value_fields: Vec<Field>,
    converter: RowConverter,
    now_ms: i64,
    clock: Arc<AtomicI64>,
    generation: i64,
    write_batch_size: usize,
    working: ahash::HashMap<ByteKey, Slot<C::Value>>,
    footprint: isize,
}

const PAIR_FIRST_TABLE: u8 = 0;
const PAIR_SECOND_TABLE: u8 = 1;
const MULTISET_MAIN_TABLE: u8 = 0;

struct OpenedDb {
    db: Arc<DB>,
    cache: Option<Cache>,
    clock: Arc<AtomicI64>,
    write_batch_size: usize,
}

/// Opens one physical DB for the stores that will share it. `ttls` gives each table's TTL for the
/// per-DB compaction filter (`None` table = every key, the single-table case); a table whose TTL
/// is off never has its value parsed, matching that table's prefix-free value layout.
fn open_shared_db(
    config: &RocksStoreConfig,
    ttls: &[(Option<u8>, i64)],
) -> Result<OpenedDb, DataFusionError> {
    std::fs::create_dir_all(&config.table_dir).map_err(ioe)?;
    let resolved = crate::state::rocks_config::FlinkRocksOptions::from_json(&config.options_json)
        .map_err(DataFusionError::Plan)?;
    let (mut options, cache) = resolved
        .build(config.shared())
        .map_err(DataFusionError::Plan)?;
    let write_batch_size = resolved.write_batch_size;
    let clock = Arc::new(AtomicI64::new(0));
    if ttls.iter().any(|&(_, ttl)| ttl > 0) {
        let filter_clock = Arc::clone(&clock);
        let table_ttls: Vec<(Option<u8>, i64)> = ttls.to_vec();
        let refresh_after = resolved
            .compaction_filter_query_time_after_num_entries
            .max(1);
        let mut remaining = 0u64;
        let mut now = 0i64;
        options.set_compaction_filter("streamfusion-state-ttl", move |_level, key, value| {
            if remaining == 0 {
                now = filter_clock.load(Ordering::Relaxed);
                remaining = refresh_after;
            }
            remaining -= 1;
            let ttl_ms = table_ttls
                .iter()
                .find(|(table, _)| table.map_or(true, |t| key.get(4) == Some(&t)))
                .map_or(0, |&(_, ttl)| ttl);
            if ttl_ms <= 0 {
                return CompactionDecision::Keep;
            }
            match persisted_write_ms(value) {
                Some(written) if now >= written.saturating_add(ttl_ms) => {
                    CompactionDecision::Remove
                }
                _ => CompactionDecision::Keep,
            }
        });
    }
    let db = Arc::new(DB::open(&options, &config.table_dir).map_err(re)?);
    Ok(OpenedDb {
        db,
        cache,
        clock,
        write_batch_size,
    })
}

impl<C: RocksStateCodec> RocksStore<C> {
    const SLOT_OVERHEAD: usize = std::mem::size_of::<Slot<C::Value>>() + GROUP_ENTRY_OVERHEAD;

    pub(crate) fn create(config: RocksStoreConfig, codec: C) -> Result<Self, DataFusionError> {
        Self::open_db(config, codec)
    }

    /// Opens two stores over one shared DB — for an operator whose Flink analog keeps two named
    /// states (the updating join's left and right sides) but checkpoints them as one table
    /// directory. Each store keys as `[key_group i32 BE][table u8][key bytes]`; the key group
    /// stays the first four bytes so rescale clipping is layout-agnostic. `config.ttl_ms` is the
    /// first store's TTL, `second_ttl_ms` the second's; the shared compaction filter dispatches
    /// on the table byte.
    pub(crate) fn create_pair(
        config: RocksStoreConfig,
        second_ttl_ms: i64,
        codecs: (C, C),
    ) -> Result<(Self, Self), DataFusionError> {
        Self::ensure_supported(&codecs.0)?;
        Self::ensure_supported(&codecs.1)?;
        debug_assert!(
            codecs.0.multiset_tables().is_empty() && codecs.1.multiset_tables().is_empty(),
            "a paired store cannot also carry multiset tables"
        );
        let mut second_config = config.clone();
        second_config.ttl_ms = second_ttl_ms;
        let opened = open_shared_db(
            &config,
            &[
                (Some(PAIR_FIRST_TABLE), config.ttl_ms),
                (Some(PAIR_SECOND_TABLE), second_ttl_ms),
            ],
        )?;
        let first = Self::attach(&opened, config, codecs.0, Some(PAIR_FIRST_TABLE))?;
        let second = Self::attach(&opened, second_config, codecs.1, Some(PAIR_SECOND_TABLE))?;
        Ok((first, second))
    }

    /// [`RocksStore::open_merged`] for a shared-DB pair: one physical restore serves both stores
    /// (the table byte rides inside every copied key, and clipping still reads only the leading
    /// key-group bytes).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_merged_pair(
        config: RocksStoreConfig,
        second_ttl_ms: i64,
        codecs: (C, C),
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
        now_ms: i64,
    ) -> Result<(Self, Self), DataFusionError> {
        if aligned && sources.len() == 1 {
            copy_checkpoint_db(&sources[0].0, &config.table_dir)?;
            let (mut first, mut second) = Self::create_pair(config, second_ttl_ms, codecs)?;
            first.set_clock(now_ms);
            second.set_clock(now_ms);
            first.generation = sources[0].1;
            return Ok((first, second));
        }
        let (mut first, mut second) = Self::create_pair(config, second_ttl_ms, codecs)?;
        first.set_clock(now_ms);
        second.set_clock(now_ms);
        let mut writes = FlinkWriteBatch::new(&first.db, first.write_batch_size);
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
        Ok((first, second))
    }

    /// Commits both stores of a shared-DB pair and takes the single native checkpoint of their DB
    /// — the pair analog of [`RocksStore::checkpoint`].
    pub(crate) fn checkpoint_pair(
        first: &mut Self,
        second: &mut Self,
        snapshot_dir: &str,
    ) -> Result<RocksCheckpointManifest, DataFusionError> {
        first.write_dirty()?;
        first.working.clear();
        second.write_dirty()?;
        second.working.clear();
        if snapshot_dir.is_empty() {
            return Ok(RocksCheckpointManifest::absent());
        }
        first.generation += 1;
        checkpoint_files(&first.db, snapshot_dir, first.generation)
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

    fn ensure_supported(codec: &C) -> Result<(), DataFusionError> {
        if codec.supported() {
            Ok(())
        } else {
            Err(DataFusionError::Plan(
                "state shape not supported by RocksDB".into(),
            ))
        }
    }

    fn open_db(config: RocksStoreConfig, codec: C) -> Result<Self, DataFusionError> {
        Self::ensure_supported(&codec)?;
        // Multiset codecs move the main rows under an explicit table byte so the compaction
        // filter's TTL applies to main rows only: a companion value is a bare count with no
        // timestamp prefix, and its lifetime follows the main row (deleted with it, or lazily on
        // reading an expired/absent main row) rather than any timestamp of its own.
        let multiset_tables = codec.multiset_tables();
        let table = (!multiset_tables.is_empty()).then_some(MULTISET_MAIN_TABLE);
        let mut ttls = vec![(table, config.ttl_ms)];
        ttls.extend(multiset_tables.iter().map(|&(table, _)| (Some(table), 0)));
        let opened = open_shared_db(&config, &ttls)?;
        Self::attach(&opened, config, codec, table)
    }

    fn attach(
        opened: &OpenedDb,
        config: RocksStoreConfig,
        codec: C,
        table: Option<u8>,
    ) -> Result<Self, DataFusionError> {
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
        let multisets = codec
            .multiset_tables()
            .into_iter()
            .map(|(table, element_type)| {
                RowConverter::new(vec![SortField::new(element_type.clone())])
                    .map(|converter| MultisetTable {
                        table,
                        element_type,
                        converter,
                    })
                    .map_err(|e| DataFusionError::External(Box::new(e)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            db: Arc::clone(&opened.db),
            _cache: opened.cache.clone(),
            config,
            codec,
            table,
            multisets,
            probed_multiset_keys: ahash::HashSet::default(),
            removed_multiset_keys: ahash::HashSet::default(),
            value_fields,
            converter,
            now_ms: 0,
            clock: Arc::clone(&opened.clock),
            generation: 0,
            write_batch_size: opened.write_batch_size,
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
        let mut out = Vec::with_capacity(self.key_prefix_len() + key.len());
        let key_group =
            flink_key_group(hash_bytes_by_words(key), self.config.max_parallelism) as i32;
        out.extend_from_slice(&key_group.to_be_bytes());
        out.extend(self.table);
        out.extend_from_slice(key);
        out
    }

    fn key_prefix_len(&self) -> usize {
        4 + usize::from(self.table.is_some())
    }

    /// The `[key_group][table][key]` prefix under which one key's multiset elements live.
    fn multiset_prefix(&self, table: u8, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + key.len());
        let key_group =
            flink_key_group(hash_bytes_by_words(key), self.config.max_parallelism) as i32;
        out.extend_from_slice(&key_group.to_be_bytes());
        out.push(table);
        out.extend_from_slice(key);
        out
    }

    /// Loads EVERY persisted multiset element of the given keys into their resident states —
    /// canonical savepoints only, which must materialize the full logical sets. The data plane
    /// never calls this: bundles hydrate per touched element through [`Self::probe_multisets`].
    fn hydrate_multisets(&mut self, keys: &[ByteKey]) -> Result<(), DataFusionError> {
        if keys.is_empty() {
            return Ok(());
        }
        for position in 0..self.multisets.len() {
            let table = &self.multisets[position];
            let mut owners: Vec<usize> = Vec::new();
            let mut counts: Vec<i64> = Vec::new();
            let mut elements: Vec<Vec<u8>> = Vec::new();
            for (owner, key) in keys.iter().enumerate() {
                let prefix = self.multiset_prefix(table.table, &key.0);
                for row in self
                    .db
                    .iterator(IteratorMode::From(&prefix, Direction::Forward))
                {
                    let (db_key, value) = row.map_err(re)?;
                    if !db_key.starts_with(&prefix) {
                        break;
                    }
                    owners.push(owner);
                    counts.push(i64::from_le_bytes(
                        value[..8].try_into().expect("multiset count"),
                    ));
                    elements.push(db_key[prefix.len()..].to_vec());
                }
            }
            if owners.is_empty() {
                continue;
            }
            let parser = table.converter.parser();
            let rows: Vec<_> = elements.iter().map(|bytes| parser.parse(bytes)).collect();
            let columns = table
                .converter
                .convert_rows(rows)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            for (row, (&owner, &count)) in owners.iter().zip(&counts).enumerate() {
                let element = ScalarValue::try_from_array(&columns[0], row)?;
                if let Some(Slot::Present { state, .. }) = self.working.get_mut(&*keys[owner].0) {
                    self.codec
                        .restore_multiset_entry(state, position, element, count);
                }
            }
        }
        Ok(())
    }

    /// Range-deletes every companion row of keys whose main row was found absent or expired on
    /// read. A main row physically dropped by the TTL compaction filter leaves its companion rows
    /// behind (their values carry no timestamp to judge them by); this lazy sweep on the key's
    /// next touch is what reclaims them — and is required for correctness before the key is
    /// re-created, since element writes are deltas against the persisted set.
    fn purge_multisets(&self, keys: &[ByteKey]) -> Result<(), DataFusionError> {
        if keys.is_empty() || self.multisets.is_empty() {
            return Ok(());
        }
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for key in keys {
            for table in &self.multisets {
                let prefix = self.multiset_prefix(table.table, &key.0);
                let upper = prefix_successor(&prefix);
                writes.delete_range(prefix, upper)?;
            }
        }
        writes.finish()
    }

    /// Point-hydrates exactly the (key, element) pairs one batch names: the codec extracts each
    /// companion table's element column, the store multi-gets the composite keys not yet probed
    /// this bundle, and the hits become the touched keys' resident partial views. Fresh and
    /// removed keys are skipped — their committed ranges hold nothing this bundle may read.
    fn probe_multisets(
        &mut self,
        batch: &RecordBatch,
        key_columns: &[usize],
        precisions: &[i32],
    ) -> Result<(), DataFusionError> {
        for position in 0..self.multisets.len() {
            let Some((elements, rows)) = self.codec.multiset_batch_elements(batch, position) else {
                continue;
            };
            if elements.is_empty() {
                continue;
            }
            let table = &self.multisets[position];
            let element_rows = table
                .converter
                .convert_columns(&[elements.clone()])
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, precisions);
            let mut probe_keys: Vec<Vec<u8>> = Vec::new();
            let mut probe_meta: Vec<(usize, ByteKey)> = Vec::new();
            for (index, &row) in rows.iter().enumerate() {
                if row == u32::MAX || elements.is_null(index) {
                    continue;
                }
                let key = encoder.encode(row as usize);
                if !matches!(self.working.get(key), Some(Slot::Present { .. }))
                    || self.removed_multiset_keys.contains(key)
                {
                    continue;
                }
                let mut db_key = self.multiset_prefix(table.table, key);
                db_key.extend_from_slice(element_rows.row(index).data());
                if !self.probed_multiset_keys.insert(db_key.clone()) {
                    continue;
                }
                probe_keys.push(db_key);
                probe_meta.push((index, ByteKey::from(key)));
            }
            if probe_keys.is_empty() {
                continue;
            }
            let fetched = self.db.multi_get(&probe_keys);
            for (value, (index, key)) in fetched.into_iter().zip(&probe_meta) {
                match value {
                    Ok(Some(bytes)) => {
                        let count =
                            i64::from_le_bytes(bytes[..8].try_into().expect("multiset count"));
                        let element = ScalarValue::try_from_array(&elements, *index)?;
                        if let Some(Slot::Present { state, .. }) = self.working.get_mut(&*key.0) {
                            self.codec
                                .restore_multiset_entry(state, position, element, count);
                        }
                    }
                    Ok(None) => {}
                    Err(error) => return Err(re(error)),
                }
            }
        }
        Ok(())
    }

    /// Drains the bundle's journaled element changes into encoded companion keys (`None` count =
    /// delete), one columnar conversion per table.
    fn drain_multiset_ops(&mut self) -> Result<Vec<(Vec<u8>, Option<i64>)>, DataFusionError> {
        if self.multisets.is_empty() {
            return Ok(Vec::new());
        }
        let tables = self.multisets.len();
        let mut scalars: Vec<Vec<ScalarValue>> = vec![Vec::new(); tables];
        let mut entries: Vec<Vec<(ByteKey, Option<i64>)>> = vec![Vec::new(); tables];
        for (key, slot) in self.working.iter_mut() {
            if let Slot::Present { state, dirty: true } = slot {
                for position in 0..tables {
                    for (element, count) in self.codec.drain_multiset_changes(state, position) {
                        scalars[position].push(element);
                        entries[position].push((key.clone(), count));
                    }
                }
            }
        }
        let mut ops = Vec::new();
        for (position, table) in self.multisets.iter().enumerate() {
            let scalars = std::mem::take(&mut scalars[position]);
            if scalars.is_empty() {
                continue;
            }
            let array = scalars_to_array(scalars, &table.element_type);
            let rows = table
                .converter
                .convert_columns(&[array])
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            for (row, (key, count)) in rows.iter().zip(&entries[position]) {
                let mut db_key = self.multiset_prefix(table.table, &key.0);
                db_key.extend_from_slice(row.data());
                ops.push((db_key, *count));
            }
        }
        Ok(ops)
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
        let multiset_changes = self.drain_multiset_ops()?;
        let removed_keys: Vec<ByteKey> = self.removed_multiset_keys.drain().collect();
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
        if keys.is_empty()
            && deletes.is_empty()
            && removed_keys.is_empty()
            && multiset_changes.is_empty()
        {
            return Ok(());
        }
        let db = Arc::clone(&self.db);
        let mut writes = FlinkWriteBatch::new(&db, self.write_batch_size);
        // A removed key's committed companion ranges go first, so a key re-created in the same
        // bundle keeps the element puts written below.
        for key in &removed_keys {
            for table in &self.multisets {
                let prefix = self.multiset_prefix(table.table, &key.0);
                let upper = prefix_successor(&prefix);
                writes.delete_range(prefix, upper)?;
            }
        }
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
        for (db_key, count) in multiset_changes {
            match count {
                Some(count) => writes.put(db_key, count.to_le_bytes())?,
                None => writes.delete(db_key)?,
            }
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
        self.probed_multiset_keys.clear();
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
        let prefix = self.key_prefix_len();
        let mut keys = std::collections::BTreeMap::<i32, Vec<ByteKey>>::new();
        let mut all_keys = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (db_key, value) = row.map_err(re)?;
            if db_key.len() < prefix || db_key.as_ref() == SNAPSHOT_TIMER_KEY {
                continue;
            }
            if self.table.is_some_and(|table| db_key[4] != table) {
                continue;
            }
            let key_group = i32::from_be_bytes(db_key[..4].try_into().unwrap());
            if key_group < 0 {
                continue; // a reserved key, never a subtask's key group
            }
            if let Some(state) = self.decode_value(&value)? {
                let key = ByteKey::from(&db_key[prefix..]);
                self.working.insert(
                    key.clone(),
                    Slot::Present {
                        state,
                        dirty: false,
                    },
                );
                keys.entry(key_group).or_default().push(key.clone());
                all_keys.push(key);
            }
        }
        self.hydrate_multisets(&all_keys)?;
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
    fn insert(&mut self, key: ByteKey, mut value: C::Value) -> &mut C::Value {
        self.codec.arm_multisets(&mut value);
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
        if !self.multisets.is_empty() {
            self.removed_multiset_keys.insert(ByteKey::from(key));
        }
        self.working
            .insert(ByteKey::from(key), Slot::Absent { dirty: true });
    }
    fn resolve_multiset_extremes(&mut self, key: &[u8]) -> Result<(), DataFusionError> {
        for position in 0..self.multisets.len() {
            let stale = match self.working.get(key) {
                Some(Slot::Present { state, .. }) => {
                    self.codec.multiset_extreme_stale(state, position)
                }
                _ => false,
            };
            if !stale {
                continue;
            }
            let table = &self.multisets[position];
            let prefix = self.multiset_prefix(table.table, key);
            let upper = prefix_successor(&prefix);
            let is_min = self.codec.multiset_extreme_is_min(position);
            let mut iterator = if is_min {
                self.db
                    .iterator(IteratorMode::From(&prefix, Direction::Forward))
            } else {
                self.db
                    .iterator(IteratorMode::From(&upper, Direction::Reverse))
            };
            let mut committed = || -> Result<Option<ScalarValue>, DataFusionError> {
                for row in iterator.by_ref() {
                    let (db_key, _) = row.map_err(re)?;
                    if !is_min && db_key.as_ref() >= upper.as_slice() {
                        continue;
                    }
                    if !db_key.starts_with(&prefix) {
                        return Ok(None);
                    }
                    return table.decode_element(&db_key[prefix.len()..]).map(Some);
                }
                Ok(None)
            };
            if let Some(Slot::Present { state, .. }) = self.working.get_mut(key) {
                self.codec
                    .resolve_multiset_extreme(state, position, &mut committed)?;
            }
        }
        Ok(())
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
            if self.multisets.is_empty() {
                return Ok(());
            }
            return self.probe_multisets(batch, key_columns, precisions);
        }
        let db_keys: Vec<_> = missing.iter().map(|key| self.db_key(&key.0)).collect();
        let fetched = self.db.multi_get(&db_keys);
        let mut hit_keys = Vec::new();
        let mut hit_values = Vec::new();
        let mut purge = Vec::new();
        for (key, value) in missing.iter().zip(&fetched) {
            match value {
                Ok(Some(bytes)) => {
                    hit_keys.push(key.clone());
                    hit_values.push(bytes.as_slice());
                }
                Ok(None) => {
                    // With TTL off, a companion row cannot outlive its main row (every main-row
                    // delete purges companions), so an absent main means no elements to sweep.
                    if !self.multisets.is_empty() && self.config.ttl_ms > 0 {
                        purge.push(key.clone());
                    }
                    self.working
                        .insert(key.clone(), Slot::Absent { dirty: false });
                }
                Err(error) => return Err(re(error.clone())),
            }
        }
        let mut hydrate = Vec::new();
        for (key, state) in hit_keys.into_iter().zip(self.decode_values(&hit_values)?) {
            let slot = match state {
                Some(state) => {
                    if !self.multisets.is_empty() {
                        hydrate.push(key.clone());
                    }
                    Slot::Present {
                        state,
                        dirty: false,
                    }
                }
                None => {
                    if !self.multisets.is_empty() {
                        purge.push(key.clone());
                    }
                    Slot::Absent { dirty: true }
                }
            };
            self.working.insert(key, slot);
        }
        if !self.multisets.is_empty() {
            self.purge_multisets(&purge)?;
            for key in &hydrate {
                if let Some(Slot::Present { state, .. }) = self.working.get_mut(&*key.0) {
                    self.codec.arm_multisets(state);
                }
            }
            self.probe_multisets(batch, key_columns, precisions)?;
        }
        Ok(())
    }
    fn end_bundle(&mut self) -> Result<(), DataFusionError> {
        self.write_dirty()?;
        self.working.clear();
        self.probed_multiset_keys.clear();
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
            let (options, cache) = resolved
                .build(config.shared())
                .map_err(DataFusionError::Plan)?;
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
        let (options, cache) = resolved
            .build(config.shared())
            .map_err(DataFusionError::Plan)?;
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
