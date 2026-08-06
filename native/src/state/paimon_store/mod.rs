//! Persistent state on local Apache Paimon primary-key tables: a write buffer over a disk table,
//! nothing else.
//!
//! The store holds exactly two components. The **write buffer** is the in-memory map of every
//! entry written since the last local flush (upserts and removals); it answers reads for
//! those keys directly and is the only state that survives across batches. The **disk table** is
//! the committed Paimon snapshot, immutable between barriers. Each processed batch resolves its
//! reads with one point-read join: the batch's keys not already in the write buffer are pushed
//! into the table reader as an exact `IN` predicate (file/page stats prune, then a single
//! hash-set pass filters rows at parquet decode), and the matched rows live only until the end
//! of the batch's bundle — there is no retained cache of clean rows between bundles; re-reads
//! are served by the OS page cache plus decode, not by a second copy of the state in memory.
//!
//! A size-triggered flush or checkpoint barrier encodes the write buffer as one Arrow batch
//! (`_VALUE_KIND` carries upsert vs delete per row), commits a local snapshot, and clears it. Local
//! commits are runtime state only; Flink durability still lands exactly at checkpoints, which pin
//! one snapshot's immutable files for incremental upload.
//!
//! The table carries a computed `kg` INT column (`flink_key_group` of the BinaryRow key bytes) as
//! the leading primary-key column, so files' row groups are key-group-clustered, but the bucket
//! count is deliberately small and decoupled from max parallelism (default 1: one LSM per
//! subtask, the RocksDB shape). Rescale clips by key-group range at recovery time
//! (`clip_from_sources`); an aligned restore adopts the files wholesale.
//!
//! This store never compacts. paimon-rust has no LSM compaction yet, and rather than carry a
//! second maintenance implementation, table maintenance belongs exclusively to the optional Java
//! Paimon compactor module, which runs stock Paimon's compaction against this table at each
//! barrier, directly beneath the data commit (the store adopts its snapshots by re-pinning at
//! checkpoint start). Without it, tables stay correct but accumulate one level-0 run per touched
//! bucket per checkpoint — the host warns when the backend runs unmaintained.
//!
//! This file holds what every store shape shares: the type mapping and codecs, the table
//! configuration, and the generic key/value store over one Paimon table. Each operator's own
//! shape — list, map, the window and join buffers, and so on — lives in its own submodule
//! beside this one and is re-exported here, so callers keep addressing `paimon_store::Thing`.

mod list;
mod map;
mod keep_first;
mod updatable_topn;
mod window_rank;
mod row_buffer;
mod over;
mod window_join;
mod window_agg;
mod session_agg;
mod interval_join;
mod temporal_join;
mod deadline;

pub(crate) use list::*;
pub(crate) use map::*;
pub(crate) use updatable_topn::*;
pub(crate) use keep_first::*;
pub(crate) use window_rank::*;
pub(crate) use row_buffer::*;
pub(crate) use over::*;
pub(crate) use window_join::*;
pub(crate) use window_agg::*;
pub(crate) use session_agg::*;
pub(crate) use interval_join::*;
pub(crate) use temporal_join::*;
pub(crate) use deadline::*;

use crate::state::dirty_region::DirtyRegion;
use crate::*;
use arrow::array::{Array, BinaryArray, Int32Array, Int64Array, Int8Array};
use paimon::catalog::Identifier;
use paimon::io::FileIO;
use paimon::spec::{
    BigIntType, BooleanType, DataField, DataType as PaimonType, Datum, DateType,
    DecimalType, DoubleType, FloatType, IntType, Predicate, PredicateBuilder,
    Schema as PaimonSchema, SmallIntType, TableSchema, TimestampType, TinyIntType,
    VarBinaryType, VarCharType, EMPTY_SERIALIZED_ROW,
};
use paimon::table::{CommitMessage, DataSplit, Table};
use std::collections::HashSet as StdHashSet;
use std::sync::OnceLock;

const KG_COLUMN: &str = "kg";
const KEY_COLUMN: &str = "k";
const VALUE_KIND_COLUMN: &str = "_VALUE_KIND";

const DEADLINE_COLUMN: &str = "cleanup_at";
const FIRED_COLUMN: &str = "fired";
const INNER_RANK_COLUMN: &str = "ir";
const KIND_COLUMN: &str = "kind";
const MATCHED_COLUMN: &str = "matched";
const ORD_COLUMN: &str = "ord";
const RT_COLUMN: &str = "rt";
const SEQ_COLUMN: &str = "seq";
const SUB_KEY_COLUMN: &str = "r";
const WINDOW_END_COLUMN: &str = "we";
const WINDOW_START_COLUMN: &str = "ws";

/// The store-managed state-TTL column: each value row's last-write wall clock (epoch millis,
/// absolute), appended as the LAST value column only when the store's TTL is on — a TTL-off
/// table keeps the pre-TTL schema exactly.
const TS_COLUMN: &str = "ts";

/// The per-operator half of the store: the value columns beyond `kg`/`k`, and how one state value
/// maps to and from one row of those columns. The store owns keys, buckets, hydration, dirty
/// tracking, and the checkpoint file protocol; a codec owns only its row shape, so a new operator
/// plugs in with a schema fragment and a scalar round-trip.
pub(crate) trait PaimonStateCodec {
    type Value;

    /// Whether this operator instance's state shape is persistable at all (type map coverage,
    /// operator-specific restrictions). False keeps the operator on the memory backend.
    fn supported(&self) -> bool;

    /// The value columns beyond `kg`/`k`, in persisted order. All are stored nullable — a
    /// tombstone row carries nulls.
    fn value_fields(&self) -> Vec<(String, DataType)>;

    /// Encodes a value as one scalar per value column, in `value_fields` order.
    fn encode(&self, value: &Self::Value) -> Vec<ScalarValue>;

    /// Decodes one probe row (one scalar per value column) — the inverse of `encode`.
    fn decode(&self, scalars: &[ScalarValue]) -> Self::Value;

    /// The value's accounted heap footprint, mirroring the operator's own per-row tracking.
    fn value_bytes(&self, value: &Self::Value) -> usize;

    /// The value's TTL timestamp (state TTL); consulted only when the store carries the ts column.
    fn write_ms(&self, _value: &Self::Value) -> i64 {
        0
    }

    /// Stamps a decoded value with its persisted TTL timestamp.
    fn stamp_write_ms(&self, _value: &mut Self::Value, _ts_ms: i64) {}
}

/// One shared runtime for all Paimon state IO: probes and commits run on the Flink task thread via
/// `block_on`, so the runtime only needs to drive opendal's local-fs operations.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("paimon-state-io")
            .enable_all()
            .build()
            .expect("paimon state runtime")
    })
}

fn pe(e: paimon::Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// A probe column cast to the row codec's expected Arrow type when the file format decoded it as
/// a different (compatible) representation, e.g. a binary view.
fn normalized_column(
    batch: &RecordBatch,
    index: usize,
    expected: &Field,
) -> Result<ArrayRef, DataFusionError> {
    let column = batch.column(index);
    if column.data_type() == expected.data_type() {
        Ok(column.clone())
    } else {
        arrow::compute::cast(column, expected.data_type())
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

fn io(e: std::io::Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// The subset of Arrow state/key types this backend persists. Anything outside it (and any
/// multiset-backed aggregate) keeps the memory backend — a per-operator fallback, never an error
/// at runtime.
fn paimon_type_of(dt: &DataType) -> Option<PaimonType> {
    Some(match dt {
        DataType::Boolean => PaimonType::Boolean(BooleanType::new()),
        DataType::Int8 => PaimonType::TinyInt(TinyIntType::new()),
        DataType::Int16 => PaimonType::SmallInt(SmallIntType::new()),
        DataType::Int32 => PaimonType::Int(IntType::new()),
        DataType::Int64 => PaimonType::BigInt(BigIntType::new()),
        DataType::Float32 => PaimonType::Float(FloatType::new()),
        DataType::Float64 => PaimonType::Double(DoubleType::new()),
        DataType::Utf8 => PaimonType::VarChar(VarCharType::string_type()),
        DataType::Binary => {
            PaimonType::VarBinary(VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).ok()?)
        }
        DataType::Date32 => PaimonType::Date(DateType::new()),
        DataType::Decimal128(p, s) if *s >= 0 => {
            PaimonType::Decimal(DecimalType::new(*p as u32, *s as u32).ok()?)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None) => {
            PaimonType::Timestamp(TimestampType::new(3).ok()?)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            PaimonType::Timestamp(TimestampType::new(6).ok()?)
        }
        // The host bridge pins every Flink TIMESTAMP/TIMESTAMP_LTZ column to nanoseconds with no
        // zone, so this arm is what row payloads carrying a rowtime column actually hit.
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
            PaimonType::Timestamp(TimestampType::new(9).ok()?)
        }
        _ => return None,
    })
}

/// True when every listed column type is persistable by this backend's type map.
pub(crate) fn paimon_row_supported(types: &[DataType]) -> bool {
    types.iter().all(|t| paimon_type_of(t).is_some())
}

/// The shared half of every row-payload codec (keep-last dedup, changelog normalize): the
/// persisted value IS the operator's stored full row as typed columns — never the transient
/// arrow-row bytes, mirroring the raw keyed-state snapshots (arrow-row encoding is not a stable
/// wire format). A side effect worth having: the state table reads like the operator's output
/// table itself.
pub(crate) struct RowPayloadCodec {
    row_types: Vec<DataType>,
    converter: arrow::row::RowConverter,
}

impl RowPayloadCodec {
    pub(crate) fn new(row_types: Vec<DataType>) -> Self {
        let converter = arrow::row::RowConverter::new(
            row_types.iter().map(|t| arrow::row::SortField::new(t.clone())).collect(),
        )
        .expect("row payload codec converter");
        RowPayloadCodec { row_types, converter }
    }

    pub(crate) fn supported(&self) -> bool {
        paimon_row_supported(&self.row_types)
    }

    pub(crate) fn fields(&self) -> Vec<(String, DataType)> {
        self.row_types
            .iter()
            .enumerate()
            .map(|(i, t)| (format!("c{i}"), t.clone()))
            .collect()
    }

    pub(crate) fn encode_payload(&self, payload: &[u8]) -> Vec<ScalarValue> {
        let parser = self.converter.parser();
        let columns = self
            .converter
            .convert_rows([parser.parse(payload)])
            .expect("decode row payload for persistence");
        columns
            .iter()
            .map(|column| ScalarValue::try_from_array(column, 0).expect("row payload scalar"))
            .collect()
    }

    /// Rebuilds the one-row typed columns and the arrow-row payload from a persisted row. The
    /// columns come back too so a codec can derive extra state from them (dedup's rowtime).
    pub(crate) fn decode_payload(&self, scalars: &[ScalarValue]) -> (Arc<[u8]>, Vec<ArrayRef>) {
        let columns: Vec<ArrayRef> = scalars
            .iter()
            .zip(&self.row_types)
            .map(|(scalar, data_type)| scalars_to_array(vec![scalar.clone()], data_type))
            .collect();
        let rows = self.converter.convert_columns(&columns).expect("encode hydrated row payload");
        (Arc::from(rows.row(0).data()), columns)
    }
}

/// True when every aggregate state column (and by construction the row codec) is persistable.
pub(crate) fn paimon_group_supported(kinds: &[i64], state_types: &[DataType]) -> bool {
    group_kinds_persistable(kinds) && paimon_row_supported(state_types)
}

pub(crate) struct PaimonStoreConfig {
    /// Absolute local directory holding this operator subtask's table (chosen by the host).
    pub table_dir: String,
    /// Flink maxParallelism — the modulus of the key-group column (`kg = hash mod this`).
    pub max_parallelism: usize,
    /// The table's Paimon bucket count. Deliberately small and decoupled from max parallelism
    /// (default 1: one LSM per subtask, the RocksDB shape): a bucket per key group wrote one
    /// file per touched key group per commit — fragmentation proportional to max parallelism.
    /// Key-group locality survives de-bucketing because `kg` leads the primary key, so files'
    /// row groups are kg-clustered and hydration prunes by key-group predicate; rescale pays a
    /// one-time clip at recovery instead of free bucket adoption (see `clip_from_sources`).
    pub buckets: usize,
    /// Paimon `file.format` for state data files.
    pub file_format: String,
    /// Paimon `file.compression` for state data files ("uncompressed", "zstd", "snappy", ...).
    /// Stamped into the table schema, so an external compactor's rewrites honor it too.
    pub file_compression: String,
    /// Stamp `deletion-vectors.enabled` on new tables. The host's Java compactor runs
    /// *synchronously at every barrier* (Paimon's own `lookup-wait` model), so every
    /// committed-and-checkpointed snapshot holds only level-1+ files whose stale rows are
    /// masked by deletion vectors — reads take the raw parquet path with exact predicate
    /// pushdown, never the merge reader. Production (the JNI layer) always passes true; the
    /// backend refuses to start without a deletion-vector-capable compactor. `false` exists
    /// for the Rust unit suite only: it reads its own commits without a Java compactor, which
    /// a deletion-vector table cannot do (level-0 runs are invisible to raw scans until
    /// maintenance up-levels them — see `paimon_deletion_vector_table_double_checkpoints`).
    pub deletion_vectors: bool,
    /// Idle-state retention millis (0 = off — Flink's default). When on, the generic KV store
    /// carries each value's last-write wall clock in a trailing `ts` column and enforces
    /// delete-on-read expiry when it decodes committed rows (see `TS_COLUMN`).
    pub ttl_ms: i64,
}

/// A checkpoint's file manifest, handed to the host for upload. `data_files` are immutable,
/// uniquely named, and shared across checkpoints (incremental dedup by name); `meta_files` are the
/// snapshot/manifest/schema documents pinned to this snapshot (re-uploaded each checkpoint —
/// small). All paths are relative to the table root; the host hard-links the files its upload
/// will read into a per-checkpoint directory, so uploads survive local compaction and GC.
#[derive(serde::Serialize)]
pub(crate) struct PaimonCheckpointManifest {
    pub snapshot_id: i64,
    pub data_files: Vec<String>,
    pub meta_files: Vec<String>,
}

impl PaimonCheckpointManifest {
    /// The manifest of a table this operator instance does not carry (id `-1`, no files) — a
    /// retention-off operator's deadlines slot in a multi-table snapshot token.
    pub(crate) fn absent() -> Self {
        PaimonCheckpointManifest { snapshot_id: -1, data_files: Vec::new(), meta_files: Vec::new() }
    }
}

/// One working-set entry. `dirty: true` slots are the write buffer — every entry written since
/// the last barrier, pinned until its checkpoint commit. `dirty: false` slots are the current
/// bundle's reads (fetched from the committed table or probed absent) and drop at `end_bundle`.
enum Slot<V> {
    Present { state: V, dirty: bool },
    Absent { dirty: bool },
}

/// The value-agnostic core every Paimon-backed store shares: table lifecycle, snapshot pinning,
/// hydration scans, commits, rescale bucket adoption, and the checkpoint file protocol
/// (listing, hard-links, local GC). The stores compose it with their own working sets and codecs.
/// Wall-clock accounting for the read-through and checkpoint paths, dumped at store close when
/// `SF_STATE_PROFILE` is set — a CPU sampler cannot see time spent *waiting* on these round
/// trips, which is exactly what a latency-bound pipeline spends.
#[derive(Default)]
struct CoreStats {
    probe_calls: u64,
    probe_ns: u64,
    probe_rows: u64,
    range_calls: u64,
    range_ns: u64,
    commits: u64,
    commit_ns: u64,
    listings: u64,
    listing_ns: u64,
}

pub(crate) struct PaimonTableCore {
    stats: CoreStats,
    table: Table,
    /// The table pinned at the last committed snapshot; probes read this.
    read_table: Option<Table>,
    read_snapshot: Option<i64>,
    /// The pinned snapshot's scan splits, planned once (a manifest walk) and reused by every
    /// per-batch key probe until the next commit re-pins the table — the snapshot is immutable,
    /// so the split list cannot go stale within a checkpoint interval.
    read_splits: Option<Vec<DataSplit>>,
    fields: Vec<DataField>,
    config: PaimonStoreConfig,
    /// Relative paths reachable from the last committed snapshot — the previous set minus the
    /// current one is exactly what local GC may unlink after a commit.
    live_files: StdHashSet<String>,
    /// Live data-file paths (`bucket-N/name`, sidecars included), maintained incrementally: a
    /// full scan plan reads the entire manifest chain — measured at hundreds of milliseconds per
    /// checkpoint listing on churn-heavy state, twice per barrier — while each snapshot's *delta*
    /// manifest is one small document. Seeded once (open, or first commit), then advanced by
    /// walking only the snapshots committed since.
    live_data: StdHashSet<String>,
    /// Snapshot id `live_data` reflects; None until seeded.
    listed_snapshot: Option<i64>,
    /// The index manifest `live_index` reflects (deletion-vector files, `index/name` paths). The
    /// index manifest is a full-state document, so a name change triggers one re-read.
    listed_index_manifest: Option<String>,
    live_index: StdHashSet<String>,
}

/// Read-through Paimon-backed store, generic over the operator's value codec (see the module
/// docs).
pub(crate) struct PaimonStore<C: PaimonStateCodec> {
    core: PaimonTableCore,
    codec: C,
    /// The value columns as Arrow fields, in persisted order after `kg`/`k`: the codec's fields
    /// plus (with TTL on) the store-managed trailing `ts` column, so the schema, hydration
    /// decode, and dirty-batch encode all follow from this one list.
    value_fields: Vec<Field>,
    /// The host's wall clock (its `ProcessingTimeService`), set before every ingest call; only
    /// read when TTL is on.
    now_ms: i64,
    working: ahash::HashMap<ByteKey, Slot<C::Value>>,
    footprint: isize,
}

impl<C: PaimonStateCodec> KeyedStateStore<C::Value> for PaimonStore<C> {
    #[inline]
    fn contains(&self, key: &[u8]) -> bool {
        matches!(self.working.get(key), Some(Slot::Present { .. }))
    }

    #[inline]
    fn get(&self, key: &[u8]) -> Option<&C::Value> {
        match self.working.get(key) {
            Some(Slot::Present { state, .. }) => Some(state),
            _ => None,
        }
    }

    #[inline]
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut C::Value> {
        match self.working.get_mut(key) {
            Some(Slot::Present { state, dirty }) => {
                *dirty = true;
                Some(state)
            }
            _ => None,
        }
    }

    #[inline]
    fn insert(&mut self, key: ByteKey, value: C::Value) -> &mut C::Value {
        let slot = self
            .working
            .entry(key)
            .insert_entry(Slot::Present { state: value, dirty: true })
            .into_mut();
        match slot {
            Slot::Present { state, .. } => state,
            Slot::Absent { .. } => unreachable!("just inserted a present slot"),
        }
    }

    #[inline]
    fn remove(&mut self, key: &[u8]) {
        if let Some(slot) = self.working.get_mut(key) {
            *slot = Slot::Absent { dirty: true };
        }
    }

    fn begin_batch(
        &mut self,
        batch: &RecordBatch,
        key_columns: &[usize],
        key_timestamp_precisions: &[i32],
    ) -> Result<(), DataFusionError> {
        let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, key_timestamp_precisions);
        let mut misses: Vec<ByteKey> = Vec::new();
        let mut seen: StdHashSet<ByteKey> = StdHashSet::new();
        for row in 0..batch.num_rows() {
            let key = encoder.encode(row);
            if !self.working.contains_key(key) && !seen.contains(key) {
                let owned = ByteKey::from(key);
                seen.insert(owned.clone());
                misses.push(owned);
            }
        }
        if !misses.is_empty() {
            self.fetch_missing(misses)?;
        }
        Ok(())
    }

    fn end_bundle(&mut self) -> Result<(), DataFusionError> {
        // Only the write buffer survives the bundle: clean slots are this bundle's join output
        // and drop here — a later bundle that touches the same keys re-reads them from the
        // page-cached table instead of a second in-memory copy of the state.
        let footprint = &mut self.footprint;
        let codec = &self.codec;
        self.working.retain(|key, slot| match slot {
            Slot::Present { state, dirty: false } => {
                *footprint -= (byte_key_bytes(&key.0)
                    + codec.value_bytes(state)
                    + Self::SLOT_OVERHEAD) as isize;
                false
            }
            Slot::Absent { dirty: false } => {
                *footprint -= Self::SLOT_OVERHEAD as isize;
                false
            }
            _ => true,
        });
        Ok(())
    }

    fn footprint_delta(&mut self) -> isize {
        std::mem::take(&mut self.footprint)
    }
}

impl Drop for PaimonTableCore {
    fn drop(&mut self) {
        if std::env::var_os("SF_STATE_PROFILE").is_none() {
            return;
        }
        let tail: Vec<&str> = self.config.table_dir.rsplit('/').take(2).collect();
        eprintln!(
            "SFPROF store={} probes={} probe_ms={} probe_rows={} ranges={} range_ms={} commits={} commit_ms={} listings={} listing_ms={}",
            tail.iter().rev().cloned().collect::<Vec<_>>().join("/"),
            self.stats.probe_calls,
            self.stats.probe_ns / 1_000_000,
            self.stats.probe_rows,
            self.stats.range_calls,
            self.stats.range_ns / 1_000_000,
            self.stats.commits,
            self.stats.commit_ns / 1_000_000,
            self.stats.listings,
            self.stats.listing_ns / 1_000_000,
        );
    }
}

impl PaimonTableCore {
    /// Creates a fresh table under `config.table_dir` (schema document + directory skeleton).
    fn create(config: PaimonStoreConfig, schema: PaimonSchema) -> Result<Self, DataFusionError> {
        let table_schema = TableSchema::new(0, &schema);
        let file_io = Self::file_io(&config.table_dir)?;
        runtime().block_on(async {
            file_io
                .mkdirs(&format!("{}/schema", config.table_dir))
                .await
                .map_err(pe)?;
            file_io
                .mkdirs(&format!("{}/snapshot", config.table_dir))
                .await
                .map_err(pe)?;
            let path = format!("{}/schema/schema-{}", config.table_dir, table_schema.id());
            let json = serde_json::to_vec(&table_schema)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            file_io
                .new_output(&path)
                .map_err(pe)?
                .write(bytes::Bytes::from(json))
                .await
                .map_err(pe)
        })?;
        Self::open_at(config, file_io, table_schema, None)
    }

    /// Opens a table directory previously materialized from a checkpoint, pinned at its snapshot.
    fn open(config: PaimonStoreConfig, snapshot_id: i64) -> Result<Self, DataFusionError> {
        let file_io = Self::file_io(&config.table_dir)?;
        let table_schema = Self::latest_schema(&file_io, &config.table_dir)?;
        Self::check_deletion_vectors(&table_schema, &config)?;
        Self::open_at(config, file_io, table_schema, Some(snapshot_id))
    }

    fn schema_deletion_vectors(schema: &TableSchema) -> bool {
        schema.options().get("deletion-vectors.enabled").map(String::as_str) == Some("true")
    }

    /// A restored table must agree with this deployment on deletion vectors; either mismatch is
    /// fatal. A deletion-vector table is only correct through raw reads that apply the vectors
    /// (a merge read resurrects masked rows), which the test-only non-vector configuration never
    /// does. The reverse — a table without the option where the deployment expects it — is a
    /// pre-deletion-vector state table: none exist (deletion vectors predate every production
    /// deployment), so refuse rather than guess at a migration.
    fn check_deletion_vectors(
        source_schema: &TableSchema,
        config: &PaimonStoreConfig,
    ) -> Result<(), DataFusionError> {
        match (Self::schema_deletion_vectors(source_schema), config.deletion_vectors) {
            (true, false) => Err(DataFusionError::Plan(
                "restored state table uses deletion vectors; deploy the streamfusion-paimon-compactor module"
                    .into(),
            )),
            (false, true) => Err(DataFusionError::Plan(
                "restored state table predates deletion vectors, which is unsupported; no such tables exist"
                    .into(),
            )),
            _ => Ok(()),
        }
    }

    fn open_at(
        config: PaimonStoreConfig,
        file_io: FileIO,
        table_schema: TableSchema,
        snapshot_id: Option<i64>,
    ) -> Result<Self, DataFusionError> {
        let fields = table_schema.fields().to_vec();
        let table = Table::new(
            file_io,
            Identifier::new("streamfusion", "state"),
            config.table_dir.clone(),
            table_schema,
            None,
        );
        let mut core = PaimonTableCore {
            read_table: None,
            read_snapshot: None,
            read_splits: None,
            fields,
            table,
            config,
            live_files: StdHashSet::new(),
            live_data: StdHashSet::new(),
            listed_snapshot: None,
            listed_index_manifest: None,
            live_index: StdHashSet::new(),
            stats: CoreStats::default(),
        };
        if let Some(id) = snapshot_id {
            core.read_snapshot = Some(id);
            core.read_table = Some(Self::pin(&core.table, id));
            core.live_files = core.reachable_files(id)?.into_iter().collect();
        }
        Ok(core)
    }

    /// Opens a restored source table pinned at its checkpoint snapshot.
    fn open_source(source_dir: &str, snapshot_id: i64) -> Result<Table, DataFusionError> {
        let file_io = Self::file_io(source_dir)?;
        let schema = Self::latest_schema(&file_io, source_dir)?;
        let source = Table::new(
            file_io,
            Identifier::new("streamfusion", "state"),
            source_dir.to_string(),
            schema,
            None,
        );
        Ok(Self::pin(&source, snapshot_id))
    }

    /// The aligned-restore fast path: the single source covers exactly this subtask's key-group
    /// range, so every bucket is adopted wholesale — data files hard-linked, committed by
    /// existing metadata, no row read or rewritten. Returns `false` without adopting when the
    /// source was written with a different bucket count: its rows sit in buckets this table's
    /// `kg mod buckets` would never look in, so the restore must clip-rewrite instead.
    fn adopt_all(&mut self, source_dir: &str, snapshot_id: i64) -> Result<bool, DataFusionError> {
        let source_file_io = Self::file_io(source_dir)?;
        let source_schema = Self::latest_schema(&source_file_io, source_dir)?;
        let source_buckets = source_schema.options().get("bucket").cloned();
        if source_buckets.as_deref() != Some(&self.config.buckets.to_string()) {
            return Ok(false);
        }
        // Adoption re-commits the source's files under this table's schema, so the field lists
        // must agree exactly (names and types). A mismatch — e.g. a pre-TTL table restored into
        // a TTL'd store, or the reverse — falls back to the clip rewrite, which maps columns by
        // name and synthesizes or drops the ts column.
        let same_fields = source_schema.fields().len() == self.fields.len()
            && source_schema
                .fields()
                .iter()
                .zip(&self.fields)
                .all(|(s, t)| s.name() == t.name() && s.data_type() == t.data_type());
        if !same_fields {
            return Ok(false);
        }
        Self::check_deletion_vectors(&source_schema, &self.config)?;
        let pinned = Self::open_source(source_dir, snapshot_id)?;
        let index_files = Self::live_index_files(&pinned, source_dir, snapshot_id)?;
        let builder = pinned.new_read_builder();
        let plan = runtime()
            .block_on(builder.new_scan().plan())
            .map_err(pe)?;
        let mut messages: Vec<CommitMessage> = Vec::new();
        for split in plan.splits() {
            let bucket = split.bucket();
            let bucket_dir = format!("{}/bucket-{}", self.config.table_dir, bucket);
            std::fs::create_dir_all(&bucket_dir).map_err(io)?;
            for file in split.data_files() {
                let from = format!("{}/bucket-{}/{}", source_dir, bucket, file.file_name);
                let to = format!("{}/{}", bucket_dir, file.file_name);
                if !std::path::Path::new(&to).exists() {
                    std::fs::hard_link(&from, &to).map_err(io)?;
                }
            }
            let mut message = CommitMessage::new(
                EMPTY_SERIALIZED_ROW.to_vec(),
                bucket,
                split.data_files().to_vec(),
            );
            // The bucket's deletion-vector index files travel with its data files: linked
            // beside them and re-registered through the commit, so the new table's index
            // manifest masks exactly what the source's did.
            if let Some(metas) = index_files.get(&bucket) {
                let index_dir = format!("{}/index", self.config.table_dir);
                std::fs::create_dir_all(&index_dir).map_err(io)?;
                for meta in metas {
                    let from = format!("{}/index/{}", source_dir, meta.file_name);
                    let to = format!("{}/{}", index_dir, meta.file_name);
                    if !std::path::Path::new(&to).exists() {
                        std::fs::hard_link(&from, &to).map_err(io)?;
                    }
                }
                message.new_index_files = metas.clone();
            }
            messages.push(message);
        }
        if !messages.is_empty() {
            let builder = self.table.new_write_builder();
            runtime()
                .block_on(builder.new_commit().commit(messages))
                .map_err(pe)?;
            self.refresh_after_commit()?;
        }
        Ok(true)
    }

    /// The live deletion-vector index files of a pinned snapshot, grouped by bucket.
    fn live_index_files(
        pinned: &Table,
        table_dir: &str,
        snapshot_id: i64,
    ) -> Result<ahash::HashMap<i32, Vec<paimon::spec::IndexFileMeta>>, DataFusionError> {
        let mut by_bucket: ahash::HashMap<i32, Vec<paimon::spec::IndexFileMeta>> =
            ahash::HashMap::default();
        let manager = pinned.snapshot_manager();
        let file_io = pinned.file_io().clone();
        let entries = runtime()
            .block_on(async {
                let snapshot = manager.get_snapshot(snapshot_id).await?;
                let Some(name) = snapshot.index_manifest() else {
                    return Ok(Vec::new());
                };
                paimon::spec::IndexManifest::read(
                    &file_io,
                    &format!("{table_dir}/manifest/{name}"),
                )
                .await
            })
            .map_err(pe)?;
        for entry in entries {
            if entry.kind == paimon::spec::FileKind::Add
                && entry.index_file.index_type == "DELETION_VECTORS"
            {
                by_bucket.entry(entry.bucket).or_default().push(entry.index_file);
            }
        }
        Ok(by_bucket)
    }

    /// The rescale path — RocksDB's restore-time clip, in Paimon terms: buckets are not
    /// partitioned by key group, so a resized subtask reads each source with a key-group range
    /// predicate (`kg` leads the primary key, so row-group pruning keeps the read proportional)
    /// and rewrites the surviving rows into its fresh table in one commit. Sources hold disjoint
    /// key-group ranges, so every rewritten primary key is unique and write order is irrelevant.
    ///
    /// Columns are matched by NAME, not position, which is what makes the clip double as the
    /// state-TTL schema migration: a target `ts` column missing from the source (pre-TTL → TTL)
    /// is synthesized as the restore time — a full retention from now, Flink's enable-TTL
    /// migration — and a source-only `ts` (TTL → no-TTL) is dropped. Any other mismatch is an
    /// error. With TTL on, rows already expired at restore time are not rewritten at all.
    fn clip_from_sources(
        &mut self,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        write_fields: &[Field],
        ttl: crate::state::StateTtl,
    ) -> Result<(), DataFusionError> {
        let mut clipped: Vec<RecordBatch> = Vec::new();
        for (source_dir, snapshot_id) in sources {
            // A deletion-vector source is only readable in a deployment that can also maintain
            // the rewritten table (the clip lands at level 0, which deletion-vector reads skip
            // until compaction) — fail closed rather than restore silently empty state.
            let source_file_io = Self::file_io(source_dir)?;
            let source_schema = Self::latest_schema(&source_file_io, source_dir)?;
            Self::check_deletion_vectors(&source_schema, &self.config)?;
            let pinned = Self::open_source(source_dir, *snapshot_id)?;
            let fields = pinned.schema().fields().to_vec();
            let source_index =
                |name: &str| fields.iter().position(|field| field.name() == name);
            for field in &fields {
                let name = field.name();
                if name != TS_COLUMN && !write_fields.iter().any(|f| f.name() == name) {
                    return Err(DataFusionError::Plan(format!(
                        "restored state column {name} has no counterpart in the target table"
                    )));
                }
            }
            for field in write_fields {
                if field.name() != TS_COLUMN && source_index(field.name()).is_none() {
                    return Err(DataFusionError::Plan(format!(
                        "target state column {} is missing from the restored table",
                        field.name()
                    )));
                }
            }
            let ts_field = Field::new(TS_COLUMN, DataType::Int64, true);
            let source_ts = source_index(TS_COLUMN);
            let builder_pred = PredicateBuilder::new(&fields);
            let predicate = Predicate::and(vec![
                builder_pred
                    .greater_or_equal(KG_COLUMN, Datum::Int(*key_groups.start()))
                    .map_err(pe)?,
                builder_pred
                    .less_or_equal(KG_COLUMN, Datum::Int(*key_groups.end()))
                    .map_err(pe)?,
            ]);
            let mut builder = pinned.new_read_builder();
            builder.with_filter(predicate);
            let batches = runtime()
                .block_on(async {
                    let plan = builder.new_scan().plan().await?;
                    let read = builder.new_read()?;
                    let mut stream = read.to_arrow(&plan.splits().to_vec())?;
                    let mut batches = Vec::new();
                    use futures::StreamExt;
                    while let Some(batch) = stream.next().await {
                        batches.push(batch?);
                    }
                    Ok::<_, paimon::Error>(batches)
                })
                .map_err(pe)?;
            for batch in batches {
                // The predicate pushdown is best-effort: re-check the range per row, and
                // normalize reader column types to the write schema.
                let kg_index = source_index(KG_COLUMN).expect("source kg column");
                let kgs = normalized_column(&batch, kg_index, &write_fields[0])?;
                let kgs = kgs
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| DataFusionError::Internal("paimon kg column".into()))?;
                let source_ts_values = source_ts
                    .filter(|_| ttl.enabled())
                    .map(|i| normalized_column(&batch, i, &ts_field))
                    .transpose()?;
                let expired = |row: usize| {
                    source_ts_values.as_ref().is_some_and(|ts| {
                        let ts = ts.as_any().downcast_ref::<Int64Array>().expect("ts column");
                        !ts.is_null(row) && ttl.expired(ts.value(row))
                    })
                };
                let keep: Vec<u32> = (0..batch.num_rows() as u32)
                    .filter(|&row| {
                        key_groups.contains(&kgs.value(row as usize)) && !expired(row as usize)
                    })
                    .collect();
                if keep.is_empty() {
                    continue;
                }
                let indices = arrow::array::UInt32Array::from(keep);
                let mut columns: Vec<ArrayRef> = Vec::with_capacity(write_fields.len() + 1);
                for field in write_fields.iter() {
                    let column = match source_index(field.name()) {
                        Some(i) => arrow::compute::take(
                            &normalized_column(&batch, i, field)?,
                            &indices,
                            None,
                        )
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                        // The target-only ts column of a pre-TTL → TTL migration: every restored
                        // row is stamped with the restore time, a full retention from now.
                        None => Arc::new(Int64Array::from(vec![ttl.now(); indices.len()])),
                    };
                    columns.push(column);
                }
                columns.push(Arc::new(Int8Array::from(vec![0i8; indices.len()])));
                let mut fields: Vec<Field> = write_fields.to_vec();
                fields.push(Field::new(VALUE_KIND_COLUMN, DataType::Int8, false));
                clipped.push(
                    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                        .expect("paimon clip batch"),
                );
            }
        }
        if !clipped.is_empty() {
            self.commit_batches(&clipped)?;
        }
        Ok(())
    }

    fn file_io(dir: &str) -> Result<FileIO, DataFusionError> {
        FileIO::from_path(dir)
            .map_err(pe)?
            .with_operator(crate::state::state_fs::state_fs_operator()?)
            .build()
            .map_err(pe)
    }

    fn latest_schema(file_io: &FileIO, dir: &str) -> Result<TableSchema, DataFusionError> {
        runtime().block_on(async {
            let manager = paimon::table::SchemaManager::new(file_io.clone(), dir.to_string());
            let schema = manager
                .latest()
                .await
                .map_err(pe)?
                .ok_or_else(|| DataFusionError::Plan(format!("no paimon schema under {dir}")))?;
            Ok(Arc::unwrap_or_clone(schema))
        })
    }

    fn pin(table: &Table, snapshot_id: i64) -> Table {
        table.copy_with_options(
            [("scan.snapshot-id".to_string(), snapshot_id.to_string())].into(),
        )
    }

    /// The shared leading schema columns of every store: the key-group bucket column and the
    /// BinaryRow key, plus the per-store columns appended by the caller.
    fn schema_builder(
        config: &PaimonStoreConfig,
    ) -> Result<paimon::spec::SchemaBuilder, DataFusionError> {
        let mut builder = PaimonSchema::builder()
            .column(KG_COLUMN, PaimonType::Int(IntType::new()))
            .column(
                KEY_COLUMN,
                PaimonType::VarBinary(
                    VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).map_err(pe)?,
                ),
            )
            .option("bucket", &config.buckets.to_string())
            .option("bucket-key", KG_COLUMN)
            .option("bucket-function.type", "mod")
            .option("file.format", &config.file_format)
            .option("file.compression", &config.file_compression)
            .option("merge-engine", "deduplicate");
        if config.deletion_vectors {
            // The Java compactor maintains the vectors via lookup compaction at every barrier
            // (see `PaimonStoreConfig::deletion_vectors`); the option makes level-1+ files
            // standalone-correct, so reads skip the merge reader entirely.
            builder = builder.option("deletion-vectors.enabled", "true");
        }
        Ok(builder)
    }

    fn key_group(&self, key: &[u8]) -> i32 {
        flink_key_group(hash_bytes_by_words(key), self.config.max_parallelism) as i32
    }

    /// The pinned snapshot's splits, planned lazily once per pin (see `read_splits`).
    fn pinned_splits(&mut self) -> Result<&[DataSplit], DataFusionError> {
        if self.read_splits.is_none() {
            let read_table = self.read_table.as_ref().expect("pinned read table");
            let builder = read_table.new_read_builder();
            let plan = runtime().block_on(builder.new_scan().plan()).map_err(pe)?;
            self.read_splits = Some(plan.splits().to_vec());
        }
        Ok(self.read_splits.as_deref().expect("planned splits"))
    }

    /// Reads the committed rows for exactly the given missing keys — the disk side of the
    /// per-batch join between an input batch's keys and (write buffer ∪ table). The key set is
    /// pushed into the reader as an `IN` predicate and enforced exactly at parquet decode (stats
    /// prune files and pages; a single hash-set pass filters rows), so returned batches hold only
    /// requested keys and only their value columns decode. A `kg IN` predicate rides along
    /// because the key-group column leads the primary key: files are kg-clustered, so it is the
    /// stats-prunable form of the same key set. In deletion-vector mode every file is
    /// standalone-correct, so the whole probe is one raw scan with the vectors applied as row
    /// masks. Empty when no snapshot is pinned yet.
    fn scan_keys(&mut self, misses: &[ByteKey]) -> Result<Vec<RecordBatch>, DataFusionError> {
        if self.read_table.is_none() || misses.is_empty() {
            return Ok(Vec::new());
        }
        let profile_start = std::time::Instant::now();
        let buckets = self.config.buckets as i32;
        let mut key_groups: Vec<i32> = misses.iter().map(|key| self.key_group(&key.0)).collect();
        key_groups.sort_unstable();
        key_groups.dedup();
        let wanted: StdHashSet<i32> = key_groups.iter().map(|kg| kg % buckets).collect();
        let builder_pred = PredicateBuilder::new(&self.fields);
        let predicate = Predicate::and(vec![
            builder_pred
                .is_in(
                    KG_COLUMN,
                    key_groups.iter().map(|kg| Datum::Int(*kg)).collect(),
                )
                .map_err(pe)?,
            builder_pred
                .is_in(
                    KEY_COLUMN,
                    misses.iter().map(|key| Datum::Bytes(key.0.to_vec())).collect(),
                )
                .map_err(pe)?,
        ]);
        let splits: Vec<DataSplit> = self
            .pinned_splits()?
            .iter()
            .filter(|split| wanted.contains(&split.bucket()))
            .cloned()
            .collect();
        let batches = self.read_splits_with_filter(&splits, predicate);
        self.stats.probe_calls += 1;
        self.stats.probe_ns += profile_start.elapsed().as_nanos() as u64;
        if let Ok(batches) = &batches {
            self.stats.probe_rows += batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
        }
        batches
    }

    /// Reads the committed rows matching an arbitrary predicate across all buckets — the disk
    /// side of a range read (watermark firing). Same split reuse as `scan_keys`; callers re-check
    /// rows where correctness demands it, since predicate pushdown is exact only for supported
    /// shapes. Empty when no snapshot is pinned yet.
    fn scan_predicate(&mut self, predicate: Predicate) -> Result<Vec<RecordBatch>, DataFusionError> {
        if self.read_table.is_none() {
            return Ok(Vec::new());
        }
        let profile_start = std::time::Instant::now();
        let splits = self.pinned_splits()?.to_vec();
        let batches = self.read_splits_with_filter(&splits, predicate);
        self.stats.range_calls += 1;
        self.stats.range_ns += profile_start.elapsed().as_nanos() as u64;
        batches
    }

    fn read_splits_with_filter(
        &self,
        splits: &[DataSplit],
        predicate: Predicate,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let read_table = self.read_table.as_ref().expect("pinned read table");
        let mut builder = read_table.new_read_builder();
        builder.with_filter(predicate);
        runtime()
            .block_on(async {
                let read = builder.new_read()?;
                let mut stream = read.to_arrow(splits)?;
                let mut batches = Vec::new();
                use futures::StreamExt;
                while let Some(batch) = stream.next().await {
                    batches.push(batch?);
                }
                Ok::<_, paimon::Error>(batches)
            })
            .map_err(pe)
    }

    /// Commits one write batch as a new snapshot and re-pins reads on it.
    fn commit(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        self.commit_batches(std::slice::from_ref(batch))
    }

    /// Commits a sequence of write batches, in order, as ONE new snapshot and re-pins reads.
    fn commit_batches(&mut self, batches: &[RecordBatch]) -> Result<(), DataFusionError> {
        let profile_start = std::time::Instant::now();
        let builder = self.table.new_write_builder();
        runtime()
            .block_on(async {
                let mut write = builder.new_write()?;
                for batch in batches {
                    write.write_arrow_batch(batch).await?;
                }
                let messages = write.prepare_commit().await?;
                builder.new_commit().commit(messages).await
            })
            .map_err(pe)?;
        let committed = self.refresh_after_commit();
        self.stats.commits += 1;
        self.stats.commit_ns += profile_start.elapsed().as_nanos() as u64;
        committed
    }

    /// The checkpoint file phase, after the dirty commit: garbage-collect local files no longer
    /// reachable and return the manifest for upload. Hard-linking the files an upload will read
    /// happens host-side, which knows which files are new against the last confirmed checkpoint —
    /// linking every reachable file here re-linked the whole table each barrier.
    fn checkpoint_manifest(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        let Some(snapshot_id) = self.read_snapshot else {
            return Ok(PaimonCheckpointManifest {
                snapshot_id: -1,
                data_files: Vec::new(),
                meta_files: Vec::new(),
            });
        };
        let profile_start = std::time::Instant::now();
        let (data_files, meta_files) = self.snapshot_file_listing(snapshot_id)?;
        self.gc_local(&data_files, &meta_files)?;
        self.stats.listings += 1;
        self.stats.listing_ns += profile_start.elapsed().as_nanos() as u64;
        Ok(PaimonCheckpointManifest { snapshot_id, data_files, meta_files })
    }

    fn refresh_after_commit(&mut self) -> Result<(), DataFusionError> {
        self.refresh_to_latest()?;
        if self.read_snapshot.is_none() {
            return Err(DataFusionError::Internal("commit produced no snapshot".into()));
        }
        Ok(())
    }

    /// Re-pins reads at the table's latest committed snapshot, if it moved.
    fn refresh_to_latest(&mut self) -> Result<(), DataFusionError> {
        let latest = runtime()
            .block_on(self.table.snapshot_manager().get_latest_snapshot_id())
            .map_err(pe)?;
        if let Some(latest) = latest {
            if self.read_snapshot != Some(latest) {
                self.read_snapshot = Some(latest);
                self.read_table = Some(Self::pin(&self.table, latest));
                self.read_splits = None;
            }
        }
        Ok(())
    }

    /// Advances the incrementally maintained live-file view to `to`: one small *delta* manifest
    /// walk per snapshot committed since the last listing, instead of re-planning the full
    /// manifest chain — the plan reads every manifest file ever written and was measured at
    /// hundreds of milliseconds per checkpoint listing on churn-heavy state. The first call
    /// seeds from a full plan once.
    fn advance_live_files(&mut self, to: i64) -> Result<(), DataFusionError> {
        match self.listed_snapshot {
            Some(listed) if listed <= to => {
                let manager = self.table.snapshot_manager();
                let file_io = self.table.file_io().clone();
                for id in (listed + 1)..=to {
                    let entries = runtime()
                        .block_on(async {
                            let snapshot = manager.get_snapshot(id).await?;
                            let delta = snapshot.delta_manifest_list().to_string();
                            let mut entries = Vec::new();
                            if !delta.is_empty() {
                                for meta in paimon::spec::ManifestList::read(
                                    &file_io,
                                    &manager.manifest_path(&delta),
                                )
                                .await?
                                {
                                    entries.extend(
                                        paimon::spec::Manifest::read(
                                            &file_io,
                                            &manager.manifest_path(meta.file_name()),
                                        )
                                        .await?,
                                    );
                                }
                            }
                            Ok::<_, paimon::Error>(entries)
                        })
                        .map_err(pe)?;
                    for entry in entries {
                        let bucket = entry.bucket();
                        let file = entry.file();
                        let mut paths = vec![format!("bucket-{}/{}", bucket, file.file_name)];
                        // Index sidecars written by the Java compactor live beside their data
                        // file and must ride uploads and local GC with it.
                        for extra in &file.extra_files {
                            paths.push(format!("bucket-{}/{}", bucket, extra));
                        }
                        match entry.kind() {
                            paimon::spec::FileKind::Add => {
                                for path in paths {
                                    self.live_data.insert(path);
                                }
                            }
                            paimon::spec::FileKind::Delete => {
                                for path in &paths {
                                    self.live_data.remove(path);
                                }
                            }
                        }
                    }
                }
            }
            _ => {
                self.live_data = self.reachable_data_files(to)?.into_iter().collect();
            }
        }
        self.listed_snapshot = Some(to);

        // The index manifest is a full-state document (the compactor's deletion-vector files,
        // immutable payload the raw read path depends on): re-read it only when its name
        // changes.
        let manager = self.table.snapshot_manager();
        let file_io = self.table.file_io().clone();
        let table_dir = self.config.table_dir.clone();
        let index_manifest = runtime()
            .block_on(async {
                Ok::<_, paimon::Error>(
                    manager.get_snapshot(to).await?.index_manifest().map(str::to_string),
                )
            })
            .map_err(pe)?;
        if index_manifest != self.listed_index_manifest {
            self.live_index.clear();
            if let Some(index) = &index_manifest {
                let entries = runtime()
                    .block_on(paimon::spec::IndexManifest::read(
                        &file_io,
                        &format!("{table_dir}/manifest/{index}"),
                    ))
                    .map_err(pe)?;
                for entry in entries {
                    if entry.kind == paimon::spec::FileKind::Add {
                        self.live_index
                            .insert(format!("index/{}", entry.index_file.file_name));
                    }
                }
            }
            self.listed_index_manifest = index_manifest;
        }
        Ok(())
    }

    /// The relative paths of everything the given snapshot needs: live data files and
    /// deletion-vector index files (shared upload candidates) and the snapshot/manifest/schema
    /// documents (private).
    fn snapshot_file_listing(
        &mut self,
        snapshot_id: i64,
    ) -> Result<(Vec<String>, Vec<String>), DataFusionError> {
        self.advance_live_files(snapshot_id)?;
        let mut data_files: Vec<String> =
            self.live_data.iter().chain(self.live_index.iter()).cloned().collect();
        data_files.sort();
        let mut meta_files = vec![format!("snapshot/snapshot-{snapshot_id}")];
        let manager = self.table.snapshot_manager();
        let file_io = self.table.file_io().clone();
        let manifest_lists = runtime()
            .block_on(async {
                let snapshot = manager.get_snapshot(snapshot_id).await?;
                let lists = vec![
                    snapshot.base_manifest_list().to_string(),
                    snapshot.delta_manifest_list().to_string(),
                ];
                let mut documents = lists.clone();
                for list in &lists {
                    if list.is_empty() {
                        continue;
                    }
                    for meta in
                        paimon::spec::ManifestList::read(&file_io, &manager.manifest_path(list))
                            .await?
                    {
                        documents.push(meta.file_name().to_string());
                    }
                }
                if let Some(index) = snapshot.index_manifest() {
                    documents.push(index.to_string());
                }
                Ok::<_, paimon::Error>(documents)
            })
            .map_err(pe)?;
        for name in manifest_lists {
            if !name.is_empty() {
                meta_files.push(format!("manifest/{name}"));
            }
        }
        for entry in std::fs::read_dir(format!("{}/schema", self.config.table_dir)).map_err(io)? {
            let entry = entry.map_err(io)?;
            meta_files.push(format!("schema/{}", entry.file_name().to_string_lossy()));
        }
        Ok((data_files, meta_files))
    }

    fn reachable_data_files(&self, snapshot_id: i64) -> Result<Vec<String>, DataFusionError> {
        let pinned = Self::pin(&self.table, snapshot_id);
        let builder = pinned.new_read_builder();
        // scan_all_files: this is a listing, not a read — a deletion-vector table's read scan
        // skips level-0 files, but an uncompacted run is still state the checkpoint must carry
        // and local GC must eventually reclaim.
        let plan = runtime()
            .block_on(builder.new_scan().with_scan_all_files().plan())
            .map_err(pe)?;
        let mut files = Vec::new();
        for split in plan.splits() {
            for file in split.data_files() {
                files.push(format!("bucket-{}/{}", split.bucket(), file.file_name));
                // Index sidecars written by the Java compactor live beside their data file and
                // must ride uploads and local GC with it.
                for extra in &file.extra_files {
                    files.push(format!("bucket-{}/{}", split.bucket(), extra));
                }
            }
        }
        Ok(files)
    }

    fn reachable_files(&mut self, snapshot_id: i64) -> Result<Vec<String>, DataFusionError> {
        let (mut data, meta) = self.snapshot_file_listing(snapshot_id)?;
        data.extend(meta);
        Ok(data)
    }

    /// Unlinks local files that the previous snapshot needed and the current one no longer does
    /// (files superseded by compaction, expired snapshot/manifest documents). Uploads for older,
    /// still-pending checkpoints read from their own hard-link dirs, so this is safe immediately.
    fn gc_local(&mut self, data_files: &[String], meta_files: &[String]) -> Result<(), DataFusionError> {
        let next: StdHashSet<String> = data_files.iter().chain(meta_files).cloned().collect();
        for stale in self.live_files.difference(&next) {
            let path = format!("{}/{}", self.config.table_dir, stale);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io(e)),
            }
        }
        self.live_files = next;
        Ok(())
    }
}

impl<C: PaimonStateCodec> PaimonStore<C> {
    const SLOT_OVERHEAD: usize = std::mem::size_of::<Slot<C::Value>>() + GROUP_ENTRY_OVERHEAD;

    /// Creates a fresh table under `config.table_dir` (schema document + directory skeleton).
    pub(crate) fn create(config: PaimonStoreConfig, codec: C) -> Result<Self, DataFusionError> {
        let schema = Self::paimon_schema(&config, &codec)?;
        Self::assemble(PaimonTableCore::create(config, schema)?, codec)
    }

    /// Opens a table directory previously materialized from a checkpoint, pinned at its snapshot.
    pub(crate) fn open(
        config: PaimonStoreConfig,
        codec: C,
        snapshot_id: i64,
    ) -> Result<Self, DataFusionError> {
        Self::assemble(PaimonTableCore::open(config, snapshot_id)?, codec)
    }

    /// Builds a fresh table at `config.table_dir` from one or more restored table directories
    /// (rescale); see `PaimonTableCore::adopt_buckets`. `now_ms` is the host's wall clock at
    /// restore, the stamp of the enable-TTL migration (see `clip_from_sources`).
    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        codec: C,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
        now_ms: i64,
    ) -> Result<Self, DataFusionError> {
        let mut store = Self::create(config, codec)?;
        store.now_ms = now_ms;
        if aligned && sources.len() == 1 {
            let (source_dir, snapshot_id) = &sources[0];
            if store.core.adopt_all(source_dir, *snapshot_id)? {
                return Ok(store);
            }
        }
        let write_fields = store.arrow_fields();
        store.core.clip_from_sources(sources, key_groups, &write_fields, store.ttl())?;
        Ok(store)
    }

    fn assemble(core: PaimonTableCore, codec: C) -> Result<Self, DataFusionError> {
        if !codec.supported() {
            return Err(DataFusionError::Plan(
                "state shape not supported by the paimon state backend".into(),
            ));
        }
        let mut value_fields: Vec<Field> = codec
            .value_fields()
            .into_iter()
            .map(|(name, data_type)| Field::new(name, data_type, true))
            .collect();
        if core.config.ttl_ms > 0 {
            value_fields.push(Field::new(TS_COLUMN, DataType::Int64, true));
        }
        Ok(PaimonStore {
            core,
            codec,
            value_fields,
            now_ms: 0,
            working: ahash::HashMap::default(),
            footprint: 0,
        })
    }

    fn paimon_schema(
        config: &PaimonStoreConfig,
        codec: &C,
    ) -> Result<PaimonSchema, DataFusionError> {
        let mut builder = PaimonTableCore::schema_builder(config)?;
        for (name, data_type) in codec.value_fields() {
            let paimon_type = paimon_type_of(&data_type).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "state type {data_type} not supported by the paimon state backend"
                ))
            })?;
            builder = builder.column(name, paimon_type);
        }
        if config.ttl_ms > 0 {
            builder = builder.column(TS_COLUMN, PaimonType::BigInt(BigIntType::new()));
        }
        builder.primary_key([KG_COLUMN, KEY_COLUMN]).build().map_err(pe)
    }

    /// Sets the host's wall clock for this ingest call (Flink's `TtlTimeProvider` reading);
    /// hydration reads it to expire committed rows, the clip to stamp migrated ones.
    pub(crate) fn set_clock(&mut self, now_ms: i64) {
        self.now_ms = now_ms;
    }

    fn ttl(&self) -> crate::state::StateTtl {
        crate::state::StateTtl::new(self.core.config.ttl_ms, self.now_ms)
    }

    /// The Arrow schema of persisted rows (also the write-batch schema, which additionally
    /// carries `_VALUE_KIND`).
    fn arrow_fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KG_COLUMN, DataType::Int32, false),
            Field::new(KEY_COLUMN, DataType::Binary, false),
        ];
        fields.extend(self.value_fields.iter().cloned());
        fields
    }

    /// Reads the missed keys from the committed table and records every missed key's result —
    /// present or absent — in the working set for the current bundle.
    fn fetch_missing(&mut self, misses: Vec<ByteKey>) -> Result<(), DataFusionError> {
        for batch in self.core.scan_keys(&misses)? {
            self.absorb_scan_batch(&batch)?;
        }
        let mut added_bytes = 0usize;
        for key in misses {
            self.working.entry(key).or_insert_with(|| {
                // Slot overhead only: if the operator creates this key, its own tracking charges
                // the key and state bytes (see `end_bundle` for the split).
                added_bytes += Self::SLOT_OVERHEAD;
                Slot::Absent { dirty: false }
            });
        }
        self.footprint += added_bytes as isize;
        Ok(())
    }

    /// Decodes scanned rows into clean working-set entries; a key already in the working set
    /// stays authoritative over the table. With TTL on this is where the persistent backend
    /// expires: a committed row past its retention hydrates as a dirty ABSENT slot — read as
    /// never seen, tombstoned by the next barrier's commit (delete-on-read) — instead of
    /// decoding; live rows decode and carry their persisted last-write timestamp.
    fn absorb_scan_batch(&mut self, batch: &RecordBatch) -> Result<usize, DataFusionError> {
        let expected = self.arrow_fields();
        let key_index = 1;
        let keys = normalized_column(batch, key_index, &expected[key_index])?;
        let keys = keys
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| DataFusionError::Internal("paimon key column".into()))?;
        let mut value_columns: Vec<ArrayRef> = Vec::with_capacity(self.value_fields.len());
        for i in 0..self.value_fields.len() {
            value_columns.push(normalized_column(batch, 2 + i, &expected[2 + i])?);
        }
        let ttl = self.ttl();
        let ts_column = if ttl.enabled() {
            let ts = value_columns.pop().expect("ttl store carries the ts column");
            Some(
                ts.as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DataFusionError::Internal("paimon ts column".into()))?
                    .clone(),
            )
        } else {
            None
        };
        let mut added = 0usize;
        let mut added_bytes = 0usize;
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            if self.working.contains_key(key) {
                continue;
            }
            // A NULL ts is defensive (no live row is written without one): it decodes as a fresh
            // write rather than expiring the row.
            let ts_ms = ts_column
                .as_ref()
                .map(|ts| if ts.is_null(row) { self.now_ms } else { ts.value(row) });
            let owned = ByteKey::from(key);
            if ts_ms.is_some_and(|ts| ttl.expired(ts)) {
                added_bytes += Self::SLOT_OVERHEAD;
                self.working.insert(owned, Slot::Absent { dirty: true });
                continue;
            }
            let mut scalars: Vec<ScalarValue> = Vec::with_capacity(value_columns.len());
            for column in &value_columns {
                scalars.push(
                    ScalarValue::try_from_array(column, row)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                );
            }
            let mut state = self.codec.decode(&scalars);
            if let Some(ts) = ts_ms {
                self.codec.stamp_write_ms(&mut state, ts);
            }
            added_bytes +=
                byte_key_bytes(&owned.0) + self.codec.value_bytes(&state) + Self::SLOT_OVERHEAD;
            self.working
                .insert(owned, Slot::Present { state, dirty: false });
            added += 1;
        }
        self.footprint += added_bytes as isize;
        Ok(added)
    }

    /// Builds the write batch for all dirty slots: upserts carry the encoded state row, deletions
    /// a `_VALUE_KIND = 3` tombstone. Returns `None` when nothing changed since the last commit.
    fn dirty_batch(&self) -> Option<RecordBatch> {
        let num_value = self.value_fields.len();
        let mut kgs: Vec<i32> = Vec::new();
        let mut keys: Vec<&[u8]> = Vec::new();
        let mut values: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_value];
        let mut kinds: Vec<i8> = Vec::new();
        let ttl_on = self.core.config.ttl_ms > 0;
        for (key, slot) in self.working.iter() {
            match slot {
                Slot::Present { state, dirty: true } => {
                    kgs.push(self.core.key_group(&key.0));
                    keys.push(&key.0);
                    for (i, scalar) in self.codec.encode(state).into_iter().enumerate() {
                        values[i].push(scalar);
                    }
                    if ttl_on {
                        values[num_value - 1]
                            .push(ScalarValue::Int64(Some(self.codec.write_ms(state))));
                    }
                    kinds.push(0); // +I upsert — deduplicate keeps the latest by sequence
                }
                Slot::Absent { dirty: true } => {
                    kgs.push(self.core.key_group(&key.0));
                    keys.push(&key.0);
                    for (i, field) in self.value_fields.iter().enumerate() {
                        values[i].push(null_scalar(field.data_type()));
                    }
                    kinds.push(3); // -D tombstone
                }
                _ => {}
            }
        }
        if keys.is_empty() {
            return None;
        }
        let mut fields = self.arrow_fields();
        fields.push(Field::new(VALUE_KIND_COLUMN, DataType::Int8, false));
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(kgs)),
            Arc::new(BinaryArray::from_iter_values(keys)),
        ];
        for (i, field) in self.value_fields.iter().enumerate() {
            columns.push(scalars_to_array(std::mem::take(&mut values[i]), field.data_type()));
        }
        columns.push(Arc::new(Int8Array::from(kinds)));
        Some(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("paimon dirty write batch"),
        )
    }

    /// Checkpoint sync phase, called at the barrier: commit the dirty write buffer as the
    /// latest local snapshot and run the checkpoint file phase (see
    /// `PaimonTableCore::checkpoint_manifest`). The same operation may run between barriers for
    /// memory pressure; ignoring the returned manifest leaves checkpoint publication unchanged.
    pub(crate) fn checkpoint(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        // An external compactor (the Java Paimon glue) may have committed a maintenance snapshot
        // just before this call: adopt the latest snapshot so the flush lands on top of it, the
        // manifest lists it, and local GC sees its file set.
        self.core.refresh_to_latest()?;
        if let Some(batch) = self.dirty_batch() {
            self.core.commit(&batch)?;
        }
        // All dirty slots are durable now; drop them (pure read-through, no cache across bundles).
        let footprint = &mut self.footprint;
        let codec = &self.codec;
        self.working.retain(|key, slot| {
            match slot {
                Slot::Present { state, .. } => {
                    *footprint -= (byte_key_bytes(&key.0)
                        + codec.value_bytes(state)
                        + Self::SLOT_OVERHEAD) as isize;
                }
                Slot::Absent { .. } => *footprint -= Self::SLOT_OVERHEAD as isize,
            }
            false
        });
        self.core.checkpoint_manifest()
    }
}
