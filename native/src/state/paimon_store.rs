//! Read-through persistent state on local Apache Paimon primary-key tables.
//!
//! State rows live in a Paimon PK table on local disk as typed Arrow columns; the operator holds
//! no resident map. Each processed batch hydrates a bounded working set with exactly the keys the
//! batch touches (one sorted probe through the table's committed snapshot), folds into it, and the
//! entries mutated since the last checkpoint stay pinned as the write buffer. At a checkpoint
//! barrier the dirty entries are appended as one Arrow batch (`_VALUE_KIND` carries upsert vs
//! delete per row) and committed — a Paimon snapshot is a manifest-pinned immutable file set, so
//! durability lands exactly at checkpoints and "new files since the last checkpoint" is a manifest
//! diff, which is what makes Flink incremental checkpoints possible upstream of this module.
//!
//! Bucket = Flink key group, exactly: the table carries a computed `kg` INT column
//! (`flink_key_group` of the BinaryRow key bytes) as the leading primary-key column and bucket
//! key, with Paimon's spec-compliant `bucket-function.type = mod` and `bucket = maxParallelism` —
//! floor-mod of an already-in-range int is the identity. Rescale therefore reassigns whole bucket
//! directories; no row is rewritten.
//!
//! This store never compacts. paimon-rust has no LSM compaction yet, and rather than carry a
//! second maintenance implementation, table maintenance belongs exclusively to the optional Java
//! Paimon compactor module, which runs stock Paimon's compaction against this table at each
//! barrier, directly beneath the data commit (the store adopts its snapshots by re-pinning at
//! checkpoint start). Without it, tables stay correct but accumulate one level-0 run per touched
//! bucket per checkpoint — the host warns when the backend runs unmaintained.

use crate::*;
use arrow::array::{Array, BinaryArray, Int32Array, Int8Array};
use paimon::catalog::Identifier;
use paimon::io::FileIO;
use paimon::spec::{
    BigIntType, BooleanType, DataField, DataFileMeta, DataType as PaimonType, Datum, DateType,
    DecimalType, DoubleType, FloatType, IntType, PredicateBuilder, Schema as PaimonSchema,
    SmallIntType, TableSchema, TimestampType, TinyIntType, VarBinaryType, VarCharType,
    EMPTY_SERIALIZED_ROW,
};
use paimon::table::{CommitMessage, Table};
use std::collections::{HashMap as StdHashMap, HashSet as StdHashSet};
use std::sync::OnceLock;

const KG_COLUMN: &str = "kg";
const KEY_COLUMN: &str = "k";
const VALUE_KIND_COLUMN: &str = "_VALUE_KIND";

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
    /// Flink maxParallelism — the bucket count; bucket id == key group id.
    pub max_parallelism: usize,
    /// Paimon `file.format` for state data files.
    pub file_format: String,
    /// Paimon `file.compression` for state data files ("uncompressed", "zstd", "snappy", ...).
    /// Stamped into the table schema, so an external compactor's rewrites honor it too.
    pub file_compression: String,
}

/// A checkpoint's file manifest, handed to the host for upload. `data_files` are immutable,
/// uniquely named, and shared across checkpoints (incremental dedup by name); `meta_files` are the
/// snapshot/manifest/schema documents pinned to this snapshot (re-uploaded each checkpoint —
/// small). All paths are relative to the table root and hard-linked under `link_dir`, so uploads
/// survive local compaction and GC.
#[derive(serde::Serialize)]
pub(crate) struct PaimonCheckpointManifest {
    pub snapshot_id: i64,
    pub data_files: Vec<String>,
    pub meta_files: Vec<String>,
}

enum Slot<V> {
    Present { state: V, dirty: bool },
    Absent { dirty: bool },
}

/// Read-through Paimon-backed store, generic over the operator's value codec (see the module
/// docs).
pub(crate) struct PaimonStore<C: PaimonStateCodec> {
    table: Table,
    /// The table pinned at the last committed snapshot; probes read this.
    read_table: Option<Table>,
    read_snapshot: Option<i64>,
    fields: Vec<DataField>,
    config: PaimonStoreConfig,
    codec: C,
    /// The codec's value columns as Arrow fields, in persisted order after `kg`/`k`.
    value_fields: Vec<Field>,
    working: ahash::HashMap<ByteKey, Slot<C::Value>>,
    /// Relative paths reachable from the last committed snapshot — the previous set minus the
    /// current one is exactly what local GC may unlink after a commit.
    live_files: StdHashSet<String>,
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
            self.hydrate(misses)?;
        }
        Ok(())
    }

    fn end_bundle(&mut self) -> Result<(), DataFusionError> {
        // Pure read-through: only the write buffer (dirty slots) survives the bundle. Clean
        // entries are re-probed on next touch; a read cache across bundles is a later,
        // benchmarked optimization.
        //
        // Accounting split: the operator's per-row tracking charges key + state bytes for entries
        // it creates, mutates, or removes; the store charges what the operator cannot see — slot
        // overhead for every hydrated entry, plus key + state for entries hydrated Present (which
        // the operator never charged). Dropping an entry reverses exactly that split.
        let footprint = &mut self.footprint;
        let codec = &self.codec;
        self.working.retain(|key, slot| match slot {
            Slot::Present { dirty: true, .. } | Slot::Absent { dirty: true } => true,
            Slot::Present { dirty: false, state } => {
                *footprint -= (byte_key_bytes(&key.0)
                    + codec.value_bytes(state)
                    + Self::SLOT_OVERHEAD) as isize;
                false
            }
            Slot::Absent { dirty: false } => {
                *footprint -= Self::SLOT_OVERHEAD as isize;
                false
            }
        });
        Ok(())
    }

    fn footprint_delta(&mut self) -> isize {
        std::mem::take(&mut self.footprint)
    }
}

impl<C: PaimonStateCodec> PaimonStore<C> {
    const SLOT_OVERHEAD: usize = std::mem::size_of::<Slot<C::Value>>() + GROUP_ENTRY_OVERHEAD;

    /// Creates a fresh table under `config.table_dir` (schema document + directory skeleton).
    pub(crate) fn create(config: PaimonStoreConfig, codec: C) -> Result<Self, DataFusionError> {
        let schema = Self::paimon_schema(&config, &codec)?;
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
        Self::open_at(config, codec, file_io, table_schema, None)
    }

    /// Opens a table directory previously materialized from a checkpoint, pinned at its snapshot.
    pub(crate) fn open(
        config: PaimonStoreConfig,
        codec: C,
        snapshot_id: i64,
    ) -> Result<Self, DataFusionError> {
        let file_io = Self::file_io(&config.table_dir)?;
        let table_schema = Self::latest_schema(&file_io, &config.table_dir)?;
        Self::open_at(config, codec, file_io, table_schema, Some(snapshot_id))
    }

    /// Builds a fresh table at `config.table_dir` from one or more restored table directories
    /// (rescale): every bucket in `key_groups` is adopted by hard-linking its data files and
    /// committing their existing metadata — no row is read or rewritten.
    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        codec: C,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
    ) -> Result<Self, DataFusionError> {
        let mut store = Self::create(config, codec)?;
        let mut messages: Vec<CommitMessage> = Vec::new();
        for (source_dir, snapshot_id) in sources {
            let file_io = Self::file_io(source_dir)?;
            let schema = Self::latest_schema(&file_io, source_dir)?;
            let source = Table::new(
                file_io,
                Identifier::new("streamfusion", "state"),
                source_dir.clone(),
                schema,
                None,
            );
            let pinned = Self::pin(&source, *snapshot_id);
            let builder = pinned.new_read_builder();
            let plan = runtime()
                .block_on(builder.new_scan().plan())
                .map_err(pe)?;
            for split in plan.splits() {
                let bucket = split.bucket();
                if !key_groups.contains(&bucket) {
                    continue;
                }
                let bucket_dir = format!("{}/bucket-{}", store.config.table_dir, bucket);
                std::fs::create_dir_all(&bucket_dir).map_err(io)?;
                for file in split.data_files() {
                    let from = format!("{}/bucket-{}/{}", source_dir, bucket, file.file_name);
                    let to = format!("{}/{}", bucket_dir, file.file_name);
                    if !std::path::Path::new(&to).exists() {
                        std::fs::hard_link(&from, &to).map_err(io)?;
                    }
                }
                messages.push(CommitMessage::new(
                    EMPTY_SERIALIZED_ROW.to_vec(),
                    bucket,
                    split.data_files().to_vec(),
                ));
            }
        }
        if !messages.is_empty() {
            let builder = store.table.new_write_builder();
            runtime()
                .block_on(builder.new_commit().commit(messages))
                .map_err(pe)?;
            store.refresh_after_commit()?;
        }
        Ok(store)
    }

    fn open_at(
        config: PaimonStoreConfig,
        codec: C,
        file_io: FileIO,
        table_schema: TableSchema,
        snapshot_id: Option<i64>,
    ) -> Result<Self, DataFusionError> {
        if !codec.supported() {
            return Err(DataFusionError::Plan(
                "state shape not supported by the paimon state backend".into(),
            ));
        }
        let value_fields: Vec<Field> = codec
            .value_fields()
            .into_iter()
            .map(|(name, data_type)| Field::new(name, data_type, true))
            .collect();
        let fields = table_schema.fields().to_vec();
        let table = Table::new(
            file_io,
            Identifier::new("streamfusion", "state"),
            config.table_dir.clone(),
            table_schema,
            None,
        );
        let mut store = PaimonStore {
            read_table: None,
            read_snapshot: None,
            fields,
            table,
            config,
            codec,
            value_fields,
            working: ahash::HashMap::default(),
            live_files: StdHashSet::new(),
            footprint: 0,
        };
        if let Some(id) = snapshot_id {
            store.read_snapshot = Some(id);
            store.read_table = Some(Self::pin(&store.table, id));
            store.live_files = store.reachable_files(id)?.into_iter().collect();
        }
        Ok(store)
    }

    fn file_io(dir: &str) -> Result<FileIO, DataFusionError> {
        FileIO::from_path(dir).map_err(pe)?.build().map_err(pe)
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

    fn paimon_schema(
        config: &PaimonStoreConfig,
        codec: &C,
    ) -> Result<PaimonSchema, DataFusionError> {
        let mut builder = PaimonSchema::builder()
            .column(KG_COLUMN, PaimonType::Int(IntType::new()))
            .column(
                KEY_COLUMN,
                PaimonType::VarBinary(
                    VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).map_err(pe)?,
                ),
            );
        for (name, data_type) in codec.value_fields() {
            let paimon_type = paimon_type_of(&data_type).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "state type {data_type} not supported by the paimon state backend"
                ))
            })?;
            builder = builder.column(name, paimon_type);
        }
        builder
            .primary_key([KG_COLUMN, KEY_COLUMN])
            .option("bucket", &config.max_parallelism.to_string())
            .option("bucket-key", KG_COLUMN)
            .option("bucket-function.type", "mod")
            .option("file.format", &config.file_format)
            .option("file.compression", &config.file_compression)
            .option("merge-engine", "deduplicate")
            .build()
            .map_err(pe)
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

    fn key_group(&self, key: &ByteKey) -> i32 {
        flink_key_group(hash_bytes_by_words(&key.0), self.config.max_parallelism) as i32
    }

    /// Probes the committed snapshot for the given missing keys and records every result —
    /// present or absent — in the working set.
    fn hydrate(&mut self, misses: Vec<ByteKey>) -> Result<(), DataFusionError> {
        let mut found = 0usize;
        if self.read_table.is_some() {
            let buckets: StdHashSet<i32> = misses.iter().map(|k| self.key_group(k)).collect();
            let key_set: StdHashSet<&[u8]> = misses.iter().map(|k| &*k.0).collect();
            let predicate = PredicateBuilder::new(&self.fields)
                .is_in(
                    KEY_COLUMN,
                    misses.iter().map(|k| Datum::Bytes(k.0.to_vec())).collect(),
                )
                .map_err(pe)?;
            let read_table = self.read_table.as_ref().expect("pinned read table");
            let mut builder = read_table.new_read_builder();
            builder.with_filter(predicate);
            let batches: Vec<RecordBatch> = runtime()
                .block_on(async {
                    let plan = builder.new_scan().plan().await?;
                    let splits: Vec<_> = plan
                        .splits()
                        .iter()
                        .filter(|split| buckets.contains(&split.bucket()))
                        .cloned()
                        .collect();
                    let read = builder.new_read()?;
                    let mut stream = read.to_arrow(&splits)?;
                    let mut batches = Vec::new();
                    use futures::StreamExt;
                    while let Some(batch) = stream.next().await {
                        batches.push(batch?);
                    }
                    Ok::<_, paimon::Error>(batches)
                })
                .map_err(pe)?;
            for batch in batches {
                found += self.absorb_probe_batch(&batch, &key_set)?;
            }
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
        let _ = found;
        Ok(())
    }

    /// Decodes probe rows into clean working-set entries, ignoring any row whose key was not
    /// probed (predicate pushdown is best-effort, not exact, on every format).
    fn absorb_probe_batch(
        &mut self,
        batch: &RecordBatch,
        probed: &StdHashSet<&[u8]>,
    ) -> Result<usize, DataFusionError> {
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
        let mut added = 0usize;
        let mut added_bytes = 0usize;
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            if !probed.contains(key) || self.working.contains_key(key) {
                continue;
            }
            let mut scalars: Vec<ScalarValue> = Vec::with_capacity(value_columns.len());
            for column in &value_columns {
                scalars.push(
                    ScalarValue::try_from_array(column, row)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                );
            }
            let state = self.codec.decode(&scalars);
            let owned = ByteKey::from(key);
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
        for (key, slot) in self.working.iter() {
            match slot {
                Slot::Present { state, dirty: true } => {
                    kgs.push(self.key_group(key));
                    keys.push(&key.0);
                    for (i, scalar) in self.codec.encode(state).into_iter().enumerate() {
                        values[i].push(scalar);
                    }
                    kinds.push(0); // +I upsert — deduplicate keeps the latest by sequence
                }
                Slot::Absent { dirty: true } => {
                    kgs.push(self.key_group(key));
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

    /// Checkpoint sync phase, called at the barrier: commit the dirty
    /// write buffer as the checkpoint's snapshot, hard-link the snapshot's reachable files under
    /// `link_dir` (so uploads survive later local GC and compaction), garbage-collect local files
    /// no longer reachable, and return the file manifest for the host to upload.
    pub(crate) fn checkpoint(
        &mut self,
        link_dir: &str,
    ) -> Result<PaimonCheckpointManifest, DataFusionError> {
        // An external compactor (the Java Paimon glue) may have committed a maintenance snapshot
        // just before this call: adopt the latest snapshot so the flush lands on top of it, the
        // manifest lists it, and local GC sees its file set.
        self.refresh_to_latest()?;
        if let Some(batch) = self.dirty_batch() {
            let builder = self.table.new_write_builder();
            runtime()
                .block_on(async {
                    let mut write = builder.new_write()?;
                    write.write_arrow_batch(&batch).await?;
                    let messages = write.prepare_commit().await?;
                    builder.new_commit().commit(messages).await
                })
                .map_err(pe)?;
            self.refresh_after_commit()?;
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
        let Some(snapshot_id) = self.read_snapshot else {
            return Ok(PaimonCheckpointManifest {
                snapshot_id: -1,
                data_files: Vec::new(),
                meta_files: Vec::new(),
            });
        };
        let (data_files, meta_files) = self.snapshot_file_listing(snapshot_id)?;
        for rel in data_files.iter().chain(meta_files.iter()) {
            let from = format!("{}/{}", self.config.table_dir, rel);
            let to = format!("{link_dir}/{rel}");
            if let Some(parent) = std::path::Path::new(&to).parent() {
                std::fs::create_dir_all(parent).map_err(io)?;
            }
            std::fs::hard_link(&from, &to).map_err(io)?;
        }
        self.gc_local(&data_files, &meta_files)?;
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
            }
        }
        Ok(())
    }

    /// The relative paths of everything the given snapshot needs: live data files (shared upload
    /// candidates) and the snapshot/manifest/schema documents (private).
    fn snapshot_file_listing(
        &self,
        snapshot_id: i64,
    ) -> Result<(Vec<String>, Vec<String>), DataFusionError> {
        let data_files = self.reachable_data_files(snapshot_id)?;
        let mut meta_files = vec![format!("snapshot/snapshot-{snapshot_id}")];
        let manager = self.table.snapshot_manager();
        let file_io = self.table.file_io().clone();
        let manifest_lists = runtime()
            .block_on(async {
                let snapshot = manager.get_snapshot(snapshot_id).await?;
                let mut lists = vec![
                    snapshot.base_manifest_list().to_string(),
                    snapshot.delta_manifest_list().to_string(),
                ];
                if let Some(index) = snapshot.index_manifest() {
                    lists.push(index.to_string());
                }
                let mut manifests = Vec::new();
                for list in &lists {
                    if list.is_empty() {
                        continue;
                    }
                    for meta in
                        paimon::spec::ManifestList::read(&file_io, &manager.manifest_path(list))
                            .await?
                    {
                        manifests.push(meta.file_name().to_string());
                    }
                }
                lists.extend(manifests);
                Ok::<_, paimon::Error>(lists)
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
        let plan = runtime().block_on(builder.new_scan().plan()).map_err(pe)?;
        let mut files = Vec::new();
        for split in plan.splits() {
            for file in split.data_files() {
                files.push(format!("bucket-{}/{}", split.bucket(), file.file_name));
            }
        }
        Ok(files)
    }

    fn reachable_files(&self, snapshot_id: i64) -> Result<Vec<String>, DataFusionError> {
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
