use super::{
    checkpoint_files, copy_checkpoint_db, open_shared_db, re, FlinkWriteBatch, OpenedDb,
    PAIR_FIRST_TABLE, PAIR_SECOND_TABLE,
};
use crate::*;
use arrow::row::{RowConverter, SortField};
use rocksdb::{Cache, Direction, IteratorMode, Options, DB};
use std::sync::Arc;

/// Three tables share the temporal join's DB, every key led by the equi-join key's group so
/// rescale clipping stays layout-agnostic and each table is key-major (one key's entries are
/// contiguous, ready for prefix scans). Probe rows — buffered until a watermark passes their time
/// — key as `[key_group i32 BE][0][key arrow-row][seq u64 BE]`, valued `[time i64 LE][kind
/// i8][row arrow-row]`, so a firing splits fired from pending rows per key in arrival order
/// without decoding payloads. Build versions key as `[key_group i32 BE][1][key arrow-row]
/// [version_ts i64 BE, sign-flipped]`, valued `[kind u8][row arrow-row]`: the sign-flipped
/// timestamp makes byte order the version order (the memory path's `BTreeMap`), and a blind put
/// is Flink's last-write-wins per timestamp. Cleanup deadlines — the blob format's third section
/// — key as `[key_group i32 BE][2][key arrow-row]`, valued `[deadline i64 LE]`, and stay resident
/// (the hysteresis re-arm reads one per touched row) with every mutation written through.
const LEFT_TABLE: u8 = PAIR_FIRST_TABLE;
const RIGHT_TABLE: u8 = PAIR_SECOND_TABLE;
const DEADLINE_TABLE: u8 = 2;
const KEY_PREFIX_LEN: usize = 5;
const SUFFIX_LEN: usize = 8;
const TS_SIGN_FLIP: u64 = 1 << 63;
const LEFT_VALUE_PREFIX_LEN: usize = 9;
const RIGHT_VALUE_PREFIX_LEN: usize = 1;

/// The probe-row sequence high-water mark, persisted at checkpoint under a reserved key whose
/// leading bytes can never be a subtask's key group (the snapshot-timer key's convention).
const SEQ_KEY: &[u8] = b"\xff\xff\xff\xffstreamfusion-temporal-seq-left";

/// One buffered probe row read back from the store, in the value's layout plus its sequence.
pub(crate) struct StoredLeftRow {
    pub(crate) seq: u64,
    pub(crate) time: i64,
    pub(crate) kind: i8,
    pub(crate) row: Box<[u8]>,
}

/// One build-side version read back from the store; scans yield a key's versions in ascending
/// timestamp order.
pub(crate) struct StoredRightVersion {
    pub(crate) ts: i64,
    pub(crate) kind: i8,
    pub(crate) row: Box<[u8]>,
}

/// Bespoke persistent state for the temporal join. Rows append (probe side) or upsert (build
/// side) on arrival — the state IS RocksDB, with no resident working set beyond the deadline map
/// — and a watermark firing walks both tables key-major, exactly the per-key drain the memory
/// path runs over its maps. The store key is `[key_group i32 BE][key arrow-row]` (the table byte
/// stripped), so map order equals scan order.
pub(crate) struct RocksTemporalJoinStore {
    db: Arc<DB>,
    _cache: Option<Cache>,
    max_parallelism: usize,
    key_types: Vec<DataType>,
    key_converter: RowConverter,
    converters: (RowConverter, RowConverter),
    deadlines: HashMap<ByteKey, i64>,
    next_seq: u64,
    generation: i64,
    write_batch_size: usize,
}

impl RocksTemporalJoinStore {
    pub(crate) fn create(
        config: RocksStoreConfig,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        left_keys: &[usize],
    ) -> Result<Self, DataFusionError> {
        let row_types = |schema: &SchemaRef| -> Vec<DataType> {
            schema
                .fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect()
        };
        let key_types: Vec<DataType> = left_keys
            .iter()
            .map(|&column| left_schema.field(column).data_type().clone())
            .collect();
        if !rocks_row_supported(&row_types(&left_schema))
            || !rocks_row_supported(&row_types(&right_schema))
        {
            return Err(DataFusionError::Plan(
                "temporal-join row shape not supported by RocksDB".into(),
            ));
        }
        let opened = open_shared_db(
            &config,
            &[
                (Some(LEFT_TABLE), 0),
                (Some(RIGHT_TABLE), 0),
                (Some(DEADLINE_TABLE), 0),
            ],
        )?;
        Self::attach(opened, &config, left_schema, right_schema, key_types)
    }

    /// [`RocksTemporalJoinStore::create`] over restored checkpoint directories: an aligned single
    /// source adopts the files wholesale; anything else clips rows by this subtask's key groups
    /// and takes the sequence high-water mark as the max across sources (a key group lives in
    /// exactly one source, so each key's arrival order survives). Both paths rehydrate the
    /// resident deadline map from the deadlines table.
    pub(crate) fn open_merged(
        config: RocksStoreConfig,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        left_keys: &[usize],
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        if aligned && sources.len() == 1 {
            copy_checkpoint_db(&sources[0].0, &config.table_dir)?;
            let mut store = Self::create(config, left_schema, right_schema, left_keys)?;
            store.generation = sources[0].1;
            store.next_seq = store
                .db
                .get(SEQ_KEY)
                .map_err(re)?
                .filter(|bytes| bytes.len() == 8)
                .map(|bytes| u64::from_be_bytes(bytes[..8].try_into().unwrap()))
                .unwrap_or(0);
            store.hydrate_deadlines()?;
            return Ok(store);
        }
        let mut store = Self::create(config, left_schema, right_schema, left_keys)?;
        let mut writes = FlinkWriteBatch::new(&store.db, store.write_batch_size);
        for (source, _) in sources {
            let source_db =
                DB::open_for_read_only(&Options::default(), source, false).map_err(re)?;
            for row in source_db.iterator(IteratorMode::Start) {
                let (key, value) = row.map_err(re)?;
                if key.as_ref() == SEQ_KEY {
                    if value.len() == 8 {
                        store.next_seq = store
                            .next_seq
                            .max(u64::from_be_bytes(value[..8].try_into().unwrap()));
                    }
                } else if key.len() >= KEY_PREFIX_LEN {
                    let kg = i32::from_be_bytes(key[..4].try_into().expect("key group prefix"));
                    if key_groups.contains(&kg) {
                        writes.put(key, value)?;
                    }
                }
            }
        }
        writes.finish()?;
        store.hydrate_deadlines()?;
        Ok(store)
    }

    fn attach(
        opened: OpenedDb,
        config: &RocksStoreConfig,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        key_types: Vec<DataType>,
    ) -> Result<Self, DataFusionError> {
        let converter = |types: &[DataType]| {
            RowConverter::new(types.iter().map(|t| SortField::new(t.clone())).collect())
                .map_err(|e| DataFusionError::External(Box::new(e)))
        };
        let row_types = |schema: &SchemaRef| -> Vec<DataType> {
            schema
                .fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect()
        };
        Ok(Self {
            db: opened.db,
            _cache: opened.cache,
            max_parallelism: config.max_parallelism,
            key_converter: converter(&key_types)?,
            key_types,
            converters: (
                converter(&row_types(&left_schema))?,
                converter(&row_types(&right_schema))?,
            ),
            deadlines: HashMap::default(),
            next_seq: 0,
            generation: 0,
            write_batch_size: opened.write_batch_size,
        })
    }

    fn hydrate_deadlines(&mut self) -> Result<(), DataFusionError> {
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() > KEY_PREFIX_LEN && key[4] == DEADLINE_TABLE && value.len() == 8 {
                self.deadlines.insert(
                    Self::store_key(&key, key.len()),
                    i64::from_le_bytes(value[..8].try_into().unwrap()),
                );
            }
        }
        Ok(())
    }

    /// The store keys (`[key_group][key arrow-row]`) of a batch's rows: the key group from the
    /// equi key's BinaryRow hash — identical routing to the blob path's raw keyed-state
    /// partitioner — and the key bytes from the memcomparable (and decodable) arrow-row codec.
    pub(crate) fn entry_keys(
        &self,
        batch: &RecordBatch,
        key_columns: &[usize],
        key_timestamp_precisions: &[i32],
    ) -> Vec<ByteKey> {
        let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, key_timestamp_precisions);
        let key_arrays: Vec<ArrayRef> = key_columns
            .iter()
            .map(|&column| batch.column(column).clone())
            .collect();
        let key_rows = self
            .key_converter
            .convert_columns(&key_arrays)
            .expect("encode temporal-join keys");
        (0..batch.num_rows())
            .map(|row| {
                let key_group = flink_key_group(encoder.hash(row), self.max_parallelism) as i32;
                let key_row = key_rows.row(row);
                let key_row = key_row.data();
                let mut out = Vec::with_capacity(4 + key_row.len());
                out.extend_from_slice(&key_group.to_be_bytes());
                out.extend_from_slice(key_row);
                ByteKey(out.into())
            })
            .collect()
    }

    /// Appends probe-side rows under fresh sequences, one KV per row in arrival order, so RocksDB
    /// holds the buffer's only copy.
    pub(crate) fn push_left(
        &mut self,
        batch: &RecordBatch,
        entry_keys: &[ByteKey],
        times: &Int64Array,
        kinds: Option<&Int8Array>,
    ) -> Result<(), DataFusionError> {
        let arity = data_arity(batch);
        let rows = self
            .converters
            .0
            .convert_columns(&batch.columns()[..arity])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (index, row) in rows.iter().enumerate() {
            let seq = self.next_seq;
            self.next_seq += 1;
            let row = row.data();
            let mut value = Vec::with_capacity(LEFT_VALUE_PREFIX_LEN + row.len());
            value.extend_from_slice(&times.value(index).to_le_bytes());
            value.push(kinds.map_or(0, |k| k.value(index)) as u8);
            value.extend_from_slice(row);
            writes.put(
                Self::db_key(&entry_keys[index], LEFT_TABLE, &seq.to_be_bytes()),
                value,
            )?;
        }
        writes.finish()
    }

    /// Upserts build-side versions: one put per (key, version), which IS Flink's
    /// last-write-wins per timestamp.
    pub(crate) fn push_right(
        &mut self,
        batch: &RecordBatch,
        entry_keys: &[ByteKey],
        times: &Int64Array,
        kinds: Option<&Int8Array>,
    ) -> Result<(), DataFusionError> {
        let arity = data_arity(batch);
        let rows = self
            .converters
            .1
            .convert_columns(&batch.columns()[..arity])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (index, row) in rows.iter().enumerate() {
            let row = row.data();
            let mut value = Vec::with_capacity(RIGHT_VALUE_PREFIX_LEN + row.len());
            value.push(kinds.map_or(0, |k| k.value(index)) as u8);
            value.extend_from_slice(row);
            writes.put(
                Self::db_key(
                    &entry_keys[index],
                    RIGHT_TABLE,
                    &Self::flip_ts(times.value(index)),
                ),
                value,
            )?;
        }
        writes.finish()
    }

    /// Every key's buffered probe rows, per-key in arrival order, keys in store order.
    pub(crate) fn scan_left(
        &self,
    ) -> Result<BTreeMap<ByteKey, Vec<StoredLeftRow>>, DataFusionError> {
        let mut out: BTreeMap<ByteKey, Vec<StoredLeftRow>> = BTreeMap::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() < KEY_PREFIX_LEN + SUFFIX_LEN || key[4] != LEFT_TABLE {
                continue;
            }
            out.entry(Self::store_key(&key, key.len() - SUFFIX_LEN))
                .or_default()
                .push(StoredLeftRow {
                    seq: u64::from_be_bytes(key[key.len() - 8..].try_into().expect("sequence")),
                    time: i64::from_le_bytes(value[..8].try_into().expect("time")),
                    kind: value[8] as i8,
                    row: value[LEFT_VALUE_PREFIX_LEN..].into(),
                });
        }
        Ok(out)
    }

    /// Every key's build versions in ascending timestamp order, keys in store order.
    pub(crate) fn scan_right(
        &self,
    ) -> Result<BTreeMap<ByteKey, Vec<StoredRightVersion>>, DataFusionError> {
        let mut out: BTreeMap<ByteKey, Vec<StoredRightVersion>> = BTreeMap::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() < KEY_PREFIX_LEN + SUFFIX_LEN || key[4] != RIGHT_TABLE {
                continue;
            }
            out.entry(Self::store_key(&key, key.len() - SUFFIX_LEN))
                .or_default()
                .push(StoredRightVersion {
                    ts: Self::unflip_ts(&key[key.len() - 8..]),
                    kind: value[0] as i8,
                    row: value[RIGHT_VALUE_PREFIX_LEN..].into(),
                });
        }
        Ok(out)
    }

    /// Removes fired probe rows of one key by sequence.
    pub(crate) fn remove_left(
        &mut self,
        key: &ByteKey,
        seqs: &[u64],
    ) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for seq in seqs {
            writes.delete(Self::db_key(key, LEFT_TABLE, &seq.to_be_bytes()))?;
        }
        writes.finish()
    }

    /// Removes pruned build versions of one key by timestamp.
    pub(crate) fn remove_right(
        &mut self,
        key: &ByteKey,
        timestamps: &[i64],
    ) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for ts in timestamps {
            writes.delete(Self::db_key(key, RIGHT_TABLE, &Self::flip_ts(*ts)))?;
        }
        writes.finish()
    }

    /// Flink's fired cleanup timer: drops the key's ENTIRE state — both sides and the deadline —
    /// silently, by prefix scan (the tables are key-major).
    pub(crate) fn clear_key(&mut self, key: &ByteKey) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for table in [LEFT_TABLE, RIGHT_TABLE] {
            let prefix = Self::table_prefix(key, table);
            for row in self
                .db
                .iterator(IteratorMode::From(&prefix, Direction::Forward))
            {
                let (db_key, _) = row.map_err(re)?;
                if db_key.len() < prefix.len() || db_key[..prefix.len()] != prefix[..] {
                    break;
                }
                writes.delete(db_key)?;
            }
        }
        if self.deadlines.remove(key).is_some() {
            writes.delete(Self::table_prefix(key, DEADLINE_TABLE))?;
        }
        writes.finish()
    }

    pub(crate) fn deadline(&self, key: &ByteKey) -> Option<i64> {
        self.deadlines.get(key).copied()
    }

    pub(crate) fn set_deadline(
        &mut self,
        key: &ByteKey,
        deadline: i64,
    ) -> Result<(), DataFusionError> {
        self.deadlines.insert(key.clone(), deadline);
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        writes.put(
            Self::table_prefix(key, DEADLINE_TABLE),
            deadline.to_le_bytes(),
        )?;
        writes.finish()
    }

    pub(crate) fn remove_deadline(&mut self, key: &ByteKey) -> Result<(), DataFusionError> {
        if self.deadlines.remove(key).is_some() {
            let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
            writes.delete(Self::table_prefix(key, DEADLINE_TABLE))?;
            writes.finish()?;
        }
        Ok(())
    }

    pub(crate) fn all_deadlines(&self) -> &HashMap<ByteKey, i64> {
        &self.deadlines
    }

    /// The keys whose deadline has passed at `now_ms`, for the periodic sweep.
    pub(crate) fn due_keys(&self, now_ms: i64) -> Vec<ByteKey> {
        self.deadlines
            .iter()
            .filter(|(_, &deadline)| now_ms >= deadline)
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Enable-retention migration (the blob restore's stamping): every key holding state on
    /// either side without a restored deadline is stamped `deadline` so it expires a full max
    /// retention after the restore instead of on first touch.
    pub(crate) fn adopt_retention(&mut self, deadline: i64) -> Result<(), DataFusionError> {
        let mut missing: Vec<ByteKey> = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, _) = row.map_err(re)?;
            if key.len() < KEY_PREFIX_LEN + SUFFIX_LEN
                || (key[4] != LEFT_TABLE && key[4] != RIGHT_TABLE)
            {
                continue;
            }
            let store_key = Self::store_key(&key, key.len() - SUFFIX_LEN);
            if !self.deadlines.contains_key(&store_key) && missing.last() != Some(&store_key) {
                missing.push(store_key);
            }
        }
        missing.sort_unstable();
        missing.dedup();
        for key in missing {
            self.set_deadline(&key, deadline)?;
        }
        Ok(())
    }

    /// Rebuilds stored rows as a batch under one side's declared data schema, so reconstructed
    /// rows match what the memory path would have buffered.
    pub(crate) fn decode<'a>(
        &self,
        left: bool,
        schema: &SchemaRef,
        rows: impl Iterator<Item = &'a [u8]>,
    ) -> Result<RecordBatch, DataFusionError> {
        let converter = if left {
            &self.converters.0
        } else {
            &self.converters.1
        };
        let parser = converter.parser();
        let parsed: Vec<_> = rows.map(|bytes| parser.parse(bytes)).collect();
        let columns = converter
            .convert_rows(parsed)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(RecordBatch::try_new(schema.clone(), columns)?)
    }

    /// Decodes store keys back to their typed key columns, for the canonical deadline section.
    pub(crate) fn decode_key_columns(&self, keys: &[&ByteKey]) -> Vec<ArrayRef> {
        let key_rows: Vec<&[u8]> = keys.iter().map(|key| &key.0[4..]).collect();
        decode_byte_keys(Some(&self.key_converter), &key_rows, &self.key_types)
    }

    pub(crate) fn key_group(key: &ByteKey) -> i32 {
        i32::from_be_bytes(key.0[..4].try_into().expect("key group"))
    }

    /// Persists the sequence high-water mark, then takes one native checkpoint of the shared DB —
    /// rows and deadlines were already written through, so there is no working set to commit.
    pub(crate) fn checkpoint(
        &mut self,
        snapshot_dir: &str,
    ) -> Result<RocksCheckpointManifest, DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        writes.put(SEQ_KEY, self.next_seq.to_be_bytes())?;
        writes.finish()?;
        if snapshot_dir.is_empty() {
            return Ok(RocksCheckpointManifest::absent());
        }
        self.generation += 1;
        checkpoint_files(&self.db, snapshot_dir, self.generation)
    }

    fn store_key(db_key: &[u8], end: usize) -> ByteKey {
        let mut out = Vec::with_capacity(end - 1);
        out.extend_from_slice(&db_key[..4]);
        out.extend_from_slice(&db_key[KEY_PREFIX_LEN..end]);
        ByteKey(out.into())
    }

    fn table_prefix(key: &ByteKey, table: u8) -> Vec<u8> {
        let mut out = Vec::with_capacity(key.0.len() + 1);
        out.extend_from_slice(&key.0[..4]);
        out.push(table);
        out.extend_from_slice(&key.0[4..]);
        out
    }

    fn db_key(key: &ByteKey, table: u8, suffix: &[u8; 8]) -> Vec<u8> {
        let mut out = Self::table_prefix(key, table);
        out.extend_from_slice(suffix);
        out
    }

    fn flip_ts(ts: i64) -> [u8; 8] {
        ((ts as u64) ^ TS_SIGN_FLIP).to_be_bytes()
    }

    fn unflip_ts(bytes: &[u8]) -> i64 {
        (u64::from_be_bytes(bytes.try_into().expect("version timestamp")) ^ TS_SIGN_FLIP) as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::rocks_config::FlinkRocksOptions;

    fn options_json() -> String {
        serde_json::to_string(&FlinkRocksOptions {
            max_background_threads: 2,
            max_open_files: -1,
            log_max_file_size: 0,
            log_file_num: 1,
            log_directory: None,
            log_level: "INFO_LEVEL".into(),
            compaction_style: "LEVEL".into(),
            compression_per_level: vec!["NO_COMPRESSION".into()],
            use_dynamic_level_size: true,
            target_file_size_base: 4 << 20,
            max_size_level_base: 16 << 20,
            write_buffer_size: 4 << 20,
            max_write_buffer_number: 2,
            min_write_buffer_number_to_merge: 1,
            write_batch_size: 2 << 20,
            compaction_filter_query_time_after_num_entries: 1000,
            periodic_compaction_seconds: 0,
            block_size: 4096,
            metadata_block_size: 4096,
            block_cache_size: 8 << 20,
            use_bloom_filter: false,
            bloom_filter_bits_per_key: 10.0,
            bloom_filter_block_based_mode: false,
        })
        .unwrap()
    }

    fn test_config(name: &str) -> RocksStoreConfig {
        let dir = std::env::temp_dir().join(format!(
            "streamfusion-temporal-join-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        RocksStoreConfig {
            table_dir: dir.to_string_lossy().into_owned(),
            max_parallelism: 128,
            options_json: options_json(),
            ttl_ms: 0,
            shared_resources: 0,
        }
    }

    fn snapshot_dir(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "streamfusion-temporal-join-{name}-snapshot-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir.to_string_lossy().into_owned()
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("rt", DataType::Int64, false),
        ]))
    }

    fn batch(keys: &[i64], values: &[i64], times: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
                Arc::new(Int64Array::from(times.to_vec())),
            ],
        )
        .unwrap()
    }

    fn kinded_batch(keys: &[i64], values: &[i64], times: &[i64], kinds: &[i8]) -> RecordBatch {
        let mut fields: Vec<Field> = schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
                Arc::new(Int64Array::from(times.to_vec())),
                Arc::new(Int8Array::from(kinds.to_vec())),
            ],
        )
        .unwrap()
    }

    fn memory_joiner(join_type: JoinKind, retention_ms: i64) -> TemporalJoiner {
        TemporalJoiner::new(vec![0], vec![0], 2, 2, join_type, schema(), schema(), None)
            .with_state_retention(retention_ms)
            .with_key_timestamp_precisions(vec![-1])
    }

    fn store(name: &str) -> RocksTemporalJoinStore {
        RocksTemporalJoinStore::create(test_config(name), schema(), schema(), &[0]).unwrap()
    }

    fn store_backed_joiner(name: &str, join_type: JoinKind, retention_ms: i64) -> TemporalJoiner {
        memory_joiner(join_type, retention_ms).with_store(store(name))
    }

    /// Cross-key emission order is backend-dependent (the memory path iterates a hash map), so
    /// multi-key comparisons sort the emitted rows; single-key tests compare batches exactly.
    fn sorted_rows(batch: &RecordBatch) -> Vec<Vec<u8>> {
        if batch.num_rows() == 0 {
            return Vec::new();
        }
        let converter = RowConverter::new(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )
        .unwrap();
        let rows = converter.convert_columns(batch.columns()).unwrap();
        let mut out: Vec<Vec<u8>> = rows.iter().map(|row| row.data().to_vec()).collect();
        out.sort();
        out
    }

    // Single key: watermark firing resolves each probe row against the version valid at its time,
    // in arrival order, and pruning keeps the latest version at the watermark for later probes.
    #[test]
    fn store_backed_left_join_matches_the_memory_path() {
        let mut memory = memory_joiner(JoinKind::LeftOuter, 0);
        let mut rocks = store_backed_joiner("left", JoinKind::LeftOuter, 0);
        for joiner in [&mut memory, &mut rocks] {
            joiner
                .push_right(&batch(&[1, 1], &[50, 60], &[100, 200]), 0)
                .unwrap();
            joiner
                .push_left(&batch(&[1, 1, 1], &[10, 11, 12], &[150, 250, 50]), 0)
                .unwrap();
        }
        let expected = memory.advance(300, 0).unwrap();
        let actual = rocks.advance(300, 0).unwrap();
        assert_eq!(expected, actual);
        assert_eq!(expected.num_rows(), 3);
        // Versions older than the one valid at the watermark were pruned identically.
        for joiner in [&mut memory, &mut rocks] {
            joiner.push_left(&batch(&[1], &[13], &[310]), 0).unwrap();
        }
        assert_eq!(
            memory.advance(400, 0).unwrap(),
            rocks.advance(400, 0).unwrap()
        );
    }

    // A -D version marks "no row here": an INNER probe drops, and last-write-wins per timestamp
    // replaces a version upserted twice.
    #[test]
    fn deleted_and_replaced_versions_match_the_memory_path() {
        let mut memory = memory_joiner(JoinKind::Inner, 0);
        let mut rocks = store_backed_joiner("versions", JoinKind::Inner, 0);
        for joiner in [&mut memory, &mut rocks] {
            joiner
                .push_right(&kinded_batch(&[1, 1], &[50, 60], &[100, 200], &[0, 1]), 0)
                .unwrap();
            joiner
                .push_right(&kinded_batch(&[1], &[70], &[100], &[0]), 0)
                .unwrap();
            joiner
                .push_left(&batch(&[1, 1], &[10, 11], &[150, 250]), 0)
                .unwrap();
        }
        let expected = memory.advance(300, 0).unwrap();
        let actual = rocks.advance(300, 0).unwrap();
        assert_eq!(expected, actual);
        assert_eq!(expected.num_rows(), 1);
    }

    #[test]
    fn store_backed_multi_key_join_matches_the_memory_path() {
        let mut memory = memory_joiner(JoinKind::LeftOuter, 0);
        let mut rocks = store_backed_joiner("multi", JoinKind::LeftOuter, 0);
        for joiner in [&mut memory, &mut rocks] {
            joiner
                .push_right(&batch(&[1, 2, 3], &[51, 52, 53], &[100, 100, 100]), 0)
                .unwrap();
            joiner
                .push_left(&batch(&[1, 2, 4], &[10, 20, 40], &[150, 150, 150]), 0)
                .unwrap();
        }
        assert_eq!(
            sorted_rows(&memory.advance(200, 0).unwrap()),
            sorted_rows(&rocks.advance(200, 0).unwrap())
        );
    }

    // A canonical savepoint of the store-backed joiner is the blob format's framed sections, so
    // it restores into a memory joiner that continues identically.
    #[test]
    fn canonical_partitions_transition_back_to_the_memory_path() {
        let mut rocks = store_backed_joiner("canonical", JoinKind::LeftOuter, 0);
        rocks
            .push_right(&batch(&[1, 2], &[51, 52], &[100, 100]), 0)
            .unwrap();
        rocks
            .push_left(&batch(&[1, 3], &[10, 30], &[150, 150]), 0)
            .unwrap();
        let snapshots: Vec<Vec<u8>> = rocks
            .canonical_partitions()
            .unwrap()
            .into_values()
            .collect();
        let mut memory = TemporalJoiner::restore_partitions(
            vec![0],
            vec![0],
            2,
            2,
            JoinKind::LeftOuter,
            schema(),
            schema(),
            None,
            &snapshots,
            0,
            0,
        );
        assert_eq!(
            sorted_rows(&memory.advance(200, 0).unwrap()),
            sorted_rows(&rocks.advance(200, 0).unwrap())
        );
    }

    // Retention parity: a key whose fired deadline passed emits nothing, a re-armed key fires.
    #[test]
    fn retention_expiry_and_rearm_match_the_memory_path() {
        let mut memory = memory_joiner(JoinKind::LeftOuter, 1000);
        let mut rocks = store_backed_joiner("retention", JoinKind::LeftOuter, 1000);
        for joiner in [&mut memory, &mut rocks] {
            joiner
                .push_right(&batch(&[1, 2], &[51, 52], &[100, 100]), 0)
                .unwrap();
            joiner
                .push_left(&batch(&[1, 2], &[10, 20], &[150, 150]), 0)
                .unwrap();
            // Touch key 1 again inside the min-retention window of its deadline: the hysteresis
            // moves it to now + max, so it survives the clock that expires key 2.
            joiner.push_left(&batch(&[1], &[11], &[160]), 1000).unwrap();
        }
        let expected = memory.advance(200, 2000).unwrap();
        let actual = rocks.advance(200, 2000).unwrap();
        assert_eq!(sorted_rows(&expected), sorted_rows(&actual));
        assert_eq!(expected.num_rows(), 2);
    }

    // Retention rides canonical savepoints as the third framed section.
    #[test]
    fn canonical_partitions_carry_cleanup_deadlines() {
        let mut rocks = store_backed_joiner("canonical-ttl", JoinKind::LeftOuter, 1000);
        rocks.push_left(&batch(&[1], &[10], &[150]), 0).unwrap();
        let snapshots: Vec<Vec<u8>> = rocks
            .canonical_partitions()
            .unwrap()
            .into_values()
            .collect();
        let mut memory = TemporalJoiner::restore_partitions(
            vec![0],
            vec![0],
            2,
            2,
            JoinKind::LeftOuter,
            schema(),
            schema(),
            None,
            &snapshots,
            1000,
            0,
        );
        // The restored deadline (0 + 1500) has passed at now=2000, so nothing fires — matching
        // the store-backed joiner's own expiry.
        assert_eq!(
            memory.advance(200, 2000).unwrap().num_rows(),
            rocks.advance(200, 2000).unwrap().num_rows()
        );
    }

    #[test]
    fn restore_continues_state_and_sequences() {
        let snapshot = snapshot_dir("restore");
        let mut memory = memory_joiner(JoinKind::LeftOuter, 0);
        let mut before = store_backed_joiner("restore", JoinKind::LeftOuter, 0);
        for joiner in [&mut memory, &mut before] {
            joiner.push_right(&batch(&[1], &[51], &[100]), 0).unwrap();
            joiner
                .push_left(&batch(&[1, 1], &[10, 11], &[150, 400]), 0)
                .unwrap();
        }
        assert_eq!(
            memory.advance(200, 0).unwrap(),
            before.advance(200, 0).unwrap()
        );
        let manifest = before.store_mut().checkpoint(&snapshot).unwrap();
        drop(before);

        let restored_store = RocksTemporalJoinStore::open_merged(
            test_config("restore-reopen"),
            schema(),
            schema(),
            &[0],
            &[(snapshot, manifest.snapshot_id)],
            0..=127,
            true,
        )
        .unwrap();
        let mut restored = memory_joiner(JoinKind::LeftOuter, 0).with_store(restored_store);
        for joiner in [&mut memory, &mut restored] {
            joiner.push_left(&batch(&[1], &[12], &[450]), 0).unwrap();
        }
        assert_eq!(
            memory.advance(500, 0).unwrap(),
            restored.advance(500, 0).unwrap()
        );
    }

    #[test]
    fn unaligned_restore_clips_key_groups() {
        let keys = [1i64, 2, 3, 4];
        let snapshot = snapshot_dir("clip");
        let mut before = store_backed_joiner("clip", JoinKind::LeftOuter, 0);
        before
            .push_left(&batch(&keys, &[10, 20, 30, 40], &[100; 4]), 0)
            .unwrap();
        let manifest = before.store_mut().checkpoint(&snapshot).unwrap();
        drop(before);

        let target = flink_key_group(
            binary_row_hash(&batch(&keys[..1], &[10], &[100]), &[0], 0, &[-1]),
            128,
        ) as i32;
        let restored_store = RocksTemporalJoinStore::open_merged(
            test_config("clip-reopen"),
            schema(),
            schema(),
            &[0],
            &[(snapshot, manifest.snapshot_id)],
            target..=target,
            false,
        )
        .unwrap();
        let mut restored = memory_joiner(JoinKind::LeftOuter, 0).with_store(restored_store);
        let pads = restored.advance(200, 0).unwrap();
        let expected: usize = keys
            .iter()
            .filter(|&&key| {
                flink_key_group(
                    binary_row_hash(&batch(&[key], &[0], &[100]), &[0], 0, &[-1]),
                    128,
                ) as i32
                    == target
            })
            .count();
        assert_eq!(pads.num_rows(), expected);
    }
}
