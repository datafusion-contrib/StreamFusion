use super::{
    checkpoint_files, copy_checkpoint_db, open_shared_db, re, FlinkWriteBatch, OpenedDb,
    PAIR_FIRST_TABLE, PAIR_SECOND_TABLE,
};
use crate::*;
use arrow::row::{RowConverter, SortField};
use rocksdb::{Cache, IteratorMode, Options, DB};
use std::sync::Arc;

/// Two tables share the keep-first deduplicator's DB, keyed `[key_group i32 BE][table u8][key
/// arrow-row]` — one entry per key on both. Pending — each key's minimum-rowtime candidate row —
/// values as `[rowtime i64 LE][seq u64 BE][row arrow-row]`: the rowtime lets a firing split
/// released from waiting candidates without decoding payloads, and the sequence (of the
/// candidate's winning arrival) reproduces the memory path's emission order, where incumbents
/// precede the rows that displaced other keys' candidates. Emitted — the fired markers — values
/// as the marker's firing timestamp alone (`[ttl_ts i64 LE]`), or zero bytes while TTL is off
/// (the raw snapshots' convention that a TTL-off format carries no timestamp); the value IS the
/// store's TTL prefix, so the shared compaction filter physically drops expired markers — sound
/// because the memory path's expired marker reads as absent and is deleted on read. Pending stays
/// TTL-exempt, mirroring Flink's deliberately un-TTL'd timer state.
const PENDING_TABLE: u8 = PAIR_FIRST_TABLE;
const EMITTED_TABLE: u8 = PAIR_SECOND_TABLE;
const KEY_PREFIX_LEN: usize = 5;
const PENDING_VALUE_PREFIX_LEN: usize = 16;

/// The candidate-arrival sequence high-water mark and the late-data watermark, persisted at
/// checkpoint under reserved keys whose leading bytes can never be a subtask's key group.
const SEQ_KEY: &[u8] = b"\xff\xff\xff\xffstreamfusion-keep-first-seq";
const WATERMARK_KEY: &[u8] = b"\xff\xff\xff\xffstreamfusion-keep-first-watermark";

/// One pending candidate read back from the store.
pub(crate) struct StoredCandidate {
    pub(crate) key: ByteKey,
    pub(crate) seq: u64,
    pub(crate) rowtime: i64,
    pub(crate) row: Box<[u8]>,
}

/// Persistent state for the keep-first deduplicator: candidates and fired markers are individual
/// KVs written through on arrival, a push probes exactly the keys its batch touches with one
/// multi_get per table, and a watermark firing range-reads the pending table.
pub(crate) struct RocksKeepFirstDedupStore {
    db: Arc<DB>,
    _cache: Option<Cache>,
    max_parallelism: usize,
    key_types: Vec<DataType>,
    key_converter: RowConverter,
    payload_converter: RowConverter,
    schema: SchemaRef,
    watermark: i64,
    next_seq: u64,
    generation: i64,
    write_batch_size: usize,
}

impl RocksKeepFirstDedupStore {
    pub(crate) fn create(
        config: RocksStoreConfig,
        schema: SchemaRef,
        partition_columns: &[usize],
    ) -> Result<Self, DataFusionError> {
        let row_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        if !rocks_row_supported(&row_types) {
            return Err(DataFusionError::Plan(
                "keep-first dedup row shape not supported by RocksDB".into(),
            ));
        }
        let key_types: Vec<DataType> = partition_columns
            .iter()
            .map(|&column| schema.field(column).data_type().clone())
            .collect();
        let opened = open_shared_db(
            &config,
            &[
                (Some(PENDING_TABLE), 0),
                (Some(EMITTED_TABLE), config.ttl_ms),
            ],
        )?;
        Self::attach(opened, &config, schema, key_types)
    }

    /// [`RocksKeepFirstDedupStore::create`] over restored checkpoint directories: an aligned
    /// single source adopts the files wholesale; anything else clips rows by this subtask's key
    /// groups. The restored watermark and sequence high-water mark are each the max across
    /// sources.
    pub(crate) fn open_merged(
        config: RocksStoreConfig,
        schema: SchemaRef,
        partition_columns: &[usize],
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        if aligned && sources.len() == 1 {
            copy_checkpoint_db(&sources[0].0, &config.table_dir)?;
            let mut store = Self::create(config, schema, partition_columns)?;
            store.generation = sources[0].1;
            let read_reserved =
                |store: &Self, key: &[u8]| -> Result<Option<i64>, DataFusionError> {
                    Ok(store
                        .db
                        .get(key)
                        .map_err(re)?
                        .filter(|bytes| bytes.len() == 8)
                        .map(|bytes| i64::from_be_bytes(bytes[..8].try_into().unwrap())))
                };
            store.next_seq = read_reserved(&store, SEQ_KEY)?.unwrap_or(0) as u64;
            store.watermark = read_reserved(&store, WATERMARK_KEY)?.unwrap_or(i64::MIN);
            return Ok(store);
        }
        let mut store = Self::create(config, schema, partition_columns)?;
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
                } else if key.as_ref() == WATERMARK_KEY {
                    if value.len() == 8 {
                        store.watermark = store
                            .watermark
                            .max(i64::from_be_bytes(value[..8].try_into().unwrap()));
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
        Ok(store)
    }

    fn attach(
        opened: OpenedDb,
        config: &RocksStoreConfig,
        schema: SchemaRef,
        key_types: Vec<DataType>,
    ) -> Result<Self, DataFusionError> {
        let converter = |types: &[DataType]| {
            RowConverter::new(types.iter().map(|t| SortField::new(t.clone())).collect())
                .map_err(|e| DataFusionError::External(Box::new(e)))
        };
        let row_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        Ok(Self {
            db: opened.db,
            _cache: opened.cache,
            max_parallelism: config.max_parallelism,
            key_converter: converter(&key_types)?,
            key_types,
            payload_converter: converter(&row_types)?,
            schema,
            watermark: i64::MIN,
            next_seq: 0,
            generation: 0,
            write_batch_size: opened.write_batch_size,
        })
    }

    /// The late-data watermark persisted by the previous run (the blob format's leading field).
    pub(crate) fn watermark(&self) -> i64 {
        self.watermark
    }

    pub(crate) fn set_watermark(&mut self, watermark: i64) {
        self.watermark = watermark;
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// The store keys (`[key_group][key arrow-row]`) of a batch's rows: the key group from the
    /// partition key's BinaryRow hash — identical routing to the blob path's raw keyed-state
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
            .expect("encode keep-first dedup keys");
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

    /// The committed fired markers of the given keys, one multi_get: `Some(stamp)` for a marker
    /// written under TTL, `None` for a TTL-off marker (no timestamp — never expires).
    pub(crate) fn markers(
        &self,
        keys: &[ByteKey],
    ) -> Result<HashMap<ByteKey, Option<i64>>, DataFusionError> {
        let db_keys: Vec<_> = keys
            .iter()
            .map(|key| Self::db_key(key, EMITTED_TABLE))
            .collect();
        let mut out = HashMap::default();
        for (key, value) in keys.iter().zip(self.db.multi_get(&db_keys)) {
            if let Some(bytes) = value.map_err(re)? {
                let stamp =
                    (bytes.len() == 8).then(|| i64::from_le_bytes(bytes[..8].try_into().unwrap()));
                out.insert(key.clone(), stamp);
            }
        }
        Ok(out)
    }

    /// The committed pending candidates of the given keys, one multi_get: `(rowtime, seq)`.
    pub(crate) fn candidates(
        &self,
        keys: &[ByteKey],
    ) -> Result<HashMap<ByteKey, (i64, u64)>, DataFusionError> {
        let db_keys: Vec<_> = keys
            .iter()
            .map(|key| Self::db_key(key, PENDING_TABLE))
            .collect();
        let mut out = HashMap::default();
        for (key, value) in keys.iter().zip(self.db.multi_get(&db_keys)) {
            if let Some(bytes) = value.map_err(re)? {
                out.insert(
                    key.clone(),
                    (
                        i64::from_le_bytes(bytes[..8].try_into().expect("rowtime")),
                        u64::from_be_bytes(bytes[8..16].try_into().expect("sequence")),
                    ),
                );
            }
        }
        Ok(out)
    }

    /// Deletes markers whose TTL lapsed — the memory path's delete-on-read.
    pub(crate) fn remove_markers(&mut self, keys: &[ByteKey]) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for key in keys {
            writes.delete(Self::db_key(key, EMITTED_TABLE))?;
        }
        writes.finish()
    }

    /// Writes a batch's winning candidates — fresh keys and strict rowtime improvements — each
    /// under a fresh sequence in row order, so a later firing reproduces the memory path's
    /// emission order.
    pub(crate) fn put_candidates(
        &mut self,
        batch: &RecordBatch,
        winners: &[(usize, ByteKey, i64)],
    ) -> Result<(), DataFusionError> {
        let rows = self
            .payload_converter
            .convert_columns(batch.columns())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (row, key, rowtime) in winners {
            let seq = self.next_seq;
            self.next_seq += 1;
            let row = rows.row(*row);
            let row = row.data();
            let mut value = Vec::with_capacity(PENDING_VALUE_PREFIX_LEN + row.len());
            value.extend_from_slice(&rowtime.to_le_bytes());
            value.extend_from_slice(&seq.to_be_bytes());
            value.extend_from_slice(row);
            writes.put(Self::db_key(key, PENDING_TABLE), value)?;
        }
        writes.finish()
    }

    /// Removes and returns every candidate the watermark released, sorted to the memory path's
    /// emission order, stamping each released key's fired marker (`stamp` is the firing wall
    /// clock under TTL, `None` writes the TTL-off empty marker).
    pub(crate) fn take_ready(
        &mut self,
        watermark: i64,
        stamp: Option<i64>,
    ) -> Result<Vec<StoredCandidate>, DataFusionError> {
        let mut ready = Vec::new();
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        let marker: Vec<u8> = stamp.map(|s| s.to_le_bytes().to_vec()).unwrap_or_default();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() < KEY_PREFIX_LEN || key[4] != PENDING_TABLE {
                continue;
            }
            let rowtime = i64::from_le_bytes(value[..8].try_into().expect("rowtime"));
            if rowtime > watermark {
                continue;
            }
            let store_key = Self::store_key(&key);
            writes.delete(&key)?;
            writes.put(Self::db_key(&store_key, EMITTED_TABLE), &marker)?;
            ready.push(StoredCandidate {
                key: store_key,
                seq: u64::from_be_bytes(value[8..16].try_into().expect("sequence")),
                rowtime,
                row: value[PENDING_VALUE_PREFIX_LEN..].into(),
            });
        }
        writes.finish()?;
        ready.sort_unstable_by_key(|candidate| candidate.seq);
        Ok(ready)
    }

    /// Every pending candidate, keys in store order, for canonical savepoints.
    pub(crate) fn scan_pending(&self) -> Result<Vec<StoredCandidate>, DataFusionError> {
        let mut out = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() < KEY_PREFIX_LEN || key[4] != PENDING_TABLE {
                continue;
            }
            out.push(StoredCandidate {
                key: Self::store_key(&key),
                seq: u64::from_be_bytes(value[8..16].try_into().expect("sequence")),
                rowtime: i64::from_le_bytes(value[..8].try_into().expect("rowtime")),
                row: value[PENDING_VALUE_PREFIX_LEN..].into(),
            });
        }
        Ok(out)
    }

    /// Every fired marker with its optional stamp, keys in store order, for canonical savepoints.
    pub(crate) fn scan_markers(&self) -> Result<Vec<(ByteKey, Option<i64>)>, DataFusionError> {
        let mut out = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() < KEY_PREFIX_LEN || key[4] != EMITTED_TABLE {
                continue;
            }
            let stamp =
                (value.len() == 8).then(|| i64::from_le_bytes(value[..8].try_into().unwrap()));
            out.push((Self::store_key(&key), stamp));
        }
        Ok(out)
    }

    /// Writes restored fired markers — the blob import's inverse of `scan_markers`: a stamped
    /// marker carries its firing wall clock, an unstamped one the TTL-off empty value.
    pub(crate) fn put_markers(
        &mut self,
        markers: &[(ByteKey, Option<i64>)],
    ) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (key, stamp) in markers {
            let value: Vec<u8> = stamp.map(|s| s.to_le_bytes().to_vec()).unwrap_or_default();
            writes.put(Self::db_key(key, EMITTED_TABLE), value)?;
        }
        writes.finish()
    }

    /// Enable-TTL migration (the blob restore's stamping): markers written by a TTL-off run carry
    /// no timestamp, so they are re-stamped a full retention from the restore instead of expiring
    /// on first probe.
    pub(crate) fn adopt_ttl(&mut self, restored_at_ms: i64) -> Result<(), DataFusionError> {
        let unstamped: Vec<ByteKey> = self
            .scan_markers()?
            .into_iter()
            .filter(|(_, stamp)| stamp.is_none())
            .map(|(key, _)| key)
            .collect();
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for key in unstamped {
            writes.put(
                Self::db_key(&key, EMITTED_TABLE),
                restored_at_ms.to_le_bytes(),
            )?;
        }
        writes.finish()
    }

    /// Rebuilds stored candidate rows as a batch under the declared input schema.
    pub(crate) fn decode<'a>(
        &self,
        rows: impl Iterator<Item = &'a [u8]>,
    ) -> Result<RecordBatch, DataFusionError> {
        let parser = self.payload_converter.parser();
        let parsed: Vec<_> = rows.map(|bytes| parser.parse(bytes)).collect();
        let columns = self
            .payload_converter
            .convert_rows(parsed)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    /// Decodes store keys back to their typed key columns, for the canonical emitted section.
    pub(crate) fn decode_key_columns(&self, keys: &[&ByteKey]) -> Vec<ArrayRef> {
        let key_rows: Vec<&[u8]> = keys.iter().map(|key| &key.0[4..]).collect();
        decode_byte_keys(Some(&self.key_converter), &key_rows, &self.key_types)
    }

    pub(crate) fn key_types(&self) -> &[DataType] {
        &self.key_types
    }

    pub(crate) fn key_group(key: &ByteKey) -> i32 {
        i32::from_be_bytes(key.0[..4].try_into().expect("key group"))
    }

    /// Persists the watermark and sequence high-water mark, then takes one native checkpoint of
    /// the shared DB — candidates and markers were already written through.
    pub(crate) fn checkpoint(
        &mut self,
        snapshot_dir: &str,
    ) -> Result<RocksCheckpointManifest, DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        writes.put(SEQ_KEY, self.next_seq.to_be_bytes())?;
        writes.put(WATERMARK_KEY, self.watermark.to_be_bytes())?;
        writes.finish()?;
        if snapshot_dir.is_empty() {
            return Ok(RocksCheckpointManifest::absent());
        }
        self.generation += 1;
        checkpoint_files(&self.db, snapshot_dir, self.generation)
    }

    fn store_key(db_key: &[u8]) -> ByteKey {
        let mut out = Vec::with_capacity(db_key.len() - 1);
        out.extend_from_slice(&db_key[..4]);
        out.extend_from_slice(&db_key[KEY_PREFIX_LEN..]);
        ByteKey(out.into())
    }

    fn db_key(key: &ByteKey, table: u8) -> Vec<u8> {
        let mut out = Vec::with_capacity(key.0.len() + 1);
        out.extend_from_slice(&key.0[..4]);
        out.push(table);
        out.extend_from_slice(&key.0[4..]);
        out
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

    fn test_config(name: &str, ttl_ms: i64) -> RocksStoreConfig {
        let dir = std::env::temp_dir().join(format!(
            "streamfusion-keep-first-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        RocksStoreConfig {
            table_dir: dir.to_string_lossy().into_owned(),
            max_parallelism: 128,
            options_json: options_json(),
            ttl_ms,
            shared_resources: 0,
        }
    }

    fn snapshot_dir(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "streamfusion-keep-first-{name}-snapshot-{}",
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

    fn memory_dedup(ttl_ms: i64) -> KeepFirstDeduplicator {
        KeepFirstDeduplicator::new(vec![0], 2)
            .with_key_timestamp_precisions(vec![-1])
            .with_state_ttl(ttl_ms)
    }

    fn store_dedup(name: &str, ttl_ms: i64) -> KeepFirstDeduplicator {
        let store =
            RocksKeepFirstDedupStore::create(test_config(name, ttl_ms), schema(), &[0]).unwrap();
        memory_dedup(ttl_ms).with_store(store)
    }

    // Emission is exactly the memory path's order: incumbents in their standing order, keys whose
    // candidate was displaced this bundle after them in arrival order.
    #[test]
    fn store_backed_firing_matches_the_memory_path_exactly() {
        let mut memory = memory_dedup(0);
        let mut rocks = store_dedup("order", 0);
        for dedup in [&mut memory, &mut rocks] {
            dedup
                .push(&batch(&[1, 2, 1], &[10, 20, 30], &[100, 200, 50]), 0)
                .unwrap();
        }
        assert_eq!(memory.flush(100, 0).unwrap(), rocks.flush(100, 0).unwrap());
        for dedup in [&mut memory, &mut rocks] {
            dedup
                .push(&batch(&[1, 2, 3], &[40, 50, 60], &[110, 100, 120]), 0)
                .unwrap();
        }
        let expected = memory.flush(150, 0).unwrap();
        let actual = rocks.flush(150, 0).unwrap();
        assert_eq!(expected, actual);
        assert_eq!(expected.num_rows(), 2);
        assert_eq!(memory.late_drops, rocks.late_drops);
    }

    // Late rows drop against the persisted watermark, and fired markers block later rows.
    #[test]
    fn late_rows_and_fired_markers_match_the_memory_path() {
        let mut memory = memory_dedup(0);
        let mut rocks = store_dedup("late", 0);
        for dedup in [&mut memory, &mut rocks] {
            dedup.push(&batch(&[1], &[10], &[100]), 0).unwrap();
            assert_eq!(dedup.flush(200, 0).unwrap().num_rows(), 1);
            dedup
                .push(&batch(&[1, 2], &[11, 20], &[300, 150]), 0)
                .unwrap();
            assert_eq!(dedup.late_drops, 1);
        }
        assert_eq!(memory.flush(400, 0).unwrap(), rocks.flush(400, 0).unwrap());
    }

    // A marker expires a fixed retention after its firing and the key can fire a second +I.
    #[test]
    fn marker_ttl_expiry_refires_like_the_memory_path() {
        let mut memory = memory_dedup(1000);
        let mut rocks = store_dedup("ttl", 1000);
        for dedup in [&mut memory, &mut rocks] {
            dedup.push(&batch(&[1], &[10], &[100]), 0).unwrap();
            assert_eq!(dedup.flush(200, 0).unwrap().num_rows(), 1);
            dedup.push(&batch(&[1], &[11], &[300]), 500).unwrap();
            assert_eq!(dedup.flush(400, 500).unwrap().num_rows(), 0);
            dedup.push(&batch(&[1], &[12], &[500]), 1000).unwrap();
        }
        let expected = memory.flush(600, 1000).unwrap();
        let actual = rocks.flush(600, 1000).unwrap();
        assert_eq!(expected, actual);
        assert_eq!(expected.num_rows(), 1);
    }

    // A canonical savepoint is the blob format (watermark, framed pending, emitted), so it
    // restores into a memory deduplicator that continues identically.
    #[test]
    fn canonical_partitions_transition_back_to_the_memory_path() {
        let mut rocks = store_dedup("canonical", 0);
        rocks
            .push(&batch(&[1, 1], &[10, 11], &[100, 300]), 0)
            .unwrap();
        assert_eq!(rocks.flush(150, 0).unwrap().num_rows(), 1);
        let snapshots = rocks.canonical_partitions().unwrap();
        assert_eq!(snapshots.len(), 1);
        let blob = snapshots.into_values().next().unwrap();
        let mut memory = KeepFirstDeduplicator::restore(vec![0], 2, &blob, 0)
            .with_key_timestamp_precisions(vec![-1]);
        // The persisted watermark late-drops on both paths.
        for dedup in [&mut memory, &mut rocks] {
            dedup.push(&batch(&[2], &[20], &[50]), 0).unwrap();
            assert_eq!(dedup.late_drops, 1);
        }
        // The marker blocks key 1, the pending candidate fires.
        for dedup in [&mut memory, &mut rocks] {
            dedup.push(&batch(&[1], &[12], &[200]), 0).unwrap();
        }
        assert_eq!(memory.flush(400, 0).unwrap(), rocks.flush(400, 0).unwrap());
    }

    #[test]
    fn restore_continues_state_watermark_and_sequences() {
        let snapshot = snapshot_dir("restore");
        let mut memory = memory_dedup(0);
        let mut before = store_dedup("restore", 0);
        for dedup in [&mut memory, &mut before] {
            dedup
                .push(&batch(&[1, 2], &[10, 20], &[100, 300]), 0)
                .unwrap();
            assert_eq!(dedup.flush(150, 0).unwrap().num_rows(), 1);
        }
        let manifest = before.store_mut().checkpoint(&snapshot).unwrap();
        drop(before);

        let store = RocksKeepFirstDedupStore::open_merged(
            test_config("restore-reopen", 0),
            schema(),
            &[0],
            &[(snapshot, manifest.snapshot_id)],
            0..=127,
            true,
        )
        .unwrap();
        let mut restored = memory_dedup(0).with_store(store);
        for dedup in [&mut memory, &mut restored] {
            dedup
                .push(&batch(&[1, 3], &[11, 30], &[50, 200]), 0)
                .unwrap();
            assert_eq!(dedup.late_drops, 1);
        }
        assert_eq!(
            memory.flush(400, 0).unwrap(),
            restored.flush(400, 0).unwrap()
        );
    }

    #[test]
    fn unaligned_restore_clips_key_groups() {
        let keys = [1i64, 2, 3, 4];
        let snapshot = snapshot_dir("clip");
        let mut before = store_dedup("clip", 0);
        before
            .push(&batch(&keys, &[10, 20, 30, 40], &[100; 4]), 0)
            .unwrap();
        let manifest = before.store_mut().checkpoint(&snapshot).unwrap();
        drop(before);

        let target = flink_key_group(
            binary_row_hash(&batch(&keys[..1], &[10], &[100]), &[0], 0, &[-1]),
            128,
        ) as i32;
        let store = RocksKeepFirstDedupStore::open_merged(
            test_config("clip-reopen", 0),
            schema(),
            &[0],
            &[(snapshot, manifest.snapshot_id)],
            target..=target,
            false,
        )
        .unwrap();
        let mut restored = memory_dedup(0).with_store(store);
        let fired = restored.flush(200, 0).unwrap();
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
        assert_eq!(fired.num_rows(), expected);
    }
}
