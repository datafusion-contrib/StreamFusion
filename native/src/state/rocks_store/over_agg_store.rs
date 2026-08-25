use super::{
    checkpoint_files, copy_checkpoint_db, open_shared_db, re, FlinkWriteBatch, OpenedDb,
    PAIR_FIRST_TABLE, PAIR_SECOND_TABLE,
};
use crate::*;
use arrow::row::{RowConverter, SortField};
use rocksdb::{Cache, IteratorMode, Options, DB};
use std::sync::Arc;

/// Two tables share the OVER aggregate's DB. Folds — the per-key running fold state — key as
/// `[key_group i32 BE][0][partition key arrow-row bytes]`, valued `[cleanup_at i64 LE][state
/// arrow-row bytes]`: the retention stamp rides as a fixed value prefix (i64::MIN while the
/// deadline scheme is off), mirroring how the raw snapshots ride it as a trailing per-key column.
/// Pending — the buffered input rows a watermark has not completed — key as
/// `[key_group i32 BE][1][arrival_seq u64 BE]` (the window join buffer's layout), valued
/// `[rowtime_millis i64 LE][input row arrow-row bytes]`, so a firing splits complete from pending
/// rows without decoding payloads. Both key groups route by the PARTITION BY key's BinaryRow hash
/// — identical to the blob partitioner — and lead the key so rescale clipping is layout-agnostic.
const FOLDS_TABLE: u8 = PAIR_FIRST_TABLE;
const PENDING_TABLE: u8 = PAIR_SECOND_TABLE;
const KEY_GROUP_LEN: usize = 4;
const PENDING_KEY_LEN: usize = 13;
const STAMP_LEN: usize = 8;

/// The late-data watermark and the pending arrival-sequence high-water mark, persisted at
/// checkpoint under reserved keys whose leading bytes can never be a subtask's key group.
const WATERMARK_KEY: &[u8] = b"\xff\xff\xff\xffstreamfusion-over-agg-watermark";
const SEQ_KEY: &[u8] = b"\xff\xff\xff\xffstreamfusion-over-agg-seq";

/// One persisted fold read back from the store: the partition key's arrow-row bytes (the
/// aggregator's own key encoding), the retention stamp, and the running state scalars in
/// snapshot order.
pub(crate) struct StoredOverFold {
    pub(crate) key: Box<[u8]>,
    pub(crate) stamp: i64,
    pub(crate) state: Vec<ScalarValue>,
}

/// Persistent backend for the event-time OVER aggregate: input rows append to the pending table
/// on arrival (the buffer IS RocksDB, with no resident copy), a watermark firing removes and
/// returns the completed rows in arrival order, and the per-key running fold hydrates from the
/// folds table for exactly the fired keys and writes back at the bundle boundary. Retention is
/// enforced by the operator's lazy deadline scheme — a compaction filter cannot honor the
/// pending-row deferral — so neither table installs one.
pub(crate) struct RocksOverAggStore {
    db: Arc<DB>,
    _cache: Option<Cache>,
    max_parallelism: usize,
    key_groups: std::ops::RangeInclusive<i32>,
    state_converter: RowConverter,
    payload_converter: RowConverter,
    payload_schema: SchemaRef,
    watermark: i64,
    next_seq: u64,
    generation: i64,
    write_batch_size: usize,
}

impl RocksOverAggStore {
    pub(crate) fn create(
        config: RocksStoreConfig,
        state_types: &[DataType],
        payload_schema: SchemaRef,
        key_groups: std::ops::RangeInclusive<i32>,
    ) -> Result<Self, DataFusionError> {
        let payload_types: Vec<DataType> = payload_schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        if !rocks_row_supported(state_types) || !rocks_row_supported(&payload_types) {
            return Err(DataFusionError::Plan(
                "over state shape not supported by RocksDB".into(),
            ));
        }
        let opened = open_shared_db(&config, &[(Some(FOLDS_TABLE), 0), (Some(PENDING_TABLE), 0)])?;
        Self::attach(opened, &config, state_types, payload_schema, key_groups)
    }

    /// [`RocksOverAggStore::create`] over restored checkpoint directories: an aligned single
    /// source adopts the files wholesale; anything else clips rows by this subtask's key groups.
    /// The restored watermark and sequence high-water mark are each the max across sources.
    pub(crate) fn open_merged(
        config: RocksStoreConfig,
        state_types: &[DataType],
        payload_schema: SchemaRef,
        key_groups: std::ops::RangeInclusive<i32>,
        sources: &[(String, i64)],
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        if aligned && sources.len() == 1 {
            copy_checkpoint_db(&sources[0].0, &config.table_dir)?;
            let mut store = Self::create(config, state_types, payload_schema, key_groups)?;
            store.generation = sources[0].1;
            store.watermark = store
                .db
                .get(WATERMARK_KEY)
                .map_err(re)?
                .filter(|bytes| bytes.len() == 8)
                .map(|bytes| i64::from_be_bytes(bytes[..8].try_into().unwrap()))
                .unwrap_or(i64::MIN);
            store.next_seq = store
                .db
                .get(SEQ_KEY)
                .map_err(re)?
                .filter(|bytes| bytes.len() == 8)
                .map(|bytes| u64::from_be_bytes(bytes[..8].try_into().unwrap()))
                .unwrap_or(0);
            return Ok(store);
        }
        let mut store = Self::create(config, state_types, payload_schema, key_groups)?;
        let mut writes = FlinkWriteBatch::new(&store.db, store.write_batch_size);
        for (source, _) in sources {
            let source_db =
                DB::open_for_read_only(&Options::default(), source, false).map_err(re)?;
            for row in source_db.iterator(IteratorMode::Start) {
                let (key, value) = row.map_err(re)?;
                if key.as_ref() == WATERMARK_KEY {
                    if value.len() == 8 {
                        store.watermark = store
                            .watermark
                            .max(i64::from_be_bytes(value[..8].try_into().unwrap()));
                    }
                } else if key.as_ref() == SEQ_KEY {
                    if value.len() == 8 {
                        store.next_seq = store
                            .next_seq
                            .max(u64::from_be_bytes(value[..8].try_into().unwrap()));
                    }
                } else if key.len() >= KEY_GROUP_LEN {
                    let kg = i32::from_be_bytes(key[..4].try_into().expect("key group prefix"));
                    if store.key_groups.contains(&kg) {
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
        state_types: &[DataType],
        payload_schema: SchemaRef,
        key_groups: std::ops::RangeInclusive<i32>,
    ) -> Result<Self, DataFusionError> {
        let converter = |types: &[DataType]| {
            RowConverter::new(types.iter().map(|t| SortField::new(t.clone())).collect())
                .map_err(|e| DataFusionError::External(Box::new(e)))
        };
        let payload_types: Vec<DataType> = payload_schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        Ok(Self {
            db: opened.db,
            _cache: opened.cache,
            max_parallelism: config.max_parallelism,
            key_groups,
            state_converter: converter(state_types)?,
            payload_converter: converter(&payload_types)?,
            payload_schema,
            watermark: i64::MIN,
            next_seq: 0,
            generation: 0,
            write_batch_size: opened.write_batch_size,
        })
    }

    /// The late-data watermark persisted by the checkpoint this store restored from.
    pub(crate) fn watermark(&self) -> i64 {
        self.watermark
    }

    /// The create-provided input schema, for a firing before any batch arrives.
    pub(crate) fn payload_schema(&self) -> SchemaRef {
        self.payload_schema.clone()
    }

    /// The Flink key group of a partition key's BinaryRow hash — identical routing to the blob
    /// path's raw keyed-state partitioner.
    pub(crate) fn key_group(&self, binary_row_hash: i32) -> i32 {
        flink_key_group(binary_row_hash, self.max_parallelism) as i32
    }

    pub(crate) fn fold_key(&self, key_group: i32, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(KEY_GROUP_LEN + 1 + key.len());
        out.extend_from_slice(&key_group.to_be_bytes());
        out.push(FOLDS_TABLE);
        out.extend_from_slice(key);
        out
    }

    /// Batched point reads for a firing's touched keys: one multi-get, one columnar decode.
    /// `None` marks a key with no committed fold.
    pub(crate) fn get_folds(
        &self,
        db_keys: &[Vec<u8>],
    ) -> Result<Vec<Option<(i64, Vec<ScalarValue>)>>, DataFusionError> {
        let fetched = self.db.multi_get(db_keys);
        let mut values = Vec::with_capacity(fetched.len());
        for value in fetched {
            values.push(value.map_err(re)?);
        }
        let hits: Vec<&[u8]> = values
            .iter()
            .flatten()
            .map(|value| &value.as_slice()[STAMP_LEN..])
            .collect();
        let mut states = self.decode_states(&hits)?.into_iter();
        Ok(values
            .iter()
            .map(|value| {
                value.as_ref().map(|value| {
                    let stamp =
                        i64::from_le_bytes(value[..STAMP_LEN].try_into().expect("fold stamp"));
                    (stamp, states.next().expect("decoded fold state"))
                })
            })
            .collect())
    }

    /// Writes a firing's touched folds through in one columnar conversion — Flink's write path,
    /// one memtable write per touched key per bundle. Each entry carries the key's current
    /// retention stamp so the persisted deadline is the post-fire re-arm.
    pub(crate) fn write_folds(
        &mut self,
        entries: &[(Vec<u8>, i64)],
        state_columns: &[ArrayRef],
    ) -> Result<(), DataFusionError> {
        if entries.is_empty() {
            return Ok(());
        }
        let rows = self
            .state_converter
            .convert_columns(state_columns)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for ((db_key, stamp), row) in entries.iter().zip(rows.iter()) {
            let row = row.data();
            let mut value = Vec::with_capacity(STAMP_LEN + row.len());
            value.extend_from_slice(&stamp.to_le_bytes());
            value.extend_from_slice(row);
            writes.put(db_key, value)?;
        }
        writes.finish()
    }

    /// Tombstones cleared keys' folds — fired cleanup deadlines clearing them — so a restore
    /// cannot resurrect the cleared state.
    pub(crate) fn delete_folds(&mut self, db_keys: &[Vec<u8>]) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for db_key in db_keys {
            writes.delete(db_key)?;
        }
        writes.finish()
    }

    /// Appends one (already late-filtered) input batch's rows to the pending table in arrival
    /// order, routed by each row's PARTITION BY key group, through a WAL-off write batch in the
    /// same call — RocksDB holds the buffer's only copy.
    pub(crate) fn push_pending(
        &mut self,
        batch: &RecordBatch,
        key_columns: &[usize],
        key_timestamp_precisions: &[i32],
        rowtimes: &Int64Array,
    ) -> Result<(), DataFusionError> {
        let rows = self
            .payload_converter
            .convert_columns(batch.columns())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, key_timestamp_precisions);
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (index, row) in rows.iter().enumerate() {
            let key_group = self.key_group(encoder.hash(index));
            let seq = self.next_seq;
            self.next_seq += 1;
            let mut key = [0u8; PENDING_KEY_LEN];
            key[..4].copy_from_slice(&key_group.to_be_bytes());
            key[4] = PENDING_TABLE;
            key[5..].copy_from_slice(&seq.to_be_bytes());
            let row = row.data();
            let mut value = Vec::with_capacity(8 + row.len());
            value.extend_from_slice(&rowtimes.value(index).to_le_bytes());
            value.extend_from_slice(row);
            writes.put(key, value)?;
        }
        writes.finish()
    }

    /// Removes and returns every pending row the watermark completed (`rowtime <= watermark`),
    /// reassembled in arrival order — the scan is key-group-major, so the completed set is
    /// re-sorted by arrival sequence before decoding, matching the memory path's buffered order.
    /// Pending rows stay put; `None` when nothing completed.
    pub(crate) fn take_complete(
        &mut self,
        watermark: i64,
        schema: &SchemaRef,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let mut complete: Vec<(u64, Box<[u8]>)> = Vec::new();
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() != PENDING_KEY_LEN || key[4] != PENDING_TABLE {
                continue;
            }
            let rowtime = i64::from_le_bytes(value[..8].try_into().expect("pending rowtime"));
            if rowtime <= watermark {
                let seq = u64::from_be_bytes(key[5..].try_into().expect("arrival sequence"));
                complete.push((seq, value[8..].into()));
                writes.delete(key)?;
            }
        }
        writes.finish()?;
        if complete.is_empty() {
            return Ok(None);
        }
        complete.sort_unstable_by_key(|&(seq, _)| seq);
        self.decode_payload(schema, complete.iter().map(|(_, row)| row.as_ref()))
            .map(Some)
    }

    /// Every pending row in arrival order — restore-time deferral derivation and canonical
    /// savepoints.
    pub(crate) fn scan_pending(
        &self,
        schema: &SchemaRef,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let mut pending: Vec<(u64, Box<[u8]>)> = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() != PENDING_KEY_LEN || key[4] != PENDING_TABLE {
                continue;
            }
            let seq = u64::from_be_bytes(key[5..].try_into().expect("arrival sequence"));
            pending.push((seq, value[8..].into()));
        }
        if pending.is_empty() {
            return Ok(None);
        }
        pending.sort_unstable_by_key(|&(seq, _)| seq);
        self.decode_payload(schema, pending.iter().map(|(_, row)| row.as_ref()))
            .map(Some)
    }

    /// Every committed fold — restore-time retention hydration and canonical savepoints.
    pub(crate) fn scan_folds(&self) -> Result<Vec<StoredOverFold>, DataFusionError> {
        let mut folds: Vec<(Box<[u8]>, i64, Box<[u8]>)> = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.as_ref() == WATERMARK_KEY
                || key.as_ref() == SEQ_KEY
                || key.len() <= KEY_GROUP_LEN + 1
                || key[4] != FOLDS_TABLE
            {
                continue;
            }
            folds.push((
                key[KEY_GROUP_LEN + 1..].into(),
                i64::from_le_bytes(value[..STAMP_LEN].try_into().expect("fold stamp")),
                value[STAMP_LEN..].into(),
            ));
        }
        let states: Vec<&[u8]> = folds.iter().map(|(_, _, state)| state.as_ref()).collect();
        let states = self.decode_states(&states)?;
        Ok(folds
            .into_iter()
            .zip(states)
            .map(|((key, stamp, _), state)| StoredOverFold { key, stamp, state })
            .collect())
    }

    fn decode_payload<'a>(
        &self,
        schema: &SchemaRef,
        rows: impl Iterator<Item = &'a [u8]>,
    ) -> Result<RecordBatch, DataFusionError> {
        let parser = self.payload_converter.parser();
        let parsed: Vec<_> = rows.map(|bytes| parser.parse(bytes)).collect();
        let columns = self
            .payload_converter
            .convert_rows(parsed)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(RecordBatch::try_new(schema.clone(), columns)?)
    }

    fn decode_states(&self, values: &[&[u8]]) -> Result<Vec<Vec<ScalarValue>>, DataFusionError> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let parser = self.state_converter.parser();
        let rows: Vec<_> = values.iter().map(|value| parser.parse(value)).collect();
        let columns = self
            .state_converter
            .convert_rows(rows)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut out = Vec::with_capacity(values.len());
        for row in 0..values.len() {
            let mut scalars = Vec::with_capacity(columns.len());
            for column in &columns {
                scalars.push(ScalarValue::try_from_array(column, row)?);
            }
            out.push(scalars);
        }
        Ok(out)
    }

    /// Persists the late-data watermark and the arrival-sequence high-water mark, then takes one
    /// native checkpoint — pending rows and folds were already written in their own calls, so
    /// there is no working set to commit.
    pub(crate) fn checkpoint(
        &mut self,
        watermark: i64,
        snapshot_dir: &str,
    ) -> Result<RocksCheckpointManifest, DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        writes.put(WATERMARK_KEY, watermark.to_be_bytes())?;
        writes.put(SEQ_KEY, self.next_seq.to_be_bytes())?;
        writes.finish()?;
        self.watermark = watermark;
        if snapshot_dir.is_empty() {
            return Ok(RocksCheckpointManifest::absent());
        }
        self.generation += 1;
        checkpoint_files(&self.db, snapshot_dir, self.generation)
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
            "streamfusion-over-agg-store-{name}-{}",
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
            "streamfusion-over-agg-store-{name}-snapshot-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir.to_string_lossy().into_owned()
    }

    fn over_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("rt", DataType::Int64, false),
        ]))
    }

    fn over_batch(k: &[i64], v: &[i64], rt: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            over_schema(),
            vec![
                Arc::new(Int64Array::from(k.to_vec())),
                Arc::new(Int64Array::from(v.to_vec())),
                Arc::new(Int64Array::from(rt.to_vec())),
            ],
        )
        .unwrap()
    }

    fn column(batch: &RecordBatch, index: usize) -> Vec<i64> {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn memory_sum_over(retention_ms: i64) -> OverWindowAggregator {
        OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 0, 0, false)
            .with_state_retention(retention_ms)
    }

    fn sum_store(name: &str) -> RocksOverAggStore {
        RocksOverAggStore::create(
            test_config(name),
            &rocks_over_state_types(&[0], &[0], 0, false).unwrap(),
            over_schema(),
            0..=127,
        )
        .unwrap()
    }

    fn store_sum_over(name: &str, retention_ms: i64) -> OverWindowAggregator {
        OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 0, 0, false)
            .with_state_retention(retention_ms)
            .with_key_timestamp_precisions(vec![-1])
            .with_store(sum_store(name), vec![DataType::Int64])
    }

    // The store-backed OVER must emit byte-identical batches to the memory path: same complete /
    // pending split, same arrival-order emission, same per-key running fold, same late drops.
    #[test]
    fn store_backed_aggregator_matches_the_memory_path() {
        let mut memory = memory_sum_over(0);
        let mut rocks = store_sum_over("parity", 0);
        for batch in [
            over_batch(&[1, 2, 1], &[10, 100, 20], &[0, 500, 1000]),
            over_batch(&[1, 3], &[5, 7], &[1500, 700]),
        ] {
            memory.push(batch.clone(), 0).unwrap();
            rocks.push(batch, 0).unwrap();
        }
        assert_eq!(
            memory.flush(1000, 0).unwrap(),
            rocks.flush(1000, 0).unwrap()
        );

        let late = over_batch(&[1], &[9], &[900]);
        memory.push(late.clone(), 0).unwrap();
        rocks.push(late, 0).unwrap();
        assert_eq!(memory.late_drops, rocks.late_drops);
        assert_eq!(
            memory.flush(2000, 0).unwrap(),
            rocks.flush(2000, 0).unwrap()
        );
        assert_eq!(
            memory.flush(3000, 0).unwrap(),
            rocks.flush(3000, 0).unwrap()
        );
    }

    #[test]
    fn store_backed_window_functions_match_the_memory_path() {
        let mut memory =
            OverWindowAggregator::new(vec![], vec![10, 11], 2, vec![], vec![0], 0, 0, false);
        let store = RocksOverAggStore::create(
            test_config("wf-parity"),
            &rocks_over_state_types(&[], &[10, 11], 0, false).unwrap(),
            over_schema(),
            0..=127,
        )
        .unwrap();
        let mut rocks =
            OverWindowAggregator::new(vec![], vec![10, 11], 2, vec![], vec![0], 0, 0, false)
                .with_key_timestamp_precisions(vec![-1])
                .with_store(store, vec![DataType::Int64]);
        for batch in [
            over_batch(&[1, 1, 2], &[0, 0, 0], &[0, 0, 500]),
            over_batch(&[1, 2], &[0, 0], &[1500, 1500]),
        ] {
            memory.push(batch.clone(), 0).unwrap();
            rocks.push(batch, 0).unwrap();
        }
        assert_eq!(
            memory.flush(1000, 0).unwrap(),
            rocks.flush(1000, 0).unwrap()
        );
        assert_eq!(
            memory.flush(2000, 0).unwrap(),
            rocks.flush(2000, 0).unwrap()
        );
    }

    // A canonical savepoint of the store-backed OVER is the memory path's own raw keyed encoding,
    // so it restores into a memory aggregator that continues identically. One key keeps every
    // buffered row in one key group — restores regroup the buffer by key group, on both backends.
    #[test]
    fn canonical_partitions_transition_back_to_the_memory_path() {
        let mut rocks = store_sum_over("canonical", 0);
        rocks
            .push(over_batch(&[1, 1], &[10, 3], &[0, 1500]), 0)
            .unwrap();
        assert_eq!(rocks.flush(1000, 0).unwrap().num_rows(), 1);
        rocks.push(over_batch(&[1], &[4], &[1600]), 0).unwrap();
        let snapshots: Vec<Vec<u8>> = rocks
            .canonical_partitions(128, &[-1])
            .unwrap()
            .into_values()
            .collect();
        let mut memory = OverWindowAggregator::restore_partitions(
            vec![0],
            vec![0],
            2,
            vec![1],
            vec![0],
            0,
            0,
            false,
            &snapshots,
            0,
            0,
        );
        assert_eq!(
            memory.flush(2000, 0).unwrap(),
            rocks.flush(2000, 0).unwrap()
        );
    }

    // Folds, pending rows, the late-data watermark, and the arrival sequence survive a native
    // checkpoint: restored pending rows keep their arrival order ahead of newly pushed ones, the
    // running fold continues from stored state, and late rows stay dropped.
    #[test]
    fn store_backed_aggregator_restores_folds_pending_and_watermark() {
        let snapshot = snapshot_dir("restore");
        let mut before = store_sum_over("restore", 0);
        before
            .push(over_batch(&[1, 1], &[10, 3], &[0, 1500]), 0)
            .unwrap();
        assert_eq!(column(&before.flush(1000, 0).unwrap(), 3), vec![10]);
        let manifest = before.checkpoint_store(&snapshot).unwrap();
        drop(before);

        let store = RocksOverAggStore::open_merged(
            test_config("restore-reopen"),
            &rocks_over_state_types(&[0], &[0], 0, false).unwrap(),
            over_schema(),
            0..=127,
            &[(snapshot, manifest.snapshot_id)],
            true,
        )
        .unwrap();
        let mut restored =
            OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 0, 0, false)
                .with_key_timestamp_precisions(vec![-1])
                .with_store(store, vec![DataType::Int64]);
        restored
            .push(over_batch(&[1, 1], &[9, 4], &[900, 1200]), 0)
            .unwrap();
        assert_eq!(restored.late_drops, 1);

        let out = restored.flush(2000, 0).unwrap();
        assert_eq!(column(&out, 1), vec![3, 4]);
        assert_eq!(column(&out, 3), vec![17, 14]);
    }

    // The deadline retention scheme behaves identically on the store: an idle key past its
    // deadline folds fresh, and the persisted stamp keeps its expiry timing across a restore.
    #[test]
    fn store_backed_retention_matches_the_memory_path_and_survives_restore() {
        let mut memory = memory_sum_over(2000);
        let mut rocks = store_sum_over("retention", 2000);
        memory.push(over_batch(&[1], &[10], &[100]), 5000).unwrap();
        rocks.push(over_batch(&[1], &[10], &[100]), 5000).unwrap();
        assert_eq!(
            memory.flush(200, 5000).unwrap(),
            rocks.flush(200, 5000).unwrap()
        );
        memory.push(over_batch(&[1], &[5], &[300]), 8000).unwrap();
        rocks.push(over_batch(&[1], &[5], &[300]), 8000).unwrap();
        let expired = rocks.flush(400, 8000).unwrap();
        assert_eq!(memory.flush(400, 8000).unwrap(), expired);
        assert_eq!(column(&expired, 3), vec![5]);

        let snapshot = snapshot_dir("retention");
        let manifest = rocks.checkpoint_store(&snapshot).unwrap();
        drop(rocks);
        let store = RocksOverAggStore::open_merged(
            test_config("retention-reopen"),
            &rocks_over_state_types(&[0], &[0], 0, false).unwrap(),
            over_schema(),
            0..=127,
            &[(snapshot, manifest.snapshot_id)],
            true,
        )
        .unwrap();
        let mut restored =
            OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 0, 0, false)
                .with_state_retention(2000)
                .with_key_timestamp_precisions(vec![-1])
                .with_store(store, vec![DataType::Int64]);
        // The stamp persisted at the last fold (8000 + 3000): alive just inside it, cleared at it.
        restored.adopt_store_retention(9000).unwrap();
        restored
            .push(over_batch(&[1], &[2], &[500]), 10999)
            .unwrap();
        assert_eq!(column(&restored.flush(600, 10999).unwrap(), 3), vec![7]);
        restored
            .push(over_batch(&[1], &[1], &[700]), 14000)
            .unwrap();
        assert_eq!(column(&restored.flush(800, 14000).unwrap(), 3), vec![1]);
    }
}
