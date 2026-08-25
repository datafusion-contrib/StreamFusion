use super::{
    checkpoint_files, copy_checkpoint_db, merged_timer_deadline, open_shared_db, re,
    stored_timer_deadline, write_timer_deadline, FlinkWriteBatch, OpenedDb, PAIR_FIRST_TABLE,
    PAIR_SECOND_TABLE, TIMER_DEADLINE_KEY,
};
use crate::*;
use arrow::row::{RowConverter, SortField};
use rocksdb::{Cache, IteratorMode, Options, DB};
use std::sync::Arc;

/// `[key_group i32 BE][table u8][arrival_seq u64 BE]` — the fixed layout of every buffered row's
/// key. The key group leads so rescale clipping stays layout-agnostic, and the sequence trails so
/// a scan yields each key group's rows in arrival order.
const KEY_LEN: usize = 13;

/// Per-table arrival-sequence high-water marks, persisted at checkpoint under reserved keys whose
/// leading bytes can never be a subtask's key group (the snapshot-timer key's convention).
const SEQ_KEYS: [&[u8]; 2] = [
    b"\xff\xff\xff\xffstreamfusion-window-seq-left",
    b"\xff\xff\xff\xffstreamfusion-window-seq-right",
];

/// Bespoke persistent buffer for the window join: both sides' pending rows live as individual
/// KVs in one shared DB (left table 0, right table 1, one checkpoint manifest). A row appends on
/// arrival — the buffer IS RocksDB, with no resident working set — under a fresh arrival
/// sequence, routed by its equi-join key's group, and valued as `[window_end i64 LE][arrow-row
/// bytes]` so firing splits closed from pending without decoding payloads. On restore new
/// sequences start above the persisted high-water marks, so restored and new keys never collide.
pub(crate) struct RocksWindowBuffer {
    db: Arc<DB>,
    _cache: Option<Cache>,
    max_parallelism: usize,
    converters: (RowConverter, RowConverter),
    next_seq: [u64; 2],
    timer_deadline: i64,
    generation: i64,
    write_batch_size: usize,
}

impl RocksWindowBuffer {
    pub(crate) fn create(
        config: RocksStoreConfig,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        let opened = open_shared_db(
            &config,
            &[(Some(PAIR_FIRST_TABLE), 0), (Some(PAIR_SECOND_TABLE), 0)],
        )?;
        Self::attach(opened, &config, left_schema, right_schema)
    }

    /// [`RocksWindowBuffer::create`] over restored checkpoint directories: an aligned single
    /// source adopts the files wholesale; anything else clips rows by this subtask's key groups
    /// and takes each sequence high-water mark as the max across sources.
    pub(crate) fn open_merged(
        config: RocksStoreConfig,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        if aligned && sources.len() == 1 {
            copy_checkpoint_db(&sources[0].0, &config.table_dir)?;
            let mut buffer = Self::create(config, left_schema, right_schema)?;
            buffer.generation = sources[0].1;
            for (table, seq_key) in SEQ_KEYS.iter().enumerate() {
                buffer.next_seq[table] = buffer
                    .db
                    .get(seq_key)
                    .map_err(re)?
                    .filter(|bytes| bytes.len() == 8)
                    .map(|bytes| u64::from_be_bytes(bytes[..8].try_into().unwrap()))
                    .unwrap_or(0);
            }
            buffer.timer_deadline = stored_timer_deadline(&buffer.db)?;
            return Ok(buffer);
        }
        let mut buffer = Self::create(config, left_schema, right_schema)?;
        let mut writes = FlinkWriteBatch::new(&buffer.db, buffer.write_batch_size);
        for (source, _) in sources {
            let source_db =
                DB::open_for_read_only(&Options::default(), source, false).map_err(re)?;
            for row in source_db.iterator(IteratorMode::Start) {
                let (key, value) = row.map_err(re)?;
                if let Some(table) = SEQ_KEYS.iter().position(|seq_key| key.as_ref() == *seq_key) {
                    if value.len() == 8 {
                        buffer.next_seq[table] = buffer.next_seq[table]
                            .max(u64::from_be_bytes(value[..8].try_into().unwrap()));
                    }
                } else if key.as_ref() == TIMER_DEADLINE_KEY {
                    buffer.timer_deadline = merged_timer_deadline(buffer.timer_deadline, &value);
                } else if key.len() >= 4 {
                    let kg = i32::from_be_bytes(key[..4].try_into().expect("key group prefix"));
                    if key_groups.contains(&kg) {
                        writes.put(key, value)?;
                    }
                }
            }
        }
        write_timer_deadline(&mut writes, buffer.timer_deadline)?;
        writes.finish()?;
        Ok(buffer)
    }

    fn attach(
        opened: OpenedDb,
        config: &RocksStoreConfig,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        let converter = |schema: &SchemaRef| {
            RowConverter::new(
                schema
                    .fields()
                    .iter()
                    .map(|field| SortField::new(field.data_type().clone()))
                    .collect(),
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))
        };
        Ok(Self {
            db: opened.db,
            _cache: opened.cache,
            max_parallelism: config.max_parallelism,
            converters: (converter(&left_schema)?, converter(&right_schema)?),
            next_seq: [0, 0],
            timer_deadline: i64::MIN,
            generation: 0,
            write_batch_size: opened.write_batch_size,
        })
    }

    /// Appends one side's (already late-filtered) rows: one KV per row in arrival order, through
    /// a WAL-off write batch in the same call, so RocksDB holds the buffer's only copy.
    pub(crate) fn push(
        &mut self,
        left: bool,
        batch: &RecordBatch,
        key_columns: &[usize],
        key_timestamp_precisions: &[i32],
        window_ends: &Int64Array,
    ) -> Result<(), DataFusionError> {
        let table = Self::table(left);
        let converter = if left {
            &self.converters.0
        } else {
            &self.converters.1
        };
        let rows = converter
            .convert_columns(batch.columns())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, key_timestamp_precisions);
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (index, row) in rows.iter().enumerate() {
            let key_group = flink_key_group(encoder.hash(index), self.max_parallelism) as i32;
            let seq = self.next_seq[table as usize];
            self.next_seq[table as usize] += 1;
            let mut key = [0u8; KEY_LEN];
            key[..4].copy_from_slice(&key_group.to_be_bytes());
            key[4] = table;
            key[5..].copy_from_slice(&seq.to_be_bytes());
            let row = row.data();
            let mut value = Vec::with_capacity(8 + row.len());
            value.extend_from_slice(&window_ends.value(index).to_le_bytes());
            value.extend_from_slice(row);
            writes.put(key, value)?;
        }
        writes.finish()
    }

    /// Removes and returns one side's rows of every closed window (`window_end <= watermark`),
    /// reassembled in arrival order — the scan is key-group-major, so the closed set is re-sorted
    /// by arrival sequence before decoding. Pending rows stay put; `None` when nothing closed.
    pub(crate) fn take_closed(
        &mut self,
        left: bool,
        watermark: i64,
        schema: &SchemaRef,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let table = Self::table(left);
        let mut closed: Vec<(u64, Box<[u8]>)> = Vec::new();
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() != KEY_LEN || key[4] != table {
                continue;
            }
            let window_end = i64::from_le_bytes(value[..8].try_into().expect("window end"));
            if window_end <= watermark {
                let seq = u64::from_be_bytes(key[5..].try_into().expect("arrival sequence"));
                closed.push((seq, value[8..].into()));
                writes.delete(key)?;
            }
        }
        writes.finish()?;
        if closed.is_empty() {
            return Ok(None);
        }
        closed.sort_unstable_by_key(|&(seq, _)| seq);
        self.decode(left, schema, closed.iter().map(|(_, row)| row.as_ref()))
            .map(Some)
    }

    /// One side's full buffered contents per key group, each group's rows in arrival order, for
    /// canonical savepoints.
    pub(crate) fn rows_by_group(
        &self,
        left: bool,
        schema: &SchemaRef,
    ) -> Result<BTreeMap<i32, RecordBatch>, DataFusionError> {
        let table = Self::table(left);
        let mut rows_by_group: BTreeMap<i32, Vec<Box<[u8]>>> = BTreeMap::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() != KEY_LEN || key[4] != table {
                continue;
            }
            let key_group = i32::from_be_bytes(key[..4].try_into().expect("key group"));
            rows_by_group
                .entry(key_group)
                .or_default()
                .push(value[8..].into());
        }
        rows_by_group
            .into_iter()
            .map(|(group, rows)| {
                Ok((
                    group,
                    self.decode(left, schema, rows.iter().map(AsRef::as_ref))?,
                ))
            })
            .collect()
    }

    /// Rebuilds decoded rows as a batch under the caller's schema — the joiner passes the side's
    /// learned input schema (the create-provided one before any push), so reconstructed batches
    /// match what the memory path would have buffered.
    fn decode<'a>(
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

    /// Persists the arrival-sequence high-water marks, then takes one native checkpoint of the
    /// shared DB — rows were already written on arrival, so there is no working set to commit.
    pub(crate) fn checkpoint(
        &mut self,
        timer_deadline: i64,
        snapshot_dir: &str,
    ) -> Result<RocksCheckpointManifest, DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (table, seq_key) in SEQ_KEYS.iter().enumerate() {
            writes.put(seq_key, self.next_seq[table].to_be_bytes())?;
        }
        write_timer_deadline(&mut writes, timer_deadline)?;
        writes.finish()?;
        self.timer_deadline = timer_deadline;
        if snapshot_dir.is_empty() {
            return Ok(RocksCheckpointManifest::absent());
        }
        self.generation += 1;
        checkpoint_files(&self.db, snapshot_dir, self.generation)
    }

    /// The processing-time timer deadline persisted by the checkpoint this store restored from.
    pub(crate) fn timer_deadline(&self) -> i64 {
        self.timer_deadline
    }

    fn table(left: bool) -> u8 {
        if left {
            PAIR_FIRST_TABLE
        } else {
            PAIR_SECOND_TABLE
        }
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
            "streamfusion-window-buffer-{name}-{}",
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
            "streamfusion-window-buffer-{name}-snapshot-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir.to_string_lossy().into_owned()
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn batch(keys: &[i64], values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
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

    #[test]
    fn firing_reassembles_arrival_order_and_keeps_pending() {
        let mut buffer =
            RocksWindowBuffer::create(test_config("order"), schema(), schema()).unwrap();
        let rows = batch(&[1, 2, 3, 1], &[10, 20, 30, 40]);
        let ends = Int64Array::from(vec![100, 100, 200, 100]);
        buffer.push(true, &rows, &[0], &[-1], &ends).unwrap();
        buffer
            .push(
                false,
                &batch(&[1], &[99]),
                &[0],
                &[-1],
                &Int64Array::from(vec![100]),
            )
            .unwrap();

        let closed = buffer.take_closed(true, 100, &schema()).unwrap().unwrap();
        assert_eq!(column(&closed, 0), vec![1, 2, 1]);
        assert_eq!(column(&closed, 1), vec![10, 20, 40]);
        assert!(buffer.take_closed(true, 100, &schema()).unwrap().is_none());

        let pending = buffer.take_closed(true, 200, &schema()).unwrap().unwrap();
        assert_eq!(column(&pending, 1), vec![30]);

        let right = buffer.take_closed(false, 100, &schema()).unwrap().unwrap();
        assert_eq!(column(&right, 1), vec![99]);
        assert!(buffer.take_closed(false, 200, &schema()).unwrap().is_none());
    }

    #[test]
    fn restore_continues_arrival_sequences_above_the_high_water_mark() {
        let snapshot = snapshot_dir("restore");
        let mut buffer =
            RocksWindowBuffer::create(test_config("restore"), schema(), schema()).unwrap();
        buffer
            .push(
                true,
                &batch(&[1, 1], &[10, 20]),
                &[0],
                &[-1],
                &Int64Array::from(vec![100, 100]),
            )
            .unwrap();
        let manifest = buffer.checkpoint(i64::MIN, &snapshot).unwrap();
        drop(buffer);

        let mut restored = RocksWindowBuffer::open_merged(
            test_config("restore-reopen"),
            schema(),
            schema(),
            &[(snapshot, manifest.snapshot_id)],
            0..=127,
            true,
        )
        .unwrap();
        restored
            .push(
                true,
                &batch(&[1, 1], &[30, 40]),
                &[0],
                &[-1],
                &Int64Array::from(vec![100, 100]),
            )
            .unwrap();

        let closed = restored.take_closed(true, 100, &schema()).unwrap().unwrap();
        assert_eq!(column(&closed, 1), vec![10, 20, 30, 40]);
    }

    #[test]
    fn unaligned_restore_clips_to_the_key_group_range() {
        let snapshot = snapshot_dir("clip");
        let keys = [1i64, 2, 3, 4];
        let rows = batch(&keys, &[10, 20, 30, 40]);
        let mut buffer =
            RocksWindowBuffer::create(test_config("clip"), schema(), schema()).unwrap();
        buffer
            .push(
                true,
                &rows,
                &[0],
                &[-1],
                &Int64Array::from(vec![100; keys.len()]),
            )
            .unwrap();
        let manifest = buffer.checkpoint(i64::MIN, &snapshot).unwrap();
        drop(buffer);

        let target = flink_key_group(binary_row_hash(&rows, &[0], 0, &[-1]), 128) as i32;
        let mut restored = RocksWindowBuffer::open_merged(
            test_config("clip-reopen"),
            schema(),
            schema(),
            &[(snapshot, manifest.snapshot_id)],
            target..=target,
            false,
        )
        .unwrap();
        restored
            .push(
                true,
                &batch(&[1], &[50]),
                &[0],
                &[-1],
                &Int64Array::from(vec![100]),
            )
            .unwrap();

        let expected: Vec<i64> = keys
            .iter()
            .zip([10i64, 20, 30, 40])
            .filter(|&(key, _)| {
                flink_key_group(binary_row_hash(&batch(&[*key], &[0]), &[0], 0, &[-1]), 128) as i32
                    == target
            })
            .map(|(_, value)| value)
            .chain([50])
            .collect();
        let closed = restored.take_closed(true, 100, &schema()).unwrap().unwrap();
        assert_eq!(column(&closed, 1), expected);
    }
}
