use super::{checkpoint_files, copy_checkpoint_db, open_shared_db, re, FlinkWriteBatch, OpenedDb};
use crate::*;
use arrow::row::{RowConverter, SortField};
use rocksdb::{Cache, IteratorMode, Options, DB};
use std::sync::Arc;

/// `[key_group i32 BE][arrival_seq u64 BE]` — the fixed layout of every buffered row's key, valued
/// `[rowtime i64 LE][row arrow-row]`. The temporal sort is unkeyed: like the host it owns exactly
/// one canonical empty key, so every row lives in key group zero (the empty key's group under max
/// parallelism one) and a rescale can move — but never split — the buffer. The family's key-group
/// prefix stays so the shared clipping logic applies unchanged.
const KEY_LEN: usize = 12;
const VALUE_PREFIX_LEN: usize = 8;
const SINGLETON_KEY_GROUP: i32 = 0;

/// The arrival-sequence high-water mark, persisted at checkpoint under a reserved key whose
/// leading bytes can never be a subtask's key group (the snapshot-timer key's convention).
const SEQ_KEY: &[u8] = b"\xff\xff\xff\xffstreamfusion-temporal-sort-seq";

/// Bespoke persistent buffer for the event-time sort: rows append on arrival — the buffer IS
/// RocksDB, with no resident copy — under a fresh arrival sequence, and a watermark firing
/// removes and returns the completed rows stably sorted by rowtime (scan order is sequence order,
/// so equal-rowtime rows keep arrival order, as the memory path's stable sort does).
pub(crate) struct RocksTemporalSortBuffer {
    db: Arc<DB>,
    _cache: Option<Cache>,
    converter: RowConverter,
    schema: SchemaRef,
    next_seq: u64,
    generation: i64,
    write_batch_size: usize,
}

impl RocksTemporalSortBuffer {
    pub(crate) fn create(
        config: RocksStoreConfig,
        schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        let row_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        if !rocks_row_supported(&row_types) {
            return Err(DataFusionError::Plan(
                "temporal-sort row shape not supported by RocksDB".into(),
            ));
        }
        let opened = open_shared_db(&config, &[(None, 0)])?;
        Self::attach(opened, schema, row_types)
    }

    /// [`RocksTemporalSortBuffer::create`] over restored checkpoint directories: an aligned single
    /// source adopts the files wholesale; anything else clips rows by this subtask's key groups
    /// (always group zero here) and takes the sequence high-water mark as the max across sources.
    pub(crate) fn open_merged(
        config: RocksStoreConfig,
        schema: SchemaRef,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        if aligned && sources.len() == 1 {
            copy_checkpoint_db(&sources[0].0, &config.table_dir)?;
            let mut buffer = Self::create(config, schema)?;
            buffer.generation = sources[0].1;
            buffer.next_seq = buffer
                .db
                .get(SEQ_KEY)
                .map_err(re)?
                .filter(|bytes| bytes.len() == 8)
                .map(|bytes| u64::from_be_bytes(bytes[..8].try_into().unwrap()))
                .unwrap_or(0);
            return Ok(buffer);
        }
        let mut buffer = Self::create(config, schema)?;
        let mut writes = FlinkWriteBatch::new(&buffer.db, buffer.write_batch_size);
        for (source, _) in sources {
            let source_db =
                DB::open_for_read_only(&Options::default(), source, false).map_err(re)?;
            for row in source_db.iterator(IteratorMode::Start) {
                let (key, value) = row.map_err(re)?;
                if key.as_ref() == SEQ_KEY {
                    if value.len() == 8 {
                        buffer.next_seq = buffer
                            .next_seq
                            .max(u64::from_be_bytes(value[..8].try_into().unwrap()));
                    }
                } else if key.len() == KEY_LEN {
                    let kg = i32::from_be_bytes(key[..4].try_into().expect("key group prefix"));
                    if key_groups.contains(&kg) {
                        writes.put(key, value)?;
                    }
                }
            }
        }
        writes.finish()?;
        Ok(buffer)
    }

    fn attach(
        opened: OpenedDb,
        schema: SchemaRef,
        row_types: Vec<DataType>,
    ) -> Result<Self, DataFusionError> {
        let converter = RowConverter::new(
            row_types
                .iter()
                .map(|data_type| SortField::new(data_type.clone()))
                .collect(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            db: opened.db,
            _cache: opened.cache,
            converter,
            schema,
            next_seq: 0,
            generation: 0,
            write_batch_size: opened.write_batch_size,
        })
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Appends a batch's rows: one KV per row in arrival order, through a WAL-off write batch in
    /// the same call, so RocksDB holds the buffer's only copy.
    pub(crate) fn push(
        &mut self,
        batch: &RecordBatch,
        rowtimes: &Int64Array,
    ) -> Result<(), DataFusionError> {
        let rows = self
            .converter
            .convert_columns(batch.columns())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (index, row) in rows.iter().enumerate() {
            let seq = self.next_seq;
            self.next_seq += 1;
            let mut key = [0u8; KEY_LEN];
            key[..4].copy_from_slice(&SINGLETON_KEY_GROUP.to_be_bytes());
            key[4..].copy_from_slice(&seq.to_be_bytes());
            let row = row.data();
            let mut value = Vec::with_capacity(VALUE_PREFIX_LEN + row.len());
            value.extend_from_slice(&rowtimes.value(index).to_le_bytes());
            value.extend_from_slice(row);
            writes.put(key, value)?;
        }
        writes.finish()
    }

    /// Removes and returns the rows the watermark has completed (`rowtime <= watermark`), stably
    /// sorted ascending by rowtime — the scan yields arrival order, so the stable sort keeps it
    /// for ties, exactly the memory path's order. `None` when nothing completed.
    pub(crate) fn take_complete(
        &mut self,
        watermark: i64,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let mut complete: Vec<(i64, Box<[u8]>)> = Vec::new();
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() != KEY_LEN {
                continue;
            }
            let rowtime = i64::from_le_bytes(value[..8].try_into().expect("rowtime"));
            if rowtime <= watermark {
                complete.push((rowtime, value[VALUE_PREFIX_LEN..].into()));
                writes.delete(key)?;
            }
        }
        writes.finish()?;
        if complete.is_empty() {
            return Ok(None);
        }
        complete.sort_by_key(|&(rowtime, _)| rowtime);
        self.decode(complete.iter().map(|(_, row)| row.as_ref()))
            .map(Some)
    }

    /// The full buffered contents in arrival order, for canonical savepoints — `None` when empty,
    /// so the exported blob stays byte-compatible with the memory snapshot's "no rows, no bytes".
    pub(crate) fn scan_buffered(&self) -> Result<Option<RecordBatch>, DataFusionError> {
        let mut rows: Vec<Box<[u8]>> = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() == KEY_LEN {
                rows.push(value[VALUE_PREFIX_LEN..].into());
            }
        }
        if rows.is_empty() {
            return Ok(None);
        }
        self.decode(rows.iter().map(AsRef::as_ref)).map(Some)
    }

    fn decode<'a>(
        &self,
        rows: impl Iterator<Item = &'a [u8]>,
    ) -> Result<RecordBatch, DataFusionError> {
        let parser = self.converter.parser();
        let parsed: Vec<_> = rows.map(|bytes| parser.parse(bytes)).collect();
        let columns = self
            .converter
            .convert_rows(parsed)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }

    /// Persists the arrival-sequence high-water mark, then takes one native checkpoint of the DB —
    /// rows were already written on arrival, so there is no working set to commit.
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
            "streamfusion-temporal-sort-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        RocksStoreConfig {
            table_dir: dir.to_string_lossy().into_owned(),
            max_parallelism: 1,
            options_json: options_json(),
            ttl_ms: 0,
            shared_resources: 0,
        }
    }

    fn snapshot_dir(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "streamfusion-temporal-sort-{name}-snapshot-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir.to_string_lossy().into_owned()
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("rt", DataType::Int64, false),
        ]))
    }

    fn batch(values: &[i64], times: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(values.to_vec())),
                Arc::new(Int64Array::from(times.to_vec())),
            ],
        )
        .unwrap()
    }

    fn store_sorter(name: &str) -> TemporalSorter {
        let store = RocksTemporalSortBuffer::create(test_config(name), schema()).unwrap();
        TemporalSorter::new(1).with_store(store)
    }

    // The firing is the memory path's exactly: ascending rowtime, ties in arrival order, the rest
    // kept buffered.
    #[test]
    fn store_backed_flush_matches_the_memory_path_exactly() {
        let mut memory = TemporalSorter::new(1);
        let mut rocks = store_sorter("order");
        for sorter in [&mut memory, &mut rocks] {
            sorter.push(batch(&[10, 20, 30], &[300, 100, 200])).unwrap();
            sorter.push(batch(&[40, 50], &[100, 400])).unwrap();
        }
        let expected = memory.flush(200).unwrap();
        let actual = rocks.flush(200).unwrap();
        assert_eq!(expected, actual);
        assert_eq!(expected.num_rows(), 3);
        assert_eq!(memory.flush(150).unwrap(), rocks.flush(150).unwrap());
        assert_eq!(memory.flush(500).unwrap(), rocks.flush(500).unwrap());
    }

    // The canonical export is the memory snapshot's own plain-IPC blob, restorable by the memory
    // sorter, which then continues identically.
    #[test]
    fn canonical_snapshot_transitions_back_to_the_memory_path() {
        let mut rocks = store_sorter("canonical");
        rocks.push(batch(&[10, 20], &[300, 100])).unwrap();
        let blob = rocks.store_snapshot().unwrap();
        let mut memory = TemporalSorter::restore(1, &blob);
        assert_eq!(memory.flush(400).unwrap(), rocks.flush(400).unwrap());
        assert!(rocks.store_snapshot().unwrap().is_empty());
    }

    #[test]
    fn restore_continues_the_buffer_and_sequences() {
        let snapshot = snapshot_dir("restore");
        let mut memory = TemporalSorter::new(1);
        let mut before = store_sorter("restore");
        for sorter in [&mut memory, &mut before] {
            sorter.push(batch(&[10, 20], &[300, 100])).unwrap();
        }
        assert_eq!(memory.flush(100).unwrap(), before.flush(100).unwrap());
        let manifest = before.store_mut().checkpoint(&snapshot).unwrap();
        drop(before);

        let store = RocksTemporalSortBuffer::open_merged(
            test_config("restore-reopen"),
            schema(),
            &[(snapshot, manifest.snapshot_id)],
            0..=0,
            true,
        )
        .unwrap();
        let mut restored = TemporalSorter::new(1).with_store(store);
        for sorter in [&mut memory, &mut restored] {
            sorter.push(batch(&[30, 40], &[300, 200])).unwrap();
        }
        assert_eq!(memory.flush(400).unwrap(), restored.flush(400).unwrap());
    }
}
