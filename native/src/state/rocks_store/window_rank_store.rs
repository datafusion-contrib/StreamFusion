use super::{
    checkpoint_files, copy_checkpoint_db, merged_timer_deadline, open_shared_db, re,
    stored_timer_deadline, write_timer_deadline, FlinkWriteBatch, OpenedDb, TIMER_DEADLINE_KEY,
};
use crate::*;
use arrow::row::{RowConverter, SortField};
use rocksdb::{Cache, Direction, IteratorMode, Options, DB};
use std::sync::Arc;

/// `[key_group i32 BE][window_end i64 BE, sign-flipped][window_start i64 BE][partition key
/// arrow-row bytes]` — the layout of every open (window, key) group's key. The key group leads so
/// rescale clipping stays layout-agnostic and a firing can seek each group's range; the window end
/// follows with its sign bit flipped so byte order equals numeric order and a key group's entries
/// iterate in window-end order; the window start and the partition key's arrow-row encoding trail
/// to complete the group identity.
const KEY_PREFIX_LEN: usize = 20;
const WINDOW_END_SIGN_FLIP: u64 = 1 << 63;

/// The window-rank watermark, persisted at checkpoint under a reserved key whose leading bytes can
/// never be a subtask's key group (the snapshot-timer key's convention).
const WATERMARK_KEY: &[u8] = b"\xff\xff\xff\xffstreamfusion-window-rank-watermark";

fn biased_window_end(end: i64) -> [u8; 8] {
    ((end as u64) ^ WINDOW_END_SIGN_FLIP).to_be_bytes()
}

fn window_end_from(bytes: &[u8]) -> i64 {
    (u64::from_be_bytes(bytes.try_into().expect("window end bytes")) ^ WINDOW_END_SIGN_FLIP) as i64
}

/// One persisted (window, key) group read back from the store: the Flink key group it was routed
/// to and the group's ranked buffer in stored order (the window bounds and partition key already
/// ride inside every buffered row).
pub(crate) struct StoredRankGroup {
    pub(crate) key_group: i32,
    pub(crate) rows: Vec<JoinRow>,
}

/// Persistent backend for the window rank / window dedup operator: every open (window, key)
/// group's ranked top-N buffer is one KV valued as the whole-list raw layout of the Top-N codecs —
/// `[n_rows: u32 LE]` then one length-framed arrow-row per buffered row, in rank order. The ranker
/// hydrates the groups a bundle touches through batched point reads, writes the touched groups
/// back at the bundle boundary, and a watermark firing range-scans each key group's closed prefix.
/// Windows close by watermark, so values carry no TTL prefix.
pub(crate) struct RocksWindowRankStore {
    db: Arc<DB>,
    _cache: Option<Cache>,
    max_parallelism: usize,
    key_groups: std::ops::RangeInclusive<i32>,
    row_converter: RowConverter,
    row_types: Vec<DataType>,
    watermark: i64,
    timer_deadline: i64,
    generation: i64,
    write_batch_size: usize,
}

impl RocksWindowRankStore {
    pub(crate) fn create(
        config: RocksStoreConfig,
        row_types: &[DataType],
        key_groups: std::ops::RangeInclusive<i32>,
    ) -> Result<Self, DataFusionError> {
        if !rocks_row_supported(row_types) {
            return Err(DataFusionError::Plan(
                "window-rank row shape not supported by RocksDB".into(),
            ));
        }
        let opened = open_shared_db(&config, &[(None, 0)])?;
        Self::attach(opened, &config, row_types, key_groups)
    }

    /// [`RocksWindowRankStore::create`] over restored checkpoint directories: an aligned single
    /// source adopts the files wholesale; anything else clips rows by this subtask's key groups.
    /// The restored watermark is the max across sources, matching the blob path's merge.
    pub(crate) fn open_merged(
        config: RocksStoreConfig,
        row_types: &[DataType],
        key_groups: std::ops::RangeInclusive<i32>,
        sources: &[(String, i64)],
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        if aligned && sources.len() == 1 {
            copy_checkpoint_db(&sources[0].0, &config.table_dir)?;
            let mut store = Self::create(config, row_types, key_groups)?;
            store.generation = sources[0].1;
            store.watermark = store
                .db
                .get(WATERMARK_KEY)
                .map_err(re)?
                .filter(|bytes| bytes.len() == 8)
                .map(|bytes| i64::from_be_bytes(bytes[..8].try_into().unwrap()))
                .unwrap_or(i64::MIN);
            store.timer_deadline = stored_timer_deadline(&store.db)?;
            return Ok(store);
        }
        let mut store = Self::create(config, row_types, key_groups)?;
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
                } else if key.as_ref() == TIMER_DEADLINE_KEY {
                    store.timer_deadline = merged_timer_deadline(store.timer_deadline, &value);
                } else if key.len() >= 4 {
                    let kg = i32::from_be_bytes(key[..4].try_into().expect("key group prefix"));
                    if store.key_groups.contains(&kg) {
                        writes.put(key, value)?;
                    }
                }
            }
        }
        write_timer_deadline(&mut writes, store.timer_deadline)?;
        writes.finish()?;
        Ok(store)
    }

    fn attach(
        opened: OpenedDb,
        config: &RocksStoreConfig,
        row_types: &[DataType],
        key_groups: std::ops::RangeInclusive<i32>,
    ) -> Result<Self, DataFusionError> {
        let row_converter = RowConverter::new(
            row_types
                .iter()
                .map(|data_type| SortField::new(data_type.clone()))
                .collect(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Self {
            db: opened.db,
            _cache: opened.cache,
            max_parallelism: config.max_parallelism,
            key_groups,
            row_converter,
            row_types: row_types.to_vec(),
            watermark: i64::MIN,
            timer_deadline: i64::MIN,
            generation: 0,
            write_batch_size: opened.write_batch_size,
        })
    }

    /// The late-data watermark persisted by the checkpoint this store restored from.
    pub(crate) fn watermark(&self) -> i64 {
        self.watermark
    }

    /// The processing-time timer deadline persisted by the checkpoint this store restored from.
    pub(crate) fn timer_deadline(&self) -> i64 {
        self.timer_deadline
    }

    /// The Flink key group of a partition key's BinaryRow hash — identical routing to the blob
    /// path's raw keyed-state partitioner.
    pub(crate) fn key_group(&self, binary_row_hash: i32) -> i32 {
        flink_key_group(binary_row_hash, self.max_parallelism) as i32
    }

    pub(crate) fn db_key(
        &self,
        key_group: i32,
        window_end: i64,
        window_start: i64,
        key: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(KEY_PREFIX_LEN + key.len());
        out.extend_from_slice(&key_group.to_be_bytes());
        out.extend_from_slice(&biased_window_end(window_end));
        out.extend_from_slice(&window_start.to_be_bytes());
        out.extend_from_slice(key);
        out
    }

    /// Batched point reads for a bundle's touched groups: one multi-get, one columnar decode.
    /// `None` marks a group with no committed buffer.
    pub(crate) fn get(
        &self,
        db_keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<JoinRow>>>, DataFusionError> {
        let fetched = self.db.multi_get(db_keys);
        let mut values = Vec::with_capacity(fetched.len());
        for value in fetched {
            values.push(value.map_err(re)?);
        }
        let hits: Vec<&[u8]> = values.iter().flatten().map(Vec::as_slice).collect();
        let mut buffers = self.decode_buffers(&hits)?.into_iter();
        Ok(values
            .iter()
            .map(|value| {
                value
                    .as_ref()
                    .map(|_| buffers.next().expect("decoded buffer"))
            })
            .collect())
    }

    /// Writes a bundle's touched groups through in one columnar conversion — Flink's write path,
    /// one memtable write per touched (window, key) per bundle.
    pub(crate) fn put(
        &mut self,
        entries: Vec<(Vec<u8>, Vec<JoinRow>)>,
    ) -> Result<(), DataFusionError> {
        if entries.is_empty() {
            return Ok(());
        }
        let flat: Vec<&JoinRow> = entries.iter().flat_map(|(_, rows)| rows).collect();
        let columns: Vec<ArrayRef> = self
            .row_types
            .iter()
            .enumerate()
            .map(|(column, data_type)| {
                scalars_to_array(
                    flat.iter().map(|row| row[column].clone()).collect(),
                    data_type,
                )
            })
            .collect();
        let rows = self
            .row_converter
            .convert_columns(&columns)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        let mut next = 0;
        for (db_key, buffer) in &entries {
            let mut value = (buffer.len() as u32).to_le_bytes().to_vec();
            for _ in 0..buffer.len() {
                let bytes = rows.row(next).data();
                value.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                value.extend_from_slice(bytes);
                next += 1;
            }
            writes.put(db_key, value)?;
        }
        writes.finish()
    }

    /// Removes and returns every closed (window, key) group (`window_end <= watermark`). Each key
    /// group's scan stops at its first pending window (entries iterate in window-end order within
    /// a key group), and the key-group-major result is re-sorted to window-end order, then window
    /// start, then partition-key byte order — a deterministic instance of the memory path's
    /// (window_end, window_start)-major emission order.
    pub(crate) fn take_closed(
        &mut self,
        watermark: i64,
    ) -> Result<Vec<StoredRankGroup>, DataFusionError> {
        let mut closed: Vec<(i64, i64, i32, Box<[u8]>, Box<[u8]>)> = Vec::new();
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for key_group in self.key_groups.clone() {
            let prefix = key_group.to_be_bytes();
            for row in self
                .db
                .iterator(IteratorMode::From(&prefix, Direction::Forward))
            {
                let (key, value) = row.map_err(re)?;
                if key.len() < KEY_PREFIX_LEN || key[..4] != prefix {
                    break;
                }
                let end = window_end_from(&key[4..12]);
                if end > watermark {
                    break;
                }
                let start = i64::from_be_bytes(key[12..KEY_PREFIX_LEN].try_into().unwrap());
                closed.push((
                    end,
                    start,
                    key_group,
                    key[KEY_PREFIX_LEN..].into(),
                    value.into(),
                ));
                writes.delete(key)?;
            }
        }
        writes.finish()?;
        closed.sort_unstable_by(|a, b| (a.0, a.1, a.3.as_ref()).cmp(&(b.0, b.1, b.3.as_ref())));
        self.into_groups(closed)
    }

    /// Every committed (window, key) group, for canonical savepoints.
    pub(crate) fn scan_all(&self) -> Result<Vec<StoredRankGroup>, DataFusionError> {
        let mut groups: Vec<(i64, i64, i32, Box<[u8]>, Box<[u8]>)> = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.as_ref() == WATERMARK_KEY
                || key.as_ref() == TIMER_DEADLINE_KEY
                || key.len() < KEY_PREFIX_LEN
            {
                continue;
            }
            let key_group = i32::from_be_bytes(key[..4].try_into().unwrap());
            let end = window_end_from(&key[4..12]);
            let start = i64::from_be_bytes(key[12..KEY_PREFIX_LEN].try_into().unwrap());
            groups.push((
                end,
                start,
                key_group,
                key[KEY_PREFIX_LEN..].into(),
                value.into(),
            ));
        }
        self.into_groups(groups)
    }

    fn into_groups(
        &self,
        groups: Vec<(i64, i64, i32, Box<[u8]>, Box<[u8]>)>,
    ) -> Result<Vec<StoredRankGroup>, DataFusionError> {
        let values: Vec<&[u8]> = groups
            .iter()
            .map(|(_, _, _, _, value)| &value[..])
            .collect();
        let buffers = self.decode_buffers(&values)?;
        Ok(groups
            .into_iter()
            .zip(buffers)
            .map(|((_, _, key_group, _, _), rows)| StoredRankGroup { key_group, rows })
            .collect())
    }

    /// Decodes a set of whole-list values back to their buffers in one columnar conversion,
    /// preserving each buffer's stored (rank) order.
    fn decode_buffers(&self, values: &[&[u8]]) -> Result<Vec<Vec<JoinRow>>, DataFusionError> {
        let mut counts = Vec::with_capacity(values.len());
        let mut row_slices: Vec<&[u8]> = Vec::new();
        for value in values {
            let mut cursor = *value;
            let count = read_u32(&mut cursor);
            counts.push(count);
            for _ in 0..count {
                let len = read_u32(&mut cursor);
                let (row, rest) = cursor.split_at(len);
                row_slices.push(row);
                cursor = rest;
            }
        }
        if row_slices.is_empty() {
            return Ok(counts.iter().map(|_| Vec::new()).collect());
        }
        let parser = self.row_converter.parser();
        let parsed: Vec<_> = row_slices.iter().map(|bytes| parser.parse(bytes)).collect();
        let columns = self
            .row_converter
            .convert_rows(parsed)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut buffers = Vec::with_capacity(counts.len());
        let mut next = 0;
        for count in counts {
            let mut rows = Vec::with_capacity(count);
            for _ in 0..count {
                let mut scalars = Vec::with_capacity(columns.len());
                for column in &columns {
                    scalars.push(ScalarValue::try_from_array(column, next)?);
                }
                rows.push(scalars);
                next += 1;
            }
            buffers.push(rows);
        }
        Ok(buffers)
    }

    /// Persists the late-data watermark, then takes one native checkpoint — touched groups were
    /// already written at their bundle boundaries, so there is no working set to commit.
    pub(crate) fn checkpoint(
        &mut self,
        watermark: i64,
        timer_deadline: i64,
        snapshot_dir: &str,
    ) -> Result<RocksCheckpointManifest, DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        writes.put(WATERMARK_KEY, watermark.to_be_bytes())?;
        write_timer_deadline(&mut writes, timer_deadline)?;
        writes.finish()?;
        self.watermark = watermark;
        self.timer_deadline = timer_deadline;
        if snapshot_dir.is_empty() {
            return Ok(RocksCheckpointManifest::absent());
        }
        self.generation += 1;
        checkpoint_files(&self.db, snapshot_dir, self.generation)
    }
}

fn read_u32(cursor: &mut &[u8]) -> usize {
    let (head, rest) = cursor.split_at(4);
    *cursor = rest;
    u32::from_le_bytes(head.try_into().expect("u32 field")) as usize
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
            "streamfusion-window-rank-store-{name}-{}",
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
            "streamfusion-window-rank-store-{name}-snapshot-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir.to_string_lossy().into_owned()
    }

    fn store(name: &str) -> RocksWindowRankStore {
        RocksWindowRankStore::create(
            test_config(name),
            &[DataType::Int64, DataType::Int64],
            0..=127,
        )
        .unwrap()
    }

    fn key_rows(keys: &[i64]) -> Vec<Vec<u8>> {
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let column: ArrayRef = Arc::new(Int64Array::from(keys.to_vec()));
        let rows = converter.convert_columns(&[column]).unwrap();
        (0..keys.len())
            .map(|i| rows.row(i).data().to_vec())
            .collect()
    }

    fn buffer(rows: &[(i64, i64)]) -> Vec<JoinRow> {
        rows.iter()
            .map(|&(key, value)| {
                vec![
                    ScalarValue::Int64(Some(key)),
                    ScalarValue::Int64(Some(value)),
                ]
            })
            .collect()
    }

    fn put_groups(store: &mut RocksWindowRankStore, groups: &[(i32, i64, i64, i64, Vec<i64>)]) {
        let keys = key_rows(&groups.iter().map(|g| g.3).collect::<Vec<_>>());
        let entries: Vec<(Vec<u8>, Vec<JoinRow>)> = groups
            .iter()
            .zip(&keys)
            .map(|(&(kg, end, start, key, ref values), key_bytes)| {
                (
                    store.db_key(kg, end, start, key_bytes),
                    buffer(&values.iter().map(|&v| (key, v)).collect::<Vec<_>>()),
                )
            })
            .collect();
        store.put(entries).unwrap();
    }

    fn values(group: &StoredRankGroup) -> Vec<i64> {
        group
            .rows
            .iter()
            .map(|row| {
                if let ScalarValue::Int64(Some(v)) = row[1] {
                    v
                } else {
                    panic!("int64 value")
                }
            })
            .collect()
    }

    #[test]
    fn point_reads_round_trip_buffers_in_order_and_miss_cleanly() {
        let mut store = store("get");
        put_groups(
            &mut store,
            &[(7, 100, 0, 1, vec![30, 10, 20]), (9, 100, 0, 2, vec![5])],
        );
        let keys = key_rows(&[1, 2, 3]);
        let fetched = store
            .get(&[
                store.db_key(7, 100, 0, &keys[0]),
                store.db_key(9, 100, 0, &keys[1]),
                store.db_key(7, 100, 0, &keys[2]),
            ])
            .unwrap();
        let first = fetched[0].as_ref().unwrap();
        assert_eq!(
            first.iter().map(|row| row[1].clone()).collect::<Vec<_>>(),
            vec![
                ScalarValue::Int64(Some(30)),
                ScalarValue::Int64(Some(10)),
                ScalarValue::Int64(Some(20)),
            ]
        );
        assert_eq!(fetched[1].as_ref().unwrap().len(), 1);
        assert!(fetched[2].is_none());
    }

    // The scan is key-group-major; the fired set must come back (window_end, window_start)-major
    // with the partition keys in memcomparable order, and pending windows must stay put.
    #[test]
    fn firing_resorts_to_window_order_and_keeps_pending() {
        let mut store = store("order");
        put_groups(
            &mut store,
            &[
                (9, 200, 100, 1, vec![40]),
                (9, 100, 0, 2, vec![20]),
                (7, 100, 0, 1, vec![10]),
                (7, 300, 200, 1, vec![50]),
                (11, 100, 0, 3, vec![30]),
            ],
        );
        let fired = store.take_closed(200).unwrap();
        assert_eq!(
            fired.iter().flat_map(values).collect::<Vec<_>>(),
            vec![10, 20, 30, 40]
        );
        assert!(store.take_closed(200).unwrap().is_empty());

        let pending = store.take_closed(300).unwrap();
        assert_eq!(
            pending.iter().flat_map(values).collect::<Vec<_>>(),
            vec![50]
        );
    }

    fn rank_batch(ws: &[i64], we: &[i64], keys: &[i64], values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            rank_schema(),
            vec![
                Arc::new(Int64Array::from(ws.to_vec())),
                Arc::new(Int64Array::from(we.to_vec())),
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn rank_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn memory_ranker() -> WindowRanker {
        WindowRanker::new(
            0,
            1,
            vec![2],
            vec![SortColumn {
                index: 3,
                ascending: false,
                nulls_first: false,
            }],
            2,
            true,
        )
    }

    fn store_backed_ranker(name: &str) -> WindowRanker {
        let row_types: Vec<DataType> = rank_schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let store = RocksWindowRankStore::create(test_config(name), &row_types, 0..=127).unwrap();
        memory_ranker()
            .with_key_timestamp_precisions(vec![-1])
            .with_store(store, rank_schema())
    }

    fn sorted_rows(batch: &RecordBatch) -> Vec<Vec<String>> {
        let mut rows: Vec<Vec<String>> = (0..batch.num_rows())
            .map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        ScalarValue::try_from_array(column, row)
                            .unwrap()
                            .to_string()
                    })
                    .collect()
            })
            .collect();
        rows.sort();
        rows
    }

    // The store-backed ranker must emit the memory path's rows: same per-group ranking (including
    // the arrival tie-break across bundles), same late drops, same rank numbers.
    #[test]
    fn store_backed_ranker_matches_the_memory_path() {
        let mut memory = memory_ranker();
        let mut rocks = store_backed_ranker("rank-parity");
        for batch in [
            rank_batch(&[0, 0, 0], &[100, 100, 100], &[1, 2, 1], &[10, 20, 30]),
            rank_batch(&[0, 100], &[100, 200], &[1, 1], &[10, 5]),
        ] {
            memory.push(&batch).unwrap();
            rocks.push(&batch).unwrap();
        }
        let memory_out = memory.flush(100).unwrap();
        let rocks_out = rocks.flush(100).unwrap();
        assert_eq!(sorted_rows(&memory_out), sorted_rows(&rocks_out));
        assert_eq!(memory_out.num_rows(), 3);

        // A late row for the fired window drops on both paths.
        let late = rank_batch(&[0], &[100], &[1], &[99]);
        memory.push(&late).unwrap();
        rocks.push(&late).unwrap();
        assert_eq!(memory.late_drops, rocks.late_drops);
        assert_eq!(rocks.late_drops, 1);

        assert_eq!(
            sorted_rows(&memory.flush(200).unwrap()),
            sorted_rows(&rocks.flush(200).unwrap())
        );
    }

    // A canonical savepoint of the store-backed ranker is the memory path's own raw keyed
    // encoding, so it restores into a memory ranker that continues identically.
    #[test]
    fn canonical_partitions_transition_back_to_the_memory_path() {
        let mut rocks = store_backed_ranker("rank-canonical");
        rocks
            .push(&rank_batch(
                &[0, 0, 100],
                &[100, 100, 200],
                &[1, 1, 2],
                &[10, 30, 7],
            ))
            .unwrap();
        let snapshots: Vec<Vec<u8>> = rocks
            .canonical_partitions()
            .unwrap()
            .into_values()
            .collect();
        let mut memory = WindowRanker::restore_partitions(
            0,
            1,
            vec![2],
            vec![SortColumn {
                index: 3,
                ascending: false,
                nulls_first: false,
            }],
            2,
            true,
            &snapshots,
        );
        assert_eq!(
            sorted_rows(&memory.flush(200).unwrap()),
            sorted_rows(&rocks.flush(200).unwrap())
        );
    }

    // Pending windows and the late-data watermark survive a native checkpoint: the restored
    // ranker drops rows for already-fired windows and fires open ones from stored buffers.
    #[test]
    fn store_backed_ranker_restores_pending_windows_and_watermark() {
        let snapshot = snapshot_dir("rank-restore");
        let mut before = store_backed_ranker("rank-restore");
        before
            .push(&rank_batch(&[0, 100], &[100, 200], &[1, 1], &[10, 3]))
            .unwrap();
        assert_eq!(before.flush(100).unwrap().num_rows(), 1);
        let manifest = before.checkpoint_store(i64::MIN, &snapshot).unwrap();
        drop(before);

        let row_types: Vec<DataType> = rank_schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let store = RocksWindowRankStore::open_merged(
            test_config("rank-restore-reopen"),
            &row_types,
            0..=127,
            &[(snapshot, manifest.snapshot_id)],
            true,
        )
        .unwrap();
        let mut restored = memory_ranker()
            .with_key_timestamp_precisions(vec![-1])
            .with_store(store, rank_schema());

        restored
            .push(&rank_batch(&[0, 100], &[100, 200], &[1, 1], &[99, 4]))
            .unwrap();
        assert_eq!(restored.late_drops, 1);

        let out = restored.flush(200).unwrap();
        assert_eq!(out.num_rows(), 2);
        let values = out
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(values, vec![4, 3]);
    }

    #[test]
    fn checkpoint_persists_the_watermark_and_restore_clips_key_groups() {
        let snapshot = snapshot_dir("restore");
        let mut store = store("restore");
        put_groups(
            &mut store,
            &[(7, 100, 0, 1, vec![10]), (9, 100, 0, 2, vec![20])],
        );
        let manifest = store.checkpoint(5000, i64::MIN, &snapshot).unwrap();
        drop(store);

        let restored = RocksWindowRankStore::open_merged(
            test_config("restore-aligned"),
            &[DataType::Int64, DataType::Int64],
            0..=127,
            &[(snapshot.clone(), manifest.snapshot_id)],
            true,
        )
        .unwrap();
        assert_eq!(restored.watermark(), 5000);
        assert_eq!(
            restored
                .scan_all()
                .unwrap()
                .iter()
                .flat_map(values)
                .collect::<Vec<_>>(),
            vec![10, 20]
        );

        let clipped = RocksWindowRankStore::open_merged(
            test_config("restore-clipped"),
            &[DataType::Int64, DataType::Int64],
            9..=9,
            &[(snapshot, manifest.snapshot_id)],
            false,
        )
        .unwrap();
        assert_eq!(clipped.watermark(), 5000);
        assert_eq!(
            clipped
                .scan_all()
                .unwrap()
                .iter()
                .flat_map(values)
                .collect::<Vec<_>>(),
            vec![20]
        );
    }
}
