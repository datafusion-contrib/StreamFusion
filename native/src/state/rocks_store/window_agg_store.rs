use super::{checkpoint_files, copy_checkpoint_db, open_shared_db, re, FlinkWriteBatch, OpenedDb};
use crate::*;
use arrow::row::{RowConverter, SortField};
use rocksdb::{Cache, Direction, IteratorMode, Options, DB};
use std::sync::Arc;

/// `[key_group i32 BE][window_end i64 BE, sign-flipped][group key arrow-row bytes]` — the layout of
/// every open (window, key) group's key. The key group leads so rescale clipping stays
/// layout-agnostic and a firing can seek each group's range; the window end follows with its sign
/// bit flipped so byte order equals numeric order and a key group's entries iterate in window-end
/// order; the group key trails as the aggregator's own memcomparable arrow-row encoding, so a
/// window's groups iterate in exactly the order the memory path emits.
const KEY_PREFIX_LEN: usize = 12;
const WINDOW_END_SIGN_FLIP: u64 = 1 << 63;

/// The aligned-window watermark, persisted at checkpoint under a reserved key whose leading bytes
/// can never be a subtask's key group (the snapshot-timer key's convention).
const WATERMARK_KEY: &[u8] = b"\xff\xff\xff\xffstreamfusion-window-agg-watermark";

fn biased_window_end(end: i64) -> [u8; 8] {
    ((end as u64) ^ WINDOW_END_SIGN_FLIP).to_be_bytes()
}

fn window_end_from(bytes: &[u8]) -> i64 {
    (u64::from_be_bytes(bytes.try_into().expect("window end bytes")) ^ WINDOW_END_SIGN_FLIP) as i64
}

/// One persisted (window, key) group read back from the store: the window bounds, the group key's
/// arrow-row bytes (the aggregator's own key encoding), and the accumulator state scalars in
/// snapshot order.
pub(crate) struct StoredWindowGroup {
    pub(crate) end: i64,
    pub(crate) start: i64,
    pub(crate) key: Box<[u8]>,
    pub(crate) state: Vec<ScalarValue>,
}

/// Persistent backend for the aligned-window aggregate (tumble/hop/cumulate): every committed
/// (window, key) group is one KV valued as `[window_start i64 LE][accumulator state arrow-row
/// bytes]`. The aggregator hydrates the groups a bundle touches through batched point reads, writes
/// the touched groups back at the bundle boundary, and a watermark firing range-scans each key
/// group's closed prefix. Windows close by watermark, so values carry no TTL prefix.
pub(crate) struct RocksWindowAggStore {
    db: Arc<DB>,
    _cache: Option<Cache>,
    max_parallelism: usize,
    key_groups: std::ops::RangeInclusive<i32>,
    state_converter: RowConverter,
    watermark: i64,
    generation: i64,
    write_batch_size: usize,
}

impl RocksWindowAggStore {
    pub(crate) fn create(
        config: RocksStoreConfig,
        state_types: &[DataType],
        key_groups: std::ops::RangeInclusive<i32>,
    ) -> Result<Self, DataFusionError> {
        if !rocks_row_supported(state_types) {
            return Err(DataFusionError::Plan(
                "window state shape not supported by RocksDB".into(),
            ));
        }
        let opened = open_shared_db(&config, &[(None, 0)])?;
        Self::attach(opened, &config, state_types, key_groups)
    }

    /// [`RocksWindowAggStore::create`] over restored checkpoint directories: an aligned single
    /// source adopts the files wholesale; anything else clips rows by this subtask's key groups.
    /// The restored watermark is the max across sources, matching the blob path's merge.
    pub(crate) fn open_merged(
        config: RocksStoreConfig,
        state_types: &[DataType],
        key_groups: std::ops::RangeInclusive<i32>,
        sources: &[(String, i64)],
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        if aligned && sources.len() == 1 {
            copy_checkpoint_db(&sources[0].0, &config.table_dir)?;
            let mut store = Self::create(config, state_types, key_groups)?;
            store.generation = sources[0].1;
            store.watermark = store
                .db
                .get(WATERMARK_KEY)
                .map_err(re)?
                .filter(|bytes| bytes.len() == 8)
                .map(|bytes| i64::from_be_bytes(bytes[..8].try_into().unwrap()))
                .unwrap_or(i64::MIN);
            return Ok(store);
        }
        let mut store = Self::create(config, state_types, key_groups)?;
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
                } else if key.len() >= 4 {
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
        key_groups: std::ops::RangeInclusive<i32>,
    ) -> Result<Self, DataFusionError> {
        let state_converter = RowConverter::new(
            state_types
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
            state_converter,
            watermark: i64::MIN,
            generation: 0,
            write_batch_size: opened.write_batch_size,
        })
    }

    /// The late-data watermark persisted by the checkpoint this store restored from.
    pub(crate) fn watermark(&self) -> i64 {
        self.watermark
    }

    /// The Flink key group of a group key's BinaryRow hash — identical routing to the blob path's
    /// raw keyed-state partitioner.
    pub(crate) fn key_group(&self, binary_row_hash: i32) -> i32 {
        flink_key_group(binary_row_hash, self.max_parallelism) as i32
    }

    pub(crate) fn db_key(&self, key_group: i32, window_end: i64, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(KEY_PREFIX_LEN + key.len());
        out.extend_from_slice(&key_group.to_be_bytes());
        out.extend_from_slice(&biased_window_end(window_end));
        out.extend_from_slice(key);
        out
    }

    /// Batched point reads for a bundle's touched groups: one multi-get, one columnar decode.
    /// `None` marks a group with no committed state.
    pub(crate) fn get(
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
            .map(|value| &value.as_slice()[8..])
            .collect();
        let mut states = self.decode_states(&hits)?.into_iter();
        Ok(values
            .iter()
            .map(|value| {
                value.as_ref().map(|value| {
                    let start = i64::from_le_bytes(value[..8].try_into().expect("window start"));
                    (start, states.next().expect("decoded state"))
                })
            })
            .collect())
    }

    /// Writes a bundle's touched groups through in one columnar conversion — Flink's write path,
    /// one memtable write per touched (window, key) per bundle.
    pub(crate) fn put(
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
        for ((db_key, start), row) in entries.iter().zip(rows.iter()) {
            let row = row.data();
            let mut value = Vec::with_capacity(8 + row.len());
            value.extend_from_slice(&start.to_le_bytes());
            value.extend_from_slice(row);
            writes.put(db_key, value)?;
        }
        writes.finish()
    }

    /// Removes and returns every closed (window, key) group (`window_end <= watermark`). Each key
    /// group's scan stops at its first pending window (entries iterate in window-end order within
    /// a key group), and the key-group-major result is re-sorted to window-end order, then group
    /// key order — the memory path's emission order.
    pub(crate) fn take_closed(
        &mut self,
        watermark: i64,
    ) -> Result<Vec<StoredWindowGroup>, DataFusionError> {
        let mut closed: Vec<(i64, i64, Box<[u8]>, Box<[u8]>)> = Vec::new();
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
                let end = window_end_from(&key[4..KEY_PREFIX_LEN]);
                if end > watermark {
                    break;
                }
                let start = i64::from_le_bytes(value[..8].try_into().expect("window start"));
                closed.push((end, start, key[KEY_PREFIX_LEN..].into(), value[8..].into()));
                writes.delete(key)?;
            }
        }
        writes.finish()?;
        closed.sort_unstable_by(|a, b| (a.0, a.2.as_ref()).cmp(&(b.0, b.2.as_ref())));
        self.into_groups(closed)
    }

    /// Every committed (window, key) group, for canonical savepoints.
    pub(crate) fn scan_all(&self) -> Result<Vec<StoredWindowGroup>, DataFusionError> {
        let mut groups: Vec<(i64, i64, Box<[u8]>, Box<[u8]>)> = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.as_ref() == WATERMARK_KEY || key.len() < KEY_PREFIX_LEN {
                continue;
            }
            let end = window_end_from(&key[4..KEY_PREFIX_LEN]);
            let start = i64::from_le_bytes(value[..8].try_into().expect("window start"));
            groups.push((end, start, key[KEY_PREFIX_LEN..].into(), value[8..].into()));
        }
        self.into_groups(groups)
    }

    fn into_groups(
        &self,
        rows: Vec<(i64, i64, Box<[u8]>, Box<[u8]>)>,
    ) -> Result<Vec<StoredWindowGroup>, DataFusionError> {
        let states: Vec<&[u8]> = rows.iter().map(|(_, _, _, state)| state.as_ref()).collect();
        let states = self.decode_states(&states)?;
        Ok(rows
            .into_iter()
            .zip(states)
            .map(|((end, start, key, _), state)| StoredWindowGroup {
                end,
                start,
                key,
                state,
            })
            .collect())
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

    /// Persists the late-data watermark, then takes one native checkpoint — touched groups were
    /// already written at their bundle boundaries, so there is no working set to commit.
    pub(crate) fn checkpoint(
        &mut self,
        watermark: i64,
        snapshot_dir: &str,
    ) -> Result<RocksCheckpointManifest, DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        writes.put(WATERMARK_KEY, watermark.to_be_bytes())?;
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
            "streamfusion-window-agg-store-{name}-{}",
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
            "streamfusion-window-agg-store-{name}-snapshot-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir.to_string_lossy().into_owned()
    }

    fn store(name: &str) -> RocksWindowAggStore {
        RocksWindowAggStore::create(test_config(name), &[DataType::Int64], 0..=127).unwrap()
    }

    fn key_rows(keys: &[i64]) -> Vec<Vec<u8>> {
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let column: ArrayRef = Arc::new(Int64Array::from(keys.to_vec()));
        let rows = converter.convert_columns(&[column]).unwrap();
        (0..keys.len())
            .map(|i| rows.row(i).data().to_vec())
            .collect()
    }

    fn put_groups(store: &mut RocksWindowAggStore, groups: &[(i32, i64, i64, i64, i64)]) {
        let keys = key_rows(&groups.iter().map(|g| g.3).collect::<Vec<_>>());
        let entries: Vec<(Vec<u8>, i64)> = groups
            .iter()
            .zip(&keys)
            .map(|(&(kg, end, start, _, _), key)| (store.db_key(kg, end, key), start))
            .collect();
        let sums: ArrayRef = Arc::new(Int64Array::from(
            groups.iter().map(|g| g.4).collect::<Vec<_>>(),
        ));
        store.put(&entries, &[sums]).unwrap();
    }

    fn sums(groups: &[StoredWindowGroup]) -> Vec<i64> {
        groups
            .iter()
            .map(|group| {
                if let ScalarValue::Int64(Some(v)) = group.state[0] {
                    v
                } else {
                    panic!("int64 state")
                }
            })
            .collect()
    }

    #[test]
    fn point_reads_round_trip_and_miss_cleanly() {
        let mut store = store("get");
        put_groups(&mut store, &[(7, 100, 0, 1, 10), (9, 100, 0, 2, 20)]);
        let keys = key_rows(&[1, 2, 3]);
        let fetched = store
            .get(&[
                store.db_key(7, 100, &keys[0]),
                store.db_key(9, 100, &keys[1]),
                store.db_key(7, 100, &keys[2]),
            ])
            .unwrap();
        assert_eq!(fetched[0].as_ref().unwrap().0, 0);
        assert_eq!(
            fetched[0].as_ref().unwrap().1,
            vec![ScalarValue::Int64(Some(10))]
        );
        assert_eq!(
            fetched[1].as_ref().unwrap().1,
            vec![ScalarValue::Int64(Some(20))]
        );
        assert!(fetched[2].is_none());
    }

    // The scan is key-group-major; the fired set must come back window-end-major, then in the
    // group keys' memcomparable order — the memory path's emission order.
    #[test]
    fn firing_resorts_to_window_end_then_key_order_and_keeps_pending() {
        let mut store = store("order");
        put_groups(
            &mut store,
            &[
                (9, 200, 100, 1, 40),
                (9, 100, 0, 2, 20),
                (7, 100, 0, 1, 10),
                (7, 300, 200, 1, 50),
                (11, 100, 0, 3, 30),
            ],
        );
        let fired = store.take_closed(200).unwrap();
        assert_eq!(
            fired.iter().map(|g| g.end).collect::<Vec<_>>(),
            vec![100, 100, 100, 200]
        );
        assert_eq!(sums(&fired), vec![10, 20, 30, 40]);
        assert_eq!(
            fired.iter().map(|g| g.start).collect::<Vec<_>>(),
            vec![0, 0, 0, 100]
        );
        assert!(store.take_closed(200).unwrap().is_empty());

        let pending = store.take_closed(300).unwrap();
        assert_eq!(sums(&pending), vec![50]);
    }

    fn keyed_window_batch(ts: &[i64], keys: &[i64], values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("ts", DataType::Int64, false),
                Field::new("value0", DataType::Int64, true),
                Field::new("key0", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(ts.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
                Arc::new(Int64Array::from(keys.to_vec())),
            ],
        )
        .unwrap()
    }

    fn store_backed_aggregator(name: &str) -> TumblingAggregator {
        let store = RocksWindowAggStore::create(
            test_config(name),
            &window_state_types(&[0], &[0]),
            0..=127,
        )
        .unwrap();
        TumblingAggregator::new(1000, 1000, false, vec![0], vec![0])
            .with_key_timestamp_precisions(vec![-1])
            .with_store(store, vec![DataType::Int64])
    }

    // The store-backed aggregator must emit byte-identical firings to the memory path: same
    // grouping, same accumulator round trip, same window-end-then-key emission order.
    #[test]
    fn store_backed_aggregator_matches_the_memory_path() {
        let mut memory = TumblingAggregator::new(1000, 1000, false, vec![0], vec![0]);
        let mut rocks = store_backed_aggregator("agg-parity");
        for batch in [
            keyed_window_batch(&[0, 0, 500], &[2, 1, 1], &[10, 20, 5]),
            keyed_window_batch(&[700, 1500], &[3, 1], &[7, 1]),
        ] {
            memory.update(&batch).unwrap();
            rocks.update(&batch).unwrap();
        }
        assert_eq!(memory.flush(1000).unwrap(), rocks.flush(1000).unwrap());
        assert_eq!(memory.flush(2000).unwrap(), rocks.flush(2000).unwrap());
        assert_eq!(memory.flush(3000).unwrap(), rocks.flush(3000).unwrap());
    }

    #[test]
    fn store_backed_global_merge_matches_the_memory_path_for_hopping_windows() {
        let partial = |keys: &[i64], partials: &[i64], slice_ends: &[i64]| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("key0", DataType::Int64, false),
                    Field::new("partial0", DataType::Int64, true),
                    Field::new("slice_end", DataType::Int64, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(keys.to_vec())),
                    Arc::new(Int64Array::from(partials.to_vec())),
                    Arc::new(Int64Array::from(slice_ends.to_vec())),
                ],
            )
            .unwrap()
        };
        let mut memory = TumblingAggregator::new(2000, 1000, false, vec![0], vec![0]);
        let store = RocksWindowAggStore::create(
            test_config("agg-global"),
            &window_state_types(&[0], &[0]),
            0..=127,
        )
        .unwrap();
        let mut rocks = TumblingAggregator::new(2000, 1000, false, vec![0], vec![0])
            .with_key_timestamp_precisions(vec![-1])
            .with_store(store, vec![DataType::Int64]);
        for batch in [
            partial(&[1, 2], &[10, 3], &[1000, 1000]),
            partial(&[1], &[5], &[2000]),
        ] {
            memory.update_partial(&batch).unwrap();
            rocks.update_partial(&batch).unwrap();
        }
        assert_eq!(memory.flush(2000).unwrap(), rocks.flush(2000).unwrap());
        assert_eq!(memory.flush(3000).unwrap(), rocks.flush(3000).unwrap());
    }

    // A canonical savepoint of the store-backed aggregator is the memory path's own raw keyed
    // encoding, so it restores into a memory aggregator that continues identically.
    #[test]
    fn canonical_partitions_transition_back_to_the_memory_path() {
        let mut rocks = store_backed_aggregator("agg-canonical");
        rocks
            .update(&keyed_window_batch(
                &[0, 700, 1500],
                &[1, 2, 1],
                &[10, 3, 4],
            ))
            .unwrap();
        let snapshots: Vec<Vec<u8>> = rocks
            .canonical_partitions(128, &[-1])
            .unwrap()
            .into_values()
            .collect();
        let mut memory =
            TumblingAggregator::restore_partitions(1000, 1000, false, vec![0], vec![0], &snapshots);
        assert_eq!(memory.flush(2000).unwrap(), rocks.flush(2000).unwrap());
    }

    // Pending windows and the late-data watermark survive a native checkpoint: the restored
    // aggregator drops rows for already-fired windows and finishes open ones from stored state.
    #[test]
    fn store_backed_aggregator_restores_pending_windows_and_watermark() {
        let snapshot = snapshot_dir("agg-restore");
        let mut before = store_backed_aggregator("agg-restore");
        before
            .update(&keyed_window_batch(&[0, 1500], &[1, 1], &[10, 3]))
            .unwrap();
        let fired = before.flush(1000).unwrap();
        assert_eq!(fired.num_rows(), 1);
        let manifest = before.checkpoint_store(&snapshot).unwrap();
        drop(before);

        let store = RocksWindowAggStore::open_merged(
            test_config("agg-restore-reopen"),
            &window_state_types(&[0], &[0]),
            0..=127,
            &[(snapshot, manifest.snapshot_id)],
            true,
        )
        .unwrap();
        let mut restored = TumblingAggregator::new(1000, 1000, false, vec![0], vec![0])
            .with_key_timestamp_precisions(vec![-1])
            .with_store(store, vec![DataType::Int64]);

        restored
            .update(&keyed_window_batch(&[0, 1600], &[1, 1], &[99, 4]))
            .unwrap();
        assert_eq!(restored.late_drops, 1);

        let out = restored.flush(2000).unwrap();
        assert_eq!(out.num_rows(), 1);
        let sums = out
            .column_by_name("result0")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(sums, vec![7]);
    }

    #[test]
    fn checkpoint_persists_the_watermark_and_restore_clips_key_groups() {
        let snapshot = snapshot_dir("restore");
        let mut store = store("restore");
        put_groups(&mut store, &[(7, 100, 0, 1, 10), (9, 100, 0, 2, 20)]);
        let manifest = store.checkpoint(5000, &snapshot).unwrap();
        drop(store);

        let restored = RocksWindowAggStore::open_merged(
            test_config("restore-aligned"),
            &[DataType::Int64],
            0..=127,
            &[(snapshot.clone(), manifest.snapshot_id)],
            true,
        )
        .unwrap();
        assert_eq!(restored.watermark(), 5000);
        assert_eq!(sums(&restored.scan_all().unwrap()), vec![10, 20]);

        let clipped = RocksWindowAggStore::open_merged(
            test_config("restore-clipped"),
            &[DataType::Int64],
            9..=9,
            &[(snapshot, manifest.snapshot_id)],
            false,
        )
        .unwrap();
        assert_eq!(clipped.watermark(), 5000);
        assert_eq!(sums(&clipped.scan_all().unwrap()), vec![20]);
    }
}
