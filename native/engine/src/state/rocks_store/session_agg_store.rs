use super::{
    checkpoint_files, copy_checkpoint_db, merged_timer_deadline, open_shared_db, re,
    stored_timer_deadline, write_timer_deadline, FlinkWriteBatch, OpenedDb, TIMER_DEADLINE_KEY,
};
use crate::*;
use arrow::row::{RowConverter, SortField};
use rocksdb::{Cache, Direction, IteratorMode, Options, DB};
use std::sync::Arc;

/// `[key_group i32 BE][group key arrow-row bytes][session_start i64 BE, sign-flipped]` — the
/// layout of every open session's key. The key group leads so rescale clipping stays
/// layout-agnostic; the group key follows (unlike the aligned-window store's window-major order)
/// because ingest must see a key's neighboring sessions to merge them — hydrating a touched key is
/// one prefix scan over its handful of sessions; the start trails with its sign bit flipped so a
/// key's sessions iterate in start order. The arrow-row key encoding is prefix-free per schema, so
/// one key's prefix can never match into another key's entries.
const KEY_GROUP_LEN: usize = 4;
const SESSION_START_LEN: usize = 8;
const MIN_KEY_LEN: usize = KEY_GROUP_LEN + SESSION_START_LEN;
const SESSION_START_SIGN_FLIP: u64 = 1 << 63;

/// The session watermark, persisted at checkpoint under a reserved key whose leading bytes can
/// never be a subtask's key group (the snapshot-timer key's convention).
const WATERMARK_KEY: &[u8] = b"\xff\xff\xff\xffstreamfusion-session-agg-watermark";

fn biased_session_start(start: i64) -> [u8; 8] {
    ((start as u64) ^ SESSION_START_SIGN_FLIP).to_be_bytes()
}

fn session_start_from(bytes: &[u8]) -> i64 {
    (u64::from_be_bytes(bytes.try_into().expect("session start bytes")) ^ SESSION_START_SIGN_FLIP)
        as i64
}

/// One persisted session read back from the store: its bounds, the group key's arrow-row bytes
/// (the aggregator's own key encoding), and the accumulator state scalars in snapshot order.
pub(crate) struct StoredSession {
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) key: Box<[u8]>,
    pub(crate) state: Vec<ScalarValue>,
}

/// Persistent backend for the session-window aggregate: every committed session is one KV valued
/// as `[session_end i64 LE][accumulator state arrow-row bytes]`. A bundle hydrates each touched
/// key's full session list through one prefix scan (merging needs the neighbors), writes the
/// survivors back and tombstones merged-away starts at the bundle boundary, and a watermark firing
/// scans the open sessions — the same per-open-session cost as the memory path's firing walk.
/// Sessions close by watermark, so values carry no TTL prefix.
pub(crate) struct RocksSessionAggStore {
    db: Arc<DB>,
    _cache: Option<Cache>,
    max_parallelism: usize,
    key_groups: std::ops::RangeInclusive<i32>,
    state_converter: RowConverter,
    watermark: i64,
    timer_deadline: i64,
    generation: i64,
    write_batch_size: usize,
}

impl RocksSessionAggStore {
    pub(crate) fn create(
        config: RocksStoreConfig,
        state_types: &[DataType],
        key_groups: std::ops::RangeInclusive<i32>,
    ) -> Result<Self, DataFusionError> {
        if !rocks_row_supported(state_types) {
            return Err(DataFusionError::Plan(
                "session state shape not supported by RocksDB".into(),
            ));
        }
        let opened = open_shared_db(&config, &[(None, 0)])?;
        Self::attach(opened, &config, state_types, key_groups)
    }

    /// [`RocksSessionAggStore::create`] over restored checkpoint directories: an aligned single
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
            store.timer_deadline = stored_timer_deadline(&store.db)?;
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

    /// Adopts the deadline a blob import restored (it arrives with the blobs, not from a store
    /// checkpoint); the next checkpoint persists it under the reserved key.
    pub(crate) fn adopt_restored(&mut self, timer_deadline: i64) {
        self.timer_deadline = self.timer_deadline.max(timer_deadline);
    }

    /// The Flink key group of a group key's BinaryRow hash — identical routing to the blob path's
    /// raw keyed-state partitioner.
    pub(crate) fn key_group(&self, binary_row_hash: i32) -> i32 {
        flink_key_group(binary_row_hash, self.max_parallelism) as i32
    }

    /// The scan prefix owning every session of one key.
    pub(crate) fn key_prefix(&self, key_group: i32, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(KEY_GROUP_LEN + key.len());
        out.extend_from_slice(&key_group.to_be_bytes());
        out.extend_from_slice(key);
        out
    }

    pub(crate) fn db_key(&self, key_group: i32, key: &[u8], start: i64) -> Vec<u8> {
        let mut out = self.key_prefix(key_group, key);
        out.extend_from_slice(&biased_session_start(start));
        out
    }

    /// A bundle's touched keys hydrated in one pass: each key's committed sessions as
    /// `(start, end, state)` in start order — one prefix scan per key, one columnar decode across
    /// all of them.
    pub(crate) fn sessions_for(
        &self,
        prefixes: &[Vec<u8>],
    ) -> Result<Vec<Vec<(i64, i64, Vec<ScalarValue>)>>, DataFusionError> {
        let mut counts = Vec::with_capacity(prefixes.len());
        let mut bounds: Vec<(i64, i64)> = Vec::new();
        let mut raw_states: Vec<Box<[u8]>> = Vec::new();
        for prefix in prefixes {
            let mut count = 0usize;
            for row in self
                .db
                .iterator(IteratorMode::From(prefix, Direction::Forward))
            {
                let (key, value) = row.map_err(re)?;
                if !key.starts_with(prefix) {
                    break;
                }
                bounds.push((
                    session_start_from(&key[key.len() - SESSION_START_LEN..]),
                    i64::from_le_bytes(value[..8].try_into().expect("session end")),
                ));
                raw_states.push(value[8..].into());
                count += 1;
            }
            counts.push(count);
        }
        let refs: Vec<&[u8]> = raw_states.iter().map(AsRef::as_ref).collect();
        let mut sessions = bounds
            .into_iter()
            .zip(self.decode_states(&refs)?)
            .map(|((start, end), state)| (start, end, state));
        Ok(counts
            .into_iter()
            .map(|count| sessions.by_ref().take(count).collect())
            .collect())
    }

    /// Writes a bundle's touched keys through in one columnar conversion: the merged-away starts'
    /// tombstones and every surviving session — Flink's write path, one memtable write per touched
    /// session per bundle.
    pub(crate) fn write(
        &mut self,
        deletes: &[Vec<u8>],
        entries: &[(Vec<u8>, i64)],
        state_columns: &[ArrayRef],
    ) -> Result<(), DataFusionError> {
        if deletes.is_empty() && entries.is_empty() {
            return Ok(());
        }
        let rows = self
            .state_converter
            .convert_columns(state_columns)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for db_key in deletes {
            writes.delete(db_key)?;
        }
        for ((db_key, end), row) in entries.iter().zip(rows.iter()) {
            let row = row.data();
            let mut value = Vec::with_capacity(8 + row.len());
            value.extend_from_slice(&end.to_le_bytes());
            value.extend_from_slice(row);
            writes.put(db_key, value)?;
        }
        writes.finish()
    }

    /// Removes and returns every closed session (`session_end <= watermark`). Sessions are
    /// key-major, so the scan visits every open session — the memory path's firing walks every
    /// open session per watermark too — and the result is re-sorted to group key order, then
    /// start order: the memory path's emission order.
    pub(crate) fn take_closed(
        &mut self,
        watermark: i64,
    ) -> Result<Vec<StoredSession>, DataFusionError> {
        let mut closed: Vec<(Box<[u8]>, i64, i64, Box<[u8]>)> = Vec::new();
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for key_group in self.key_groups.clone() {
            let prefix = key_group.to_be_bytes();
            for row in self
                .db
                .iterator(IteratorMode::From(&prefix, Direction::Forward))
            {
                let (key, value) = row.map_err(re)?;
                if key.len() < MIN_KEY_LEN || key[..KEY_GROUP_LEN] != prefix {
                    break;
                }
                let end = i64::from_le_bytes(value[..8].try_into().expect("session end"));
                if end > watermark {
                    continue;
                }
                let start = session_start_from(&key[key.len() - SESSION_START_LEN..]);
                closed.push((
                    key[KEY_GROUP_LEN..key.len() - SESSION_START_LEN].into(),
                    start,
                    end,
                    value[8..].into(),
                ));
                writes.delete(key)?;
            }
        }
        writes.finish()?;
        closed.sort_unstable_by(|a, b| (a.0.as_ref(), a.1).cmp(&(b.0.as_ref(), b.1)));
        self.into_sessions(closed)
    }

    /// Every committed session, for canonical savepoints.
    pub(crate) fn scan_all(&self) -> Result<Vec<StoredSession>, DataFusionError> {
        let mut sessions: Vec<(Box<[u8]>, i64, i64, Box<[u8]>)> = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.as_ref() == WATERMARK_KEY
                || key.as_ref() == TIMER_DEADLINE_KEY
                || key.len() < MIN_KEY_LEN
            {
                continue;
            }
            sessions.push((
                key[KEY_GROUP_LEN..key.len() - SESSION_START_LEN].into(),
                session_start_from(&key[key.len() - SESSION_START_LEN..]),
                i64::from_le_bytes(value[..8].try_into().expect("session end")),
                value[8..].into(),
            ));
        }
        self.into_sessions(sessions)
    }

    fn into_sessions(
        &self,
        rows: Vec<(Box<[u8]>, i64, i64, Box<[u8]>)>,
    ) -> Result<Vec<StoredSession>, DataFusionError> {
        let states: Vec<&[u8]> = rows.iter().map(|(_, _, _, state)| state.as_ref()).collect();
        let states = self.decode_states(&states)?;
        Ok(rows
            .into_iter()
            .zip(states)
            .map(|((key, start, end, _), state)| StoredSession {
                start,
                end,
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

    /// Persists the late-data watermark, then takes one native checkpoint — touched keys were
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
            "streamfusion-session-agg-store-{name}-{}",
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
            "streamfusion-session-agg-store-{name}-snapshot-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir.to_string_lossy().into_owned()
    }

    fn store(name: &str) -> RocksSessionAggStore {
        RocksSessionAggStore::create(test_config(name), &[DataType::Int64], 0..=127).unwrap()
    }

    fn key_rows(keys: &[i64]) -> Vec<Vec<u8>> {
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let column: ArrayRef = Arc::new(Int64Array::from(keys.to_vec()));
        let rows = converter.convert_columns(&[column]).unwrap();
        (0..keys.len())
            .map(|i| rows.row(i).data().to_vec())
            .collect()
    }

    fn put_sessions(store: &mut RocksSessionAggStore, sessions: &[(i32, i64, i64, i64, i64)]) {
        let keys = key_rows(&sessions.iter().map(|s| s.3).collect::<Vec<_>>());
        let entries: Vec<(Vec<u8>, i64)> = sessions
            .iter()
            .zip(&keys)
            .map(|(&(kg, start, end, _, _), key)| (store.db_key(kg, key, start), end))
            .collect();
        let sums: ArrayRef = Arc::new(Int64Array::from(
            sessions.iter().map(|s| s.4).collect::<Vec<_>>(),
        ));
        store.write(&[], &entries, &[sums]).unwrap();
    }

    fn sums(sessions: &[StoredSession]) -> Vec<i64> {
        sessions
            .iter()
            .map(|session| {
                if let ScalarValue::Int64(Some(v)) = session.state[0] {
                    v
                } else {
                    panic!("int64 state")
                }
            })
            .collect()
    }

    #[test]
    fn prefix_reads_return_a_key_full_session_list_and_miss_cleanly() {
        let mut store = store("get");
        put_sessions(
            &mut store,
            &[
                (7, 0, 1000, 1, 10),
                (7, 2000, 3000, 1, 20),
                (9, 0, 1000, 2, 30),
            ],
        );
        let keys = key_rows(&[1, 2, 3]);
        let fetched = store
            .sessions_for(&[
                store.key_prefix(7, &keys[0]),
                store.key_prefix(9, &keys[1]),
                store.key_prefix(7, &keys[2]),
            ])
            .unwrap();
        assert_eq!(
            fetched[0]
                .iter()
                .map(|(start, end, _)| (*start, *end))
                .collect::<Vec<_>>(),
            vec![(0, 1000), (2000, 3000)]
        );
        assert_eq!(fetched[0][0].2, vec![ScalarValue::Int64(Some(10))]);
        assert_eq!(fetched[1][0].2, vec![ScalarValue::Int64(Some(30))]);
        assert!(fetched[2].is_empty());
    }

    #[test]
    fn tombstones_remove_merged_away_starts() {
        let mut store = store("tombstone");
        put_sessions(&mut store, &[(7, 0, 1000, 1, 10), (7, 2000, 3000, 1, 20)]);
        let key = &key_rows(&[1])[0];
        let merged: ArrayRef = Arc::new(Int64Array::from(vec![30i64]));
        store
            .write(
                &[store.db_key(7, key, 2000)],
                &[(store.db_key(7, key, 0), 3000)],
                &[merged],
            )
            .unwrap();
        let all = store.scan_all().unwrap();
        assert_eq!(
            all.iter().map(|s| (s.start, s.end)).collect::<Vec<_>>(),
            vec![(0, 3000)]
        );
        assert_eq!(sums(&all), vec![30]);
    }

    // The scan is key-group-major; the fired set must come back in group-key order, then start
    // order — the memory path's emission order — and pending sessions must stay put.
    #[test]
    fn firing_sorts_by_key_then_start_and_keeps_pending() {
        let mut store = store("order");
        put_sessions(
            &mut store,
            &[
                (9, 500, 1500, 2, 20),
                (7, 0, 1000, 1, 10),
                (7, 2000, 3000, 1, 40),
                (11, 100, 900, 3, 30),
            ],
        );
        let fired = store.take_closed(1500).unwrap();
        assert_eq!(sums(&fired), vec![10, 20, 30]);
        assert_eq!(
            fired.iter().map(|s| (s.start, s.end)).collect::<Vec<_>>(),
            vec![(0, 1000), (500, 1500), (100, 900)]
        );
        assert!(store.take_closed(1500).unwrap().is_empty());

        let pending = store.take_closed(3000).unwrap();
        assert_eq!(sums(&pending), vec![40]);
    }

    fn session_batch(ts: &[i64], keys: &[i64], values: &[i64]) -> RecordBatch {
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

    fn store_backed_aggregator(name: &str) -> SessionAggregator {
        let store = RocksSessionAggStore::create(
            test_config(name),
            &window_state_types(&[0], &[0]),
            0..=127,
        )
        .unwrap();
        SessionAggregator::new(1000, vec![0], vec![0])
            .with_key_timestamp_precisions(vec![-1])
            .with_store(store, vec![DataType::Int64])
    }

    // The store-backed aggregator must emit byte-identical firings to the memory path, including
    // when a bundle's rows bridge two sessions committed by earlier bundles into one.
    #[test]
    fn store_backed_aggregator_matches_the_memory_path() {
        let mut memory = SessionAggregator::new(1000, vec![0], vec![0]);
        let mut rocks = store_backed_aggregator("agg-parity");
        for batch in [
            session_batch(&[0, 2500, 100], &[1, 1, 2], &[10, 20, 5]),
            session_batch(&[1600, 300], &[1, 2], &[3, 7]),
            session_batch(&[900], &[1], &[1]),
        ] {
            memory.update(&batch).unwrap();
            rocks.update(&batch).unwrap();
        }
        assert_eq!(memory.flush(2000).unwrap(), rocks.flush(2000).unwrap());
        assert_eq!(memory.flush(4000).unwrap(), rocks.flush(4000).unwrap());
        assert_eq!(memory.flush(5000).unwrap(), rocks.flush(5000).unwrap());
    }

    // A later bundle's row bridges two committed sessions: the survivor keeps the earlier start
    // and the consumed start's entry is tombstoned, so the merged session fires exactly once.
    #[test]
    fn cross_bundle_merge_tombstones_the_consumed_start() {
        let mut rocks = store_backed_aggregator("agg-merge");
        rocks
            .update(&session_batch(&[0, 2000], &[1, 1], &[10, 20]))
            .unwrap();
        rocks.update(&session_batch(&[1000], &[1], &[3])).unwrap();
        let out = rocks.flush(5000).unwrap();
        assert_eq!(out.num_rows(), 1);
        let starts = column_i64(&out, "window_start");
        let ends = column_i64(&out, "window_end");
        assert_eq!((starts.value(0), ends.value(0)), (0, 3000));
        let sums = column_i64(&out, "result0");
        assert_eq!(sums.value(0), 33);
    }

    // A canonical savepoint of the store-backed aggregator is the memory path's own raw keyed
    // encoding, so it restores into a memory aggregator that continues identically.
    #[test]
    fn canonical_partitions_transition_back_to_the_memory_path() {
        let mut rocks = store_backed_aggregator("agg-canonical");
        rocks
            .update(&session_batch(&[0, 2500, 100], &[1, 1, 2], &[10, 3, 4]))
            .unwrap();
        let snapshots: Vec<Vec<u8>> = rocks
            .canonical_partitions(128, &[-1])
            .unwrap()
            .into_values()
            .collect();
        let mut memory = SessionAggregator::restore_partitions(1000, vec![0], vec![0], &snapshots);
        assert_eq!(memory.flush(2000).unwrap(), rocks.flush(2000).unwrap());
        assert_eq!(memory.flush(4000).unwrap(), rocks.flush(4000).unwrap());
    }

    // Pending sessions and the late-data watermark survive a native checkpoint: the restored
    // aggregator drops rows for already-fired sessions, extends open ones from stored state, and
    // still accepts a late row that merges into an open session.
    #[test]
    fn store_backed_aggregator_restores_pending_sessions_and_watermark() {
        let snapshot = snapshot_dir("agg-restore");
        let mut before = store_backed_aggregator("agg-restore");
        before
            .update(&session_batch(&[0, 2500], &[1, 1], &[10, 3]))
            .unwrap();
        let fired = before.flush(1000).unwrap();
        assert_eq!(fired.num_rows(), 1);
        let manifest = before.checkpoint_store(i64::MIN, &snapshot).unwrap();
        drop(before);

        let store = RocksSessionAggStore::open_merged(
            test_config("agg-restore-reopen"),
            &window_state_types(&[0], &[0]),
            0..=127,
            &[(snapshot, manifest.snapshot_id)],
            true,
        )
        .unwrap();
        let mut restored = SessionAggregator::new(1000, vec![0], vec![0])
            .with_key_timestamp_precisions(vec![-1])
            .with_store(store, vec![DataType::Int64]);

        restored
            .update(&session_batch(&[0, 2600], &[1, 1], &[99, 4]))
            .unwrap();
        assert_eq!(restored.late_drops, 1);

        let out = restored.flush(4000).unwrap();
        assert_eq!(out.num_rows(), 1);
        let sums = column_i64(&out, "result0");
        assert_eq!(sums.value(0), 7);
    }

    #[test]
    fn checkpoint_persists_the_watermark_and_restore_clips_key_groups() {
        let snapshot = snapshot_dir("restore");
        let mut store = store("restore");
        put_sessions(&mut store, &[(7, 0, 1000, 1, 10), (9, 0, 1000, 2, 20)]);
        let manifest = store.checkpoint(5000, i64::MIN, &snapshot).unwrap();
        drop(store);

        let restored = RocksSessionAggStore::open_merged(
            test_config("restore-aligned"),
            &[DataType::Int64],
            0..=127,
            &[(snapshot.clone(), manifest.snapshot_id)],
            true,
        )
        .unwrap();
        assert_eq!(restored.watermark(), 5000);
        assert_eq!(sums(&restored.scan_all().unwrap()), vec![10, 20]);

        let clipped = RocksSessionAggStore::open_merged(
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
