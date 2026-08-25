use super::{
    checkpoint_files, copy_checkpoint_db, merged_timer_deadline, open_shared_db, re,
    stored_timer_deadline, write_timer_deadline, FlinkWriteBatch, OpenedDb, PAIR_FIRST_TABLE,
    PAIR_SECOND_TABLE, TIMER_DEADLINE_KEY,
};
use crate::*;
use arrow::row::{RowConverter, SortField};
use rocksdb::Cache;
use rocksdb::{Direction, IteratorMode, Options, DB};
use std::sync::Arc;

/// `[key_group i32 BE][table u8][seq u64 BE]` — the fixed layout of every buffered row's key. The
/// key group leads so rescale clipping stays layout-agnostic and a probe can seek exactly the key
/// groups an incoming batch can match; the sequence trails so a scan yields each key group's rows
/// in arrival order. The sequence IS the joiner's outer-join row id, so it must stay unique within
/// a side.
const KEY_LEN: usize = 13;

/// The fixed prefix ahead of every value's arrow-row bytes: the row's eviction rowtime and its
/// outer-join matched flag, so a watermark firing and a match flip never decode payloads.
const VALUE_PREFIX_LEN: usize = 9;

/// Per-table sequence high-water marks, persisted at checkpoint under reserved keys whose leading
/// bytes can never be a subtask's key group (the snapshot-timer key's convention).
const SEQ_KEYS: [&[u8]; 2] = [
    b"\xff\xff\xff\xffstreamfusion-interval-seq-left",
    b"\xff\xff\xff\xffstreamfusion-interval-seq-right",
];

/// One buffered row read back from the store: its routing key group, its sequence (the joiner's
/// row id), the eviction rowtime and matched flag from the value prefix, and the arrow-row bytes.
pub(crate) struct BufferedIntervalRow {
    pub(crate) key_group: i32,
    pub(crate) seq: u64,
    pub(crate) rowtime: i64,
    pub(crate) matched: bool,
    pub(crate) row: Box<[u8]>,
}

/// Bespoke persistent buffer for the interval join: both sides' live rows are individual KVs in
/// one shared DB (left table 0, right table 1, one checkpoint manifest). A row appends on arrival
/// — the buffer IS RocksDB, with no resident working set — under a fresh sequence, routed by its
/// equi-join key's group, and valued as `[rowtime i64 LE][matched u8][arrow-row bytes]`. A push
/// probes only the key groups its batch hashes to, a match flips the flag with one re-put, and a
/// watermark firing splits expired from live rows on the value prefix alone. On restore new
/// sequences start above the persisted high-water marks, so restored and new row ids never
/// collide.
pub(crate) struct RocksIntervalBuffer {
    db: Arc<DB>,
    _cache: Option<Cache>,
    max_parallelism: usize,
    converters: (RowConverter, RowConverter),
    next_seq: [u64; 2],
    timer_deadline: i64,
    generation: i64,
    write_batch_size: usize,
}

impl RocksIntervalBuffer {
    pub(crate) fn create(
        config: RocksStoreConfig,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        let supported = |schema: &SchemaRef| {
            rocks_row_supported(
                &schema
                    .fields()
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect::<Vec<_>>(),
            )
        };
        if !supported(&left_schema) || !supported(&right_schema) {
            return Err(DataFusionError::Plan(
                "interval-join row shape not supported by RocksDB".into(),
            ));
        }
        let opened = open_shared_db(
            &config,
            &[(Some(PAIR_FIRST_TABLE), 0), (Some(PAIR_SECOND_TABLE), 0)],
        )?;
        Self::attach(opened, &config, left_schema, right_schema)
    }

    /// [`RocksIntervalBuffer::create`] over restored checkpoint directories. An aligned single
    /// source adopts the files wholesale and continues sequences above the persisted high-water
    /// marks. Anything else clips rows by this subtask's key groups and RE-SEQUENCES them: unlike
    /// the window join's buffer (where the sequence only orders), the sequence here is the
    /// outer-join row id, and independently-allocated ids from different source subtasks can
    /// collide — the same reason the memory path's restore remaps row ids. Iteration is key-ordered,
    /// so each (key group, table)'s relative arrival order survives the renumbering.
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
                if key.as_ref() == TIMER_DEADLINE_KEY {
                    buffer.timer_deadline = merged_timer_deadline(buffer.timer_deadline, &value);
                    continue;
                }
                if key.len() != KEY_LEN {
                    continue;
                }
                let kg = i32::from_be_bytes(key[..4].try_into().expect("key group prefix"));
                if !key_groups.contains(&kg) {
                    continue;
                }
                let table = key[4] as usize;
                let seq = buffer.next_seq[table];
                buffer.next_seq[table] += 1;
                let mut new_key = key.to_vec();
                new_key[5..].copy_from_slice(&seq.to_be_bytes());
                writes.put(new_key, value)?;
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

    /// The row id the next appended row of one side will take.
    pub(crate) fn next_row_id(&self, left: bool) -> i64 {
        self.next_seq[Self::table(left) as usize] as i64
    }

    /// The Flink key group of an equi-key's BinaryRow hash — identical routing to the blob path's
    /// raw keyed-state partitioner.
    pub(crate) fn key_group(&self, binary_row_hash: i32) -> i32 {
        flink_key_group(binary_row_hash, self.max_parallelism) as i32
    }

    /// Appends one side's rows: one KV per row in arrival order, through a WAL-off write batch in
    /// the same call, so RocksDB holds the buffer's only copy. Each row carries its final matched
    /// flag from the arrival probe, so only a LATER first match re-puts it.
    pub(crate) fn push(
        &mut self,
        left: bool,
        batch: &RecordBatch,
        key_columns: &[usize],
        key_timestamp_precisions: &[i32],
        rowtimes: &Int64Array,
        matched: &[bool],
    ) -> Result<(), DataFusionError> {
        let table = Self::table(left);
        let converter = self.converter(left);
        let rows = converter
            .convert_columns(batch.columns())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, key_timestamp_precisions);
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (index, row) in rows.iter().enumerate() {
            let key_group = flink_key_group(encoder.hash(index), self.max_parallelism) as i32;
            let seq = self.next_seq[table as usize];
            self.next_seq[table as usize] += 1;
            let row = row.data();
            let mut value = Vec::with_capacity(VALUE_PREFIX_LEN + row.len());
            value.extend_from_slice(&rowtimes.value(index).to_le_bytes());
            value.push(matched[index] as u8);
            value.extend_from_slice(row);
            writes.put(Self::db_key(key_group, table, seq), value)?;
        }
        writes.finish()
    }

    /// One side's rows in the given key groups — the only rows an incoming batch hashing to those
    /// groups can equi-match — re-sorted to sequence order, the memory path's buffer order.
    pub(crate) fn scan_groups(
        &self,
        left: bool,
        key_groups: &[i32],
    ) -> Result<Vec<BufferedIntervalRow>, DataFusionError> {
        let table = Self::table(left);
        let mut rows = Vec::new();
        for &key_group in key_groups {
            let mut prefix = [0u8; 5];
            prefix[..4].copy_from_slice(&key_group.to_be_bytes());
            prefix[4] = table;
            for row in self
                .db
                .iterator(IteratorMode::From(&prefix, Direction::Forward))
            {
                let (key, value) = row.map_err(re)?;
                if key.len() != KEY_LEN || key[..5] != prefix {
                    break;
                }
                rows.push(Self::buffered_row(&key, &value));
            }
        }
        rows.sort_unstable_by_key(|row| row.seq);
        Ok(rows)
    }

    /// Removes and returns one side's rows the watermark has retired (`keep(rowtime)` false), in
    /// sequence order — the memory path's eviction reads the same buffer order. Live rows stay put.
    pub(crate) fn take_expired(
        &mut self,
        left: bool,
        keep: impl Fn(i64) -> bool,
    ) -> Result<Vec<BufferedIntervalRow>, DataFusionError> {
        let table = Self::table(left);
        let mut expired = Vec::new();
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() != KEY_LEN || key[4] != table {
                continue;
            }
            let rowtime = i64::from_le_bytes(value[..8].try_into().expect("rowtime"));
            if !keep(rowtime) {
                expired.push(Self::buffered_row(&key, &value));
                writes.delete(key)?;
            }
        }
        writes.finish()?;
        expired.sort_unstable_by_key(|row| row.seq);
        Ok(expired)
    }

    /// Flips the matched flag of rows that gained their first match this push — one re-put per
    /// flipped row, the payload bytes unchanged.
    pub(crate) fn mark_matched(
        &mut self,
        left: bool,
        rows: &[&BufferedIntervalRow],
    ) -> Result<(), DataFusionError> {
        let table = Self::table(left);
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for row in rows {
            let mut value = Vec::with_capacity(VALUE_PREFIX_LEN + row.row.len());
            value.extend_from_slice(&row.rowtime.to_le_bytes());
            value.push(1);
            value.extend_from_slice(&row.row);
            writes.put(Self::db_key(row.key_group, table, row.seq), value)?;
        }
        writes.finish()
    }

    /// One side's full buffered contents per key group, each group's rows in sequence order, for
    /// canonical savepoints.
    pub(crate) fn rows_by_group(
        &self,
        left: bool,
    ) -> Result<BTreeMap<i32, Vec<BufferedIntervalRow>>, DataFusionError> {
        let table = Self::table(left);
        let mut rows_by_group: BTreeMap<i32, Vec<BufferedIntervalRow>> = BTreeMap::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (key, value) = row.map_err(re)?;
            if key.len() != KEY_LEN || key[4] != table {
                continue;
            }
            let row = Self::buffered_row(&key, &value);
            rows_by_group.entry(row.key_group).or_default().push(row);
        }
        Ok(rows_by_group)
    }

    /// Rebuilds buffered rows as a batch under the side's declared data schema, so reconstructed
    /// batches match what the memory path would have buffered.
    pub(crate) fn decode(
        &self,
        left: bool,
        schema: &SchemaRef,
        rows: &[&BufferedIntervalRow],
    ) -> Result<RecordBatch, DataFusionError> {
        let converter = self.converter(left);
        let parser = converter.parser();
        let parsed: Vec<_> = rows.iter().map(|row| parser.parse(&row.row)).collect();
        let columns = converter
            .convert_rows(parsed)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(RecordBatch::try_new(schema.clone(), columns)?)
    }

    /// Persists the sequence high-water marks, then takes one native checkpoint of the shared DB —
    /// rows were already written on arrival, so there is no working set to commit.
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

    fn buffered_row(key: &[u8], value: &[u8]) -> BufferedIntervalRow {
        BufferedIntervalRow {
            key_group: i32::from_be_bytes(key[..4].try_into().expect("key group")),
            seq: u64::from_be_bytes(key[5..].try_into().expect("sequence")),
            rowtime: i64::from_le_bytes(value[..8].try_into().expect("rowtime")),
            matched: value[8] != 0,
            row: value[VALUE_PREFIX_LEN..].into(),
        }
    }

    fn db_key(key_group: i32, table: u8, seq: u64) -> [u8; KEY_LEN] {
        let mut key = [0u8; KEY_LEN];
        key[..4].copy_from_slice(&key_group.to_be_bytes());
        key[4] = table;
        key[5..].copy_from_slice(&seq.to_be_bytes());
        key
    }

    fn converter(&self, left: bool) -> &RowConverter {
        if left {
            &self.converters.0
        } else {
            &self.converters.1
        }
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
            "streamfusion-interval-buffer-{name}-{}",
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
            "streamfusion-interval-buffer-{name}-snapshot-{}",
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

    fn buffer(name: &str) -> RocksIntervalBuffer {
        RocksIntervalBuffer::create(test_config(name), schema(), schema()).unwrap()
    }

    fn values(buffer: &RocksIntervalBuffer, rows: &[&BufferedIntervalRow]) -> Vec<i64> {
        if rows.is_empty() {
            return Vec::new();
        }
        let decoded = buffer.decode(true, &schema(), rows).unwrap();
        decoded
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn probe_scans_only_the_touched_key_groups_in_sequence_order() {
        let mut store = buffer("probe");
        let rows = batch(&[1, 2, 1], &[10, 20, 30]);
        store
            .push(
                true,
                &rows,
                &[0],
                &[-1],
                &Int64Array::from(vec![100, 200, 300]),
                &[false, false, false],
            )
            .unwrap();
        let group_of = |key: i64| {
            flink_key_group(binary_row_hash(&batch(&[key], &[0]), &[0], 0, &[-1]), 128) as i32
        };
        let scanned = store.scan_groups(true, &[group_of(1)]).unwrap();
        let mut expected = vec![10, 30];
        if group_of(2) == group_of(1) {
            expected = vec![10, 20, 30];
        }
        assert_eq!(
            values(&store, &scanned.iter().collect::<Vec<_>>()),
            expected
        );
        assert!(store.scan_groups(false, &[group_of(1)]).unwrap().is_empty());
    }

    #[test]
    fn eviction_takes_expired_rows_with_flags_and_keeps_live_ones() {
        let mut store = buffer("evict");
        store
            .push(
                true,
                &batch(&[1, 1, 1], &[10, 20, 30]),
                &[0],
                &[-1],
                &Int64Array::from(vec![100, 200, 300]),
                &[false, true, false],
            )
            .unwrap();
        let expired = store.take_expired(true, |rt| rt > 200).unwrap();
        assert_eq!(
            values(&store, &expired.iter().collect::<Vec<_>>()),
            vec![10, 20]
        );
        assert_eq!(
            expired.iter().map(|row| row.matched).collect::<Vec<_>>(),
            vec![false, true]
        );
        let remaining = store.take_expired(true, |_| false).unwrap();
        assert_eq!(
            values(&store, &remaining.iter().collect::<Vec<_>>()),
            vec![30]
        );
    }

    #[test]
    fn a_first_match_flips_the_flag_with_one_reput() {
        let mut store = buffer("match");
        store
            .push(
                true,
                &batch(&[1, 1], &[10, 20]),
                &[0],
                &[-1],
                &Int64Array::from(vec![100, 200]),
                &[false, false],
            )
            .unwrap();
        let scanned = store
            .scan_groups(
                true,
                &[
                    flink_key_group(binary_row_hash(&batch(&[1], &[0]), &[0], 0, &[-1]), 128)
                        as i32,
                ],
            )
            .unwrap();
        store.mark_matched(true, &[&scanned[1]]).unwrap();
        let expired = store.take_expired(true, |_| false).unwrap();
        assert_eq!(
            expired.iter().map(|row| row.matched).collect::<Vec<_>>(),
            vec![false, true]
        );
        assert_eq!(
            values(&store, &expired.iter().collect::<Vec<_>>()),
            vec![10, 20]
        );
    }

    fn timed_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("rt", DataType::Int64, false),
        ]))
    }

    fn timed_batch(keys: &[i64], values: &[i64], times: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            timed_schema(),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
                Arc::new(Int64Array::from(times.to_vec())),
            ],
        )
        .unwrap()
    }

    fn memory_joiner(join_type: JoinKind) -> IntervalJoiner {
        IntervalJoiner::new(
            vec![0],
            vec![0],
            2,
            2,
            -100,
            100,
            None,
            join_type,
            timed_schema(),
            timed_schema(),
        )
    }

    fn store_backed_joiner(name: &str, join_type: JoinKind) -> IntervalJoiner {
        let store =
            RocksIntervalBuffer::create(test_config(name), timed_schema(), timed_schema()).unwrap();
        memory_joiner(join_type)
            .with_key_timestamp_precisions(vec![-1])
            .with_store(store)
    }

    // The store-backed joiner must emit the memory path's batches, in the memory path's order:
    // each push probes the opposite buffer and the pairs come out of the same join.
    #[test]
    fn store_backed_inner_join_matches_the_memory_path() {
        let mut memory = memory_joiner(JoinKind::Inner);
        let mut rocks = store_backed_joiner("join-inner", JoinKind::Inner);
        let pushes: Vec<(bool, RecordBatch)> = vec![
            (true, timed_batch(&[1, 2], &[10, 20], &[100, 100])),
            (
                false,
                timed_batch(&[1, 1, 3], &[51, 52, 53], &[150, 400, 150]),
            ),
            (true, timed_batch(&[1], &[11], &[350])),
            (false, timed_batch(&[2], &[54], &[120])),
        ];
        for (left, batch) in pushes {
            let expected = if left {
                memory.push_left(batch.clone(), None).unwrap()
            } else {
                memory.push_right(batch.clone(), None).unwrap()
            };
            let actual = if left {
                rocks.push_left(batch, None).unwrap()
            } else {
                rocks.push_right(batch, None).unwrap()
            };
            assert_eq!(expected, actual);
        }
        assert_eq!(memory.advance(300).unwrap(), rocks.advance(300).unwrap());
        let batch = timed_batch(&[1], &[12], &[420]);
        assert_eq!(
            memory.push_left(batch.clone(), None).unwrap(),
            rocks.push_left(batch, None).unwrap()
        );
    }

    // Outer parity: match flags persist across pushes and evictions, so the null-padded rows for
    // never-matched retired rows come out identical (and only once).
    #[test]
    fn store_backed_full_outer_join_matches_the_memory_path() {
        let mut memory = memory_joiner(JoinKind::FullOuter);
        let mut rocks = store_backed_joiner("join-outer", JoinKind::FullOuter);
        let pushes: Vec<(bool, RecordBatch)> = vec![
            (true, timed_batch(&[1, 2], &[10, 20], &[100, 100])),
            (false, timed_batch(&[1, 4], &[51, 54], &[150, 150])),
            (true, timed_batch(&[1], &[11], &[350])),
        ];
        for (left, batch) in pushes {
            let expected = if left {
                memory.push_left(batch.clone(), None).unwrap()
            } else {
                memory.push_right(batch.clone(), None).unwrap()
            };
            let actual = if left {
                rocks.push_left(batch, None).unwrap()
            } else {
                rocks.push_right(batch, None).unwrap()
            };
            assert_eq!(expected, actual);
        }
        assert_eq!(memory.advance(300).unwrap(), rocks.advance(300).unwrap());
        assert_eq!(memory.advance(600).unwrap(), rocks.advance(600).unwrap());
    }

    // A canonical savepoint of the store-backed joiner is the memory path's own four-section
    // key-group encoding, so it restores into a memory joiner that continues identically.
    #[test]
    fn canonical_partitions_transition_back_to_the_memory_path() {
        let mut rocks = store_backed_joiner("join-canonical", JoinKind::FullOuter);
        rocks
            .push_left(timed_batch(&[1, 2], &[10, 20], &[100, 100]), None)
            .unwrap();
        rocks
            .push_right(timed_batch(&[1], &[51], &[150]), None)
            .unwrap();
        let snapshots: Vec<Vec<u8>> = rocks
            .canonical_partitions()
            .unwrap()
            .into_values()
            .collect();
        let mut memory = IntervalJoiner::restore_partitions(
            vec![0],
            vec![0],
            2,
            2,
            -100,
            100,
            None,
            JoinKind::FullOuter,
            timed_schema(),
            timed_schema(),
            &snapshots,
        );
        // Key 1 matched before the savepoint, so only key 2's left row null-pads on both paths.
        assert_eq!(memory.advance(600).unwrap(), rocks.advance(600).unwrap());
    }

    // Match flags survive a native checkpoint: a row matched before the restore never null-pads.
    #[test]
    fn store_backed_joiner_restores_matched_flags_and_row_ids() {
        let snapshot = snapshot_dir("join-restore");
        let mut before = store_backed_joiner("join-restore", JoinKind::LeftOuter);
        before
            .push_left(timed_batch(&[1, 2], &[10, 20], &[100, 100]), None)
            .unwrap();
        let pairs = before
            .push_right(timed_batch(&[1], &[51], &[150]), None)
            .unwrap();
        assert_eq!(pairs.num_rows(), 1);
        let manifest = before.store_mut().checkpoint(i64::MIN, &snapshot).unwrap();
        drop(before);

        let store = RocksIntervalBuffer::open_merged(
            test_config("join-restore-reopen"),
            timed_schema(),
            timed_schema(),
            &[(snapshot, manifest.snapshot_id)],
            0..=127,
            true,
        )
        .unwrap();
        let mut restored = memory_joiner(JoinKind::LeftOuter)
            .with_key_timestamp_precisions(vec![-1])
            .with_store(store);
        let pads = restored.advance(600).unwrap();
        assert_eq!(pads.num_rows(), 1);
        let padded_key = ScalarValue::try_from_array(pads.column(0), 0).unwrap();
        assert_eq!(padded_key, ScalarValue::Int64(Some(2)));
    }

    #[test]
    fn restore_continues_row_ids_above_the_high_water_mark() {
        let snapshot = snapshot_dir("restore");
        let mut store = buffer("restore");
        store
            .push(
                true,
                &batch(&[1, 1], &[10, 20]),
                &[0],
                &[-1],
                &Int64Array::from(vec![100, 100]),
                &[false, false],
            )
            .unwrap();
        let manifest = store.checkpoint(i64::MIN, &snapshot).unwrap();
        drop(store);

        let mut restored = RocksIntervalBuffer::open_merged(
            test_config("restore-reopen"),
            schema(),
            schema(),
            &[(snapshot, manifest.snapshot_id)],
            0..=127,
            true,
        )
        .unwrap();
        assert_eq!(restored.next_row_id(true), 2);
        restored
            .push(
                true,
                &batch(&[1], &[30]),
                &[0],
                &[-1],
                &Int64Array::from(vec![100]),
                &[false],
            )
            .unwrap();
        let all = restored.take_expired(true, |_| false).unwrap();
        assert_eq!(
            all.iter().map(|row| row.seq).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            values(&restored, &all.iter().collect::<Vec<_>>()),
            vec![10, 20, 30]
        );
    }

    #[test]
    fn unaligned_restore_clips_key_groups_and_resequences_row_ids() {
        let keys = [1i64, 2, 3, 4];
        let snapshot = snapshot_dir("clip");
        let mut store = buffer("clip");
        store
            .push(
                true,
                &batch(&keys, &[10, 20, 30, 40]),
                &[0],
                &[-1],
                &Int64Array::from(vec![100; keys.len()]),
                &[false; 4],
            )
            .unwrap();
        let manifest = store.checkpoint(i64::MIN, &snapshot).unwrap();
        drop(store);

        let target = flink_key_group(
            binary_row_hash(&batch(&keys[..1], &[10]), &[0], 0, &[-1]),
            128,
        ) as i32;
        let restored = RocksIntervalBuffer::open_merged(
            test_config("clip-reopen"),
            schema(),
            schema(),
            &[(snapshot, manifest.snapshot_id)],
            target..=target,
            false,
        )
        .unwrap();
        let kept = restored.rows_by_group(true).unwrap();
        let expected: Vec<i64> = keys
            .iter()
            .zip([10i64, 20, 30, 40])
            .filter(|&(key, _)| {
                flink_key_group(binary_row_hash(&batch(&[*key], &[0]), &[0], 0, &[-1]), 128) as i32
                    == target
            })
            .map(|(_, value)| value)
            .collect();
        let rows: Vec<&BufferedIntervalRow> = kept.values().flatten().collect();
        assert_eq!(values(&restored, &rows), expected);
        assert_eq!(
            rows.iter().map(|row| row.seq).collect::<Vec<_>>(),
            (0..rows.len() as u64).collect::<Vec<_>>()
        );
        assert_eq!(restored.next_row_id(true), rows.len() as i64);
    }
}
