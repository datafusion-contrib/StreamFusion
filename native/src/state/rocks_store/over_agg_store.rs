use super::{
    checkpoint_files, copy_checkpoint_db, open_shared_db, prefix_successor, re, FlinkWriteBatch,
    OpenedDb, PAIR_FIRST_TABLE, PAIR_SECOND_TABLE,
};
use crate::*;
use arrow::row::{RowConverter, SortField};
use rocksdb::{Cache, Direction, IteratorMode, Options, DB};
use std::sync::Arc;

/// The OVER aggregate's DB holds one table per state shape, every key led by the PARTITION BY
/// key's group (BinaryRow-hash routed — identical to the blob partitioner) so rescale clipping is
/// layout-agnostic. Folds — the per-key running fold state of the unbounded shapes — key as
/// `[key_group i32 BE][0][partition key arrow-row bytes]`, valued `[stamp i64 LE][state arrow-row
/// bytes]`: the retention stamp (a cleanup deadline for the rowtime shapes, the last-write clock
/// for the proctime per-value TTL; i64::MIN while retention is off) rides as a fixed value prefix,
/// mirroring how the raw snapshots ride it as a trailing per-key column. Pending — the buffered
/// input rows a watermark has not completed — key as `[key_group i32 BE][1][arrival_seq u64 BE]`
/// (the window join buffer's layout), valued `[rowtime_millis i64 LE][input row arrow-row bytes]`,
/// so a firing splits complete from pending rows without decoding payloads. Frames — the bounded
/// shapes' per-key row buffers — key as `[key_group i32 BE][2][key arrow-row][rt i64 BE,
/// sign-flipped][seq u64 BE]`, valued `[per-aggregate values arrow-row bytes]`: byte order equals
/// the memory buffer's (rowtime ascending, arrival order for ties), so a key's prefix scan IS its
/// sorted buffer. Stamps — the bounded shapes' per-key cleanup deadlines (they have no fold row to
/// carry one) — key as `[key_group i32 BE][3][key arrow-row]`, valued `[deadline i64 LE]`.
/// Distinct seen-sets — one table per DISTINCT aggregate of the unbounded fold — key as
/// `[key_group i32 BE][4 + slot][key arrow-row][element arrow-row]` with an empty value: OVER
/// input is insert-only, so presence alone gates the fold (no multiplicity).
const FOLDS_TABLE: u8 = PAIR_FIRST_TABLE;
const PENDING_TABLE: u8 = PAIR_SECOND_TABLE;
const FRAMES_TABLE: u8 = 2;
const STAMPS_TABLE: u8 = 3;
const DISTINCT_TABLE_BASE: u8 = 4;
const KEY_GROUP_LEN: usize = 4;
const KEY_PREFIX_LEN: usize = 5;
const PENDING_KEY_LEN: usize = 13;
const STAMP_LEN: usize = 8;
const FRAME_SUFFIX_LEN: usize = 16;
const RT_SIGN_FLIP: u64 = 1 << 63;

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

/// One persisted bounded-frame row read back from the store: its position in the key's sorted
/// buffer (rowtime, then arrival sequence for ties) and the per-aggregate value scalars.
pub(crate) struct StoredFrameRow {
    pub(crate) rt: i64,
    pub(crate) seq: u64,
    pub(crate) values: Vec<ScalarValue>,
}

/// Persistent backend for the OVER aggregate, every admitted shape. Rowtime: input rows append to
/// the pending table on arrival (the buffer IS RocksDB, with no resident copy), a watermark
/// firing removes and returns the completed rows in arrival order, and the fired keys' state —
/// the running fold, or a bounded frame's buffer — hydrates from its table for exactly those keys
/// and writes back at the bundle boundary (a frame write-back is the diff: appended survivors
/// insert, evicted rows delete; an untouched key's frame evicts lazily on its next touch).
/// Proctime: no pending table — each eager push is one bundle over the same layouts, ordered by
/// the persisted arrival counter. Retention is enforced by the operator's lazy schemes — a
/// compaction filter cannot honor the pending-row deferral — so no table installs one.
pub(crate) struct RocksOverAggStore {
    db: Arc<DB>,
    _cache: Option<Cache>,
    max_parallelism: usize,
    key_groups: std::ops::RangeInclusive<i32>,
    /// `None` for the bounded shapes, whose per-key state is the frames table, not a fold row.
    state_converter: Option<RowConverter>,
    /// `Some` for the bounded shapes: the frame rows' per-aggregate value columns.
    frames_converter: Option<RowConverter>,
    frame_value_types: Vec<DataType>,
    /// One element codec per DISTINCT aggregate slot, in gate order.
    distinct_converters: Vec<RowConverter>,
    distinct_element_types: Vec<DataType>,
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
        frame_value_types: &[DataType],
        distinct_element_types: &[DataType],
        payload_schema: SchemaRef,
        key_groups: std::ops::RangeInclusive<i32>,
    ) -> Result<Self, DataFusionError> {
        let payload_types: Vec<DataType> = payload_schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        if !rocks_row_supported(state_types)
            || !rocks_row_supported(frame_value_types)
            || !rocks_row_supported(distinct_element_types)
            || !rocks_row_supported(&payload_types)
        {
            return Err(DataFusionError::Plan(
                "over state shape not supported by RocksDB".into(),
            ));
        }
        let mut tables = vec![
            (Some(FOLDS_TABLE), 0),
            (Some(PENDING_TABLE), 0),
            (Some(FRAMES_TABLE), 0),
            (Some(STAMPS_TABLE), 0),
        ];
        for slot in 0..distinct_element_types.len() {
            tables.push((Some(DISTINCT_TABLE_BASE + slot as u8), 0));
        }
        let opened = open_shared_db(&config, &tables)?;
        Self::attach(
            opened,
            &config,
            state_types,
            frame_value_types,
            distinct_element_types,
            payload_schema,
            key_groups,
        )
    }

    /// [`RocksOverAggStore::create`] over restored checkpoint directories: an aligned single
    /// source adopts the files wholesale; anything else clips rows by this subtask's key groups.
    /// The restored watermark and sequence high-water mark are each the max across sources.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_merged(
        config: RocksStoreConfig,
        state_types: &[DataType],
        frame_value_types: &[DataType],
        distinct_element_types: &[DataType],
        payload_schema: SchemaRef,
        key_groups: std::ops::RangeInclusive<i32>,
        sources: &[(String, i64)],
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        if aligned && sources.len() == 1 {
            copy_checkpoint_db(&sources[0].0, &config.table_dir)?;
            let mut store = Self::create(
                config,
                state_types,
                frame_value_types,
                distinct_element_types,
                payload_schema,
                key_groups,
            )?;
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
        let mut store = Self::create(
            config,
            state_types,
            frame_value_types,
            distinct_element_types,
            payload_schema,
            key_groups,
        )?;
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
        frame_value_types: &[DataType],
        distinct_element_types: &[DataType],
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
            state_converter: (!state_types.is_empty())
                .then(|| converter(state_types))
                .transpose()?,
            frames_converter: (!frame_value_types.is_empty())
                .then(|| converter(frame_value_types))
                .transpose()?,
            frame_value_types: frame_value_types.to_vec(),
            distinct_converters: distinct_element_types
                .iter()
                .map(|element| converter(std::slice::from_ref(element)))
                .collect::<Result<_, _>>()?,
            distinct_element_types: distinct_element_types.to_vec(),
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
            .as_ref()
            .expect("over fold codec")
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

    /// The `[key_group][FRAMES][key]` prefix under which one key's frame buffer lives.
    pub(crate) fn frame_prefix(&self, key_group: i32, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(KEY_PREFIX_LEN + key.len());
        out.extend_from_slice(&key_group.to_be_bytes());
        out.push(FRAMES_TABLE);
        out.extend_from_slice(key);
        out
    }

    fn frame_db_key(prefix: &[u8], rt: i64, seq: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(prefix.len() + FRAME_SUFFIX_LEN);
        out.extend_from_slice(prefix);
        out.extend_from_slice(&((rt as u64) ^ RT_SIGN_FLIP).to_be_bytes());
        out.extend_from_slice(&seq.to_be_bytes());
        out
    }

    /// Allocates `count` fresh arrival sequences from the persisted high-water mark — the frame
    /// rows' tie-breaking order and the proctime shapes' ordering key.
    pub(crate) fn allocate_seqs(&mut self, count: usize) -> u64 {
        let start = self.next_seq;
        self.next_seq += count as u64;
        start
    }

    /// The persisted arrival-sequence high-water mark (the proctime ordering counter).
    pub(crate) fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Floors the arrival counter at a restored blob's high-water mark, so sequences assigned
    /// after a canonical import stay above every imported one.
    pub(crate) fn adopt_next_seq(&mut self, floor: u64) {
        self.next_seq = self.next_seq.max(floor);
    }

    /// One prefix scan per touched key: the key's buffered frame rows in buffer order (rowtime
    /// ascending, arrival sequence for ties), decoded in a single columnar pass across all keys.
    pub(crate) fn load_frames(
        &self,
        prefixes: &[Vec<u8>],
    ) -> Result<Vec<Vec<StoredFrameRow>>, DataFusionError> {
        let mut owners: Vec<usize> = Vec::new();
        let mut positions: Vec<(i64, u64)> = Vec::new();
        let mut values: Vec<Box<[u8]>> = Vec::new();
        for (owner, prefix) in prefixes.iter().enumerate() {
            for row in self
                .db
                .iterator(IteratorMode::From(prefix, Direction::Forward))
            {
                let (db_key, value) = row.map_err(re)?;
                if !db_key.starts_with(prefix) {
                    break;
                }
                let suffix = &db_key[db_key.len() - FRAME_SUFFIX_LEN..];
                let rt = (u64::from_be_bytes(suffix[..8].try_into().expect("frame rt"))
                    ^ RT_SIGN_FLIP) as i64;
                let seq = u64::from_be_bytes(suffix[8..].try_into().expect("frame seq"));
                owners.push(owner);
                positions.push((rt, seq));
                values.push(value.into());
            }
        }
        let value_refs: Vec<&[u8]> = values.iter().map(|value| value.as_ref()).collect();
        let decoded = Self::decode_rows(
            self.frames_converter.as_ref().expect("over frame codec"),
            &value_refs,
        )?;
        let mut out: Vec<Vec<StoredFrameRow>> = (0..prefixes.len()).map(|_| Vec::new()).collect();
        for ((owner, (rt, seq)), values) in owners.into_iter().zip(positions).zip(decoded) {
            out[owner].push(StoredFrameRow { rt, seq, values });
        }
        Ok(out)
    }

    /// Writes appended frame rows through in one columnar conversion; `entries` gives each row's
    /// key prefix and buffer position, `value_columns` its per-aggregate values in entry order.
    pub(crate) fn write_frames(
        &mut self,
        entries: &[(Vec<u8>, i64, u64)],
        value_columns: &[ArrayRef],
    ) -> Result<(), DataFusionError> {
        if entries.is_empty() {
            return Ok(());
        }
        let rows = self
            .frames_converter
            .as_ref()
            .expect("over frame codec")
            .convert_columns(value_columns)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for ((prefix, rt, seq), row) in entries.iter().zip(rows.iter()) {
            writes.put(Self::frame_db_key(prefix, *rt, *seq), row.data())?;
        }
        writes.finish()
    }

    /// Deletes evicted frame rows (rows no future frame can reach) by exact position.
    pub(crate) fn delete_frames(
        &mut self,
        entries: &[(Vec<u8>, i64, u64)],
    ) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (prefix, rt, seq) in entries {
            writes.delete(Self::frame_db_key(prefix, *rt, *seq))?;
        }
        writes.finish()
    }

    /// Every persisted frame row, grouped per partition key in buffer order — canonical
    /// savepoints and restore-time retention hydration.
    pub(crate) fn scan_frames(
        &self,
    ) -> Result<Vec<(Box<[u8]>, Vec<StoredFrameRow>)>, DataFusionError> {
        let mut keys: Vec<Box<[u8]>> = Vec::new();
        let mut positions: Vec<(i64, u64)> = Vec::new();
        let mut values: Vec<Box<[u8]>> = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (db_key, value) = row.map_err(re)?;
            if db_key.len() < KEY_PREFIX_LEN + FRAME_SUFFIX_LEN || db_key[4] != FRAMES_TABLE {
                continue;
            }
            let suffix = &db_key[db_key.len() - FRAME_SUFFIX_LEN..];
            keys.push(db_key[KEY_PREFIX_LEN..db_key.len() - FRAME_SUFFIX_LEN].into());
            positions.push((
                (u64::from_be_bytes(suffix[..8].try_into().expect("frame rt")) ^ RT_SIGN_FLIP)
                    as i64,
                u64::from_be_bytes(suffix[8..].try_into().expect("frame seq")),
            ));
            values.push(value.into());
        }
        let value_refs: Vec<&[u8]> = values.iter().map(|value| value.as_ref()).collect();
        let decoded = Self::decode_rows(
            self.frames_converter.as_ref().expect("over frame codec"),
            &value_refs,
        )?;
        let mut out: Vec<(Box<[u8]>, Vec<StoredFrameRow>)> = Vec::new();
        for ((key, (rt, seq)), values) in keys.into_iter().zip(positions).zip(decoded) {
            match out.last_mut() {
                Some((last, rows)) if *last == key => rows.push(StoredFrameRow { rt, seq, values }),
                _ => out.push((key, vec![StoredFrameRow { rt, seq, values }])),
            }
        }
        Ok(out)
    }

    /// The `[key_group][STAMPS][key]` row carrying a bounded-shape key's cleanup deadline.
    pub(crate) fn stamp_key(&self, key_group: i32, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(KEY_PREFIX_LEN + key.len());
        out.extend_from_slice(&key_group.to_be_bytes());
        out.push(STAMPS_TABLE);
        out.extend_from_slice(key);
        out
    }

    /// Writes touched keys' cleanup deadlines; a `None` stamp deletes the row (the key's buffer
    /// emptied — the raw snapshots likewise drop a bufferless key's stamp).
    pub(crate) fn write_stamps(
        &mut self,
        entries: &[(Vec<u8>, Option<i64>)],
    ) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for (db_key, stamp) in entries {
            match stamp {
                Some(stamp) => writes.put(db_key, stamp.to_le_bytes())?,
                None => writes.delete(db_key)?,
            }
        }
        writes.finish()
    }

    /// Every persisted per-key stamp — restore-time retention hydration for the bounded shapes.
    pub(crate) fn scan_stamps(&self) -> Result<Vec<(Box<[u8]>, i64)>, DataFusionError> {
        let mut out = Vec::new();
        for row in self.db.iterator(IteratorMode::Start) {
            let (db_key, value) = row.map_err(re)?;
            if db_key.len() < KEY_PREFIX_LEN || db_key[4] != STAMPS_TABLE || value.len() != 8 {
                continue;
            }
            out.push((
                db_key[KEY_PREFIX_LEN..].into(),
                i64::from_le_bytes(value[..8].try_into().expect("stamp")),
            ));
        }
        Ok(out)
    }

    /// The `[key_group][DISTINCT + slot][key]` prefix of one key's seen-set for one DISTINCT
    /// aggregate slot.
    pub(crate) fn distinct_prefix(&self, slot: usize, key_group: i32, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(KEY_PREFIX_LEN + key.len());
        out.extend_from_slice(&key_group.to_be_bytes());
        out.push(DISTINCT_TABLE_BASE + slot as u8);
        out.extend_from_slice(key);
        out
    }

    /// Encodes one DISTINCT slot's batch elements to their arrow-row key bytes in one pass.
    pub(crate) fn encode_distinct_elements(
        &self,
        slot: usize,
        elements: &ArrayRef,
    ) -> Result<Vec<Vec<u8>>, DataFusionError> {
        let rows = self.distinct_converters[slot]
            .convert_columns(std::slice::from_ref(elements))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(rows.iter().map(|row| row.data().to_vec()).collect())
    }

    /// Presence probe for a batch's (key, element) pairs: one multi-get.
    pub(crate) fn probe_distinct(&self, db_keys: &[Vec<u8>]) -> Result<Vec<bool>, DataFusionError> {
        let fetched = self.db.multi_get(db_keys);
        let mut out = Vec::with_capacity(fetched.len());
        for value in fetched {
            out.push(value.map_err(re)?.is_some());
        }
        Ok(out)
    }

    /// Inserts newly seen elements (empty values — OVER input is insert-only, so presence is the
    /// whole set).
    pub(crate) fn write_distinct(&mut self, db_keys: &[Vec<u8>]) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for db_key in db_keys {
            writes.put(db_key, [])?;
        }
        writes.finish()
    }

    /// One key's full seen-set for one DISTINCT slot — canonical savepoints only; the data plane
    /// probes per element.
    pub(crate) fn scan_distinct(
        &self,
        slot: usize,
        key_group: i32,
        key: &[u8],
    ) -> Result<Vec<ScalarValue>, DataFusionError> {
        let prefix = self.distinct_prefix(slot, key_group, key);
        let mut elements: Vec<Box<[u8]>> = Vec::new();
        for row in self
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (db_key, _) = row.map_err(re)?;
            if !db_key.starts_with(&prefix) {
                break;
            }
            elements.push(db_key[prefix.len()..].into());
        }
        let element_refs: Vec<&[u8]> = elements.iter().map(|bytes| bytes.as_ref()).collect();
        Ok(
            Self::decode_rows(&self.distinct_converters[slot], &element_refs)?
                .into_iter()
                .map(|mut scalars| scalars.remove(0))
                .collect(),
        )
    }

    /// Stages the range covering everything under `prefix` for deletion — a cleared key's frames
    /// or seen-set.
    pub(crate) fn delete_prefix_range(
        &mut self,
        prefixes: &[Vec<u8>],
    ) -> Result<(), DataFusionError> {
        let mut writes = FlinkWriteBatch::new(&self.db, self.write_batch_size);
        for prefix in prefixes {
            writes.delete_range(prefix.clone(), prefix_successor(prefix))?;
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
        Self::decode_rows(
            self.state_converter.as_ref().expect("over fold codec"),
            values,
        )
    }

    fn decode_rows(
        converter: &RowConverter,
        values: &[&[u8]],
    ) -> Result<Vec<Vec<ScalarValue>>, DataFusionError> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let parser = converter.parser();
        let rows: Vec<_> = values.iter().map(|value| parser.parse(value)).collect();
        let columns = converter
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
            &[],
            &[],
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

    fn shape_store(
        name: &str,
        value_types: &[i64],
        kinds: &[i64],
        frame_kind: i64,
        proctime: bool,
    ) -> RocksOverAggStore {
        RocksOverAggStore::create(
            test_config(name),
            &rocks_over_state_types(value_types, kinds, frame_kind, proctime).unwrap(),
            &rocks_over_frame_value_types(value_types, frame_kind),
            &rocks_over_distinct_element_types(value_types, kinds, frame_kind),
            over_schema(),
            0..=127,
        )
        .unwrap()
    }

    fn reopen_shape_store(
        name: &str,
        value_types: &[i64],
        kinds: &[i64],
        frame_kind: i64,
        proctime: bool,
        snapshot: String,
        snapshot_id: i64,
    ) -> RocksOverAggStore {
        RocksOverAggStore::open_merged(
            test_config(name),
            &rocks_over_state_types(value_types, kinds, frame_kind, proctime).unwrap(),
            &rocks_over_frame_value_types(value_types, frame_kind),
            &rocks_over_distinct_element_types(value_types, kinds, frame_kind),
            over_schema(),
            0..=127,
            &[(snapshot, snapshot_id)],
            true,
        )
        .unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn shape_pair(
        name: &str,
        value_types: Vec<i64>,
        kinds: Vec<i64>,
        frame_kind: i64,
        frame_offset: i64,
        proctime: bool,
        retention_ms: i64,
    ) -> (OverWindowAggregator, OverWindowAggregator) {
        let value_columns = if kinds.iter().all(|&k| is_window_function_kind(k)) {
            Vec::new()
        } else {
            vec![1; kinds.len()]
        };
        let build = || {
            OverWindowAggregator::new(
                value_types.clone(),
                kinds.clone(),
                2,
                value_columns.clone(),
                vec![0],
                frame_kind,
                frame_offset,
                proctime,
            )
            .with_state_retention(retention_ms)
        };
        let store = shape_store(name, &value_types, &kinds, frame_kind, proctime);
        (
            build(),
            build()
                .with_key_timestamp_precisions(vec![-1])
                .with_store(store, vec![DataType::Int64]),
        )
    }

    // A bounded ROWS frame on the store: appended rows, eviction, rowtime ties, and multiple
    // firings all emit byte-identically to the memory path's recompute.
    #[test]
    fn store_backed_bounded_rows_frame_matches_the_memory_path() {
        let (mut memory, mut rocks) = shape_pair("bounded-rows", vec![0], vec![0], 1, 2, false, 0);
        for batch in [
            over_batch(&[1, 1, 2, 1], &[10, 20, 100, 30], &[0, 100, 100, 200]),
            over_batch(&[1, 2, 1], &[40, 200, 50], &[300, 300, 300]),
        ] {
            memory.push(batch.clone(), 0).unwrap();
            rocks.push(batch, 0).unwrap();
        }
        assert_eq!(memory.flush(150, 0).unwrap(), rocks.flush(150, 0).unwrap());
        assert_eq!(memory.flush(300, 0).unwrap(), rocks.flush(300, 0).unwrap());
        let late = over_batch(&[1, 2], &[60, 300], &[400, 500]);
        memory.push(late.clone(), 0).unwrap();
        rocks.push(late, 0).unwrap();
        assert_eq!(memory.flush(600, 0).unwrap(), rocks.flush(600, 0).unwrap());
    }

    // A bounded RANGE frame on the store: the rowtime-interval frame and its eviction bound
    // behave as in memory, including tied rowtimes sharing one frame.
    #[test]
    fn store_backed_bounded_range_frame_matches_the_memory_path() {
        let (mut memory, mut rocks) =
            shape_pair("bounded-range", vec![0], vec![0], 2, 150, false, 0);
        for batch in [
            over_batch(&[1, 1, 1], &[10, 20, 30], &[0, 100, 100]),
            over_batch(&[1, 2], &[40, 100], &[260, 300]),
        ] {
            memory.push(batch.clone(), 0).unwrap();
            rocks.push(batch, 0).unwrap();
        }
        assert_eq!(memory.flush(100, 0).unwrap(), rocks.flush(100, 0).unwrap());
        assert_eq!(memory.flush(400, 0).unwrap(), rocks.flush(400, 0).unwrap());
        let next = over_batch(&[1], &[50], &[420]);
        memory.push(next.clone(), 0).unwrap();
        rocks.push(next, 0).unwrap();
        assert_eq!(memory.flush(500, 0).unwrap(), rocks.flush(500, 0).unwrap());
    }

    // Bounded frames survive a native checkpoint: the restored buffer keeps its row order (ties
    // included), continues evicting, and new rows append after the restored ones.
    #[test]
    fn store_backed_bounded_frame_restores() {
        let snapshot = snapshot_dir("bounded-restore");
        let (mut memory, mut rocks) =
            shape_pair("bounded-restore", vec![0], vec![0], 1, 2, false, 0);
        let first = over_batch(&[1, 1, 1], &[10, 20, 30], &[0, 100, 100]);
        memory.push(first.clone(), 0).unwrap();
        rocks.push(first, 0).unwrap();
        assert_eq!(memory.flush(100, 0).unwrap(), rocks.flush(100, 0).unwrap());
        let manifest = rocks.checkpoint_store(&snapshot).unwrap();
        drop(rocks);

        let store = reopen_shape_store(
            "bounded-restore-reopen",
            &[0],
            &[0],
            1,
            false,
            snapshot,
            manifest.snapshot_id,
        );
        let mut restored =
            OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 1, 2, false)
                .with_key_timestamp_precisions(vec![-1])
                .with_store(store, vec![DataType::Int64]);
        let next = over_batch(&[1, 1], &[40, 50], &[200, 300]);
        memory.push(next.clone(), 0).unwrap();
        restored.push(next, 0).unwrap();
        assert_eq!(
            memory.flush(300, 0).unwrap(),
            restored.flush(300, 0).unwrap()
        );
    }

    // A canonical savepoint of a store-backed bounded frame restores into a memory aggregator
    // that continues identically, and a memory blob imports into the typed store at open.
    #[test]
    fn bounded_frame_transitions_between_backends() {
        let (mut memory, mut rocks) =
            shape_pair("bounded-canonical", vec![0], vec![0], 1, 2, false, 0);
        let first = over_batch(&[1, 1, 1, 2], &[10, 20, 30, 100], &[0, 100, 100, 50]);
        memory.push(first.clone(), 0).unwrap();
        rocks.push(first, 0).unwrap();
        assert_eq!(memory.flush(100, 0).unwrap(), rocks.flush(100, 0).unwrap());

        // Store -> memory: the canonical snapshot is the raw keyed encoding.
        let snapshots: Vec<Vec<u8>> = rocks
            .canonical_partitions(128, &[-1])
            .unwrap()
            .into_values()
            .collect();
        let mut from_canonical = OverWindowAggregator::restore_partitions(
            vec![0],
            vec![0],
            2,
            vec![1],
            vec![0],
            1,
            2,
            false,
            &snapshots,
            0,
            0,
        );
        let next = over_batch(&[1, 2], &[40, 200], &[200, 200]);
        memory.push(next.clone(), 0).unwrap();
        from_canonical.push(next.clone(), 0).unwrap();
        assert_eq!(
            memory.flush(200, 0).unwrap(),
            from_canonical.flush(200, 0).unwrap()
        );

        // Memory -> store: the memory blob imports into the frames table at open.
        let blob = memory.snapshot_partitions(128, &[-1]);
        let mut imported =
            OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 1, 2, false)
                .with_key_timestamp_precisions(vec![-1])
                .with_store(
                    shape_store("bounded-import", &[0], &[0], 1, false),
                    vec![DataType::Int64],
                );
        imported
            .import_partitions(&blob.into_values().collect::<Vec<_>>())
            .unwrap();
        imported.adopt_store_retention(0).unwrap();
        let tail = over_batch(&[1, 1], &[60, 70], &[300, 400]);
        memory.push(tail.clone(), 0).unwrap();
        imported.push(tail, 0).unwrap();
        assert_eq!(
            memory.flush(400, 0).unwrap(),
            imported.flush(400, 0).unwrap()
        );
    }

    // The proctime unbounded fold on the store: eager per-batch emission matches memory, and the
    // fold plus the arrival counter survive a checkpoint (RANK numbering must continue, not
    // restart or tie).
    #[test]
    fn store_backed_proctime_unbounded_matches_and_restores() {
        let (mut memory, mut rocks) = shape_pair("proctime-fold", vec![0], vec![0], 0, 0, true, 0);
        for batch in [
            over_batch(&[1, 2, 1], &[10, 100, 20], &[0, 0, 0]),
            over_batch(&[1, 3], &[5, 7], &[0, 0]),
        ] {
            assert_eq!(
                memory.push_proctime(batch.clone(), 0).unwrap(),
                rocks.push_proctime(batch, 0).unwrap()
            );
        }
        let snapshot = snapshot_dir("proctime-fold");
        let manifest = rocks.checkpoint_store(&snapshot).unwrap();
        drop(rocks);
        let store = reopen_shape_store(
            "proctime-fold-reopen",
            &[0],
            &[0],
            0,
            true,
            snapshot,
            manifest.snapshot_id,
        );
        let mut restored =
            OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 0, 0, true)
                .with_key_timestamp_precisions(vec![-1])
                .with_store(store, vec![DataType::Int64]);
        restored.adopt_store_retention(0).unwrap();
        let tail = over_batch(&[1, 2], &[1, 2], &[0, 0]);
        assert_eq!(
            memory.push_proctime(tail.clone(), 0).unwrap(),
            restored.push_proctime(tail, 0).unwrap()
        );
    }

    // Proctime window functions (ROW_NUMBER + RANK) on the store: per-key numbering matches
    // memory and continues across a restore (the persisted arrival counter keeps RANK's order
    // values monotone).
    #[test]
    fn store_backed_proctime_window_functions_match_and_restore() {
        let (mut memory, mut rocks) =
            shape_pair("proctime-wf", vec![], vec![10, 11], 0, 0, true, 0);
        for batch in [
            over_batch(&[1, 1, 2], &[0, 0, 0], &[0, 0, 0]),
            over_batch(&[2, 1], &[0, 0], &[0, 0]),
        ] {
            assert_eq!(
                memory.push_proctime(batch.clone(), 0).unwrap(),
                rocks.push_proctime(batch, 0).unwrap()
            );
        }
        let snapshot = snapshot_dir("proctime-wf");
        let manifest = rocks.checkpoint_store(&snapshot).unwrap();
        drop(rocks);
        let store = reopen_shape_store(
            "proctime-wf-reopen",
            &[],
            &[10, 11],
            0,
            true,
            snapshot,
            manifest.snapshot_id,
        );
        let mut restored =
            OverWindowAggregator::new(vec![], vec![10, 11], 2, vec![], vec![0], 0, 0, true)
                .with_key_timestamp_precisions(vec![-1])
                .with_store(store, vec![DataType::Int64]);
        restored.adopt_store_retention(0).unwrap();
        let tail = over_batch(&[1, 2, 1], &[0, 0, 0], &[0, 0, 0]);
        assert_eq!(
            memory.push_proctime(tail.clone(), 0).unwrap(),
            restored.push_proctime(tail, 0).unwrap()
        );
    }

    // The proctime bounded-ROWS frame on the store: the sliding frame recomputes identically and
    // its deadline retention clears the frame unconditionally, exactly as memory mode.
    #[test]
    fn store_backed_proctime_bounded_rows_matches_the_memory_path() {
        let (mut memory, mut rocks) =
            shape_pair("proctime-rows", vec![0], vec![0], 1, 1, true, 5000);
        for (batch, now) in [
            (over_batch(&[1, 1, 1], &[10, 20, 30], &[0, 0, 0]), 1000i64),
            (over_batch(&[1, 2], &[40, 100], &[0, 0]), 2000),
        ] {
            assert_eq!(
                memory.push_proctime(batch.clone(), now).unwrap(),
                rocks.push_proctime(batch, now).unwrap()
            );
        }
        // Past the deadline (2000 + 7500): the frame restarts short on both backends.
        let expired = over_batch(&[1], &[50], &[0]);
        assert_eq!(
            memory.push_proctime(expired.clone(), 20000).unwrap(),
            rocks.push_proctime(expired, 20000).unwrap()
        );
    }

    // The proctime per-value TTL on the store: an expired key restarts its running fold from
    // zero on both backends, and the persisted last-write stamp keeps expiry timing across a
    // restore.
    #[test]
    fn store_backed_proctime_value_ttl_matches_and_survives_restore() {
        let (mut memory, mut rocks) =
            shape_pair("proctime-ttl", vec![0], vec![0], 0, 0, true, 2000);
        let first = over_batch(&[1], &[10], &[0]);
        assert_eq!(
            memory.push_proctime(first.clone(), 1000).unwrap(),
            rocks.push_proctime(first, 1000).unwrap()
        );
        let alive = over_batch(&[1], &[5], &[0]);
        assert_eq!(
            memory.push_proctime(alive.clone(), 2500).unwrap(),
            rocks.push_proctime(alive, 2500).unwrap()
        );
        let snapshot = snapshot_dir("proctime-ttl");
        let manifest = rocks.checkpoint_store(&snapshot).unwrap();
        drop(rocks);
        let store = reopen_shape_store(
            "proctime-ttl-reopen",
            &[0],
            &[0],
            0,
            true,
            snapshot,
            manifest.snapshot_id,
        );
        let mut restored =
            OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 0, 0, true)
                .with_state_retention(2000)
                .with_key_timestamp_precisions(vec![-1])
                .with_store(store, vec![DataType::Int64]);
        restored.adopt_store_retention(3000).unwrap();
        // Last write was 2500; expired at 4500. Alive just inside, restarted past it.
        let inside = over_batch(&[1], &[3], &[0]);
        let out = restored.push_proctime(inside, 4499).unwrap();
        assert_eq!(column(&out, 3), vec![18]);
        let expired = over_batch(&[1], &[2], &[0]);
        let out = restored.push_proctime(expired, 9000).unwrap();
        assert_eq!(column(&out, 3), vec![2]);
    }

    // DISTINCT aggregates of the unbounded fold: the seen-set lives in per-element companion
    // rows, point-probed per batch — duplicates are skipped identically to memory, across
    // firings, a restore, and both backend transitions.
    #[test]
    fn store_backed_distinct_matches_restores_and_transitions() {
        let (mut memory, mut rocks) = shape_pair("distinct", vec![0], vec![100], 0, 0, false, 0);
        for batch in [
            over_batch(&[1, 1, 1, 2], &[10, 10, 20, 10], &[0, 100, 200, 100]),
            over_batch(&[1, 2], &[10, 10], &[300, 300]),
        ] {
            memory.push(batch.clone(), 0).unwrap();
            rocks.push(batch, 0).unwrap();
        }
        assert_eq!(memory.flush(200, 0).unwrap(), rocks.flush(200, 0).unwrap());

        let snapshot = snapshot_dir("distinct");
        let manifest = rocks.checkpoint_store(&snapshot).unwrap();
        drop(rocks);
        let store = reopen_shape_store(
            "distinct-reopen",
            &[0],
            &[100],
            0,
            false,
            snapshot,
            manifest.snapshot_id,
        );
        let mut restored =
            OverWindowAggregator::new(vec![0], vec![100], 2, vec![1], vec![0], 0, 0, false)
                .with_key_timestamp_precisions(vec![-1])
                .with_store(store, vec![DataType::Int64]);
        restored.adopt_store_retention(0).unwrap();
        assert_eq!(
            memory.flush(300, 0).unwrap(),
            restored.flush(300, 0).unwrap()
        );

        // Store -> memory canonical: the seen-set rides the distinct list column.
        let snapshots: Vec<Vec<u8>> = restored
            .canonical_partitions(128, &[-1])
            .unwrap()
            .into_values()
            .collect();
        let mut from_canonical = OverWindowAggregator::restore_partitions(
            vec![0],
            vec![100],
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
        let next = over_batch(&[1, 2], &[10, 30], &[400, 400]);
        memory.push(next.clone(), 0).unwrap();
        from_canonical.push(next.clone(), 0).unwrap();
        restored.push(next, 0).unwrap();
        let expected = memory.flush(400, 0).unwrap();
        assert_eq!(expected, from_canonical.flush(400, 0).unwrap());
        assert_eq!(expected, restored.flush(400, 0).unwrap());

        // Memory -> store: the blob's seen-sets fan out to the companion rows at open.
        let blob = memory.snapshot_partitions(128, &[-1]);
        let mut imported =
            OverWindowAggregator::new(vec![0], vec![100], 2, vec![1], vec![0], 0, 0, false)
                .with_key_timestamp_precisions(vec![-1])
                .with_store(
                    shape_store("distinct-import", &[0], &[100], 0, false),
                    vec![DataType::Int64],
                );
        imported
            .import_partitions(&blob.into_values().collect::<Vec<_>>())
            .unwrap();
        imported.adopt_store_retention(0).unwrap();
        let tail = over_batch(&[1, 1], &[10, 40], &[500, 500]);
        memory.push(tail.clone(), 0).unwrap();
        imported.push(tail, 0).unwrap();
        assert_eq!(
            memory.flush(500, 0).unwrap(),
            imported.flush(500, 0).unwrap()
        );
    }

    // The bounded ROWS deadline retention on the store: idle keys clear identically, and the
    // per-key stamp row keeps expiry timing across a restore.
    #[test]
    fn store_backed_bounded_retention_matches_and_survives_restore() {
        let (mut memory, mut rocks) =
            shape_pair("bounded-retention", vec![0], vec![0], 1, 2, false, 2000);
        let first = over_batch(&[1], &[10], &[100]);
        memory.push(first.clone(), 5000).unwrap();
        rocks.push(first, 5000).unwrap();
        assert_eq!(
            memory.flush(200, 5000).unwrap(),
            rocks.flush(200, 5000).unwrap()
        );

        let snapshot = snapshot_dir("bounded-retention");
        let manifest = rocks.checkpoint_store(&snapshot).unwrap();
        drop(rocks);
        let store = reopen_shape_store(
            "bounded-retention-reopen",
            &[0],
            &[0],
            1,
            false,
            snapshot,
            manifest.snapshot_id,
        );
        let mut restored =
            OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 1, 2, false)
                .with_state_retention(2000)
                .with_key_timestamp_precisions(vec![-1])
                .with_store(store, vec![DataType::Int64]);
        restored.adopt_store_retention(6000).unwrap();
        // The stamp persisted at the fire (5000 + 3000 = 8000): the frame carries over just
        // inside it and restarts short past it, matching the memory path's timing.
        memory.push(over_batch(&[1], &[5], &[300]), 7999).unwrap();
        restored.push(over_batch(&[1], &[5], &[300]), 7999).unwrap();
        assert_eq!(
            memory.flush(400, 7999).unwrap(),
            restored.flush(400, 7999).unwrap()
        );
        memory.push(over_batch(&[1], &[1], &[500]), 20000).unwrap();
        restored
            .push(over_batch(&[1], &[1], &[500]), 20000)
            .unwrap();
        let expired = restored.flush(600, 20000).unwrap();
        assert_eq!(memory.flush(600, 20000).unwrap(), expired);
        assert_eq!(column(&expired, 3), vec![1]);
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
            &[],
            &[],
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
            &[],
            &[],
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
            &[],
            &[],
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
