//! Rust-owned RocksDB state. Operators retain their existing typed codecs and write-buffer
//! semantics; committed entries are keyed by Flink key group plus BinaryRow bytes and are read
//! directly through RocksDB without a Java/JNI data-plane round trip.

use crate::*;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use rocksdb::{Cache, DB, IteratorMode, Options, WriteBatch};
use std::collections::HashSet;
use std::io::Cursor;

const TS_COLUMN: &str = "ts";

pub(crate) trait RocksStateCodec {
    type Value;
    fn supported(&self) -> bool;
    fn value_fields(&self) -> Vec<(String, DataType)>;
    fn encode(&self, value: &Self::Value) -> Vec<ScalarValue>;
    fn decode(&self, scalars: &[ScalarValue]) -> Self::Value;
    fn value_bytes(&self, value: &Self::Value) -> usize;
    fn write_ms(&self, _value: &Self::Value) -> i64 { 0 }
    fn stamp_write_ms(&self, _value: &mut Self::Value, _ts_ms: i64) {}
}

pub(crate) fn rocks_row_supported(types: &[DataType]) -> bool {
    types.iter().all(|data_type| matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::Float32 | DataType::Float64 | DataType::Utf8 | DataType::Binary
            | DataType::Date32 | DataType::Decimal128(_, _)
            | DataType::Timestamp(_, None)
    ))
}

pub(crate) fn rocks_group_supported(kinds: &[i64], state_types: &[DataType]) -> bool {
    group_kinds_persistable(kinds) && rocks_row_supported(state_types)
}

pub(crate) struct RowPayloadCodec {
    row_types: Vec<DataType>,
    converter: arrow::row::RowConverter,
}

impl RowPayloadCodec {
    pub(crate) fn new(row_types: Vec<DataType>) -> Self {
        let converter = arrow::row::RowConverter::new(
            row_types.iter().map(|t| arrow::row::SortField::new(t.clone())).collect(),
        ).expect("row payload codec converter");
        Self { row_types, converter }
    }
    pub(crate) fn supported(&self) -> bool { rocks_row_supported(&self.row_types) }
    pub(crate) fn fields(&self) -> Vec<(String, DataType)> {
        self.row_types.iter().enumerate().map(|(i, t)| (format!("c{i}"), t.clone())).collect()
    }
    pub(crate) fn encode_payload(&self, payload: &[u8]) -> Vec<ScalarValue> {
        let parser = self.converter.parser();
        self.converter.convert_rows([parser.parse(payload)]).expect("decode row payload")
            .iter().map(|c| ScalarValue::try_from_array(c, 0).expect("row scalar")).collect()
    }
    pub(crate) fn decode_payload(&self, scalars: &[ScalarValue]) -> (Arc<[u8]>, Vec<ArrayRef>) {
        let columns: Vec<_> = scalars.iter().zip(&self.row_types)
            .map(|(s, t)| scalars_to_array(vec![s.clone()], t)).collect();
        let rows = self.converter.convert_columns(&columns).expect("encode row payload");
        (Arc::from(rows.row(0).data()), columns)
    }
}

#[derive(Clone)]
pub(crate) struct RocksStoreConfig {
    pub table_dir: String,
    pub max_parallelism: usize,
    pub options_json: String,
    pub ttl_ms: i64,
}

#[derive(serde::Serialize)]
pub(crate) struct RocksCheckpointManifest {
    pub snapshot_id: i64,
    pub data_files: Vec<String>,
    pub meta_files: Vec<String>,
}

impl RocksCheckpointManifest {
    pub(crate) fn absent() -> Self {
        Self { snapshot_id: -1, data_files: Vec::new(), meta_files: Vec::new() }
    }
}

enum Slot<V> { Present { state: V, dirty: bool }, Absent { dirty: bool } }

pub(crate) struct RocksStore<C: RocksStateCodec> {
    db: DB,
    _cache: Option<Cache>,
    config: RocksStoreConfig,
    codec: C,
    value_fields: Vec<Field>,
    now_ms: i64,
    generation: i64,
    working: ahash::HashMap<ByteKey, Slot<C::Value>>,
    footprint: isize,
}

impl<C: RocksStateCodec> RocksStore<C> {
    const SLOT_OVERHEAD: usize = std::mem::size_of::<Slot<C::Value>>() + GROUP_ENTRY_OVERHEAD;

    pub(crate) fn create(config: RocksStoreConfig, codec: C) -> Result<Self, DataFusionError> {
        Self::open_db(config, codec)
    }

    pub(crate) fn open_merged(
        config: RocksStoreConfig,
        codec: C,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        _aligned: bool,
        now_ms: i64,
    ) -> Result<Self, DataFusionError> {
        let mut store = Self::open_db(config, codec)?;
        store.now_ms = now_ms;
        let mut writes = WriteBatch::default();
        for (source, _) in sources {
            let source_db = DB::open_for_read_only(&Options::default(), source, false).map_err(re)?;
            for row in source_db.iterator(IteratorMode::Start) {
                let (key, value) = row.map_err(re)?;
                if key.len() >= 4 {
                    let kg = i32::from_be_bytes(key[0..4].try_into().expect("key group prefix"));
                    if key_groups.contains(&kg) { writes.put(key, value); }
                }
            }
        }
        store.db.write(writes).map_err(re)?;
        Ok(store)
    }

    fn open_db(config: RocksStoreConfig, codec: C) -> Result<Self, DataFusionError> {
        if !codec.supported() { return Err(DataFusionError::Plan("state shape not supported by RocksDB".into())); }
        std::fs::create_dir_all(&config.table_dir).map_err(ioe)?;
        let resolved = crate::state::rocks_config::FlinkRocksOptions::from_json(&config.options_json)
            .map_err(DataFusionError::Plan)?;
        let (options, cache) = resolved.build().map_err(DataFusionError::Plan)?;
        let db = DB::open(&options, &config.table_dir).map_err(re)?;
        let mut value_fields: Vec<_> = codec.value_fields().into_iter()
            .map(|(n, t)| Field::new(n, t, true)).collect();
        if config.ttl_ms > 0 { value_fields.push(Field::new(TS_COLUMN, DataType::Int64, true)); }
        Ok(Self { db, _cache: cache, config, codec, value_fields, now_ms: 0, generation: 0,
            working: ahash::HashMap::default(), footprint: 0 })
    }

    pub(crate) fn set_clock(&mut self, now_ms: i64) { self.now_ms = now_ms; }
    pub(crate) fn staging_bytes(&self) -> usize { self.working.len() * Self::SLOT_OVERHEAD }
    pub(crate) fn staged_keys(&self) -> usize { self.working.len() }
    pub(crate) fn metric_entry_count(&self) -> usize { self.working.len() }

    fn db_key(&self, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + key.len());
        let key_group = flink_key_group(hash_bytes_by_words(key), self.config.max_parallelism) as i32;
        out.extend_from_slice(&key_group.to_be_bytes());
        out.extend_from_slice(key);
        out
    }

    fn encode_value(&self, state: &C::Value) -> Result<Vec<u8>, DataFusionError> {
        let mut scalars = self.codec.encode(state);
        if self.config.ttl_ms > 0 { scalars.push(ScalarValue::Int64(Some(self.codec.write_ms(state)))); }
        let arrays: Vec<_> = scalars.into_iter().zip(&self.value_fields)
            .map(|(s, f)| scalars_to_array(vec![s], f.data_type())).collect();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(self.value_fields.clone())), arrays)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut bytes = Vec::new();
        { let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema()).map_err(ae)?;
          writer.write(&batch).map_err(ae)?; writer.finish().map_err(ae)?; }
        Ok(bytes)
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<Option<C::Value>, DataFusionError> {
        let mut reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(ae)?;
        let batch = reader.next().transpose().map_err(ae)?.ok_or_else(|| DataFusionError::Internal("empty RocksDB state value".into()))?;
        let mut scalars: Vec<_> = batch.columns().iter()
            .map(|c| ScalarValue::try_from_array(c, 0)).collect::<Result<_, _>>()?;
        let ts = if self.config.ttl_ms > 0 { match scalars.pop() { Some(ScalarValue::Int64(v)) => v, _ => None } } else { None };
        if ts.is_some_and(|t| self.now_ms >= t.saturating_add(self.config.ttl_ms)) { return Ok(None); }
        let mut state = self.codec.decode(&scalars);
        if let Some(ts) = ts { self.codec.stamp_write_ms(&mut state, ts); }
        Ok(Some(state))
    }

    pub(crate) fn checkpoint(&mut self) -> Result<RocksCheckpointManifest, DataFusionError> {
        let mut writes = WriteBatch::default();
        for (key, slot) in &self.working {
            let db_key = self.db_key(&key.0);
            match slot {
                Slot::Present { state, dirty: true } => writes.put(db_key, self.encode_value(state)?),
                Slot::Absent { dirty: true } => writes.delete(db_key),
                _ => {}
            }
        }
        self.db.write(writes).map_err(re)?;
        self.db.flush().map_err(re)?;
        self.working.clear();
        self.generation += 1;
        let mut data_files = Vec::new(); let mut meta_files = Vec::new();
        for entry in std::fs::read_dir(&self.config.table_dir).map_err(ioe)? {
            let entry = entry.map_err(ioe)?; if !entry.file_type().map_err(ioe)?.is_file() { continue; }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "LOCK" || name.starts_with("LOG") { continue; }
            if name.ends_with(".sst") { data_files.push(name); } else { meta_files.push(name); }
        }
        data_files.sort(); meta_files.sort();
        Ok(RocksCheckpointManifest { snapshot_id: self.generation, data_files, meta_files })
    }
}

impl<C: RocksStateCodec> KeyedStateStore<C::Value> for RocksStore<C> {
    fn contains(&self, key: &[u8]) -> bool { matches!(self.working.get(key), Some(Slot::Present { .. })) }
    fn get(&self, key: &[u8]) -> Option<&C::Value> { match self.working.get(key) { Some(Slot::Present { state, .. }) => Some(state), _ => None } }
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut C::Value> { match self.working.get_mut(key) { Some(Slot::Present { state, dirty }) => { *dirty = true; Some(state) }, _ => None } }
    fn insert(&mut self, key: ByteKey, value: C::Value) -> &mut C::Value { match self.working.entry(key).insert_entry(Slot::Present { state: value, dirty: true }).into_mut() { Slot::Present { state, .. } => state, _ => unreachable!() } }
    fn remove(&mut self, key: &[u8]) { self.working.insert(ByteKey::from(key), Slot::Absent { dirty: true }); }
    fn begin_batch(&mut self, batch: &RecordBatch, key_columns: &[usize], precisions: &[i32]) -> Result<(), DataFusionError> {
        let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, precisions);
        let mut missing = HashSet::new();
        for row in 0..batch.num_rows() { let key = ByteKey::from(encoder.encode(row)); if !self.working.contains_key(&key) { missing.insert(key); } }
        for key in missing { let value = self.db.get(self.db_key(&key.0)).map_err(re)?; let slot = match value { Some(v) => match self.decode_value(&v)? { Some(state) => Slot::Present { state, dirty: false }, None => Slot::Absent { dirty: true } }, None => Slot::Absent { dirty: false } }; self.working.insert(key, slot); }
        Ok(())
    }
    fn end_bundle(&mut self) -> Result<(), DataFusionError> { self.working.retain(|_, slot| match slot { Slot::Present { dirty, .. } | Slot::Absent { dirty } => *dirty }); Ok(()) }
    fn footprint_delta(&mut self) -> isize { std::mem::take(&mut self.footprint) }
}

fn re(error: rocksdb::Error) -> DataFusionError { DataFusionError::External(Box::new(error)) }
fn ioe(error: std::io::Error) -> DataFusionError { DataFusionError::External(Box::new(error)) }
fn ae(error: arrow::error::ArrowError) -> DataFusionError { DataFusionError::External(Box::new(error)) }
