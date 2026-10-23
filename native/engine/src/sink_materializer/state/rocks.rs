use super::*;
use rocksdb::{Direction, IteratorMode};

const DESCRIPTOR: &[u8] = b"\xff\xff\xff\xffsink-materializer-v1";
const VALUE: u8 = 1;
const NODE: u8 = 2;
const INDEX: u8 = 3;
fn error(e: rocksdb::Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
fn integer(bytes: &[u8]) -> i64 {
    i64::from_le_bytes(bytes[..8].try_into().unwrap())
}
fn option(bytes: &mut Vec<u8>, value: Option<i64>) {
    bytes.push(u8::from(value.is_some()));
    bytes.extend_from_slice(&value.unwrap_or(0).to_le_bytes());
}
fn read_option(bytes: &[u8]) -> Option<i64> {
    (bytes[0] != 0).then(|| integer(&bytes[1..]))
}
struct Meta {
    linked: bool,
    written: i64,
    size: i64,
    high: Option<i64>,
    next: i64,
}
impl Meta {
    fn new(strategy: Strategy) -> Self {
        Self {
            linked: matches!(strategy, Strategy::Map { .. }),
            written: 0,
            size: 0,
            high: None,
            next: 0,
        }
    }
    fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![u8::from(self.linked)];
        bytes.extend_from_slice(&self.written.to_le_bytes());
        bytes.extend_from_slice(&self.size.to_le_bytes());
        option(&mut bytes, self.high);
        bytes.extend_from_slice(&self.next.to_le_bytes());
        bytes
    }
    fn decode(bytes: &[u8]) -> Self {
        Self {
            linked: bytes[0] != 0,
            written: integer(&bytes[1..]),
            size: integer(&bytes[9..]),
            high: read_option(&bytes[17..]),
            next: integer(&bytes[26..]),
        }
    }
}
/// Candidate payloads and index entries live in RocksDB; a point operation hydrates only its neighbours.
pub(in crate::sink_materializer) struct RocksCandidates {
    pub(in crate::sink_materializer) table: RocksRawStore,
    max_parallelism: usize,
    schema: std::sync::OnceLock<SchemaRef>,
    work: std::cell::RefCell<Option<MemoryReservation>>,
    work_size: std::cell::Cell<usize>,
    work_active: std::cell::Cell<bool>,
    sweep_cursor: std::cell::RefCell<Option<Vec<u8>>>,
    last_sweep: std::cell::Cell<i64>,
}
impl RocksCandidates {
    pub(in crate::sink_materializer) fn open(
        config: RocksStoreConfig,
        sources: &[(String, i64)],
        groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
    ) -> Result<Self, DataFusionError> {
        let max_parallelism = config.max_parallelism;
        Ok(Self {
            table: RocksRawStore::open(config, sources, groups, aligned)?,
            max_parallelism,
            schema: std::sync::OnceLock::new(),
            work: std::cell::RefCell::new(None),
            work_size: std::cell::Cell::new(0),
            work_active: std::cell::Cell::new(false),
            sweep_cursor: std::cell::RefCell::new(None),
            last_sweep: std::cell::Cell::new(i64::MIN),
        })
    }
    pub(in crate::sink_materializer) fn legacy_partitions(
        &self,
    ) -> Result<Vec<Vec<u8>>, DataFusionError> {
        // Former snapshot-store keys are exactly the four-byte key group. Point reads avoid
        // walking the candidate tables when adopting a new record-level checkpoint.
        (0..self.max_parallelism as i32)
            .filter_map(|group| self.get(&group.to_be_bytes()).transpose())
            .collect()
    }
    pub(in crate::sink_materializer) fn clear_legacy(&self) -> Result<(), DataFusionError> {
        self.table.apply(
            (0..self.max_parallelism as i32).map(|group| (group.to_be_bytes().to_vec(), None)),
        )
    }
    pub(in crate::sink_materializer) fn descriptor(
        &self,
    ) -> Result<Option<(SchemaRef, i64)>, DataFusionError> {
        use base64::Engine;
        self.get(DESCRIPTOR)?
            .map(|bytes| {
                let (schema, ttl): (String, i64) =
                    serde_json::from_slice(&bytes).expect("materializer descriptor");
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(schema)
                    .expect("materializer schema");
                Ok((read_ipc(&bytes)[0].schema(), ttl))
            })
            .transpose()
    }
    pub(in crate::sink_materializer) fn install_schema(
        &self,
        schema: &SchemaRef,
        ttl: i64,
    ) -> Result<(), DataFusionError> {
        if let Some(old) = self.schema.get() {
            assert_eq!(old, schema, "RocksDB materializer schema changed");
            return Ok(());
        }
        if let Some((old, _)) = self.descriptor()? {
            assert_eq!(&old, schema, "RocksDB materializer schema changed");
        }
        let descriptor = serde_json::to_vec(&(encode_schema_metadata(schema), ttl)).unwrap();
        self.table
            .apply([(DESCRIPTOR.to_vec(), Some(descriptor))])?;
        let _ = self.schema.set(schema.clone());
        Ok(())
    }
    pub(in crate::sink_materializer) fn keys(&self) -> Result<Vec<ByteKey>, DataFusionError> {
        let mut keys = Vec::new();
        for item in self.table.db.iterator(IteratorMode::Start) {
            let (key, _) = item.map_err(error)?;
            if key.len() < 9 || key[..4] == [255; 4] {
                continue;
            }
            let len = u32::from_be_bytes(key[4..8].try_into().unwrap()) as usize;
            if key.len() == 9 + len && key[8 + len] == 0 {
                keys.push(ByteKey::from(&key[8..8 + len]));
            }
        }
        Ok(keys)
    }
    pub(in crate::sink_materializer) fn adopt_ttl(&self, now: i64) -> Result<(), DataFusionError> {
        for key in self.keys()? {
            let prefix = self.prefix(&key.0);
            let mut meta = self.meta(&prefix)?.unwrap();
            meta.written = now;
            self.write_meta(&prefix, &meta)?;
        }
        Ok(())
    }
    pub(in crate::sink_materializer) fn read(
        &self,
        key: &[u8],
        codec: &RowsCodec,
    ) -> Result<Candidates, DataFusionError> {
        let prefix = self.prefix(key);
        let meta = self.meta(&prefix)?.expect("stored materializer group");
        let mut linked = Linked {
            high: meta.high,
            size: meta.size,
            ..Default::default()
        };
        let mut values = Values::default();
        for item in self
            .table
            .db
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (record, bytes) = item.map_err(error)?;
            if !record.starts_with(&prefix) {
                break;
            }
            let suffix = &record[prefix.len() + 1..];
            match record[prefix.len()] {
                VALUE => values.put(codec.candidate(Arc::from(bytes.as_ref()), None, None), None),
                NODE => {
                    let seq =
                        (u64::from_be_bytes(suffix.try_into().unwrap()) ^ (1u64 << 63)) as i64;
                    let mut node = self.node(&prefix, seq, codec)?.unwrap();
                    node.row = codec.candidate(node.row.payload.clone(), None, None);
                    linked.put(node);
                }
                INDEX => {
                    let identity = codec.decode_identity(&bytes[16..]);
                    linked.add_index(
                        codec.hash_key(&identity, Some(suffix)),
                        identity,
                        Some(Arc::from(suffix)),
                        integer(&bytes),
                        integer(&bytes[8..]),
                    );
                }
                _ => {}
            }
        }
        Ok(Candidates {
            storage: if meta.linked {
                Storage::Linked(linked)
            } else {
                Storage::Value(values)
            },
            written_at: meta.written,
        })
    }
    pub(in crate::sink_materializer) fn import(
        &self,
        key: &[u8],
        rows: &Candidates,
        codec: &RowsCodec,
    ) -> Result<(), DataFusionError> {
        let prefix = self.prefix(key);
        self.table.clear_prefix(&prefix)?;
        let mut meta = Meta::new(Strategy::Value);
        meta.written = rows.written_at;
        match &rows.storage {
            Storage::Value(values) => {
                for (seq, row) in values.rows.iter().enumerate() {
                    self.table.apply([(
                        Self::seq(&prefix, VALUE, seq as i64),
                        Some(row.payload.to_vec()),
                    )])?;
                }
                meta.size = values.rows.len() as i64;
                meta.next = meta.size;
            }
            Storage::Linked(linked) => {
                meta.linked = true;
                meta.size = linked.size;
                meta.high = linked.high;
                for (&seq, node) in &linked.nodes {
                    self.table
                        .apply([(Self::seq(&prefix, NODE, seq), Some(Self::encode_node(node)))])?;
                }
                for entry in linked.index.values().flatten() {
                    let binary = entry
                        .serialized
                        .clone()
                        .or_else(|| codec.serialize_identity(&entry.identity))
                        .expect("RocksDB serialized index");
                    let mut bytes = entry.first.to_le_bytes().to_vec();
                    bytes.extend_from_slice(&entry.last.to_le_bytes());
                    bytes.extend_from_slice(&codec.encode_identity(&entry.identity));
                    self.table
                        .apply([(Self::key(&prefix, INDEX, &binary), Some(bytes))])?;
                }
            }
        }
        self.write_meta(&prefix, &meta)
    }
    pub(in crate::sink_materializer) fn sweep_expired(
        &self,
        ttl: StateTtl,
        interval: i64,
    ) -> Result<(), DataFusionError> {
        if !ttl.enabled()
            || self.sweep_cursor.borrow().is_none()
                && ttl.now() < self.last_sweep.get().saturating_add(interval)
        {
            return Ok(());
        }
        // Seek past each key's candidate tables, so cleanup never loads a large candidate set.
        for _ in 0..64 {
            let cursor = self.sweep_cursor.borrow().clone().unwrap_or_default();
            let next = self
                .table
                .db
                .iterator(IteratorMode::From(&cursor, Direction::Forward))
                .next()
                .transpose()
                .map_err(error)?;
            let Some((key, _)) = next else {
                *self.sweep_cursor.borrow_mut() = None;
                self.last_sweep.set(ttl.now());
                return Ok(());
            };
            if key.len() < 9 || key[..4] == [255; 4] {
                *self.sweep_cursor.borrow_mut() = None;
                self.last_sweep.set(ttl.now());
                return Ok(());
            }
            let len = u32::from_be_bytes(key[4..8].try_into().unwrap()) as usize;
            let prefix = &key[..8 + len];
            if self
                .meta(prefix)?
                .is_some_and(|meta| ttl.expired(meta.written))
            {
                self.table.clear_prefix(prefix)?;
            }
            *self.sweep_cursor.borrow_mut() = Some(RocksRawStore::prefix_end(prefix));
        }
        Ok(())
    }
    fn prefix(&self, key: &[u8]) -> Vec<u8> {
        let group = flink_key_group(hash_bytes_by_words(key), self.max_parallelism) as i32;
        let mut out = group.to_be_bytes().to_vec();
        out.extend_from_slice(&(key.len() as u32).to_be_bytes());
        out.extend_from_slice(key);
        out
    }
    fn key(prefix: &[u8], table: u8, suffix: &[u8]) -> Vec<u8> {
        let mut key = prefix.to_vec();
        key.push(table);
        key.extend_from_slice(suffix);
        key
    }
    fn seq(prefix: &[u8], table: u8, seq: i64) -> Vec<u8> {
        Self::key(prefix, table, &((seq as u64) ^ (1u64 << 63)).to_be_bytes())
    }
    pub(in crate::sink_materializer) fn attach_work(&self, memory: Option<MemoryReservation>) {
        *self.work.borrow_mut() = memory;
    }
    fn reserve_work(&self, bytes: usize) -> Result<(), DataFusionError> {
        if self.work_active.get() {
            if let Some(memory) = self.work.borrow_mut().as_mut() {
                if bytes > memory.size() {
                    memory.try_resize(bytes)?;
                }
            }
        }
        Ok(())
    }
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DataFusionError> {
        let value = self.table.db.get_pinned(key).map_err(error)?;
        if self.work_active.get() {
            let bytes = self.work_size.get().saturating_add(
                value
                    .as_ref()
                    .map_or(0, |v| (v.len() + key.len()).saturating_mul(3)),
            );
            self.reserve_work(bytes)?;
            self.work_size.set(bytes);
        }
        Ok(value.map(|v| v.to_vec()))
    }
    pub(in crate::sink_materializer) fn live(
        &self,
        key: &[u8],
        ttl: StateTtl,
    ) -> Result<bool, DataFusionError> {
        Ok(self
            .meta(&self.prefix(key))?
            .is_some_and(|m| !ttl.expired(m.written)))
    }

    fn meta(&self, prefix: &[u8]) -> Result<Option<Meta>, DataFusionError> {
        Ok(self
            .get(&Self::key(prefix, 0, &[]))?
            .map(|bytes| Meta::decode(&bytes)))
    }
    fn write_meta(&self, prefix: &[u8], meta: &Meta) -> Result<(), DataFusionError> {
        self.table
            .apply([(Self::key(prefix, 0, &[]), Some(meta.encode()))])
    }
    fn node(
        &self,
        prefix: &[u8],
        seq: i64,
        _codec: &RowsCodec,
    ) -> Result<Option<Node>, DataFusionError> {
        Ok(self.get(&Self::seq(prefix, NODE, seq))?.map(|bytes| Node {
            links: Links {
                seq,
                prev: read_option(&bytes),
                next: read_option(&bytes[9..]),
                next_for: read_option(&bytes[18..]),
            },
            row: Candidate {
                payload: Arc::from(&bytes[27..]),
                identity: Identity::Bytes(Arc::from([])),
                serialized: None,
                hash: 0,
            },
        }))
    }
    fn encode_node(node: &Node) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(27 + node.row.payload.len());
        option(&mut bytes, node.links.prev);
        option(&mut bytes, node.links.next);
        option(&mut bytes, node.links.next_for);
        bytes.extend_from_slice(&node.row.payload);
        bytes
    }
    fn linked(
        &self,
        prefix: &[u8],
        meta: &mut Meta,
        query: &Candidate,
        add: Option<Candidate>,
        replace: bool,
        codec: &RowsCodec,
    ) -> Result<(bool, Action), DataFusionError> {
        let binary = query
            .serialized
            .as_deref()
            .expect("RocksDB MAP serialized key");
        let index_key = Self::key(prefix, INDEX, binary);
        let found = self.get(&index_key)?;
        if add.is_none() && found.is_none() {
            return Ok((false, Action::Silent));
        }
        let mut linked = Linked {
            high: meta.high,
            size: meta.size,
            ..Default::default()
        };
        let mut required = Vec::new();
        if let Some(bytes) = found {
            let first = integer(&bytes);
            let last = integer(&bytes[8..]);
            linked.add_index(
                query.hash,
                Identity::Bytes(Arc::from(&bytes[16..])),
                Some(Arc::from(binary)),
                first,
                last,
            );
            required.extend([first, last]);
            if add.is_none() {
                let node = self
                    .node(prefix, first, codec)?
                    .expect("linked removal node");
                required.extend([node.links.prev, node.links.next].into_iter().flatten());
            }
        }
        if let Some(high) = meta.high {
            required.extend([high, high.wrapping_add(1)]);
        }
        required.sort_unstable();
        required.dedup();
        for seq in &required {
            if let Some(node) = self.node(prefix, *seq, codec)? {
                linked.put(node);
            }
        }
        let result = if let Some(candidate) = add {
            linked.add(candidate, replace, codec);
            (true, Action::Silent)
        } else {
            linked.remove(&query.identity, Some(binary), query.hash, codec)
        };
        meta.high = linked.high;
        meta.size = linked.size;
        if linked.high.is_none() {
            self.table.clear_prefix(prefix)?;
            return Ok(result);
        }
        let mut changes = Vec::new();
        for seq in required {
            if !linked.nodes.contains_key(&seq) {
                changes.push((Self::seq(prefix, NODE, seq), None));
            }
        }
        for (seq, node) in linked.nodes {
            changes.push((Self::seq(prefix, NODE, seq), Some(Self::encode_node(&node))));
        }
        let entry = linked
            .index
            .get(&query.hash)
            .and_then(|entries| entries.first());
        let bytes = entry.map(|entry| {
            let mut bytes = entry.first.to_le_bytes().to_vec();
            bytes.extend_from_slice(&entry.last.to_le_bytes());
            bytes.extend_from_slice(&codec.encode_identity(&entry.identity));
            bytes
        });
        changes.push((index_key, bytes));
        self.table.apply(changes)?;
        Ok(result)
    }
    fn last_value(&self, prefix: &[u8]) -> Result<Option<(Vec<u8>, Arc<[u8]>)>, DataFusionError> {
        let start = Self::key(prefix, NODE, &[]);
        let values = Self::key(prefix, VALUE, &[]);
        if let Some(item) = self
            .table
            .db
            .iterator(IteratorMode::From(&start, Direction::Reverse))
            .next()
        {
            let (key, value) = item.map_err(error)?;
            return Ok(key
                .starts_with(&values)
                .then(|| (key.to_vec(), Arc::from(value.as_ref()))));
        }
        Ok(None)
    }
    fn values(
        &self,
        prefix: &[u8],
        meta: &mut Meta,
        query: &Candidate,
        add: Option<Candidate>,
        replace: bool,
        strategy: Strategy,
        codec: &RowsCodec,
    ) -> Result<(bool, Action), DataFusionError> {
        let start = Self::key(prefix, VALUE, &[]);
        let mut found = None;
        if add.is_none() || replace {
            for item in self
                .table
                .db
                .iterator(IteratorMode::From(&start, Direction::Forward))
            {
                let (key, value) = item.map_err(error)?;
                if !key.starts_with(&start) {
                    break;
                }
                self.reserve_work(
                    self.work_size
                        .get()
                        .saturating_add(value.len().saturating_mul(3)),
                )?;
                let existing = codec.candidate(Arc::from(value.as_ref()), None, None);
                let equal = if add.is_some() && !matches!(strategy, Strategy::Legacy) {
                    codec.matches(&existing.identity, &query.identity)
                } else {
                    codec.matches(&query.identity, &existing.identity)
                };
                if equal {
                    found = Some(key.to_vec());
                    break;
                }
            }
        }
        if let Some(candidate) = add {
            let key = found.unwrap_or_else(|| {
                let seq = meta.next;
                meta.next = seq.wrapping_add(1);
                meta.size += 1;
                Self::seq(prefix, VALUE, seq)
            });
            self.table
                .apply([(key, Some(candidate.payload.to_vec()))])?;
            Ok((true, Action::Silent))
        } else if let Some(key) = found {
            let removed_visible = self.last_value(prefix)?.expect("visible candidate").0 == key;
            self.table.apply([(key, None)])?;
            meta.size -= 1;
            if meta.size == 0 {
                self.table.clear_prefix(prefix)?;
                return Ok((true, Action::Delete));
            }
            Ok((
                true,
                if removed_visible {
                    Action::Restore(self.last_value(prefix)?.unwrap().1)
                } else {
                    Action::Silent
                },
            ))
        } else {
            Ok((false, Action::Silent))
        }
    }
    fn switch(
        &self,
        prefix: &[u8],
        meta: &mut Meta,
        codec: &RowsCodec,
    ) -> Result<(), DataFusionError> {
        let from = if meta.linked { NODE } else { VALUE };
        let start = Self::key(prefix, from, &[]);
        if meta.linked {
            let mut count = 0;
            for item in self
                .table
                .db
                .iterator(IteratorMode::From(&start, Direction::Forward))
            {
                let (key, value) = item.map_err(error)?;
                if !key.starts_with(&start) {
                    break;
                }
                self.table
                    .apply([(Self::seq(prefix, VALUE, count), Some(value[27..].to_vec()))])?;
                count += 1;
            }
            meta.size = count;
            meta.next = count;
            meta.high = None;
            self.table.clear_prefix(&Self::key(prefix, INDEX, &[]))?;
        } else {
            meta.size = 0;
            meta.high = None;
            for item in self
                .table
                .db
                .iterator(IteratorMode::From(&start, Direction::Forward))
            {
                let (key, value) = item.map_err(error)?;
                if !key.starts_with(&start) {
                    break;
                }
                let row = codec.candidate(Arc::from(value.as_ref()), None, None);
                let query = codec.candidate(row.payload.clone(), None, None);
                self.linked(prefix, meta, &query, Some(row), false, codec)?;
            }
        }
        self.table.clear_prefix(&start)?;
        meta.linked = !meta.linked;
        Ok(())
    }
    pub(in crate::sink_materializer) fn apply(
        &self,
        key: &[u8],
        row: Candidate,
        addition: bool,
        upsert: bool,
        strategy: Strategy,
        codec: &RowsCodec,
        ttl: StateTtl,
    ) -> Result<(bool, Action), DataFusionError> {
        self.work_active.set(true);
        self.work_size.set(0);
        let _working = WorkingSet(self);
        self.work_size.set(row.bytes().saturating_mul(3));
        self.reserve_work(self.work_size.get())?;
        let prefix = self.prefix(key);
        let mut meta = self.meta(&prefix)?;
        if meta.as_ref().is_some_and(|m| ttl.expired(m.written)) {
            self.table.clear_prefix(&prefix)?;
            meta = None;
        }
        let fresh = meta.is_none();
        if !addition && fresh {
            return Ok((false, Action::Silent));
        }
        let mut meta = meta.unwrap_or_else(|| Meta::new(strategy));
        if matches!(strategy, Strategy::Map { .. }) && !meta.linked
            || matches!(strategy, Strategy::Legacy | Strategy::Value) && meta.linked
        {
            self.switch(&prefix, &mut meta, codec)?;
        }
        let query = codec.candidate(
            row.payload.clone(),
            Some(row.identity.clone()),
            row.serialized.clone(),
        );
        let result = if meta.linked {
            self.linked(
                &prefix,
                &mut meta,
                &query,
                addition.then_some(row),
                upsert,
                codec,
            )?
        } else {
            self.values(
                &prefix,
                &mut meta,
                &query,
                addition.then_some(row),
                upsert,
                strategy,
                codec,
            )?
        };
        if result.0 {
            meta.written = ttl.now();
            if meta.size != 0 {
                if let Strategy::Adaptive { high, low, .. } = strategy {
                    if !meta.linked && meta.size >= high || meta.linked && meta.size <= low {
                        self.switch(&prefix, &mut meta, codec)?;
                    }
                }
                self.write_meta(&prefix, &meta)?;
            }
        }
        Ok((fresh, result.1))
    }
}

struct WorkingSet<'a>(&'a RocksCandidates);
impl Drop for WorkingSet<'_> {
    fn drop(&mut self) {
        self.0.work_active.set(false);
        self.0.work_size.set(0);
        if let Some(memory) = self.0.work.borrow_mut().as_mut() {
            let _ = memory.try_resize(0);
        }
    }
}
