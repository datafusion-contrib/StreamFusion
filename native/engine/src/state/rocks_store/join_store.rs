use super::*;
use crate::updating_join::{JoinProbe, JoinRowRef, JoinStateStore};

const RECORD_MARKER: &[u8] = b"\xff\xff\xff\xffJOIN\x01";

struct RecordIndex {
    prefix: Vec<u8>,
    write_ms: i64,
}

struct Point {
    original: Option<RowMeta>,
    current: Option<RowMeta>,
}

/// Immediate INNER joins use record-level state. Other families retain their bucket store.
pub(crate) struct RocksJoinStore {
    inner: RocksStore<JoinStateCodec>,
    records: bool,
    points: ahash::HashMap<Vec<u8>, Point>,
    indices: ahash::HashMap<ByteKey, RecordIndex>,
    replaced: ahash::HashSet<Vec<u8>>,
    probe_cache: ahash::HashMap<ByteKey, JoinBucket>,
    #[cfg(test)]
    probe_cache_bytes: Option<usize>,
    scratch: Vec<u8>,
    work_memory: Option<MemoryReservation>,
    cache_memory: Option<MemoryReservation>,
    work_bound: usize,
    cache_bound: usize,
}

impl RocksJoinStore {
    pub(crate) fn new(inner: RocksStore<JoinStateCodec>, records: bool) -> Self {
        Self {
            inner,
            records,
            points: ahash::HashMap::default(),
            indices: ahash::HashMap::default(),
            replaced: ahash::HashSet::default(),
            probe_cache: ahash::HashMap::default(),
            #[cfg(test)]
            probe_cache_bytes: None,
            work_memory: None,
            cache_memory: None,
            work_bound: 0,
            cache_bound: 0,
            scratch: Vec::new(),
        }
    }

    pub(crate) fn prepare_import(&mut self, bytes: usize) -> Result<(), DataFusionError> {
        if self.records {
            self.reserve_work(bytes.saturating_mul(4).saturating_add(4096))?;
        }
        Ok(())
    }

    pub(crate) fn set_clock(&mut self, now: i64) {
        self.inner.set_clock(now);
    }

    pub(crate) fn checkpoint_pair(
        left: &mut Self,
        right: &mut Self,
        directory: &str,
    ) -> Result<RocksCheckpointManifest, DataFusionError> {
        left.end_bundle()?;
        right.end_bundle()?;
        RocksStore::checkpoint_pair(&mut left.inner, &mut right.inner, directory)
    }

    fn reserve_work(&mut self, additional: usize) -> Result<(), DataFusionError> {
        let required = self.work_bound.saturating_add(additional);
        if let Some(memory) = &self.work_memory {
            if required > memory.size() {
                let capacity = required.checked_next_power_of_two().unwrap_or(required);
                if memory.try_resize(capacity).is_err() {
                    memory.try_resize(required)?;
                }
            }
        }
        self.work_bound = required;
        Ok(())
    }

    fn prefix(&self, key: &[u8]) -> Vec<u8> {
        let mut prefix = self.inner.db_key(key);
        prefix[4] += 2;
        prefix.splice(5..5, (key.len() as u32).to_be_bytes());
        prefix
    }

    fn marker(&self, write_ms: i64) -> Vec<u8> {
        let mut marker = Vec::with_capacity(self.inner.value_prefix_len() + RECORD_MARKER.len());
        if self.inner.config.ttl_ms > 0 {
            marker.extend_from_slice(&write_ms.to_le_bytes());
        }
        marker.extend_from_slice(RECORD_MARKER);
        marker
    }

    fn ensure_key(&mut self, key: &[u8]) -> Result<(), DataFusionError> {
        if self.indices.contains_key(key) {
            return Ok(());
        }
        self.reserve_work(key.len().saturating_mul(4).saturating_add(512))?;
        let db = Arc::clone(&self.inner.db);
        let db_key = self.inner.db_key(key);
        let prefix = self.prefix(key);
        let value = db.get_pinned(&db_key).map_err(re)?;
        let mut write_ms = 0;
        if let Some(value) = value {
            let ttl_prefix = self.inner.value_prefix_len();
            if value.len() < ttl_prefix {
                return Err(invalid_record());
            }
            if ttl_prefix > 0 {
                write_ms = i64::from_le_bytes(value[..8].try_into().unwrap());
            }
            let bytes = &value[ttl_prefix..];
            if bytes != RECORD_MARKER {
                // Old checkpoints remain readable. Migrate a bucket directly from its pinned
                // byte framing, without allocating a second full multiset in Rust.
                let mut bytes = bytes;
                let rows = take_u32(&mut bytes)?;
                let bound = value
                    .len()
                    .saturating_add(rows.saturating_mul(prefix.len()));
                let migration_memory = self.work_memory.as_ref().map(MemoryReservation::new_empty);
                let write_bound = bound.min(self.inner.write_batch_size);
                let mut largest_record = 0usize;
                if let Some(memory) = &migration_memory {
                    memory.try_resize(write_bound.saturating_add(4096))?;
                }
                let mut writes = FlinkWriteBatch::new(&db, self.inner.write_batch_size);
                for _ in 0..rows {
                    let len = take_u32(&mut bytes)?;
                    let payload = take(&mut bytes, len)?;
                    let count = i64::from_le_bytes(take(&mut bytes, 8)?.try_into().unwrap());
                    let num_assoc = i32::from_le_bytes(take(&mut bytes, 4)?.try_into().unwrap());
                    let last_write_ms =
                        i64::from_le_bytes(take(&mut bytes, 8)?.try_into().unwrap());
                    largest_record = largest_record.max(prefix.len().saturating_add(payload.len()));
                    if let Some(memory) = &migration_memory {
                        memory.try_resize(
                            write_bound
                                .saturating_add(largest_record.saturating_mul(2))
                                .saturating_add(4096),
                        )?;
                    }
                    let mut record_key = prefix.clone();
                    record_key.extend_from_slice(payload);
                    writes.put(
                        record_key,
                        encode_meta(RowMeta {
                            count,
                            num_assoc,
                            last_write_ms,
                        }),
                    )?;
                }
                if !bytes.is_empty() {
                    return Err(invalid_record());
                }
                writes.put(db_key, self.marker(write_ms))?;
                writes.finish()?;
            }
        }
        self.indices
            .insert(ByteKey::from(key), RecordIndex { prefix, write_ms });
        Ok(())
    }

    fn prepare_keys(
        &mut self,
        batch: &RecordBatch,
        keys: &[usize],
        precisions: &[i32],
    ) -> Result<(), DataFusionError> {
        let mut encoder = BinaryRowBatchEncoder::new(batch, keys, precisions);
        for row in 0..batch.num_rows() {
            self.ensure_key(encoder.encode(row))?;
        }
        Ok(())
    }

    fn probe_limit_exceeded(&self, _bytes: usize) -> bool {
        #[cfg(test)]
        {
            self.probe_cache_bytes.is_some_and(|limit| _bytes > limit)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    fn cache_probe(&mut self, key: &ByteKey) -> Result<(), DataFusionError> {
        let prefix = &self.indices.get(key).expect("prepared key").prefix;
        let mut iterator = self.inner.db.raw_iterator();
        iterator.seek(prefix);
        let before = self.cache_bound;
        let mut bound = before.saturating_add(key.0.len()).saturating_add(256);
        if self.probe_limit_exceeded(bound)
            || self
                .cache_memory
                .as_ref()
                .is_some_and(|memory| memory.try_resize(bound).is_err())
        {
            return Ok(());
        }
        let mut bucket = JoinBucket::default();
        while iterator.valid() {
            let db_key = iterator.key().ok_or_else(invalid_record)?;
            if !db_key.starts_with(prefix) {
                break;
            }
            let payload = &db_key[prefix.len()..];
            let meta = decode_meta(iterator.value().ok_or_else(invalid_record)?)?;
            let expired = self.inner.config.ttl_ms > 0
                && self.inner.now_ms >= meta.last_write_ms.saturating_add(self.inner.config.ttl_ms);
            if !expired {
                bound = bound.saturating_add(payload.len()).saturating_add(192);
                if self.probe_limit_exceeded(bound)
                    || self
                        .cache_memory
                        .as_ref()
                        .is_some_and(|memory| memory.try_resize(bound).is_err())
                {
                    drop(bucket);
                    if let Some(memory) = &self.cache_memory {
                        memory.try_resize(before)?;
                    }
                    return Ok(());
                }
                bucket.insert(ByteKey::from(payload), meta);
            }
            iterator.next();
        }
        iterator.status().map_err(re)?;
        if let Some(memory) = &self.cache_memory {
            memory.try_resize(bound)?;
        }
        self.cache_bound = bound;
        self.probe_cache.insert(key.clone(), bucket);
        Ok(())
    }

    fn commit_points(&mut self) -> Result<(), DataFusionError> {
        let db = Arc::clone(&self.inner.db);
        let mut writes = FlinkWriteBatch::new(&db, self.inner.write_batch_size);
        for prefix in &self.replaced {
            writes.delete_range(prefix, &prefix_successor(prefix))?;
        }
        let mut touched = ahash::HashMap::<ByteKey, bool>::default();
        for (db_key, point) in &self.points {
            let len = u32::from_be_bytes(db_key[5..9].try_into().unwrap()) as usize;
            let key = &db_key[9..9 + len];
            let prefix = &db_key[..9 + len];
            if point.original == point.current && !self.replaced.contains(prefix) {
                continue;
            }
            let retained = touched.entry(ByteKey::from(key)).or_default();
            *retained |= point.current.is_some();
            match point.current {
                Some(meta) => writes.put(db_key, encode_meta(meta))?,
                None => writes.delete(db_key)?,
            }
        }
        writes.finish()?;
        let mut markers = FlinkWriteBatch::new(&db, self.inner.write_batch_size);
        for (key, retained) in touched {
            let index = self.indices.get(&key).expect("prepared input key");
            let present = if retained {
                true
            } else {
                let mut iterator = db.raw_iterator();
                iterator.seek(&index.prefix);
                let present = iterator.valid()
                    && iterator
                        .key()
                        .is_some_and(|key| key.starts_with(&index.prefix));
                iterator.status().map_err(re)?;
                present
            };
            if present {
                markers.put(self.inner.db_key(&key.0), self.marker(index.write_ms))?;
            } else {
                markers.delete(self.inner.db_key(&key.0))?;
            }
        }
        markers.finish()
    }

    fn commit_imported_buckets(&mut self) -> Result<(), DataFusionError> {
        let db = Arc::clone(&self.inner.db);
        let mut writes = FlinkWriteBatch::new(&db, self.inner.write_batch_size);
        for (key, slot) in &self.inner.working {
            match slot {
                Slot::Present { state, dirty: true } => {
                    let prefix = self.prefix(&key.0);
                    writes.delete_range(&prefix, &prefix_successor(&prefix))?;
                    for (payload, meta) in state {
                        let mut db_key = prefix.clone();
                        db_key.extend_from_slice(&payload.0);
                        writes.put(db_key, encode_meta(*meta))?;
                    }
                    if state.is_empty() {
                        writes.delete(self.inner.db_key(&key.0))?;
                    } else {
                        writes.put(
                            self.inner.db_key(&key.0),
                            self.marker(self.inner.codec.write_ms(state)),
                        )?;
                    }
                }
                Slot::Absent { dirty: true } => {
                    let prefix = self.prefix(&key.0);
                    writes.delete_range(&prefix, &prefix_successor(&prefix))?;
                    writes.delete(self.inner.db_key(&key.0))?;
                }
                _ => {}
            }
        }
        writes.finish()?;
        self.inner.working = ahash::HashMap::default();
        Ok(())
    }

    pub(crate) fn canonical_keys_by_group(
        &mut self,
    ) -> Result<BTreeMap<i32, Vec<ByteKey>>, DataFusionError> {
        if !self.records {
            return self.inner.canonical_keys_by_group();
        }
        self.end_bundle()?;
        let db = Arc::clone(&self.inner.db);
        let side = self.inner.table.expect("paired join table");
        let mut groups: BTreeMap<i32, Vec<ByteKey>> = BTreeMap::new();
        for item in db.iterator(IteratorMode::Start) {
            let (db_key, value) = item.map_err(re)?;
            if db_key.len() < 5 || db_key[4] != side {
                continue;
            }
            let group = i32::from_be_bytes(db_key[..4].try_into().unwrap());
            if group < 0 {
                continue;
            }
            let key = ByteKey::from(&db_key[5..]);
            let prefix_len = self.inner.value_prefix_len();
            if value.len() < prefix_len {
                return Err(invalid_record());
            }
            let state = if &value[prefix_len..] == RECORD_MARKER {
                let prefix = self.prefix(&key.0);
                let mut records = db.raw_iterator();
                records.seek(&prefix);
                let mut bucket = JoinBucket::default();
                while records.valid() {
                    let record_key = records.key().ok_or_else(invalid_record)?;
                    if !record_key.starts_with(&prefix) {
                        break;
                    }
                    let payload = &record_key[prefix.len()..];
                    self.reserve_work(payload.len().saturating_add(192))?;
                    bucket.insert(
                        ByteKey::from(payload),
                        decode_meta(records.value().ok_or_else(invalid_record)?)?,
                    );
                    records.next();
                }
                records.status().map_err(re)?;
                bucket
            } else {
                self.reserve_work(value.len().saturating_mul(3).saturating_add(256))?;
                self.inner.decode_value(&value)?.unwrap_or_default()
            };
            if !state.is_empty() {
                self.inner.working.insert(
                    key.clone(),
                    Slot::Present {
                        state,
                        dirty: false,
                    },
                );
                groups.entry(group).or_default().push(key);
            }
        }
        Ok(groups)
    }

    pub(crate) fn finish_canonical_scan(&mut self) {
        self.inner.working = ahash::HashMap::default();
        self.work_bound = 0;
        if let Some(memory) = &self.work_memory {
            memory.try_resize(0).expect("release JOIN canonical memory");
        }
    }
}

impl KeyedStateStore<JoinBucket> for RocksJoinStore {
    fn contains(&self, key: &[u8]) -> bool {
        self.inner.contains(key)
    }
    fn get(&self, key: &[u8]) -> Option<&JoinBucket> {
        self.inner.get(key)
    }
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut JoinBucket> {
        self.inner.get_mut(key)
    }
    fn insert(&mut self, key: ByteKey, value: JoinBucket) -> &mut JoinBucket {
        self.inner.insert(key, value)
    }
    fn remove(&mut self, key: &[u8]) {
        self.inner.remove(key);
    }
    fn begin_batch(
        &mut self,
        batch: &RecordBatch,
        keys: &[usize],
        precisions: &[i32],
    ) -> Result<(), DataFusionError> {
        self.inner.begin_batch(batch, keys, precisions)
    }
    fn end_bundle(&mut self) -> Result<(), DataFusionError> {
        if !self.records {
            return self.inner.end_bundle();
        }
        self.commit_points()?;
        self.commit_imported_buckets()?;
        self.points = ahash::HashMap::default();
        self.indices = ahash::HashMap::default();
        self.replaced = ahash::HashSet::default();
        self.probe_cache = ahash::HashMap::default();
        self.scratch = Vec::new();
        self.work_bound = 0;
        self.cache_bound = 0;
        if let Some(memory) = &self.work_memory {
            memory.try_resize(0)?;
        }
        if let Some(memory) = &self.cache_memory {
            memory.try_resize(0)?;
        }
        Ok(())
    }
    fn footprint_delta(&mut self) -> isize {
        self.inner.footprint_delta()
    }
}

impl JoinStateStore for RocksJoinStore {
    fn attach_read_memory(&mut self, reservation: Option<MemoryReservation>) {
        if self.records {
            self.cache_memory = reservation.as_ref().map(MemoryReservation::new_empty);
            self.work_memory = reservation;
        }
    }

    fn prepare_inner_input(
        &mut self,
        batch: &RecordBatch,
        keys: &[usize],
        precisions: &[i32],
        payloads: &Rows,
    ) -> Result<(), DataFusionError> {
        if !self.records {
            return self.inner.begin_batch(batch, keys, precisions);
        }
        self.prepare_keys(batch, keys, precisions)?;
        let mut encoder = BinaryRowBatchEncoder::new(batch, keys, precisions);
        let mut probes = Vec::new();
        for row in 0..batch.num_rows() {
            let key = encoder.encode(row);
            let prefix = &self.indices.get(key).expect("prepared input key").prefix;
            let payload = payloads.row(row);
            let size = prefix.len().saturating_add(payload.as_ref().len());
            self.reserve_work(size.saturating_mul(3).saturating_add(256))?;
            let mut db_key = self.indices.get(key).unwrap().prefix.clone();
            db_key.extend_from_slice(payload.as_ref());
            if !self.points.contains_key(db_key.as_slice()) {
                probes.push(db_key.clone());
                self.points.insert(
                    db_key,
                    Point {
                        original: None,
                        current: None,
                    },
                );
            }
        }
        let db = Arc::clone(&self.inner.db);
        let fetched = multi_get_pinned(&db, &probes);
        for (key, value) in probes.iter().zip(fetched) {
            let meta = value
                .map_err(re)?
                .map(|value| decode_meta(&value))
                .transpose()?;
            let point = self.points.get_mut(key).expect("prepared point");
            point.original = meta;
            point.current = meta;
        }
        Ok(())
    }

    fn prepare_inner_probe(
        &mut self,
        batch: &RecordBatch,
        keys: &[usize],
        precisions: &[i32],
    ) -> Result<(), DataFusionError> {
        if !self.records {
            return self.inner.begin_batch(batch, keys, precisions);
        }
        self.prepare_keys(batch, keys, precisions)?;
        if !self.probe_limit_exceeded(1) {
            let selected: Vec<_> = self.indices.keys().cloned().collect();
            for key in selected {
                self.cache_probe(&key)?;
            }
        }
        Ok(())
    }

    fn inner_probe(&self, key: &[u8]) -> Result<JoinProbe<'_>, DataFusionError> {
        if !self.records {
            return Ok(self.inner.get(key).map_or(JoinProbe::Empty, |bucket| {
                JoinProbe::Resident(bucket.iter())
            }));
        }
        if let Some(bucket) = self.probe_cache.get(key) {
            return Ok(JoinProbe::Resident(bucket.iter()));
        }
        let prefix = self
            .indices
            .get(key)
            .expect("prepared probe key")
            .prefix
            .clone();
        let memory = self.work_memory.as_ref().map(MemoryReservation::new_empty);
        if let Some(memory) = &memory {
            memory.try_resize(prefix.len().saturating_add(4096))?;
        }
        let mut iterator = self.inner.db.raw_iterator();
        iterator.seek(&prefix);
        Ok(JoinProbe::Stored(RocksJoinProbe {
            iterator,
            prefix,
            memory,
            started: false,
            done: false,
            ttl: StateTtl::new(self.inner.config.ttl_ms, self.inner.now_ms),
        }))
    }

    fn apply_inner_input(
        &mut self,
        key: &[u8],
        payload: &[u8],
        kind: i8,
        unique: bool,
        ttl: StateTtl,
    ) -> Result<bool, DataFusionError> {
        if !self.records {
            return Ok(false);
        }
        let index = self.indices.get(key).expect("prepared input key");
        self.scratch.clear();
        self.scratch.extend_from_slice(&index.prefix);
        self.scratch.extend_from_slice(payload);
        if unique && matches!(kind, 0 | 2) {
            let prefix = index.prefix.clone();
            self.replaced.insert(prefix.clone());
            for (key, point) in &mut self.points {
                if key.starts_with(&prefix) {
                    point.current = None;
                }
            }
        }
        let point = self
            .points
            .get_mut(self.scratch.as_slice())
            .expect("prepared input record");
        if matches!(kind, 0 | 2) {
            let mut meta = point
                .current
                .filter(|meta| !ttl.expired(meta.last_write_ms))
                .unwrap_or(RowMeta {
                    count: 0,
                    num_assoc: -1,
                    last_write_ms: 0,
                });
            meta.count += 1;
            if ttl.enabled() {
                meta.last_write_ms = ttl.now();
            }
            point.current = Some(meta);
        } else if let Some(mut meta) = point.current {
            meta.count -= 1;
            if ttl.expired(meta.last_write_ms) || meta.count <= 0 {
                point.current = None;
            } else {
                if ttl.enabled() {
                    meta.last_write_ms = ttl.now();
                }
                point.current = Some(meta);
            }
        }
        if let Some(meta) = point.current {
            let index = self.indices.get_mut(key).unwrap();
            index.write_ms = index.write_ms.max(meta.last_write_ms);
        }
        Ok(true)
    }
}

pub(crate) struct RocksJoinProbe<'a> {
    iterator: rocksdb::DBRawIterator<'a>,
    prefix: Vec<u8>,
    memory: Option<MemoryReservation>,
    started: bool,
    done: bool,
    ttl: StateTtl,
}

impl Iterator for RocksJoinProbe<'_> {
    type Item = Result<(JoinRowRef<'static>, RowMeta), DataFusionError>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            }
            if self.started {
                self.iterator.next();
            }
            self.started = true;
            if !self.iterator.valid() {
                self.done = true;
                return self.iterator.status().err().map(|error| Err(re(error)));
            }
            let key = match self.iterator.key() {
                Some(key) if key.starts_with(&self.prefix) => key,
                _ => {
                    self.done = true;
                    return None;
                }
            };
            let meta = match self
                .iterator
                .value()
                .ok_or_else(invalid_record)
                .and_then(decode_meta)
            {
                Ok(meta) => meta,
                Err(error) => return Some(Err(error)),
            };
            if self.ttl.expired(meta.last_write_ms) || meta.count <= 0 {
                continue;
            }
            let payload = &key[self.prefix.len()..];
            if let Some(memory) = &self.memory {
                if let Err(error) = memory.try_resize(
                    self.prefix
                        .len()
                        .saturating_add(payload.len())
                        .saturating_add(4096),
                ) {
                    // A drained output chunk can make room. Retry this same record next time.
                    self.started = false;
                    return Some(Err(error));
                }
            }
            return Some(Ok((
                JoinRowRef::Stored(Arc::new(ByteKey::from(payload))),
                meta,
            )));
        }
    }
}

fn invalid_record() -> DataFusionError {
    DataFusionError::Execution("malformed persistent JOIN record".into())
}
fn take<'a>(bytes: &mut &'a [u8], len: usize) -> Result<&'a [u8], DataFusionError> {
    if len > bytes.len() {
        return Err(invalid_record());
    }
    let (head, tail) = bytes.split_at(len);
    *bytes = tail;
    Ok(head)
}
fn take_u32(bytes: &mut &[u8]) -> Result<usize, DataFusionError> {
    Ok(u32::from_le_bytes(take(bytes, 4)?.try_into().unwrap()) as usize)
}
fn encode_meta(meta: RowMeta) -> [u8; 20] {
    let mut bytes = [0; 20];
    bytes[..8].copy_from_slice(&meta.last_write_ms.to_le_bytes());
    bytes[8..16].copy_from_slice(&meta.count.to_le_bytes());
    bytes[16..].copy_from_slice(&meta.num_assoc.to_le_bytes());
    bytes
}
fn decode_meta(bytes: &[u8]) -> Result<RowMeta, DataFusionError> {
    if bytes.len() != 20 {
        return Err(invalid_record());
    }
    Ok(RowMeta {
        last_write_ms: i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        count: i64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        num_assoc: i32::from_le_bytes(bytes[16..].try_into().unwrap()),
    })
}

#[cfg(test)]
mod tests;
