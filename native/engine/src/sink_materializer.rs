use crate::*;
mod equal;
mod hash;
mod serialized;
mod state;
use state::{Action, Candidates, Strategy};

#[cfg(test)]
mod tests;

#[derive(Clone)]
enum Identity {
    Bytes(Arc<[u8]>),
    Values(Vec<ScalarValue>),
}

impl Identity {
    fn matches(&self, other: &Self, fields: &[Arc<Field>], full_row: bool) -> bool {
        match (self, other) {
            (Self::Bytes(a), Self::Bytes(b)) => a == b,
            (Self::Values(a), Self::Values(b)) => equal::rows(fields, a, b, full_row),
            _ => unreachable!("one identity representation per materializer"),
        }
    }

    fn bytes(&self) -> usize {
        match self {
            Self::Bytes(bytes) => bytes.len(),
            Self::Values(values) => scalar_row_bytes(values),
        }
    }
}

struct Candidate {
    hash: i32,
    serialized: Option<Arc<[u8]>>,
    payload: Arc<[u8]>,
    identity: Identity,
}

impl Candidate {
    fn bytes(&self) -> usize {
        self.payload.len() + self.identity.bytes() + self.serialized.as_ref().map_or(0, |s| s.len())
    }
}

struct RowsCodec {
    schema: SchemaRef,
    payload: RowConverter,
    identity: RowConverter,
    match_columns: Vec<usize>,
    match_fields: Vec<Arc<Field>>,
    byte_equality: bool,
    compute_hash: bool,
    full_row: bool,
    serialized: bool,
    match_precisions: Vec<i32>,
}

impl RowsCodec {
    fn new(
        schema: SchemaRef,
        upsert_columns: &[usize],
        strategy: Strategy,
        match_precisions: &[i32],
    ) -> Self {
        let match_columns = if upsert_columns.is_empty() {
            (0..schema.fields().len()).collect()
        } else {
            upsert_columns.to_vec()
        };
        let types: Vec<_> = match_columns
            .iter()
            .map(|&i| schema.field(i).data_type().clone())
            .collect();
        // Full rows arrive as BinaryRowData after Flink's required keyBy. Projected upsert keys
        // instead use the generated field equaliser, whose primitive floating equality differs.
        let byte_equality = types.iter().all(|t| {
            !matches!(
                t,
                DataType::Struct(_)
                    | DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(..)
                    | DataType::Map(..)
            ) && (upsert_columns.is_empty()
                || !matches!(t, DataType::Float16 | DataType::Float32 | DataType::Float64))
        });
        Self {
            match_fields: match_columns
                .iter()
                .map(|&i| schema.fields()[i].clone())
                .collect(),
            payload: RowConverter::new(
                schema
                    .fields()
                    .iter()
                    .map(|f| SortField::new(f.data_type().clone()))
                    .collect(),
            )
            .expect("materializer payload codec"),
            identity: key_row_converter_from_types(&types),
            schema,
            match_columns,
            byte_equality,
            compute_hash: strategy.hashed(),
            serialized: strategy.serialized(),
            match_precisions: match_precisions.to_vec(),
            full_row: upsert_columns.is_empty(),
        }
    }

    fn identities(&self, batch: &RecordBatch) -> Option<Rows> {
        self.byte_equality.then(|| {
            let columns: Vec<_> = self
                .match_columns
                .iter()
                .map(|&i| batch.column(i).clone())
                .collect();
            encode_group_keys(&self.identity, &columns, batch.num_rows())
        })
    }

    fn identity(&self, batch: &RecordBatch, row: usize, encoded: Option<&Rows>) -> Identity {
        match encoded {
            Some(rows) => Identity::Bytes(Arc::from(rows.row(row).data())),
            None => Identity::Values(
                self.match_columns
                    .iter()
                    .map(|&i| {
                        ScalarValue::try_from_array(batch.column(i), row)
                            .expect("materializer match value")
                    })
                    .collect(),
            ),
        }
    }

    fn decode(&self, payloads: &[Arc<[u8]>]) -> Vec<ArrayRef> {
        let parser = self.payload.parser();
        self.payload
            .convert_rows(payloads.iter().map(|row| parser.parse(row)))
            .expect("decode materializer rows")
    }

    fn candidate(
        &self,
        payload: Arc<[u8]>,
        identity: Option<Identity>,
        serialized: Option<Arc<[u8]>>,
    ) -> Candidate {
        let owned = if identity.is_none() || self.serialized && serialized.is_none() {
            Some(
                RecordBatch::try_new(
                    self.schema.clone(),
                    self.decode(std::slice::from_ref(&payload)),
                )
                .expect("owned materializer row"),
            )
        } else {
            None
        };
        let identity = identity.unwrap_or_else(|| {
            let batch = owned.as_ref().unwrap();
            self.identity(batch, 0, self.identities(batch).as_ref())
        });
        let serialized = serialized.or_else(|| {
            self.serialized.then(|| {
                let mut encoder = BinaryRowBatchEncoder::new(
                    owned.as_ref().unwrap(),
                    &self.match_columns,
                    &self.match_precisions,
                );
                Arc::from(encoder.encode(0))
            })
        });
        Candidate {
            hash: self.hash_key(&identity, serialized.as_deref()),
            payload,
            identity,
            serialized,
        }
    }

    fn matches(&self, a: &Identity, b: &Identity) -> bool {
        a.matches(b, &self.match_fields, self.full_row)
    }
    fn hash_key(&self, identity: &Identity, serialized: Option<&[u8]>) -> i32 {
        if !self.compute_hash {
            return 0;
        }
        if let Some(bytes) = serialized {
            return hash::bytes(bytes);
        }
        match identity {
            Identity::Bytes(bytes) => hash::bytes(bytes),
            Identity::Values(values) => hash::row(&self.match_fields, values),
        }
    }

    fn map_matches(
        &self,
        a: &Identity,
        a_binary: Option<&[u8]>,
        b: &Identity,
        b_binary: Option<&[u8]>,
    ) -> bool {
        if self.serialized {
            a_binary.expect("serialized query") == b_binary.expect("serialized index")
        } else {
            self.matches(a, b)
        }
    }

    fn serialize_identity(&self, identity: &Identity) -> Option<Arc<[u8]>> {
        self.serialized.then(|| {
            let arrays = match identity {
                Identity::Bytes(bytes) => {
                    let parser = self.identity.parser();
                    self.identity
                        .convert_rows([parser.parse(bytes)])
                        .expect("identity columns")
                }
                Identity::Values(values) => values
                    .iter()
                    .map(|v| v.to_array().expect("identity column"))
                    .collect(),
            };
            let schema = Arc::new(Schema::new(self.match_fields.clone()));
            let batch = RecordBatch::try_new(schema, arrays).expect("identity batch");
            let columns: Vec<_> = (0..self.match_fields.len()).collect();
            Arc::from(
                BinaryRowBatchEncoder::new(&batch, &columns, &self.match_precisions).encode(0),
            )
        })
    }

    fn encode_identity(&self, identity: &Identity) -> Arc<[u8]> {
        match identity {
            Identity::Bytes(bytes) => bytes.clone(),
            Identity::Values(values) => {
                let columns: Vec<_> = values
                    .iter()
                    .map(|v| v.to_array().expect("identity array"))
                    .collect();
                Arc::from(encode_group_keys(&self.identity, &columns, 1).row(0).data())
            }
        }
    }
    fn decode_identity(&self, bytes: &[u8]) -> Identity {
        if self.byte_equality {
            return Identity::Bytes(Arc::from(bytes));
        }
        let parser = self.identity.parser();
        let arrays = self
            .identity
            .convert_rows([parser.parse(bytes)])
            .expect("identity restore");
        Identity::Values(
            arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a, 0).expect("identity value"))
                .collect(),
        )
    }
}

/// Flink's sink upsert materializer: candidates retain insertion order, and a retract removes
/// the first matching candidate. An inferred upstream key replaces a candidate in place.
pub(crate) struct SinkMaterializer {
    key_columns: Vec<usize>,
    key_precisions: Vec<i32>,
    upsert_columns: Vec<usize>,
    ttl_ms: i64,
    strategy: Strategy,
    match_precisions: Vec<i32>,
    last_sweep: i64,
    codec: Option<RowsCodec>,
    groups: MemoryStateStore<Candidates>,
    #[cfg(feature = "rocksdb-state")]
    rocks: Option<state::rocks::RocksCandidates>,
    pub(crate) memory: OperatorMemory,
}

impl SinkMaterializer {
    pub(crate) fn new(
        keys: Vec<usize>,
        precisions: Vec<i32>,
        upsert: Vec<usize>,
        ttl_ms: i64,
    ) -> Self {
        Self {
            key_columns: keys,
            key_precisions: precisions,
            upsert_columns: upsert,
            ttl_ms: ttl_ms.max(0),
            strategy: Strategy::Legacy,
            match_precisions: vec![],
            last_sweep: 0,
            codec: None,
            groups: MemoryStateStore::default(),
            #[cfg(feature = "rocksdb-state")]
            rocks: None,
            memory: OperatorMemory::unaccounted(),
        }
    }

    fn with_strategy(mut self, strategy: Strategy, match_precisions: Vec<i32>) -> Self {
        if !matches!(strategy, Strategy::Legacy) {
            self.ttl_ms = 0;
        }
        self.strategy = strategy;
        self.match_precisions = match_precisions;
        self
    }
    #[cfg(feature = "rocksdb-state")]
    fn with_rocks(
        mut self,
        store: state::rocks::RocksCandidates,
        now: i64,
    ) -> Result<Self, DataFusionError> {
        if let Some((schema, old_ttl)) = store.descriptor()? {
            self.codec = Some(RowsCodec::new(
                schema.clone(),
                &self.upsert_columns,
                self.strategy,
                &self.match_precisions,
            ));
            if self.ttl_ms > 0 && old_ttl <= 0 {
                store.adopt_ttl(now)?;
            }
            store.install_schema(&schema, self.ttl_ms)?;
        }
        let legacy = store.legacy_partitions()?;
        self.rocks = Some(store);
        if !legacy.is_empty() {
            self.restore(&legacy, now);
            self.rocks.as_ref().unwrap().clear_legacy()?;
        }
        Ok(self)
    }

    pub(crate) fn with_budget(mut self, budget: i64) -> Result<Self, DataFusionError> {
        let bytes = self
            .groups
            .iter()
            .map(|(key, rows)| byte_key_bytes(&key.0) + rows.bytes())
            .sum();
        self.memory
            .attach("sink-upsert-materialize", budget, bytes)?;
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.rocks {
            store.attach_work(self.memory.temporary_reservation());
        }
        Ok(self)
    }

    pub(crate) fn push(
        &mut self,
        batch: &RecordBatch,
        now: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        // The required Flink keyBy serializes generic arrays before materialization. Preserve
        // that normalization without copying ordinary input columns or already canonical arrays.
        let normalized = serialized::batch(batch);
        let batch = normalized.as_ref().unwrap_or(batch);
        let ttl = StateTtl::new(self.ttl_ms, now);
        let track = self.memory.tracking();
        if ttl.enabled() && now >= self.last_sweep.saturating_add(self.ttl_ms) {
            let mut reclaimed = 0;
            self.groups.retain_live(&mut |key, rows| {
                if ttl.expired(rows.written_at) {
                    reclaimed += byte_key_bytes(key) + rows.bytes();
                    false
                } else {
                    true
                }
            });
            if track {
                self.memory.record(-(reclaimed as isize));
            }
            self.last_sweep = now;
        }
        let schema = data_schema(batch);
        let codec = self.codec.get_or_insert_with(|| {
            RowsCodec::new(
                schema.clone(),
                &self.upsert_columns,
                self.strategy,
                &self.match_precisions,
            )
        });
        assert_eq!(codec.schema, schema, "materializer input schema changed");
        let kinds = row_kind_column(batch);
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.rocks {
            store.sweep_expired(ttl, self.ttl_ms)?;
        }
        if batch.num_rows() >= 64
            && !self.upsert_columns.is_empty()
            && codec.byte_equality
            && self.upsert_columns == self.key_columns
            && kinds.is_none_or(|k| k.values().iter().all(|v| *v == 0 || *v == 2))
        {
            let mut key_encoder =
                BinaryRowBatchEncoder::new(batch, &self.key_columns, &self.key_precisions);
            let first = key_encoder.encode(0).to_vec();
            if key_encoder.encode(1) == first.as_slice() {
                return self.push_single_candidate_updates(batch, now);
            }
        }
        #[cfg(feature = "rocksdb-state")]
        if self.rocks.is_some() {
            return self.push_rocks(batch, now);
        }
        let payloads = codec
            .payload
            .convert_columns(&batch.columns()[..data_arity(batch)])
            .expect("encode materializer rows");
        let identities = codec.identities(batch);
        let kinds = row_kind_column(batch);
        let mut keys = BinaryRowBatchEncoder::new(batch, &self.key_columns, &self.key_precisions);
        let mut match_encoder = codec.serialized.then(|| {
            BinaryRowBatchEncoder::new(batch, &codec.match_columns, &codec.match_precisions)
        });
        let mut output = Vec::new();
        let mut output_kinds = Vec::new();
        let mut input_rows = Vec::new();
        for row in 0..batch.num_rows() {
            let key = keys.encode(row);
            let expired = self
                .groups
                .get(key)
                .is_some_and(|v| ttl.expired(v.written_at));
            if expired {
                if track {
                    self.memory.record(
                        -((byte_key_bytes(key) + self.groups.get(key).unwrap().bytes()) as isize),
                    );
                }
                self.groups.remove(key);
            }
            let kind = kinds.map_or(0, |k| k.value(row));
            assert!((0..=3).contains(&kind), "invalid materializer row kind");
            let identity = codec.identity(batch, row, identities.as_ref());
            let payload: Arc<[u8]> = Arc::from(payloads.row(row).data());
            let serialized: Option<Arc<[u8]>> =
                match_encoder.as_mut().map(|e| Arc::from(e.encode(row)));
            if kind == 0 || kind == 2 {
                let fresh = !self.groups.contains(key);
                let values = if fresh {
                    self.groups
                        .insert(ByteKey::from(key), Candidates::new(self.strategy))
                } else {
                    self.groups.get_mut(key).unwrap()
                };
                let before = if fresh {
                    0
                } else {
                    byte_key_bytes(key) + values.bytes()
                };
                output_kinds.push(if values.is_empty() { 0 } else { 2 });
                output.push(payload.clone());
                input_rows.push(Some(row as u32));
                // Owned key arrays must not retain unrelated rows of an incoming Arrow batch.
                let owned_identity = codec.byte_equality.then_some(identity);
                values.add(
                    codec.candidate(payload, owned_identity, serialized),
                    !self.upsert_columns.is_empty(),
                    self.strategy,
                    codec,
                );
                values.written_at = now;
                if track {
                    self.memory
                        .record((byte_key_bytes(key) + values.bytes()) as isize - before as isize);
                }
            } else if let Some(values) = self.groups.get_mut(key) {
                let before = byte_key_bytes(key) + values.bytes();
                let (found, action) = values.remove(
                    &identity,
                    serialized.as_deref(),
                    codec.hash_key(&identity, serialized.as_deref()),
                    self.strategy,
                    codec,
                );
                match action {
                    Action::Silent => {}
                    Action::Delete => {
                        output.push(payload);
                        input_rows.push(Some(row as u32));
                        output_kinds.push(3);
                    }
                    Action::Restore(payload) => {
                        output.push(payload);
                        input_rows.push(None);
                        output_kinds.push(2);
                    }
                }
                if found {
                    values.written_at = now;
                }
                let empty = values.is_empty();
                let after = if empty {
                    0
                } else {
                    byte_key_bytes(key) + values.bytes()
                };
                if track {
                    self.memory.record(after as isize - before as isize);
                }
                if empty {
                    self.groups.remove(key);
                }
            }
        }
        self.memory.account()?;
        Self::output(codec, batch, output, input_rows, output_kinds)
    }

    fn output(
        codec: &RowsCodec,
        batch: &RecordBatch,
        output: Vec<Arc<[u8]>>,
        input_rows: Vec<Option<u32>>,
        output_kinds: Vec<i8>,
    ) -> Result<RecordBatch, DataFusionError> {
        if output.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        // Ordinary upserts emit the incoming payload. Retain its buffers rather than round-trip
        // those output values through the state codec; only predecessor restoration needs decoding.
        let mut columns = if input_rows.iter().all(Option::is_some) {
            if input_rows.len() == batch.num_rows() {
                batch.columns()[..data_arity(batch)].to_vec()
            } else {
                let indices =
                    UInt32Array::from_iter_values(input_rows.into_iter().map(Option::unwrap));
                batch.columns()[..data_arity(batch)]
                    .iter()
                    .map(|column| {
                        take(column, &indices, None).expect("materializer input selection")
                    })
                    .collect()
            }
        } else {
            codec.decode(&output)
        };
        let mut fields = codec.schema.fields().to_vec();
        fields.push(Arc::new(Field::new(ROW_KIND_COLUMN, DataType::Int8, false)));
        columns.push(Arc::new(Int8Array::from(output_kinds)));
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("materialized changelog"))
    }

    #[cfg(feature = "rocksdb-state")]
    fn push_rocks(
        &mut self,
        batch: &RecordBatch,
        now: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        let codec = self.codec.as_ref().unwrap();
        let store = self.rocks.as_ref().unwrap();
        store.install_schema(&codec.schema, self.ttl_ms)?;
        let columns = &batch.columns()[..data_arity(batch)];
        let payloads = codec.payload.convert_columns(columns)?;
        let mut reservation = self.memory.temporary_reservation();
        if let Some(memory) = &mut reservation {
            memory.try_resize(payloads.size())?;
        }
        let identities = codec.identities(batch);
        let mut binary = codec.serialized.then(|| {
            BinaryRowBatchEncoder::new(batch, &codec.match_columns, &codec.match_precisions)
        });
        let mut keys = BinaryRowBatchEncoder::new(batch, &self.key_columns, &self.key_precisions);
        let kinds = row_kind_column(batch);
        let mut output = Vec::new();
        let mut inputs = Vec::new();
        let mut output_kinds = Vec::new();
        for row in 0..batch.num_rows() {
            let kind = kinds.map_or(0, |k| k.value(row));
            let payload: Arc<[u8]> = Arc::from(payloads.row(row).data());
            let candidate = codec.candidate(
                payload.clone(),
                Some(codec.identity(batch, row, identities.as_ref())),
                binary.as_mut().map(|b| Arc::from(b.encode(row))),
            );
            let addition = kind == 0 || kind == 2;
            let (fresh, action) = store.apply(
                keys.encode(row),
                candidate,
                addition,
                !self.upsert_columns.is_empty(),
                self.strategy,
                codec,
                StateTtl::new(self.ttl_ms, now),
            )?;
            if addition {
                output.push(payload);
                inputs.push(Some(row as u32));
                output_kinds.push(if fresh { 0 } else { 2 });
            } else {
                match action {
                    Action::Silent => {}
                    Action::Delete => {
                        output.push(payload);
                        inputs.push(Some(row as u32));
                        output_kinds.push(3);
                    }
                    Action::Restore(previous) => {
                        output.push(previous);
                        inputs.push(None);
                        output_kinds.push(2);
                    }
                }
            }
        }
        Self::output(codec, batch, output, inputs, output_kinds)
    }

    fn fresh(&self, key: &[u8], ttl: StateTtl) -> Result<bool, DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.rocks {
            return store.live(key, ttl).map(|live| !live);
        }
        Ok(self
            .groups
            .get(key)
            .is_none_or(|v| ttl.expired(v.written_at)))
    }

    // With identical input and sink keys every key has one candidate. Positive changes still
    // emit per arrival, but only the final replacement in this batch needs an owned state row.
    fn push_single_candidate_updates(
        &mut self,
        batch: &RecordBatch,
        now: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        let codec = self.codec.as_ref().unwrap();
        let ttl = StateTtl::new(self.ttl_ms, now);
        let mut keys = BinaryRowBatchEncoder::new(batch, &self.key_columns, &self.key_precisions);
        let identities = codec.identities(batch).expect("scalar sink keys");
        let mut last = std::collections::HashMap::<Vec<u8>, (usize, Vec<u8>)>::new();
        let mut kinds = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let identity = identities.row(row);
            if let Some((index, _)) = last.get_mut(identity.data()) {
                *index = row;
                kinds.push(2);
            } else {
                let key = keys.encode(row);
                let fresh = self.fresh(key, ttl)?;
                kinds.push(if fresh { 0 } else { 2 });
                last.insert(identity.data().to_vec(), (row, key.to_vec()));
            }
        }
        let retained: Vec<_> = last.into_values().map(|(row, key)| (key, row)).collect();
        let indices = UInt32Array::from_iter_values(retained.iter().map(|(_, row)| *row as u32));
        let columns = batch.columns()[..data_arity(batch)]
            .iter()
            .map(|column| take(column, &indices, None).expect("materializer final replacements"))
            .collect();
        let selected =
            RecordBatch::try_new(codec.schema.clone(), columns).expect("replacement rows");
        let payloads = codec
            .payload
            .convert_columns(selected.columns())
            .expect("encode replacements");
        #[cfg(feature = "rocksdb-state")]
        let mut reservation = self
            .rocks
            .as_ref()
            .and_then(|_| self.memory.temporary_reservation());
        #[cfg(feature = "rocksdb-state")]
        if let Some(memory) = &mut reservation {
            memory.try_resize(payloads.size())?;
        }
        let identities = codec.identities(&selected);
        let mut serialized = codec.serialized.then(|| {
            BinaryRowBatchEncoder::new(&selected, &codec.match_columns, &codec.match_precisions)
        });
        let track = self.memory.tracking();
        for (row, (key, _)) in retained.iter().enumerate() {
            #[cfg(feature = "rocksdb-state")]
            if let Some(store) = &self.rocks {
                store.install_schema(&codec.schema, self.ttl_ms)?;
                let identity = codec.identity(&selected, row, identities.as_ref());
                let binary = serialized.as_mut().map(|e| Arc::from(e.encode(row)));
                store.apply(
                    key,
                    codec.candidate(Arc::from(payloads.row(row).data()), Some(identity), binary),
                    true,
                    true,
                    self.strategy,
                    codec,
                    ttl,
                )?;
                continue;
            }
            let before = self
                .groups
                .get(key)
                .map_or(0, |v| byte_key_bytes(key) + v.bytes());
            if self
                .groups
                .get(key)
                .is_some_and(|v| ttl.expired(v.written_at))
            {
                self.groups.remove(key);
            }
            let values = if self.groups.contains(key) {
                self.groups.get_mut(key).unwrap()
            } else {
                self.groups.insert(
                    ByteKey::from(key.as_slice()),
                    Candidates::new(self.strategy),
                )
            };
            let identity = codec.identity(&selected, row, identities.as_ref());
            let binary = serialized.as_mut().map(|e| Arc::from(e.encode(row)));
            values.add(
                codec.candidate(Arc::from(payloads.row(row).data()), Some(identity), binary),
                true,
                self.strategy,
                codec,
            );
            values.written_at = now;
            if track {
                let after = byte_key_bytes(key) + values.bytes();
                self.memory.record(after as isize - before as isize);
            }
        }
        self.memory.account()?;
        let mut fields = codec.schema.fields().to_vec();
        fields.push(Arc::new(Field::new(ROW_KIND_COLUMN, DataType::Int8, false)));
        let mut columns = batch.columns()[..data_arity(batch)].to_vec();
        columns.push(Arc::new(Int8Array::from(kinds)));
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("materialized upserts"))
    }

    pub(crate) fn snapshot(&self, max_parallelism: usize) -> BTreeMap<i32, Vec<u8>> {
        #[cfg(feature = "rocksdb-state")]
        if let Some(store) = &self.rocks {
            return self
                .snapshot_rocks(store, max_parallelism)
                .expect("materializer canonical export");
        }
        let Some(codec) = &self.codec else {
            return BTreeMap::new();
        };
        let mut groups: BTreeMap<i32, Vec<(&ByteKey, &Candidates)>> = BTreeMap::new();
        for (key, rows) in self.groups.iter() {
            let group = flink_key_group(hash_bytes_by_words(&key.0), max_parallelism) as i32;
            groups.entry(group).or_default().push((key, rows));
        }
        groups
            .into_iter()
            .map(|(group, values)| {
                let mut keys = BinaryBuilder::new();
                let mut rows = BinaryBuilder::new();
                let mut times = Int64Builder::new();
                let mut shapes = StringBuilder::new();
                for (key, candidates) in values {
                    let (shape, payloads) = candidates.snapshot(codec);
                    keys.append_value(&key.0);
                    rows.append_value([]);
                    times.append_value(candidates.written_at);
                    shapes.append_value(shape);
                    for payload in payloads {
                        keys.append_value(&key.0);
                        rows.append_value(payload);
                        times.append_value(candidates.written_at);
                        shapes.append_null();
                    }
                }
                let schema = Schema::new_with_metadata(
                    vec![
                        Field::new(RAW_SNAPSHOT_KEY, DataType::Binary, false),
                        Field::new(RAW_SNAPSHOT_ROW, DataType::Binary, false),
                        Field::new(TTL_TS_COLUMN, DataType::Int64, false),
                        Field::new("__sink_shape__", DataType::Utf8, true),
                    ],
                    std::collections::HashMap::from([
                        (
                            RAW_SNAPSHOT_PAYLOAD_SCHEMA.to_string(),
                            encode_schema_metadata(&codec.schema),
                        ),
                        (
                            "streamfusion.sink-materializer.version".to_string(),
                            "2".to_string(),
                        ),
                        (
                            "streamfusion.sink-materializer.ttl-ms".to_string(),
                            self.ttl_ms.to_string(),
                        ),
                    ]),
                );
                let batch = RecordBatch::try_new(
                    Arc::new(schema),
                    vec![
                        Arc::new(keys.finish()),
                        Arc::new(rows.finish()),
                        Arc::new(times.finish()),
                        Arc::new(shapes.finish()),
                    ],
                )
                .expect("materializer snapshot");
                (group, write_ipc(&batch))
            })
            .collect()
    }

    #[cfg(feature = "rocksdb-state")]
    fn snapshot_rocks(
        &self,
        store: &state::rocks::RocksCandidates,
        groups: usize,
    ) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError> {
        let Some(codec) = &self.codec else {
            return Ok(BTreeMap::new());
        };
        let mut writers = BTreeMap::<i32, arrow::ipc::writer::StreamWriter<Vec<u8>>>::new();
        for key in store.keys()? {
            let candidates = store.read(&key.0, codec)?;
            let mut one = SinkMaterializer::new(
                self.key_columns.clone(),
                self.key_precisions.clone(),
                self.upsert_columns.clone(),
                self.ttl_ms,
            )
            .with_strategy(self.strategy, self.match_precisions.clone());
            one.codec = Some(RowsCodec::new(
                codec.schema.clone(),
                &self.upsert_columns,
                self.strategy,
                &self.match_precisions,
            ));
            one.groups.insert(key, candidates);
            for (group, bytes) in one.snapshot(groups) {
                for batch in read_ipc(&bytes) {
                    let writer = writers.entry(group).or_insert_with(|| {
                        arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &batch.schema())
                            .expect("snapshot stream")
                    });
                    writer.write(&batch)?;
                }
            }
        }
        writers
            .into_iter()
            .map(|(group, writer)| Ok((group, writer.into_inner()?)))
            .collect()
    }

    pub(crate) fn restore(&mut self, snapshots: &[Vec<u8>], now: i64) {
        for bytes in snapshots {
            for batch in read_ipc_if_present(bytes) {
                assert_eq!(
                    batch
                        .schema_ref()
                        .metadata()
                        .get("streamfusion.sink-materializer.version")
                        .map(String::as_str),
                    Some("2")
                );
                let had_ttl = batch
                    .schema_ref()
                    .metadata()
                    .get("streamfusion.sink-materializer.ttl-ms")
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(0)
                    > 0;
                let schema = decode_schema_metadata(&batch).expect("materializer snapshot schema");
                let codec = self.codec.get_or_insert_with(|| {
                    RowsCodec::new(
                        schema.clone(),
                        &self.upsert_columns,
                        self.strategy,
                        &self.match_precisions,
                    )
                });
                assert_eq!(codec.schema, schema, "materializer restore schema mismatch");
                let keys = column_binary(&batch, RAW_SNAPSHOT_KEY);
                let rows = column_binary(&batch, RAW_SNAPSHOT_ROW);
                let times = column_i64(&batch, TTL_TS_COLUMN);
                let shapes = batch
                    .column_by_name("__sink_shape__")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let mut row = 0;
                while row < batch.num_rows() {
                    let shape = shapes.value(row);
                    let count = Candidates::snapshot_rows(shape);
                    let written_at = if self.ttl_ms > 0 && !had_ttl {
                        now
                    } else {
                        times.value(row)
                    };
                    let mut values = Candidates::restore(
                        shape,
                        (row + 1..row + 1 + count).map(|i| rows.value(i).to_vec()),
                        written_at,
                        codec,
                    );
                    values.reconfigure(self.strategy, codec);
                    self.groups.insert(ByteKey::from(keys.value(row)), values);
                    row += count + 1;
                }
            }
            #[cfg(feature = "rocksdb-state")]
            if let Some(store) = &self.rocks {
                if let Some(codec) = &self.codec {
                    store
                        .install_schema(&codec.schema, self.ttl_ms)
                        .expect("materializer schema import");
                    for (key, rows) in self.groups.iter() {
                        store
                            .import(&key.0, rows, codec)
                            .expect("materializer state import");
                    }
                }
                self.groups = MemoryStateStore::default();
            }
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createSinkMaterializer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    keys: JIntArray<'local>,
    precisions: JIntArray<'local>,
    upsert: JIntArray<'local>,
    ttl: jlong,
    strategy: jint,
    high: jlong,
    low: jlong,
    match_precisions: JIntArray<'local>,
    budget: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        equal::init_java_cache(&mut env);
        let materializer = SinkMaterializer::new(
            read_columns(&env, &keys),
            read_i32_array(&env, &precisions),
            read_columns(&env, &upsert),
            ttl,
        )
        .with_strategy(
            Strategy::from_config(strategy, high, low),
            read_i32_array(&env, &match_precisions),
        )
        .with_budget(budget);
        boxed_or_throw(&mut env, materializer)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreSinkMaterializer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    keys: JIntArray<'local>,
    precisions: JIntArray<'local>,
    upsert: JIntArray<'local>,
    ttl: jlong,
    snapshots: JObjectArray<'local>,
    now: jlong,
    strategy: jint,
    high: jlong,
    low: jlong,
    match_precisions: JIntArray<'local>,
    budget: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        equal::init_java_cache(&mut env);
        let mut materializer = SinkMaterializer::new(
            read_columns(&env, &keys),
            read_i32_array(&env, &precisions),
            read_columns(&env, &upsert),
            ttl,
        )
        .with_strategy(
            Strategy::from_config(strategy, high, low),
            read_i32_array(&env, &match_precisions),
        );
        let mut restored = Vec::new();
        for i in 0..env
            .get_array_length(&snapshots)
            .expect("materializer partition count")
        {
            let bytes = JByteArray::from(
                env.get_object_array_element(&snapshots, i)
                    .expect("materializer partition"),
            );
            restored.push(
                env.convert_byte_array(&bytes)
                    .expect("materializer partition bytes"),
            );
        }
        materializer.restore(&restored, now);
        boxed_or_throw(&mut env, materializer.with_budget(budget))
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushSinkMaterializer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    input_array: jlong,
    input_schema: jlong,
    now: jlong,
    output_array: jlong,
    output_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let materializer = unsafe { &mut *(handle as *mut SinkMaterializer) };
        let result = {
            let batch = import_record_batch(input_array, input_schema);
            materializer.push(&batch, now)
        };
        match result {
            Ok(batch) => export_record_batch(batch, output_array, output_schema),
            Err(error) => throw_memory_limit(&mut env, &error.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotSinkMaterializer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let materializer = unsafe { &*(handle as *const SinkMaterializer) };
        keyed_state_partition_array(
            &mut env,
            materializer.snapshot(max_parallelism as usize),
            "sink-materializer",
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_sinkMaterializerStateBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const SinkMaterializer) }
            .memory
            .state_bytes as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeSinkMaterializer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<SinkMaterializer>(handle));
    })
}

#[cfg(feature = "rocksdb-state")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBSinkMaterializer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    keys: JIntArray<'local>,
    precisions: JIntArray<'local>,
    upsert: JIntArray<'local>,
    ttl: jlong,
    strategy: jint,
    high: jlong,
    low: jlong,
    match_precisions: JIntArray<'local>,
    budget: jlong,
    now: jlong,
    directory: jni::objects::JString<'local>,
    max_parallelism: jint,
    options: jni::objects::JString<'local>,
    shared: jlong,
    source_dirs: JObjectArray<'local>,
    source_tokens: JObjectArray<'local>,
    start: jint,
    end: jint,
    aligned: jni::sys::jboolean,
    snapshots: JObjectArray<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        use crate::state::rocks_jni::{read_restored_partitions, read_string, read_strings};
        equal::init_java_cache(&mut env);
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options),
            shared_resources: shared,
            ttl_ms: ttl.max(0),
        };
        let sources = read_strings(&mut env, &source_dirs)
            .into_iter()
            .flatten()
            .zip(
                read_strings(&mut env, &source_tokens)
                    .into_iter()
                    .flatten()
                    .map(|v| v.parse::<i64>().expect("checkpoint token")),
            )
            .collect::<Vec<_>>();
        let result =
            state::rocks::RocksCandidates::open(config, &sources, start..=end, aligned != 0)
                .and_then(|store| {
                    let mut materializer = SinkMaterializer::new(
                        read_columns(&env, &keys),
                        read_i32_array(&env, &precisions),
                        read_columns(&env, &upsert),
                        ttl,
                    )
                    .with_strategy(
                        Strategy::from_config(strategy, high, low),
                        read_i32_array(&env, &match_precisions),
                    )
                    .with_rocks(store, now)?;
                    materializer.restore(&read_restored_partitions(&mut env, &snapshots), now);
                    materializer.with_budget(budget)
                });
        boxed_or_throw(&mut env, result)
    })
}

#[cfg(feature = "rocksdb-state")]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBSinkMaterializer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    directory: jni::objects::JString<'local>,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        use crate::state::rocks_jni::{manifest_array, read_string};
        let materializer = unsafe { &mut *(handle as *mut SinkMaterializer) };
        match materializer
            .rocks
            .as_mut()
            .expect("direct materializer store")
            .table
            .checkpoint(&read_string(&mut env, &directory))
        {
            Ok(manifest) => manifest_array(&mut env, &manifest),
            Err(error) => {
                let _ = env.throw_new("java/lang/RuntimeException", error.to_string());
                std::ptr::null_mut()
            }
        }
    })
}
