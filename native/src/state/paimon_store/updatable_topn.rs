use super::*;


/// One entry's hydrated image — the flush base the barrier's per-entry diff compares against.
/// Equality deliberately includes the clock (see the map store): a timestamp-only refresh must
/// re-persist its row.
struct PersistedEntry {
    payload: Arc<OwnedRow>,
    inner_rank: i64,
    ts_ms: i64,
}

impl PersistedEntry {
    fn matches(&self, entry: &UpdatableRow, inner_rank: i64) -> bool {
        self.inner_rank == inner_rank
            && self.ts_ms == entry.ts_ms
            && self.payload.row() == entry.payload.row()
    }
}

enum UpdatableSlot {
    Present {
        entries: Vec<UpdatableRow>,
        dirty: bool,
        persisted: ahash::HashMap<ByteKey, PersistedEntry>,
    },
    Absent {
        dirty: bool,
        persisted: ahash::HashMap<ByteKey, PersistedEntry>,
    },
}

impl UpdatableSlot {
    fn take_persisted(&mut self) -> ahash::HashMap<ByteKey, PersistedEntry> {
        match self {
            UpdatableSlot::Present { persisted, .. } | UpdatableSlot::Absent { persisted, .. } => {
                std::mem::take(persisted)
            }
        }
    }
}

/// The update-fast Top-N's persistent shape — the row-keyed MAP shape over a sorted working
/// buffer, mirroring Flink's `UpdatableTopNFunction` state (`MapState<rowKey, (row, innerRank)>`):
/// one table row per buffered entry under PK `[kg, k, r]`, where `r` is the entry's unique-row-key
/// bytes — already a stable Flink BinaryRow encoding, computed by the ranker from the row's key
/// columns. The persisted columns are the payload row's typed columns plus the entry's inner rank
/// (its position among byte-equal sort-key ties — Flink's innerRank; the memcomparable sort key
/// itself re-derives from the payload on hydration), so the sorted buffer rebuilds exactly: order
/// by the re-derived sort key, ties by inner rank. The flush is per entry against the hydrated
/// image — an in-place payload replace, the shape's dominant write, rewrites one row; a row key no
/// longer buffered gets a tombstone — where the list shape would rewrite a touched partition's
/// whole buffer. Working set and checkpoint discipline as in the map store, over the shared core.
pub(crate) struct PaimonUpdatableTopNStore {
    core: PaimonTableCore,
    codec: TopNStateCodec,
    /// The entry columns as Arrow fields, in persisted order after `kg`/`k`/`r`: the payload
    /// columns, the inner rank, and (with TTL on) the store-managed trailing `ts` column.
    value_fields: Vec<Field>,
    /// The payload columns' count — the inner rank sits right after them.
    payload_columns: usize,
    /// The host's wall clock, set before every ingest call; only read when TTL is on.
    now_ms: i64,
    working: ahash::HashMap<ByteKey, UpdatableSlot>,
    footprint: isize,
}

impl KeyedStateStore<Vec<UpdatableRow>> for PaimonUpdatableTopNStore {
    #[inline]
    fn contains(&self, key: &[u8]) -> bool {
        matches!(self.working.get(key), Some(UpdatableSlot::Present { .. }))
    }

    #[inline]
    fn get(&self, key: &[u8]) -> Option<&Vec<UpdatableRow>> {
        match self.working.get(key) {
            Some(UpdatableSlot::Present { entries, .. }) => Some(entries),
            _ => None,
        }
    }

    #[inline]
    fn get_mut(&mut self, key: &[u8]) -> Option<&mut Vec<UpdatableRow>> {
        match self.working.get_mut(key) {
            Some(UpdatableSlot::Present { entries, dirty, .. }) => {
                *dirty = true;
                Some(entries)
            }
            _ => None,
        }
    }

    #[inline]
    fn insert(&mut self, key: ByteKey, value: Vec<UpdatableRow>) -> &mut Vec<UpdatableRow> {
        // An overwritten slot keeps its persisted image: row keys already in the table still
        // need tombstones at the next checkpoint if the new buffer lacks them.
        let persisted = match self.working.get_mut(&*key.0) {
            Some(slot) => slot.take_persisted(),
            None => ahash::HashMap::default(),
        };
        let slot = self
            .working
            .entry(key)
            .insert_entry(UpdatableSlot::Present { entries: value, dirty: true, persisted })
            .into_mut();
        match slot {
            UpdatableSlot::Present { entries, .. } => entries,
            UpdatableSlot::Absent { .. } => unreachable!("just inserted a present slot"),
        }
    }

    #[inline]
    fn remove(&mut self, key: &[u8]) {
        if let Some(slot) = self.working.get_mut(key) {
            let persisted = slot.take_persisted();
            *slot = UpdatableSlot::Absent { dirty: true, persisted };
        }
    }

    fn begin_batch(
        &mut self,
        batch: &RecordBatch,
        key_columns: &[usize],
        key_timestamp_precisions: &[i32],
    ) -> Result<(), DataFusionError> {
        let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, key_timestamp_precisions);
        let mut misses: Vec<ByteKey> = Vec::new();
        let mut seen: StdHashSet<ByteKey> = StdHashSet::new();
        for row in 0..batch.num_rows() {
            let key = encoder.encode(row);
            if !self.working.contains_key(key) && !seen.contains(key) {
                let owned = ByteKey::from(key);
                seen.insert(owned.clone());
                misses.push(owned);
            }
        }
        if !misses.is_empty() {
            self.fetch_missing(misses)?;
        }
        Ok(())
    }

    fn end_bundle(&mut self) -> Result<(), DataFusionError> {
        // See the single-value store: only the write buffer survives the bundle. A dirty slot
        // keeps its persisted image too — it is the flush base the barrier diffs against.
        let footprint = &mut self.footprint;
        self.working.retain(|key, slot| match slot {
            UpdatableSlot::Present { entries, persisted, dirty: false } => {
                *footprint -= (byte_key_bytes(&key.0)
                    + entries.iter().map(updatable_entry_bytes).sum::<usize>()
                    + persisted.len() * Self::IMAGE_ENTRY_BYTES
                    + Self::SLOT_OVERHEAD) as isize;
                false
            }
            UpdatableSlot::Absent { persisted, dirty: false } => {
                *footprint -=
                    (persisted.len() * Self::IMAGE_ENTRY_BYTES + Self::SLOT_OVERHEAD) as isize;
                false
            }
            _ => true,
        });
        Ok(())
    }

    fn footprint_delta(&mut self) -> isize {
        std::mem::take(&mut self.footprint)
    }
}

impl PaimonUpdatableTopNStore {
    pub(crate) fn metric_entry_count(&self) -> usize {
        self.working
            .values()
            .map(|slot| match slot {
                UpdatableSlot::Present { entries, .. } => entries.len(),
                UpdatableSlot::Absent { .. } => 0,
            })
            .sum()
    }

    const SLOT_OVERHEAD: usize =
        std::mem::size_of::<UpdatableSlot>() + GROUP_ENTRY_OVERHEAD;
    /// One persisted-image entry: an owned row-key copy plus the image (the payload row is
    /// Arc-shared with the live buffer, so its bytes are accounted once, by the live entry).
    const IMAGE_ENTRY_BYTES: usize =
        std::mem::size_of::<(ByteKey, PersistedEntry)>() + GROUP_ENTRY_OVERHEAD;

    /// Creates a fresh table under `config.table_dir` (schema document + directory skeleton).
    pub(crate) fn create(
        config: PaimonStoreConfig,
        codec: TopNStateCodec,
    ) -> Result<Self, DataFusionError> {
        let schema = Self::paimon_schema(&config, &codec)?;
        Self::assemble(PaimonTableCore::create(config, schema)?, codec)
    }

    /// Opens a table directory previously materialized from a checkpoint, pinned at its snapshot.
    pub(crate) fn open(
        config: PaimonStoreConfig,
        codec: TopNStateCodec,
        snapshot_id: i64,
    ) -> Result<Self, DataFusionError> {
        Self::assemble(PaimonTableCore::open(config, snapshot_id)?, codec)
    }

    /// Builds a fresh table at `config.table_dir` from one or more restored table directories
    /// (rescale); see `PaimonTableCore::adopt_buckets`. `now_ms` is the host's wall clock at
    /// restore, the stamp of the enable-TTL migration (see `clip_from_sources`). Like the map
    /// shape — and unlike the list — the clip may shed rows already expired at restore: the
    /// update-fast ranker expires per row-key entry, so every persisted clock is individually
    /// truthful.
    pub(crate) fn open_merged(
        config: PaimonStoreConfig,
        codec: TopNStateCodec,
        sources: &[(String, i64)],
        key_groups: std::ops::RangeInclusive<i32>,
        aligned: bool,
        now_ms: i64,
    ) -> Result<Self, DataFusionError> {
        let mut store = Self::create(config, codec)?;
        store.now_ms = now_ms;
        if aligned && sources.len() == 1 {
            let (source_dir, snapshot_id) = &sources[0];
            if store.core.adopt_all(source_dir, *snapshot_id)? {
                return Ok(store);
            }
        }
        let write_fields = store.arrow_fields();
        store.core.clip_from_sources(sources, key_groups, &write_fields, store.ttl())?;
        Ok(store)
    }

    fn assemble(core: PaimonTableCore, codec: TopNStateCodec) -> Result<Self, DataFusionError> {
        use crate::state::PaimonListCodec;
        if !codec.supported() {
            return Err(DataFusionError::Plan(
                "state shape not supported by the paimon state backend".into(),
            ));
        }
        let mut value_fields: Vec<Field> = codec
            .value_fields()
            .into_iter()
            .map(|(name, data_type)| Field::new(name, data_type, true))
            .collect();
        let payload_columns = value_fields.len();
        value_fields.push(Field::new(INNER_RANK_COLUMN, DataType::Int64, true));
        if core.config.ttl_ms > 0 {
            value_fields.push(Field::new(TS_COLUMN, DataType::Int64, true));
        }
        Ok(PaimonUpdatableTopNStore {
            core,
            codec,
            value_fields,
            payload_columns,
            now_ms: 0,
            working: ahash::HashMap::default(),
            footprint: 0,
        })
    }

    fn paimon_schema(
        config: &PaimonStoreConfig,
        codec: &TopNStateCodec,
    ) -> Result<PaimonSchema, DataFusionError> {
        use crate::state::PaimonListCodec;
        let mut builder = PaimonTableCore::schema_builder(config)?.column(
            SUB_KEY_COLUMN,
            PaimonType::VarBinary(
                VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).map_err(pe)?,
            ),
        );
        for (name, data_type) in codec.value_fields() {
            let paimon_type = paimon_type_of(&data_type).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "state type {data_type} not supported by the paimon state backend"
                ))
            })?;
            builder = builder.column(name, paimon_type);
        }
        builder = builder.column(INNER_RANK_COLUMN, PaimonType::BigInt(BigIntType::new()));
        if config.ttl_ms > 0 {
            builder = builder.column(TS_COLUMN, PaimonType::BigInt(BigIntType::new()));
        }
        builder
            .primary_key([KG_COLUMN, KEY_COLUMN, SUB_KEY_COLUMN])
            .build()
            .map_err(pe)
    }

    /// Sets the host's wall clock for this ingest call (Flink's `TtlTimeProvider` reading);
    /// hydration reads it to expire committed entries, the clip to stamp migrated ones.
    pub(crate) fn set_clock(&mut self, now_ms: i64) {
        self.now_ms = now_ms;
    }

    fn ttl(&self) -> crate::state::StateTtl {
        crate::state::StateTtl::new(self.core.config.ttl_ms, self.now_ms)
    }

    /// The Arrow schema of persisted rows (also the write-batch schema, which additionally
    /// carries `_VALUE_KIND`).
    fn arrow_fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new(KG_COLUMN, DataType::Int32, false),
            Field::new(KEY_COLUMN, DataType::Binary, false),
            Field::new(SUB_KEY_COLUMN, DataType::Binary, false),
        ];
        fields.extend(self.value_fields.iter().cloned());
        fields
    }

    /// Reads the missed keys from the committed table. Entries are collected across ALL probe
    /// batches before assembly — the merge reader may split one key's rows across batch
    /// boundaries — then the sorted buffer reassembles by (re-derived sort key, inner rank).
    /// With TTL on this is where the persistent backend expires, per entry (the update-fast
    /// ranker's granularity IS the row-key entry): a committed entry past its retention drops
    /// out of the live buffer but stays in the flush base, so the next barrier's per-entry diff
    /// commits its tombstone (delete-on-read); live entries carry their persisted clock.
    fn fetch_missing(&mut self, misses: Vec<ByteKey>) -> Result<(), DataFusionError> {
        use crate::state::PaimonListCodec;
        let batches = self.core.scan_keys(&misses)?;
        let mut collected: ahash::HashMap<ByteKey, Vec<(ByteKey, Vec<ScalarValue>)>> =
            ahash::HashMap::default();
        for batch in &batches {
            let expected = self.arrow_fields();
            let keys = normalized_column(batch, 1, &expected[1])?;
            let keys = keys
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("paimon key column".into()))?;
            let row_keys = normalized_column(batch, 2, &expected[2])?;
            let row_keys = row_keys
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| DataFusionError::Internal("paimon row-key column".into()))?;
            let mut value_columns: Vec<ArrayRef> = Vec::with_capacity(self.value_fields.len());
            for i in 0..self.value_fields.len() {
                value_columns.push(normalized_column(batch, 3 + i, &expected[3 + i])?);
            }
            for row in 0..batch.num_rows() {
                let key = keys.value(row);
                // A key already in the working set stays authoritative over the table.
                if self.working.contains_key(key) {
                    continue;
                }
                let mut scalars: Vec<ScalarValue> = Vec::with_capacity(value_columns.len());
                for column in &value_columns {
                    scalars.push(
                        ScalarValue::try_from_array(column, row)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
                    );
                }
                collected
                    .entry(ByteKey::from(key))
                    .or_default()
                    .push((ByteKey::from(row_keys.value(row)), scalars));
            }
        }
        let ttl = self.ttl();
        let mut added_bytes = 0usize;
        for (key, rows) in collected {
            let mut hydrated: Vec<(i64, UpdatableRow)> = Vec::with_capacity(rows.len());
            let mut persisted: ahash::HashMap<ByteKey, PersistedEntry> =
                ahash::HashMap::default();
            let mut expired_any = false;
            for (row_key, mut scalars) in rows {
                // A NULL ts is defensive (no live entry is written without one): it decodes as a
                // fresh write rather than expiring the entry.
                let ts_ms = ttl.enabled().then(|| match scalars.pop() {
                    Some(ScalarValue::Int64(Some(ts))) => ts,
                    _ => self.now_ms,
                });
                let inner_rank = match scalars.pop() {
                    Some(ScalarValue::Int64(Some(rank))) => rank,
                    _ => 0,
                };
                let decoded = self.codec.decode(&scalars);
                let entry = UpdatableRow {
                    sort: decoded.sort,
                    payload: decoded.payload,
                    row_key: row_key.clone(),
                    ts_ms: ts_ms.unwrap_or(0),
                };
                // The image shares the live buffer's payload Arc; only the copies are new.
                persisted.insert(
                    row_key,
                    PersistedEntry {
                        payload: Arc::clone(&entry.payload),
                        inner_rank,
                        ts_ms: entry.ts_ms,
                    },
                );
                if ts_ms.is_some_and(|ts| ttl.expired(ts)) {
                    expired_any = true;
                    continue;
                }
                added_bytes += updatable_entry_bytes(&entry);
                hydrated.push((inner_rank, entry));
            }
            hydrated.sort_by(|(rank_a, a), (rank_b, b)| {
                a.sort.row().cmp(&b.sort.row()).then(rank_a.cmp(rank_b))
            });
            let entries: Vec<UpdatableRow> = hydrated.into_iter().map(|(_, e)| e).collect();
            added_bytes += persisted.len() * Self::IMAGE_ENTRY_BYTES;
            added_bytes += byte_key_bytes(&key.0) + Self::SLOT_OVERHEAD;
            self.working.insert(
                key,
                UpdatableSlot::Present { entries, dirty: expired_any, persisted },
            );
        }
        for key in misses {
            self.working.entry(key).or_insert_with(|| {
                added_bytes += Self::SLOT_OVERHEAD;
                UpdatableSlot::Absent { dirty: false, persisted: ahash::HashMap::default() }
            });
        }
        self.footprint += added_bytes as isize;
        Ok(())
    }

    /// Builds the write batch for all dirty slots: one upsert per live entry whose (payload,
    /// inner rank, clock) differs from the hydrated image — an in-place replace touches exactly
    /// one row — and one tombstone per hydrated row key no longer buffered. At most one row per
    /// `(k, r)` per checkpoint by construction (upserts are live, tombstones are not).
    pub(crate) fn dirty_batch(&self) -> Option<RecordBatch> {
        let num_value = self.value_fields.len();
        let mut kgs: Vec<i32> = Vec::new();
        let mut keys: Vec<&[u8]> = Vec::new();
        let mut subs: Vec<&[u8]> = Vec::new();
        let mut values: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_value];
        let mut kinds: Vec<i8> = Vec::new();
        let ttl_on = self.core.config.ttl_ms > 0;
        for (key, slot) in self.working.iter() {
            let (entries, persisted, dirty) = match slot {
                UpdatableSlot::Present { entries, persisted, dirty } => {
                    (Some(entries), persisted, *dirty)
                }
                UpdatableSlot::Absent { persisted, dirty } => (None, persisted, *dirty),
            };
            if !dirty {
                continue;
            }
            let kg = self.core.key_group(&key.0);
            if let Some(entries) = entries {
                let mut tie_start = 0usize;
                for (index, entry) in entries.iter().enumerate() {
                    // The buffer is sorted, so byte-equal sort keys sit contiguously; the inner
                    // rank is the entry's position within its tie run.
                    if index > 0 && entries[index - 1].sort != entry.sort {
                        tie_start = index;
                    }
                    let inner_rank = (index - tie_start) as i64;
                    if persisted
                        .get(&*entry.row_key.0)
                        .is_some_and(|image| image.matches(entry, inner_rank))
                    {
                        continue; // unchanged since hydration — the table already holds it
                    }
                    kgs.push(kg);
                    keys.push(&key.0);
                    subs.push(&entry.row_key.0);
                    for (column, scalar) in
                        values.iter_mut().zip(self.codec.encode_payload(&entry.payload))
                    {
                        column.push(scalar);
                    }
                    values[self.payload_columns].push(ScalarValue::Int64(Some(inner_rank)));
                    if ttl_on {
                        values[num_value - 1].push(ScalarValue::Int64(Some(entry.ts_ms)));
                    }
                    kinds.push(0); // +I upsert — deduplicate keeps the latest by sequence
                }
            }
            for row_key in persisted.keys() {
                if entries
                    .is_some_and(|entries| entries.iter().any(|e| e.row_key.0 == row_key.0))
                {
                    continue;
                }
                kgs.push(kg);
                keys.push(&key.0);
                subs.push(&row_key.0);
                for (column, field) in values.iter_mut().zip(self.value_fields.iter()) {
                    column.push(null_scalar(field.data_type()));
                }
                kinds.push(3); // -D tombstone for a vanished row key
            }
        }
        if keys.is_empty() {
            return None;
        }
        let mut fields = self.arrow_fields();
        fields.push(Field::new(VALUE_KIND_COLUMN, DataType::Int8, false));
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(kgs)),
            Arc::new(BinaryArray::from_iter_values(keys)),
            Arc::new(BinaryArray::from_iter_values(subs)),
        ];
        for (i, field) in self.value_fields.iter().enumerate() {
            columns.push(scalars_to_array(std::mem::take(&mut values[i]), field.data_type()));
        }
        columns.push(Arc::new(Int8Array::from(kinds)));
        Some(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("paimon update-fast top-n dirty write batch"),
        )
    }

    /// Checkpoint sync phase, called at the barrier; see the single-value store's `checkpoint`.
    pub(crate) fn checkpoint(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        self.core.refresh_to_latest()?;
        if let Some(batch) = self.dirty_batch() {
            self.core.commit(&batch)?;
        }
        let footprint = &mut self.footprint;
        self.working.retain(|key, slot| {
            match slot {
                UpdatableSlot::Present { entries, persisted, .. } => {
                    *footprint -= (byte_key_bytes(&key.0)
                        + entries.iter().map(updatable_entry_bytes).sum::<usize>()
                        + persisted.len() * Self::IMAGE_ENTRY_BYTES
                        + Self::SLOT_OVERHEAD) as isize;
                }
                UpdatableSlot::Absent { persisted, .. } => {
                    *footprint -=
                        (persisted.len() * Self::IMAGE_ENTRY_BYTES + Self::SLOT_OVERHEAD) as isize;
                }
            }
            false
        });
        self.core.checkpoint_manifest()
    }
}
