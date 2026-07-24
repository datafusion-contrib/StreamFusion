use crate::*;

pub(crate) struct UpdatingJoiner<S: KeyedStateStore<JoinBucket> = MemoryJoinStore> {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    key_timestamp_precisions: Vec<i32>,
    kind: JoinKind,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    predicate: Option<JoinPredicate>,
    /// arrow-row codecs for the value-encoded full row per side. State holds these bytes, not
    /// `Vec<ScalarValue>` — so build/probe hash a byte slice and a stored row is one byte buffer.
    /// The equi-key encodes as Flink BinaryRow bytes (the encoding whose hash IS the key group;
    /// both sides' key types are equal, so equal keys encode to equal bytes and match).
    left_payload: RowConverter,
    right_payload: RowConverter,
    /// A value-encoded all-null row per side, used to null-pad the absent side of an outer join.
    left_null: ByteKey,
    right_null: ByteKey,
    left_state: S,
    right_state: S,
    mini_batch: bool,
    left_staged: MiniBatchChanges<ByteKey, ByteKey>,
    right_staged: MiniBatchChanges<ByteKey, ByteKey>,
    pub(crate) memory: OperatorMemory,
}

/// One side's per-key multiset: row bytes → appear-count/degree. Keys are `ByteKey`, so the
/// steady-state probe — an input row whose key and content are already stored — hashes the
/// borrowed arrow-row bytes and allocates nothing; only a first insert copies them. This was the
/// differential profile's system-allocator signal vs Flink's pooled BinaryRowData.
pub(crate) type JoinBucket = ahash::HashMap<ByteKey, RowMeta>;

/// The resident default backend for a join side (see `state/` for the seam).
pub(crate) type MemoryJoinStore = MemoryStateStore<JoinBucket>;

/// The joined `[left fields.., right fields..]` schema (columns named `c0..`) the non-equi
/// predicate's input refs index into.
pub(crate) fn joined_schema(left_schema: &SchemaRef, right_schema: &SchemaRef) -> SchemaRef {
    let fields: Vec<Field> = left_schema
        .fields()
        .iter()
        .chain(right_schema.fields().iter())
        .enumerate()
        .map(|(j, f)| Field::new(format!("c{j}"), f.data_type().clone(), true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Estimated footprint of one stored join row (arrow-row bytes + its appear-count meta + map entry).
pub(crate) fn join_row_entry_bytes(row: &[u8]) -> usize {
    row.len() + std::mem::size_of::<RowMeta>() + GROUP_ENTRY_OVERHEAD
}

/// Estimated footprint of a stored key entry.
pub(crate) fn join_key_entry_bytes(key: &[u8]) -> usize {
    key.len() + GROUP_ENTRY_OVERHEAD
}

/// Estimated footprint of one side of the updating join's state (for the restore-time account).
pub(crate) fn join_state_bytes(state: &MemoryJoinStore) -> usize {
    state
        .iter()
        .map(|(key, bucket)| {
            join_key_entry_bytes(&key.0)
                + bucket.keys().map(|r| join_row_entry_bytes(&r.0)).sum::<usize>()
        })
        .sum()
}

impl UpdatingJoiner {
    pub(crate) fn new(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        kind: JoinKind,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        predicate: Option<JoinPredicate>,
    ) -> Self {
        let left_payload = payload_converter(&left_schema);
        let right_payload = payload_converter(&right_schema);
        let left_null = ByteKey::from(encode_null_row(&left_payload, &left_schema).row().as_ref());
        let right_null = ByteKey::from(encode_null_row(&right_payload, &right_schema).row().as_ref());
        let key_arity = left_keys.len();
        UpdatingJoiner {
            left_keys,
            right_keys,
            key_timestamp_precisions: vec![-1; key_arity],
            kind,
            left_schema,
            right_schema,
            predicate,
            left_payload,
            right_payload,
            left_null,
            right_null,
            left_state: MemoryJoinStore::default(),
            right_state: MemoryJoinStore::default(),
            mini_batch: false,
            left_staged: MiniBatchChanges::default(),
            right_staged: MiniBatchChanges::default(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds both sides' row state by the operator's managed-memory budget (negative =
    /// unaccounted), accounting any restored rows immediately.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state = join_state_bytes(&self.left_state) + join_state_bytes(&self.right_state);
        self.memory.attach("updating-join", budget_bytes, state)?;
        Ok(self)
    }
}

impl<S: KeyedStateStore<JoinBucket>> UpdatingJoiner<S> {
    /// Moves this freshly built (empty, memory-backed) joiner's configuration onto another state
    /// backend (one store per side); construction goes through `new` + builders first so backend
    /// choice stays orthogonal to the shape builders.
    pub(crate) fn with_backend<T: KeyedStateStore<JoinBucket>>(
        self,
        left_state: T,
        right_state: T,
    ) -> UpdatingJoiner<T> {
        UpdatingJoiner {
            left_keys: self.left_keys,
            right_keys: self.right_keys,
            key_timestamp_precisions: self.key_timestamp_precisions,
            kind: self.kind,
            left_schema: self.left_schema,
            right_schema: self.right_schema,
            predicate: self.predicate,
            left_payload: self.left_payload,
            right_payload: self.right_payload,
            left_null: self.left_null,
            right_null: self.right_null,
            left_state,
            right_state,
            mini_batch: self.mini_batch,
            left_staged: self.left_staged,
            right_staged: self.right_staged,
            memory: self.memory,
        }
    }

    /// Attaches the managed-memory budget for a backend that starts with nothing resident (a
    /// read-through store hydrates on demand; there is no restored map to pre-account).
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory.attach("updating-join", budget_bytes, 0)?;
        Ok(self)
    }

    /// The backing stores, for backend-specific control paths (checkpointing persistent stores).
    pub(crate) fn stores_mut(&mut self) -> (&mut S, &mut S) {
        (&mut self.left_state, &mut self.right_state)
    }

    pub(crate) fn with_key_timestamp_precisions(
        mut self,
        key_timestamp_precisions: Vec<i32>,
    ) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        self
    }

    pub(crate) fn with_mini_batch(mut self, mini_batch: bool) -> Self {
        self.mini_batch = mini_batch;
        self
    }

    /// Drops the candidate matches whose `[left.., right..]` pair fails the residual non-equi
    /// predicate, so only condition-satisfying rows feed the degree and the emitted output (Flink's
    /// `condition.apply` filter). A no-op when there is no predicate.
    fn filter_associated(&mut self, full: &[u8], is_left: bool, associated: &mut Vec<OuterRecord>) {
        if associated.is_empty() || self.predicate.is_none() {
            return;
        }
        let joined = joined_schema(&self.left_schema, &self.right_schema);
        let n = associated.len();
        // Assemble the candidate `[left.., right..]` batch directly from arrows, no ScalarValue
        // round-trip: decode the input row's columns once (length-1 arrays), broadcast them to `n`
        // rows via a zero-index gather, and decode all associated other-side rows in ONE vectorized
        // pass (a per-row convert_rows here was a measured regression on q7, whose price-equi predicate
        // builds large associated sets). Only joins with a residual non-equi condition reach this.
        let input_conv = if is_left { &self.left_payload } else { &self.right_payload };
        let input_parser = input_conv.parser();
        let input_cols =
            input_conv.convert_rows([input_parser.parse(full)]).expect("decode join input row");
        let zero = UInt32Array::from(vec![0u32; n]);
        let input_broadcast: Vec<ArrayRef> = input_cols
            .iter()
            .map(|a| take(a.as_ref(), &zero, None).expect("broadcast join input row"))
            .collect();
        let other_conv = if is_left { &self.right_payload } else { &self.left_payload };
        let other_parser = other_conv.parser();
        let other_cols = other_conv
            .convert_rows(associated.iter().map(|o| other_parser.parse(&o.record.0)))
            .expect("decode associated rows");
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(input_broadcast.len() + other_cols.len());
        if is_left {
            columns.extend(input_broadcast);
            columns.extend(other_cols);
        } else {
            columns.extend(other_cols);
            columns.extend(input_broadcast);
        }
        let batch = RecordBatch::try_new(joined.clone(), columns).expect("build join-predicate batch");
        let mask = self.predicate.as_mut().expect("predicate present").evaluate_batch(&joined, &batch);
        let mut keep = mask.into_iter();
        associated.retain(|_| keep.next().unwrap_or(false));
    }

    /// Gathers the matching rows on `other_state` for `key`, expanding each distinct row by its
    /// appear-times (so multiplicity is preserved) and capturing its degree once per distinct row. A
    /// null in the equi-key matches nothing (Flink's null-filtering equi semantics), so an empty key
    /// match means "no associated rows".
    fn associated(other_state: &S, key: &[u8]) -> Vec<OuterRecord> {
        // A null in the equi-key matches nothing (Flink's null-rejecting equality); the caller skips
        // association for null-key rows (a null can't be read back from the encoded key), so this just
        // gathers the matches for a non-null key.
        let mut out = Vec::new();
        if let Some(bucket) = other_state.get(key) {
            for (row, meta) in bucket.iter() {
                for _ in 0..meta.count.max(0) {
                    out.push(OuterRecord { record: row.clone(), num_assoc: meta.num_assoc });
                }
            }
        }
        out
    }

    /// The bucket for `key`, creating it on first touch — the probe hashes the borrowed bytes and
    /// copies them only when the key is new.
    fn bucket_mut<'s>(
        state: &'s mut S,
        key: &[u8],
        track: bool,
        delta: &mut isize,
    ) -> &'s mut JoinBucket {
        if !state.contains(key) {
            if track {
                *delta += join_key_entry_bytes(key) as isize;
            }
            return state.insert(ByteKey::from(key), JoinBucket::default());
        }
        state.get_mut(key).expect("bucket just ensured")
    }

    /// Bumps `row`'s appear-times in `bucket`, applying `on_existing` to its meta when it is already
    /// stored (the zero-allocation steady state) and inserting a fresh entry otherwise.
    fn bump_row(
        bucket: &mut JoinBucket,
        row: &[u8],
        fresh: RowMeta,
        on_existing: impl FnOnce(&mut RowMeta),
        track: bool,
        delta: &mut isize,
    ) {
        if let Some(meta) = bucket.get_mut(row) {
            on_existing(meta);
            return;
        }
        if track {
            *delta += join_row_entry_bytes(row) as isize;
        }
        bucket.insert(ByteKey::from(row), fresh);
    }

    /// `state.addRecord(record, num_assoc)` — bumps appear-times and (re)sets the degree, as Flink's
    /// no-unique-key `OuterJoinRecordStateView.addRecord`.
    fn add_record(
        state: &mut S,
        key: &[u8],
        row: &[u8],
        num_assoc: i32,
        track: bool,
        delta: &mut isize,
    ) {
        let bucket = Self::bucket_mut(state, key, track, delta);
        Self::bump_row(
            bucket,
            row,
            RowMeta { count: 1, num_assoc },
            |m| {
                m.count += 1;
                m.num_assoc = num_assoc;
            },
            track,
            delta,
        );
    }

    /// `state.updateNumOfAssociations(record, num_assoc)` — sets the degree of an existing row.
    fn update_num_assoc(
        state: &mut S,
        key: &[u8],
        row: &[u8],
        num_assoc: i32,
        track: bool,
        delta: &mut isize,
    ) {
        let bucket = Self::bucket_mut(state, key, track, delta);
        Self::bump_row(
            bucket,
            row,
            RowMeta { count: 1, num_assoc },
            |m| m.num_assoc = num_assoc,
            track,
            delta,
        );
    }

    /// `state.retractRecord(record)` — drops one appear-time, removing the row (and emptied key) at 0.
    fn retract_record(state: &mut S, key: &[u8], row: &[u8], track: bool, delta: &mut isize) {
        let mut emptied = false;
        if let Some(bucket) = state.get_mut(key) {
            if let Some(meta) = bucket.get_mut(row) {
                meta.count -= 1;
                if meta.count <= 0 {
                    bucket.remove(row);
                    if track {
                        *delta -= join_row_entry_bytes(row) as isize;
                    }
                }
            }
            emptied = bucket.is_empty();
        }
        if emptied {
            state.remove(key);
            if track {
                *delta -= join_key_entry_bytes(key) as isize;
            }
        }
    }

    /// Folds an input batch into its side and emits the join changelog it produces.
    pub(crate) fn push(&mut self, batch: &RecordBatch, is_left: bool) -> Result<RecordBatch, DataFusionError> {
        if self.mini_batch {
            return self.push_mini_batch(batch, is_left);
        }
        self.push_immediate(batch, is_left)
    }

    fn push_immediate(
        &mut self,
        batch: &RecordBatch,
        is_left: bool,
    ) -> Result<RecordBatch, DataFusionError> {
        let arity = data_arity(batch);
        let key_indices: &[usize] = if is_left { &self.left_keys } else { &self.right_keys };
        let key_arrays: Vec<ArrayRef> = key_indices.iter().map(|&i| batch.column(i).clone()).collect();
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let row_kinds = row_kind_column(batch);
        // A probe reads (and, for the degree, writes) the OTHER side's whole bucket for each input
        // key, so both sides hydrate the batch's equi-keys before the fold.
        self.left_state
            .begin_batch(batch, key_indices, &self.key_timestamp_precisions)?;
        self.right_state
            .begin_batch(batch, key_indices, &self.key_timestamp_precisions)?;
        let payloads = if is_left { &self.left_payload } else { &self.right_payload }
            .convert_columns(&data_arrays)
            .expect("encode join payload");
        // A null in any equi-key column matches nothing; flagged per row off the key arrays (the null
        // can't be recovered from the encoded key bytes).
        let key_null: Vec<bool> =
            (0..batch.num_rows()).map(|r| key_arrays.iter().any(|a| a.is_null(r))).collect();

        // INNER keeps no degree and never mutates the probe (other) side, so the whole batch's rows are
        // independent: each probes a fixed other-side state. That lets us gather every candidate pair,
        // decode/evaluate the residual predicate once per batch, and emit by filtering — no per-row
        // convert_rows/predicate batch, no per-pair row clone, no emit round-trip (the hot q3/q9/q23
        // path). The per-row state machine below still serves the degree-bearing outer/semi/anti kinds.
        if self.kind == JoinKind::Inner {
            return self.push_inner(is_left, batch, &payloads, &key_null, row_kinds);
        }

        let track = self.memory.tracking();
        let mut delta = 0isize;
        let mut out_left: Vec<ByteKey> = Vec::new();
        let mut out_right: Vec<ByteKey> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();

        let mut key_encoder =
            BinaryRowBatchEncoder::new(batch, key_indices, &self.key_timestamp_precisions);
        for row in 0..batch.num_rows() {
            // Absent `$row_kind$` (insert-only columnar input) ⇒ every row is an INSERT.
            let kind = row_kinds.map_or(0, |kinds| kinds.value(row));
            // Borrowed byte rows: the state probes hash these directly and copy only on first insert.
            let key = key_encoder.encode(row);
            let full = payloads.row(row);
            if self.kind.is_semi_anti() {
                self.process_semi_anti(
                    key, full.as_ref(), kind, is_left, key_null[row], &mut out_left,
                    &mut out_kinds, track, &mut delta,
                );
            } else {
                self.process_inner_outer(
                    key, full.as_ref(), kind, is_left, key_null[row], &mut out_left,
                    &mut out_right, &mut out_kinds, track, &mut delta,
                );
            }
        }
        self.left_state.end_bundle()?;
        self.right_state.end_bundle()?;
        self.memory.record(
            delta + self.left_state.footprint_delta() + self.right_state.footprint_delta(),
        );
        self.memory.account()?;
        Ok(self.emit(out_left, out_right, out_kinds))
    }

    fn staged_bytes_for(changes: &MiniBatchChanges<ByteKey, ByteKey>) -> usize {
        changes.retained_bytes(
            |key| key.0.len() + GROUP_ENTRY_OVERHEAD,
            |row| row.0.len() + GROUP_ENTRY_OVERHEAD,
        )
    }

    pub(crate) fn staging_bytes(&self) -> usize {
        Self::staged_bytes_for(&self.left_staged) + Self::staged_bytes_for(&self.right_staged)
    }

    pub(crate) fn staged_keys(&self) -> usize {
        self.left_staged.touched_keys() + self.right_staged.touched_keys()
    }

    /// Folds a unique-key side to its first durable row and final bundle row. Durable join state is
    /// unchanged until flush, so repeated replacements avoid all intermediate probes and output.
    fn push_mini_batch(
        &mut self,
        batch: &RecordBatch,
        is_left: bool,
    ) -> Result<RecordBatch, DataFusionError> {
        let arity = data_arity(batch);
        let key_indices: &[usize] = if is_left { &self.left_keys } else { &self.right_keys };
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        // The first-durable-row capture reads the input side's buckets, so they must be resident.
        if is_left {
            self.left_state
                .begin_batch(batch, key_indices, &self.key_timestamp_precisions)?;
        } else {
            self.right_state
                .begin_batch(batch, key_indices, &self.key_timestamp_precisions)?;
        }
        let payloads = if is_left { &self.left_payload } else { &self.right_payload }
            .convert_columns(&data_arrays)
            .expect("encode join payload");
        let row_kinds = row_kind_column(batch);
        let before_bytes = if self.memory.tracking() { self.staging_bytes() } else { 0 };
        let mut key_encoder =
            BinaryRowBatchEncoder::new(batch, key_indices, &self.key_timestamp_precisions);
        let (staged, state) = if is_left {
            (&mut self.left_staged, &self.left_state)
        } else {
            (&mut self.right_staged, &self.right_state)
        };
        for row in 0..batch.num_rows() {
            let key = key_encoder.encode(row);
            if !staged.contains_key(key) {
                let durable = state.get(key).and_then(|bucket| {
                    bucket
                        .iter()
                        .find(|(_, meta)| meta.count > 0)
                        .map(|(payload, _)| payload.clone())
                });
                staged.touch(ByteKey::from(key), durable);
            }
            let kind = row_kinds.map_or(0, |kinds| kinds.value(row));
            let after = if matches!(kind, 1 | 3) {
                None
            } else {
                Some(ByteKey::from(payloads.row(row).as_ref()))
            };
            staged.set_after(key, after);
        }
        if is_left {
            self.left_state.end_bundle()?;
        } else {
            self.right_state.end_bundle()?;
        }
        if self.memory.tracking() {
            self.memory.record(self.staging_bytes() as isize - before_bytes as isize);
        }
        self.memory.record(
            self.left_state.footprint_delta() + self.right_state.footprint_delta(),
        );
        self.memory.account()?;
        Ok(RecordBatch::new_empty(Arc::new(Schema::empty())))
    }

    fn staged_batch(
        &self,
        changes: Vec<(ByteKey, MiniBatchChange<ByteKey>)>,
        is_left: bool,
    ) -> Option<RecordBatch> {
        if changes.is_empty() {
            return None;
        }
        let mut rows = Vec::with_capacity(changes.len() * 2);
        let mut kinds = Vec::with_capacity(changes.len() * 2);
        for (_, change) in changes {
            match change {
                MiniBatchChange::Insert(after) => {
                    rows.push(after);
                    kinds.push(0);
                }
                MiniBatchChange::Delete(before) => {
                    rows.push(before);
                    kinds.push(3);
                }
                MiniBatchChange::Update { before, after } => {
                    rows.push(before);
                    kinds.push(3);
                    rows.push(after);
                    kinds.push(0);
                }
            }
        }
        let (schema, converter) = if is_left {
            (&self.left_schema, &self.left_payload)
        } else {
            (&self.right_schema, &self.right_payload)
        };
        let parser = converter.parser();
        let mut columns = converter
            .convert_rows(rows.iter().map(|row| parser.parse(&row.0)))
            .expect("decode staged join rows");
        let mut fields: Vec<Field> = schema.fields().iter().map(|field| field.as_ref().clone()).collect();
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(kinds)));
        Some(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("staged join batch"))
    }

    pub(crate) fn flush_mini_batch(&mut self) -> Result<RecordBatch, DataFusionError> {
        let staged_bytes = if self.memory.tracking() { self.staging_bytes() } else { 0 };
        let left = self.left_staged.drain();
        let right = self.right_staged.drain();
        self.memory.forget(staged_bytes);
        self.memory.account_shrink();

        let left_batch = self.staged_batch(left, true);
        let right_batch = self.staged_batch(right, false);
        let mut outputs = Vec::new();
        if let Some(batch) = left_batch {
            let out = self.push_immediate(&batch, true)?;
            if out.num_rows() > 0 {
                outputs.push(out);
            }
        }
        if let Some(batch) = right_batch {
            let out = self.push_immediate(&batch, false)?;
            if out.num_rows() > 0 {
                outputs.push(out);
            }
        }
        if outputs.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        concat_batches(&outputs[0].schema(), outputs.iter()).map_err(DataFusionError::from)
    }

    /// Rebuilds the joined changelog batch from the emitted byte rows: one vectorized `convert_rows`
    /// per side (left, and right for non-semi joins), concatenated, then the `$row_kind$` byte column.
    fn emit(&self, out_left: Vec<ByteKey>, out_right: Vec<ByteKey>, out_kinds: Vec<i8>) -> RecordBatch {
        if out_left.is_empty() {
            return RecordBatch::new_empty(Arc::new(Schema::empty()));
        }
        let left_parser = self.left_payload.parser();
        let mut columns: Vec<ArrayRef> = self
            .left_payload
            .convert_rows(out_left.iter().map(|r| left_parser.parse(&r.0)))
            .expect("decode left rows");
        if !self.kind.is_semi_anti() {
            let right_parser = self.right_payload.parser();
            let right_columns = self
                .right_payload
                .convert_rows(out_right.iter().map(|r| right_parser.parse(&r.0)))
                .expect("decode right rows");
            columns.extend(right_columns);
        }
        let mut fields: Vec<Field> = (0..columns.len())
            .map(|j| Field::new(format!("c{j}"), columns[j].data_type().clone(), true))
            .collect();
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build updating-join changelog batch")
    }

    /// Batched INNER push (the common case for q3/q9/q23). Because INNER keeps no degree and never
    /// touches the probe side, every input row associates against the same fixed other-side state, so
    /// the rows are independent: gather all candidate `[left.., right..]` pairs for the batch, decode
    /// and evaluate the residual predicate once, and emit by filtering. Byte-identical to the per-row
    /// `process_inner_outer` path (same match order, multiplicity, and per-row RowKind) but with one
    /// pair of `convert_rows` and one predicate eval per batch instead of per row, and the output built
    /// directly by `filter_record_batch` (no per-pair `OwnedRow` clone or emit round-trip).
    fn push_inner(
        &mut self,
        is_left: bool,
        batch: &RecordBatch,
        payloads: &Rows,
        key_null: &[bool],
        row_kinds: Option<&Int8Array>,
    ) -> Result<RecordBatch, DataFusionError> {
        // Split the two state stores so the input side can be mutated while the probe side is borrowed
        // (INNER never mutates the probe side, so the gathered match rows stay valid for the batch).
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let key_indices: &[usize] = if is_left { &self.left_keys } else { &self.right_keys };
        let mut key_encoder =
            BinaryRowBatchEncoder::new(batch, key_indices, &self.key_timestamp_precisions);
        let (input_state, other_state) = if is_left {
            (&mut self.left_state, &self.right_state)
        } else {
            (&mut self.right_state, &self.left_state)
        };
        let mut cand_input_idx: Vec<usize> = Vec::new();
        let mut cand_other: Vec<&ByteKey> = Vec::new();
        let mut cand_kind: Vec<i8> = Vec::new();
        for row in 0..batch.num_rows() {
            let kind = row_kinds.map_or(0, |kinds| kinds.value(row));
            // Borrowed byte rows: probes hash these directly; a steady-state row (key and content
            // already stored) allocates nothing — the SYS_ALLOC a differential profile flagged vs
            // Flink's reused BinaryRowData.
            let key = key_encoder.encode(row);
            let full = payloads.row(row);
            if !key_null[row] {
                if let Some(bucket) = other_state.get(key) {
                    for (other, meta) in bucket.iter() {
                        for _ in 0..meta.count.max(0) {
                            cand_input_idx.push(row);
                            cand_other.push(other);
                            cand_kind.push(kind);
                        }
                    }
                }
            }
            if kind == 0 || kind == 2 {
                let bucket = Self::bucket_mut(input_state, key, track, &mut delta);
                Self::bump_row(
                    bucket,
                    full.as_ref(),
                    RowMeta { count: 1, num_assoc: -1 },
                    |m| m.count += 1,
                    track,
                    &mut delta,
                );
            } else {
                Self::retract_record(input_state, key, full.as_ref(), track, &mut delta);
            }
        }
        self.memory.record(delta);
        self.memory.account()?;
        if cand_input_idx.is_empty() {
            self.left_state.end_bundle()?;
            self.right_state.end_bundle()?;
            self.memory.record(
                self.left_state.footprint_delta() + self.right_state.footprint_delta(),
            );
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        // Decode the matched other-side rows in one pass (releases the probe-side borrow — which
        // must happen before the bundle ends and drops clean hydrated slots), then the input rows
        // repeated per candidate — assembled into the joined `[left.., right..]` layout.
        let other_conv = if is_left { &self.right_payload } else { &self.left_payload };
        let other_parser = other_conv.parser();
        let other_cols = other_conv
            .convert_rows(cand_other.iter().map(|b| other_parser.parse(&b.0)))
            .expect("decode associated rows");
        drop(cand_other);
        self.left_state.end_bundle()?;
        self.right_state.end_bundle()?;
        self.memory
            .record(self.left_state.footprint_delta() + self.right_state.footprint_delta());
        let input_conv = if is_left { &self.left_payload } else { &self.right_payload };
        let input_cols = input_conv
            .convert_rows(cand_input_idx.iter().map(|&r| payloads.row(r)))
            .expect("decode join input rows");
        let joined = joined_schema(&self.left_schema, &self.right_schema);
        let mut data_columns: Vec<ArrayRef> = Vec::with_capacity(input_cols.len() + other_cols.len());
        if is_left {
            data_columns.extend(input_cols);
            data_columns.extend(other_cols);
        } else {
            data_columns.extend(other_cols);
            data_columns.extend(input_cols);
        }
        let data_batch =
            RecordBatch::try_new(joined.clone(), data_columns).expect("build join candidate batch");

        // A residual non-equi condition (Flink's `condition.apply`) is evaluated once over the whole
        // candidate batch; no condition means every pair is a match (q3/q20/q23) — skip the filter.
        let mask = self
            .predicate
            .as_mut()
            .map(|pred| BooleanArray::from(pred.evaluate_batch(&joined, &data_batch)));

        let mut fields: Vec<Field> = (0..data_batch.num_columns())
            .map(|j| Field::new(format!("c{j}"), data_batch.column(j).data_type().clone(), true))
            .collect();
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        let mut columns: Vec<ArrayRef> = data_batch.columns().to_vec();
        columns.push(Arc::new(Int8Array::from(cand_kind)));
        let full_batch =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("build inner-join batch");
        Ok(match mask {
            Some(mask) => filter_record_batch(&full_batch, &mask).expect("filter inner-join batch"),
            None => full_batch,
        })
    }

    /// INNER/LEFT/RIGHT/FULL — a faithful port of `StreamingJoinOperator.processElement`. `is_left`
    /// is whether the input arrived on the left; `kind` is the input row's `RowKind` byte
    /// (0=+I,1=-U,2=+U,3=-D). Output rows are `[left cols.., right cols..]`.
    fn process_inner_outer(
        &mut self,
        key: &[u8],
        full: &[u8],
        kind: i8,
        is_left: bool,
        key_has_null: bool,
        out_left: &mut Vec<ByteKey>,
        out_right: &mut Vec<ByteKey>,
        out_kinds: &mut Vec<i8>,
        track: bool,
        delta: &mut isize,
    ) {
        let accumulate = kind == 0 || kind == 2;
        let input_is_outer = if is_left { self.kind.left_is_outer() } else { self.kind.right_is_outer() };
        let other_is_outer = if is_left { self.kind.right_is_outer() } else { self.kind.left_is_outer() };
        let left_null = self.left_null.clone();
        let right_null = self.right_null.clone();
        // Each builder returns the `(left, right)` byte rows for one emitted output row; the input goes
        // on its own side, the other side is the match or a null pad.
        let paired = |other: &ByteKey| -> (ByteKey, ByteKey) {
            if is_left {
                (ByteKey::from(full), other.clone())
            } else {
                (other.clone(), ByteKey::from(full))
            }
        };
        let input_padded: (ByteKey, ByteKey) = if is_left {
            (ByteKey::from(full), right_null.clone())
        } else {
            (left_null.clone(), ByteKey::from(full))
        };
        let other_padded = |other: &ByteKey| -> (ByteKey, ByteKey) {
            if is_left { (left_null.clone(), other.clone()) } else { (other.clone(), right_null.clone()) }
        };

        // Gather the matching other-side rows (a null equi-key matches nothing), then drop those failing
        // the residual non-equi predicate — Flink's `condition.apply` filter. Done before the per-side
        // mutations below so no state borrow is held across the predicate eval.
        let mut associated = if key_has_null {
            Vec::new()
        } else {
            Self::associated(if is_left { &self.right_state } else { &self.left_state }, key)
        };
        self.filter_associated(full, is_left, &mut associated);

        if accumulate {
            if input_is_outer {
                if associated.is_empty() {
                    let (l, r) = input_padded;
                    out_left.push(l);
                    out_right.push(r);
                    out_kinds.push(0); // +I[record+null]
                    Self::add_record(self.input_state(is_left), key, full, 0, track, delta);
                } else {
                    let num = associated.len() as i32;
                    for other in &associated {
                        if other_is_outer {
                            if other.num_assoc == 0 {
                                let (l, r) = other_padded(&other.record);
                                out_left.push(l);
                                out_right.push(r);
                                out_kinds.push(3); // -D[null+other]
                            }
                            Self::update_num_assoc(self.other_state(is_left), key, &other.record.0, other.num_assoc + 1, track, delta);
                        }
                        let (l, r) = paired(&other.record);
                        out_left.push(l);
                        out_right.push(r);
                        out_kinds.push(0); // +I[record+other]
                    }
                    Self::add_record(self.input_state(is_left), key, full, num, track, delta);
                }
            } else {
                Self::add_record(self.input_state(is_left), key, full, -1, track, delta);
                for other in &associated {
                    if other_is_outer {
                        if other.num_assoc == 0 {
                            let (l, r) = other_padded(&other.record);
                            out_left.push(l);
                            out_right.push(r);
                            out_kinds.push(3); // -D[null+other]
                        }
                        Self::update_num_assoc(self.other_state(is_left), key, &other.record.0, other.num_assoc + 1, track, delta);
                        let (l, r) = paired(&other.record);
                        out_left.push(l);
                        out_right.push(r);
                        out_kinds.push(0); // +I[record+other]
                    } else {
                        let (l, r) = paired(&other.record);
                        out_left.push(l);
                        out_right.push(r);
                        out_kinds.push(kind); // +I/+U[record+other] (input RowKind)
                    }
                }
            }
        } else {
            Self::retract_record(self.input_state(is_left), key, full, track, delta);
            if associated.is_empty() {
                if input_is_outer {
                    let (l, r) = input_padded;
                    out_left.push(l);
                    out_right.push(r);
                    out_kinds.push(3); // -D[record+null]
                }
            } else {
                for other in &associated {
                    let (l, r) = paired(&other.record);
                    out_left.push(l);
                    out_right.push(r);
                    out_kinds.push(if input_is_outer { 3 } else { kind }); // -D / -D|-U (input RowKind)
                    if other_is_outer {
                        if other.num_assoc == 1 {
                            let (l, r) = other_padded(&other.record);
                            out_left.push(l);
                            out_right.push(r);
                            out_kinds.push(0); // +I[null+other]
                        }
                        Self::update_num_assoc(self.other_state(is_left), key, &other.record.0, other.num_assoc - 1, track, delta);
                    }
                }
            }
        }
    }

    /// The state store for the arriving (input) side.
    fn input_state(&mut self, is_left: bool) -> &mut S {
        if is_left { &mut self.left_state } else { &mut self.right_state }
    }

    /// The state store for the side opposite the arriving one.
    fn other_state(&mut self, is_left: bool) -> &mut S {
        if is_left { &mut self.right_state } else { &mut self.left_state }
    }

    /// SEMI/ANTI — a faithful port of `StreamingSemiAntiJoinOperator`. The left side carries the
    /// degree (it is the side whose rows are emitted); the right side is plain. Output rows are the
    /// left columns only.
    fn process_semi_anti(
        &mut self,
        key: &[u8],
        full: &[u8],
        kind: i8,
        is_left: bool,
        key_has_null: bool,
        out_rows: &mut Vec<ByteKey>,
        out_kinds: &mut Vec<i8>,
        track: bool,
        delta: &mut isize,
    ) {
        let accumulate = kind == 0 || kind == 2;
        let is_anti = self.kind == JoinKind::Anti;
        if is_left {
            // processElement1: emit the input row when it has (semi) / lacks (anti) a match, then
            // record it with its current match count as its degree.
            let mut associated =
                if key_has_null { Vec::new() } else { Self::associated(&self.right_state, key) };
            self.filter_associated(full, true, &mut associated);
            let matched = !associated.is_empty();
            if matched != is_anti {
                out_rows.push(ByteKey::from(full));
                out_kinds.push(kind); // forward input RowKind
            }
            if accumulate {
                Self::add_record(&mut self.left_state, key, full, associated.len() as i32, track, delta);
            } else {
                Self::retract_record(&mut self.left_state, key, full, track, delta);
            }
        } else {
            // processElement2: a right row flips associated left rows' degree across 0↔1, emitting or
            // retracting them (semi) or the inverse (anti).
            let mut associated =
                if key_has_null { Vec::new() } else { Self::associated(&self.left_state, key) };
            self.filter_associated(full, false, &mut associated);
            if accumulate {
                Self::add_record(&mut self.right_state, key, full, -1, track, delta);
                for other in &associated {
                    if other.num_assoc == 0 {
                        // anti: -D[left]; semi: +I/+U[left] (input RowKind)
                        out_rows.push(other.record.clone());
                        out_kinds.push(if is_anti { 3 } else { kind });
                    }
                    Self::update_num_assoc(&mut self.left_state, key, &other.record.0, other.num_assoc + 1, track, delta);
                }
            } else {
                Self::retract_record(&mut self.right_state, key, full, track, delta);
                for other in &associated {
                    if other.num_assoc == 1 {
                        // semi: -D/-U[left] (input RowKind); anti: +I[left]
                        out_rows.push(other.record.clone());
                        out_kinds.push(if is_anti { 0 } else { kind });
                    }
                    Self::update_num_assoc(&mut self.left_state, key, &other.record.0, other.num_assoc - 1, track, delta);
                }
            }
        }
    }

}

/// The raw keyed-state snapshot/restore surface exists only on the memory backend — a persistent
/// store checkpoints through its own commit path instead of materializing the key space.
impl UpdatingJoiner {
    /// Materializes one side's multiset as `[data cols.., __count__, __assoc__]` (one row per
    /// distinct live row), or no batch when the side has no rows yet.
    fn side_snapshot_batch(&self, is_left: bool) -> Option<RecordBatch> {
        let (schema, state, conv) = if is_left {
            (&self.left_schema, &self.left_state, &self.left_payload)
        } else {
            (&self.right_schema, &self.right_state, &self.right_payload)
        };
        let parser = conv.parser();
        let mut rows: Vec<&ByteKey> = Vec::new();
        let mut counts: Vec<i64> = Vec::new();
        let mut assocs: Vec<i32> = Vec::new();
        for (_, bucket) in state.iter() {
            for (row, meta) in bucket.iter() {
                rows.push(row);
                counts.push(meta.count);
                assocs.push(meta.num_assoc);
            }
        }
        if rows.is_empty() {
            return None;
        }
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = conv
            .convert_rows(rows.iter().map(|b| parser.parse(&b.0)))
            .expect("decode join side rows");
        fields.push(Field::new("__count__", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(counts)));
        fields.push(Field::new("__assoc__", DataType::Int32, false));
        columns.push(Arc::new(Int32Array::from(assocs)));
        Some(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("join side"))
    }

    fn serialize_side(&self, is_left: bool) -> Vec<u8> {
        self.side_snapshot_batch(is_left)
            .map(|batch| write_ipc(&batch))
            .unwrap_or_default()
    }

    pub(crate) fn snapshot(&self) -> Vec<u8> {
        let left = self.serialize_side(true);
        let right = self.serialize_side(false);
        Self::snapshot_parts(left, right)
    }

    fn snapshot_parts(left: Vec<u8>, right: Vec<u8>) -> Vec<u8> {
        let mut out = (left.len() as u32).to_le_bytes().to_vec();
        out.extend_from_slice(&left);
        out.extend_from_slice(&right);
        out
    }

    pub(crate) fn snapshot_partitions(
        &self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        self.raw_snapshot_partitions(max_parallelism, timestamp_precisions)
    }

    fn raw_snapshot_partitions(
        &self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        let left = self.side_raw_partitions(
            self.side_snapshot_batch(true),
            &self.left_keys,
            max_parallelism,
            timestamp_precisions,
        );
        let right = self.side_raw_partitions(
            self.side_snapshot_batch(false),
            &self.right_keys,
            max_parallelism,
            timestamp_precisions,
        );
        let mut groups: Vec<i32> = left.keys().chain(right.keys()).copied().collect();
        groups.sort_unstable();
        groups.dedup();
        let mut snapshots = BTreeMap::new();
        for key_group in groups {
            snapshots.insert(
                key_group,
                Self::snapshot_parts(
                    left.get(&key_group).map(Self::merge_snapshot_batches).unwrap_or_default(),
                    right.get(&key_group).map(Self::merge_snapshot_batches).unwrap_or_default(),
                ),
            );
        }
        snapshots
    }

    fn side_raw_partitions(
        &self,
        batch: Option<RecordBatch>,
        key_columns: &[usize],
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<RecordBatch>> {
        let mut partitions = BTreeMap::new();
        if let Some(batch) = batch {
            let mut rows_by_group: BTreeMap<i32, Vec<u32>> = BTreeMap::new();
            for row in 0..batch.num_rows() {
                let key_group = flink_key_group(
                    binary_row_hash(&batch, key_columns, row, timestamp_precisions),
                    max_parallelism,
                ) as i32;
                rows_by_group.entry(key_group).or_default().push(row as u32);
            }
            for (key_group, rows) in rows_by_group {
                let indices = UInt32Array::from(rows);
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| take(column, &indices, None).expect("partition join snapshot"))
                    .collect();
                partitions
                    .entry(key_group)
                    .or_insert_with(Vec::new)
                    .push(
                        RecordBatch::try_new(batch.schema(), columns)
                            .expect("partitioned join snapshot"),
                    );
            }
        }
        partitions
    }

    fn merge_snapshot_batches(batches: &Vec<RecordBatch>) -> Vec<u8> {
        let combined = concat_batches(&batches[0].schema(), batches.iter())
            .expect("merge updating-join raw partitions");
        write_ipc(&combined)
    }

    pub(crate) fn restore(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        kind: JoinKind,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        predicate: Option<JoinPredicate>,
        bytes: &[u8],
    ) -> Self {
        let mut joiner =
            UpdatingJoiner::new(left_keys, right_keys, kind, left_schema, right_schema, predicate);
        if bytes.is_empty() {
            return joiner;
        }
        let left_len = u32::from_le_bytes(bytes[0..4].try_into().expect("snapshot len")) as usize;
        joiner.load_side(true, &bytes[4..4 + left_len]);
        joiner.load_side(false, &bytes[4 + left_len..]);
        joiner
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore_partitions(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        kind: JoinKind,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        predicate: Option<JoinPredicate>,
        snapshots: &[Vec<u8>],
    ) -> Self {
        let mut left_batches = Vec::new();
        let mut right_batches = Vec::new();
        for bytes in snapshots {
            if bytes.len() < 4 {
                continue;
            }
            let left_len = u32::from_le_bytes(bytes[0..4].try_into().expect("snapshot len")) as usize;
            assert!(4 + left_len <= bytes.len(), "truncated updating-join raw key-group snapshot");
            left_batches.extend(read_ipc_if_present(&bytes[4..4 + left_len]));
            right_batches.extend(read_ipc_if_present(&bytes[4 + left_len..]));
        }
        let left = (!left_batches.is_empty())
            .then(|| Self::merge_snapshot_batches(&left_batches))
            .unwrap_or_default();
        let right = (!right_batches.is_empty())
            .then(|| Self::merge_snapshot_batches(&right_batches))
            .unwrap_or_default();
        UpdatingJoiner::restore(
            left_keys,
            right_keys,
            kind,
            left_schema,
            right_schema,
            predicate,
            &Self::snapshot_parts(left, right),
        )
    }

    fn load_side(&mut self, is_left: bool, bytes: &[u8]) {
        for batch in read_ipc_if_present(bytes) {
            // The snapshot side batch is `[data cols.., __count__, __assoc__]`; the data columns are
            // all but the two trailing bookkeeping columns.
            let arity = batch.num_columns() - 2;
            let key_indices = if is_left { &self.left_keys } else { &self.right_keys };
            let key_arrays: Vec<ArrayRef> = key_indices.iter().map(|&i| batch.column(i).clone()).collect();
            let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
            let counts = column_i64(&batch, "__count__");
            let assocs = column_i32(&batch, "__assoc__");
            let mut key_encoder =
                BinaryRowBatchEncoder::new(&batch, key_indices, &self.key_timestamp_precisions);
            let payloads = if is_left { &self.left_payload } else { &self.right_payload }
                .convert_columns(&data_arrays)
                .expect("encode join payload");
            let state = if is_left { &mut self.left_state } else { &mut self.right_state };
            for row in 0..batch.num_rows() {
                let key = key_encoder.encode(row);
                let bucket = if state.contains(key) {
                    state.get_mut(key).expect("bucket present")
                } else {
                    state.insert(ByteKey::from(key), JoinBucket::default())
                };
                bucket.insert(
                    ByteKey::from(payloads.row(row).as_ref()),
                    RowMeta { count: counts.value(row), num_assoc: assocs.value(row) },
                );
            }
        }
    }
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_updatingJoinerStateBytes, UpdatingJoiner);

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updatingJoinerStagingBytes<'local>(
    _env: JNIEnv<'local>, _class: JClass<'local>, handle: jlong,
) -> jlong {
    let joiner = unsafe { &*(handle as *const UpdatingJoiner) };
    joiner.staging_bytes() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updatingJoinerStagedKeys<'local>(
    _env: JNIEnv<'local>, _class: JClass<'local>, handle: jlong,
) -> jlong {
    let joiner = unsafe { &*(handle as *const UpdatingJoiner) };
    joiner.staged_keys() as jlong
}

/// Creates a regular (non-windowed) updating joiner and returns an opaque handle. The key column
/// indices locate the equi-join key within each side's input batch (whose trailing column is the
/// `$row_kind$` byte); the join type selects INNER/outer/semi-anti; the two schema addresses seed the
/// per-side data schemas (so outer null-padding can be typed); the encoded arrays carry the optional
/// residual non-equi predicate. The JVM owns the handle and must release it with the matching close.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createUpdatingJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
    mini_batch: jboolean,
    memory_budget_bytes: jlong,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let left_schema = import_schema(left_schema_address);
    let right_schema = import_schema(right_schema_address);
    let predicate = read_join_predicate(
        &mut env,
        &pred_kinds,
        &pred_payload,
        &pred_child_counts,
        &pred_longs,
        &pred_doubles,
        &pred_strings,
    );
    let joiner = UpdatingJoiner::new(
        left,
        right,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
        predicate,
    )
    .with_mini_batch(mini_batch != 0)
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, joiner)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushUpdatingJoiner<'local>(
    mut env: JNIEnv<'local>, _class: JClass<'local>, handle: jlong,
    out_array_address: jlong, out_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut UpdatingJoiner) };
    match joiner.flush_mini_batch() {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Folds a left batch into state and exports the join changelog it produces (left cols, right cols,
/// then `$row_kind$`).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushLeftUpdatingJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut UpdatingJoiner) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        joiner.push(&batch, true)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Folds a right batch into state and exports the join changelog it produces.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushRightUpdatingJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut UpdatingJoiner) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        joiner.push(&batch, false)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Serializes the updating joiner's per-side state for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotUpdatingJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let joiner = unsafe { &mut *(handle as *mut UpdatingJoiner) };
    env.byte_array_from_slice(&joiner.snapshot())
        .expect("failed to allocate updating-join snapshot array")
        .into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotUpdatingJoinerPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    let joiner = unsafe { &*(handle as *const UpdatingJoiner) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    keyed_state_partition_array(
        &mut env,
        joiner.snapshot_partitions(max_parallelism as usize, &precisions),
        "updating-join",
    )
}

/// Rebuilds an updating joiner from a snapshot and returns a fresh handle.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreUpdatingJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
    mini_batch: jboolean,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let left_schema = import_schema(left_schema_address);
    let right_schema = import_schema(right_schema_address);
    let predicate = read_join_predicate(
        &mut env,
        &pred_kinds,
        &pred_payload,
        &pred_child_counts,
        &pred_longs,
        &pred_doubles,
        &pred_strings,
    );
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read updating-join snapshot");
    let joiner = UpdatingJoiner::restore(
        left,
        right,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
        predicate,
        &bytes,
    )
    .with_mini_batch(mini_batch != 0)
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, joiner)
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreUpdatingJoinerPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
    mini_batch: jboolean,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let left_schema = import_schema(left_schema_address);
    let right_schema = import_schema(right_schema_address);
    let predicate = read_join_predicate(
        &mut env,
        &pred_kinds,
        &pred_payload,
        &pred_child_counts,
        &pred_longs,
        &pred_doubles,
        &pred_strings,
    );
    let count = env
        .get_array_length(&snapshots)
        .expect("read updating-join raw partition count");
    let mut restored = Vec::with_capacity(count as usize);
    for index in 0..count {
        let bytes = JByteArray::from(
            env.get_object_array_element(&snapshots, index)
                .expect("read updating-join raw partition"),
        );
        restored.push(
            env.convert_byte_array(&bytes)
                .expect("read updating-join raw partition bytes"),
        );
    }
    let joiner = UpdatingJoiner::restore_partitions(
        left,
        right,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
        predicate,
        &restored,
    )
    .with_mini_batch(mini_batch != 0)
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, joiner)
}

/// Releases an updating joiner handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeUpdatingJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<UpdatingJoiner>(handle));
    }
}
