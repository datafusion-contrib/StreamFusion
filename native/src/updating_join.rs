use crate::*;

pub(crate) struct UpdatingJoiner {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    kind: JoinKind,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    predicate: Option<JoinPredicate>,
    /// arrow-row codecs: the memcomparable equi-key (shared config across sides, so equal keys encode
    /// to equal bytes and match), and the value-encoded full row per side. State holds these bytes, not
    /// `Vec<ScalarValue>` — so build/probe hash a byte slice and a stored row is one byte buffer.
    key_converter: RowConverter,
    left_payload: RowConverter,
    right_payload: RowConverter,
    /// A value-encoded all-null row per side, used to null-pad the absent side of an outer join.
    left_null: ByteKey,
    right_null: ByteKey,
    left_state: SideState,
    right_state: SideState,
    pub(crate) memory: OperatorMemory,
}

/// One side's multiset state: equi-key bytes → (row bytes → appear-count/degree). Keys are
/// `ByteKey`, so the steady-state probe — an input row whose key and content are already stored —
/// hashes the borrowed arrow-row bytes and allocates nothing; only a first insert copies them.
/// This was the differential profile's system-allocator signal vs Flink's pooled BinaryRowData.
pub(crate) type SideState = ahash::HashMap<ByteKey, ahash::HashMap<ByteKey, RowMeta>>;

/// Estimated footprint of one stored join row (arrow-row bytes + its appear-count meta + map entry).
pub(crate) fn join_row_entry_bytes(row: &[u8]) -> usize {
    row.len() + std::mem::size_of::<RowMeta>() + GROUP_ENTRY_OVERHEAD
}

/// Estimated footprint of a stored key entry.
pub(crate) fn join_key_entry_bytes(key: &[u8]) -> usize {
    key.len() + GROUP_ENTRY_OVERHEAD
}

/// Estimated footprint of one side of the updating join's state (for the restore-time account).
pub(crate) fn join_state_bytes(state: &SideState) -> usize {
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
        // The equi-key types are the same on both sides; one converter config encodes either side's
        // key columns to identical bytes for equal values.
        let key_converter = RowConverter::new(
            left_keys.iter().map(|&i| SortField::new(left_schema.field(i).data_type().clone())).collect(),
        )
        .expect("join key converter");
        let left_payload = payload_converter(&left_schema);
        let right_payload = payload_converter(&right_schema);
        let left_null = ByteKey::from(encode_null_row(&left_payload, &left_schema).row().as_ref());
        let right_null = ByteKey::from(encode_null_row(&right_payload, &right_schema).row().as_ref());
        UpdatingJoiner {
            left_keys,
            right_keys,
            kind,
            left_schema,
            right_schema,
            predicate,
            key_converter,
            left_payload,
            right_payload,
            left_null,
            right_null,
            left_state: ahash::HashMap::default(),
            right_state: ahash::HashMap::default(),
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

    /// Drops the candidate matches whose `[left.., right..]` pair fails the residual non-equi
    /// predicate, so only condition-satisfying rows feed the degree and the emitted output (Flink's
    /// `condition.apply` filter). A no-op when there is no predicate.
    fn filter_associated(&mut self, full: &[u8], is_left: bool, associated: &mut Vec<OuterRecord>) {
        if associated.is_empty() || self.predicate.is_none() {
            return;
        }
        let joined = Self::joined_schema(&self.left_schema, &self.right_schema);
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
    fn associated(other_state: &SideState, key: &[u8]) -> Vec<OuterRecord> {
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
        state: &'s mut SideState,
        key: &[u8],
        track: bool,
        delta: &mut isize,
    ) -> &'s mut ahash::HashMap<ByteKey, RowMeta> {
        if !state.contains_key(key) {
            if track {
                *delta += join_key_entry_bytes(key) as isize;
            }
            state.insert(ByteKey::from(key), ahash::HashMap::default());
        }
        state.get_mut(key).expect("bucket just ensured")
    }

    /// Bumps `row`'s appear-times in `bucket`, applying `on_existing` to its meta when it is already
    /// stored (the zero-allocation steady state) and inserting a fresh entry otherwise.
    fn bump_row(
        bucket: &mut ahash::HashMap<ByteKey, RowMeta>,
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
        state: &mut SideState,
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
        state: &mut SideState,
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
    fn retract_record(state: &mut SideState, key: &[u8], row: &[u8], track: bool, delta: &mut isize) {
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
            if bucket.is_empty() {
                state.remove(key);
                if track {
                    *delta -= join_key_entry_bytes(key) as isize;
                }
            }
        }
    }

    /// Folds an input batch into its side and emits the join changelog it produces.
    pub(crate) fn push(&mut self, batch: &RecordBatch, is_left: bool) -> Result<RecordBatch, DataFusionError> {
        let arity = data_arity(batch);
        let key_indices = if is_left { &self.left_keys } else { &self.right_keys };
        let key_arrays: Vec<ArrayRef> = key_indices.iter().map(|&i| batch.column(i).clone()).collect();
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let row_kinds = row_kind_column(batch);
        // Encode the batch's equi-keys and full rows columnar->row in two vectorized passes (these
        // `Rows` own their buffers, so no borrow of self is held into the per-row loop below).
        let keys = self.key_converter.convert_columns(&key_arrays).expect("encode join key");
        let payloads = if is_left { &self.left_payload } else { &self.right_payload }
            .convert_columns(&data_arrays)
            .expect("encode join payload");
        // A null in any equi-key column matches nothing; flagged per row off the key arrays (the null
        // can't be recovered from the memcomparable key bytes once encoded).
        let key_null: Vec<bool> =
            (0..batch.num_rows()).map(|r| key_arrays.iter().any(|a| a.is_null(r))).collect();

        // INNER keeps no degree and never mutates the probe (other) side, so the whole batch's rows are
        // independent: each probes a fixed other-side state. That lets us gather every candidate pair,
        // decode/evaluate the residual predicate once per batch, and emit by filtering — no per-row
        // convert_rows/predicate batch, no per-pair row clone, no emit round-trip (the hot q3/q9/q23
        // path). The per-row state machine below still serves the degree-bearing outer/semi/anti kinds.
        if self.kind == JoinKind::Inner {
            return self.push_inner(is_left, &keys, &payloads, &key_null, row_kinds);
        }

        let track = self.memory.tracking();
        let mut delta = 0isize;
        let mut out_left: Vec<ByteKey> = Vec::new();
        let mut out_right: Vec<ByteKey> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();

        for row in 0..batch.num_rows() {
            // Absent `$row_kind$` (insert-only columnar input) ⇒ every row is an INSERT.
            let kind = row_kinds.map_or(0, |kinds| kinds.value(row));
            // Borrowed byte rows: the state probes hash these directly and copy only on first insert.
            let key = keys.row(row);
            let full = payloads.row(row);
            if self.kind.is_semi_anti() {
                self.process_semi_anti(
                    key.as_ref(), full.as_ref(), kind, is_left, key_null[row], &mut out_left,
                    &mut out_kinds, track, &mut delta,
                );
            } else {
                self.process_inner_outer(
                    key.as_ref(), full.as_ref(), kind, is_left, key_null[row], &mut out_left,
                    &mut out_right, &mut out_kinds, track, &mut delta,
                );
            }
        }
        self.memory.record(delta);
        self.memory.account()?;
        Ok(self.emit(out_left, out_right, out_kinds))
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
        keys: &Rows,
        payloads: &Rows,
        key_null: &[bool],
        row_kinds: Option<&Int8Array>,
    ) -> Result<RecordBatch, DataFusionError> {
        // Split the two state maps so the input side can be mutated while the probe side is borrowed
        // (INNER never mutates the probe side, so the gathered match rows stay valid for the batch).
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let (input_state, other_state) = if is_left {
            (&mut self.left_state, &self.right_state)
        } else {
            (&mut self.right_state, &self.left_state)
        };
        let mut cand_input_idx: Vec<usize> = Vec::new();
        let mut cand_other: Vec<&ByteKey> = Vec::new();
        let mut cand_kind: Vec<i8> = Vec::new();
        for row in 0..keys.num_rows() {
            let kind = row_kinds.map_or(0, |kinds| kinds.value(row));
            // Borrowed byte rows: probes hash these directly; a steady-state row (key and content
            // already stored) allocates nothing — the SYS_ALLOC a differential profile flagged vs
            // Flink's reused BinaryRowData.
            let key = keys.row(row);
            let full = payloads.row(row);
            if !key_null[row] {
                if let Some(bucket) = other_state.get(key.as_ref()) {
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
                let bucket = Self::bucket_mut(input_state, key.as_ref(), track, &mut delta);
                Self::bump_row(
                    bucket,
                    full.as_ref(),
                    RowMeta { count: 1, num_assoc: -1 },
                    |m| m.count += 1,
                    track,
                    &mut delta,
                );
            } else {
                Self::retract_record(input_state, key.as_ref(), full.as_ref(), track, &mut delta);
            }
        }
        self.memory.record(delta);
        self.memory.account()?;
        if cand_input_idx.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        // Decode the matched other-side rows in one pass (releases the probe-side borrow), then the
        // input rows repeated per candidate — assembled into the joined `[left.., right..]` layout.
        let other_conv = if is_left { &self.right_payload } else { &self.left_payload };
        let other_parser = other_conv.parser();
        let other_cols = other_conv
            .convert_rows(cand_other.iter().map(|b| other_parser.parse(&b.0)))
            .expect("decode associated rows");
        let input_conv = if is_left { &self.left_payload } else { &self.right_payload };
        let input_cols = input_conv
            .convert_rows(cand_input_idx.iter().map(|&r| payloads.row(r)))
            .expect("decode join input rows");
        let joined = Self::joined_schema(&self.left_schema, &self.right_schema);
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

    /// The state map for the arriving (input) side.
    fn input_state(&mut self, is_left: bool) -> &mut SideState {
        if is_left { &mut self.left_state } else { &mut self.right_state }
    }

    /// The state map for the side opposite the arriving one.
    fn other_state(&mut self, is_left: bool) -> &mut SideState {
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

    /// Serializes one side's multiset as `[data cols.., __count__, __assoc__]` (one row per distinct
    /// live row), or no bytes when the side has no rows yet.
    fn serialize_side(&self, is_left: bool) -> Vec<u8> {
        let (schema, state, conv) = if is_left {
            (&self.left_schema, &self.left_state, &self.left_payload)
        } else {
            (&self.right_schema, &self.right_state, &self.right_payload)
        };
        let parser = conv.parser();
        let mut rows: Vec<&ByteKey> = Vec::new();
        let mut counts: Vec<i64> = Vec::new();
        let mut assocs: Vec<i32> = Vec::new();
        for bucket in state.values() {
            for (row, meta) in bucket.iter() {
                rows.push(row);
                counts.push(meta.count);
                assocs.push(meta.num_assoc);
            }
        }
        if rows.is_empty() {
            return Vec::new();
        }
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = conv
            .convert_rows(rows.iter().map(|b| parser.parse(&b.0)))
            .expect("decode join side rows");
        fields.push(Field::new("__count__", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(counts)));
        fields.push(Field::new("__assoc__", DataType::Int32, false));
        columns.push(Arc::new(Int32Array::from(assocs)));
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("join side"))
    }

    pub(crate) fn snapshot(&self) -> Vec<u8> {
        let left = self.serialize_side(true);
        let right = self.serialize_side(false);
        let mut out = (left.len() as u32).to_le_bytes().to_vec();
        out.extend_from_slice(&left);
        out.extend_from_slice(&right);
        out
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
        let left_len = u32::from_le_bytes(bytes[0..4].try_into().expect("snapshot len")) as usize;
        joiner.load_side(true, &bytes[4..4 + left_len]);
        joiner.load_side(false, &bytes[4 + left_len..]);
        joiner
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
            let keys = self.key_converter.convert_columns(&key_arrays).expect("encode join key");
            let payloads = if is_left { &self.left_payload } else { &self.right_payload }
                .convert_columns(&data_arrays)
                .expect("encode join payload");
            let state = if is_left { &mut self.left_state } else { &mut self.right_state };
            for row in 0..batch.num_rows() {
                state.entry(ByteKey::from(keys.row(row).as_ref())).or_default().insert(
                    ByteKey::from(payloads.row(row).as_ref()),
                    RowMeta { count: counts.value(row), num_assoc: assocs.value(row) },
                );
            }
        }
    }
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_updatingJoinerStateBytes, UpdatingJoiner);

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
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, joiner)
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
