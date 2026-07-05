use crate::*;

/// Append-only keep-first deduplication on a rowtime order — Flink's
/// `RowTimeDeduplicateKeepFirstRowFunction`. Per partition key it keeps the row with the minimum
/// rowtime and emits it exactly once, when a watermark reaches that rowtime; every later row for the
/// key is then ignored, and a row arriving with a rowtime already below the watermark is dropped as
/// late. Insert-only: once a key's candidate fires, no smaller-rowtime row can still arrive (it would
/// be late), so the emitted row is final and never retracted.
///
/// Columnar: the per-key candidates live as a single Arrow batch — one row per pending key — and row
/// data moves only through `filter`/`take`/`concat` kernels, never materialized into scalars. Each
/// batch is reduced to its per-key minimum-rowtime row and merged with the standing candidates; only
/// the key (for grouping) and the rowtime (i64) are read per row, as any keyed reduction must.
pub(crate) struct KeepFirstDeduplicator {
    partition_columns: Vec<usize>,
    rt_column: usize,
    current_watermark: i64,
    /// One row per pending key — that key's minimum-rowtime candidate — awaiting its release.
    pending: Option<RecordBatch>,
    /// Keys whose first row has already been emitted; later rows for them are ignored.
    emitted: HashSet<GroupKey>,
    schema: Option<SchemaRef>,
    memory: OperatorMemory,
}

impl KeepFirstDeduplicator {
    fn new(partition_columns: Vec<usize>, rt_column: usize) -> Self {
        KeepFirstDeduplicator {
            partition_columns,
            rt_column,
            current_watermark: i64::MIN,
            pending: None,
            emitted: HashSet::default(),
            schema: None,
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds this deduplicator's state (the pending batch plus the emitted-key set) by the
    /// operator's managed-memory budget (negative = unaccounted).
    fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state = self.pending.as_ref().map_or(0, |b| b.get_array_memory_size())
            + self.emitted.iter().map(group_key_bytes).sum::<usize>();
        self.memory.attach("keep-first-deduplicate", budget_bytes, state)?;
        Ok(self)
    }

    fn push(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let schema = batch.schema();
        self.schema = Some(schema.clone());
        // Drop late rows (rowtime already below the watermark) with a columnar filter.
        let rt = rt_to_millis(batch.column(self.rt_column));
        let live_mask: BooleanArray =
            rt.iter().map(|v| Some(v.unwrap() >= self.current_watermark)).collect();
        let live = filter_record_batch(batch, &live_mask).expect("dedup late filter");
        // Merge with the standing candidates and reduce to one minimum-rowtime row per pending key.
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let combined = match self.pending.take() {
            Some(prev) => {
                if track {
                    delta -= prev.get_array_memory_size() as isize;
                }
                concat_batches(&schema, [&prev, &live]).expect("dedup concat")
            }
            None => live,
        };
        let reduced = self.min_per_key(&combined);
        if track && reduced.num_rows() > 0 {
            delta += reduced.get_array_memory_size() as isize;
        }
        self.pending = (reduced.num_rows() > 0).then_some(reduced);
        self.memory.record(delta);
        self.memory.account()
    }

    /// Reduces a batch to one row per non-emitted key: the row with the minimum rowtime, ties going to
    /// the earlier position (candidates precede new rows in `combined`, so a tie keeps the incumbent —
    /// Flink's keep-first rule of replacing only on a strictly smaller rowtime). The winning rows are
    /// gathered with `take`; the row data is never materialized into scalars.
    fn min_per_key(&self, batch: &RecordBatch) -> RecordBatch {
        let key_arrays: Vec<&ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i)).collect();
        let rt = rt_to_millis(batch.column(self.rt_column));
        let mut best: HashMap<GroupKey, (i64, u32)> = HashMap::default();
        for row in 0..batch.num_rows() {
            let key = read_key(&key_arrays, row);
            if self.emitted.contains(&key) {
                continue; // this key's first row already emitted
            }
            let rowtime = rt.value(row);
            match best.get(&key) {
                Some((existing, _)) if *existing <= rowtime => {}
                _ => {
                    best.insert(key, (rowtime, row as u32));
                }
            }
        }
        let mut indices: Vec<u32> = best.into_values().map(|(_, idx)| idx).collect();
        indices.sort_unstable();
        let idx = UInt32Array::from(indices);
        let columns: Vec<ArrayRef> =
            batch.columns().iter().map(|c| take(c, &idx, None).expect("dedup take")).collect();
        RecordBatch::try_new(batch.schema(), columns).expect("dedup compacted batch")
    }

    /// Emits each pending key's candidate whose rowtime the watermark has now reached (insert-only),
    /// records those keys as emitted, and keeps the rest. Both partitions are columnar filters.
    fn flush(&mut self, watermark: i64) -> Result<RecordBatch, DataFusionError> {
        self.current_watermark = watermark;
        let Some(pending) = self.pending.take() else {
            return Ok(self.empty());
        };
        let track = self.memory.tracking();
        let mut delta = 0isize;
        if track {
            delta -= pending.get_array_memory_size() as isize;
        }
        let rt = rt_to_millis(pending.column(self.rt_column));
        let ready_mask: BooleanArray = rt.iter().map(|v| Some(v.unwrap() <= watermark)).collect();
        let ready = filter_record_batch(&pending, &ready_mask).expect("dedup ready filter");
        let not_ready =
            filter_record_batch(&pending, &arrow::compute::not(&ready_mask).expect("dedup not"))
                .expect("dedup keep filter");
        if track && not_ready.num_rows() > 0 {
            delta += not_ready.get_array_memory_size() as isize;
        }
        self.pending = (not_ready.num_rows() > 0).then_some(not_ready);
        if ready.num_rows() > 0 {
            let key_arrays: Vec<&ArrayRef> =
                self.partition_columns.iter().map(|&i| ready.column(i)).collect();
            for row in 0..ready.num_rows() {
                let key = read_key(&key_arrays, row);
                let key_bytes = if track { group_key_bytes(&key) } else { 0 };
                // The emitted-key set grows for the operator's lifetime, so a flush can grow state.
                if self.emitted.insert(key) {
                    delta += key_bytes as isize;
                }
            }
        }
        self.memory.record(delta);
        self.memory.account()?;
        Ok(ready)
    }

    fn empty(&self) -> RecordBatch {
        match &self.schema {
            Some(schema) => RecordBatch::new_empty(schema.clone()),
            None => RecordBatch::new_empty(Arc::new(Schema::empty())),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let mut out = self.current_watermark.to_le_bytes().to_vec();
        let pending = match &self.pending {
            Some(batch) if batch.num_rows() > 0 => write_ipc(batch),
            _ => Vec::new(),
        };
        out.extend_from_slice(&(pending.len() as u32).to_le_bytes());
        out.extend_from_slice(&pending);
        out.extend_from_slice(&self.snapshot_emitted());
        out
    }

    /// The emitted keys as an IPC batch of just the key columns (their types taken from the scalars).
    fn snapshot_emitted(&self) -> Vec<u8> {
        if self.emitted.is_empty() {
            return Vec::new();
        }
        let keys: Vec<&GroupKey> = self.emitted.iter().collect();
        let arity = keys[0].len();
        let fields: Vec<Field> = (0..arity)
            .map(|j| Field::new(format!("key{j}"), keys[0][j].data_type(), true))
            .collect();
        let columns: Vec<ArrayRef> = (0..arity)
            .map(|j| scalars_to_array(keys.iter().map(|k| k[j].clone()).collect(), fields[j].data_type()))
            .collect();
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("dedup emitted"))
    }

    fn restore(partition_columns: Vec<usize>, rt_column: usize, bytes: &[u8]) -> Self {
        let mut dedup = KeepFirstDeduplicator::new(partition_columns, rt_column);
        if bytes.len() < 8 {
            return dedup;
        }
        dedup.current_watermark = i64::from_le_bytes(bytes[0..8].try_into().expect("watermark"));
        let pending_len = u32::from_le_bytes(bytes[8..12].try_into().expect("pending len")) as usize;
        for batch in read_ipc_if_present(&bytes[12..12 + pending_len]) {
            dedup.schema = Some(batch.schema());
            dedup.pending = Some(batch);
        }
        for batch in read_ipc_if_present(&bytes[12 + pending_len..]) {
            let key_arrays: Vec<&ArrayRef> = (0..batch.num_columns()).map(|i| batch.column(i)).collect();
            for row in 0..batch.num_rows() {
                dedup.emitted.insert(read_key(&key_arrays, row));
            }
        }
        dedup
    }
}

/// Eager (push→emit, no watermark buffering) deduplication keyed by a partition key. Serves three of
/// the four dedup variants — the watermark-buffered event-time keep-first lives in
/// {@link KeepFirstDeduplicator}:
///   * **rowtime keep-last** — Flink's `RowTimeDeduplicateFunction`: keep the **maximum**-rowtime row;
///     the first row emits `+I`, a later row (rowtime `>=` the stored one) emits `-U`(previous, gated
///     on `generate_update_before`)/`+U`(new), and a smaller-rowtime row is ignored.
///   * **proctime keep-last** — Flink's `ProcTimeDeduplicateKeepLastRowFunction`: the same, but in
///     arrival order, so every later row replaces (no rowtime read or comparison).
///   * **proctime keep-first** — Flink's `ProcTimeDeduplicateKeepFirstRowFunction`: the first row per
///     key emits `+I` and every later row is dropped; insert-only output (no `$row_kind$`).
/// Insert-only input. The stored full row per key lives as scalars and is rebuilt with
/// `scalars_to_array` on emit, like the changelog normalizer below.
pub(crate) struct KeepLastDeduplicator {
    partition_columns: Vec<usize>,
    rt_column: usize,
    generate_update_before: bool,
    /// Whether the order is a rowtime (read + compared) or proctime (arrival order; rt ignored).
    rowtime_ordered: bool,
    /// Keep-first (insert-only, first row wins) vs keep-last (retract changelog, latest row wins).
    keep_first: bool,
    schema: Option<SchemaRef>,
    /// arrow-row encoders (partition key, value-encoded full row), built once from the first batch.
    partition_converter: Option<RowConverter>,
    payload_converter: Option<RowConverter>,
    /// Per key: the stored row's rowtime (millis, 0 in proctime) and its full row as arrow-row bytes.
    /// The key is a `ByteKey`, so the per-row probe hashes the BORROWED encoded bytes and the steady
    /// state (key already stored) allocates nothing for the key. The payload is an `Arc<[u8]>`: a new
    /// row is copied once into state, emitting it (and retracting the previous row) just bumps the
    /// refcount — the `-U` moves the replaced payload out of the map, never re-copying it.
    rows: HashMap<ByteKey, (i64, Arc<[u8]>)>,
    memory: OperatorMemory,
}

/// Estimated footprint of one stored last-row entry (arrow-row key + payload + map entry).
pub(crate) fn dedup_entry_bytes(key: &[u8], payload: &[u8]) -> usize {
    key.len() + payload.len() + GROUP_ENTRY_OVERHEAD
}

impl KeepLastDeduplicator {
    pub(crate) fn new(
        partition_columns: Vec<usize>,
        rt_column: usize,
        generate_update_before: bool,
        rowtime_ordered: bool,
        keep_first: bool,
    ) -> Self {
        KeepLastDeduplicator {
            partition_columns,
            rt_column,
            generate_update_before,
            rowtime_ordered,
            keep_first,
            schema: None,
            partition_converter: None,
            payload_converter: None,
            rows: HashMap::default(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds this deduplicator's stored rows by the operator's managed-memory budget (negative =
    /// unaccounted), accounting any restored rows immediately.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize =
            self.rows.iter().map(|(key, (_, payload))| dedup_entry_bytes(&key.0, payload)).sum();
        self.memory.attach("deduplicate", budget_bytes, state)?;
        Ok(self)
    }

    /// Builds the partition-key and full-row arrow-row converters from a batch's column types, once.
    fn ensure_converters(&mut self, batch: &RecordBatch, arity: usize) {
        if self.payload_converter.is_some() {
            return;
        }
        self.payload_converter = Some(
            RowConverter::new(
                (0..arity).map(|i| SortField::new(batch.column(i).data_type().clone())).collect(),
            )
            .expect("dedup payload converter"),
        );
        self.partition_converter = Some(
            RowConverter::new(
                self.partition_columns
                    .iter()
                    .map(|&i| SortField::new(batch.column(i).data_type().clone()))
                    .collect(),
            )
            .expect("dedup partition converter"),
        );
    }

    pub(crate) fn push(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        self.ensure_converters(batch, arity);
        let partition_arrays: Vec<ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i).clone()).collect();
        let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
        let parts =
            self.partition_converter.as_ref().unwrap().convert_columns(&partition_arrays).expect("encode dedup key");
        let payloads =
            self.payload_converter.as_ref().unwrap().convert_columns(&data_arrays).expect("encode dedup payload");
        // The rowtime is read only for a rowtime order; proctime dedup uses arrival order.
        let rt = self.rowtime_ordered.then(|| rt_to_millis(batch.column(self.rt_column)));

        let keep_first = self.keep_first;
        let rowtime_ordered = self.rowtime_ordered;
        let generate_update_before = self.generate_update_before;
        let track = self.memory.tracking();
        let mut delta = 0isize;
        let rows = &mut self.rows;
        let mut out_rows: Vec<Arc<[u8]>> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        for row in 0..batch.num_rows() {
            // Borrowed probe: the key bytes are copied into the map only when a key first appears,
            // and a dropped/ignored row allocates nothing at all.
            let key = parts.row(row).data();
            // keep-first: the first row per key wins, later rows are dropped (insert-only).
            if keep_first {
                if rows.contains_key(key) {
                    continue;
                }
                let payload: Arc<[u8]> = Arc::from(payloads.row(row).data());
                if track {
                    delta += dedup_entry_bytes(key, &payload) as isize;
                }
                out_rows.push(payload.clone());
                out_kinds.push(0); // +I — first row for the key
                rows.insert(ByteKey::from(key), (0, payload));
                continue;
            }
            let rowtime = rt.as_ref().map_or(0, |rt| rt.value(row));
            match rows.get_mut(key) {
                None => {
                    let payload: Arc<[u8]> = Arc::from(payloads.row(row).data());
                    if track {
                        delta += dedup_entry_bytes(key, &payload) as isize;
                    }
                    out_rows.push(payload.clone());
                    out_kinds.push(0); // +I — first row for the key
                    rows.insert(ByteKey::from(key), (rowtime, payload));
                }
                // A rowtime order ignores an older (smaller-rowtime) row; proctime always replaces.
                Some((stored_rt, _)) if rowtime_ordered && rowtime < *stored_rt => {
                    continue;
                }
                Some(stored) => {
                    let payload: Arc<[u8]> = Arc::from(payloads.row(row).data());
                    if track {
                        // Same key: only the payload is replaced.
                        delta += payload.len() as isize - stored.1.len() as isize;
                    }
                    // The -U moves the replaced payload out of state — no copy.
                    let prev = std::mem::replace(stored, (rowtime, payload.clone()));
                    if generate_update_before {
                        out_rows.push(prev.1);
                        out_kinds.push(1); // -U the previous row
                    }
                    out_rows.push(payload);
                    out_kinds.push(2); // +U the new (later) row
                }
            }
        }
        self.memory.record(delta);
        self.memory.account()?;
        Ok(self.emit(out_rows, out_kinds))
    }

    fn emit(&self, out_rows: Vec<Arc<[u8]>>, out_kinds: Vec<i8>) -> RecordBatch {
        if out_rows.is_empty() {
            return RecordBatch::new_empty(Arc::new(Schema::empty()));
        }
        let schema = self.schema.as_ref().expect("schema set once a row was processed");
        let conv = self.payload_converter.as_ref().expect("converter set");
        // One vectorized row->columnar pass rebuilds every data column (cf. the per-cell scalar build).
        let parser = conv.parser();
        let mut columns: Vec<ArrayRef> =
            conv.convert_rows(out_rows.iter().map(|r| parser.parse(r))).expect("decode dedup payloads");
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        // Keep-first is insert-only (every emitted row is a +I), so it carries no $row_kind$ column;
        // keep-last emits a changelog and tags each row's kind.
        if !self.keep_first {
            fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
            columns.push(Arc::new(Int8Array::from(out_kinds)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build keep-last dedup batch")
    }

    /// Serializes the stored last-row-per-key set; the rowtime is re-derived from each row on restore.
    fn snapshot(&self) -> Vec<u8> {
        let Some(schema) = &self.schema else { return Vec::new() };
        let Some(conv) = &self.payload_converter else { return Vec::new() };
        if self.rows.is_empty() {
            return Vec::new();
        }
        let parser = conv.parser();
        let columns = conv
            .convert_rows(self.rows.values().map(|(_, row)| parser.parse(row)))
            .expect("decode dedup snapshot payloads");
        write_ipc(&RecordBatch::try_new(schema.clone(), columns).expect("keep-last snapshot"))
    }

    fn restore(
        partition_columns: Vec<usize>,
        rt_column: usize,
        generate_update_before: bool,
        rowtime_ordered: bool,
        keep_first: bool,
        bytes: &[u8],
    ) -> Self {
        let mut dedup = KeepLastDeduplicator::new(
            partition_columns,
            rt_column,
            generate_update_before,
            rowtime_ordered,
            keep_first,
        );
        for batch in read_ipc_if_present(bytes) {
            let arity = batch.num_columns();
            dedup.schema = Some(batch.schema());
            dedup.ensure_converters(&batch, arity);
            // The stored rowtime matters only to the rowtime keep-last comparison; proctime stores 0.
            let rt = rowtime_ordered.then(|| rt_to_millis(batch.column(dedup.rt_column)));
            let partition_arrays: Vec<ArrayRef> =
                dedup.partition_columns.iter().map(|&i| batch.column(i).clone()).collect();
            let data_arrays: Vec<ArrayRef> = (0..arity).map(|i| batch.column(i).clone()).collect();
            let parts =
                dedup.partition_converter.as_ref().unwrap().convert_columns(&partition_arrays).expect("encode key");
            let payloads =
                dedup.payload_converter.as_ref().unwrap().convert_columns(&data_arrays).expect("encode payload");
            let rows = &mut dedup.rows;
            for row in 0..batch.num_rows() {
                rows.insert(
                    ByteKey::from(parts.row(row).data()),
                    (rt.as_ref().map_or(0, |rt| rt.value(row)), Arc::from(payloads.row(row).data())),
                );
            }
        }
        dedup
    }
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_keepFirstDeduplicatorStateBytes, KeepFirstDeduplicator);

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_keepLastDeduplicatorStateBytes, KeepLastDeduplicator);

/// Creates a keep-first deduplicator over the given partition-key columns and rowtime column.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createKeepFirstDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    rt_column: jint,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let dedup = KeepFirstDeduplicator::new(partitions, rt_column as usize)
        .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, dedup)
}

/// Buffers an input batch (no output); each key's minimum-rowtime row is emitted later, on the
/// watermark that reaches its rowtime.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushKeepFirstDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        dedup.push(&batch)
    };
    if let Err(e) = result {
        throw_memory_limit(&mut env, &e.to_string());
    }
}

/// Exports each key's first (minimum-rowtime) row whose rowtime the watermark has reached.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushKeepFirstDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
    // The emitted-key set grows here, so even a flush can exceed the budget.
    match dedup.flush(watermark_millis) {
        Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Releases the deduplicator and its per-key state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeKeepFirstDeduplicator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<KeepFirstDeduplicator>(handle));
    }
}

/// Serializes the deduplicator's pending candidates, emitted keys, and watermark for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotKeepFirstDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
    env.byte_array_from_slice(&dedup.snapshot())
        .expect("failed to allocate dedup snapshot array")
        .into_raw()
}

/// Rebuilds a keep-first deduplicator from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreKeepFirstDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    rt_column: jint,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read dedup snapshot");
    let dedup = KeepFirstDeduplicator::restore(partitions, rt_column as usize, &bytes)
        .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, dedup)
}

/// Creates an eager deduplicator (rowtime/proctime keep-last, or proctime keep-first) and returns an
/// opaque handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createKeepLastDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    rt_column: jint,
    generate_update_before: jboolean,
    rowtime_ordered: jboolean,
    keep_first: jboolean,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let dedup = KeepLastDeduplicator::new(
        partitions,
        rt_column as usize,
        generate_update_before != 0,
        rowtime_ordered != 0,
        keep_first != 0,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, dedup)
}

/// Folds an input batch and returns the retract changelog it produces (emitted eagerly per row).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushKeepLastDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let dedup = unsafe { &mut *(handle as *mut KeepLastDeduplicator) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        dedup.push(&batch)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeKeepLastDeduplicator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<KeepLastDeduplicator>(handle));
    }
}

/// Serializes the keep-last deduplicator's per-key stored rows for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotKeepLastDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let dedup = unsafe { &mut *(handle as *mut KeepLastDeduplicator) };
    env.byte_array_from_slice(&dedup.snapshot())
        .expect("failed to allocate dedup snapshot array")
        .into_raw()
}

/// Rebuilds an eager deduplicator from a snapshot and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreKeepLastDeduplicator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    rt_column: jint,
    generate_update_before: jboolean,
    rowtime_ordered: jboolean,
    keep_first: jboolean,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read dedup snapshot");
    let dedup = KeepLastDeduplicator::restore(
        partitions,
        rt_column as usize,
        generate_update_before != 0,
        rowtime_ordered != 0,
        keep_first != 0,
        &bytes,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, dedup)
}
