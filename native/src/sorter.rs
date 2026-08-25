use crate::*;

/// Event-time sort (Flink's `RowTimeSortOperator`): buffers input rows and, on a watermark, emits
/// the rows whose rowtime is at or before it in ascending rowtime order, keeping the rest. Insert-
/// only — the watermark guarantees no earlier-rowtime row can still arrive, so the emitted order is
/// final. Ties at the same rowtime keep arrival order (a stable sort), matching the host. There is no
/// key (a single distribution gathers the stream), so this is the columnar analog of the host's
/// single-input event-time sort.
pub(crate) struct TemporalSorter {
    rt_column: usize,
    buffered: Vec<RecordBatch>,
    input_schema: Option<SchemaRef>,
    /// Persistent-state mode: the buffered rows live in the persistent store — the in-memory
    /// buffer stays empty, and a watermark firing is a range read over the stored rowtimes.
    #[cfg(feature = "rocksdb-state")]
    store: Option<crate::state::RocksTemporalSortBuffer>,
    pub(crate) memory: OperatorMemory,
}

impl TemporalSorter {
    pub(crate) fn new(rt_column: usize) -> Self {
        TemporalSorter {
            rt_column,
            buffered: Vec::new(),
            input_schema: None,
            #[cfg(feature = "rocksdb-state")]
            store: None,
            memory: OperatorMemory::unaccounted(),
        }
    }

    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_store(mut self, store: crate::state::RocksTemporalSortBuffer) -> Self {
        self.store = Some(store);
        self
    }

    /// Attaches the task off-heap budget for a backend that starts with nothing resident.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory.attach("temporal-sort", budget_bytes, 0)?;
        Ok(self)
    }

    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn store_mut(&mut self) -> &mut crate::state::RocksTemporalSortBuffer {
        self.store.as_mut().expect("temporal-sort rocksdb store")
    }

    /// The persistent buffer serialized as the memory snapshot's own plain-IPC blob (empty bytes
    /// when nothing is buffered), for backend-independent canonical savepoints.
    #[cfg(feature = "rocksdb-state")]
    pub(crate) fn store_snapshot(&self) -> Result<Vec<u8>, DataFusionError> {
        let store = self.store.as_ref().expect("temporal-sort rocksdb store");
        Ok(store
            .scan_buffered()?
            .map(|batch| write_ipc(&batch))
            .unwrap_or_default())
    }

    /// Bounds the sort buffer by the operator's task off-heap budget (negative = unaccounted),
    /// accounting any restored buffer immediately.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state = buffered_batches_bytes(&self.buffered);
        self.memory.attach("temporal-sort", budget_bytes, state)?;
        Ok(self)
    }

    pub(crate) fn push(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        self.input_schema = Some(batch.schema());
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            let rowtimes = rt_to_millis(batch.column(self.rt_column));
            return self.store_mut().push(&batch, &rowtimes);
        }
        if self.memory.tracking() {
            self.memory.record(batch.get_array_memory_size() as isize);
        }
        self.buffered.push(batch);
        self.memory.account()
    }

    /// Emits the rows the watermark has completed, sorted ascending by rowtime, and keeps the rest
    /// buffered. Returns an empty batch when nothing is complete. Fallible only in
    /// persistent-state mode (the firing reads the committed table).
    pub(crate) fn flush(&mut self, watermark: i64) -> Result<RecordBatch, DataFusionError> {
        #[cfg(feature = "rocksdb-state")]
        if self.store.is_some() {
            let store = self.store.as_mut().expect("temporal-sort rocksdb store");
            let schema = store.schema();
            return Ok(store
                .take_complete(watermark)?
                .unwrap_or_else(|| RecordBatch::new_empty(schema)));
        }
        Ok(self.flush_memory(watermark))
    }

    fn flush_memory(&mut self, watermark: i64) -> RecordBatch {
        let schema = match &self.input_schema {
            Some(schema) => schema.clone(),
            None => return RecordBatch::new_empty(Arc::new(Schema::empty())),
        };
        let all = concat_batches(&schema, &self.buffered).expect("failed to concat sort buffer");
        let rt_millis = rt_to_millis(all.column(self.rt_column));
        let complete_mask: BooleanArray = rt_millis
            .iter()
            .map(|v| Some(v.unwrap() <= watermark))
            .collect();
        let complete =
            filter_record_batch(&all, &complete_mask).expect("failed to filter complete");
        let pending_mask = arrow::compute::not(&complete_mask).expect("failed to negate mask");
        let pending = filter_record_batch(&all, &pending_mask).expect("failed to filter pending");
        self.buffered = if pending.num_rows() > 0 {
            vec![pending]
        } else {
            Vec::new()
        };
        if self.memory.tracking() {
            self.memory.set(buffered_batches_bytes(&self.buffered));
            self.memory.account_shrink();
        }
        if complete.num_rows() == 0 {
            return RecordBatch::new_empty(schema);
        }
        // Stable ascending sort by rowtime: Rust's sort_by_key is stable, so equal-rowtime rows keep
        // their arrival order, as the host's sort does.
        let rt_complete = rt_to_millis(complete.column(self.rt_column));
        let values = rt_complete.values();
        let mut order: Vec<u32> = (0..complete.num_rows() as u32).collect();
        order.sort_by_key(|&i| values[i as usize]);
        let indices = UInt32Array::from(order);
        let columns: Vec<ArrayRef> = complete
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).expect("failed to sort column"))
            .collect();
        RecordBatch::try_new(complete.schema(), columns).expect("failed to build sorted batch")
    }

    fn snapshot(&mut self) -> Vec<u8> {
        match (&self.input_schema, self.buffered.is_empty()) {
            (Some(schema), false) => {
                let all = concat_batches(schema, &self.buffered).expect("concat sort buffer");
                let mut bytes = Vec::new();
                let mut writer =
                    arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &all.schema())
                        .expect("sort buffer writer");
                writer.write(&all).expect("write sort buffer");
                writer.finish().expect("finish sort buffer");
                drop(writer);
                bytes
            }
            _ => Vec::new(),
        }
    }

    pub(crate) fn restore(rt_column: usize, bytes: &[u8]) -> Self {
        let mut sorter = TemporalSorter::new(rt_column);
        if !bytes.is_empty() {
            let reader =
                arrow::ipc::reader::StreamReader::try_new(bytes, None).expect("sort buffer reader");
            for batch in reader {
                let batch = batch.expect("read sort buffer");
                sorter.input_schema = Some(batch.schema());
                sorter.buffered.push(batch);
            }
        }
        sorter
    }
}

/// Reads a timestamp column as epoch millis, regardless of its stored unit.
pub(crate) fn rt_to_millis(array: &ArrayRef) -> Int64Array {
    use arrow::datatypes::TimeUnit;
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let ts = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts ns");
            ts.iter().map(|v| v.map(|x| x / 1_000_000)).collect()
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let ts = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("ts us");
            ts.iter().map(|v| v.map(|x| x / 1_000)).collect()
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let ts = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("ts ms");
            ts.iter().map(|v| v.map(|x| x)).collect()
        }
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64")
            .clone(),
        other => panic!("unsupported rowtime column type: {other:?}"),
    }
}

state_bytes_getter!(
    Java_tech_streamfusion_Native_temporalSorterStateBytes,
    TemporalSorter
);

/// Creates an event-time sorter over the given rowtime column and returns an opaque handle.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    rt_column: jint,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let sorter =
            TemporalSorter::new(rt_column as usize).with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, sorter)
    })
}

/// Buffers an input batch (no output); the rows are emitted later, in rowtime order, as watermarks
/// complete them.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let sorter = unsafe { &mut *(handle as *mut TemporalSorter) };
        // The pushed batch is retained in the sort buffer (not dropped), so no JVM release upcall runs
        // between a failed account and the throw (see updateTumblingAggregator).
        let result = sorter.push(import_record_batch(in_array_address, in_schema_address));
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Exports the rows the watermark has completed, sorted ascending by rowtime.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let sorter = unsafe { &mut *(handle as *mut TemporalSorter) };
        match sorter.flush(watermark_millis) {
            Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Releases the event-time sorter and its buffered rows.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<TemporalSorter>(handle));
    })
}

/// Serializes the sorter's buffered rows for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    crate::bridge::jni_guard(env, move |env| {
        let sorter = unsafe { &mut *(handle as *mut TemporalSorter) };
        env.byte_array_from_slice(&sorter.snapshot())
            .expect("failed to allocate sort snapshot array")
            .into_raw()
    })
}

/// Rebuilds an event-time sorter from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    rt_column: jint,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let bytes = env
            .convert_byte_array(&snapshot)
            .expect("failed to read sort snapshot");
        let sorter = TemporalSorter::restore(rt_column as usize, &bytes)
            .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, sorter)
    })
}
