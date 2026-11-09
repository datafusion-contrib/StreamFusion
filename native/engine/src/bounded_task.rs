use crate::bounded_exec::{BoundedExecution, BoundedInput, ExecutionEvent};
use crate::*;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::physical_plan::ExecutionPlan;

pub(crate) struct BoundedTask {
    input: BoundedInput,
    execution: BoundedExecution,
    staged: Option<RecordBatch>,
    input_ended: bool,
    pub(crate) input_memory: MemoryReservation,
    pub(crate) output_memory: MemoryReservation,
    pool: Arc<dyn MemoryPool>,
    ingesting: bool,
    output_rows: usize,
    plan: Arc<dyn ExecutionPlan>,
}

impl BoundedTask {
    pub(crate) fn try_new(
        input: BoundedInput,
        output: Arc<dyn ExecutionPlan>,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<Self, DataFusionError> {
        let pool = context.runtime_env().memory_pool.clone();
        let output_rows = context.session_config().batch_size();
        let input_memory = MemoryConsumer::new("BoundedArrowInput").register(&pool);
        let output_memory = MemoryConsumer::new("BoundedArrowOutput").register(&pool);
        let execution = BoundedExecution::try_new(output, vec![input.clone()], context)?;
        Ok(Self {
            input,
            execution,
            staged: None,
            input_ended: false,
            input_memory,
            output_memory,
            pool,
            ingesting: false,
            output_rows,
            plan,
        })
    }

    pub(crate) fn push(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        if self.input_ended || self.staged.is_some() || self.ingesting {
            return Err(DataFusionError::Execution(
                "bounded task input is closed or backpressured".into(),
            ));
        }
        self.staged = Some(batch.with_schema(self.input.schema().clone())?);
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> Result<(), DataFusionError> {
        self.input_ended = true;
        if self.staged.is_none() {
            self.input.finish()?;
        }
        Ok(())
    }

    pub(crate) async fn next_event(&mut self) -> Result<Option<ExecutionEvent>, DataFusionError> {
        if self.output_memory.size() != 0 {
            return Err(DataFusionError::Execution(
                "release the exported bounded output before polling again".into(),
            ));
        }
        if let Some(batch) = self.staged.take() {
            if let Some(pool) = self.pool.downcast_ref::<FlinkTaskOffHeapPool>() {
                let output_frame = batch.slice(0, batch.num_rows().min(self.output_rows));
                let mut headroom = copy_budget(&output_frame)?.saturating_mul(2).max(64 * 1024);
                if !self.input_ended {
                    headroom = headroom.max(copy_budget(&batch)?);
                }
                pool.preserve_spill_headroom(headroom);
            }
            let copied = compact_copy(&batch, &self.input_memory);
            // Java has already closed its input view. Release the borrowed C Data buffers before
            // DataFusion retains the separate native copy under its own working reservation.
            drop(batch);
            match copied.and_then(|batch| self.input.push(batch)) {
                Ok(()) => {}
                Err(error) => {
                    self.input_memory.free();
                    return Err(error);
                }
            }
            if self.input_ended {
                self.input.finish()?;
            }
            self.ingesting = true;
        }
        let event = match tokio::time::timeout(
            std::time::Duration::from_millis(10),
            self.execution.next_event(),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => return Ok(None),
        };
        self.ingesting = false;
        // Reuse one copy workspace while reading input. At EOF the sorter owns its retained
        // inputs and final output no longer needs the input workspace.
        if self.input_ended || event.is_err() {
            self.input_memory.free();
        }
        match event? {
            ExecutionEvent::Output(batch) => {
                let copied = compact_copy(&batch, &self.output_memory);
                drop(batch);
                match copied {
                    Ok(batch) => Ok(Some(ExecutionEvent::Output(batch))),
                    Err(error) => {
                        self.output_memory.free();
                        Err(error)
                    }
                }
            }
            event => Ok(Some(event)),
        }
    }
}

pub(crate) fn copyable(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => true,
        DataType::Struct(fields) => fields.iter().all(|field| copyable(field.data_type())),
        _ => false,
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_supportsBoundedSorterSchema<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema_address: jlong,
) -> jboolean {
    jni_guard(env, move |_env| {
        let schema = import_schema(schema_address);
        u8::from(
            schema
                .fields()
                .iter()
                .all(|field| copyable(field.data_type())),
        )
    })
}

/// Compact take owns every admitted payload buffer, including sliced output batches. Copying
/// buffers verbatim would retain/copy the full sort run for each small output slice.
fn copy_budget(batch: &RecordBatch) -> Result<usize, DataFusionError> {
    let mut budget = batch
        .num_rows()
        .checked_mul(4)
        .and_then(|size| size.checked_add(64))
        .ok_or_else(|| DataFusionError::Execution("bounded Arrow copy size overflow".into()))?;
    for column in batch.columns() {
        let data = column.to_data();
        let size = data.get_slice_memory_size()?;
        let overhead = data
            .get_array_memory_size()
            .saturating_sub(data.get_buffer_memory_size());
        budget = budget
            .checked_add(size.saturating_mul(2))
            .and_then(|size| size.checked_add(overhead + 128))
            .ok_or_else(|| DataFusionError::Execution("bounded Arrow copy size overflow".into()))?;
    }
    Ok(budget)
}

pub(crate) fn compact_copy(
    batch: &RecordBatch,
    reservation: &MemoryReservation,
) -> Result<RecordBatch, DataFusionError> {
    let budget = copy_budget(batch)?;
    if reservation.size() < budget {
        reservation.try_resize(budget)?;
    }
    let indices = UInt32Array::from_iter_values(
        0..u32::try_from(batch.num_rows()).map_err(|_| {
            DataFusionError::Execution("bounded Arrow batch exceeds u32 row count".into())
        })?,
    );
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    let copy = RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?;
    if copy.get_array_memory_size() > budget {
        return Err(DataFusionError::Execution(
            "bounded Arrow copy exceeded its reserved footprint".into(),
        ));
    }
    Ok(copy)
}

pub(crate) fn throw_error(env: &mut JNIEnv, error: DataFusionError) {
    if matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)) {
        throw_memory_limit(env, &error.to_string());
    } else {
        let _ = env.throw_new("tech/streamfusion/NativeException", error.to_string());
    }
}

pub(crate) fn bounded_context(
    directories: Vec<String>,
    task_name: &str,
    batch_rows: usize,
    spill_reservation: usize,
) -> Result<Arc<TaskContext>, DataFusionError> {
    let pool = owned_task_memory_pool(task_name)?;
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(
            DiskManagerMode::Directories(directories.into_iter().map(Into::into).collect()),
        ))
        .build_arc()?;
    Ok(Arc::new(
        TaskContext::default()
            .with_runtime(runtime_env)
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(batch_rows)
                    .with_sort_spill_reservation_bytes(spill_reservation),
            ),
    ))
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushBoundedSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array_address: jlong,
) {
    jni_guard(env, move |mut env| {
        let sorter = unsafe { &mut *(handle as *mut BoundedTask) };
        // Stage borrowed input; the next poll copies it after the Java view has been closed.
        let batch = import_record_batch_with_schema(array_address, sorter.input.schema());
        if let Err(error) = sorter.push(batch) {
            throw_error(&mut env, error);
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_finishBoundedSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(env, move |mut env| {
        let sorter = unsafe { &mut *(handle as *mut BoundedTask) };
        if let Err(error) = sorter.finish() {
            throw_error(&mut env, error);
        }
    })
}

/// 0 requests input, 1 exports one batch, 2 yields during native I/O, -1 is complete. Returning
/// during I/O lets the host observe task interruption before polling the same execution again.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_nextBoundedSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array_address: jlong,
    schema_address: jlong,
) -> jint {
    jni_guard(env, move |mut env| {
        let sorter = unsafe { &mut *(handle as *mut BoundedTask) };
        match runtime().block_on(sorter.next_event()) {
            Ok(Some(ExecutionEvent::InputNeeded(_))) => 0,
            Ok(Some(ExecutionEvent::Output(batch))) => {
                export_record_batch(batch, array_address, schema_address);
                1
            }
            Ok(Some(ExecutionEvent::Finished)) => -1,
            Ok(None) => 2,
            Err(error) => {
                throw_error(&mut env, error);
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeBoundedSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(env, move |_env| unsafe {
        drop(from_handle::<BoundedTask>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_releaseBoundedSorterOutput<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(env, move |_env| {
        let sorter = unsafe { &mut *(handle as *mut BoundedTask) };
        sorter.output_memory.free();
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_boundedSorterExecutionStats<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jlongArray {
    jni_guard(env, move |env| {
        let sorter = unsafe { &*(handle as *const BoundedTask) };
        let metrics = sorter.plan.metrics();
        let spills = metrics
            .as_ref()
            .and_then(|metrics| metrics.spill_count())
            .unwrap_or(0);
        let spilled_bytes = metrics
            .as_ref()
            .and_then(|metrics| metrics.spilled_bytes())
            .unwrap_or(0);
        let stats = env.new_long_array(3).expect("bounded sort statistics");
        env.set_long_array_region(
            &stats,
            0,
            &[
                spills as i64,
                spilled_bytes as i64,
                sorter.pool.reserved() as i64,
            ],
        )
        .expect("store bounded sort statistics");
        stats.into_raw()
    })
}

/// Small-functionality timing starts after input import and ends before any output export.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_consumeBoundedSorterTimed<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jlongArray {
    jni_guard(env, move |mut env| {
        let sorter = unsafe { &mut *(handle as *mut BoundedTask) };
        let start = std::time::Instant::now();
        let result = runtime().block_on(async {
            let mut rows = 0_i64;
            let mut checksum = 0_i64;
            loop {
                match sorter.next_event().await? {
                    Some(ExecutionEvent::Output(batch)) => {
                        let keys = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Execution(
                                    "bounded sort benchmark requires an Int64 first column".into(),
                                )
                            })?;
                        for key in keys.iter() {
                            checksum = checksum
                                .wrapping_mul(31)
                                .wrapping_add(key.unwrap_or(i64::MIN));
                        }
                        rows += batch.num_rows() as i64;
                        std::hint::black_box(&batch);
                        drop(batch);
                        sorter.output_memory.free();
                    }
                    Some(ExecutionEvent::Finished) => {
                        return Ok::<_, DataFusionError>((rows, checksum))
                    }
                    Some(ExecutionEvent::InputNeeded(_)) => {
                        return Err(DataFusionError::Execution(
                            "finish the benchmark input before timing".into(),
                        ))
                    }
                    None => {}
                }
            }
        });
        let nanos = start.elapsed().as_nanos() as i64;
        match result {
            Ok((rows, checksum)) => {
                let values = env.new_long_array(3).expect("bounded sort timing result");
                env.set_long_array_region(&values, 0, &[nanos, rows, checksum])
                    .expect("store bounded sort timing");
                values.into_raw()
            }
            Err(error) => {
                throw_error(&mut env, error);
                std::ptr::null_mut()
            }
        }
    })
}
