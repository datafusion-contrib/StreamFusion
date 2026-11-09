use crate::bounded_exec::{BoundedExecution, BoundedInput, ExecutionEvent};
use crate::*;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::{sorts::sort::SortExec, ExecutionPlan};

struct BoundedSorter {
    input: BoundedInput,
    execution: BoundedExecution,
    staged: Option<RecordBatch>,
    input_ended: bool,
    input_memory: MemoryReservation,
    output_memory: MemoryReservation,
    pool: Arc<dyn MemoryPool>,
    ingesting: bool,
    output_rows: usize,
    plan: Arc<dyn ExecutionPlan>,
}

impl BoundedSorter {
    fn try_new(
        schema: SchemaRef,
        columns: &[usize],
        flags: &[i32],
        context: Arc<TaskContext>,
    ) -> Result<Self, DataFusionError> {
        // Flink source nullability is a logical hint; actual NULL slots can still arrive.
        let schema = Arc::new(Schema::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone().with_nullable(true))
                .collect::<Vec<_>>(),
            schema.metadata().clone(),
        ));
        if columns.is_empty()
            || columns.len() != flags.len()
            || !schema
                .fields()
                .iter()
                .all(|field| copyable(field.data_type()))
        {
            return Err(DataFusionError::Execution(
                "invalid bounded sort keys".into(),
            ));
        }
        let mut ordering = Vec::with_capacity(columns.len());
        for (&index, &flags) in columns.iter().zip(flags) {
            let field = schema.fields().get(index).ok_or_else(|| {
                DataFusionError::Execution("bounded sort key is outside its input schema".into())
            })?;
            if !matches!(
                field.data_type(),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            ) || flags & !3 != 0
            {
                return Err(DataFusionError::Execution(
                    "unverified bounded sort key type or options".into(),
                ));
            }
            ordering.push(PhysicalSortExpr {
                expr: Arc::new(Column::new(field.name(), index)),
                options: SortOptions {
                    descending: flags & 1 != 0,
                    nulls_first: flags & 2 != 0,
                },
            });
        }
        let input = BoundedInput::new(schema);
        let plan = Arc::new(SortExec::new(
            LexOrdering::new(ordering).unwrap(),
            input.execution_plan()?,
        ));
        let pool = context.runtime_env().memory_pool.clone();
        let output_rows = context.session_config().batch_size();
        let input_memory = MemoryConsumer::new("BoundedArrowInput").register(&pool);
        let output_memory = MemoryConsumer::new("BoundedArrowOutput").register(&pool);
        let execution = BoundedExecution::try_new(plan.clone(), vec![input.clone()], context)?;
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

    fn push(&mut self, batch: RecordBatch) -> Result<(), DataFusionError> {
        if self.input_ended || self.staged.is_some() || self.ingesting {
            return Err(DataFusionError::Execution(
                "bounded sorter input is closed or backpressured".into(),
            ));
        }
        self.staged = Some(batch.with_schema(self.input.schema().clone())?);
        Ok(())
    }

    fn finish(&mut self) -> Result<(), DataFusionError> {
        self.input_ended = true;
        if self.staged.is_none() {
            self.input.finish()?;
        }
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<ExecutionEvent>, DataFusionError> {
        if self.output_memory.size() != 0 {
            return Err(DataFusionError::Execution(
                "release the exported sort output before polling again".into(),
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

fn copyable(data_type: &DataType) -> bool {
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

fn compact_copy(
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

fn throw_error(env: &mut JNIEnv, error: DataFusionError) {
    if matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)) {
        throw_memory_limit(env, &error.to_string());
    } else {
        let _ = env.throw_new("tech/streamfusion/NativeException", error.to_string());
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createBoundedSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema_address: jlong,
    columns: JIntArray<'local>,
    flags: JIntArray<'local>,
    directories: JObjectArray<'local>,
    batch_rows: jint,
    spill_reservation: jlong,
    task_name: JString<'local>,
) -> jlong {
    jni_guard(env, move |mut env| {
        let schema = import_schema(schema_address);
        let columns = read_columns(&env, &columns);
        let flags = read_i32_array(&env, &flags);
        let directories = read_string_array(&mut env, &directories);
        let task_name: String = env
            .get_string(&task_name)
            .expect("bounded task name")
            .into();
        let result = (|| {
            if directories.is_empty() || batch_rows <= 0 || spill_reservation <= 0 {
                return Err(DataFusionError::Execution("bounded sort needs host spill directories and positive batch/reservation sizes".into()));
            }
            let pool = owned_task_memory_pool(&task_name)?;
            let runtime_env = RuntimeEnvBuilder::new()
                .with_memory_pool(pool)
                .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(
                    DiskManagerMode::Directories(directories.into_iter().map(Into::into).collect()),
                ))
                .build_arc()?;
            let context = Arc::new(
                TaskContext::default()
                    .with_runtime(runtime_env)
                    .with_session_config(
                        SessionConfig::new()
                            .with_batch_size(batch_rows as usize)
                            .with_sort_spill_reservation_bytes(spill_reservation as usize),
                    ),
            );
            let _runtime = runtime().enter();
            BoundedSorter::try_new(schema, &columns, &flags, context)
        })();
        match result {
            Ok(sorter) => into_handle(sorter),
            Err(error) => {
                throw_error(&mut env, error);
                0
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushBoundedSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array_address: jlong,
) {
    jni_guard(env, move |mut env| {
        let sorter = unsafe { &mut *(handle as *mut BoundedSorter) };
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
        let sorter = unsafe { &mut *(handle as *mut BoundedSorter) };
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
        let sorter = unsafe { &mut *(handle as *mut BoundedSorter) };
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
        drop(from_handle::<BoundedSorter>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_releaseBoundedSorterOutput<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(env, move |_env| {
        let sorter = unsafe { &mut *(handle as *mut BoundedSorter) };
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
        let sorter = unsafe { &*(handle as *const BoundedSorter) };
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
        let sorter = unsafe { &mut *(handle as *mut BoundedSorter) };
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
                        for key in keys.iter().flatten() {
                            checksum = checksum.wrapping_add(key);
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use datafusion::physical_plan::SendableRecordBatchStream;

    #[test]
    fn compact_output_owns_only_the_selected_rows() {
        let source: ArrayRef = Arc::new(Int64Array::from_iter_values(0..131072));
        let source_weak = Arc::downgrade(&source);
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![source]).unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
        let reservation = MemoryConsumer::new("copy-test").register(&pool);
        let copied = compact_copy(&batch.slice(120000, 3), &reservation).unwrap();
        drop(batch);
        assert!(source_weak.upgrade().is_none());
        let values = copied
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values().as_ref(), &[120000, 120001, 120002]);
        assert!(copied.get_array_memory_size() < 1024);
        drop(copied);
        drop(reservation);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn accepting_input_releases_the_borrowed_buffers_and_reuses_one_workspace() {
        runtime().block_on(async {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
            let context = Arc::new(
                TaskContext::default()
                    .with_runtime(
                        RuntimeEnvBuilder::new()
                            .with_memory_pool(pool.clone())
                            .build_arc()
                            .unwrap(),
                    )
                    .with_session_config(
                        SessionConfig::new().with_sort_spill_reservation_bytes(32768),
                    ),
            );
            let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
            let mut sorter = BoundedSorter::try_new(schema.clone(), &[0], &[0], context).unwrap();
            let mut workspace = 0;
            for value in [3, 1, 2] {
                let source: ArrayRef = Arc::new(Int64Array::from(vec![value]));
                let borrowed = Arc::downgrade(&source);
                sorter
                    .push(RecordBatch::try_new(schema.clone(), vec![source]).unwrap())
                    .unwrap();
                assert!(matches!(
                    sorter.next_event().await.unwrap(),
                    Some(ExecutionEvent::InputNeeded(_))
                ));
                assert!(borrowed.upgrade().is_none());
                if workspace == 0 {
                    workspace = sorter.input_memory.size();
                }
                assert_eq!(sorter.input_memory.size(), workspace);
            }
            sorter.finish().unwrap();
            let mut actual = Vec::new();
            loop {
                match sorter.next_event().await.unwrap() {
                    Some(ExecutionEvent::Output(batch)) => {
                        actual.extend(
                            batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .values()
                                .iter()
                                .copied(),
                        );
                        assert!(sorter.output_memory.size() > 0);
                        assert!(
                            sorter.next_event().await.is_err(),
                            "unreleased output must backpressure"
                        );
                        drop(batch);
                        sorter.output_memory.free();
                    }
                    Some(ExecutionEvent::Finished) => break,
                    None => {}
                    _ => panic!("input demand after EOF"),
                }
            }
            assert_eq!(actual, vec![1, 2, 3]);
            assert_eq!(sorter.input_memory.size(), 0);
            assert_eq!(pool.reserved(), 0);
        });
    }

    #[test]
    fn compact_copy_preserves_nullable_sliced_strings_and_binary_payload() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("prefix"),
            None,
            Some("wide-λ-data"),
            Some("last"),
        ]));
        let bytes: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&b"prefix"[..]),
            Some(&b"a\0b"[..]),
            None,
            Some(&b"last"[..]),
        ]));
        let source = RecordBatch::try_from_iter(vec![("text", strings), ("bytes", bytes)])
            .unwrap()
            .slice(1, 2);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
        let memory = MemoryConsumer::new("copy-test").register(&pool);
        let copy = compact_copy(&source, &memory).unwrap();
        assert_eq!(source, copy);
        assert_ne!(
            source.column(0).to_data().buffers()[1].as_ptr(),
            copy.column(0).to_data().buffers()[1].as_ptr()
        );
    }

    #[derive(Debug)]
    struct DelayedInput(SchemaRef);

    impl PartitionStream for DelayedInput {
        fn schema(&self) -> &SchemaRef {
            &self.0
        }

        fn execute(&self, _context: Arc<TaskContext>) -> SendableRecordBatchStream {
            let schema = self.0.clone();
            Box::pin(RecordBatchStreamAdapter::new(
                schema.clone(),
                futures::stream::once(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
                    Ok(
                        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))])
                            .unwrap(),
                    )
                }),
            ))
        }
    }

    #[test]
    fn native_io_yields_to_the_host_without_losing_the_pending_stream() {
        runtime().block_on(async {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]));
            let input = BoundedInput::new(schema.clone());
            let plan = Arc::new(
                StreamingTableExec::try_new(
                    schema.clone(),
                    vec![Arc::new(DelayedInput(schema))],
                    None,
                    [],
                    false,
                    None,
                )
                .unwrap(),
            );
            let execution = BoundedExecution::try_new(
                plan.clone(),
                vec![input.clone()],
                Arc::new(TaskContext::default()),
            )
            .unwrap();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
            let mut sorter = BoundedSorter {
                input,
                execution,
                staged: None,
                input_ended: false,
                input_memory: MemoryConsumer::new("input-test").register(&pool),
                output_memory: MemoryConsumer::new("output-test").register(&pool),
                pool,
                ingesting: false,
                output_rows: 1024,
                plan,
            };
            assert!(sorter.next_event().await.unwrap().is_none());
            let mut produced = false;
            for _ in 0..20 {
                if let Some(ExecutionEvent::Output(batch)) = sorter.next_event().await.unwrap() {
                    assert_eq!(batch.num_rows(), 1);
                    drop(batch);
                    sorter.output_memory.free();
                    produced = true;
                    break;
                }
            }
            assert!(produced);
            assert!(matches!(
                sorter.next_event().await.unwrap(),
                Some(ExecutionEvent::Finished)
            ));
        });
    }
}
