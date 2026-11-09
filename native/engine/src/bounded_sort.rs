use crate::bounded_exec::{BoundedExecution, BoundedInput, ExecutionEvent};
use crate::*;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;

struct BoundedSorter {
    input: BoundedInput,
    execution: BoundedExecution,
}

impl BoundedSorter {
    fn try_new(
        schema: SchemaRef,
        columns: &[usize],
        flags: &[i32],
        context: Arc<TaskContext>,
    ) -> Result<Self, DataFusionError> {
        if columns.is_empty() || columns.len() != flags.len() {
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
        let execution = BoundedExecution::try_new(plan, vec![input.clone()], context)?;
        Ok(Self { input, execution })
    }

    async fn next_event(&mut self) -> Result<Option<ExecutionEvent>, DataFusionError> {
        match tokio::time::timeout(
            std::time::Duration::from_millis(10),
            self.execution.next_event(),
        )
        .await
        {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None),
        }
    }
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
        // Imported buffers retain their C Data ownership until DataFusion consumes or spills them.
        let batch = import_record_batch_with_schema(array_address, sorter.input.schema());
        if let Err(error) = sorter.input.push(batch) {
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
        if let Err(error) = sorter.input.finish() {
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
                plan,
                vec![input.clone()],
                Arc::new(TaskContext::default()),
            )
            .unwrap();
            let mut sorter = BoundedSorter { input, execution };
            assert!(sorter.next_event().await.unwrap().is_none());
            let mut produced = false;
            for _ in 0..20 {
                if let Some(ExecutionEvent::Output(batch)) = sorter.next_event().await.unwrap() {
                    assert_eq!(batch.num_rows(), 1);
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
