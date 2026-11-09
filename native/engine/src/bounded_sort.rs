use crate::bounded_exec::{BoundedInput, ExecutionEvent};
use crate::bounded_task::{bounded_context, compact_copy, copyable, throw_error, BoundedTask};
use crate::*;
use datafusion::execution::config::SessionConfig;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::{limit::GlobalLimitExec, sorts::sort::SortExec, ExecutionPlan};

struct BoundedSorter;

impl BoundedSorter {
    fn try_new(
        schema: SchemaRef,
        columns: &[usize],
        flags: &[i32],
        context: Arc<TaskContext>,
    ) -> Result<BoundedTask, DataFusionError> {
        Self::try_new_limited(schema, columns, flags, 0, None, context)
    }

    fn try_new_limited(
        schema: SchemaRef,
        columns: &[usize],
        flags: &[i32],
        mut offset: usize,
        mut end: Option<usize>,
        context: Arc<TaskContext>,
    ) -> Result<BoundedTask, DataFusionError> {
        if end.is_some_and(|end| end < offset) {
            return Err(DataFusionError::Execution(
                "invalid bounded sort limit".into(),
            ));
        }
        // A zero-row limit must still consume its input, preserving upstream evaluation/failures.
        if end == Some(offset) {
            offset = offset
                .checked_add(1)
                .ok_or_else(|| DataFusionError::Execution("bounded sort limit overflow".into()))?;
            end = Some(offset);
        }
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
        let plan = Arc::new(
            SortExec::new(LexOrdering::new(ordering).unwrap(), input.execution_plan()?)
                .with_fetch(end),
        );
        let output: Arc<dyn ExecutionPlan> = if offset == 0 {
            plan.clone()
        } else {
            Arc::new(GlobalLimitExec::new(plan.clone(), offset, None))
        };
        BoundedTask::try_new(input, output, plan, context)
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
    jni_guard(env, move |env| {
        create_sorter(
            env,
            schema_address,
            columns,
            flags,
            directories,
            batch_rows,
            spill_reservation,
            task_name,
            0,
            -1,
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_supportsBoundedSorterLimits<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jboolean {
    jni_guard(env, move |_env| 1)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createBoundedLimitedSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema_address: jlong,
    columns: JIntArray<'local>,
    flags: JIntArray<'local>,
    directories: JObjectArray<'local>,
    batch_rows: jint,
    spill_reservation: jlong,
    task_name: JString<'local>,
    offset: jlong,
    end: jlong,
) -> jlong {
    jni_guard(env, move |env| {
        create_sorter(
            env,
            schema_address,
            columns,
            flags,
            directories,
            batch_rows,
            spill_reservation,
            task_name,
            offset,
            end,
        )
    })
}

fn create_sorter<'local>(
    env: &mut JNIEnv<'local>,
    schema_address: jlong,
    columns: JIntArray<'local>,
    flags: JIntArray<'local>,
    directories: JObjectArray<'local>,
    batch_rows: jint,
    spill_reservation: jlong,
    task_name: JString<'local>,
    offset: jlong,
    end: jlong,
) -> jlong {
    let schema = import_schema(schema_address);
    let columns = read_columns(env, &columns);
    let flags = read_i32_array(env, &flags);
    let directories = read_string_array(env, &directories);
    let task_name: String = env
        .get_string(&task_name)
        .expect("bounded task name")
        .into();
    let result = (|| {
        if directories.is_empty() || batch_rows <= 0 || spill_reservation <= 0 {
            return Err(DataFusionError::Execution(
                "bounded sort needs host spill directories and positive batch/reservation sizes"
                    .into(),
            ));
        }
        let context = bounded_context(
            directories,
            &task_name,
            batch_rows as usize,
            spill_reservation as usize,
        )?;
        let _runtime = runtime().enter();
        if offset < 0 || end < -1 || end >= 0 && end < offset {
            return Err(DataFusionError::Execution(
                "invalid bounded sort limit".into(),
            ));
        }
        BoundedSorter::try_new_limited(
            schema,
            &columns,
            &flags,
            offset as usize,
            (end >= 0).then_some(end as usize),
            context,
        )
    })();
    match result {
        Ok(sorter) => into_handle(sorter),
        Err(error) => {
            throw_error(env, error);
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use datafusion::physical_plan::SendableRecordBatchStream;

    #[test]
    fn bounded_topk_consumes_all_batches_before_applying_offset_and_fetch() {
        runtime().block_on(async {
            let expected = [None, None, Some(1_i64), Some(2), Some(4), Some(4)];
            for (offset, end) in [(0, 1), (2, 5), (5, 12), (9, 12), (0, 0), (2, 2)] {
                let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
                let context = Arc::new(
                    TaskContext::default()
                        .with_runtime(
                            RuntimeEnvBuilder::new()
                                .with_memory_pool(pool.clone())
                                .build_arc()
                                .unwrap(),
                        )
                        .with_session_config(SessionConfig::new().with_batch_size(2)),
                );
                let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)]));
                let mut sorter = BoundedSorter::try_new_limited(
                    schema.clone(),
                    &[0],
                    &[2],
                    offset,
                    Some(end),
                    context,
                )
                .unwrap();
                for values in [
                    vec![Some(4_i64), None],
                    vec![Some(2), Some(4)],
                    vec![None, Some(1)],
                ] {
                    sorter
                        .push(
                            RecordBatch::try_new(
                                schema.clone(),
                                vec![Arc::new(Int64Array::from(values))],
                            )
                            .unwrap(),
                        )
                        .unwrap();
                    assert!(matches!(
                        sorter.next_event().await.unwrap(),
                        Some(ExecutionEvent::InputNeeded(_))
                    ));
                }
                sorter.finish().unwrap();
                let mut actual = Vec::new();
                loop {
                    match sorter.next_event().await.unwrap() {
                        Some(ExecutionEvent::Output(batch)) => {
                            assert!(batch.num_rows() <= 2);
                            actual.extend(
                                batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .iter(),
                            );
                            drop(batch);
                            sorter.output_memory.free();
                        }
                        Some(ExecutionEvent::Finished) => break,
                        None => {}
                        _ => panic!("input demand after EOF"),
                    }
                }
                assert_eq!(
                    actual,
                    expected[offset.min(expected.len())..end.min(expected.len())]
                );
                drop(sorter);
                assert_eq!(pool.reserved(), 0);
            }
        });
    }

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
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
            let context = Arc::new(
                TaskContext::default().with_runtime(
                    RuntimeEnvBuilder::new()
                        .with_memory_pool(pool)
                        .build_arc()
                        .unwrap(),
                ),
            );
            let mut sorter = BoundedTask::try_new(input, plan.clone(), plan, context).unwrap();
            sorter.finish().unwrap();
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
