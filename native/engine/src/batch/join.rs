use crate::batch::exec::BoundedInput;
use crate::batch::task::{bounded_context, copyable, throw_error, BoundedTask};
use crate::*;
use datafusion::common::{JoinSide, JoinType, NullEquality};
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

pub(crate) struct JoinPlan {
    inputs: Vec<BoundedInput>,
    output: Arc<dyn ExecutionPlan>,
    join: Arc<dyn ExecutionPlan>,
}

impl JoinPlan {
    pub(crate) fn try_new(
        left: SchemaRef,
        right: SchemaRef,
        declared: SchemaRef,
        left_keys: &[usize],
        right_keys: &[usize],
        kind: i32,
        build_left: bool,
        null_equals: bool,
    ) -> Result<Self, DataFusionError> {
        Self::try_new_plan(
            left,
            right,
            declared,
            left_keys,
            right_keys,
            kind,
            build_left,
            null_equals,
            false,
            false,
            None,
        )
    }

    fn try_new_plan(
        left: SchemaRef,
        right: SchemaRef,
        declared: SchemaRef,
        left_keys: &[usize],
        right_keys: &[usize],
        kind: i32,
        build_left: bool,
        null_equals: bool,
        nested_loop: bool,
        merge_join: bool,
        predicate: Option<&crate::calc::FilterExpression>,
    ) -> Result<Self, DataFusionError> {
        let join_type = match kind {
            0 => JoinType::Inner,
            1 => JoinType::Left,
            2 => JoinType::Right,
            3 => JoinType::Full,
            4 => JoinType::LeftSemi,
            5 => JoinType::LeftAnti,
            _ => return datafusion::common::exec_err!("unverified bounded join kind"),
        };
        if !nested_loop && left_keys.is_empty()
            || left_keys.len() != right_keys.len()
            || !left
                .fields()
                .iter()
                .chain(right.fields())
                .all(|f| copyable(f.data_type()))
        {
            return datafusion::common::exec_err!("invalid bounded join specification");
        }
        let nullable = |schema: &Schema| {
            Arc::new(Schema::new_with_metadata(
                schema
                    .fields()
                    .iter()
                    .map(|f| f.as_ref().clone().with_nullable(true))
                    .collect::<Vec<_>>(),
                schema.metadata().clone(),
            ))
        };
        let left = nullable(&left);
        let right = nullable(&right);
        let mut keys = Vec::new();
        for (&l, &r) in left_keys.iter().zip(right_keys) {
            let (Some(lf), Some(rf)) = (left.fields().get(l), right.fields().get(r)) else {
                return datafusion::common::exec_err!("bounded join key outside schema");
            };
            if lf.data_type() != rf.data_type()
                || !matches!(
                    lf.data_type(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Boolean
                        | DataType::Date32
                        | DataType::Utf8
                        | DataType::Decimal128(1..=38, _)
                )
            {
                return datafusion::common::exec_err!("unverified bounded join key type");
            }
            let l = Arc::new(Column::new(lf.name(), l)) as Arc<dyn PhysicalExpr>;
            let r = Arc::new(Column::new(rf.name(), r)) as Arc<dyn PhysicalExpr>;
            keys.push(if build_left { (l, r) } else { (r, l) });
        }
        let inputs = vec![
            BoundedInput::new(left.clone()),
            BoundedInput::new(right.clone()),
        ];
        let (build, probe, join_type) = if build_left {
            (
                inputs[0].execution_plan()?,
                inputs[1].execution_plan()?,
                join_type,
            )
        } else {
            (
                inputs[1].execution_plan()?,
                inputs[0].execution_plan()?,
                join_type.swap(),
            )
        };
        let filter = if let Some(predicate) = predicate {
            let fields = left
                .fields()
                .iter()
                .chain(right.fields())
                .cloned()
                .collect::<Vec<_>>();
            let schema = Arc::new(Schema::new(fields));
            let expression = crate::expr::try_compile_expr(
                &schema,
                &predicate.kinds,
                &predicate.payload,
                &predicate.child_counts,
                &predicate.longs,
                &predicate.doubles,
                &predicate.strings,
                0,
            )?;
            if expression.data_type(&schema)? != DataType::Boolean {
                return datafusion::common::exec_err!("bounded join predicate is not boolean");
            }
            let columns = (0..left.fields().len())
                .map(|index| ColumnIndex {
                    index,
                    side: if build_left {
                        JoinSide::Left
                    } else {
                        JoinSide::Right
                    },
                })
                .chain((0..right.fields().len()).map(|index| ColumnIndex {
                    index,
                    side: if build_left {
                        JoinSide::Right
                    } else {
                        JoinSide::Left
                    },
                }))
                .collect();
            Some(JoinFilter::new(expression, columns, schema))
        } else {
            None
        };
        let join: Arc<dyn ExecutionPlan> = if merge_join {
            use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
            use datafusion::physical_plan::sorts::sort::SortExec;
            let options = SortOptions {
                descending: false,
                nulls_first: true,
            };
            let (left_order, right_order): (Vec<_>, Vec<_>) = keys
                .iter()
                .map(|(left, right)| {
                    (
                        PhysicalSortExpr {
                            expr: left.clone(),
                            options,
                        },
                        PhysicalSortExpr {
                            expr: right.clone(),
                            options,
                        },
                    )
                })
                .unzip();
            let left = Arc::new(SortExec::new(LexOrdering::new(left_order).unwrap(), build));
            let right = Arc::new(SortExec::new(LexOrdering::new(right_order).unwrap(), probe));
            let orders = vec![options; keys.len()];
            Arc::new(SortMergeJoinExec::try_new(
                left,
                right,
                keys,
                filter,
                join_type,
                orders,
                if null_equals {
                    NullEquality::NullEqualsNull
                } else {
                    NullEquality::NullEqualsNothing
                },
            )?)
        } else if nested_loop {
            Arc::new(NestedLoopJoinExec::try_new(
                build, probe, filter, &join_type, None,
            )?)
        } else {
            Arc::new(HashJoinExec::try_new(
                build,
                probe,
                keys,
                filter,
                &join_type,
                None,
                PartitionMode::Partitioned,
                if null_equals {
                    NullEquality::NullEqualsNull
                } else {
                    NullEquality::NullEqualsNothing
                },
                false,
            )?)
        };
        let indices: Vec<usize> = if !build_left && kind < 4 {
            (right.fields().len()..right.fields().len() + left.fields().len())
                .chain(0..right.fields().len())
                .collect()
        } else {
            (0..join.schema().fields().len()).collect()
        };
        if declared.fields().len() != indices.len() {
            return datafusion::common::exec_err!("bounded join output width mismatch");
        }
        let schema = join.schema();
        let mut outputs = Vec::new();
        for (&index, target) in indices.iter().zip(declared.fields()) {
            if schema.field(index).data_type() != target.data_type() {
                return datafusion::common::exec_err!("bounded join output type mismatch");
            }
            outputs.push((
                Arc::new(Column::new(schema.field(index).name(), index)) as Arc<dyn PhysicalExpr>,
                target.name().clone(),
            ));
        }
        let output = Arc::new(ProjectionExec::try_new(outputs, join.clone())?);
        Ok(Self {
            inputs,
            output,
            join,
        })
    }

    pub(crate) fn execute(self, context: Arc<TaskContext>) -> Result<BoundedTask, DataFusionError> {
        BoundedTask::try_new_inputs(self.inputs, self.output, self.join, context, None)
    }
}

fn read_plan<'local>(
    env: &mut JNIEnv<'local>,
    left: jlong,
    right: jlong,
    output: jlong,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    kind: jint,
    build_left: jboolean,
    null_equals: jboolean,
) -> Result<JoinPlan, DataFusionError> {
    JoinPlan::try_new(
        import_schema(left),
        import_schema(right),
        import_schema(output),
        &read_columns(env, &left_keys),
        &read_columns(env, &right_keys),
        kind,
        build_left != 0,
        null_equals != 0,
    )
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_supportsBoundedHashJoin<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left: jlong,
    right: jlong,
    output: jlong,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    kind: jint,
    build_left: jboolean,
    null_equals: jboolean,
) -> jboolean {
    jni_guard(env, move |env| {
        read_plan(
            env,
            left,
            right,
            output,
            left_keys,
            right_keys,
            kind,
            build_left,
            null_equals,
        )
        .is_ok() as jboolean
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createBoundedHashJoin<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left: jlong,
    right: jlong,
    output: jlong,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    kind: jint,
    build_left: jboolean,
    null_equals: jboolean,
    directories: JObjectArray<'local>,
    batch_rows: jint,
    task_name: JString<'local>,
) -> jlong {
    jni_guard(env, move |env| {
        let result = (|| {
            let plan = read_plan(
                env,
                left,
                right,
                output,
                left_keys,
                right_keys,
                kind,
                build_left,
                null_equals,
            )?;
            let directories = read_string_array(env, &directories);
            if directories.is_empty() || batch_rows <= 0 {
                return datafusion::common::exec_err!(
                    "bounded join needs spill directories and a positive batch size"
                );
            }
            let name: String = env
                .get_string(&task_name)
                .expect("bounded task name")
                .into();
            let context = bounded_context(directories, &name, batch_rows as usize, 64 * 1024)?;
            let _runtime = runtime().enter();
            plan.execute(context)
        })();
        match result {
            Ok(task) => into_handle(task),
            Err(error) => {
                throw_error(env, error);
                0
            }
        }
    })
}

fn read_generic_plan<'local>(
    env: &mut JNIEnv<'local>,
    left: jlong,
    right: jlong,
    output: jlong,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    kind: jint,
    build_left: jboolean,
    null_equals: jboolean,
    nested_loop: jboolean,
    predicate_handle: jlong,
) -> Result<JoinPlan, DataFusionError> {
    let predicate = if predicate_handle == 0 {
        None
    } else {
        Some(unsafe { &*(predicate_handle as *const crate::calc::FilterExpression) })
    };
    JoinPlan::try_new_plan(
        import_schema(left),
        import_schema(right),
        import_schema(output),
        &read_columns(env, &left_keys),
        &read_columns(env, &right_keys),
        kind,
        build_left != 0,
        null_equals != 0,
        nested_loop != 0,
        false,
        predicate,
    )
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_supportsBoundedJoinPlan<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left: jlong,
    right: jlong,
    output: jlong,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    kind: jint,
    build_left: jboolean,
    null_equals: jboolean,
    nested_loop: jboolean,
    predicate_handle: jlong,
) -> jboolean {
    jni_guard(env, move |env| {
        read_generic_plan(
            env,
            left,
            right,
            output,
            left_keys,
            right_keys,
            kind,
            build_left,
            null_equals,
            nested_loop,
            predicate_handle,
        )
        .is_ok() as jboolean
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createBoundedJoinPlan<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left: jlong,
    right: jlong,
    output: jlong,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    kind: jint,
    build_left: jboolean,
    null_equals: jboolean,
    nested_loop: jboolean,
    predicate_handle: jlong,
    directories: JObjectArray<'local>,
    batch_rows: jint,
    task_name: JString<'local>,
) -> jlong {
    jni_guard(env, move |env| {
        let result = (|| {
            let plan = read_generic_plan(
                env,
                left,
                right,
                output,
                left_keys,
                right_keys,
                kind,
                build_left,
                null_equals,
                nested_loop,
                predicate_handle,
            )?;
            let directories = read_string_array(env, &directories);
            if directories.is_empty() || batch_rows <= 0 {
                return datafusion::common::exec_err!(
                    "bounded join needs spill directories and a positive batch size"
                );
            }
            let name: String = env
                .get_string(&task_name)
                .expect("bounded task name")
                .into();
            let context = bounded_context(directories, &name, batch_rows as usize, 64 * 1024)?;
            let _runtime = runtime().enter();
            plan.execute(context)
        })();
        match result {
            Ok(task) => into_handle(task),
            Err(error) => {
                throw_error(env, error);
                0
            }
        }
    })
}

fn read_merge_plan<'local>(
    env: &mut JNIEnv<'local>,
    left: jlong,
    right: jlong,
    output: jlong,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    kind: jint,
    null_equals: jboolean,
    predicate_handle: jlong,
) -> Result<JoinPlan, DataFusionError> {
    let predicate = if predicate_handle == 0 {
        None
    } else {
        Some(unsafe { &*(predicate_handle as *const crate::calc::FilterExpression) })
    };
    JoinPlan::try_new_plan(
        import_schema(left),
        import_schema(right),
        import_schema(output),
        &read_columns(env, &left_keys),
        &read_columns(env, &right_keys),
        kind,
        true,
        null_equals != 0,
        false,
        true,
        predicate,
    )
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_supportsBoundedMergeJoin<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left: jlong,
    right: jlong,
    output: jlong,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    kind: jint,
    null_equals: jboolean,
    predicate_handle: jlong,
) -> jboolean {
    jni_guard(env, move |env| {
        read_merge_plan(
            env,
            left,
            right,
            output,
            left_keys,
            right_keys,
            kind,
            null_equals,
            predicate_handle,
        )
        .is_ok() as jboolean
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createBoundedMergeJoin<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left: jlong,
    right: jlong,
    output: jlong,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    kind: jint,
    null_equals: jboolean,
    predicate_handle: jlong,
    directories: JObjectArray<'local>,
    batch_rows: jint,
    task_name: JString<'local>,
) -> jlong {
    jni_guard(env, move |env| {
        let result = (|| {
            let plan = read_merge_plan(
                env,
                left,
                right,
                output,
                left_keys,
                right_keys,
                kind,
                null_equals,
                predicate_handle,
            )?;
            let directories = read_string_array(env, &directories);
            if directories.is_empty() || batch_rows <= 0 {
                return datafusion::common::exec_err!(
                    "bounded join needs spill directories and a positive batch size"
                );
            }
            let name: String = env
                .get_string(&task_name)
                .expect("bounded task name")
                .into();
            let context = bounded_context(directories, &name, batch_rows as usize, 64 * 1024)?;
            let _runtime = runtime().enter();
            plan.execute(context)
        })();
        match result {
            Ok(task) => into_handle(task),
            Err(error) => {
                throw_error(env, error);
                0
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::exec::ExecutionEvent;
    use datafusion::execution::config::SessionConfig;

    #[test]
    fn bounded_join_denial_and_cancellation_release_copied_build_state() {
        runtime().block_on(async {
            for build_left in [false, true] {
                for deny in [false, true] {
                    let schema = Arc::new(Schema::new(vec![
                        Field::new("key", DataType::Int64, true),
                        Field::new("payload", DataType::Binary, true),
                    ]));
                    let output = Arc::new(Schema::new(vec![
                        Field::new("lk", DataType::Int64, true),
                        Field::new("lv", DataType::Binary, true),
                        Field::new("rk", DataType::Int64, true),
                        Field::new("rv", DataType::Binary, true),
                    ]));
                    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(512 * 1024));
                    let context = Arc::new(
                        TaskContext::default()
                            .with_runtime(
                                RuntimeEnvBuilder::new()
                                    .with_memory_pool(pool.clone())
                                    .build_arc()
                                    .unwrap(),
                            )
                            .with_session_config(SessionConfig::new().with_batch_size(128)),
                    );
                    let plan = JoinPlan::try_new(
                        schema.clone(),
                        schema.clone(),
                        output,
                        &[0],
                        &[0],
                        0,
                        build_left,
                        false,
                    )
                    .unwrap();
                    let mut task = plan.execute(context).unwrap();
                    let port = if build_left { 0 } else { 1 };
                    let mut failed = false;
                    loop {
                        match task.next_event().await.unwrap() {
                            Some(ExecutionEvent::InputNeeded(ports)) => {
                                assert!(ports.contains(&port));
                                break;
                            }
                            None => {}
                            _ => panic!("join emitted before completing its build input"),
                        }
                    }
                    for batch_number in 0..if deny { 128 } else { 2 } {
                        let payload: ArrayRef = Arc::new(BinaryArray::from_iter_values(
                            (0..128).map(|_| vec![0_u8; 128]),
                        ));
                        let borrowed = Arc::downgrade(&payload);
                        let batch = RecordBatch::try_new(
                            schema.clone(),
                            vec![
                                Arc::new(Int64Array::from_iter_values(
                                    (0..128).map(|i| batch_number * 128 + i),
                                )),
                                payload,
                            ],
                        )
                        .unwrap();
                        task.push_input(port, batch).unwrap();
                        loop {
                            match task.next_event().await {
                                Ok(Some(ExecutionEvent::InputNeeded(ports))) => {
                                    assert!(ports.contains(&port));
                                    break;
                                }
                                Ok(None) => {}
                                Err(error) => {
                                    assert!(
                                        matches!(
                                            error.find_root(),
                                            DataFusionError::ResourcesExhausted(_)
                                        ),
                                        "{error}"
                                    );
                                    failed = true;
                                    break;
                                }
                                _ => panic!("join emitted before completing its build input"),
                            }
                        }
                        assert!(
                            borrowed.upgrade().is_none(),
                            "build state must own its compact buffers"
                        );
                        if failed {
                            break;
                        }
                    }
                    assert_eq!(deny, failed);
                    if !deny {
                        assert!(pool.reserved() > 0);
                    }
                    drop(task);
                    tokio::time::timeout(std::time::Duration::from_secs(2), async {
                        while pool.reserved() != 0 {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect("cancelled build reservations must retire");
                }
            }
        });
    }
}
