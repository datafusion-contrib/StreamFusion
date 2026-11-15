//! Bounded analytic windows use Arroyo's partition/sort/DataFusion plan structure.
use crate::batch::exec::BoundedInput;
use crate::batch::task::{bounded_context, copyable, throw_error, BoundedTask};
use crate::*;
use datafusion::logical_expr::{
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::physical_expr::expressions::cast;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::{
    projection::ProjectionExec,
    sorts::sort::SortExec,
    windows::{create_window_expr, WindowAggExec},
    ExecutionPlan,
};

type Plan = Arc<dyn ExecutionPlan>;

struct WindowPlan {
    input: BoundedInput,
    output: Plan,
    window: Plan,
}

impl WindowPlan {
    fn try_rank(
        schema: SchemaRef,
        declared: SchemaRef,
        partition: &[usize],
        order: &[usize],
        flags: &[i32],
        kinds: &[i32],
        columns: &[i32],
        first: i64,
        last: i64,
        keep_rank: bool,
    ) -> Result<Self, DataFusionError> {
        if kinds.len() != 1
            || !matches!(kinds[0], 10..=12)
            || first < 1
            || last < first
            || order.len() != flags.len()
            || flags.iter().any(|flag| flag & !3 != 0)
            || declared.fields().len() != schema.fields().len() + usize::from(keep_rank)
        {
            return datafusion::common::exec_err!("Invalid bounded rank specification");
        }
        let mut fields = schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        fields.push(if keep_rank {
            declared.field(fields.len()).clone()
        } else {
            Field::new("__sf_rank", DataType::Int64, true)
        });
        let intermediate = Arc::new(Schema::new(fields));
        // Flink Rank prefixes partitions ASC NULLS FIRST; these keys are constant within each one.
        let rank_flags = order
            .iter()
            .zip(flags)
            .map(|(column, flag)| if partition.contains(column) { 2 } else { *flag })
            .collect::<Vec<_>>();
        let mut plan = Self::try_new(
            schema.clone(),
            intermediate,
            partition,
            order,
            &rank_flags,
            kinds,
            columns,
            false,
            false,
        )?;
        let output_schema = plan.output.schema();
        let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new(
            output_schema.field(schema.fields().len()).name(),
            schema.fields().len(),
        ));
        let predicate = Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
            Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                value.clone(),
                Operator::GtEq,
                lit(ScalarValue::Int64(Some(first))),
            )),
            Operator::And,
            Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                value,
                Operator::LtEq,
                lit(ScalarValue::Int64(Some(last))),
            )),
        ));
        let filtered = Arc::new(datafusion::physical_plan::filter::FilterExec::try_new(
            predicate,
            plan.output,
        )?);
        let fields: Vec<_> = declared
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                (
                    Arc::new(Column::new(output_schema.field(i).name(), i))
                        as Arc<dyn PhysicalExpr>,
                    f.name().clone(),
                )
            })
            .collect();
        plan.output = Arc::new(ProjectionExec::try_new(fields, filtered)?);
        Ok(plan)
    }

    fn try_new(
        schema: SchemaRef,
        declared: SchemaRef,
        partition: &[usize],
        order: &[usize],
        flags: &[i32],
        kinds: &[i32],
        columns: &[i32],
        rows: bool,
        whole: bool,
    ) -> Result<Self, DataFusionError> {
        if kinds.is_empty()
            || kinds.len() != columns.len()
            || order.len() != flags.len()
            || declared.fields().len() != schema.fields().len() + kinds.len()
            || !schema.fields().iter().all(|f| copyable(f.data_type()))
        {
            return datafusion::common::exec_err!("Invalid bounded window specification");
        }
        let schema = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .map(|f| f.as_ref().clone().with_nullable(true))
                .collect::<Vec<_>>(),
        ));
        let column = |index: usize| -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
            let field = schema.fields().get(index).ok_or_else(|| {
                DataFusionError::Execution("Window column outside input schema".into())
            })?;
            if !super::aggregate::grouping_key(field.data_type()) {
                return datafusion::common::exec_err!("Unverified window partition or order type");
            }
            Ok(Arc::new(Column::new(field.name(), index)))
        };
        let partition_by = partition
            .iter()
            .map(|&i| column(i))
            .collect::<Result<Vec<_>, _>>()?;
        let ordering = order
            .iter()
            .zip(flags)
            .map(|(&i, &f)| {
                if f & !3 != 0 {
                    return datafusion::common::exec_err!("Invalid window sort flags");
                }
                Ok(PhysicalSortExpr {
                    expr: column(i)?,
                    options: SortOptions {
                        descending: f & 1 != 0,
                        nulls_first: f & 2 != 0,
                    },
                })
            })
            .collect::<Result<Vec<_>, DataFusionError>>()?;
        let frame = Arc::new(WindowFrame::new_bounds(
            if rows {
                WindowFrameUnits::Rows
            } else {
                WindowFrameUnits::Range
            },
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            if whole {
                WindowFrameBound::Following(ScalarValue::UInt64(None))
            } else {
                WindowFrameBound::CurrentRow
            },
        ));
        let mut expressions = Vec::new();
        for (index, (&kind, &value_column)) in kinds.iter().zip(columns).enumerate() {
            let (function, args) = if kind >= 10 {
                let function = match kind {
                    10 => datafusion::functions_window::rank::rank_udwf(),
                    11 => datafusion::functions_window::rank::dense_rank_udwf(),
                    12 => datafusion::functions_window::row_number::row_number_udwf(),
                    _ => {
                        return datafusion::common::exec_err!("Unverified bounded ranking function")
                    }
                };
                (WindowFunctionDefinition::WindowUDF(function), vec![])
            } else {
                let value: Arc<dyn PhysicalExpr> = if value_column == -1 && kind == 1 {
                    lit(ScalarValue::Boolean(Some(true)))
                } else {
                    let i = usize::try_from(value_column).map_err(|_| {
                        DataFusionError::Execution("Invalid window argument".into())
                    })?;
                    let field = schema.fields().get(i).ok_or_else(|| {
                        DataFusionError::Execution("Window argument outside schema".into())
                    })?;
                    Arc::new(Column::new(field.name(), i))
                };
                let ty = value.data_type(&schema)?;
                let function = match kind {
                    1 => count_udaf(),
                    0 | 5 if matches!(ty, DataType::Decimal128(1..=38, _)) => {
                        if kind == 5 {
                            super::aggregate::decimal_sum_zero(ty, false)
                        } else {
                            super::aggregate::decimal_sum(ty)
                        }
                    }
                    2 if super::aggregate::grouping_key(&ty) && ty != DataType::Utf8 => min_udaf(),
                    3 if super::aggregate::grouping_key(&ty) && ty != DataType::Utf8 => max_udaf(),
                    _ => {
                        return datafusion::common::exec_err!(
                            "Unverified bounded window aggregate type"
                        )
                    }
                };
                (
                    WindowFunctionDefinition::AggregateUDF(function),
                    vec![value],
                )
            };
            expressions.push(create_window_expr(
                &function,
                format!("window{index}"),
                &args,
                &partition_by,
                &ordering,
                frame.clone(),
                schema.clone(),
                false,
                false,
                None,
            )?);
        }
        let input = BoundedInput::new(schema.clone());
        let mut sorted = partition
            .iter()
            .zip(&partition_by)
            .map(|(column, expr)| PhysicalSortExpr {
                expr: expr.clone(),
                options: order
                    .iter()
                    .position(|ordered| ordered == column)
                    .map(|index| ordering[index].options)
                    .unwrap_or(SortOptions {
                        descending: false,
                        nulls_first: true,
                    }),
            })
            .collect::<Vec<_>>();
        sorted.extend(
            order
                .iter()
                .zip(ordering)
                .filter(|(column, _)| !partition.contains(column))
                .map(|(_, expression)| expression),
        );
        let source: Plan = match LexOrdering::new(sorted) {
            Some(ordering) => Arc::new(SortExec::new(ordering, input.execution_plan()?)),
            None => input.execution_plan()?,
        };
        let window: Plan = Arc::new(WindowAggExec::try_new(
            expressions,
            source,
            !partition.is_empty(),
        )?);
        let actual = window.schema();
        let mut output = Vec::new();
        for (i, field) in declared.fields().iter().enumerate() {
            let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new(actual.field(i).name(), i));
            let value = if actual.field(i).data_type() == field.data_type() {
                value
            } else if i >= schema.fields().len()
                && matches!(kinds[i - schema.fields().len()], 10..=12)
                && field.data_type() == &DataType::Int64
            {
                cast(value, &actual, DataType::Int64)?
            } else {
                return datafusion::common::exec_err!("Bounded window output type mismatch");
            };
            output.push((value, field.name().clone()));
        }
        Ok(Self {
            input,
            output: Arc::new(ProjectionExec::try_new(output, window.clone())?),
            window,
        })
    }
}

fn read_plan(
    env: &mut JNIEnv<'_>,
    schema: jlong,
    output: jlong,
    partition: &JIntArray<'_>,
    order: &JIntArray<'_>,
    flags: &JIntArray<'_>,
    kinds: &JIntArray<'_>,
    columns: &JIntArray<'_>,
    rows: jboolean,
    whole: jboolean,
    first: jlong,
    last: jlong,
    keep_rank: jboolean,
) -> Result<WindowPlan, DataFusionError> {
    let schema = import_schema(schema);
    let output = import_schema(output);
    let partition = read_columns(env, partition);
    let order = read_columns(env, order);
    let flags = read_i32_array(env, flags);
    let kinds = read_i32_array(env, kinds);
    let columns = read_i32_array(env, columns);
    if last >= 0 {
        WindowPlan::try_rank(
            schema,
            output,
            &partition,
            &order,
            &flags,
            &kinds,
            &columns,
            first,
            last,
            keep_rank != 0,
        )
    } else {
        WindowPlan::try_new(
            schema,
            output,
            &partition,
            &order,
            &flags,
            &kinds,
            &columns,
            rows != 0,
            whole != 0,
        )
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_supportsBoundedWindow<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema: jlong,
    output: jlong,
    partition: JIntArray<'local>,
    order: JIntArray<'local>,
    flags: JIntArray<'local>,
    kinds: JIntArray<'local>,
    columns: JIntArray<'local>,
    rows: jboolean,
    whole: jboolean,
    first: jlong,
    last: jlong,
    keep_rank: jboolean,
) -> jboolean {
    jni_guard(env, move |env| {
        u8::from(
            read_plan(
                env, schema, output, &partition, &order, &flags, &kinds, &columns, rows, whole,
                first, last, keep_rank,
            )
            .is_ok(),
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createBoundedWindow<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema: jlong,
    output: jlong,
    partition: JIntArray<'local>,
    order: JIntArray<'local>,
    flags: JIntArray<'local>,
    kinds: JIntArray<'local>,
    columns: JIntArray<'local>,
    rows: jboolean,
    whole: jboolean,
    first: jlong,
    last: jlong,
    keep_rank: jboolean,
    directories: JObjectArray<'local>,
    batch_rows: jint,
    reservation: jlong,
    task_name: JString<'local>,
) -> jlong {
    jni_guard(env, move |env| {
        let result = (|| {
            let plan = read_plan(
                env, schema, output, &partition, &order, &flags, &kinds, &columns, rows, whole,
                first, last, keep_rank,
            )?;
            let dirs = read_string_array(env, &directories);
            if dirs.is_empty() || batch_rows <= 0 || reservation <= 0 {
                return datafusion::common::exec_err!(
                    "Bounded windows need host spill directories and positive sizes"
                );
            }
            let name: String = env
                .get_string(&task_name)
                .expect("bounded window task name")
                .into();
            let context = bounded_context(dirs, &name, batch_rows as usize, reservation as usize)?;
            let _runtime = runtime().enter();
            BoundedTask::try_new(plan.input, plan.output, plan.window, context, None)
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

    async fn collect(plan: WindowPlan, batch: RecordBatch) -> Vec<Vec<ScalarValue>> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2 << 20));
        let mut config = SessionConfig::new().with_batch_size(2);
        config.options_mut().execution.sort_spill_reservation_bytes = 64 << 10;
        let context = Arc::new(
            TaskContext::default()
                .with_runtime(
                    RuntimeEnvBuilder::new()
                        .with_memory_pool(pool.clone())
                        .build_arc()
                        .unwrap(),
                )
                .with_session_config(config),
        );
        let mut task =
            BoundedTask::try_new(plan.input, plan.output, plan.window, context, None).unwrap();
        task.push(batch).unwrap();
        loop {
            match task.next_event().await.unwrap() {
                Some(ExecutionEvent::InputNeeded(_)) => break,
                None => {}
                _ => panic!("sorted window emitted before EOF"),
            }
        }
        task.finish().unwrap();
        let mut rows = Vec::new();
        loop {
            match task.next_event().await.unwrap() {
                Some(ExecutionEvent::Output(batch)) => {
                    for row in 0..batch.num_rows() {
                        rows.push(
                            batch
                                .columns()
                                .iter()
                                .map(|c| ScalarValue::try_from_array(c, row).unwrap())
                                .collect(),
                        );
                    }
                    drop(batch);
                    task.output_memory.free();
                }
                Some(ExecutionEvent::Finished) => break,
                None => {}
                _ => panic!("window requested input after EOF"),
            }
        }
        drop(task);
        assert_eq!(pool.reserved(), 0);
        rows
    }

    #[test]
    fn overlapping_partition_order_matches_flink_over_and_rank_collations() {
        runtime().block_on(async {
            let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![
                    Some(1),
                    None,
                    Some(2),
                    Some(1),
                    None,
                    Some(2),
                ]))],
            )
            .unwrap();
            let declared = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, true),
                Field::new("n", DataType::Int64, true),
            ]));
            for (flags, expected) in [
                (0, vec![Some(1), Some(1), Some(2), Some(2), None, None]),
                (1, vec![Some(2), Some(2), Some(1), Some(1), None, None]),
                (2, vec![None, None, Some(1), Some(1), Some(2), Some(2)]),
                (3, vec![None, None, Some(2), Some(2), Some(1), Some(1)]),
            ] {
                let plan = WindowPlan::try_new(
                    schema.clone(),
                    declared.clone(),
                    &[0],
                    &[0],
                    &[flags],
                    &[1],
                    &[-1],
                    false,
                    false,
                )
                .unwrap();
                let rows = collect(plan, batch.clone()).await;
                assert_eq!(
                    rows.iter().map(|row| row[0].clone()).collect::<Vec<_>>(),
                    expected
                        .into_iter()
                        .map(ScalarValue::Int32)
                        .collect::<Vec<_>>(),
                    "OVER partition ordering flags={flags}",
                );
                assert!(rows.iter().all(|row| row[1] == ScalarValue::Int64(Some(2))));
            }
            let plan = WindowPlan::try_rank(
                schema.clone(),
                schema,
                &[0],
                &[0],
                &[1],
                &[10],
                &[-1],
                1,
                1,
                false,
            )
            .unwrap();
            let rows = collect(plan, batch).await;
            assert_eq!(
                rows.into_iter()
                    .map(|row| row[0].clone())
                    .collect::<Vec<_>>(),
                vec![None, None, Some(1), Some(1), Some(2), Some(2)]
                    .into_iter()
                    .map(ScalarValue::Int32)
                    .collect::<Vec<_>>(),
                "Rank retains Flink's ascending NULLS FIRST partition prefix",
            );
        });
    }

    #[test]
    fn whole_partition_count_and_decimal_sum_zero_preserve_null_frames() {
        runtime().block_on(async {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, true),
                Field::new("v", DataType::Decimal128(38, 2), true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![
                        Some(1),
                        None,
                        Some(1),
                        None,
                        Some(2),
                    ])),
                    Arc::new(
                        Decimal128Array::from(vec![Some(100), None, Some(-50), None, None])
                            .with_precision_and_scale(38, 2)
                            .unwrap(),
                    ),
                ],
            )
            .unwrap();
            let mut fields = schema
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>();
            fields.extend([
                Field::new("n", DataType::Int64, true),
                Field::new("sum", DataType::Decimal128(38, 2), true),
            ]);
            let plan = WindowPlan::try_new(
                schema,
                Arc::new(Schema::new(fields)),
                &[0],
                &[],
                &[],
                &[1, 5],
                &[1, 1],
                false,
                true,
            )
            .unwrap();
            let rows = collect(plan, batch).await;
            assert_eq!(rows.len(), 5);
            for row in rows {
                let (count, sum) = if row[0] == ScalarValue::Int32(Some(1)) {
                    (2, 50)
                } else {
                    (0, 0)
                };
                assert_eq!(row[2], ScalarValue::Int64(Some(count)));
                assert_eq!(row[3], ScalarValue::Decimal128(Some(sum), 38, 2));
            }
        });
    }

    #[test]
    fn rank_filter_keeps_ties_without_exposing_an_internal_column() {
        runtime().block_on(async {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, true),
                Field::new("v", DataType::Int64, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 1, 1, 2])),
                    Arc::new(Int64Array::from(vec![7, 3, 7, 9, 2])),
                ],
            )
            .unwrap();
            let plan = WindowPlan::try_rank(
                schema.clone(),
                schema,
                &[0],
                &[1],
                &[0],
                &[10],
                &[-1],
                1,
                1,
                false,
            )
            .unwrap();
            let rows = collect(plan, batch).await;
            assert_eq!(
                rows,
                vec![
                    vec![ScalarValue::Int32(Some(1)), ScalarValue::Int64(Some(7))],
                    vec![ScalarValue::Int32(Some(1)), ScalarValue::Int64(Some(7))],
                    vec![ScalarValue::Int32(Some(2)), ScalarValue::Int64(Some(2))]
                ]
            );
        });
    }
}
