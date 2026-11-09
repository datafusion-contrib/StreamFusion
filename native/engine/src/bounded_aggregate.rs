use crate::bounded_exec::BoundedInput;
use crate::bounded_task::{copyable, BoundedTask};
use crate::*;
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_expr::expressions::{cast, BinaryExpr, CaseExpr, IsNullExpr};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use streamfusion_bridge::prelude::*;

type Plan = Arc<dyn ExecutionPlan>;

pub(crate) struct AggregatePlan {
    input: BoundedInput,
    output: Plan,
    aggregate: Plan,
    scratch_per_row: usize,
    filtered: bool,
}

impl AggregatePlan {
    pub(crate) fn try_new(
        schema: SchemaRef,
        declared: SchemaRef,
        kinds: &[i32],
        columns: &[i32],
        filters: &[i32],
        merge: bool,
        final_output: bool,
    ) -> Result<Self, DataFusionError> {
        if kinds.len() != columns.len()
            || kinds.len() != filters.len()
            || !schema
                .fields()
                .iter()
                .all(|field| copyable(field.data_type()))
        {
            return datafusion::common::exec_err!("invalid bounded aggregate specification");
        }
        let schema = Arc::new(Schema::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone().with_nullable(true))
                .collect::<Vec<_>>(),
            schema.metadata().clone(),
        ));
        let input = BoundedInput::new(schema.clone());
        let mut aggregates = Vec::new();
        let mut predicates = Vec::new();
        let mut outputs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
        let mut cursor = 0;
        // Each output projection records the source aggregate indices and its host result type.
        let mut results = Vec::new();
        for ((&kind, &column), &filter) in kinds.iter().zip(columns).zip(filters) {
            let field = declared.fields().get(cursor).ok_or_else(|| {
                DataFusionError::Execution("bounded aggregate output is too short".into())
            })?;
            let value = if column < 0 {
                if kind != 1 || merge {
                    return datafusion::common::exec_err!("invalid aggregate input column");
                }
                lit(ScalarValue::Boolean(Some(true)))
            } else {
                input_column(&schema, column as usize)?
            };
            let value_type = value.data_type(&schema)?;
            if kind != 1 && !signed_integer(&value_type) {
                return datafusion::common::exec_err!(
                    "bounded aggregate requires signed integer values"
                );
            }
            let predicate = if merge || filter < 0 {
                None
            } else {
                let predicate = input_column(&schema, filter as usize)?;
                if predicate.data_type(&schema)? != DataType::Boolean {
                    return datafusion::common::exec_err!("aggregate FILTER is not boolean");
                }
                Some(predicate)
            };
            let first = aggregates.len();
            match kind {
                0 | 5 => append(
                    &mut aggregates,
                    &mut predicates,
                    sum_udaf(),
                    cast(value, &schema, DataType::Int64)?,
                    &schema,
                    predicate,
                )?,
                1 => {
                    if merge {
                        if value_type != DataType::Int64 {
                            return datafusion::common::exec_err!("count state is not BIGINT");
                        }
                        append(
                            &mut aggregates,
                            &mut predicates,
                            sum_udaf(),
                            value,
                            &schema,
                            None,
                        )?
                    } else {
                        append(
                            &mut aggregates,
                            &mut predicates,
                            count_udaf(),
                            value,
                            &schema,
                            predicate,
                        )?
                    }
                }
                2 | 3 => append(
                    &mut aggregates,
                    &mut predicates,
                    if kind == 2 { min_udaf() } else { max_udaf() },
                    value,
                    &schema,
                    predicate,
                )?,
                4 => {
                    append(
                        &mut aggregates,
                        &mut predicates,
                        sum_udaf(),
                        cast(value.clone(), &schema, DataType::Int64)?,
                        &schema,
                        predicate.clone(),
                    )?;
                    if merge {
                        let counts = input_column(&schema, column as usize + 1)?;
                        if counts.data_type(&schema)? != DataType::Int64
                            || value_type != DataType::Int64
                        {
                            return datafusion::common::exec_err!(
                                "integer AVG state is not BIGINT/BIGINT"
                            );
                        }
                        append(
                            &mut aggregates,
                            &mut predicates,
                            sum_udaf(),
                            counts,
                            &schema,
                            None,
                        )?;
                    } else {
                        append(
                            &mut aggregates,
                            &mut predicates,
                            count_udaf(),
                            value,
                            &schema,
                            predicate,
                        )?;
                    }
                }
                _ => return datafusion::common::exec_err!("unverified bounded aggregate kind"),
            }
            results.push((kind, first, field.data_type().clone()));
            cursor += if kind == 4 && !final_output { 2 } else { 1 };
        }
        if cursor != declared.fields().len() {
            return datafusion::common::exec_err!("bounded aggregate output width mismatch");
        }
        let has_input = if final_output {
            None
        } else {
            let index = aggregates.len();
            append(
                &mut aggregates,
                &mut predicates,
                count_udaf(),
                lit(ScalarValue::Boolean(Some(true))),
                &schema,
                None,
            )?;
            Some(index)
        };
        if aggregates.is_empty() {
            append(
                &mut aggregates,
                &mut predicates,
                count_udaf(),
                lit(ScalarValue::Null),
                &schema,
                None,
            )?;
        }
        // Single global aggregation charges accumulator growth after evaluation, but not the
        // evaluated cast/literal arrays or per-call FILTER copies. Reserve their bounded workspace
        // before polling; primitive arguments need at most two eight-byte arrays plus validity and
        // selection buffers per function. Filtered variable-width payloads are budgeted separately.
        let scratch_per_row = (aggregates.len() + 1).checked_mul(32).ok_or_else(|| {
            DataFusionError::ResourcesExhausted("aggregate workspace size overflow".into())
        })?;
        let filtered = predicates.iter().any(Option::is_some);
        let aggregate: Plan = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![]),
            aggregates,
            predicates,
            input.execution_plan()?,
            schema,
        )?);
        let aggregate_schema = aggregate.schema();
        for (kind, index, target) in results {
            let value = input_column(&aggregate_schema, index)?;
            match kind {
                0 => outputs.push((narrow(value, &aggregate_schema, target)?, String::new())),
                5 => outputs.push((
                    narrow(zero_if_null(value)?, &aggregate_schema, target)?,
                    String::new(),
                )),
                1 => outputs.push((zero_if_null(value)?, String::new())),
                2 | 3 => outputs.push((value, String::new())),
                4 => {
                    let count = zero_if_null(input_column(&aggregate_schema, index + 1)?)?;
                    if final_output {
                        let empty = Arc::new(BinaryExpr::new(
                            count.clone(),
                            Operator::Eq,
                            lit(ScalarValue::Int64(Some(0))),
                        ));
                        let quotient = Arc::new(BinaryExpr::new(value, Operator::Divide, count));
                        let average = Arc::new(CaseExpr::try_new(
                            None,
                            vec![(empty, lit(ScalarValue::Int64(None)))],
                            Some(quotient),
                        )?);
                        outputs.push((narrow(average, &aggregate_schema, target)?, String::new()));
                    } else {
                        outputs.push((zero_if_null(value)?, String::new()));
                        outputs.push((count, String::new()));
                    }
                }
                _ => unreachable!(),
            }
        }
        for ((expression, name), field) in outputs.iter_mut().zip(declared.fields()) {
            if expression.data_type(&aggregate_schema)? != *field.data_type() {
                return datafusion::common::exec_err!("bounded aggregate output type mismatch");
            }
            *name = field.name().clone();
        }
        let filtered_plan: Plan = if let Some(index) = has_input {
            Arc::new(FilterExec::try_new(
                Arc::new(BinaryExpr::new(
                    input_column(&aggregate_schema, index)?,
                    Operator::Gt,
                    lit(ScalarValue::Int64(Some(0))),
                )),
                aggregate.clone(),
            )?)
        } else {
            aggregate.clone()
        };
        let output = Arc::new(ProjectionExec::try_new(outputs, filtered_plan)?);
        Ok(Self {
            input,
            output,
            aggregate,
            scratch_per_row,
            filtered,
        })
    }

    pub(crate) fn execute(self, context: Arc<TaskContext>) -> Result<BoundedTask, DataFusionError> {
        // The admitted global SUM/COUNT/MIN/MAX accumulators retain only scalar values.
        BoundedTask::try_new(
            self.input,
            self.output,
            self.aggregate,
            context,
            Some((self.scratch_per_row, self.filtered)),
        )
    }
}

fn signed_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn input_column(schema: &Schema, index: usize) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let field = schema
        .fields()
        .get(index)
        .ok_or_else(|| DataFusionError::Execution("aggregate column outside input".into()))?;
    Ok(Arc::new(Column::new(field.name(), index)))
}

fn append(
    aggregates: &mut Vec<Arc<AggregateFunctionExpr>>,
    predicates: &mut Vec<Option<Arc<dyn PhysicalExpr>>>,
    function: Arc<AggregateUDF>,
    value: Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
) -> Result<(), DataFusionError> {
    let aggregate = AggregateExprBuilder::new(function, vec![value])
        .schema(schema.clone())
        .alias(format!("aggregate{}", aggregates.len()))
        .build()?;
    aggregates.push(Arc::new(aggregate));
    predicates.push(predicate);
    Ok(())
}

fn zero_if_null(value: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(
            Arc::new(IsNullExpr::new(value.clone())),
            lit(ScalarValue::Int64(Some(0))),
        )],
        Some(value),
    )?))
}

fn narrow(
    value: Arc<dyn PhysicalExpr>,
    schema: &Schema,
    target: DataType,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    if value.data_type(schema)? == target {
        return Ok(value);
    }
    if !signed_integer(&target) {
        return datafusion::common::exec_err!("aggregate result is not a signed integer");
    }
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        Arc::new(ScalarUDF::new_from_impl(crate::expr::NarrowingCast::new(
            target,
        ))),
        vec![value],
        schema,
        Arc::new(ConfigOptions::default()),
    )?))
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_supportsBoundedAggregate<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema_address: jlong,
    output_address: jlong,
    kinds: JIntArray<'local>,
    columns: JIntArray<'local>,
    filters: JIntArray<'local>,
    merge: jboolean,
    final_output: jboolean,
) -> jboolean {
    jni_guard(env, move |env| {
        let schema = import_schema(schema_address);
        let output = import_schema(output_address);
        let kinds = read_i32_array(env, &kinds);
        let columns = read_i32_array(env, &columns);
        let filters = read_i32_array(env, &filters);
        u8::from(
            AggregatePlan::try_new(
                schema,
                output,
                &kinds,
                &columns,
                &filters,
                merge != 0,
                final_output != 0,
            )
            .is_ok(),
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createBoundedAggregate<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema_address: jlong,
    output_address: jlong,
    kinds: JIntArray<'local>,
    columns: JIntArray<'local>,
    filters: JIntArray<'local>,
    merge: jboolean,
    final_output: jboolean,
    directories: JObjectArray<'local>,
    batch_rows: jint,
    task_name: JString<'local>,
) -> jlong {
    jni_guard(env, move |env| {
        let schema = import_schema(schema_address);
        let output = import_schema(output_address);
        let kinds = read_i32_array(env, &kinds);
        let columns = read_i32_array(env, &columns);
        let filters = read_i32_array(env, &filters);
        let directories = read_string_array(env, &directories);
        let task_name: String = env
            .get_string(&task_name)
            .expect("aggregate task name")
            .into();
        let result = (|| {
            if directories.is_empty() || batch_rows <= 0 {
                return datafusion::common::exec_err!(
                    "bounded aggregate needs host spill directories and a positive batch size"
                );
            }
            let context = crate::bounded_task::bounded_context(
                directories,
                &task_name,
                batch_rows as usize,
                64 * 1024,
            )?;
            let _runtime = runtime().enter();
            AggregatePlan::try_new(
                schema,
                output,
                &kinds,
                &columns,
                &filters,
                merge != 0,
                final_output != 0,
            )?
            .execute(context)
        })();
        match result {
            Ok(task) => into_handle(task),
            Err(error) => {
                crate::bounded_task::throw_error(env, error);
                0
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn integer_array(data_type: &DataType, values: Vec<Option<i64>>) -> ArrayRef {
        match data_type {
            DataType::Int8 => Arc::new(Int8Array::from_iter(
                values.into_iter().map(|v| v.map(|v| v as i8)),
            )),
            DataType::Int16 => Arc::new(Int16Array::from_iter(
                values.into_iter().map(|v| v.map(|v| v as i16)),
            )),
            DataType::Int32 => Arc::new(Int32Array::from_iter(
                values.into_iter().map(|v| v.map(|v| v as i32)),
            )),
            _ => Arc::new(Int64Array::from(values)),
        }
    }

    fn result_schema(data_type: &DataType, final_output: bool) -> SchemaRef {
        let mut fields = vec![
            Field::new("sum", data_type.clone(), true),
            Field::new("count", DataType::Int64, true),
            Field::new("star", DataType::Int64, true),
            Field::new("min", data_type.clone(), true),
            Field::new("max", data_type.clone(), true),
        ];
        if final_output {
            fields.push(Field::new("avg", data_type.clone(), true));
        } else {
            fields.push(Field::new("avg_sum", DataType::Int64, true));
            fields.push(Field::new("avg_count", DataType::Int64, true));
        }
        Arc::new(Schema::new(fields))
    }

    fn scalar(data_type: &DataType, value: Option<i64>) -> ScalarValue {
        match data_type {
            DataType::Int8 => ScalarValue::Int8(value.map(|v| v as i8)),
            DataType::Int16 => ScalarValue::Int16(value.map(|v| v as i16)),
            DataType::Int32 => ScalarValue::Int32(value.map(|v| v as i32)),
            _ => ScalarValue::Int64(value),
        }
    }

    async fn run(
        schema: SchemaRef,
        output: SchemaRef,
        batches: Vec<RecordBatch>,
        columns: &[i32],
        filtered: bool,
        merge: bool,
        final_output: bool,
    ) -> Vec<Vec<ScalarValue>> {
        let plan = AggregatePlan::try_new(
            schema,
            output,
            &[0, 1, 1, 2, 3, 4],
            columns,
            &vec![if filtered { 1 } else { -1 }; 6],
            merge,
            final_output,
        )
        .unwrap();
        run_plan(plan, batches).await
    }

    async fn run_plan(plan: AggregatePlan, batches: Vec<RecordBatch>) -> Vec<Vec<ScalarValue>> {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(2 * 1024 * 1024));
        let context = Arc::new(
            TaskContext::default().with_runtime(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(pool.clone())
                    .build_arc()
                    .unwrap(),
            ),
        );
        let mut task = plan.execute(context).unwrap();
        for batch in batches {
            task.push(batch).unwrap();
            loop {
                match task.next_event().await.unwrap() {
                    Some(crate::bounded_exec::ExecutionEvent::InputNeeded(_)) => break,
                    None => {}
                    _ => panic!("global aggregate emitted before EOF"),
                }
            }
        }
        task.finish().unwrap();
        let mut values = Vec::new();
        loop {
            match task.next_event().await.unwrap() {
                Some(crate::bounded_exec::ExecutionEvent::Output(batch)) => {
                    for row in 0..batch.num_rows() {
                        values.push(
                            batch
                                .columns()
                                .iter()
                                .map(|a| ScalarValue::try_from_array(a, row).unwrap())
                                .collect(),
                        );
                    }
                    drop(batch);
                    task.output_memory.free();
                }
                Some(crate::bounded_exec::ExecutionEvent::Finished) => break,
                None => {}
                _ => panic!("input demand after EOF"),
            }
        }
        drop(task);
        assert_eq!(pool.reserved(), 0);
        values
    }

    #[test]
    fn bounded_sum_zero_preserves_optimizer_rewrite_defaults() {
        runtime().block_on(async {
            let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int8, true)]));
            for (merge, final_output) in [(false, true), (false, false), (true, true)] {
                for (values, expected) in [
                    (vec![], 0_i8),
                    (vec![None, None], 0),
                    (vec![Some(127), None, Some(1)], -128),
                ] {
                    let batches = if values.is_empty() {
                        vec![]
                    } else {
                        vec![RecordBatch::try_new(
                            schema.clone(),
                            vec![Arc::new(Int8Array::from(values))],
                        )
                        .unwrap()]
                    };
                    let empty_local = batches.is_empty() && !final_output;
                    let plan = AggregatePlan::try_new(
                        schema.clone(),
                        schema.clone(),
                        &[5],
                        &[0],
                        &[-1],
                        merge,
                        final_output,
                    )
                    .unwrap();
                    let result = run_plan(plan, batches).await;
                    assert_eq!(
                        result,
                        if empty_local {
                            vec![]
                        } else {
                            vec![vec![ScalarValue::Int8(Some(expected))]]
                        }
                    );
                }
            }
        });
    }

    #[test]
    fn bounded_integer_aggregates_match_wrapping_and_partial_state_contracts() {
        runtime().block_on(async {
            for (data_type, maximum) in [
                (DataType::Int8, i8::MAX as i64),
                (DataType::Int16, i16::MAX as i64),
                (DataType::Int32, i32::MAX as i64),
                (DataType::Int64, i64::MAX),
            ] {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("x", data_type.clone(), true),
                    Field::new("keep", DataType::Boolean, true),
                ]));
                for filtered in [false, true] {
                    let batches = vec![
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![
                                integer_array(&data_type, vec![Some(maximum), None]),
                                Arc::new(BooleanArray::from(vec![Some(true), Some(true)])),
                            ],
                        )
                        .unwrap(),
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![
                                integer_array(&data_type, vec![Some(maximum), Some(1), Some(-3)]),
                                Arc::new(BooleanArray::from(vec![
                                    Some(false),
                                    Some(true),
                                    Some(true),
                                ])),
                            ],
                        )
                        .unwrap(),
                    ];
                    let sum = if filtered { maximum - 2 } else { -4 };
                    let count = if filtered { 3 } else { 4 };
                    let average_sum = if filtered {
                        maximum - 2
                    } else {
                        maximum
                            .wrapping_add(maximum)
                            .wrapping_add(1)
                            .wrapping_sub(3)
                    };
                    let expected = vec![
                        scalar(&data_type, Some(sum)),
                        ScalarValue::Int64(Some(count)),
                        ScalarValue::Int64(Some(if filtered { 4 } else { 5 })),
                        scalar(&data_type, Some(-3)),
                        scalar(&data_type, Some(maximum)),
                        scalar(&data_type, Some(average_sum / count)),
                    ];
                    let single = run(
                        schema.clone(),
                        result_schema(&data_type, true),
                        batches.clone(),
                        &[0, 0, -1, 0, 0, 0],
                        filtered,
                        false,
                        true,
                    )
                    .await;
                    assert_eq!(single, vec![expected.clone()]);
                    let partial_schema = result_schema(&data_type, false);
                    let mut partials = Vec::new();
                    for batch in batches {
                        let values = run(
                            schema.clone(),
                            partial_schema.clone(),
                            vec![batch],
                            &[0, 0, -1, 0, 0, 0],
                            filtered,
                            false,
                            false,
                        )
                        .await;
                        partials.push(
                            RecordBatch::try_new(
                                partial_schema.clone(),
                                values[0]
                                    .iter()
                                    .map(|value| value.to_array_of_size(1).unwrap())
                                    .collect(),
                            )
                            .unwrap(),
                        );
                    }
                    let final_values = run(
                        partial_schema,
                        result_schema(&data_type, true),
                        partials,
                        &[0, 1, 2, 3, 4, 5],
                        filtered,
                        true,
                        true,
                    )
                    .await;
                    assert_eq!(final_values, vec![expected]);
                }
            }
        });
    }

    #[test]
    fn bounded_global_aggregate_empty_null_and_filtered_inputs_keep_host_defaults() {
        runtime().block_on(async {
            let schema = Arc::new(Schema::new(vec![
                Field::new("x", DataType::Int64, true),
                Field::new("keep", DataType::Boolean, true),
            ]));
            for (values, filters, filtered, star) in [
                (vec![], vec![], false, 0),
                (vec![None, None], vec![None, Some(true)], false, 2),
                (vec![Some(3), None], vec![Some(false), None], true, 0),
            ] {
                let batches = if values.is_empty() {
                    vec![]
                } else {
                    vec![RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(Int64Array::from(values)),
                            Arc::new(BooleanArray::from(filters)),
                        ],
                    )
                    .unwrap()]
                };
                let expected = vec![
                    ScalarValue::Int64(None),
                    ScalarValue::Int64(Some(0)),
                    ScalarValue::Int64(Some(star)),
                    ScalarValue::Int64(None),
                    ScalarValue::Int64(None),
                    ScalarValue::Int64(None),
                ];
                assert_eq!(
                    run(
                        schema.clone(),
                        result_schema(&DataType::Int64, true),
                        batches.clone(),
                        &[0, 0, -1, 0, 0, 0],
                        filtered,
                        false,
                        true
                    )
                    .await,
                    vec![expected]
                );
                let empty_input = batches.is_empty();
                let partial = run(
                    schema.clone(),
                    result_schema(&DataType::Int64, false),
                    batches,
                    &[0, 0, -1, 0, 0, 0],
                    filtered,
                    false,
                    false,
                )
                .await;
                assert_eq!(partial.is_empty(), empty_input);
                if !empty_input {
                    assert_eq!(
                        partial[0][5..],
                        [ScalarValue::Int64(Some(0)), ScalarValue::Int64(Some(0))]
                    );
                }
            }
        });
    }
}
