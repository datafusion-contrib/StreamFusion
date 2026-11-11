use crate::batch::exec::BoundedInput;
use crate::batch::task::{copyable, BoundedTask, ExpressionWorkspace};
use crate::*;
use arrow::datatypes::TimeUnit;
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
    grouped: bool,
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
        Self::try_new_grouped(
            schema,
            declared,
            &[],
            kinds,
            columns,
            filters,
            merge,
            final_output,
        )
    }

    pub(crate) fn try_new_grouped(
        schema: SchemaRef,
        declared: SchemaRef,
        keys: &[usize],
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
        let mut groups = Vec::with_capacity(keys.len());
        let mut cursor = keys.len();
        for (index, &column) in keys.iter().enumerate() {
            let key = input_column(&schema, column)?;
            let data_type = key.data_type(&schema)?;
            if !grouping_key(&data_type)
                || declared
                    .fields()
                    .get(index)
                    .is_none_or(|field| *field.data_type() != data_type)
            {
                return datafusion::common::exec_err!("unverified bounded grouping key");
            }
            groups.push((key, format!("group{index}")));
            outputs.push((
                Arc::new(Column::new(&format!("group{index}"), index)),
                String::new(),
            ));
        }
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
            if kind != 1
                && kind != 6
                && !(if kind == 2 || kind == 3 {
                    fixed_extremum(&value_type)
                } else {
                    signed_integer(&value_type)
                        || matches!(kind, 0 | 4 | 5)
                            && matches!(value_type, DataType::Decimal128(1..=38, _))
                })
            {
                return datafusion::common::exec_err!("unverified bounded aggregate value type");
            }
            let value = if matches!(kind, 2 | 3)
                && matches!(value_type, DataType::Time32(_) | DataType::Time64(_))
                && value_type != DataType::Time32(TimeUnit::Millisecond)
            {
                Arc::new(ScalarFunctionExpr::try_new(
                    Arc::new(ScalarUDF::new_from_impl(MillisecondTime::new())),
                    vec![value],
                    &schema,
                    Arc::new(ConfigOptions::default()),
                )?) as Arc<dyn PhysicalExpr>
            } else {
                value
            };
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
                0 if matches!(value_type, DataType::Decimal128(_, _)) => append(
                    &mut aggregates,
                    &mut predicates,
                    decimal_sum(value_type.clone()),
                    value,
                    &schema,
                    predicate,
                )?,
                5 if matches!(value_type, DataType::Decimal128(_, _)) => append(
                    &mut aggregates,
                    &mut predicates,
                    decimal_sum_zero(value_type.clone(), merge),
                    value,
                    &schema,
                    predicate,
                )?,
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
                4 if matches!(value_type, DataType::Decimal128(_, _)) => {
                    let mut values = vec![value];
                    if merge {
                        let count = input_column(&schema, column as usize + 1)?;
                        if count.data_type(&schema)? != DataType::Int64 {
                            return datafusion::common::exec_err!(
                                "decimal AVG count state is not BIGINT"
                            );
                        }
                        values.push(count);
                    }
                    append_values(
                        &mut aggregates,
                        &mut predicates,
                        decimal_average(value_type.clone(), merge, !final_output),
                        values,
                        &schema,
                        predicate,
                    )?;
                }
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
                6 if final_output && !merge && keys.is_empty() => append(
                    &mut aggregates,
                    &mut predicates,
                    datafusion::functions_aggregate::first_last::first_value_udaf(),
                    value,
                    &schema,
                    predicate,
                )?,
                _ => return datafusion::common::exec_err!("unverified bounded aggregate kind"),
            }
            results.push((
                kind,
                first + keys.len(),
                field.data_type().clone(),
                kind == 4 && matches!(value_type, DataType::Decimal128(_, _)),
            ));
            cursor += if kind == 4 && !final_output { 2 } else { 1 };
        }
        if cursor != declared.fields().len() {
            return datafusion::common::exec_err!("bounded aggregate output width mismatch");
        }
        let has_input = if final_output || !keys.is_empty() {
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
        let scratch_per_row = (aggregates.len() + keys.len() + 1)
            .checked_mul(32)
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted("aggregate workspace size overflow".into())
            })?;
        let filtered = predicates.iter().any(Option::is_some)
            || keys
                .iter()
                .any(|&key| matches!(schema.field(key).data_type(), DataType::Utf8));
        let aggregate: Plan = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(groups),
            aggregates,
            predicates,
            input.execution_plan()?,
            schema,
        )?);
        let aggregate_schema = aggregate.schema();
        for (kind, index, target, decimal_avg) in results {
            let value = input_column(&aggregate_schema, index)?;
            match kind {
                0 => outputs.push((narrow(value, &aggregate_schema, target)?, String::new())),
                5 if matches!(target, DataType::Decimal128(_, _)) => {
                    outputs.push((value, String::new()))
                }
                5 => outputs.push((
                    narrow(zero_if_null(value)?, &aggregate_schema, target)?,
                    String::new(),
                )),
                1 => outputs.push((zero_if_null(value)?, String::new())),
                2 | 3 | 6 => outputs.push((value, String::new())),
                4 if decimal_avg => {
                    if final_output {
                        outputs.push((value, String::new()));
                    } else {
                        for name in ["sum", "count"] {
                            outputs.push((
                                Arc::new(ScalarFunctionExpr::try_new(
                                    datafusion::functions::core::get_field(),
                                    vec![value.clone(), lit(ScalarValue::Utf8(Some(name.into())))],
                                    &aggregate_schema,
                                    Arc::new(ConfigOptions::default()),
                                )?),
                                String::new(),
                            ));
                        }
                    }
                }
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
            grouped: !keys.is_empty(),
        })
    }

    pub(crate) fn execute(self, context: Arc<TaskContext>) -> Result<BoundedTask, DataFusionError> {
        // DataFusion grouping keys and primitive accumulators own their values rather than retaining
        // borrowed Arrow inputs. Output expression workspace also covers every grouped result batch.
        let output_rows = if self.grouped {
            context.session_config().batch_size()
        } else {
            1
        };
        BoundedTask::try_new(
            self.input,
            self.output,
            self.aggregate,
            context,
            Some(ExpressionWorkspace {
                bytes_per_row: self.scratch_per_row,
                variable_input: self.filtered,
                output_rows,
            }),
        )
    }
}

fn decimal_sum(input_type: DataType) -> Arc<AggregateUDF> {
    let DataType::Decimal128(_, scale) = input_type else {
        unreachable!()
    };
    let result_type = DataType::Decimal128(38, scale);
    Arc::new(datafusion::logical_expr::create_udaf(
        "flink_decimal_sum",
        vec![input_type],
        Arc::new(result_type.clone()),
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(move |_| {
            Ok(crate::aggregates::WindowAggregate::DecimalSum { scale }.create_accumulator())
        }),
        Arc::new(vec![result_type]),
    ))
}

fn grouping_key(data_type: &DataType) -> bool {
    signed_integer(data_type)
        || matches!(
            data_type,
            DataType::Utf8 | DataType::Boolean | DataType::Decimal128(1..=38, _) | DataType::Date32
        )
}

fn decimal_average(input_type: DataType, merge_inputs: bool, partial: bool) -> Arc<AggregateUDF> {
    let DataType::Decimal128(_, scale) = input_type else {
        unreachable!()
    };
    let states = vec![DataType::Decimal128(38, scale), DataType::Int64];
    let fields: arrow::datatypes::Fields = vec![
        Field::new("sum", states[0].clone(), true),
        Field::new("count", states[1].clone(), true),
    ]
    .into();
    let result = if partial {
        DataType::Struct(fields.clone())
    } else {
        DataType::Decimal128(38, scale.max(6))
    };
    let inputs = if merge_inputs {
        states.clone()
    } else {
        vec![input_type]
    };
    Arc::new(datafusion::logical_expr::create_udaf(
        "flink_decimal_average",
        inputs,
        Arc::new(result),
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(move |_| {
            Ok(Box::new(DecimalAverage {
                inner: crate::aggregates::WindowAggregate::DecimalAvg {
                    scale,
                    retracting: false,
                }
                .create_accumulator(),
                merge_inputs,
                partial,
                fields: fields.clone(),
            }))
        }),
        Arc::new(states),
    ))
}

/// Only the evaluation shape changes: the existing host-compatible accumulator owns all arithmetic.
#[derive(Debug)]
struct DecimalAverage {
    inner: Box<dyn datafusion::logical_expr::Accumulator>,
    merge_inputs: bool,
    partial: bool,
    fields: arrow::datatypes::Fields,
}

impl datafusion::logical_expr::Accumulator for DecimalAverage {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        if self.merge_inputs {
            self.inner.merge_batch(values)
        } else {
            self.inner.update_batch(values)
        }
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.inner.merge_batch(states)
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError> {
        self.inner.state()
    }
    fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError> {
        if !self.partial {
            return self.inner.evaluate();
        }
        let mut states = self.inner.state()?;
        // Flink's local AVG buffer starts at numeric zero even for an all-NULL group.
        if matches!(states[1], ScalarValue::Int64(Some(0))) {
            if let ScalarValue::Decimal128(None, precision, scale) = states[0] {
                states[0] = ScalarValue::Decimal128(Some(0), precision, scale);
            }
        }
        let arrays = states
            .into_iter()
            .map(|value| value.to_array())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ScalarValue::Struct(Arc::new(
            arrow::array::StructArray::try_new(self.fields.clone(), arrays, None)?,
        )))
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.inner.size()
    }
}

fn decimal_sum_zero(input_type: DataType, merge_inputs: bool) -> Arc<AggregateUDF> {
    let DataType::Decimal128(_, scale) = input_type else {
        unreachable!()
    };
    let result = DataType::Decimal128(38, scale);
    Arc::new(datafusion::logical_expr::create_udaf(
        "flink_decimal_sum_zero",
        vec![input_type],
        Arc::new(result.clone()),
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(move |_| {
            Ok(Box::new(DecimalSumZero {
                inner: crate::aggregates::WindowAggregate::DecimalAvg {
                    scale,
                    retracting: false,
                }
                .create_accumulator(),
                merge_inputs,
            }))
        }),
        Arc::new(vec![result, DataType::Int64]),
    ))
}

/// SUM0 shares AVG's zero initial value and sticky overflow; its host merge also propagates NULL.
#[derive(Debug)]
struct DecimalSumZero {
    inner: Box<dyn datafusion::logical_expr::Accumulator>,
    merge_inputs: bool,
}

impl datafusion::logical_expr::Accumulator for DecimalSumZero {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        if self.merge_inputs {
            self.inner.merge_batch(&[
                values[0].clone(),
                Arc::new(Int64Array::from_value(1, values[0].len())),
            ])
        } else {
            self.inner.update_batch(values)
        }
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.inner.merge_batch(states)
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError> {
        self.inner.state()
    }
    fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError> {
        let states = self.inner.state()?;
        Ok(match (&states[0], &states[1]) {
            (ScalarValue::Decimal128(None, precision, scale), ScalarValue::Int64(Some(0))) => {
                ScalarValue::Decimal128(Some(0), *precision, *scale)
            }
            _ => states[0].clone(),
        })
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.inner.size()
    }
}

fn signed_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn fixed_extremum(data_type: &DataType) -> bool {
    signed_integer(data_type)
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::Decimal128(1..=38, _)
                | DataType::Date32
                | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
                | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        )
}

/// Match ArrowTimeColumnVector's millisecond read, including Java's wrapping int narrowing.
#[derive(Debug, PartialEq, Eq, Hash)]
struct MillisecondTime {
    signature: datafusion::logical_expr::Signature,
}

impl MillisecondTime {
    fn new() -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::any(
                1,
                datafusion::logical_expr::Volatility::Immutable,
            ),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for MillisecondTime {
    fn name(&self) -> &str {
        "flink_millisecond_time"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Time32(TimeUnit::Millisecond))
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> Result<datafusion::logical_expr::ColumnarValue, DataFusionError> {
        use arrow::datatypes::Time32MillisecondType;
        use datafusion::logical_expr::ColumnarValue;
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let values = &arrays[0];
        let result: ArrayRef = match values.data_type() {
            DataType::Time32(TimeUnit::Second) => Arc::new(
                values
                    .as_any()
                    .downcast_ref::<arrow::array::Time32SecondArray>()
                    .unwrap()
                    .unary::<_, Time32MillisecondType>(|v| v.wrapping_mul(1000)),
            ),
            DataType::Time64(TimeUnit::Microsecond) => Arc::new(
                values
                    .as_any()
                    .downcast_ref::<arrow::array::Time64MicrosecondArray>()
                    .unwrap()
                    .unary::<_, Time32MillisecondType>(|v| (v / 1000) as i32),
            ),
            DataType::Time64(TimeUnit::Nanosecond) => Arc::new(
                values
                    .as_any()
                    .downcast_ref::<arrow::array::Time64NanosecondArray>()
                    .unwrap()
                    .unary::<_, Time32MillisecondType>(|v| (v / 1_000_000) as i32),
            ),
            _ => return datafusion::common::exec_err!("unverified extrema TIME representation"),
        };
        Ok(ColumnarValue::Array(result))
    }
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
    append_values(
        aggregates,
        predicates,
        function,
        vec![value],
        schema,
        predicate,
    )
}

fn append_values(
    aggregates: &mut Vec<Arc<AggregateFunctionExpr>>,
    predicates: &mut Vec<Option<Arc<dyn PhysicalExpr>>>,
    function: Arc<AggregateUDF>,
    values: Vec<Arc<dyn PhysicalExpr>>,
    schema: &SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
) -> Result<(), DataFusionError> {
    let aggregate = AggregateExprBuilder::new(function, values)
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
        u8::from(
            read_aggregate_plan(
                env,
                schema_address,
                output_address,
                None,
                kinds,
                columns,
                filters,
                merge,
                final_output,
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
        create_aggregate(
            env,
            schema_address,
            output_address,
            None,
            kinds,
            columns,
            filters,
            merge,
            final_output,
            directories,
            batch_rows,
            task_name,
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_supportsBoundedGroupedAggregate<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema_address: jlong,
    output_address: jlong,
    keys: JIntArray<'local>,
    kinds: JIntArray<'local>,
    columns: JIntArray<'local>,
    filters: JIntArray<'local>,
    merge: jboolean,
    final_output: jboolean,
) -> jboolean {
    jni_guard(env, move |env| {
        u8::from(
            read_aggregate_plan(
                env,
                schema_address,
                output_address,
                Some(keys),
                kinds,
                columns,
                filters,
                merge,
                final_output,
            )
            .is_ok(),
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createBoundedGroupedAggregate<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema_address: jlong,
    output_address: jlong,
    keys: JIntArray<'local>,
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
        create_aggregate(
            env,
            schema_address,
            output_address,
            Some(keys),
            kinds,
            columns,
            filters,
            merge,
            final_output,
            directories,
            batch_rows,
            task_name,
        )
    })
}

fn read_aggregate_plan<'local>(
    env: &mut JNIEnv<'local>,
    schema_address: jlong,
    output_address: jlong,
    keys: Option<JIntArray<'local>>,
    kinds: JIntArray<'local>,
    columns: JIntArray<'local>,
    filters: JIntArray<'local>,
    merge: jboolean,
    final_output: jboolean,
) -> Result<AggregatePlan, DataFusionError> {
    let schema = import_schema(schema_address);
    let output = import_schema(output_address);
    let keys = keys
        .map(|keys| read_columns(env, &keys))
        .unwrap_or_default();
    let kinds = read_i32_array(env, &kinds);
    let columns = read_i32_array(env, &columns);
    let filters = read_i32_array(env, &filters);
    AggregatePlan::try_new_grouped(
        schema,
        output,
        &keys,
        &kinds,
        &columns,
        &filters,
        merge != 0,
        final_output != 0,
    )
}

fn create_aggregate<'local>(
    env: &mut JNIEnv<'local>,
    schema_address: jlong,
    output_address: jlong,
    keys: Option<JIntArray<'local>>,
    kinds: JIntArray<'local>,
    columns: JIntArray<'local>,
    filters: JIntArray<'local>,
    merge: jboolean,
    final_output: jboolean,
    directories: JObjectArray<'local>,
    batch_rows: jint,
    task_name: JString<'local>,
) -> jlong {
    let directories = read_string_array(env, &directories);
    let task_name: String = env
        .get_string(&task_name)
        .expect("aggregate task name")
        .into();
    let result = (|| {
        let plan = read_aggregate_plan(
            env,
            schema_address,
            output_address,
            keys,
            kinds,
            columns,
            filters,
            merge,
            final_output,
        )?;
        if directories.is_empty() || batch_rows <= 0 {
            return datafusion::common::exec_err!(
                "bounded aggregate needs host spill directories and a positive batch size"
            );
        }
        let context = crate::batch::task::bounded_context(
            directories,
            &task_name,
            batch_rows as usize,
            64 * 1024,
        )?;
        let _runtime = runtime().enter();
        plan.execute(context)
    })();
    match result {
        Ok(task) => into_handle(task),
        Err(error) => {
            crate::batch::task::throw_error(env, error);
            0
        }
    }
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
        run_plan_with_limit(plan, batches, 2 * 1024 * 1024, 8192).await
    }

    async fn run_plan_with_limit(
        plan: AggregatePlan,
        batches: Vec<RecordBatch>,
        limit: usize,
        batch_rows: usize,
    ) -> Vec<Vec<ScalarValue>> {
        use datafusion::execution::config::SessionConfig;
        use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
        let directory = tempfile::tempdir().unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let context = Arc::new(
            TaskContext::default()
                .with_runtime(
                    RuntimeEnvBuilder::new()
                        .with_memory_pool(pool.clone())
                        .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(
                            DiskManagerMode::Directories(vec![directory.path().to_owned()]),
                        ))
                        .build_arc()
                        .unwrap(),
                )
                .with_session_config(SessionConfig::new().with_batch_size(batch_rows)),
        );
        let mut task = plan.execute(context).unwrap();
        for batch in batches {
            task.push(batch).unwrap();
            loop {
                match task.next_event().await.unwrap() {
                    Some(crate::batch::exec::ExecutionEvent::InputNeeded(_)) => break,
                    None => {}
                    _ => panic!("global aggregate emitted before EOF"),
                }
            }
        }
        task.finish().unwrap();
        let mut values = Vec::new();
        loop {
            match task.next_event().await.unwrap() {
                Some(crate::batch::exec::ExecutionEvent::Output(batch)) => {
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
                Some(crate::batch::exec::ExecutionEvent::Finished) => break,
                None => {}
                _ => panic!("input demand after EOF"),
            }
        }
        drop(task);
        assert_eq!(pool.reserved(), 0);
        values
    }

    #[test]
    fn bounded_decimal_average_preserves_local_states_rounding_and_sticky_overflow() {
        runtime().block_on(async {
            let input = Arc::new(Schema::new(vec![Field::new(
                "v",
                DataType::Decimal128(38, 2),
                true,
            )]));
            let partial = Arc::new(Schema::new(vec![
                Field::new("sum", DataType::Decimal128(38, 2), true),
                Field::new("count", DataType::Int64, true),
            ]));
            let output = Arc::new(Schema::new(vec![Field::new(
                "avg",
                DataType::Decimal128(38, 6),
                true,
            )]));
            for (values, expected) in [
                (vec![None, None], None),
                (vec![Some(100), None, Some(101), Some(101)], Some(1_006_667)),
                (vec![Some(-100), Some(-101), Some(-101)], Some(-1_006_667)),
                (vec![Some(10_i128.pow(38) - 1), Some(1), Some(2)], None),
            ] {
                let batch = RecordBatch::try_new(
                    input.clone(),
                    vec![Arc::new(
                        Decimal128Array::from(values)
                            .with_precision_and_scale(38, 2)
                            .unwrap(),
                    )],
                )
                .unwrap();
                let single = AggregatePlan::try_new(
                    input.clone(),
                    output.clone(),
                    &[4],
                    &[0],
                    &[-1],
                    false,
                    true,
                )
                .unwrap();
                let expected = vec![vec![ScalarValue::Decimal128(expected, 38, 6)]];
                assert_eq!(run_plan(single, vec![batch.clone()]).await, expected);
                let local = AggregatePlan::try_new(
                    input.clone(),
                    partial.clone(),
                    &[4],
                    &[0],
                    &[-1],
                    false,
                    false,
                )
                .unwrap();
                let states = run_plan(local, vec![batch]).await;
                let state = &states[0];
                let batch = RecordBatch::try_new(
                    partial.clone(),
                    state.iter().map(|s| s.to_array().unwrap()).collect(),
                )
                .unwrap();
                let final_plan = AggregatePlan::try_new(
                    partial.clone(),
                    output.clone(),
                    &[4],
                    &[0],
                    &[-1],
                    true,
                    true,
                )
                .unwrap();
                assert_eq!(run_plan(final_plan, vec![batch]).await, expected);
            }
            let final_plan =
                AggregatePlan::try_new(partial, output, &[4], &[0], &[-1], true, true).unwrap();
            assert_eq!(
                run_plan(final_plan, vec![]).await,
                vec![vec![ScalarValue::Decimal128(None, 38, 6)]]
            );
        });
    }

    #[test]
    fn bounded_decimal_sum_reuses_host_overflow_and_partial_state_contract() {
        runtime().block_on(async {
            let input = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Decimal128(38, 2),
                true,
            )]));
            for values in [
                vec![],
                vec![None],
                vec![Some(1), None, Some(2)],
                vec![Some(10_i128.pow(38) - 1), Some(1), Some(2)],
            ] {
                let expected = match values.len() {
                    0 | 1 => None,
                    3 if values[0] == Some(1) => Some(3),
                    _ => Some(2),
                };
                let batches = if values.is_empty() {
                    vec![]
                } else {
                    vec![RecordBatch::try_new(
                        input.clone(),
                        vec![Arc::new(
                            arrow::array::Decimal128Array::from(values)
                                .with_precision_and_scale(38, 2)
                                .unwrap(),
                        )],
                    )
                    .unwrap()]
                };
                let make = |merge, final_output| {
                    AggregatePlan::try_new(
                        input.clone(),
                        input.clone(),
                        &[0],
                        &[0],
                        &[-1],
                        merge,
                        final_output,
                    )
                    .unwrap()
                };
                assert_eq!(
                    run_plan(make(false, true), batches.clone()).await,
                    vec![vec![ScalarValue::Decimal128(expected, 38, 2)]]
                );
                let states = run_plan(make(false, false), batches)
                    .await
                    .into_iter()
                    .map(|row| {
                        RecordBatch::try_new(
                            input.clone(),
                            vec![row[0].to_array_of_size(1).unwrap()],
                        )
                        .unwrap()
                    })
                    .collect();
                assert_eq!(
                    run_plan(make(true, true), states).await,
                    vec![vec![ScalarValue::Decimal128(expected, 38, 2)]]
                );
            }
        });
    }

    #[test]
    fn bounded_grouped_reductions_spill_and_replay_under_the_same_pool() {
        runtime().block_on(async {
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, true),
                Field::new("value", DataType::Int8, true),
            ]));
            let output = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, true),
                Field::new("sum", DataType::Int8, true),
                Field::new("count", DataType::Int64, true),
                Field::new("avg", DataType::Int8, true),
            ]));
            let batches: Vec<_> = (0..160)
                .map(|batch| {
                    let keys: Vec<_> = (0..128)
                        .map(|row| ((batch * 128 + row) % 10000) as i64)
                        .collect();
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(Int64Array::from(keys.clone())),
                            Arc::new(Int8Array::from(
                                keys.iter()
                                    .map(|key| (*key % 127) as i8)
                                    .collect::<Vec<_>>(),
                            )),
                        ],
                    )
                    .unwrap()
                })
                .collect();
            let plan = AggregatePlan::try_new_grouped(
                schema,
                output,
                &[0],
                &[0, 1, 4],
                &[1, 1, 1],
                &[-1; 3],
                false,
                true,
            )
            .unwrap();
            let observed = plan.aggregate.clone();
            let result = run_plan_with_limit(plan, batches, 256 * 1024, 128).await;
            assert_eq!(result.len(), 10000);
            let mut seen = std::collections::HashSet::new();
            for row in result {
                let ScalarValue::Int64(Some(key)) = row[0] else {
                    panic!("missing key")
                };
                assert!(seen.insert(key));
                let count = if key < 480 { 3 } else { 2 };
                assert_eq!(row[1], ScalarValue::Int8(Some(((key % 127) * count) as i8)));
                assert_eq!(row[2], ScalarValue::Int64(Some(count)));
                assert_eq!(row[3], ScalarValue::Int8(Some((key % 127) as i8)));
            }
            let metrics = observed.metrics().unwrap();
            assert!(metrics.spill_count().unwrap() > 0);
            assert!(metrics.spilled_bytes().unwrap() > 0);
        });
    }

    #[test]
    fn bounded_grouping_handles_null_keys_empty_input_and_distinct() {
        runtime().block_on(async {
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, true),
                Field::new("value", DataType::Int64, true),
            ]));
            let output = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, true),
                Field::new("sum", DataType::Int64, true),
                Field::new("count", DataType::Int64, true),
            ]));
            let make = || {
                AggregatePlan::try_new_grouped(
                    schema.clone(),
                    output.clone(),
                    &[0],
                    &[0, 1],
                    &[1, 1],
                    &[-1; 2],
                    false,
                    true,
                )
                .unwrap()
            };
            assert!(run_plan(make(), vec![]).await.is_empty());
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![None, Some(1), None, Some(2)])),
                    Arc::new(Int64Array::from(vec![Some(3), None, Some(4), Some(5)])),
                ],
            )
            .unwrap();
            let mut actual = run_plan(make(), vec![batch.clone()]).await;
            actual.sort_by_key(|row| format!("{:?}", row[0]));
            let mut expected = vec![
                vec![
                    ScalarValue::Int64(None),
                    ScalarValue::Int64(Some(7)),
                    ScalarValue::Int64(Some(2)),
                ],
                vec![
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(None),
                    ScalarValue::Int64(Some(0)),
                ],
                vec![
                    ScalarValue::Int64(Some(2)),
                    ScalarValue::Int64(Some(5)),
                    ScalarValue::Int64(Some(1)),
                ],
            ];
            expected.sort_by_key(|row| format!("{:?}", row[0]));
            assert_eq!(actual, expected);
            let distinct = AggregatePlan::try_new_grouped(
                schema.clone(),
                Arc::new(Schema::new(vec![schema.field(0).clone()])),
                &[0],
                &[],
                &[],
                &[],
                false,
                true,
            )
            .unwrap();
            assert_eq!(run_plan(distinct, vec![batch]).await.len(), 3);
        });
    }

    #[test]
    fn bounded_fixed_extrema_keep_nulls_filters_and_partial_schema() {
        runtime().block_on(async {
            for (low, high, middle) in [
                (
                    ScalarValue::Boolean(Some(false)),
                    ScalarValue::Boolean(Some(true)),
                    ScalarValue::Boolean(Some(false)),
                ),
                (
                    ScalarValue::Decimal128(Some(-9999999999), 10, 2),
                    ScalarValue::Decimal128(Some(9999999999), 10, 2),
                    ScalarValue::Decimal128(Some(0), 10, 2),
                ),
                (
                    ScalarValue::Decimal128(Some(-(10_i128.pow(38) - 1)), 38, 18),
                    ScalarValue::Decimal128(Some(10_i128.pow(38) - 1), 38, 18),
                    ScalarValue::Decimal128(Some(0), 38, 18),
                ),
                (
                    ScalarValue::Date32(Some(-719528)),
                    ScalarValue::Date32(Some(2932896)),
                    ScalarValue::Date32(Some(0)),
                ),
                (
                    ScalarValue::Time32Millisecond(Some(0)),
                    ScalarValue::Time32Millisecond(Some(86399999)),
                    ScalarValue::Time32Millisecond(Some(12345)),
                ),
            ] {
                let data_type = low.data_type();
                let null = ScalarValue::try_from(&data_type).unwrap();
                let schema = Arc::new(Schema::new(vec![
                    Field::new("value", data_type.clone(), true),
                    Field::new("keep", DataType::Boolean, true),
                ]));
                let output = Arc::new(Schema::new(vec![
                    Field::new("min", data_type.clone(), true),
                    Field::new("max", data_type.clone(), true),
                    Field::new("filtered_min", data_type.clone(), true),
                    Field::new("filtered_max", data_type, true),
                    Field::new("count", DataType::Int64, true),
                ]));
                for scenario in 0..4 {
                    let empty = scenario == 1;
                    let no_values = scenario == 2;
                    let rejected = scenario == 3;
                    let batches = if empty {
                        vec![]
                    } else {
                        vec![RecordBatch::try_new(
                            schema.clone(),
                            vec![
                                ScalarValue::iter_to_array(if no_values {
                                    vec![null.clone(); 4]
                                } else {
                                    vec![low.clone(), null.clone(), high.clone(), middle.clone()]
                                })
                                .unwrap(),
                                Arc::new(BooleanArray::from(if rejected {
                                    vec![Some(false); 4]
                                } else {
                                    vec![Some(false), Some(true), Some(true), None]
                                })),
                            ],
                        )
                        .unwrap()]
                    };
                    let expected = if empty || no_values {
                        vec![
                            null.clone(),
                            null.clone(),
                            null.clone(),
                            null.clone(),
                            ScalarValue::Int64(Some(0)),
                        ]
                    } else {
                        vec![
                            low.clone(),
                            high.clone(),
                            if rejected { null.clone() } else { high.clone() },
                            if rejected { null.clone() } else { high.clone() },
                            ScalarValue::Int64(Some(if rejected { 0 } else { 1 })),
                        ]
                    };
                    let make = |final_output| {
                        AggregatePlan::try_new(
                            schema.clone(),
                            output.clone(),
                            &[2, 3, 2, 3, 1],
                            &[0, 0, 0, 0, 0],
                            &[-1, -1, 1, 1, 1],
                            false,
                            final_output,
                        )
                        .unwrap()
                    };
                    assert_eq!(
                        run_plan(make(true), batches.clone()).await,
                        vec![expected.clone()]
                    );
                    let partial = run_plan(make(false), batches).await;
                    assert_eq!(partial.is_empty(), empty);
                    let states = partial
                        .into_iter()
                        .map(|row| {
                            RecordBatch::try_new(
                                output.clone(),
                                row.into_iter()
                                    .map(|v| v.to_array_of_size(1).unwrap())
                                    .collect(),
                            )
                            .unwrap()
                        })
                        .collect();
                    let merge = AggregatePlan::try_new(
                        output.clone(),
                        output.clone(),
                        &[2, 3, 2, 3, 1],
                        &[0, 1, 2, 3, 4],
                        &[-1; 5],
                        true,
                        true,
                    )
                    .unwrap();
                    assert_eq!(run_plan(merge, states).await, vec![expected]);
                }
            }
        });
    }

    #[test]
    fn bounded_time_extrema_normalize_before_ordering_with_host_wrapping() {
        runtime().block_on(async {
            for (data_type, values, normalized) in [
                (
                    DataType::Time32(TimeUnit::Second),
                    vec![i32::MAX as i64, 1, -1],
                    vec![i32::MAX.wrapping_mul(1000), 1000, -1000],
                ),
                (
                    DataType::Time64(TimeUnit::Microsecond),
                    vec![i64::MAX, 1_234_999, -1_234_999],
                    vec![(i64::MAX / 1000) as i32, 1234, -1234],
                ),
                (
                    DataType::Time64(TimeUnit::Nanosecond),
                    vec![i64::MAX, 1_234_999_999, -1_234_999_999],
                    vec![(i64::MAX / 1_000_000) as i32, 1234, -1234],
                ),
            ] {
                let values = match data_type {
                    DataType::Time32(_) => Arc::new(arrow::array::Time32SecondArray::from(vec![
                        None,
                        Some(values[0] as i32),
                        Some(values[1] as i32),
                        Some(values[2] as i32),
                    ])) as ArrayRef,
                    DataType::Time64(TimeUnit::Microsecond) => {
                        Arc::new(arrow::array::Time64MicrosecondArray::from(vec![
                            None,
                            Some(values[0]),
                            Some(values[1]),
                            Some(values[2]),
                        ]))
                    }
                    _ => Arc::new(arrow::array::Time64NanosecondArray::from(vec![
                        None,
                        Some(values[0]),
                        Some(values[1]),
                        Some(values[2]),
                    ])),
                };
                let schema = Arc::new(Schema::new(vec![Field::new("value", data_type, true)]));
                let output = Arc::new(Schema::new(vec![
                    Field::new("min", DataType::Time32(TimeUnit::Millisecond), true),
                    Field::new("max", DataType::Time32(TimeUnit::Millisecond), true),
                ]));
                let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
                let plan = AggregatePlan::try_new(
                    schema,
                    output,
                    &[2, 3],
                    &[0, 0],
                    &[-1, -1],
                    false,
                    true,
                )
                .unwrap();
                assert_eq!(
                    run_plan(plan, vec![batch]).await,
                    vec![vec![
                        ScalarValue::Time32Millisecond(normalized.iter().copied().min()),
                        ScalarValue::Time32Millisecond(normalized.iter().copied().max())
                    ]]
                );
            }
        });
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
