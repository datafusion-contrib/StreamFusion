use crate::*;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::physical_expr::utils::collect_columns;

/// Runs a filter as a full DataFusion plan over a batch from the JVM, keeping rows whose int32
/// column exceeds `threshold`, and exports the surviving column back.
///
/// Unlike a bare expression, a plan executes asynchronously and yields a stream of batches, so a
/// control-plane thread blocks on the shared runtime and pulls that stream to completion. This is
/// the drive model every stateful operator will use; a filter is the simplest plan that exercises
/// it because it actually changes the row count.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_filterGreaterThan<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    threshold: jint,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let ffi_array = unsafe {
            std::ptr::replace(
                in_array_address as *mut FFI_ArrowArray,
                FFI_ArrowArray::empty(),
            )
        };
        let ffi_schema = unsafe {
            std::ptr::replace(
                in_schema_address as *mut FFI_ArrowSchema,
                FFI_ArrowSchema::empty(),
            )
        };

        let field = Field::try_from(&ffi_schema).expect("failed to import Arrow field");
        let schema = Arc::new(Schema::new(vec![field]));
        let column_name = schema.field(0).name().clone();

        let mut data =
            unsafe { from_ffi(ffi_array, &ffi_schema) }.expect("failed to import Arrow array");
        data.align_buffers();
        let batch = RecordBatch::try_new(schema.clone(), vec![make_array(data)])
            .expect("failed to build batch");

        let result = runtime().block_on(async move {
            let ctx = SessionContext::new();
            let frame = ctx
                .read_batch(batch)
                .expect("failed to read batch")
                .filter(logical_col(&column_name).gt(logical_lit(threshold)))
                .expect("failed to build filter");
            let mut stream = frame
                .execute_stream()
                .await
                .expect("failed to execute plan");
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                batches.push(batch.expect("failed to pull batch"));
            }
            concat_batches(&schema, &batches).expect("failed to assemble result")
        });

        let out_data = result.column(0).to_data();
        let out_array = FFI_ArrowArray::new(&out_data);
        let out_schema =
            FFI_ArrowSchema::try_from(out_data.data_type()).expect("failed to export Arrow schema");
        unsafe {
            std::ptr::write(out_array_address as *mut FFI_ArrowArray, out_array);
            std::ptr::write(out_schema_address as *mut FFI_ArrowSchema, out_schema);
        }
    })
}

/// A compiled filter predicate held across batches: the decoded expression tree plus the physical
/// expression, which is built once against the first batch's schema and reused for every later
/// batch. This follows Comet — the plan is compiled once at operator construction, not re-planned
/// per batch — and evaluates the predicate directly (no per-batch `SessionContext` or async plan).
pub(crate) struct FilterExpression {
    pub(crate) kinds: Vec<i64>,
    pub(crate) payload: Vec<i64>,
    pub(crate) child_counts: Vec<i64>,
    pub(crate) longs: Vec<i64>,
    pub(crate) doubles: Vec<f64>,
    pub(crate) strings: Vec<Option<String>>,
    pub(crate) compiled: Option<Arc<dyn PhysicalExpr>>,
}

impl FilterExpression {
    /// The physical predicate, decoded and compiled against the schema on first use and cached.
    fn predicate(&mut self, schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
        if let Some(expr) = &self.compiled {
            return expr.clone();
        }
        let mut cursor = 0usize;
        let logical = build_expr(
            schema,
            &self.kinds,
            &self.payload,
            &self.child_counts,
            &self.longs,
            &self.doubles,
            &self.strings,
            &mut cursor,
        );
        let df_schema =
            Arc::new(DFSchema::try_from(schema.as_ref().clone()).expect("failed to build schema"));
        // Match the planner's logical pipeline: coerce operand types (e.g. an int column against a
        // bigint literal) before building the physical expression, which assumes coerced types.
        let context = SimplifyContext::builder()
            .with_schema(df_schema.clone())
            .build();
        let coerced = ExprSimplifier::new(context)
            .coerce(logical, df_schema.as_ref())
            .expect("failed to coerce predicate");
        let physical = create_physical_expr(&coerced, df_schema.as_ref(), &ExecutionProps::new())
            .expect("failed to compile predicate");
        self.compiled = Some(physical.clone());
        physical
    }

    /// Keeps the rows for which the predicate is true; a null result drops the row, as `WHERE` requires.
    pub(crate) fn filter(&mut self, batch: RecordBatch) -> RecordBatch {
        let predicate = self.predicate(&batch.schema());
        let evaluated = predicate
            .evaluate(&batch)
            .expect("failed to evaluate predicate")
            .into_array(batch.num_rows())
            .expect("failed to materialize predicate");
        let mask = evaluated
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("predicate must be boolean");
        filter_record_batch(&batch, mask).expect("failed to filter batch")
    }
}

/// Compiles a general predicate expression (the JVM's encoded tree) into a reusable handle. The
/// handle owns the compiled plan and must be released with `closeFilterExpression`.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createFilterExpression<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    kinds: JIntArray<'local>,
    payload: JIntArray<'local>,
    child_counts: JIntArray<'local>,
    longs: JLongArray<'local>,
    doubles: JDoubleArray<'local>,
    strings: JObjectArray<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let expression = FilterExpression {
            kinds: read_int_array(&env, &kinds),
            payload: read_int_array(&env, &payload),
            child_counts: read_int_array(&env, &child_counts),
            longs: read_longs(&env, &longs),
            doubles: read_doubles(&env, &doubles),
            strings: read_strings(&mut env, &strings),
            compiled: None,
        };
        into_handle(expression)
    })
}

/// Filters a batch from the JVM through a compiled predicate handle, exporting the surviving rows.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_filterExpression<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let expression = unsafe { &mut *(handle as *mut FilterExpression) };
        let batch = import_record_batch(in_array_address, in_schema_address);
        let result = expression.filter(batch);
        export_record_batch(result, out_array_address, out_schema_address);
    })
}

/// Releases a compiled predicate handle and its native state.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeFilterExpression<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<FilterExpression>(handle));
    })
}

/// The compiled form of a Calc: an optional filter predicate plus the projection expressions.
pub(crate) struct CompiledCalc {
    condition: Option<Arc<dyn PhysicalExpr>>,
    projections: Vec<Arc<dyn PhysicalExpr>>,
    pub(crate) projection_input_indices: Vec<usize>,
    input_schema: SchemaRef,
}

fn remap_projection_columns(
    projection: Arc<dyn PhysicalExpr>,
    input_indices: &[usize],
) -> Arc<dyn PhysicalExpr> {
    let remap: HashMap<usize, usize> = input_indices
        .iter()
        .enumerate()
        .map(|(projected, &original)| (original, projected))
        .collect();
    projection
        .transform_down(|expr| {
            if let Some(column) = expr.downcast_ref::<Column>() {
                let projected = remap
                    .get(&column.index())
                    .copied()
                    .expect("projection input column was not retained");
                return Ok(Transformed::yes(Arc::new(Column::new(
                    column.name(),
                    projected,
                ))));
            }
            Ok(Transformed::no(expr))
        })
        .data()
        .expect("failed to remap Calc projection columns")
}

/// A compiled Calc held across batches: an optional condition and a list of projection expressions
/// (each an encoded tree rooted in the shared pools), built once against the first batch's schema.
/// It filters rows by the condition, then evaluates each projection over the survivors to form the
/// output batch — the general form of the filter-plus-column-subset path, also covering computed
/// columns and constants.
pub(crate) struct CalcExpression {
    pub(crate) kinds: Vec<i64>,
    pub(crate) payload: Vec<i64>,
    pub(crate) child_counts: Vec<i64>,
    pub(crate) longs: Vec<i64>,
    pub(crate) doubles: Vec<f64>,
    pub(crate) strings: Vec<Option<String>>,
    pub(crate) projection_roots: Vec<usize>,
    pub(crate) condition_root: i64,
    pub(crate) output_names: Vec<String>,
    pub(crate) compiled: Option<CompiledCalc>,
}

impl CalcExpression {
    fn compiled(&mut self, schema: &SchemaRef) -> &CompiledCalc {
        if self.compiled.is_none() {
            let df_schema =
                DFSchema::try_from(schema.as_ref().clone()).expect("failed to build schema");
            let compile = |root: usize| {
                compile_expr(
                    schema,
                    &df_schema,
                    &self.kinds,
                    &self.payload,
                    &self.child_counts,
                    &self.longs,
                    &self.doubles,
                    &self.strings,
                    root,
                )
            };
            let condition =
                (self.condition_root >= 0).then(|| compile(self.condition_root as usize));
            let projections: Vec<_> = self.projection_roots.iter().map(|&r| compile(r)).collect();
            let mut projection_input_indices: Vec<_> = projections
                .iter()
                .flat_map(collect_columns)
                .map(|column| column.index())
                .collect();
            if let Ok(row_kind) = schema.index_of(ROW_KIND_COLUMN) {
                projection_input_indices.push(row_kind);
            }
            projection_input_indices.sort_unstable();
            projection_input_indices.dedup();
            let projections = projections
                .into_iter()
                .map(|projection| remap_projection_columns(projection, &projection_input_indices))
                .collect();
            self.compiled = Some(CompiledCalc {
                condition,
                projections,
                projection_input_indices,
                input_schema: schema.clone(),
            });
        }
        self.compiled.as_ref().unwrap()
    }

    pub(crate) fn evaluate(&mut self, batch: RecordBatch) -> RecordBatch {
        let (condition, projections, projection_input_indices) = {
            let compiled = self.compiled(&batch.schema());
            (
                compiled.condition.clone(),
                compiled.projections.clone(),
                compiled.projection_input_indices.clone(),
            )
        };
        // Flink's generated Calc evaluates its condition first and constructs only the projected
        // row when it passes. Mirror that for a columnar batch: narrow to the projection's input
        // columns before applying the selection, instead of copying every column through
        // filter_record_batch and immediately discarding most of them.
        let projected = if projection_input_indices.is_empty() {
            RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                Vec::new(),
                &arrow::record_batch::RecordBatchOptions::new()
                    .with_row_count(Some(batch.num_rows())),
            )
            .expect("failed to preserve Calc row count without projection inputs")
        } else {
            batch
                .project(&projection_input_indices)
                .expect("failed to prune Calc projection inputs")
        };
        let filtered = match condition {
            Some(predicate) => {
                let evaluated = predicate
                    .evaluate(&batch)
                    .expect("failed to evaluate condition")
                    .into_array(batch.num_rows())
                    .expect("failed to materialize condition");
                let mask = evaluated
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("condition must be boolean");
                if mask.true_count() == mask.len() {
                    projected
                } else {
                    filter_record_batch(&projected, mask).expect("failed to filter batch")
                }
            }
            None => projected,
        };
        let rows = filtered.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(projections.len());
        let mut fields: Vec<Field> = Vec::with_capacity(projections.len());
        for (i, projection) in projections.iter().enumerate() {
            let array = projection
                .evaluate(&filtered)
                .expect("failed to evaluate projection")
                .into_array(rows)
                .expect("failed to materialize projection");
            fields.push(Field::new(
                &self.output_names[i],
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }
        // Carry the changelog tag through: a Calc transforms each row independently (per-row
        // projection, optional deterministic filter), so a `$row_kind$` column rides through unchanged
        // — filtered alongside the rows by the condition above. This makes the Calc changelog-safe,
        // matching the host's per-row Calc over a retracting stream.
        if let Some(kind) = filtered.column_by_name(ROW_KIND_COLUMN) {
            fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
            columns.push(kind.clone());
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build output")
    }
}

/// Compiles an encoded Calc (optional condition + projection trees) into a reusable handle, released
/// with `closeCalcExpression`.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createCalcExpression<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    kinds: JIntArray<'local>,
    payload: JIntArray<'local>,
    child_counts: JIntArray<'local>,
    longs: JLongArray<'local>,
    doubles: JDoubleArray<'local>,
    strings: JObjectArray<'local>,
    projection_roots: JIntArray<'local>,
    condition_root: jint,
    output_names: JObjectArray<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        capture_jvm(&env); // so a JvmUdf node in this Calc can upcall the JVM bridge at evaluation time
        let output_names = read_strings(&mut env, &output_names)
            .into_iter()
            .map(|s| s.expect("output name"))
            .collect();
        let expression = read_calc_expression(
            &mut env,
            &kinds,
            &payload,
            &child_counts,
            &longs,
            &doubles,
            &strings,
            &projection_roots,
            condition_root,
            output_names,
        );
        into_handle(expression)
    })
}

#[allow(clippy::too_many_arguments)]
fn read_calc_expression<'local>(
    env: &mut JNIEnv<'local>,
    kinds: &JIntArray<'local>,
    payload: &JIntArray<'local>,
    child_counts: &JIntArray<'local>,
    longs: &JLongArray<'local>,
    doubles: &JDoubleArray<'local>,
    strings: &JObjectArray<'local>,
    projection_roots: &JIntArray<'local>,
    condition_root: jint,
    output_names: Vec<String>,
) -> CalcExpression {
    CalcExpression {
        kinds: read_int_array(env, kinds),
        payload: read_int_array(env, payload),
        child_counts: read_int_array(env, child_counts),
        longs: read_longs(env, longs),
        doubles: read_doubles(env, doubles),
        strings: read_strings(env, strings),
        projection_roots: read_int_array(env, projection_roots)
            .into_iter()
            .map(|r| r as usize)
            .collect(),
        condition_root: condition_root as i64,
        output_names,
        compiled: None,
    }
}

/// The Arrow schema an encoded Calc would produce over `input`, without evaluating it: one field per
/// projection, typed by compiling the tree the way the operator will. A tree DataFusion cannot
/// compile, or a condition that is not boolean, is reported instead — either would otherwise fail
/// the task on its first batch.
pub(crate) fn infer_calc_output_schema(
    input: &SchemaRef,
    expression: &CalcExpression,
) -> Result<Schema, String> {
    let infer = |root: usize| {
        infer_expr_type(
            input,
            &expression.kinds,
            &expression.payload,
            &expression.child_counts,
            &expression.longs,
            &expression.doubles,
            &expression.strings,
            root,
        )
    };
    if expression.condition_root >= 0 {
        match infer(expression.condition_root as usize) {
            Err(e) => return Err(format!("condition does not compile natively: {e}")),
            Ok(DataType::Boolean) => {}
            Ok(other) => {
                return Err(format!(
                    "condition evaluates natively as {other} rather than Boolean"
                ))
            }
        }
    }
    let fields = expression
        .projection_roots
        .iter()
        .zip(&expression.output_names)
        .map(|(root, name)| match infer(*root) {
            Ok(data_type) => Ok(Field::new(name, data_type, true)),
            Err(e) => Err(format!("projection `{name}` does not compile natively: {e}")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Schema::new(fields))
}

/// Plan-time view of an encoded Calc's output; see [`infer_calc_output_schema`]. The input schema
/// arrives, and the inferred schema leaves, through the C Data Interface (schemas only, no data).
/// Returns null once the output schema is written, else the reason no schema could be inferred.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_inferCalcOutputSchema<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    kinds: JIntArray<'local>,
    payload: JIntArray<'local>,
    child_counts: JIntArray<'local>,
    longs: JLongArray<'local>,
    doubles: JDoubleArray<'local>,
    strings: JObjectArray<'local>,
    projection_roots: JIntArray<'local>,
    condition_root: jint,
    output_names: JObjectArray<'local>,
    input_schema_address: jlong,
    output_schema_address: jlong,
) -> jstring {
    crate::bridge::jni_guard(env, move |env| {
        let input = import_schema(input_schema_address);
        let output_names = read_strings(env, &output_names)
            .into_iter()
            .map(|s| s.expect("output name"))
            .collect();
        let expression = read_calc_expression(
            env,
            &kinds,
            &payload,
            &child_counts,
            &longs,
            &doubles,
            &strings,
            &projection_roots,
            condition_root,
            output_names,
        );
        match infer_calc_output_schema(&input, &expression) {
            Ok(schema) => {
                export_schema(&schema, output_schema_address);
                std::ptr::null_mut()
            }
            Err(reason) => env
                .new_string(reason)
                .expect("inference failure string")
                .into_raw(),
        }
    })
}

/// Runs a batch from the JVM through a compiled Calc handle, exporting the projected output batch.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_calcExpression<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let expression = unsafe { &mut *(handle as *mut CalcExpression) };
        let batch = import_record_batch(in_array_address, in_schema_address);
        let result = expression.evaluate(batch);
        export_record_batch(result, out_array_address, out_schema_address);
    })
}

/// Runs a later batch through a Calc after the first call established its immutable input schema.
/// Only the ArrowArray crosses the boundary; the compiled handle supplies the cached struct type.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_calcExpressionArray<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let expression = unsafe { &mut *(handle as *mut CalcExpression) };
        let schema = expression
            .compiled
            .as_ref()
            .expect("Calc schema must be established by the first batch")
            .input_schema
            .clone();
        let batch = import_record_batch_with_schema(in_array_address, &schema);
        let result = expression.evaluate(batch);
        export_record_batch(result, out_array_address, out_schema_address);
    })
}

/// Releases a compiled Calc handle and its native state.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeCalcExpression<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<CalcExpression>(handle));
    })
}
