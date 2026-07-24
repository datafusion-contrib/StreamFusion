use crate::*;

/// An empty batch (no schema), the joiner's "nothing to emit" result.
pub(crate) fn empty_batch() -> RecordBatch {
    RecordBatch::new_empty(Arc::new(Schema::empty()))
}

pub(crate) const ROW_ID_COLUMN: &str = "__rowid__";

/// A data schema plus a trailing non-null `__rowid__` Int64 field.
pub(crate) fn with_rowid_schema(data: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = data.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(ROW_ID_COLUMN, DataType::Int64, false));
    Arc::new(Schema::new(fields))
}

/// Appends a monotonic Int64 `__rowid__` column to a data batch, advancing the counter.
pub(crate) fn append_rowids(batch: &RecordBatch, next_id: &mut i64) -> RecordBatch {
    let n = batch.num_rows() as i64;
    let ids = Int64Array::from((0..n).map(|i| *next_id + i).collect::<Vec<_>>());
    *next_id += n;
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(ids));
    RecordBatch::try_new(with_rowid_schema(&batch.schema()), columns).expect("append rowids")
}

/// All-null columns of the given types, `n` rows each — to null-pad the absent side of an outer row.
pub(crate) fn null_columns(types: &[DataType], n: usize) -> Vec<ArrayRef> {
    types.iter().map(|t| new_null_array(t, n)).collect()
}

/// Null-pads the rows of a closed window-join side whose transient row-id (== row index) is not in
/// `matched`, or None when every row matched. Used by the window join, where a window's rows all close
/// together so match state is known within the flush.
pub(crate) fn unmatched_null_pad(
    rows: &RecordBatch,
    matched: &HashSet<i64>,
    left_types: &[DataType],
    right_types: &[DataType],
    is_left: bool,
) -> Option<RecordBatch> {
    let mask: BooleanArray =
        (0..rows.num_rows()).map(|i| Some(!matched.contains(&(i as i64)))).collect();
    let unmatched = filter_record_batch(rows, &mask).expect("filter unmatched window rows");
    if unmatched.num_rows() == 0 {
        None
    } else {
        Some(build_null_pad(&unmatched, left_types, right_types, is_left))
    }
}

/// Builds null-padded output `[left data.., right data..]` (columns `c0..`) for the rows of one side:
/// that side's first `left_types.len()`/`right_types.len()` columns of `rows` (any trailing `__rowid__`
/// ignored) beside all-null columns for the other side.
pub(crate) fn build_null_pad(
    rows: &RecordBatch,
    left_types: &[DataType],
    right_types: &[DataType],
    is_left: bool,
) -> RecordBatch {
    let n = rows.num_rows();
    let data_arity = if is_left { left_types.len() } else { right_types.len() };
    let data: Vec<ArrayRef> = (0..data_arity).map(|i| rows.column(i).clone()).collect();
    let columns: Vec<ArrayRef> = if is_left {
        data.into_iter().chain(null_columns(right_types, n)).collect()
    } else {
        null_columns(left_types, n).into_iter().chain(data).collect()
    };
    let types: Vec<DataType> = left_types.iter().chain(right_types).cloned().collect();
    let fields: Vec<Field> =
        (0..types.len()).map(|j| Field::new(format!("c{j}"), types[j].clone(), true)).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("build null-pad")
}

/// The largest `__rowid__` across a side's buffered batches, or -1 if none.
pub(crate) fn max_rowid(buffered: &[RecordBatch]) -> i64 {
    buffered
        .iter()
        .flat_map(|b| {
            let rid = b.column(b.num_columns() - 1).as_any().downcast_ref::<Int64Array>().expect("rid");
            (0..rid.len()).map(|i| rid.value(i)).collect::<Vec<_>>()
        })
        .max()
        .unwrap_or(-1)
}

/// Renames a batch's fields to `c0..` (positional) so a joined batch with duplicate input field
/// names round-trips the C Data Interface cleanly — the downstream conversion is positional.
pub(crate) fn rename_positional(batch: &RecordBatch) -> RecordBatch {
    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| Field::new(format!("c{i}"), f.data_type().clone(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())
        .expect("failed to rename join output")
}

/// Runs an INNER hash join of `left` and `right` on the given equi-key column-index pairs, with an
/// optional residual filter, and returns the joined rows as one batch (left columns then right
/// columns, fields renamed `c0..`). Empty when nothing matches. We own the buffering, keying, and
/// eviction of join state; the match itself is delegated to DataFusion's `HashJoinExec` — the same
/// split Arroyo's joins use (it runs a DataFusion join plan over the batches it has buffered).
/// The join runs under the caller's `TaskContext`, so with a managed-memory budget attached its
/// transient working memory (the build side `HashJoinExec` reserves) draws on the operator's pool;
/// a denial fails the join with the budget remedy rather than growing unaccounted.
pub(crate) fn hash_join_inner(
    left: RecordBatch,
    right: RecordBatch,
    key_pairs: &[(usize, usize)],
    filter: Option<JoinFilter>,
    ctx: Arc<TaskContext>,
) -> Result<RecordBatch, DataFusionError> {
    let left_schema = left.schema();
    let right_schema = right.schema();
    let on: JoinOn = key_pairs
        .iter()
        .map(|&(l, r)| {
            let left_key: Arc<dyn PhysicalExpr> = Arc::new(Column::new(left_schema.field(l).name(), l));
            let right_key: Arc<dyn PhysicalExpr> =
                Arc::new(Column::new(right_schema.field(r).name(), r));
            (left_key, right_key)
        })
        .collect();
    let left_exec = MemorySourceConfig::try_new_exec(&[vec![left]], left_schema, None)
        .expect("failed to build left join input");
    let right_exec = MemorySourceConfig::try_new_exec(&[vec![right]], right_schema, None)
        .expect("failed to build right join input");
    let join = HashJoinExec::try_new(
        left_exec,
        right_exec,
        on,
        filter,
        &JoinType::Inner,
        None,
        PartitionMode::CollectLeft,
        // Null keys never match — Flink's INNER equi-join filters them (filterNulls).
        NullEquality::NullEqualsNothing,
        false,
    )
    .expect("failed to build hash join");
    let batches = runtime().block_on(collect(Arc::new(join), ctx)).map_err(|e| {
        match e.find_root() {
            DataFusionError::ResourcesExhausted(_) => DataFusionError::ResourcesExhausted(format!(
                "native join working memory exceeded the operator's managed-memory budget; raise \
                 taskmanager.memory.managed.size or the operator's managed-memory weight ({e})"
            )),
            _ => e,
        }
    })?;
    if batches.iter().all(|batch| batch.num_rows() == 0) {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }
    let schema = batches[0].schema();
    let joined = concat_batches(&schema, &batches).expect("failed to concat join output");
    Ok(rename_positional(&joined))
}

/// Builds the residual `JoinFilter` for a time-bounded join: the interval bounds (if any) AND the
/// residual non-equi predicate (if any), over the full joined `[left.., right..]` schema. Returns
/// None when neither is present. The intermediate schema is the joined schema (columns `c0..`), so a
/// predicate compiled against that schema and the interval bounds (referencing the two rowtime
/// columns by their joined index) share one filter; `column_indices` maps each joined column back to
/// its side. The interval bounds are `left.rt >= right.rt + lower AND left.rt <= right.rt + upper`,
/// expressed over the rowtime columns directly so the comparison works on the timestamp type.
pub(crate) fn residual_filter(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    interval: Option<(usize, usize, i64, i64)>,
    predicate: Option<&mut JoinPredicate>,
) -> Option<JoinFilter> {
    let left_n = left_schema.fields().len();
    let right_n = right_schema.fields().len();
    let intermediate = joined_schema(left_schema, right_schema);
    let mut conjuncts: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
    if let Some((left_rt, right_rt, lower, upper)) = interval {
        let right_type = right_schema.field(right_rt).data_type();
        conjuncts.push(interval_bounds_expr(&intermediate, left_rt, left_n + right_rt, right_type, lower, upper));
    }
    if let Some(predicate) = predicate {
        conjuncts.push(predicate.compiled(&intermediate));
    }
    if conjuncts.is_empty() {
        return None;
    }
    let expression = conjuncts
        .into_iter()
        .reduce(|a, b| binary(a, Operator::And, b, &intermediate).expect("failed to AND residual filter"))
        .expect("at least one conjunct");
    let column_indices = (0..left_n)
        .map(|i| ColumnIndex { index: i, side: JoinSide::Left })
        .chain((0..right_n).map(|i| ColumnIndex { index: i, side: JoinSide::Right }))
        .collect();
    Some(JoinFilter::new(expression, column_indices, intermediate))
}

/// A full input row carried as scalars, used as a multiset key in the updating join's state.
pub(crate) type JoinRow = Vec<ScalarValue>;

/// Reads a batch's data schema (every column except a trailing `$row_kind$`, if present).
pub(crate) fn data_schema(batch: &RecordBatch) -> SchemaRef {
    let arity = data_arity(batch);
    Arc::new(Schema::new(
        batch.schema().fields().iter().take(arity).map(|f| f.as_ref().clone()).collect::<Vec<_>>(),
    ))
}

/// One distinct stored row's bookkeeping: how many times it currently appears (`count`, ≥1 while
/// present — Flink's "appear-times") and its match-degree (`num_assoc`, the count of currently
/// associated rows on the other side). The degree is maintained only when this side is an outer side
/// (the outer input of LEFT/RIGHT/FULL, or the probe side of SEMI/ANTI); otherwise it stays `-1` and
/// is ignored — mirroring Flink's `OuterJoinRecordStateView` vs `JoinRecordStateView` and RisingWave's
/// optional degree table.
#[derive(Clone, Copy)]
pub(crate) struct RowMeta {
    pub(crate) count: i64,
    pub(crate) num_assoc: i32,
}

/// The join family the updating joiner runs. INNER carries no degree; LEFT/RIGHT/FULL maintain a
/// per-row degree on the outer side(s); SEMI/ANTI maintain a degree on the left (probe) side.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Semi,
    Anti,
}

impl JoinKind {
    pub(crate) fn from_code(code: i32) -> Self {
        match code {
            0 => JoinKind::Inner,
            1 => JoinKind::LeftOuter,
            2 => JoinKind::RightOuter,
            3 => JoinKind::FullOuter,
            4 => JoinKind::Semi,
            5 => JoinKind::Anti,
            other => panic!("unknown updating-join type code {other}"),
        }
    }

    pub(crate) fn left_is_outer(self) -> bool {
        matches!(self, JoinKind::LeftOuter | JoinKind::FullOuter)
    }

    pub(crate) fn right_is_outer(self) -> bool {
        matches!(self, JoinKind::RightOuter | JoinKind::FullOuter)
    }

    pub(crate) fn is_semi_anti(self) -> bool {
        matches!(self, JoinKind::Semi | JoinKind::Anti)
    }
}

/// One associated row gathered from the other side when probing for matches: a clone of the stored
/// row and the match-degree it carried at probe time (`-1` when the other side keeps no degree). The
/// degree is captured once per distinct row and shared across its copies, exactly as Flink's
/// `OuterJoinRecordStateViews` iterator reuses one `numOfAssociations` for a record's appear-times.
pub(crate) struct OuterRecord {
    pub(crate) record: ByteKey,
    pub(crate) num_assoc: i32,
}

/// A residual non-equi join predicate (Flink's `joinSpec.getNonEquiCondition()`), encoded in the same
/// pre-order form as the filter engine and evaluated over candidate `[left.., right..]` pairs. A pair
/// is a match only when the predicate is true (null ⇒ not a match, as a join condition treats it), so
/// it gates which rows feed the degree and the emitted output — mirroring Flink's `condition.apply`
/// filter inside the associated-records iterator. The compiled expression is built once against the
/// joined schema and cached.
pub(crate) struct JoinPredicate {
    pub(crate) kinds: Vec<i64>,
    pub(crate) payload: Vec<i64>,
    pub(crate) child_counts: Vec<i64>,
    pub(crate) longs: Vec<i64>,
    pub(crate) doubles: Vec<f64>,
    pub(crate) strings: Vec<Option<String>>,
    pub(crate) compiled: Option<Arc<dyn PhysicalExpr>>,
}

impl JoinPredicate {
    /// The physical predicate compiled against the joined `[left.., right..]` schema (columns `c0..`)
    /// its input refs index into, cached on first use. The schema is supplied by the caller because
    /// the time-bounded joins learn it from their input batches rather than at construction.
    fn compiled(&mut self, schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
        if let Some(expr) = &self.compiled {
            return expr.clone();
        }
        let df_schema = Arc::new(
            DFSchema::try_from(schema.as_ref().clone()).expect("failed to build join-predicate schema"),
        );
        let physical = compile_expr(
            schema,
            &df_schema,
            &self.kinds,
            &self.payload,
            &self.child_counts,
            &self.longs,
            &self.doubles,
            &self.strings,
            0,
        );
        self.compiled = Some(physical.clone());
        physical
    }

    /// Evaluates the predicate over candidate `[left.., right..]` rows carried as scalars (the temporal
    /// join keeps its state as `ScalarValue` rows). Builds the columnar batch then defers to
    /// {@link evaluate_batch}. The updating join, whose state is arrow-row bytes, assembles the batch
    /// itself and calls `evaluate_batch` directly — no scalar round-trip.
    pub(crate) fn evaluate(&mut self, schema: &SchemaRef, rows: &[JoinRow]) -> Vec<bool> {
        let types: Vec<DataType> = schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let columns: Vec<ArrayRef> = (0..types.len())
            .map(|j| scalars_to_array(rows.iter().map(|r| r[j].clone()).collect(), &types[j]))
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .expect("failed to build join-predicate batch");
        self.evaluate_batch(schema, &batch)
    }

    /// Evaluates the compiled predicate over a candidate `[left.., right..]` batch the caller has
    /// assembled columnar (laid out by `schema`), returning one boolean per row (a null result is
    /// `false` — not a match). Batch-in avoids materializing the candidate rows as `ScalarValue`s.
    pub(crate) fn evaluate_batch(&mut self, schema: &SchemaRef, batch: &RecordBatch) -> Vec<bool> {
        let predicate = self.compiled(schema);
        let evaluated = predicate
            .evaluate(batch)
            .expect("failed to evaluate join predicate")
            .into_array(batch.num_rows())
            .expect("failed to materialize join predicate");
        let mask =
            evaluated.as_any().downcast_ref::<BooleanArray>().expect("join predicate must be boolean");
        (0..mask.len()).map(|i| mask.is_valid(i) && mask.value(i)).collect()
    }
}

/// Regular (non-windowed) equi-join over a changelog, the "updating join" — INNER, LEFT/RIGHT/FULL
/// outer, and SEMI/ANTI. Unlike the time-bounded interval/window joins (which buffer a batch and
/// delegate the match to a DataFusion hash join), this keeps a per-side keyed multiset of live rows
/// and probes it incrementally per input row, because retract correctness needs per-row counts a
/// batch join does not give. This is how the standalone streaming engines do it (RisingWave's
/// `JoinHashMap` + optional degree table, Proton's `MemoryHashJoin`; see divergences/14).
///
/// The output the operator must reproduce is Flink's collapsed changelog. The per-element state
/// machine is a faithful port of `StreamingJoinOperator` (INNER/outer) and
/// `StreamingSemiAntiJoinOperator` (semi/anti): on an accumulate/retract row the arriving side emits
/// one output per matching row on the other side (repeated by that row's multiset count) carrying the
/// input `RowKind`, and — when a side is outer — emits or retracts null-padded rows as a row's degree
/// crosses 0↔1, tracking that degree on the outer side's stored rows. State grows until rows are
/// retracted (no time eviction). The emitted batch is `[left cols.., right cols..]` (inner/outer) or
/// `[left cols..]` (semi/anti) plus the `$row_kind$` byte column.
/// An arrow-row codec over every column of a schema (value encoding, default order — used to store and
/// rebuild full rows, not to compare them).
pub(crate) fn payload_converter(schema: &SchemaRef) -> RowConverter {
    RowConverter::new(schema.fields().iter().map(|f| SortField::new(f.data_type().clone())).collect())
        .expect("payload converter")
}

/// Value-encodes a single all-null row for `schema` (the outer-join null pad), as arrow-row bytes.
pub(crate) fn encode_null_row(conv: &RowConverter, schema: &SchemaRef) -> OwnedRow {
    let columns: Vec<ArrayRef> =
        schema.fields().iter().map(|f| arrow::array::new_null_array(f.data_type(), 1)).collect();
    conv.convert_columns(&columns).expect("encode null row").row(0).owned()
}

/// Reads the encoded residual non-equi join predicate (empty `kinds` ⇒ no predicate). It compiles
/// lazily against the joined `[left.., right..]` schema supplied at evaluation time.
pub(crate) fn read_join_predicate(
    env: &mut JNIEnv,
    kinds: &JIntArray,
    payload: &JIntArray,
    child_counts: &JIntArray,
    longs: &JLongArray,
    doubles: &JDoubleArray,
    strings: &JObjectArray,
) -> Option<JoinPredicate> {
    let kinds = read_int_array(env, kinds);
    if kinds.is_empty() {
        return None;
    }
    Some(JoinPredicate {
        kinds,
        payload: read_int_array(env, payload),
        child_counts: read_int_array(env, child_counts),
        longs: read_longs(env, longs),
        doubles: read_doubles(env, doubles),
        strings: read_strings(env, strings),
        compiled: None,
    })
}
