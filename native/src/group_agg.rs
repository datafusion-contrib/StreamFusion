use crate::*;

/// Total ordering over f64 so a MIN/MAX value multiset can be a `BTreeMap` (floats compared by
/// `total_cmp`); a given aggregate's column has no NaN in practice, so the tie-break is moot.
#[derive(Clone, Copy, PartialEq)]
pub(crate) struct OrdF64(f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// A MIN/MAX value used as an ordered multiset key. Each aggregate only ever stores one variant (its
/// fixed value type), so the derived cross-variant ordering is never exercised.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MinMaxKey {
    I64(i64),
    I32(i32),
    F64(OrdF64),
    // A DECIMAL extreme as its unscaled i128. All values of one aggregate share a scale, so ordering
    // the raw i128 is the decimal ordering; the precision/scale are restored from the result type on
    // emit (the key carries only the value, since the fold path has no precision/scale).
    Decimal128(i128),
    // A string extreme. Rust's String ordering is UTF-8 byte-lexicographic, matching Flink's
    // BinaryStringData byte comparison (its common binary path) — see divergences/07 for the
    // supplementary-plane edge where Flink's materialized-Java-object path would differ.
    Str(String),
}

impl MinMaxKey {
    fn of(num: Num) -> Self {
        match num {
            Num::I64(v) => MinMaxKey::I64(v),
            Num::I32(v) => MinMaxKey::I32(v),
            Num::F64(v) => MinMaxKey::F64(OrdF64(v)),
            Num::I128(v) => MinMaxKey::Decimal128(v),
            // The Extremes multiset (GROUP BY retractable MIN/MAX) is only built for the wider types
            // its matcher admits; narrow ints / 4-byte float reach MIN/MAX only via the OVER running
            // path (RunningAgg's narrow variants), never here.
            Num::I16(_) | Num::I8(_) | Num::F32(_) => {
                unreachable!("narrow MIN/MAX uses the running path, not the Extremes multiset")
            }
        }
    }

    fn from_scalar(scalar: &ScalarValue) -> Self {
        match scalar {
            ScalarValue::Int64(Some(v)) => MinMaxKey::I64(*v),
            ScalarValue::Int32(Some(v)) => MinMaxKey::I32(*v),
            ScalarValue::Float64(Some(v)) => MinMaxKey::F64(OrdF64(*v)),
            ScalarValue::Decimal128(Some(v), _, _) => MinMaxKey::Decimal128(*v),
            ScalarValue::Utf8(Some(v))
            | ScalarValue::LargeUtf8(Some(v))
            | ScalarValue::Utf8View(Some(v)) => MinMaxKey::Str(v.clone()),
            other => panic!("unexpected MIN/MAX value scalar: {other:?}"),
        }
    }

    /// Rebuilds the scalar; a decimal extreme takes its precision/scale from `result_type`.
    fn scalar(&self, result_type: &DataType) -> ScalarValue {
        match self {
            MinMaxKey::I64(v) => ScalarValue::Int64(Some(*v)),
            MinMaxKey::I32(v) => ScalarValue::Int32(Some(*v)),
            MinMaxKey::F64(v) => ScalarValue::Float64(Some(v.0)),
            MinMaxKey::Decimal128(v) => match result_type {
                DataType::Decimal128(p, s) => ScalarValue::Decimal128(Some(*v), *p, *s),
                other => panic!("decimal MIN/MAX result type must be Decimal128, got {other:?}"),
            },
            MinMaxKey::Str(v) => ScalarValue::Utf8(Some(v.clone())),
        }
    }
}

/// A distinct-value multiplicity map, specialized by the value column's type: a BIGINT column (the
/// common Nexmark shape — `COUNT(DISTINCT bidder)`) keys a plain `i64` map, so the per-row fold reads
/// the primitive straight off the array with no `ScalarValue` construction, boxed hash, or per-value
/// heap churn; any other type keys scalars as before. The q16 profile put ~half the group aggregate
/// in exactly that scalar construct/hash/drop traffic.
pub(crate) enum DistinctSet {
    I64(ahash::HashMap<i64, i64>),
    Scalar(ahash::HashMap<ScalarValue, i64>),
}

impl DistinctSet {
    fn new(value_type: &DataType) -> Self {
        match value_type {
            DataType::Int64 => DistinctSet::I64(ahash::HashMap::default()),
            _ => DistinctSet::Scalar(ahash::HashMap::default()),
        }
    }

    fn len(&self) -> usize {
        match self {
            DistinctSet::I64(m) => m.len(),
            DistinctSet::Scalar(m) => m.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Adds one occurrence; returns true when the value enters the set (first occurrence).
    fn add_i64(&mut self, value: i64) -> bool {
        match self {
            DistinctSet::I64(m) => {
                let count = m.entry(value).or_insert(0);
                *count += 1;
                *count == 1
            }
            DistinctSet::Scalar(_) => self.add_scalar(ScalarValue::Int64(Some(value))),
        }
    }

    /// Removes one occurrence; returns true when the value leaves the set (last occurrence).
    fn remove_i64(&mut self, value: i64) -> bool {
        match self {
            DistinctSet::I64(m) => {
                if let Some(count) = m.get_mut(&value) {
                    *count -= 1;
                    if *count <= 0 {
                        m.remove(&value);
                        return true;
                    }
                }
                false
            }
            DistinctSet::Scalar(_) => self.remove_scalar(&ScalarValue::Int64(Some(value))),
        }
    }

    fn add_scalar(&mut self, value: ScalarValue) -> bool {
        match self {
            DistinctSet::I64(_) => match value {
                ScalarValue::Int64(Some(v)) => self.add_i64(v),
                other => unreachable!("i64 distinct set fed a non-int64 scalar: {other:?}"),
            },
            DistinctSet::Scalar(m) => {
                let count = m.entry(value).or_insert(0);
                *count += 1;
                *count == 1
            }
        }
    }

    fn remove_scalar(&mut self, value: &ScalarValue) -> bool {
        match self {
            DistinctSet::I64(_) => match value {
                ScalarValue::Int64(Some(v)) => self.remove_i64(*v),
                other => unreachable!("i64 distinct set retracting a non-int64 scalar: {other:?}"),
            },
            DistinctSet::Scalar(m) => {
                if let Some(count) = m.get_mut(value) {
                    *count -= 1;
                    if *count <= 0 {
                        m.remove(value);
                        return true;
                    }
                }
                false
            }
        }
    }

    /// The live (value, multiplicity) pairs as scalars — the snapshot wire format, unchanged by the
    /// typed specialization.
    fn scalar_entries(&self) -> Vec<(ScalarValue, i64)> {
        match self {
            DistinctSet::I64(m) => {
                m.iter().map(|(v, c)| (ScalarValue::Int64(Some(*v)), *c)).collect()
            }
            DistinctSet::Scalar(m) => m.iter().map(|(v, c)| (v.clone(), *c)).collect(),
        }
    }

    /// Restores one snapshot entry with its multiplicity.
    fn insert_restored(&mut self, value: ScalarValue, count: i64) {
        match self {
            DistinctSet::I64(m) => match value {
                ScalarValue::Int64(Some(v)) => {
                    m.insert(v, count);
                }
                other => unreachable!("i64 distinct set restoring a non-int64 scalar: {other:?}"),
            },
            DistinctSet::Scalar(m) => {
                m.insert(value, count);
            }
        }
    }
}

/// Per-(key, aggregate) state. SUM and COUNT fold/retract a single running value (SUM also keeps a
/// non-null count so it reports NULL once fully retracted). MIN/MAX cannot be retracted from a single
/// value, so they keep a value→count multiset and read the extreme off its ends — what makes them
/// retractable (Flink's `*WithRetractAccumulator` uses a `MapView`; Arroyo calls this the batch state).
pub(crate) enum GroupAggState {
    Running { agg: RunningAgg, non_null: i64 },
    Extremes { is_min: bool, counts: BTreeMap<MinMaxKey, i64> },
    // COUNT(DISTINCT x): a value→multiplicity map (Flink's DistinctAccumulator MapView). The count is
    // the number of live entries; a value's multiplicity tracks how many input rows carry it so a
    // retraction removes it only when the last one is retracted. Nulls are never inserted.
    Distinct(DistinctSet),
    // SUM(DISTINCT x): the same value→multiplicity map plus a running SUM folded only when a value
    // enters the set and retracted only when its last occurrence leaves — Flink's DistinctAccumulator
    // wrapping the SUM accumulator, kept incremental so the emit stays O(1).
    DistinctRunning { counts: DistinctSet, agg: RunningAgg },
}

impl GroupAggState {
    fn new(kind: i64, value_type: &DataType) -> Self {
        match kind {
            1 => GroupAggState::Extremes { is_min: true, counts: BTreeMap::new() }, // MIN
            2 => GroupAggState::Extremes { is_min: false, counts: BTreeMap::new() }, // MAX
            7 => GroupAggState::Distinct(DistinctSet::new(value_type)), // COUNT(DISTINCT)
            // SUM(DISTINCT): the inner running aggregate is a plain SUM (kind 0) over the value type.
            9 => GroupAggState::DistinctRunning {
                counts: DistinctSet::new(value_type),
                agg: RunningAgg::new(0, value_type),
            },
            _ => GroupAggState::Running { agg: RunningAgg::new(kind, value_type), non_null: 0 },
        }
    }

    fn accumulate(&mut self, value: Num) {
        match self {
            GroupAggState::Running { agg, non_null } => {
                agg.fold(value);
                *non_null += 1;
            }
            GroupAggState::Extremes { counts, .. } => {
                *counts.entry(MinMaxKey::of(value)).or_insert(0) += 1;
            }
            GroupAggState::Distinct(_) | GroupAggState::DistinctRunning { .. } => {
                unreachable!("distinct folds a scalar, not a Num")
            }
        }
    }

    /// Folds one two-phase AVG partial pair: the pre-summed sum partial and the partial's non-null
    /// count (instead of +1 per row). The state is the ordinary AVG state, so the emit (divide,
    /// truncate, cast back) is byte-identical to the single-phase path.
    fn accumulate_merged(&mut self, value: Num, count: i64) {
        match self {
            GroupAggState::Running { agg, non_null } => {
                agg.fold(value);
                *non_null += count;
            }
            _ => unreachable!("merged accumulate on a non-running aggregate"),
        }
    }

    /// The changelog reversal of {@link accumulate_merged} (the global's input is insert-only today;
    /// kept symmetric with the other aggregate states).
    fn retract_merged(&mut self, value: Num, count: i64) {
        match self {
            GroupAggState::Running { agg, non_null } => {
                agg.retract(value);
                *non_null -= count;
            }
            _ => unreachable!("merged retract on a non-running aggregate"),
        }
    }

    fn retract(&mut self, value: Num) {
        match self {
            GroupAggState::Running { agg, non_null } => {
                agg.retract(value);
                *non_null -= 1;
            }
            GroupAggState::Extremes { counts, .. } => {
                let key = MinMaxKey::of(value);
                if let Some(count) = counts.get_mut(&key) {
                    *count -= 1;
                    if *count <= 0 {
                        counts.remove(&key);
                    }
                }
            }
            GroupAggState::Distinct(_) | GroupAggState::DistinctRunning { .. } => {
                unreachable!("distinct retracts a scalar, not a Num")
            }
        }
    }

    /// Adds one occurrence of a non-numeric (string) MIN/MAX extreme, read as a scalar rather than a
    /// Num (the Num path is numeric only). The multiset orders entries by `MinMaxKey`.
    fn accumulate_extreme(&mut self, value: ScalarValue) {
        match self {
            GroupAggState::Extremes { counts, .. } => {
                *counts.entry(MinMaxKey::from_scalar(&value)).or_insert(0) += 1;
            }
            _ => unreachable!("accumulate_extreme on a non-extremes aggregate"),
        }
    }

    /// Removes one occurrence of a string MIN/MAX extreme (the changelog retraction).
    fn retract_extreme(&mut self, value: ScalarValue) {
        match self {
            GroupAggState::Extremes { counts, .. } => {
                let key = MinMaxKey::from_scalar(&value);
                if let Some(count) = counts.get_mut(&key) {
                    *count -= 1;
                    if *count <= 0 {
                        counts.remove(&key);
                    }
                }
            }
            _ => unreachable!("retract_extreme on a non-extremes aggregate"),
        }
    }

    /// Adds one occurrence of a distinct value (COUNT/SUM DISTINCT); a value entering the set for the
    /// first time also folds into a distinct SUM's running aggregate — later duplicates don't.
    fn accumulate_distinct(&mut self, value: ScalarValue) {
        match self {
            GroupAggState::Distinct(counts) => {
                counts.add_scalar(value);
            }
            GroupAggState::DistinctRunning { counts, agg } => {
                let num = distinct_num(&value);
                if counts.add_scalar(value) {
                    agg.fold(num);
                }
            }
            _ => unreachable!("accumulate_distinct on a non-distinct aggregate"),
        }
    }

    /// The BIGINT fast path of {@link accumulate_distinct}: the value comes straight off the Int64
    /// array, no scalar is built.
    fn accumulate_distinct_i64(&mut self, value: i64) {
        match self {
            GroupAggState::Distinct(counts) => {
                counts.add_i64(value);
            }
            GroupAggState::DistinctRunning { counts, agg } => {
                if counts.add_i64(value) {
                    agg.fold(Num::I64(value));
                }
            }
            _ => unreachable!("accumulate_distinct on a non-distinct aggregate"),
        }
    }

    /// Removes one occurrence; the value leaves the distinct set when its last occurrence is retracted
    /// (which is also when a distinct SUM's running aggregate retracts it).
    fn retract_distinct(&mut self, value: ScalarValue) {
        match self {
            GroupAggState::Distinct(counts) => {
                counts.remove_scalar(&value);
            }
            GroupAggState::DistinctRunning { counts, agg } => {
                if counts.remove_scalar(&value) {
                    agg.retract(distinct_num(&value));
                }
            }
            _ => unreachable!("retract_distinct on a non-distinct aggregate"),
        }
    }

    /// The BIGINT fast path of {@link retract_distinct}.
    fn retract_distinct_i64(&mut self, value: i64) {
        match self {
            GroupAggState::Distinct(counts) => {
                counts.remove_i64(value);
            }
            GroupAggState::DistinctRunning { counts, agg } => {
                if counts.remove_i64(value) {
                    agg.retract(Num::I64(value));
                }
            }
            _ => unreachable!("retract_distinct on a non-distinct aggregate"),
        }
    }

    /// The current output value; SUM and MIN/MAX report NULL when they hold no live non-null input.
    fn emit(&self, result_type: &DataType) -> ScalarValue {
        match self {
            GroupAggState::Running { agg, non_null } => match agg {
                RunningAgg::Count(_) => agg.emit(),
                _ if *non_null == 0 => null_scalar(result_type),
                // AVG divides the running sum by the live non-null count, truncating toward zero for an
                // integer result (Flink's div) and casting back to the input type — see AvgAggFunction.
                RunningAgg::AvgInt { sum, result } => avg_int_scalar(*sum / *non_null, result),
                RunningAgg::AvgFloat { sum, result } => {
                    avg_float_scalar(*sum / *non_null as f64, result)
                }
                // Decimal AVG divides with Flink's exact decimal division (38-significant-digit
                // quotient, HALF_UP) and reports DECIMAL(38, max(6, s)) — findAvgAggType's type. An
                // overflowed sum reports NULL, like SUM.
                RunningAgg::AvgDecimal { sum, scale, overflow } => {
                    let result_scale = (*scale).max(6);
                    if *overflow {
                        ScalarValue::Decimal128(None, 38, result_scale)
                    } else {
                        let (unscaled, qscale) =
                            quotient_38_digits(*sum, *scale, *non_null as i128, 0);
                        ScalarValue::Decimal128(
                            rescale_half_up(unscaled, qscale, 38, result_scale),
                            38,
                            result_scale,
                        )
                    }
                }
                _ => agg.emit(),
            },
            GroupAggState::Extremes { is_min, counts } => {
                let extreme = if *is_min { counts.keys().next() } else { counts.keys().next_back() };
                extreme.map_or_else(|| null_scalar(result_type), |k| k.scalar(result_type))
            }
            // COUNT(DISTINCT) is the number of live distinct values (never NULL — empty is 0).
            GroupAggState::Distinct(counts) => ScalarValue::Int64(Some(counts.len() as i64)),
            // SUM(DISTINCT) reports NULL with no live values, like SUM.
            GroupAggState::DistinctRunning { counts, agg } => {
                if counts.is_empty() {
                    null_scalar(result_type)
                } else {
                    agg.emit()
                }
            }
        }
    }
}

/// Per-key state for a `GROUP BY` group: the per-aggregate state and the live record count (reaching
/// zero deletes the group).
pub(crate) struct GroupKeyState {
    aggs: Vec<GroupAggState>,
    records: i64,
    /// The tuple last emitted for this group — the per-row changelog needs the pre-update value of
    /// every touched group, and caching it halves the output materialization (the q16 profile put
    /// ~half the operator in exactly that scalar build/clone churn). `None` after restore (the
    /// snapshot doesn't carry it); the first touch then recomputes it from the aggregate state.
    last_output: Option<Vec<ScalarValue>>,
}

/// Non-windowed `GROUP BY` aggregation over a changelog. Holds per-key state — no windows, no
/// watermark — and processes a batch in input order like the host's per-record aggregate, so the
/// emitted change sequence matches byte for byte. Each row's `RowKind` (carried on `$row_kind$`)
/// selects accumulate (`+I`/`+U`) or retract (`-U`/`-D`); a key's first row inserts, a result change
/// retracts the previous value then appends the new (the `-U` gated on `generate_update_before`, an
/// unchanged result suppressed), and a key whose record count reaches zero is deleted (`-D`). An
/// append-only input is the same path with no retractions. SUM/COUNT retract a running value; MIN/MAX
/// retract by keeping a per-key value multiset. The emitted batch is `[key0.., result0..]` plus the
/// `$row_kind$` byte column.
pub(crate) struct GroupAggregator {
    kinds: Vec<i64>,
    value_types: Vec<DataType>,
    result_types: Vec<DataType>,
    // The Arrow type of each aggregate's checkpointed state scalar — equals result_types except for
    // AVG, whose snapshot stores the wider running sum (BIGINT / DOUBLE).
    state_types: Vec<DataType>,
    value_columns: Vec<i64>,
    // Per-aggregate FILTER column index (the boolean the host computes for `AGG(x) FILTER (WHERE p)`),
    // or -1 for an unfiltered aggregate. A row folds into aggregate i only when its filter is TRUE.
    filter_columns: Vec<i64>,
    // Per-aggregate count-partial column for a two-phase AVG merge (-1 otherwise): the value column
    // is then the local's pre-summed sum partial and each row bumps the count by this column.
    count_columns: Vec<i64>,
    key_columns: Vec<usize>,
    generate_update_before: bool,
    // The group map is keyed by the arrow-row memcomparable encoding of the key columns, not a
    // `Vec<ScalarValue>`: a native-vs-Flink differential on q17 showed the scalar key's hash/alloc/drop
    // (ScalarValue::hash, per-row Vec churn) was the dominant cost native paid and Flink (byte keys)
    // did not. `key_converter` encodes the key columns per batch and decodes them back for the output.
    keys: ahash::HashMap<OwnedRow, GroupKeyState>,
    key_converter: Option<RowConverter>,
    key_types: Vec<DataType>,
    pub(crate) memory: OperatorMemory,
}

/// Estimated per-entry footprint of a MIN/MAX or DISTINCT multiset node (key enum + count + node
/// overhead). String contents are under-counted by design: measuring them would make the per-row
/// state measurement O(multiset), and the estimate only has to bound growth, not audit it.
pub(crate) const MULTISET_ENTRY_BYTES: usize = 64;

/// O(1) estimated footprint of one aggregate's per-key state (multisets counted by `len`).
pub(crate) fn group_agg_state_bytes(state: &GroupAggState) -> usize {
    let inner = match state {
        GroupAggState::Running { .. } => 0,
        GroupAggState::Extremes { counts, .. } => counts.len() * MULTISET_ENTRY_BYTES,
        GroupAggState::Distinct(counts) => counts.len() * MULTISET_ENTRY_BYTES,
        GroupAggState::DistinctRunning { counts, .. } => counts.len() * MULTISET_ENTRY_BYTES,
    };
    std::mem::size_of::<GroupAggState>() + inner
}

/// Estimated footprint of one group's full state (all aggregates plus the record counter).
pub(crate) fn group_key_state_bytes(state: &GroupKeyState) -> usize {
    state.aggs.iter().map(group_agg_state_bytes).sum::<usize>()
        + state.last_output.as_ref().map_or(0, |v| scalar_row_bytes(v))
        + std::mem::size_of::<GroupKeyState>()
}

/// A group's current output tuple (each aggregate reports NULL while it has no live input).
fn output_of(state: &GroupKeyState, result_types: &[DataType]) -> Vec<ScalarValue> {
    state.aggs.iter().zip(result_types).map(|(agg, rt)| agg.emit(rt)).collect()
}

/// Estimated footprint of an arrow-row byte key plus its map entry.
pub(crate) fn owned_row_bytes(row: &OwnedRow) -> usize {
    row.row().as_ref().len() + GROUP_ENTRY_OVERHEAD
}

impl GroupAggregator {
    pub(crate) fn new(
        kinds: Vec<i64>,
        value_types: Vec<i64>,
        value_columns: Vec<i64>,
        key_columns: Vec<usize>,
        generate_update_before: bool,
    ) -> Self {
        let value_types: Vec<DataType> = value_types.iter().map(|&code| value_data_type(code)).collect();
        let result_types = kinds
            .iter()
            .zip(&value_types)
            .map(|(&kind, vt)| RunningAgg::new(kind, vt).result_type())
            .collect();
        let state_types = kinds
            .iter()
            .zip(&value_types)
            .map(|(&kind, vt)| RunningAgg::new(kind, vt).state_type())
            .collect();
        let filter_columns = vec![-1; kinds.len()];
        let count_columns = vec![-1; kinds.len()];
        GroupAggregator {
            kinds,
            value_types,
            result_types,
            state_types,
            value_columns,
            key_columns,
            generate_update_before,
            keys: ahash::HashMap::default(),
            key_converter: None,
            key_types: Vec::new(),
            filter_columns,
            count_columns,
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds this aggregator's state by the operator's managed-memory budget (negative =
    /// unaccounted), accounting any restored groups immediately.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .keys
            .iter()
            .map(|(key, state)| owned_row_bytes(key) + group_key_state_bytes(state))
            .sum();
        self.memory.attach("group-aggregate", budget_bytes, state)?;
        Ok(self)
    }

    /// Sets the per-aggregate FILTER columns (-1 = unfiltered). A builder so the many existing
    /// call sites that construct an unfiltered aggregator stay unchanged.
    pub(crate) fn with_filter_columns(mut self, filter_columns: Vec<i64>) -> Self {
        if !filter_columns.is_empty() {
            self.filter_columns = filter_columns;
        }
        self
    }

    /// Sets the per-aggregate two-phase AVG count-partial columns (-1 = not a merge). A builder for
    /// the same reason as {@link with_filter_columns}.
    fn with_count_columns(mut self, count_columns: Vec<i64>) -> Self {
        if !count_columns.is_empty() {
            self.count_columns = count_columns;
        }
        self
    }

    /// The per-key state, created (empty) on first touch.
    fn state(&mut self, key: OwnedRow) -> &mut GroupKeyState {
        let (kinds, value_types) = (&self.kinds, &self.value_types);
        self.keys.entry(key).or_insert_with(|| GroupKeyState {
            aggs: kinds.iter().zip(value_types).map(|(&kind, vt)| GroupAggState::new(kind, vt)).collect(),
            records: 0,
            last_output: None,
        })
    }



    /// Folds the batch's rows into per-key state in input order, honoring each row's `RowKind`, and
    /// returns the changelog rows produced, in emission order.
    pub(crate) fn update(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        let n = batch.num_rows();
        let num_agg = self.kinds.len();
        // `None` is a COUNT(*) aggregate (no argument column): it counts every row. A present column
        // counts/folds only non-null rows, matching the host's COUNT(col)/SUM null handling.
        let value_columns: Vec<Option<ValueColumn>> = (0..num_agg)
            .map(|i| {
                if self.value_columns[i] < 0 {
                    return None;
                }
                let column = batch.column(self.value_columns[i] as usize);
                // Build from the column's actual type: the numeric folds read a typed value, while a
                // non-numeric column (only COUNT admits one) is read for null-ness alone.
                Some(match column.data_type() {
                    DataType::Int64 => {
                        ValueColumn::I64(column.as_any().downcast_ref().expect("int64 value"))
                    }
                    DataType::Int32 => {
                        ValueColumn::I32(column.as_any().downcast_ref().expect("int32 value"))
                    }
                    DataType::Float64 => {
                        ValueColumn::F64(column.as_any().downcast_ref().expect("float64 value"))
                    }
                    DataType::Decimal128(_, _) => {
                        ValueColumn::Decimal128(column.as_any().downcast_ref().expect("decimal128 value"))
                    }
                    _ => ValueColumn::NullOnly(column),
                })
            })
            .collect();
        let key_arrays: Vec<&ArrayRef> = self.key_columns.iter().map(|&i| batch.column(i)).collect();
        self.key_types = key_types(&key_arrays);
        if self.key_converter.is_none() {
            self.key_converter = Some(key_row_converter(&key_arrays));
        }
        // Encode all key columns to memcomparable byte rows in one pass; the per-row group key is then
        // a byte slice, not a freshly allocated Vec<ScalarValue>.
        let key_owned: Vec<ArrayRef> = key_arrays.iter().map(|a| (*a).clone()).collect();
        let keys_encoded =
            encode_group_keys(self.key_converter.as_ref().unwrap(), &key_owned, n);
        let row_kinds = row_kind_column(batch);
        // Per aggregate, the value column index for a COUNT(DISTINCT) (kind 7) or SUM(DISTINCT)
        // (kind 9), else None. Captured before the per-row loop so the loop body reads no `self`
        // field while `state` is borrowed.
        let distinct_cols: Vec<Option<usize>> = (0..num_agg)
            .map(|i| matches!(self.kinds[i], 7 | 9).then_some(self.value_columns[i] as usize))
            .collect();
        // The BIGINT fast path per distinct aggregate: fold the primitive straight off the array
        // (no per-row ScalarValue). Present exactly when the value column is Int64.
        let distinct_i64_cols: Vec<Option<&Int64Array>> = (0..num_agg)
            .map(|i| {
                distinct_cols[i].and_then(|c| batch.column(c).as_any().downcast_ref::<Int64Array>())
            })
            .collect();
        // Per aggregate, a string MIN/MAX value column (kind 1/2 over Utf8) — folded as a scalar into
        // the Extremes multiset, not through the numeric Num path.
        let extreme_str_cols: Vec<Option<usize>> = (0..num_agg)
            .map(|i| {
                if matches!(self.kinds[i], 1 | 2) && self.value_columns[i] >= 0 {
                    let col = self.value_columns[i] as usize;
                    matches!(
                        batch.column(col).data_type(),
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    )
                    .then_some(col)
                } else {
                    None
                }
            })
            .collect();
        // Per aggregate, the two-phase AVG count-partial column (None = not a merge): the value
        // column is the pre-summed sum partial and the count folds from this column, not +1 per row.
        let merge_count_cols: Vec<Option<&Int64Array>> = (0..num_agg)
            .map(|i| {
                (self.count_columns[i] >= 0).then(|| {
                    batch
                        .column(self.count_columns[i] as usize)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("avg count partial column must be bigint")
                })
            })
            .collect();
        // Per aggregate, the FILTER boolean column (None = unfiltered). A row folds into aggregate i
        // only where this is TRUE — a NULL or FALSE skips it, matching SQL FILTER / Flink's filterArg.
        let filter_cols: Vec<Option<&BooleanArray>> = (0..num_agg)
            .map(|i| {
                (self.filter_columns[i] >= 0).then(|| {
                    batch
                        .column(self.filter_columns[i] as usize)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .expect("filter column must be boolean")
                })
            })
            .collect();

        let mut out_keys: Vec<OwnedRow> = Vec::new();
        let mut out_results: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_agg];
        let mut out_kinds: Vec<i8> = Vec::new();
        let mut push = |kind: i8, key: &OwnedRow, values: Vec<ScalarValue>| {
            out_keys.push(key.clone());
            for (i, v) in values.into_iter().enumerate() {
                out_results[i].push(v);
            }
            out_kinds.push(kind);
        };

        let track = self.memory.tracking();
        for row in 0..n {
            let key = keys_encoded.row(row).owned();
            // RowKind: 0 +I, 1 -U, 2 +U, 3 -D (absent column ⇒ INSERT). UB/delete retract; I/UA add.
            let kind = row_kinds.map_or(0, |kinds| kinds.value(row));
            let retract = kind == 1 || kind == 3;
            let exists = self.keys.contains_key(&key);
            let before = if track && exists {
                group_key_state_bytes(self.keys.get(&key).expect("key present")) as isize
            } else {
                0
            };
            if !exists && retract {
                continue; // no accumulator for a key's first message being a retraction (host skips it)
            }
            // The pre-update output comes from the group's cache (recomputed only after a restore,
            // which does not carry it) — the second full materialization per row goes away.
            let prev = if exists {
                let state = self.keys.get_mut(&key).expect("key present");
                let cached = state.last_output.take();
                Some(cached.unwrap_or_else(|| output_of(state, &self.result_types)))
            } else {
                None
            };
            {
                // Clone the key into the map only when inserting a new group; an existing group
                // (the steady state) is reached by reference, avoiding a per-row key allocation.
                let state = if exists {
                    self.keys.get_mut(&key).expect("key present")
                } else {
                    self.state(key.clone())
                };
                for i in 0..num_agg {
                    // FILTER: fold into this aggregate only where its filter is TRUE (NULL/FALSE skip).
                    if let Some(filter) = filter_cols[i] {
                        if filter.is_null(row) || !filter.value(row) {
                            continue;
                        }
                    }
                    // Two-phase AVG merge: fold the pre-summed sum partial and bump the count by the
                    // count partial. A NULL sum partial means the local saw no non-null input for the
                    // key (its count partial is 0) — nothing to fold.
                    if let Some(counts) = merge_count_cols[i] {
                        if let Some(column) = &value_columns[i] {
                            if let Some(num) = column.at(row) {
                                let count = if counts.is_null(row) { 0 } else { counts.value(row) };
                                if retract {
                                    state.aggs[i].retract_merged(num, count);
                                } else {
                                    state.aggs[i].accumulate_merged(num, count);
                                }
                            }
                        }
                        continue;
                    }
                    // MIN/MAX over a string folds the value as a scalar into the Extremes multiset
                    // (skipping nulls — MIN/MAX ignore them), ordered by MinMaxKey.
                    if let Some(col_idx) = extreme_str_cols[i] {
                        let column = batch.column(col_idx);
                        if !column.is_null(row) {
                            let scalar =
                                ScalarValue::try_from_array(column, row).expect("extreme string scalar");
                            if retract {
                                state.aggs[i].retract_extreme(scalar);
                            } else {
                                state.aggs[i].accumulate_extreme(scalar);
                            }
                        }
                        continue;
                    }
                    // COUNT(DISTINCT x) (kind 7) folds the value itself, not a Num — read its scalar
                    // (skipping nulls, which DISTINCT ignores) and add/remove it from the value set.
                    if let Some(col_idx) = distinct_cols[i] {
                        if let Some(ints) = distinct_i64_cols[i] {
                            if !ints.is_null(row) {
                                if retract {
                                    state.aggs[i].retract_distinct_i64(ints.value(row));
                                } else {
                                    state.aggs[i].accumulate_distinct_i64(ints.value(row));
                                }
                            }
                            continue;
                        }
                        let column = batch.column(col_idx);
                        if !column.is_null(row) {
                            let scalar =
                                ScalarValue::try_from_array(column, row).expect("distinct value scalar");
                            if retract {
                                state.aggs[i].retract_distinct(scalar);
                            } else {
                                state.aggs[i].accumulate_distinct(scalar);
                            }
                        }
                        continue;
                    }
                    match &value_columns[i] {
                        // COUNT(*): the value is ignored, so any number drives the count.
                        None => {
                            if retract {
                                state.aggs[i].retract(Num::I64(0));
                            } else {
                                state.aggs[i].accumulate(Num::I64(0));
                            }
                        }
                        Some(column) => {
                            if let Some(num) = column.at(row) {
                                if retract {
                                    state.aggs[i].retract(num);
                                } else {
                                    state.aggs[i].accumulate(num);
                                }
                            }
                        }
                    }
                }
                state.records += if retract { -1 } else { 1 };
            }

            if self.keys.get(&key).unwrap().records > 0 {
                let state = self.keys.get_mut(&key).expect("key present");
                let new = output_of(state, &self.result_types);
                match prev {
                    None => {
                        // +I — first row for the key; the emitted tuple seeds the cache.
                        state.last_output = Some(new.clone());
                        push(0, &key, new);
                    }
                    Some(prev) if new != prev => {
                        state.last_output = Some(new.clone());
                        if self.generate_update_before {
                            push(1, &key, prev); // -U — moved out of the cache, not recomputed
                        }
                        push(2, &key, new); // +U
                    }
                    Some(prev) => {
                        state.last_output = Some(prev); // unchanged result — suppressed
                    }
                }
            } else {
                // The last record for the key was retracted: delete the group.
                push(3, &key, prev.expect("a retraction implies the key existed")); // -D
                self.keys.remove(&key);
            }
            if track {
                // The touched group's footprint change, plus its key when created or deleted.
                let mut delta = -before;
                if let Some(state) = self.keys.get(&key) {
                    delta += group_key_state_bytes(state) as isize;
                    if !exists {
                        delta += owned_row_bytes(&key) as isize;
                    }
                } else if exists {
                    delta -= owned_row_bytes(&key) as isize;
                }
                self.memory.record(delta);
            }
        }
        drop(push);
        self.memory.account()?;

        let mut fields = key_fields(&self.key_types);
        let mut columns = decode_keys(self.key_converter.as_ref(), &out_keys, &self.key_types);
        for (i, rt) in self.result_types.iter().enumerate() {
            fields.push(Field::new(format!("result{i}"), rt.clone(), true));
            columns.push(scalars_to_array(std::mem::take(&mut out_results[i]), rt));
        }
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build group-by changelog batch"))
    }

    /// Serializes per-key state. A main batch carries `[key0.., records, state{i}, nonnull{i}…]` (the
    /// raw running value and non-null count for SUM/COUNT; a NULL placeholder for MIN/MAX), and a side
    /// batch per MIN/MAX aggregate carries its `[key0.., value, count]` multiset rows.
    pub(crate) fn snapshot(&mut self) -> Vec<u8> {
        let num_agg = self.kinds.len();
        let mut keys: Vec<OwnedRow> = Vec::new();
        let mut records: Vec<i64> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_agg];
        let mut non_null_columns: Vec<Vec<i64>> = vec![Vec::new(); num_agg];
        let mut multiset_keys: Vec<Vec<OwnedRow>> = vec![Vec::new(); num_agg];
        let mut multiset_values: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_agg];
        let mut multiset_counts: Vec<Vec<i64>> = vec![Vec::new(); num_agg];
        for (key, state) in self.keys.iter() {
            keys.push(key.clone());
            records.push(state.records);
            for i in 0..num_agg {
                match &state.aggs[i] {
                    GroupAggState::Running { agg, non_null } => {
                        state_columns[i].push(agg.emit());
                        non_null_columns[i].push(*non_null);
                    }
                    GroupAggState::Extremes { counts, .. } => {
                        state_columns[i].push(null_scalar(&self.result_types[i]));
                        non_null_columns[i].push(0);
                        for (value, count) in counts.iter() {
                            multiset_keys[i].push(key.clone());
                            multiset_values[i].push(value.scalar(&self.result_types[i]));
                            multiset_counts[i].push(*count);
                        }
                    }
                    GroupAggState::Distinct(counts) => {
                        // The count is recomputed from the side batch on restore (placeholder here).
                        state_columns[i].push(null_scalar(&self.result_types[i]));
                        non_null_columns[i].push(0);
                        for (value, count) in counts.scalar_entries() {
                            multiset_keys[i].push(key.clone());
                            multiset_values[i].push(value); // the distinct value itself
                            multiset_counts[i].push(count);
                        }
                    }
                    GroupAggState::DistinctRunning { counts, .. } => {
                        // The running sum is refolded from the side batch's values on restore.
                        state_columns[i].push(null_scalar(&self.result_types[i]));
                        non_null_columns[i].push(0);
                        for (value, count) in counts.scalar_entries() {
                            multiset_keys[i].push(key.clone());
                            multiset_values[i].push(value);
                            multiset_counts[i].push(count);
                        }
                    }
                }
            }
        }

        let mut fields = key_fields(&self.key_types);
        let mut columns = decode_keys(self.key_converter.as_ref(), &keys, &self.key_types);
        fields.push(Field::new("records", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(records)));
        for i in 0..num_agg {
            fields.push(Field::new(format!("state{i}"), self.state_types[i].clone(), true));
            columns.push(scalars_to_array(std::mem::take(&mut state_columns[i]), &self.state_types[i]));
            fields.push(Field::new(format!("nonnull{i}"), DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(std::mem::take(&mut non_null_columns[i]))));
        }
        let mut batches =
            vec![RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("main snapshot")];
        for i in 0..num_agg {
            if matches!(self.kinds[i], 1 | 2 | 7 | 9) {
                let mut f = key_fields(&self.key_types);
                let mut c = decode_keys(self.key_converter.as_ref(), &multiset_keys[i], &self.key_types);
                // MIN/MAX values take the aggregate's result type; a distinct value keeps its own type
                // (a COUNT's bigint result type does not describe it), inferred from the scalars.
                let values = std::mem::take(&mut multiset_values[i]);
                let value_array: ArrayRef = if matches!(self.kinds[i], 7 | 9) {
                    if values.is_empty() {
                        new_empty_array(&DataType::Int64) // 0 rows — type is immaterial on restore
                    } else {
                        ScalarValue::iter_to_array(values).expect("distinct value column")
                    }
                } else {
                    scalars_to_array(values, &self.result_types[i])
                };
                f.push(Field::new("value", value_array.data_type().clone(), true));
                c.push(value_array);
                f.push(Field::new("count", DataType::Int64, false));
                c.push(Arc::new(Int64Array::from(std::mem::take(&mut multiset_counts[i]))));
                batches.push(RecordBatch::try_new(Arc::new(Schema::new(f)), c).expect("multiset snapshot"));
            }
        }
        write_framed(&batches)
    }

    pub(crate) fn restore(
        kinds: Vec<i64>,
        value_types: Vec<i64>,
        value_columns: Vec<i64>,
        key_columns: Vec<usize>,
        generate_update_before: bool,
        bytes: &[u8],
    ) -> Self {
        let mut aggregator =
            GroupAggregator::new(kinds, value_types, value_columns, key_columns, generate_update_before);
        let num_agg = aggregator.kinds.len();
        let batches = read_framed(bytes);
        if batches.is_empty() {
            return aggregator;
        }
        // Main batch: key0.., records, then (state, nonnull) per aggregate.
        let main = &batches[0];
        let arity = main.num_columns() - 1 - 2 * num_agg;
        let key_arrays: Vec<&ArrayRef> = (0..arity).map(|j| main.column(j)).collect();
        aggregator.key_types = key_types(&key_arrays);
        aggregator.key_converter = Some(key_row_converter(&key_arrays));
        let key_owned: Vec<ArrayRef> = key_arrays.iter().map(|a| (*a).clone()).collect();
        let keys_encoded =
            encode_group_keys(aggregator.key_converter.as_ref().unwrap(), &key_owned, main.num_rows());
        let records = column_i64(main, "records");
        for row in 0..main.num_rows() {
            let key = keys_encoded.row(row).owned();
            let state = aggregator.state(key);
            state.records = records.value(row);
            for i in 0..num_agg {
                if let GroupAggState::Running { agg, non_null } = &mut state.aggs[i] {
                    let scalar = ScalarValue::try_from_array(main.column(arity + 1 + 2 * i), row)
                        .expect("group state scalar");
                    agg.restore_value(&scalar);
                    *non_null = main
                        .column(arity + 2 + 2 * i)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("nonnull int64")
                        .value(row);
                }
            }
        }
        // One side batch per MIN/MAX or DISTINCT aggregate, in aggregate order: key0.., value, count.
        let mut frame = 1;
        for i in 0..num_agg {
            if !matches!(aggregator.kinds[i], 1 | 2 | 7 | 9) {
                continue;
            }
            let side = &batches[frame];
            frame += 1;
            let side_arity = side.num_columns() - 2;
            let side_keys: Vec<&ArrayRef> = (0..side_arity).map(|j| side.column(j)).collect();
            let side_key_owned: Vec<ArrayRef> = side_keys.iter().map(|a| (*a).clone()).collect();
            let side_keys_enc = encode_group_keys(
                aggregator.key_converter.as_ref().expect("key converter set by the main batch"),
                &side_key_owned,
                side.num_rows(),
            );
            let values = side.column(side_arity);
            let counts = column_i64(side, "count");
            for row in 0..side.num_rows() {
                let key = side_keys_enc.row(row).owned();
                let value = ScalarValue::try_from_array(values, row).expect("multiset value");
                match aggregator.keys.get_mut(&key).map(|s| &mut s.aggs[i]) {
                    Some(GroupAggState::Extremes { counts: map, .. }) => {
                        map.insert(MinMaxKey::from_scalar(&value), counts.value(row));
                    }
                    Some(GroupAggState::Distinct(set)) => {
                        set.insert_restored(value, counts.value(row));
                    }
                    // SUM(DISTINCT): rebuild the set and refold each live value into the running sum.
                    Some(GroupAggState::DistinctRunning { counts: set, agg }) => {
                        agg.fold(distinct_num(&value));
                        set.insert_restored(value, counts.value(row));
                    }
                    _ => {}
                }
            }
        }
        aggregator
    }
}

/// Local half of two-phase non-windowed `GROUP BY`: a transient mini-batch pre-aggregate. It folds
/// each incoming (insert-only) batch into per-key accumulators held in memory and, on a flush (driven
/// by the mini-batch marker, a size trigger, or a pre-checkpoint drain on the JVM side), emits one
/// partial row per buffered key — `[key0.., partial0..]`, no `$row_kind$` (insert-only) — the
/// intermediate accumulator the stateful global half then merges. The buffer is transient: it is
/// always drained before a checkpoint barrier, so nothing is persisted here (the global keeps the
/// durable state). This mirrors Flink's `MapBundleOperator` + `MiniBatchLocalGroupAggFunction` and
/// RisingWave's stateless two-phase local. SUM/MIN/MAX emit NULL for an all-null group; COUNT(*)
/// counts rows. Group order follows first appearance across the buffered batches.
pub(crate) struct LocalGroupAggregator {
    kinds: Vec<i64>,
    value_types: Vec<DataType>,
    value_columns: Vec<i64>,
    key_columns: Vec<usize>,
    result_types: Vec<DataType>,
    order: Vec<GroupKey>,
    states: HashMap<GroupKey, Vec<GroupAggState>>,
    key_types: Vec<DataType>,
    pub(crate) memory: OperatorMemory,
}

/// Estimated footprint of one buffered local-aggregate entry: the key is held twice (the states map
/// and the first-appearance order), plus the per-aggregate partial states.
pub(crate) fn local_entry_bytes(key: &GroupKey, states: &[GroupAggState]) -> usize {
    group_key_bytes(key) * 2 + states.iter().map(group_agg_state_bytes).sum::<usize>()
}

impl LocalGroupAggregator {
    pub(crate) fn new(
        kinds: Vec<i64>,
        value_types: Vec<i64>,
        value_columns: Vec<i64>,
        key_columns: Vec<usize>,
    ) -> Self {
        let value_types: Vec<DataType> = value_types.iter().map(|&c| value_data_type(c)).collect();
        let result_types = kinds
            .iter()
            .zip(&value_types)
            .map(|(&kind, vt)| RunningAgg::new(kind, vt).result_type())
            .collect();
        LocalGroupAggregator {
            kinds,
            value_types,
            value_columns,
            key_columns,
            result_types,
            order: Vec::new(),
            states: HashMap::default(),
            key_types: Vec::new(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds the buffered partials by a managed-memory budget (negative = unaccounted). The buffer
    /// drains at every mini-batch flush, but a high-cardinality interval can still spike.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let current =
            self.states.iter().map(|(key, states)| local_entry_bytes(key, states)).sum();
        self.memory.attach("local-group-aggregate", budget_bytes, current)?;
        Ok(self)
    }

    /// Folds the batch's rows into the buffered per-key accumulators (append-only — the local's input
    /// is insert-only, so there is no retraction). Nothing is emitted until a flush.
    pub(crate) fn update(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let n = batch.num_rows();
        let num_agg = self.kinds.len();
        // `None` is a COUNT(*) aggregate (no argument); a present column folds/counts non-null rows.
        let cols: Vec<Option<ValueColumn>> = (0..num_agg)
            .map(|i| {
                if self.value_columns[i] < 0 {
                    return None;
                }
                let column = batch.column(self.value_columns[i] as usize);
                Some(match column.data_type() {
                    DataType::Int64 => ValueColumn::I64(column.as_any().downcast_ref().expect("int64 value")),
                    DataType::Int32 => ValueColumn::I32(column.as_any().downcast_ref().expect("int32 value")),
                    DataType::Float64 => ValueColumn::F64(column.as_any().downcast_ref().expect("float64 value")),
                    _ => ValueColumn::NullOnly(column),
                })
            })
            .collect();
        let key_arrays: Vec<&ArrayRef> = self.key_columns.iter().map(|&i| batch.column(i)).collect();
        self.key_types = key_types(&key_arrays);
        let track = self.memory.tracking();
        for row in 0..n {
            let key = read_key(&key_arrays, row);
            let mut delta = 0isize;
            if !self.states.contains_key(&key) {
                let init: Vec<GroupAggState> = self
                    .kinds
                    .iter()
                    .zip(&self.value_types)
                    .map(|(&kind, vt)| GroupAggState::new(kind, vt))
                    .collect();
                if track {
                    delta += (group_key_bytes(&key) * 2) as isize;
                }
                self.order.push(key.clone());
                self.states.insert(key.clone(), init);
            }
            let entry = self.states.get_mut(&key).expect("key present");
            if track {
                delta -= entry.iter().map(group_agg_state_bytes).sum::<usize>() as isize;
            }
            for i in 0..num_agg {
                match &cols[i] {
                    None => entry[i].accumulate(Num::I64(0)),
                    Some(column) => {
                        if let Some(num) = column.at(row) {
                            entry[i].accumulate(num);
                        }
                    }
                }
            }
            if track {
                delta += entry.iter().map(group_agg_state_bytes).sum::<usize>() as isize;
                self.memory.record(delta);
            }
        }
        self.memory.account()
    }

    /// Emits the buffered partials (`[key0.., partial0..]`, in first-appearance order) and clears the
    /// buffer; the key types are retained so an empty flush still carries the right schema.
    pub(crate) fn flush(&mut self) -> RecordBatch {
        let order = std::mem::take(&mut self.order);
        let states = std::mem::take(&mut self.states);
        self.memory.set(0);
        self.memory.account_shrink();
        let mut fields = key_fields(&self.key_types);
        let mut columns = key_columns(&order, &self.key_types);
        for (i, rt) in self.result_types.iter().enumerate() {
            let scalars: Vec<ScalarValue> = order.iter().map(|key| states[key][i].emit(rt)).collect();
            fields.push(Field::new(format!("partial{i}"), rt.clone(), true));
            columns.push(scalars_to_array(scalars, rt));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build local group-by partial batch")
    }
}

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_groupAggregatorStateBytes, GroupAggregator);

state_bytes_getter!(Java_io_github_jordepic_streamfusion_Native_localGroupAggregatorStateBytes, LocalGroupAggregator);

/// Creates a buffering local two-phase GROUP BY pre-aggregate and returns an opaque handle. It
/// accumulates across batches in memory until flushed; the buffer is transient (drained before each
/// checkpoint on the JVM side), so there is no snapshot/restore.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createLocalGroupAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    aggregate_kinds: JIntArray<'local>,
    value_types: JIntArray<'local>,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let value_columns = read_int_array(&env, &value_columns);
    let key_cols = read_columns(&env, &key_columns);
    let aggregator = LocalGroupAggregator::new(kinds, value_types, value_columns, key_cols)
        .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}

/// Folds an Arrow batch the JVM exported into the buffered per-key accumulators; emits nothing.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updateLocalGroupAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut LocalGroupAggregator) };
    // The batch must drop before a throw: its release callback upcalls into the JVM, which would
    // clear the pending exception (see updateTumblingAggregator).
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        aggregator.update(&batch)
    };
    if let Err(e) = result {
        throw_memory_limit(&mut env, &e.to_string());
    }
}

/// Emits the buffered partials (one row per key) and clears the buffer.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushLocalGroupAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut LocalGroupAggregator) };
    let result = aggregator.flush();
    export_record_batch(result, out_array_address, out_schema_address);
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeLocalGroupAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<LocalGroupAggregator>(handle));
    }
}

/// Creates a non-windowed `GROUP BY` aggregator and returns an opaque handle. The aggregate kinds
/// and per-aggregate value-type codes are positional; `generate_update_before` is the host's
/// per-node changelog flag. Grouping keys travel as `key0..` columns on each input batch.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createGroupAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    aggregate_kinds: JIntArray<'local>,
    value_types: JIntArray<'local>,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    filter_columns: JIntArray<'local>,
    count_columns: JIntArray<'local>,
    generate_update_before: jboolean,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let value_columns = read_int_array(&env, &value_columns);
    let filter_columns = read_int_array(&env, &filter_columns);
    let count_columns = read_int_array(&env, &count_columns);
    let key_columns = read_columns(&env, &key_columns);
    let aggregator =
        GroupAggregator::new(kinds, value_types, value_columns, key_columns, generate_update_before != 0)
            .with_filter_columns(filter_columns)
            .with_count_columns(count_columns)
            .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}

/// Folds an input batch into per-key state and exports the changelog rows it produces (the row kinds
/// ride the `$row_kind$` column of the result).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updateGroupAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut GroupAggregator) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        aggregator.update(&batch)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Serializes the aggregator's per-key state for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let aggregator = unsafe { &mut *(handle as *mut GroupAggregator) };
    env.byte_array_from_slice(&aggregator.snapshot())
        .expect("failed to allocate group-by snapshot array")
        .into_raw()
}

/// Rebuilds a `GROUP BY` aggregator from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreGroupAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    aggregate_kinds: JIntArray<'local>,
    value_types: JIntArray<'local>,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    filter_columns: JIntArray<'local>,
    count_columns: JIntArray<'local>,
    generate_update_before: jboolean,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let value_columns = read_int_array(&env, &value_columns);
    let filter_columns = read_int_array(&env, &filter_columns);
    let count_columns = read_int_array(&env, &count_columns);
    let key_columns = read_columns(&env, &key_columns);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read group-by snapshot");
    let aggregator = GroupAggregator::restore(
        kinds,
        value_types,
        value_columns,
        key_columns,
        generate_update_before != 0,
        &bytes,
    )
    .with_filter_columns(filter_columns)
    .with_count_columns(count_columns)
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}

/// Releases the `GROUP BY` aggregator and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeGroupAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<GroupAggregator>(handle));
    }
}
