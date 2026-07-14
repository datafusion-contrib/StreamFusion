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

    /// Adds `n` occurrences at once (a two-phase merge folding a local bundle's per-value count);
    /// returns true when the value enters the set.
    fn add_i64_n(&mut self, value: i64, n: i64) -> bool {
        match self {
            DistinctSet::I64(m) => {
                let count = m.entry(value).or_insert(0);
                *count += n;
                *count == n
            }
            DistinctSet::Scalar(_) => self.add_scalar_n(ScalarValue::Int64(Some(value)), n),
        }
    }

    /// The scalar form of {@link add_i64_n}.
    fn add_scalar_n(&mut self, value: ScalarValue, n: i64) -> bool {
        match self {
            DistinctSet::I64(_) => match value {
                ScalarValue::Int64(Some(v)) => self.add_i64_n(v, n),
                other => unreachable!("i64 distinct set fed a non-int64 scalar: {other:?}"),
            },
            DistinctSet::Scalar(m) => {
                let count = m.entry(value).or_insert(0);
                *count += n;
                *count == n
            }
        }
    }

    /// The live (value, multiplicity) pairs as scalars — the snapshot wire format, unchanged by the
    /// typed specialization.
    fn scalar_entries(&self) -> Vec<(ScalarValue, i64)> {
        match self {
            DistinctSet::I64(m) => m
                .iter()
                .map(|(v, c)| (ScalarValue::Int64(Some(*v)), *c))
                .collect(),
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
/// non-null count so it reports NULL once fully retracted). Retracting MIN/MAX cannot use a single
/// value, so they keep a value→count multiset and read the extreme off its ends — what makes them
/// retractable (Flink's `*WithRetractAccumulator` uses a `MapView`; Arroyo calls this the batch state).
pub(crate) enum GroupAggState {
    Running {
        agg: RunningAgg,
        non_null: i64,
    },
    Extremes {
        is_min: bool,
        counts: BTreeMap<MinMaxKey, i64>,
    },
    // COUNT(DISTINCT x): a value→multiplicity map (Flink's DistinctAccumulator MapView). The count is
    // the number of live entries; a value's multiplicity tracks how many input rows carry it so a
    // retraction removes it only when the last one is retracted. Nulls are never inserted.
    Distinct(DistinctSet),
    // SUM(DISTINCT x): the same value→multiplicity map plus a running SUM folded only when a value
    // enters the set and retracted only when its last occurrence leaves — Flink's DistinctAccumulator
    // wrapping the SUM accumulator, kept incremental so the emit stays O(1).
    DistinctRunning {
        counts: DistinctSet,
        agg: RunningAgg,
    },
}

impl GroupAggState {
    fn new(kind: i64, value_type: &DataType) -> Self {
        match kind {
            1 => GroupAggState::Extremes {
                is_min: true,
                counts: BTreeMap::new(),
            }, // MIN
            2 => GroupAggState::Extremes {
                is_min: false,
                counts: BTreeMap::new(),
            }, // MAX
            7 => GroupAggState::Distinct(DistinctSet::new(value_type)), // COUNT(DISTINCT)
            // SUM(DISTINCT): the inner running aggregate is a plain SUM (kind 0) over the value type.
            9 => GroupAggState::DistinctRunning {
                counts: DistinctSet::new(value_type),
                agg: RunningAgg::new(0, value_type),
            },
            _ => GroupAggState::Running {
                agg: RunningAgg::new(kind, value_type),
                non_null: 0,
            },
        }
    }

    fn new_local(kind: i64, value_type: &DataType, append_only: bool) -> Self {
        // The local half of an insert-only two-phase aggregate never retracts its inputs. Its
        // numeric MIN/MAX partial therefore needs one value, not the global/retracting multiset.
        let running_extreme = append_only
            && matches!(kind, 1 | 2)
            && matches!(
                value_type,
                DataType::Int64
                    | DataType::Int32
                    | DataType::Int16
                    | DataType::Int8
                    | DataType::Float64
                    | DataType::Float32
            );
        if running_extreme {
            GroupAggState::Running {
                agg: RunningAgg::new(kind, value_type),
                non_null: 0,
            }
        } else {
            Self::new(kind, value_type)
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

    /// Folds a decimal AVG partial whose bundle sum overflowed (a NULL sum with a live count): the
    /// merged sum latches NULL — sticky, the lost magnitude cannot be recovered — while the count
    /// still moves, keeping the group's live-record bookkeeping exact.
    fn merge_overflowed(&mut self, count: i64, retract: bool) {
        match self {
            GroupAggState::Running {
                agg: RunningAgg::AvgDecimal { overflow, .. },
                non_null,
            } => {
                *overflow = true;
                *non_null += if retract { -count } else { count };
            }
            _ => unreachable!("a NULL sum partial with a live count is decimal-only"),
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

    /// Folds one (value, count) entry of a local bundle's distinct view into the merged set — the
    /// two-phase merge of {@link accumulate_distinct}. A value newly entering the merged set also
    /// folds once into a distinct SUM's running aggregate, exactly as the per-row path does. The
    /// two-phase distinct input is insert-only (the local's bundle is append-only), so there is no
    /// retracting counterpart.
    fn merge_distinct(&mut self, value: ScalarValue, count: i64) {
        match self {
            GroupAggState::Distinct(counts) => {
                counts.add_scalar_n(value, count);
            }
            GroupAggState::DistinctRunning { counts, agg } => {
                let num = distinct_num(&value);
                if counts.add_scalar_n(value, count) {
                    agg.fold(num);
                }
            }
            _ => unreachable!("distinct merge on a non-distinct aggregate"),
        }
    }

    /// The BIGINT fast path of {@link merge_distinct}.
    fn merge_distinct_i64(&mut self, value: i64, count: i64) {
        match self {
            GroupAggState::Distinct(counts) => {
                counts.add_i64_n(value, count);
            }
            GroupAggState::DistinctRunning { counts, agg } => {
                if counts.add_i64_n(value, count) {
                    agg.fold(Num::I64(value));
                }
            }
            _ => unreachable!("distinct merge on a non-distinct aggregate"),
        }
    }

    /// The live (value, multiplicity) pairs of a distinct aggregate's set — the local half reads
    /// these to emit its bundle's distinct view column.
    fn distinct_entries(&self) -> Vec<(ScalarValue, i64)> {
        match self {
            GroupAggState::Distinct(counts) => counts.scalar_entries(),
            GroupAggState::DistinctRunning { counts, .. } => counts.scalar_entries(),
            _ => unreachable!("distinct entries on a non-distinct aggregate"),
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
                RunningAgg::AvgDecimal {
                    sum,
                    scale,
                    overflow,
                } => {
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
                let extreme = if *is_min {
                    counts.keys().next()
                } else {
                    counts.keys().next_back()
                };
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
    /// `scalar_row_bytes` of the cached tuple, maintained alongside it: the state measurement
    /// runs twice per touched row, and re-walking the tuple's scalars each time was a measurable
    /// slice of the accounted aggregates.
    last_output_bytes: usize,
}

struct StagedGroupChange {
    old: Option<Vec<ScalarValue>>,
    key_batch: usize,
    key_row: usize,
}

fn staged_group_change_bytes(key: &ByteKey, old: &Option<Vec<ScalarValue>>) -> usize {
    byte_key_bytes(&key.0)
        + key.len()
        + std::mem::size_of::<ByteKey>()
        + std::mem::size_of::<StagedGroupChange>()
        + old.as_ref().map_or(0, |values| scalar_row_bytes(values))
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
    // Per-aggregate distinct-view column for a two-phase distinct merge (-1 otherwise): the column
    // carries the local bundle's (value, count) entries as a list of structs, folded into the
    // per-key distinct set with multiplicities instead of one value per row.
    distinct_view_columns: Vec<i64>,
    // The count1 partial column of a retracting two-phase merge (-1 otherwise): each row bumps the
    // key's record count by this column's value instead of ±1, so liveness (the -D on zero) follows
    // the local's netted retractions — Flink's RecordCounter over indexOfCountStar.
    record_count_column: i64,
    key_columns: Vec<usize>,
    key_timestamp_precisions: Vec<i32>,
    generate_update_before: bool,
    // The group map uses Flink BinaryRow bytes. Besides giving equality the same representation as
    // the keyed exchange, this admits Arrow MAP values, which arrow-row cannot encode.
    keys: ahash::HashMap<ByteKey, GroupKeyState>,
    // Materialized once per checkpoint so the JVM can write one raw keyed-state payload per Flink
    // key group without repeatedly traversing the full native map.
    snapshot_cache: Option<GroupSnapshotCache>,
    mini_batch: bool,
    staged_order: Vec<ByteKey>,
    staged_changes: HashMap<ByteKey, StagedGroupChange>,
    staged_key_batches: Vec<RecordBatch>,
    staged_bytes: usize,
    pub(crate) memory: OperatorMemory,
}

struct GroupSnapshotCache {
    max_parallelism: usize,
    timestamp_precisions: Vec<i32>,
    snapshots: std::collections::BTreeMap<i32, Vec<u8>>,
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
        + state.last_output_bytes
        + std::mem::size_of::<GroupKeyState>()
}

/// A group's current output tuple (each aggregate reports NULL while it has no live input).
fn output_of(state: &GroupKeyState, result_types: &[DataType]) -> Vec<ScalarValue> {
    state
        .aggs
        .iter()
        .zip(result_types)
        .map(|(agg, rt)| agg.emit(rt))
        .collect()
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
        let value_types: Vec<DataType> = value_types
            .iter()
            .map(|&code| value_data_type(code))
            .collect();
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
        let distinct_view_columns = vec![-1; kinds.len()];
        GroupAggregator {
            kinds,
            value_types,
            result_types,
            state_types,
            value_columns,
            key_timestamp_precisions: vec![-1; key_columns.len()],
            key_columns,
            generate_update_before,
            keys: ahash::HashMap::default(),
            snapshot_cache: None,
            mini_batch: false,
            staged_order: Vec::new(),
            staged_changes: HashMap::default(),
            staged_key_batches: Vec::new(),
            staged_bytes: 0,
            filter_columns,
            count_columns,
            distinct_view_columns,
            record_count_column: -1,
            memory: OperatorMemory::unaccounted(),
        }
    }

    pub(crate) fn with_mini_batch(mut self) -> Self {
        self.mini_batch = true;
        self
    }

    pub(crate) fn staged_keys(&self) -> usize {
        self.staged_order.len()
    }

    pub(crate) fn staging_bytes(&self) -> usize {
        self.staged_bytes
    }

    /// Bounds this aggregator's state by the operator's managed-memory budget (negative =
    /// unaccounted), accounting any restored groups immediately.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .keys
            .iter()
            .map(|(key, state)| byte_key_bytes(&key.0) + group_key_state_bytes(state))
            .sum();
        self.memory.attach("group-aggregate", budget_bytes, state)?;
        Ok(self)
    }

    fn with_key_timestamp_precisions(mut self, key_timestamp_precisions: Vec<i32>) -> Self {
        self.key_timestamp_precisions = key_timestamp_precisions;
        self
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

    /// Sets the per-aggregate two-phase distinct-view columns (-1 = not a distinct merge). A builder
    /// for the same reason as {@link with_filter_columns}.
    fn with_distinct_view_columns(mut self, distinct_view_columns: Vec<i64>) -> Self {
        if !distinct_view_columns.is_empty() {
            self.distinct_view_columns = distinct_view_columns;
        }
        self
    }

    /// Sets the count1 record-counter partial column of a retracting two-phase merge (-1 = count
    /// rows ±1). A builder for the same reason as {@link with_filter_columns}.
    fn with_record_count_column(mut self, record_count_column: i64) -> Self {
        self.record_count_column = record_count_column;
        self
    }

    /// The per-key state, created (empty) on first touch.
    fn state(&mut self, key: ByteKey) -> &mut GroupKeyState {
        let (kinds, value_types) = (&self.kinds, &self.value_types);
        self.keys.entry(key).or_insert_with(|| GroupKeyState {
            aggs: kinds
                .iter()
                .zip(value_types)
                .map(|(&kind, vt)| GroupAggState::new(kind, vt))
                .collect(),
            records: 0,
            last_output: None,
            last_output_bytes: 0,
        })
    }

    /// Folds the batch's rows into per-key state in input order, honoring each row's `RowKind`, and
    /// returns the changelog rows produced, in emission order.
    pub(crate) fn update(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DataFusionError> {
        self.snapshot_cache = None;
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
                    DataType::Int16 => {
                        ValueColumn::I16(column.as_any().downcast_ref().expect("int16 value"))
                    }
                    DataType::Int8 => {
                        ValueColumn::I8(column.as_any().downcast_ref().expect("int8 value"))
                    }
                    DataType::Float64 => {
                        ValueColumn::F64(column.as_any().downcast_ref().expect("float64 value"))
                    }
                    DataType::Float32 => {
                        ValueColumn::F32(column.as_any().downcast_ref().expect("float32 value"))
                    }
                    DataType::Decimal128(_, _) => ValueColumn::Decimal128(
                        column.as_any().downcast_ref().expect("decimal128 value"),
                    ),
                    _ => ValueColumn::NullOnly(column),
                })
            })
            .collect();
        // Keys are encoded on the fly into the encoder's reused buffer: state probes borrow the
        // bytes, and a key is copied into an owned `ByteKey` only when it first enters the map.
        let mut key_encoder =
            BinaryRowBatchEncoder::new(batch, &self.key_columns, &self.key_timestamp_precisions);
        let row_kinds = row_kind_column(batch);
        // Per aggregate, a two-phase distinct-view column: the local bundle's (value, count)
        // entries as a list of structs, merged with multiplicities instead of per-row values.
        let view_cols: Vec<Option<(&arrow::array::ListArray, &ArrayRef, &Int64Array)>> = (0
            ..num_agg)
            .map(|i| {
                (self.distinct_view_columns[i] >= 0).then(|| {
                    let list = batch
                        .column(self.distinct_view_columns[i] as usize)
                        .as_any()
                        .downcast_ref::<arrow::array::ListArray>()
                        .expect("distinct view column must be a list");
                    let entries = list
                        .values()
                        .as_any()
                        .downcast_ref::<arrow::array::StructArray>()
                        .expect("distinct view entries must be structs");
                    let counts = entries
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("distinct view counts must be bigint");
                    (list, entries.column(0), counts)
                })
            })
            .collect();
        // Per aggregate, the value column index for a COUNT(DISTINCT) (kind 7) or SUM(DISTINCT)
        // (kind 9), else None — the single-phase per-row fold; view-merged aggregates take the
        // list path above instead. Captured before the per-row loop so the loop body reads no
        // `self` field while `state` is borrowed.
        let distinct_cols: Vec<Option<usize>> = (0..num_agg)
            .map(|i| {
                (matches!(self.kinds[i], 7 | 9) && self.distinct_view_columns[i] < 0)
                    .then_some(self.value_columns[i] as usize)
            })
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
        // A retracting two-phase merge: each row's contribution to the key's record count is the
        // count1 partial (the local's netted ±rows), not ±1.
        let record_counts: Option<&Int64Array> = (self.record_count_column >= 0).then(|| {
            batch
                .column(self.record_count_column as usize)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("record count partial column must be bigint")
        });

        let mut out_rows: Vec<u32> = Vec::new();
        let mut out_results: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_agg];
        let mut out_kinds: Vec<i8> = Vec::new();
        // Every output is caused by one input row, so its original Arrow key values can be gathered
        // directly. This avoids making Flink BinaryRow bytes decodable just to emit a changelog row.
        let mut push = |kind: i8, row: usize, values: Vec<ScalarValue>| {
            out_rows.push(row as u32);
            for (i, v) in values.into_iter().enumerate() {
                out_results[i].push(v);
            }
            out_kinds.push(kind);
        };

        let track = self.memory.tracking();
        let staged_key_batch = self.staged_key_batches.len();
        let mut retained_key_batch = false;
        let mut staged_delta = 0usize;
        for row in 0..n {
            let key = key_encoder.encode(row);
            // RowKind: 0 +I, 1 -U, 2 +U, 3 -D (absent column ⇒ INSERT). UB/delete retract; I/UA add.
            let kind = row_kinds.map_or(0, |kinds| kinds.value(row));
            let retract = kind == 1 || kind == 3;
            let exists = self.keys.contains_key(key);
            let before = if track && exists {
                group_key_state_bytes(self.keys.get(key).expect("key present")) as isize
            } else {
                0
            };
            if !exists && retract {
                continue; // no accumulator for a key's first message being a retraction (host skips it)
            }
            // Immediate mode needs the preceding tuple for every input row. Mini-batch mode moves
            // it out only on the key's first touch and retains an Arrow row reference for the key;
            // intermediate rows then mutate state without materializing aggregate output.
            let first_bundle_touch = self.mini_batch && !self.staged_changes.contains_key(key);
            let (mut prev, prev_bytes) = if exists && (!self.mini_batch || first_bundle_touch) {
                let state = self.keys.get_mut(key).expect("key present");
                match state.last_output.take() {
                    Some(cached) => {
                        let bytes = std::mem::take(&mut state.last_output_bytes);
                        (Some(cached), bytes)
                    }
                    // Only after a restore (the snapshot doesn't carry the cache).
                    None => {
                        let tuple = output_of(state, &self.result_types);
                        let bytes = scalar_row_bytes(&tuple);
                        (Some(tuple), bytes)
                    }
                }
            } else {
                (None, 0)
            };
            if first_bundle_touch {
                if !retained_key_batch {
                    staged_delta += batch.get_array_memory_size();
                    self.staged_key_batches.push(batch.clone());
                    retained_key_batch = true;
                }
                let owned = ByteKey::from(key);
                self.staged_order.push(owned.clone());
                staged_delta += staged_group_change_bytes(&owned, &prev);
                self.staged_changes.insert(
                    owned,
                    StagedGroupChange { old: prev.take(), key_batch: staged_key_batch, key_row: row },
                );
            }
            {
                let state = if exists {
                    self.keys.get_mut(key).expect("key present")
                } else {
                    self.state(ByteKey::from(key))
                };
                for i in 0..num_agg {
                    // FILTER: fold into this aggregate only where its filter is TRUE (NULL/FALSE skip).
                    if let Some(filter) = filter_cols[i] {
                        if filter.is_null(row) || !filter.value(row) {
                            continue;
                        }
                    }
                    // Two-phase AVG merge: fold the pre-summed sum partial and bump the count by the
                    // count partial. A NULL sum partial with count 0 means the local saw no non-null
                    // input for the key — nothing to fold; a NULL sum with a LIVE count is a decimal
                    // partial whose bundle sum overflowed DECIMAL(38, s), which latches the merged
                    // sum NULL (the lost magnitude cannot be recovered).
                    if let Some(counts) = merge_count_cols[i] {
                        if let Some(column) = &value_columns[i] {
                            let count = if counts.is_null(row) {
                                0
                            } else {
                                counts.value(row)
                            };
                            match column.at(row) {
                                Some(num) => {
                                    if retract {
                                        state.aggs[i].retract_merged(num, count);
                                    } else {
                                        state.aggs[i].accumulate_merged(num, count);
                                    }
                                }
                                None if count > 0 => state.aggs[i].merge_overflowed(count, retract),
                                None => {}
                            }
                        }
                        continue;
                    }
                    // MIN/MAX over a string folds the value as a scalar into the Extremes multiset
                    // (skipping nulls — MIN/MAX ignore them), ordered by MinMaxKey.
                    if let Some(col_idx) = extreme_str_cols[i] {
                        let column = batch.column(col_idx);
                        if !column.is_null(row) {
                            let scalar = ScalarValue::try_from_array(column, row)
                                .expect("extreme string scalar");
                            if retract {
                                state.aggs[i].retract_extreme(scalar);
                            } else {
                                state.aggs[i].accumulate_extreme(scalar);
                            }
                        }
                        continue;
                    }
                    // Two-phase distinct merge: fold the local bundle's (value, count) entries into
                    // the per-key set with multiplicities. The partials are insert-only, so a
                    // retracting row kind cannot reach this path.
                    if let Some((list, values, counts)) = view_cols[i] {
                        assert!(!retract, "distinct view partials are insert-only");
                        let start = list.value_offsets()[row] as usize;
                        let end = list.value_offsets()[row + 1] as usize;
                        if let Some(ints) = values.as_any().downcast_ref::<Int64Array>() {
                            for e in start..end {
                                state.aggs[i].merge_distinct_i64(ints.value(e), counts.value(e));
                            }
                        } else {
                            for e in start..end {
                                let scalar = ScalarValue::try_from_array(values, e)
                                    .expect("distinct view value scalar");
                                state.aggs[i].merge_distinct(scalar, counts.value(e));
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
                            let scalar = ScalarValue::try_from_array(column, row)
                                .expect("distinct value scalar");
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
                state.records += match record_counts {
                    Some(counts) => {
                        if counts.is_null(row) {
                            0
                        } else {
                            counts.value(row)
                        }
                    }
                    None => {
                        if retract {
                            -1
                        } else {
                            1
                        }
                    }
                };
            }

            if self.mini_batch {
                if self.keys.get(key).unwrap().records <= 0 {
                    self.keys.remove(key);
                }
            } else if self.keys.get(key).unwrap().records > 0 {
                let state = self.keys.get_mut(key).expect("key present");
                let new = output_of(state, &self.result_types);
                match prev {
                    None => {
                        // +I — first row for the key; the emitted tuple seeds the cache.
                        state.last_output_bytes = scalar_row_bytes(&new);
                        state.last_output = Some(new.clone());
                        push(0, row, new);
                    }
                    Some(prev) if new != prev => {
                        state.last_output_bytes = scalar_row_bytes(&new);
                        state.last_output = Some(new.clone());
                        if self.generate_update_before {
                            push(1, row, prev); // -U — moved out of the cache, not recomputed
                        }
                        push(2, row, new); // +U
                    }
                    Some(prev) => {
                        state.last_output_bytes = prev_bytes;
                        state.last_output = Some(prev); // unchanged result — suppressed
                    }
                }
            } else {
                // The last record for the key was retracted: delete the group, emitting -D only if
                // the key ever emitted (a new key whose first merged count1 nets to zero — Flink's
                // firstRow-and-empty case — is dropped silently).
                if let Some(prev) = prev {
                    push(3, row, prev); // -D
                }
                self.keys.remove(key);
            }
            if track {
                // The touched group's footprint change, plus its key when created or deleted.
                let mut delta = -before;
                if let Some(state) = self.keys.get(key) {
                    delta += group_key_state_bytes(state) as isize;
                    if !exists {
                        delta += byte_key_bytes(key) as isize;
                    }
                } else if exists {
                    delta -= byte_key_bytes(key) as isize;
                }
                self.memory.record(delta);
            }
        }
        drop(push);
        self.staged_bytes += staged_delta;
        if track {
            self.memory.record(staged_delta as isize);
        }
        self.memory.account()?;

        // Mini-batch output is produced only by `flush_mini_batch`. Avoid constructing empty key
        // takes and result arrays on every physical push; the bridge only needs an importable empty
        // batch to preserve the existing update ABI.
        if self.mini_batch {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        let indices = UInt32Array::from(out_rows);
        let mut fields: Vec<Field> = self
            .key_columns
            .iter()
            .enumerate()
            .map(|(position, &column)| {
                Field::new(
                    format!("key{position}"),
                    batch.column(column).data_type().clone(),
                    true,
                )
            })
            .collect();
        let mut columns: Vec<ArrayRef> = self
            .key_columns
            .iter()
            .map(|&column| {
                take(batch.column(column), &indices, None).expect("take group output key")
            })
            .collect();
        for (i, rt) in self.result_types.iter().enumerate() {
            fields.push(Field::new(format!("result{i}"), rt.clone(), true));
            columns.push(scalars_to_array(std::mem::take(&mut out_results[i]), rt));
        }
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build group-by changelog batch"))
    }

    /// Finalizes the first-preimage/final-postimage transition for every key touched in the
    /// current logical mini-batch. Key columns are gathered directly from retained Arrow inputs;
    /// only one row per emitted transition is copied into the compact output.
    pub(crate) fn flush_mini_batch(&mut self) -> Result<RecordBatch, DataFusionError> {
        if !self.mini_batch || self.staged_order.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        let order = std::mem::take(&mut self.staged_order);
        let changes = std::mem::take(&mut self.staged_changes);
        let key_batches = std::mem::take(&mut self.staged_key_batches);
        let num_agg = self.kinds.len();
        let mut key_rows: Vec<(usize, usize)> = Vec::new();
        let mut out_results: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_agg];
        let mut out_kinds: Vec<i8> = Vec::new();
        let mut new_cache_bytes = 0usize;

        let mut push = |kind: i8, staged: &StagedGroupChange, values: Vec<ScalarValue>| {
            key_rows.push((staged.key_batch, staged.key_row));
            for (i, value) in values.into_iter().enumerate() {
                out_results[i].push(value);
            }
            out_kinds.push(kind);
        };

        for key in order {
            let staged = &changes[&key];
            let new = self.keys.get(&key).map(|state| output_of(state, &self.result_types));
            match (&staged.old, &new) {
                (None, Some(after)) => push(0, staged, after.clone()),
                (Some(before), None) => push(3, staged, before.clone()),
                (Some(before), Some(after)) if before != after => {
                    if self.generate_update_before {
                        push(1, staged, before.clone());
                    }
                    push(2, staged, after.clone());
                }
                _ => {}
            }
            if let (Some(state), Some(after)) = (self.keys.get_mut(&key), new) {
                state.last_output_bytes = scalar_row_bytes(&after);
                new_cache_bytes += state.last_output_bytes;
                state.last_output = Some(after);
            }
        }
        drop(push);

        let first = &key_batches[0];
        let mut fields: Vec<Field> = self
            .key_columns
            .iter()
            .enumerate()
            .map(|(position, &column)| {
                Field::new(
                    format!("key{position}"),
                    first.column(column).data_type().clone(),
                    true,
                )
            })
            .collect();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.key_columns.len() + num_agg + 1);
        for &column in &self.key_columns {
            let data: Vec<_> = key_batches.iter().map(|batch| batch.column(column).to_data()).collect();
            let refs: Vec<_> = data.iter().collect();
            let mut gathered = MutableArrayData::new(refs, false, key_rows.len());
            for &(batch, row) in &key_rows {
                gathered.extend(batch, row, row + 1);
            }
            columns.push(make_array(gathered.freeze()));
        }
        for (i, result_type) in self.result_types.iter().enumerate() {
            fields.push(Field::new(format!("result{i}"), result_type.clone(), true));
            columns.push(scalars_to_array(std::mem::take(&mut out_results[i]), result_type));
        }
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        self.memory.forget(self.staged_bytes);
        self.staged_bytes = 0;
        if self.memory.tracking() {
            self.memory.record(new_cache_bytes as isize);
        }
        self.memory.account()?;
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build mini-batch group-by changelog"))
    }

    /// Serializes per-key state. A main batch carries `[key0.., records, state{i}, nonnull{i}…]` (the
    /// raw running value and non-null count for SUM/COUNT; a NULL placeholder for MIN/MAX), and a side
    /// batch per MIN/MAX aggregate carries its `[key0.., value, count]` multiset rows.
    pub(crate) fn snapshot(&mut self) -> Vec<u8> {
        let selected: Vec<ByteKey> = self.keys.keys().cloned().collect();
        self.snapshot_keys(&selected)
    }

    fn snapshot_keys(&self, selected: &[ByteKey]) -> Vec<u8> {
        let num_agg = self.kinds.len();
        let mut encoded_keys: Vec<&[u8]> = Vec::new();
        let mut records: Vec<i64> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_agg];
        let mut non_null_columns: Vec<Vec<i64>> = vec![Vec::new(); num_agg];
        let mut multiset_keys: Vec<Vec<&[u8]>> = vec![Vec::new(); num_agg];
        let mut multiset_values: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_agg];
        let mut multiset_counts: Vec<Vec<i64>> = vec![Vec::new(); num_agg];
        for key in selected {
            let state = self
                .keys
                .get(key)
                .expect("snapshot key remains in group state");
            encoded_keys.push(&key.0);
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
                            multiset_keys[i].push(&key.0);
                            multiset_values[i].push(value.scalar(&self.result_types[i]));
                            multiset_counts[i].push(*count);
                        }
                    }
                    GroupAggState::Distinct(counts) => {
                        // The count is recomputed from the side batch on restore (placeholder here).
                        state_columns[i].push(null_scalar(&self.result_types[i]));
                        non_null_columns[i].push(0);
                        for (value, count) in counts.scalar_entries() {
                            multiset_keys[i].push(&key.0);
                            multiset_values[i].push(value); // the distinct value itself
                            multiset_counts[i].push(count);
                        }
                    }
                    GroupAggState::DistinctRunning { counts, .. } => {
                        // The running sum is refolded from the side batch's values on restore.
                        state_columns[i].push(null_scalar(&self.result_types[i]));
                        non_null_columns[i].push(0);
                        for (value, count) in counts.scalar_entries() {
                            multiset_keys[i].push(&key.0);
                            multiset_values[i].push(value);
                            multiset_counts[i].push(count);
                        }
                    }
                }
            }
        }

        let mut fields = vec![Field::new("binary_key", DataType::Binary, false)];
        let mut columns: Vec<ArrayRef> = vec![Arc::new(
            arrow::array::BinaryArray::from_iter_values(encoded_keys.iter().copied()),
        )];
        fields.push(Field::new("records", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(records)));
        for i in 0..num_agg {
            fields.push(Field::new(
                format!("state{i}"),
                self.state_types[i].clone(),
                true,
            ));
            columns.push(scalars_to_array(
                std::mem::take(&mut state_columns[i]),
                &self.state_types[i],
            ));
            fields.push(Field::new(format!("nonnull{i}"), DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(std::mem::take(
                &mut non_null_columns[i],
            ))));
        }
        let mut batches =
            vec![RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("main snapshot")];
        for i in 0..num_agg {
            if matches!(self.kinds[i], 1 | 2 | 7 | 9) {
                let mut f = vec![Field::new("binary_key", DataType::Binary, false)];
                let mut c: Vec<ArrayRef> = vec![Arc::new(
                    arrow::array::BinaryArray::from_iter_values(multiset_keys[i].iter().copied()),
                )];
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
                c.push(Arc::new(Int64Array::from(std::mem::take(
                    &mut multiset_counts[i],
                ))));
                batches.push(
                    RecordBatch::try_new(Arc::new(Schema::new(f)), c).expect("multiset snapshot"),
                );
            }
        }
        write_framed(&batches)
    }

    /// Returns the non-empty Flink key groups in this checkpoint. The companion payload method
    /// reuses the materialized map, so the JVM writes each raw key group without re-walking state.
    pub(crate) fn snapshot_key_groups(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> Vec<i32> {
        self.materialize_raw_keyed_snapshots(max_parallelism, timestamp_precisions);
        self.snapshot_cache
            .as_ref()
            .expect("raw keyed snapshot cache")
            .snapshots
            .keys()
            .copied()
            .collect()
    }

    /// Returns one previously materialized key-group payload. Calling this for an empty group is a
    /// caller error: raw keyed state deliberately omits empty key groups from a checkpoint.
    pub(crate) fn snapshot_key_group(
        &mut self,
        key_group: i32,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> Vec<u8> {
        self.materialize_raw_keyed_snapshots(max_parallelism, timestamp_precisions);
        self.snapshot_cache
            .as_ref()
            .expect("raw keyed snapshot cache")
            .snapshots
            .get(&key_group)
            .cloned()
            .expect("requested non-empty raw keyed group")
    }

    fn materialize_raw_keyed_snapshots(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) {
        assert_eq!(self.key_timestamp_precisions, timestamp_precisions);
        if self.snapshot_cache.as_ref().is_some_and(|cache| {
            cache.max_parallelism == max_parallelism
                && cache.timestamp_precisions.as_slice() == timestamp_precisions
        }) {
            return;
        }

        let selected: Vec<ByteKey> = self.keys.keys().cloned().collect();
        let mut keys_by_group: std::collections::BTreeMap<i32, Vec<ByteKey>> =
            std::collections::BTreeMap::new();
        for key in selected {
            let group = flink_key_group(hash_bytes_by_words(&key.0), max_parallelism) as i32;
            keys_by_group.entry(group).or_default().push(key);
        }

        let snapshots = keys_by_group
            .iter()
            .map(|(&group, keys)| (group, self.snapshot_keys(keys)))
            .collect();
        self.snapshot_cache = Some(GroupSnapshotCache {
            max_parallelism,
            timestamp_precisions: timestamp_precisions.to_vec(),
            snapshots,
        });
    }

    pub(crate) fn restore(
        kinds: Vec<i64>,
        value_types: Vec<i64>,
        value_columns: Vec<i64>,
        key_columns: Vec<usize>,
        generate_update_before: bool,
        bytes: &[u8],
    ) -> Self {
        let mut aggregator = GroupAggregator::new(
            kinds,
            value_types,
            value_columns,
            key_columns,
            generate_update_before,
        );
        let num_agg = aggregator.kinds.len();
        let batches = read_framed(bytes);
        if batches.is_empty() {
            return aggregator;
        }
        // Main batch: BinaryRow key, records, then (state, nonnull) per aggregate.
        let main = &batches[0];
        assert_eq!(main.num_columns(), 2 + 2 * num_agg, "group snapshot schema");
        let keys = main
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .expect("group snapshot binary keys");
        let records = column_i64(main, "records");
        for row in 0..main.num_rows() {
            let key = ByteKey::from(keys.value(row));
            let state = aggregator.state(key);
            state.records = records.value(row);
            for i in 0..num_agg {
                if let GroupAggState::Running { agg, non_null } = &mut state.aggs[i] {
                    let scalar = ScalarValue::try_from_array(main.column(2 + 2 * i), row)
                        .expect("group state scalar");
                    agg.restore_value(&scalar);
                    *non_null = main
                        .column(3 + 2 * i)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("nonnull int64")
                        .value(row);
                }
            }
        }
        // One side batch per MIN/MAX or DISTINCT aggregate: BinaryRow key, value, count.
        let mut frame = 1;
        for i in 0..num_agg {
            if !matches!(aggregator.kinds[i], 1 | 2 | 7 | 9) {
                continue;
            }
            let side = &batches[frame];
            frame += 1;
            assert_eq!(side.num_columns(), 3, "group side snapshot schema");
            let keys = side
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .expect("group side snapshot binary keys");
            let values = side.column(1);
            let counts = column_i64(side, "count");
            for row in 0..side.num_rows() {
                let key = keys.value(row);
                let value = ScalarValue::try_from_array(values, row).expect("multiset value");
                match aggregator.keys.get_mut(key).map(|s| &mut s.aggs[i]) {
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

    /// Rebuilds a single in-memory aggregator from the disjoint raw keyed-state payloads assigned
    /// to this subtask after restore/rescale.
    pub(crate) fn restore_partitions(
        kinds: Vec<i64>,
        value_types: Vec<i64>,
        value_columns: Vec<i64>,
        key_columns: Vec<usize>,
        generate_update_before: bool,
        snapshots: &[Vec<u8>],
    ) -> Self {
        let mut merged = GroupAggregator::new(
            kinds.clone(),
            value_types.clone(),
            value_columns.clone(),
            key_columns.clone(),
            generate_update_before,
        );
        for bytes in snapshots {
            let restored = GroupAggregator::restore(
                kinds.clone(),
                value_types.clone(),
                value_columns.clone(),
                key_columns.clone(),
                generate_update_before,
                bytes,
            );
            merged.keys.extend(restored.keys);
        }
        merged
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
    // Per-aggregate FILTER column index (the boolean the host computes for `AGG(x) FILTER (WHERE p)`),
    // or -1 for an unfiltered aggregate. A row folds into aggregate i only when its filter is TRUE;
    // the global merge is filter-blind because the partials are already filtered here.
    filter_columns: Vec<i64>,
    key_columns: Vec<usize>,
    result_types: Vec<DataType>,
    // Per distinct view column (trailing the partials, in Flink's declared order), the index of the
    // aggregate whose distinct set backs it — the flush emits that set's (value, count) entries as a
    // list column for the global to merge. Empty when no aggregate is distinct.
    distinct_view_sources: Vec<i64>,
    order: Vec<LocalGroupKey>,
    states: HashMap<ByteKey, LocalGroupEntry>,
    scalar_states: HashMap<GroupKey, LocalGroupEntry>,
    key_converter: Option<RowConverter>,
    key_batches: Vec<RecordBatch>,
    scalar_key_mode: Option<bool>,
    key_types: Vec<DataType>,
    pub(crate) memory: OperatorMemory,
}

struct LocalGroupEntry {
    states: Vec<GroupAggState>,
    key_batch: usize,
    key_row: usize,
}

enum LocalGroupKey {
    Byte(ByteKey),
    Scalar(GroupKey),
}

/// Estimated footprint of one buffered local-aggregate entry: the key is held twice (the states map
/// and the first-appearance order), plus the per-aggregate partial states.
fn local_entry_state_bytes(entry: &LocalGroupEntry) -> usize {
    std::mem::size_of::<LocalGroupEntry>()
        + entry
            .states
            .iter()
            .map(group_agg_state_bytes)
            .sum::<usize>()
}

/// The struct fields of one distinct-view entry: the distinct value and its in-bundle multiplicity.
fn distinct_entry_fields(value_type: &DataType) -> arrow::datatypes::Fields {
    vec![
        Arc::new(Field::new("value", value_type.clone(), true)),
        Arc::new(Field::new("count", DataType::Int64, false)),
    ]
    .into()
}

impl LocalGroupAggregator {
    pub(crate) fn new(
        kinds: Vec<i64>,
        value_types: Vec<i64>,
        value_columns: Vec<i64>,
        filter_columns: Vec<i64>,
        key_columns: Vec<usize>,
        distinct_view_sources: Vec<i64>,
    ) -> Self {
        let value_types: Vec<DataType> = value_types.iter().map(|&c| value_data_type(c)).collect();
        let result_types = kinds
            .iter()
            .zip(&value_types)
            .map(|(&kind, vt)| RunningAgg::new(kind, vt).result_type())
            .collect();
        let filter_columns = if filter_columns.is_empty() {
            vec![-1; kinds.len()]
        } else {
            filter_columns
        };
        LocalGroupAggregator {
            kinds,
            value_types,
            value_columns,
            filter_columns,
            key_columns,
            result_types,
            distinct_view_sources,
            order: Vec::new(),
            states: HashMap::default(),
            scalar_states: HashMap::default(),
            key_converter: None,
            key_batches: Vec::new(),
            scalar_key_mode: None,
            key_types: Vec::new(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds the buffered partials by a managed-memory budget (negative = unaccounted). The buffer
    /// drains at every mini-batch flush, but a high-cardinality interval can still spike.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let current = self
            .states
            .iter()
            .map(|(key, entry)| byte_key_bytes(&key.0) * 2 + local_entry_state_bytes(entry))
            .sum::<usize>()
            + self
                .scalar_states
                .iter()
                .map(|(key, entry)| group_key_bytes(key) * 2 + local_entry_state_bytes(entry))
                .sum::<usize>();
        self.memory
            .attach("local-group-aggregate", budget_bytes, current)?;
        Ok(self)
    }

    /// Folds the batch's rows into the buffered per-key accumulators, honoring each row's `RowKind`
    /// when the input is a retracting changelog (a -U/-D subtracts, exactly Flink's local retract
    /// path — the admitted COUNT/AVG accumulators are layout-invariant under retraction, so a
    /// bundle's partial can go negative and the global's merge folds it back out). Nothing is
    /// emitted until a flush.
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
                    DataType::Int64 => {
                        ValueColumn::I64(column.as_any().downcast_ref().expect("int64 value"))
                    }
                    DataType::Int32 => {
                        ValueColumn::I32(column.as_any().downcast_ref().expect("int32 value"))
                    }
                    DataType::Int16 => {
                        ValueColumn::I16(column.as_any().downcast_ref().expect("int16 value"))
                    }
                    DataType::Int8 => {
                        ValueColumn::I8(column.as_any().downcast_ref().expect("int8 value"))
                    }
                    DataType::Float64 => {
                        ValueColumn::F64(column.as_any().downcast_ref().expect("float64 value"))
                    }
                    DataType::Float32 => {
                        ValueColumn::F32(column.as_any().downcast_ref().expect("float32 value"))
                    }
                    DataType::Decimal128(_, _) => ValueColumn::Decimal128(
                        column.as_any().downcast_ref().expect("decimal128 value"),
                    ),
                    _ => ValueColumn::NullOnly(column),
                })
            })
            .collect();
        let key_arrays: Vec<&ArrayRef> =
            self.key_columns.iter().map(|&i| batch.column(i)).collect();
        self.key_types = key_types(&key_arrays);
        // Arrow-row amortizes key encoding across a real bundle. For a stream of single-row physical
        // batches, preserve the cheaper direct ScalarValue path. Pin the representation until flush
        // so later physical batches in the same logical bundle probe the same state map.
        let scalar_key_mode = *self.scalar_key_mode.get_or_insert(n == 1);
        let arrow_keys =
            (!scalar_key_mode).then(|| encode_keys(&mut self.key_converter, &key_arrays, n));
        let key_batch = self.key_batches.len();
        let mut retained_key_batch = false;
        // Distinct aggregates (kind 7/9) fold the value itself into their per-bundle set, not a Num;
        // a BIGINT value column takes the primitive fast path.
        let distinct_cols: Vec<Option<usize>> = (0..num_agg)
            .map(|i| matches!(self.kinds[i], 7 | 9).then_some(self.value_columns[i] as usize))
            .collect();
        let distinct_i64_cols: Vec<Option<&Int64Array>> = (0..num_agg)
            .map(|i| {
                distinct_cols[i].and_then(|c| batch.column(c).as_any().downcast_ref::<Int64Array>())
            })
            .collect();
        // Per aggregate, a string MIN/MAX value column (kind 1/2 over Utf8) — folded as a scalar
        // into the Extremes multiset, not through the numeric Num path.
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
        // Per aggregate, the FILTER boolean column (None = unfiltered). A row folds into aggregate i
        // only where this is TRUE — NULL or FALSE skips it, matching SQL FILTER / Flink's filterArg.
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
        let row_kinds = row_kind_column(batch);
        let append_only = row_kinds.is_none();
        let track = self.memory.tracking();
        for row in 0..n {
            let entry = if scalar_key_mode {
                let key = read_key(&key_arrays, row);
                if !self.scalar_states.contains_key(&key) {
                    let init: Vec<GroupAggState> = self
                        .kinds
                        .iter()
                        .zip(&self.value_types)
                        .map(|(&kind, vt)| GroupAggState::new_local(kind, vt, append_only))
                        .collect();
                    if track {
                        self.memory.record(
                            (group_key_bytes(&key) * 2 + std::mem::size_of::<LocalGroupEntry>())
                                as isize,
                        );
                    }
                    self.order.push(LocalGroupKey::Scalar(key.clone()));
                    self.scalar_states.insert(
                        key.clone(),
                        LocalGroupEntry {
                            states: init,
                            key_batch,
                            key_row: row,
                        },
                    );
                }
                self.scalar_states
                    .get_mut(&key)
                    .expect("scalar key present")
            } else {
                let row_key = arrow_keys.as_ref().expect("arrow keys configured").row(row);
                let key = row_key.as_ref();
                if !self.states.contains_key(key) {
                    let init: Vec<GroupAggState> = self
                        .kinds
                        .iter()
                        .zip(&self.value_types)
                        .map(|(&kind, vt)| GroupAggState::new_local(kind, vt, append_only))
                        .collect();
                    let owned = ByteKey::from(key);
                    if track {
                        self.memory.record(
                            (byte_key_bytes(key) * 2 + std::mem::size_of::<LocalGroupEntry>())
                                as isize,
                        );
                    }
                    self.order.push(LocalGroupKey::Byte(owned.clone()));
                    self.states.insert(
                        owned,
                        LocalGroupEntry {
                            states: init,
                            key_batch,
                            key_row: row,
                        },
                    );
                    retained_key_batch = true;
                }
                self.states.get_mut(key).expect("byte key present")
            };
            // RowKind: 0 +I, 1 -U, 2 +U, 3 -D (absent column ⇒ INSERT). UB/delete retract; I/UA add.
            let retract = row_kinds.map_or(false, |kinds| matches!(kinds.value(row), 1 | 3));
            let mut delta = 0isize;
            if track {
                delta -= entry
                    .states
                    .iter()
                    .map(group_agg_state_bytes)
                    .sum::<usize>() as isize;
            }
            for i in 0..num_agg {
                if let Some(filter) = filter_cols[i] {
                    if filter.is_null(row) || !filter.value(row) {
                        continue;
                    }
                }
                if let Some(col_idx) = extreme_str_cols[i] {
                    let column = batch.column(col_idx);
                    if !column.is_null(row) {
                        let scalar = ScalarValue::try_from_array(column, row)
                            .expect("extreme string scalar");
                        if retract {
                            entry.states[i].retract_extreme(scalar);
                        } else {
                            entry.states[i].accumulate_extreme(scalar);
                        }
                    }
                    continue;
                }
                if let Some(col_idx) = distinct_cols[i] {
                    if let Some(ints) = distinct_i64_cols[i] {
                        if !ints.is_null(row) {
                            if retract {
                                entry.states[i].retract_distinct_i64(ints.value(row));
                            } else {
                                entry.states[i].accumulate_distinct_i64(ints.value(row));
                            }
                        }
                        continue;
                    }
                    let column = batch.column(col_idx);
                    if !column.is_null(row) {
                        let scalar = ScalarValue::try_from_array(column, row)
                            .expect("distinct value scalar");
                        if retract {
                            entry.states[i].retract_distinct(scalar);
                        } else {
                            entry.states[i].accumulate_distinct(scalar);
                        }
                    }
                    continue;
                }
                match &cols[i] {
                    None => {
                        if retract {
                            entry.states[i].retract(Num::I64(0));
                        } else {
                            entry.states[i].accumulate(Num::I64(0));
                        }
                    }
                    Some(column) => {
                        if let Some(num) = column.at(row) {
                            if retract {
                                entry.states[i].retract(num);
                            } else {
                                entry.states[i].accumulate(num);
                            }
                        }
                    }
                }
            }
            if track {
                delta += entry
                    .states
                    .iter()
                    .map(group_agg_state_bytes)
                    .sum::<usize>() as isize;
                self.memory.record(delta);
            }
        }
        if retained_key_batch && !scalar_key_mode {
            self.key_batches.push(batch.clone());
        }
        self.memory.account()
    }

    /// Emits the buffered partials (`[key0.., partial0.., distinct-view0..]`, in first-appearance
    /// order) and clears the buffer; the key types are retained so an empty flush still carries the
    /// right schema. Each distinct view column carries its bundle set's (value, count) entries as a
    /// list of structs — the wire form of Flink's serialized MapView partial — for the global to
    /// merge with multiplicities.
    pub(crate) fn flush(&mut self) -> RecordBatch {
        let order = std::mem::take(&mut self.order);
        let states = std::mem::take(&mut self.states);
        let scalar_states = std::mem::take(&mut self.scalar_states);
        let key_batches = std::mem::take(&mut self.key_batches);
        let scalar_key_mode = self.scalar_key_mode.take().unwrap_or(false);
        self.memory.set(0);
        self.memory.account_shrink();
        let mut fields = key_fields(&self.key_types);
        let mut columns: Vec<ArrayRef> = if scalar_key_mode {
            let keys: Vec<GroupKey> = order
                .iter()
                .map(|key| match key {
                    LocalGroupKey::Scalar(key) => key.clone(),
                    LocalGroupKey::Byte(_) => unreachable!("scalar bundle contains byte key"),
                })
                .collect();
            key_columns(&keys, &self.key_types)
        } else if order.is_empty() {
            self.key_types.iter().map(new_empty_array).collect()
        } else {
            let mut columns = Vec::with_capacity(
                self.key_columns.len() + self.result_types.len() + self.distinct_view_sources.len(),
            );
            for &column in &self.key_columns {
                let data: Vec<_> = key_batches
                    .iter()
                    .map(|batch| batch.column(column).to_data())
                    .collect();
                let refs: Vec<_> = data.iter().collect();
                let mut gathered = MutableArrayData::new(refs, false, order.len());
                for key in &order {
                    let entry = match key {
                        LocalGroupKey::Byte(key) => &states[key],
                        LocalGroupKey::Scalar(_) => unreachable!("byte bundle contains scalar key"),
                    };
                    gathered.extend(entry.key_batch, entry.key_row, entry.key_row + 1);
                }
                columns.push(make_array(gathered.freeze()));
            }
            columns
        };
        for (i, rt) in self.result_types.iter().enumerate() {
            let scalars: Vec<ScalarValue> = order
                .iter()
                .map(|key| match key {
                    LocalGroupKey::Byte(key) => states[key].states[i].emit(rt),
                    LocalGroupKey::Scalar(key) => scalar_states[key].states[i].emit(rt),
                })
                .collect();
            fields.push(Field::new(format!("partial{i}"), rt.clone(), true));
            columns.push(scalars_to_array(scalars, rt));
        }
        for (v, &source) in self.distinct_view_sources.iter().enumerate() {
            let source = source as usize;
            let value_type = &self.value_types[source];
            let mut offsets: Vec<i32> = Vec::with_capacity(order.len() + 1);
            offsets.push(0);
            let mut values: Vec<ScalarValue> = Vec::new();
            let mut counts: Vec<i64> = Vec::new();
            for key in &order {
                let entry = match key {
                    LocalGroupKey::Byte(key) => &states[key],
                    LocalGroupKey::Scalar(key) => &scalar_states[key],
                };
                for (value, count) in entry.states[source].distinct_entries() {
                    values.push(value);
                    counts.push(count);
                }
                offsets.push(values.len() as i32);
            }
            let entries = arrow::array::StructArray::new(
                distinct_entry_fields(value_type),
                vec![
                    scalars_to_array(values, value_type),
                    Arc::new(Int64Array::from(counts)),
                ],
                None,
            );
            let list = arrow::array::ListArray::new(
                Arc::new(Field::new("item", entries.data_type().clone(), false)),
                arrow::buffer::OffsetBuffer::new(offsets.into()),
                Arc::new(entries),
                None,
            );
            fields.push(Field::new(
                format!("distinct{v}"),
                list.data_type().clone(),
                false,
            ));
            columns.push(Arc::new(list));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build local group-by partial batch")
    }
}

state_bytes_getter!(
    Java_io_github_jordepic_streamfusion_Native_groupAggregatorStateBytes,
    GroupAggregator
);

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_groupAggregatorStagingBytes<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let aggregator = unsafe { &*(handle as *const GroupAggregator) };
    aggregator.staging_bytes() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_groupAggregatorStagedKeys<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let aggregator = unsafe { &*(handle as *const GroupAggregator) };
    aggregator.staged_keys() as jlong
}

state_bytes_getter!(
    Java_io_github_jordepic_streamfusion_Native_localGroupAggregatorStateBytes,
    LocalGroupAggregator
);

/// Creates a buffering local two-phase GROUP BY pre-aggregate and returns an opaque handle. It
/// accumulates across batches in memory until flushed; the buffer is transient (drained before each
/// checkpoint on the JVM side), so there is no snapshot/restore.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createLocalGroupAggregator<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    aggregate_kinds: JIntArray<'local>,
    value_types: JIntArray<'local>,
    value_columns: JIntArray<'local>,
    filter_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    distinct_view_sources: JIntArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let value_columns = read_int_array(&env, &value_columns);
    let filter_columns = read_int_array(&env, &filter_columns);
    let key_cols = read_columns(&env, &key_columns);
    let view_sources = read_int_array(&env, &distinct_view_sources);
    let aggregator = LocalGroupAggregator::new(
        kinds,
        value_types,
        value_columns,
        filter_columns,
        key_cols,
        view_sources,
    )
    .with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}

/// Folds an Arrow batch the JVM exported into the buffered per-key accumulators; emits nothing.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updateLocalGroupAggregator<
    'local,
>(
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
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushLocalGroupAggregator<
    'local,
>(
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
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeLocalGroupAggregator<
    'local,
>(
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
    key_timestamp_precisions: JIntArray<'local>,
    filter_columns: JIntArray<'local>,
    count_columns: JIntArray<'local>,
    distinct_view_columns: JIntArray<'local>,
    record_count_column: jint,
    generate_update_before: jboolean,
    mini_batch: jboolean,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let value_columns = read_int_array(&env, &value_columns);
    let filter_columns = read_int_array(&env, &filter_columns);
    let count_columns = read_int_array(&env, &count_columns);
    let distinct_view_columns = read_int_array(&env, &distinct_view_columns);
    let key_columns = read_columns(&env, &key_columns);
    let key_timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let mut aggregator = GroupAggregator::new(
        kinds,
        value_types,
        value_columns,
        key_columns,
        generate_update_before != 0,
    )
    .with_key_timestamp_precisions(key_timestamp_precisions)
    .with_filter_columns(filter_columns)
    .with_count_columns(count_columns)
    .with_distinct_view_columns(distinct_view_columns)
    .with_record_count_column(record_count_column as i64);
    if mini_batch != 0 {
        aggregator = aggregator.with_mini_batch();
    }
    let aggregator = aggregator.with_memory_budget(memory_budget_bytes);
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

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushGroupAggregator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut GroupAggregator) };
    match aggregator.flush_mini_batch() {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Serializes the aggregator's per-key state for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotGroupAggregator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let aggregator = unsafe { &mut *(handle as *mut GroupAggregator) };
    env.byte_array_from_slice(&aggregator.snapshot())
        .expect("failed to allocate group-by snapshot array")
        .into_raw()
}

/// Lists the non-empty Flink key groups represented by this group aggregator's current state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_groupAggregatorSnapshotKeyGroups<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jintArray {
    let aggregator = unsafe { &mut *(handle as *mut GroupAggregator) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let groups = aggregator.snapshot_key_groups(max_parallelism as usize, &precisions);
    let output = env
        .new_int_array(groups.len() as i32)
        .expect("allocate raw group key-group list");
    env.set_int_array_region(&output, 0, &groups)
        .expect("write raw group key-group list");
    output.into_raw()
}

/// Returns one raw keyed-state payload after `groupAggregatorSnapshotKeyGroups` materialized the
/// checkpoint's disjoint group snapshots.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotGroupAggregatorKeyGroup<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key_group: jint,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jbyteArray {
    let aggregator = unsafe { &mut *(handle as *mut GroupAggregator) };
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let snapshot = aggregator.snapshot_key_group(key_group, max_parallelism as usize, &precisions);
    env.byte_array_from_slice(&snapshot)
        .expect("allocate raw group key-group snapshot")
        .into_raw()
}

/// Rebuilds a `GROUP BY` aggregator from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreGroupAggregator<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    aggregate_kinds: JIntArray<'local>,
    value_types: JIntArray<'local>,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    filter_columns: JIntArray<'local>,
    count_columns: JIntArray<'local>,
    distinct_view_columns: JIntArray<'local>,
    record_count_column: jint,
    generate_update_before: jboolean,
    mini_batch: jboolean,
    snapshot: JByteArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let value_columns = read_int_array(&env, &value_columns);
    let filter_columns = read_int_array(&env, &filter_columns);
    let count_columns = read_int_array(&env, &count_columns);
    let distinct_view_columns = read_int_array(&env, &distinct_view_columns);
    let key_columns = read_columns(&env, &key_columns);
    let key_timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let bytes = env
        .convert_byte_array(&snapshot)
        .expect("failed to read group-by snapshot");
    let mut aggregator = GroupAggregator::restore(
        kinds,
        value_types,
        value_columns,
        key_columns,
        generate_update_before != 0,
        &bytes,
    )
    .with_key_timestamp_precisions(key_timestamp_precisions)
    .with_filter_columns(filter_columns)
    .with_count_columns(count_columns)
    .with_distinct_view_columns(distinct_view_columns)
    .with_record_count_column(record_count_column as i64);
    if mini_batch != 0 {
        aggregator = aggregator.with_mini_batch();
    }
    let aggregator = aggregator.with_memory_budget(memory_budget_bytes);
    boxed_or_throw(&mut env, aggregator)
}

/// Rebuilds a `GROUP BY` aggregator from all raw keyed-state partitions assigned to this subtask.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreGroupAggregatorPartitions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    aggregate_kinds: JIntArray<'local>,
    value_types: JIntArray<'local>,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    filter_columns: JIntArray<'local>,
    count_columns: JIntArray<'local>,
    distinct_view_columns: JIntArray<'local>,
    record_count_column: jint,
    generate_update_before: jboolean,
    mini_batch: jboolean,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let value_columns = read_int_array(&env, &value_columns);
    let filter_columns = read_int_array(&env, &filter_columns);
    let count_columns = read_int_array(&env, &count_columns);
    let distinct_view_columns = read_int_array(&env, &distinct_view_columns);
    let key_columns = read_columns(&env, &key_columns);
    let key_timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let count = env
        .get_array_length(&snapshots)
        .expect("read raw group partition count");
    let mut restored = Vec::with_capacity(count as usize);
    for index in 0..count {
        let object = env
            .get_object_array_element(&snapshots, index)
            .expect("read raw group partition");
        let bytes = JByteArray::from(object);
        restored.push(
            env.convert_byte_array(&bytes)
                .expect("read raw group partition bytes"),
        );
    }
    let mut aggregator = GroupAggregator::restore_partitions(
        kinds,
        value_types,
        value_columns,
        key_columns,
        generate_update_before != 0,
        &restored,
    )
    .with_key_timestamp_precisions(key_timestamp_precisions)
    .with_filter_columns(filter_columns)
    .with_count_columns(count_columns)
    .with_distinct_view_columns(distinct_view_columns)
    .with_record_count_column(record_count_column as i64);
    if mini_batch != 0 {
        aggregator = aggregator.with_mini_batch();
    }
    let aggregator = aggregator.with_memory_budget(memory_budget_bytes);
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
