use crate::*;

mod decimal_map_order;
mod distinct;
mod java_map_order;
use decimal_map_order::DecimalMapOrder;
use java_map_order::JavaMapOrder;
mod ordered_value;
pub(crate) use distinct::DistinctSet;
use distinct::{DistinctColumn, DistinctValues};
use ordered_value::{is_ordered_value, OrderedValueState};

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

// Bit-pattern hash, consistent with the total_cmp equality (equal iff same bits).
impl std::hash::Hash for OrdF64 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

/// A MIN/MAX value used as an ordered multiset key. Each aggregate only ever stores one variant (its
/// fixed value type), so the derived cross-variant ordering is never exercised.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum MinMaxKey {
    I64(i64),
    I32(i32),
    I16(i16),
    I8(i8),
    F64(OrdF64),
    // A DECIMAL extreme as its unscaled i128. All values of one aggregate share a scale, so ordering
    // the raw i128 is the decimal ordering; the precision/scale are restored from the result type on
    // emit (the key carries only the value, since the fold path has no precision/scale).
    Decimal128(i128),
    // A string extreme. Rust's String ordering is UTF-8 byte-lexicographic, matching Flink's
    // BinaryStringData byte comparison (its common binary path) — see divergences/07 for the
    // supplementary-plane edge where Flink's materialized-Java-object path would differ.
    Str(String),
    // i128 nanoseconds cover the complete Flink i64-millisecond range without truncation.
    Timestamp(i128),
    Date(i32),
    Time(i32),
    Boolean(bool),
}

impl MinMaxKey {
    fn of(num: Num) -> Self {
        match num {
            Num::I64(v) => MinMaxKey::I64(v),
            Num::I32(v) => MinMaxKey::I32(v),
            Num::I16(v) => MinMaxKey::I16(v),
            Num::I8(v) => MinMaxKey::I8(v),
            Num::F64(v) => MinMaxKey::F64(OrdF64(v)),
            Num::I128(v) => MinMaxKey::Decimal128(v),
            Num::F32(_) => {
                unreachable!("float MIN/MAX uses the running path, not the Extremes multiset")
            }
        }
    }

    fn from_scalar(scalar: &ScalarValue) -> Self {
        match scalar {
            ScalarValue::Date32(Some(v)) => MinMaxKey::Date(*v),
            ScalarValue::Time32Millisecond(Some(v)) => MinMaxKey::Time(*v),
            ScalarValue::Boolean(Some(v)) => MinMaxKey::Boolean(*v),
            ScalarValue::Int64(Some(v)) => MinMaxKey::I64(*v),
            ScalarValue::Int32(Some(v)) => MinMaxKey::I32(*v),
            ScalarValue::Int16(Some(v)) => MinMaxKey::I16(*v),
            ScalarValue::Int8(Some(v)) => MinMaxKey::I8(*v),
            ScalarValue::Float64(Some(v)) => MinMaxKey::F64(OrdF64(*v)),
            ScalarValue::Decimal128(Some(v), _, _) => MinMaxKey::Decimal128(*v),
            ScalarValue::Utf8(Some(v))
            | ScalarValue::LargeUtf8(Some(v))
            | ScalarValue::Utf8View(Some(v)) => MinMaxKey::Str(v.clone()),
            value if streamfusion_bridge::timestamp::is_timestamp(&value.data_type()) => {
                let array = value.to_array().expect("timestamp extreme array");
                let column =
                    streamfusion_bridge::timestamp::TimestampColumn::try_new(array.as_ref())
                        .expect("timestamp extreme column");
                MinMaxKey::Timestamp(column.value(0).expect("timestamp extreme value").nanos())
            }
            other => panic!("unexpected MIN/MAX value scalar: {other:?}"),
        }
    }

    /// Rebuilds the scalar; a decimal extreme takes its precision/scale from `result_type`.
    fn scalar(&self, result_type: &DataType) -> ScalarValue {
        match self {
            MinMaxKey::Date(v) => ScalarValue::Date32(Some(*v)),
            MinMaxKey::Time(v) => ScalarValue::Time32Millisecond(Some(*v)),
            MinMaxKey::Boolean(v) => ScalarValue::Boolean(Some(*v)),
            MinMaxKey::I64(v) => ScalarValue::Int64(Some(*v)),
            MinMaxKey::I32(v) => ScalarValue::Int32(Some(*v)),
            MinMaxKey::I16(v) => ScalarValue::Int16(Some(*v)),
            MinMaxKey::I8(v) => ScalarValue::Int8(Some(*v)),
            MinMaxKey::F64(v) => ScalarValue::Float64(Some(v.0)),
            MinMaxKey::Decimal128(v) => match result_type {
                DataType::Decimal128(p, s) => ScalarValue::Decimal128(Some(*v), *p, *s),
                other => panic!("decimal MIN/MAX result type must be Decimal128, got {other:?}"),
            },
            MinMaxKey::Str(v) => ScalarValue::Utf8(Some(v.clone())),
            MinMaxKey::Timestamp(v) => {
                ScalarValue::Struct(Arc::new(streamfusion_bridge::timestamp::timestamp_array([
                    Some(
                        streamfusion_bridge::timestamp::TimestampValue::from_nanos(*v)
                            .expect("timestamp extreme range"),
                    ),
                ])))
            }
        }
    }
}

/// Records a touched multiset element into an armed journal (no-op when disarmed). The journal is
/// boxed so a disarmed (memory-backend) aggregate pays one pointer, keeping the enum's layout —
/// and every running-only fold's cache footprint — what it was before journals existed.
fn note<T: std::hash::Hash + Eq + Clone>(journal: &mut Option<Box<ahash::HashSet<T>>>, key: &T) {
    if let Some(journal) = journal {
        if !journal.contains(key) {
            journal.insert(key.clone());
        }
    }
}

#[cfg(test)]
mod distinct_set_tests {
    use super::*;

    #[test]
    fn decimal_view_repeated_counts_preserve_null_and_filtered_arrival_order() {
        let mut buffer = DecimalViewBuffer::new(0);
        let mut expected = DecimalMapOrder::new(0);
        for (value, count) in [
            (Some(5), 0),
            (Some(8), 1),
            (Some(5), 3),
            (None, 0),
            (Some(8), 0),
            (Some(8), 2),
            (Some(11), -1),
            (Some(11), 4),
        ] {
            expected.insert_optional(value);
            buffer.merge(value, count);
            assert_eq!(
                buffer.order.iter().collect::<Vec<_>>(),
                expected.iter().collect::<Vec<_>>()
            );
        }
        assert_eq!(buffer.counts.len(), 3);
        assert_eq!(buffer.counts[&5], 3);
        assert_eq!(buffer.counts[&8], 3);
        assert_eq!(buffer.counts[&11], 4);
    }

    #[test]
    fn nan_payloads_share_counts_through_merge_restore_and_retraction() {
        for (datatype, first, second, zero, negative_zero) in [
            (
                DataType::Float64,
                ScalarValue::Float64(Some(f64::from_bits(0x7ff8000000000001))),
                ScalarValue::Float64(Some(f64::from_bits(0xfff8000000000002))),
                ScalarValue::Float64(Some(0.0)),
                ScalarValue::Float64(Some(-0.0)),
            ),
            (
                DataType::Float32,
                ScalarValue::Float32(Some(f32::from_bits(0x7fc00001))),
                ScalarValue::Float32(Some(f32::from_bits(0xffc00002))),
                ScalarValue::Float32(Some(0.0)),
                ScalarValue::Float32(Some(-0.0)),
            ),
        ] {
            let mut set = DistinctSet::new(&datatype);
            assert!(set.add_scalar(first.clone()));
            assert!(!set.add_scalar_n(second.clone(), 2));
            assert_eq!(set.len(), 1);
            let mut restored = DistinctSet::new(&datatype);
            for (value, count) in set.scalar_entries() {
                restored.insert_restored(value, count);
            }
            assert!(!restored.remove_scalar(&second));
            assert!(!restored.remove_scalar(&first));
            assert!(restored.remove_scalar(&second));
            assert!(restored.is_empty());
            assert!(restored.add_scalar(zero));
            assert!(restored.add_scalar(negative_zero));
            assert_eq!(restored.len(), 2);
        }
    }

    #[test]
    fn planner_default_i64_set_promotes_for_time_values() {
        let one = ScalarValue::Time32Second(Some(1));
        let two = ScalarValue::Time32Second(Some(2));
        let mut set = DistinctSet::new(&DataType::Int64);

        assert!(set.add_scalar(one.clone()));
        assert!(!set.add_scalar(one.clone()));
        assert!(set.add_scalar(two));
        assert_eq!(set.len(), 2);
        assert!(!set.remove_scalar(&one));
        assert!(set.remove_scalar(&one));
        assert_eq!(set.len(), 1);

        let mut restored = DistinctSet::new(&DataType::Int64);
        restored.insert_restored(ScalarValue::Time32Second(Some(3)), 2);
        assert_eq!(restored.len(), 1);
    }
}

/// Per-(key, aggregate) state. SUM and COUNT fold/retract a single running value (SUM also keeps a
/// non-null count so it reports NULL once fully retracted). Retracting MIN/MAX cannot use a single
/// value, so they keep a value→count multiset and read the extreme off its ends — what makes them
/// retractable (Flink's `*WithRetractAccumulator` uses a `MapView`; Arroyo calls this the batch state).
///
/// On the memory backend a multiset is fully resident. On a per-element persistent backend
/// (journal armed) the resident multiset is a PARTIAL view — exactly the elements the bundle's
/// batches touch, point-read from the store — so every aggregate also maintains the running value
/// its emit needs (the distinct cardinality, the SUM(DISTINCT) fold, the current extreme), which
/// the backend persists in the main row.
pub(crate) enum GroupAggState {
    Ordered(OrderedValueState),
    Running {
        agg: RunningAgg,
        non_null: i64,
    },
    Extremes {
        is_min: bool,
        counts: BTreeMap<MinMaxKey, i64>,
        // Change journal for a per-element persistent backend (see DistinctSet's journal field).
        // Armed also means partial-view mode: retracted-to-zero entries stay as tombstones (the
        // committed row still exists until the bundle commit) and `extreme` drives the emit.
        journal: Option<Box<ahash::HashSet<MinMaxKey>>>,
        // Partial-view mode: the current live extreme (`None` = empty multiset), maintained
        // incrementally and persisted in the main row. Unused on the memory backend, whose full
        // multiset answers the emit directly.
        extreme: Option<MinMaxKey>,
        // Partial-view mode: a retraction removed the current extreme; the backend must reseek
        // the committed table before the next emit.
        stale: bool,
    },
    // COUNT(DISTINCT x): a value→multiplicity map (Flink's DistinctAccumulator MapView) plus the
    // running number of live entries; a value's multiplicity tracks how many input rows carry it so
    // a retraction removes it only when the last one is retracted. Nulls are never inserted.
    Distinct {
        set: DistinctSet,
        live: i64,
    },
    // SUM/AVG(DISTINCT x): a value→multiplicity map plus a running accumulator folded when a value
    // enters the set and retracted only when its last occurrence leaves — Flink's DistinctAccumulator
    // wrapping the SUM accumulator, kept incremental so the emit stays O(1).
    DistinctRunning {
        counts: DistinctSet,
        agg: RunningAgg,
        live: i64,
    },
}

fn emit_running(agg: &RunningAgg, count: i64, result_type: &DataType) -> ScalarValue {
    match agg {
        // AVG partials carry an accumulator, not an evaluated result. A -U/+U bundle
        // can have a zero net count but a nonzero sum adjustment for the global merge.
        RunningAgg::Count(_)
        | RunningAgg::AvgPartialSumInt(_)
        | RunningAgg::AvgPartialSumFloat(_)
        | RunningAgg::AvgPartialSumDecimal { .. } => agg.emit(),
        _ if count == 0 => null_scalar(result_type),
        // AVG divides the running sum by the live non-null count, truncating toward zero for an
        // integer result (Flink's div) and casting back to the input type — see AvgAggFunction.
        RunningAgg::AvgInt { sum, result } => avg_int_scalar(*sum / count, result),
        RunningAgg::AvgFloat { sum, result } => avg_float_scalar(*sum / count as f64, result),
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
                let (unscaled, qscale) = quotient_38_digits(*sum, *scale, count as i128, 0);
                ScalarValue::Decimal128(
                    rescale_half_up(unscaled, qscale, 38, result_scale),
                    38,
                    result_scale,
                )
            }
        }
        _ => agg.emit(),
    }
}

impl GroupAggState {
    fn new(kind: i64, value_type: &DataType) -> Self {
        match kind {
            12..=16 | 19..=21 => Self::Ordered(OrderedValueState::new(kind, value_type)),
            1 => GroupAggState::Extremes {
                is_min: true,
                counts: BTreeMap::new(),
                journal: None,
                extreme: None,
                stale: false,
            }, // MIN
            2 => GroupAggState::Extremes {
                is_min: false,
                counts: BTreeMap::new(),
                journal: None,
                extreme: None,
                stale: false,
            }, // MAX
            7 => GroupAggState::Distinct {
                set: DistinctSet::new(value_type),
                live: 0,
            }, // COUNT(DISTINCT)
            // DISTINCT wraps the ordinary SUM/AVG accumulator and folds only set transitions.
            9 | 17 | 18 => GroupAggState::DistinctRunning {
                counts: DistinctSet::new(value_type),
                agg: RunningAgg::new(kind, value_type),
                live: 0,
            },
            _ => GroupAggState::Running {
                agg: RunningAgg::new(kind, value_type),
                non_null: 0,
            },
        }
    }

    fn accumulate_typed_extreme(&mut self, column: &ArrayRef, row: usize) {
        let value = match column.data_type() {
            DataType::Date32 => column
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .unwrap()
                .value(row),
            DataType::Time32(arrow::datatypes::TimeUnit::Millisecond) => column
                .as_any()
                .downcast_ref::<arrow::array::Time32MillisecondArray>()
                .unwrap()
                .value(row),
            DataType::Boolean => i32::from(
                column
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(row),
            ),
            _ => unreachable!("typed running extreme column"),
        };
        self.accumulate(Num::I32(value));
    }

    fn new_local(kind: i64, value_type: &DataType, append_only: bool) -> Self {
        // The local half of an insert-only two-phase aggregate never retracts its inputs. Its
        // fixed-width MIN/MAX partial therefore needs one value, not the global/retracting multiset.
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
                    | DataType::Date32
                    | DataType::Time32(arrow::datatypes::TimeUnit::Millisecond)
                    | DataType::Boolean
            );
        if running_extreme {
            GroupAggState::Running {
                agg: RunningAgg::new(kind + 9, value_type),
                non_null: 0,
            }
        } else {
            Self::new(kind, value_type)
        }
    }

    /// Adds one occurrence of a MIN/MAX extreme to the multiset, maintaining the partial-view
    /// running extreme when the journal is armed.
    fn accumulate_extreme_key(&mut self, key: MinMaxKey) {
        match self {
            GroupAggState::Extremes {
                is_min,
                counts,
                journal,
                extreme,
                stale,
            } => {
                if journal.is_some() && !*stale {
                    let better =
                        extreme
                            .as_ref()
                            .map_or(true, |e| if *is_min { key < *e } else { key > *e });
                    if better {
                        *extreme = Some(key.clone());
                    }
                }
                note(journal, &key);
                *counts.entry(key).or_insert(0) += 1;
            }
            _ => unreachable!("accumulate_extreme on a non-extremes aggregate"),
        }
    }

    /// Removes one occurrence of a MIN/MAX extreme. In partial-view mode a zeroed entry stays as a
    /// tombstone overriding its committed row, and removing the current extreme marks it stale for
    /// the backend's committed-table reseek.
    fn retract_extreme_key(&mut self, key: MinMaxKey) {
        match self {
            GroupAggState::Extremes {
                counts,
                journal,
                extreme,
                stale,
                ..
            } => {
                note(journal, &key);
                if let Some(count) = counts.get_mut(&key) {
                    if *count > 0 {
                        *count -= 1;
                        if *count == 0 {
                            if journal.is_some() {
                                if !*stale && extreme.as_ref() == Some(&key) {
                                    *extreme = None;
                                    *stale = true;
                                }
                            } else {
                                counts.remove(&key);
                            }
                        }
                    }
                }
            }
            _ => unreachable!("retract_extreme on a non-extremes aggregate"),
        }
    }

    fn accumulate(&mut self, value: Num) {
        match self {
            GroupAggState::Running { agg, non_null } => {
                agg.fold(value);
                *non_null += 1;
            }
            GroupAggState::Extremes { .. } => self.accumulate_extreme_key(MinMaxKey::of(value)),
            GroupAggState::Ordered(_)
            | GroupAggState::Distinct { .. }
            | GroupAggState::DistinctRunning { .. } => {
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

    /// Folds a decimal AVG partial whose bundle sum overflowed (a NULL sum at any net count): the
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
            _ => unreachable!("a NULL AVG sum partial is decimal-only"),
        }
    }

    fn retract(&mut self, value: Num) {
        match self {
            GroupAggState::Running { agg, non_null } => {
                agg.retract(value);
                *non_null -= 1;
            }
            GroupAggState::Extremes { .. } => self.retract_extreme_key(MinMaxKey::of(value)),
            GroupAggState::Ordered(_)
            | GroupAggState::Distinct { .. }
            | GroupAggState::DistinctRunning { .. } => {
                unreachable!("distinct retracts a scalar, not a Num")
            }
        }
    }

    /// Adds one occurrence of a non-numeric (string) MIN/MAX extreme, read as a scalar rather than a
    /// Num (the Num path is numeric only). The multiset orders entries by `MinMaxKey`.
    fn accumulate_extreme(&mut self, value: ScalarValue) {
        self.accumulate_extreme_key(MinMaxKey::from_scalar(&value));
    }

    /// Removes one occurrence of a string MIN/MAX extreme (the changelog retraction).
    fn retract_extreme(&mut self, value: ScalarValue) {
        self.retract_extreme_key(MinMaxKey::from_scalar(&value));
    }

    /// Folds one (value, count) entry of a local bundle's distinct view into the merged set — the
    /// two-phase merge. A value newly entering the merged set also
    /// folds once into its running SUM/AVG accumulator, exactly as the per-row path does. The
    /// two-phase distinct input is insert-only (the local's bundle is append-only), so there is no
    /// retracting counterpart.
    fn merge_distinct(&mut self, value: ScalarValue, count: i64) {
        match self {
            GroupAggState::Distinct { set, live } => {
                if set.add_scalar_n(value, count) {
                    *live += 1;
                }
            }
            GroupAggState::DistinctRunning { counts, agg, live } => {
                let num = distinct_num(&value);
                if counts.add_scalar_n(value, count) {
                    agg.fold(num);
                    *live += 1;
                }
            }
            _ => unreachable!("distinct merge on a non-distinct aggregate"),
        }
    }

    fn decimal_distinct_count(&self, value: &ScalarValue) -> i64 {
        let set = match self {
            Self::Distinct { set, .. } => set,
            Self::DistinctRunning { counts, .. } => counts,
            _ => unreachable!("decimal membership on a non-distinct aggregate"),
        };
        set.decimal_count(value)
    }

    /// The current output value; SUM and MIN/MAX report NULL when they hold no live non-null input.
    fn emit(&self, result_type: &DataType) -> ScalarValue {
        match self {
            Self::Ordered(state) => state.emit(),
            GroupAggState::Running { agg, non_null } => emit_running(agg, *non_null, result_type),
            GroupAggState::Extremes {
                is_min,
                counts,
                journal,
                extreme,
                stale,
            } => {
                // Partial view: the maintained extreme (never emitted stale — a killing retraction
                // is resolved against the committed table before any output). Memory: the full
                // multiset's ends.
                let extreme = if journal.is_some() {
                    debug_assert!(!stale, "extremes emitted before the backend reseek");
                    extreme.as_ref()
                } else if *is_min {
                    counts.keys().next()
                } else {
                    counts.keys().next_back()
                };
                extreme.map_or_else(|| null_scalar(result_type), |k| k.scalar(result_type))
            }
            // COUNT(DISTINCT) is the number of live distinct values (never NULL — empty is 0).
            GroupAggState::Distinct { set, live } => {
                debug_assert!(set.journaled() || set.len() as i64 == *live);
                ScalarValue::Int64(Some(*live))
            }
            // SUM/AVG(DISTINCT) use distinct cardinality rather than input row count.
            GroupAggState::DistinctRunning { counts, agg, live } => {
                debug_assert!(counts.journaled() || counts.len() as i64 == *live);
                emit_running(agg, *live, result_type)
            }
        }
    }

    /// Imports one blob side-batch entry — the multiset restore shared by the memory rebuild and
    /// the typed persistent import. With the journal armed (the import), every element is recorded
    /// so the import's commit writes it through, and the running extreme is established for the
    /// main row; a SUM(DISTINCT) refolds each element into its running sum either way.
    fn import_multiset_entry(&mut self, value: ScalarValue, count: i64) {
        match self {
            Self::Ordered(state) => state.restore_entry(value, count),
            GroupAggState::Extremes {
                is_min,
                counts,
                journal,
                extreme,
                stale,
            } => {
                let key = MinMaxKey::from_scalar(&value);
                if journal.is_some() && !*stale {
                    let better =
                        extreme
                            .as_ref()
                            .map_or(true, |e| if *is_min { key < *e } else { key > *e });
                    if better {
                        *extreme = Some(key.clone());
                    }
                }
                note(journal, &key);
                counts.insert(key, count);
            }
            GroupAggState::Distinct { set, live } => {
                set.insert_imported(value, count);
                *live += 1;
            }
            GroupAggState::DistinctRunning { counts, agg, live } => {
                agg.fold(distinct_num(&value));
                counts.insert_imported(value, count);
                *live += 1;
            }
            // An insert-only MIN/MAX (kind 10/11) restoring a blob written by the retractable
            // multiset representation: fold each element back into the running extreme, counting
            // its occurrences so the NULL-when-empty emit stays exact.
            GroupAggState::Running { agg, non_null } => {
                agg.fold(distinct_num(&value));
                *non_null += count;
            }
        }
    }

    /// Arms the multiset change journal for a per-element persistent backend; every later
    /// accumulate/retract records the touched element so the bundle commit writes only what moved,
    /// and the resident multiset becomes a partial view (see the enum doc).
    #[cfg(feature = "rocksdb-state")]
    fn arm_multiset_journal(&mut self) {
        match self {
            GroupAggState::Extremes { journal, .. } => *journal = Some(Box::default()),
            GroupAggState::Distinct { set, .. } => set.arm_journal(),
            GroupAggState::DistinctRunning { counts, .. } => counts.arm_journal(),
            GroupAggState::Ordered(_) | GroupAggState::Running { .. } => {}
        }
    }

    /// Hydrates one point-read committed element into the partial view. The running values (live
    /// count, distinct sum, extreme) come from the main row, so nothing is refolded here.
    #[cfg(feature = "rocksdb-state")]
    fn restore_multiset_entry(&mut self, value: ScalarValue, count: i64) {
        match self {
            GroupAggState::Extremes { counts, .. } => {
                counts.insert(MinMaxKey::from_scalar(&value), count);
            }
            GroupAggState::Distinct { set, .. } => set.insert_restored(value, count),
            GroupAggState::DistinctRunning { counts, .. } => counts.insert_restored(value, count),
            GroupAggState::Ordered(_) | GroupAggState::Running { .. } => {
                unreachable!("multiset restore on an aggregate without a multiset")
            }
        }
    }

    /// Drains the journal into (element, live multiplicity) pairs; `None` marks a persistent-row
    /// delete (an extremes tombstone drains as a delete). `element_type` restores a decimal
    /// extreme's precision/scale.
    #[cfg(feature = "rocksdb-state")]
    fn drain_multiset_changes(
        &mut self,
        element_type: &DataType,
    ) -> Vec<(ScalarValue, Option<i64>)> {
        match self {
            GroupAggState::Extremes {
                counts, journal, ..
            } => journal.as_mut().map_or_else(Vec::new, |journal| {
                journal
                    .drain()
                    .map(|key| {
                        let count = counts.get(&key).copied().filter(|count| *count > 0);
                        (key.scalar(element_type), count)
                    })
                    .collect()
            }),
            GroupAggState::Distinct { set, .. } => set.drain_journal(),
            GroupAggState::DistinctRunning { counts, .. } => counts.drain_journal(),
            GroupAggState::Ordered(_) | GroupAggState::Running { .. } => {
                unreachable!("multiset drain on an aggregate without a multiset")
            }
        }
    }

    /// Whether a retraction this bundle removed the current extreme, so the backend must reseek
    /// the committed table before the next emit.
    #[cfg(feature = "rocksdb-state")]
    fn multiset_extreme_stale(&self) -> bool {
        matches!(self, GroupAggState::Extremes { stale: true, .. })
    }

    /// Re-establishes a killed extreme: the best of the resident live elements and the committed
    /// elements the backend feeds in extreme order. A resident entry — including this bundle's
    /// zeroed tombstones — overrides its committed row, so the first committed element not
    /// resident is the committed side's candidate.
    #[cfg(feature = "rocksdb-state")]
    fn resolve_multiset_extreme(
        &mut self,
        committed: &mut dyn FnMut() -> Result<Option<ScalarValue>, DataFusionError>,
    ) -> Result<(), DataFusionError> {
        let GroupAggState::Extremes {
            is_min,
            counts,
            extreme,
            stale,
            ..
        } = self
        else {
            unreachable!("extreme resolution on a non-extremes aggregate")
        };
        let resident = if *is_min {
            counts.iter().find(|(_, count)| **count > 0)
        } else {
            counts.iter().rev().find(|(_, count)| **count > 0)
        };
        let mut best = resident.map(|(key, _)| key.clone());
        while let Some(scalar) = committed()? {
            let key = MinMaxKey::from_scalar(&scalar);
            if counts.contains_key(&key) {
                continue;
            }
            best = match best.take() {
                None => Some(key),
                Some(current) if (*is_min && key < current) || (!*is_min && key > current) => {
                    Some(key)
                }
                Some(current) => Some(current),
            };
            break;
        }
        *extreme = best;
        *stale = false;
        Ok(())
    }
}

/// Per-key state for a `GROUP BY` group: the per-aggregate state and the live record count (reaching
/// zero deletes the group).
pub(crate) struct GroupKeyState {
    aggs: Vec<GroupAggState>,
    records: i64,
    /// The tuple last emitted for this group — the per-row changelog needs the pre-update value of
    /// every touched group, and caching it halves the output materialization (the q16 profile put
    /// ~half the operator in exactly that scalar build/clone churn). Snapshots omit this cache.
    /// An immediate, unfiltered single-value result also needs no duplicate tuple; when the cache
    /// is absent, the next touch reconstructs the preimage from the aggregate state.
    last_output: Option<Vec<ScalarValue>>,
    /// `scalar_row_bytes` of the cached tuple, maintained alongside it: the state measurement
    /// runs twice per touched row, and re-walking the tuple's scalars each time was a measurable
    /// slice of the accounted aggregates.
    last_output_bytes: usize,
    /// Wall-clock millis of the group's last write (Flink state TTL, `OnCreateAndWrite`); stays 0
    /// while TTL is off.
    last_write_ms: i64,
}

/// The resident default backend for the group store (see `state/` for the seam).
pub(crate) type MemoryGroupStore = MemoryStateStore<GroupKeyState>;

/// The group aggregate's persistent backend: the generic persistent store under the group row codec.

#[cfg(feature = "rocksdb-state")]
pub(crate) type RocksGroupStore = crate::state::RocksStore<GroupStateCodec>;

/// The group aggregate's value codec for the persistent store: one row per group holding the live
/// record count and each aggregate's `(state scalar, non-null count)` pair. A
/// MIN/MAX-with-retraction or DISTINCT aggregate's multiset rides companion element tables (one
/// per such aggregate); its main-row slot carries the running value the emit needs — the distinct
/// cardinality, the SUM(DISTINCT) fold (with the cardinality in the count slot), or the current
/// extreme — so a bundle hydrates only the elements it touches, never the whole multiset.
#[cfg(feature = "rocksdb-state")]
pub(crate) struct GroupStateCodec {
    kinds: Vec<i64>,
    value_types: Vec<DataType>,
    state_types: Vec<DataType>,
    value_columns: Vec<i64>,
    distinct_view_columns: Vec<i64>,
    multiset_aggs: Vec<usize>,
}

#[cfg(feature = "rocksdb-state")]
impl GroupStateCodec {
    pub(crate) fn new(
        kinds: Vec<i64>,
        value_types: Vec<DataType>,
        value_columns: Vec<i64>,
        distinct_view_columns: Vec<i64>,
    ) -> Self {
        let state_types = group_state_types(&kinds, &value_types);
        let multiset_aggs: Vec<usize> = kinds
            .iter()
            .enumerate()
            .filter(|&(_, &kind)| matches!(kind, 1 | 2 | 7 | 9 | 17 | 18))
            .map(|(i, _)| i)
            .collect();
        let distinct_view_columns = if distinct_view_columns.is_empty() {
            vec![-1; kinds.len()]
        } else {
            distinct_view_columns
        };
        GroupStateCodec {
            kinds,
            value_types,
            state_types,
            value_columns,
            distinct_view_columns,
            multiset_aggs,
        }
    }

    /// A MIN/MAX multiset element is the aggregate's state scalar (the extreme's own type); a
    /// DISTINCT element is the distinct value itself.
    fn element_type(&self, agg: usize) -> DataType {
        match self.kinds[agg] {
            1 | 2 => self.state_types[agg].clone(),
            _ => self.value_types[agg].clone(),
        }
    }
}

#[cfg(feature = "rocksdb-state")]
impl crate::state::RocksStateCodec for GroupStateCodec {
    type Value = GroupKeyState;
    fn supported(&self) -> bool {
        crate::state::rocks_group_supported(&self.kinds, &self.value_types, &self.state_types)
    }
    fn multiset_tables(&self) -> Vec<(u8, DataType)> {
        self.multiset_aggs
            .iter()
            .map(|&agg| (1 + agg as u8, self.element_type(agg)))
            .collect()
    }
    fn arm_multisets(&self, state: &mut GroupKeyState) {
        for &agg in &self.multiset_aggs {
            state.aggs[agg].arm_multiset_journal();
        }
    }
    fn restore_multiset_entry(
        &self,
        state: &mut GroupKeyState,
        table: usize,
        element: ScalarValue,
        count: i64,
    ) {
        state.aggs[self.multiset_aggs[table]].restore_multiset_entry(element, count);
    }
    fn drain_multiset_changes(
        &self,
        state: &mut GroupKeyState,
        table: usize,
    ) -> Vec<(ScalarValue, Option<i64>)> {
        let agg = self.multiset_aggs[table];
        state.aggs[agg].drain_multiset_changes(&self.element_type(agg))
    }
    fn multiset_batch_elements(
        &self,
        batch: &RecordBatch,
        table: usize,
    ) -> Option<(ArrayRef, Vec<u32>)> {
        let agg = self.multiset_aggs[table];
        if self.distinct_view_columns[agg] >= 0 {
            // Two-phase distinct merge: the elements are the rows' (value, count) view entries.
            let list = batch
                .column(self.distinct_view_columns[agg] as usize)
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .expect("distinct view column must be a list");
            let entries = list
                .values()
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .expect("distinct view entries must be structs");
            let values = crate::flink_float::canonical_array(entries.column(0));
            let mut rows = vec![u32::MAX; values.len()];
            let offsets = list.value_offsets();
            for row in 0..batch.num_rows() {
                for entry in offsets[row] as usize..offsets[row + 1] as usize {
                    rows[entry] = row as u32;
                }
            }
            return Some((values, rows));
        }
        if self.value_columns[agg] < 0 {
            return None;
        }
        let column = batch.column(self.value_columns[agg] as usize);
        let column = if matches!(self.kinds[agg], 7 | 9 | 17 | 18) {
            crate::flink_float::canonical_array(column)
        } else {
            column.clone()
        };
        let rows = (0..batch.num_rows() as u32).collect();
        Some((column, rows))
    }
    fn multiset_extreme_is_min(&self, table: usize) -> bool {
        self.kinds[self.multiset_aggs[table]] == 1
    }
    fn multiset_extreme_stale(&self, state: &GroupKeyState, table: usize) -> bool {
        state.aggs[self.multiset_aggs[table]].multiset_extreme_stale()
    }
    fn resolve_multiset_extreme(
        &self,
        state: &mut GroupKeyState,
        table: usize,
        committed: &mut dyn FnMut() -> Result<Option<ScalarValue>, DataFusionError>,
    ) -> Result<(), DataFusionError> {
        state.aggs[self.multiset_aggs[table]].resolve_multiset_extreme(committed)
    }
    fn value_fields(&self) -> Vec<(String, DataType)> {
        let mut fields = vec![("records".to_string(), DataType::Int64)];
        for (i, state_type) in self.state_types.iter().enumerate() {
            fields.push((format!("s{i}"), state_type.clone()));
            fields.push((format!("n{i}"), DataType::Int64));
        }
        fields
    }
    fn encode_columns(&self, states: &[&GroupKeyState]) -> Vec<ArrayRef> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(1 + 2 * self.state_types.len());
        columns.push(Arc::new(Int64Array::from_iter_values(
            states.iter().map(|state| state.records),
        )));
        for (i, state_type) in self.state_types.iter().enumerate() {
            // Numeric scalars stay on the stack: no per-group scalar row, transpose, or
            // intermediate Vec<ScalarValue> before Arrow's typed column builder.
            let values = states
                .iter()
                .map(|state| state.aggs[i].persisted_scalar(state_type));
            macro_rules! primitive_column {
                ($array:ty, $scalar:ident) => {
                    Arc::new(<$array>::from_iter(values.map(|value| match value {
                        ScalarValue::$scalar(value) => value,
                        _ => unreachable!("aggregate state type mismatch"),
                    }))) as ArrayRef
                };
            }
            let column = match state_type {
                DataType::Int8 => primitive_column!(Int8Array, Int8),
                DataType::Int16 => primitive_column!(Int16Array, Int16),
                DataType::Int32 => primitive_column!(Int32Array, Int32),
                DataType::Int64 => primitive_column!(Int64Array, Int64),
                DataType::Float32 => primitive_column!(Float32Array, Float32),
                DataType::Float64 => primitive_column!(arrow::array::Float64Array, Float64),
                // Keep the general converter for decimal and string states, including typed
                // NULLs and decimal precision/scale. These still avoid the per-group rows.
                _ => scalars_to_array(values.collect(), state_type),
            };
            columns.push(column);
            columns.push(Arc::new(Int64Array::from_iter_values(
                states.iter().map(|state| state.aggs[i].persisted_count()),
            )));
        }
        columns
    }
    fn decode_row(
        &self,
        columns: &[ArrayRef],
        row: usize,
    ) -> Result<GroupKeyState, DataFusionError> {
        let count = |column: usize| {
            let values = columns[column]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            if values.is_null(row) {
                0
            } else {
                values.value(row)
            }
        };
        let mut aggs = Vec::with_capacity(self.kinds.len());
        for (i, (&kind, value_type)) in self.kinds.iter().zip(&self.value_types).enumerate() {
            let scalar = ScalarValue::try_from_array(&columns[1 + 2 * i], row)?;
            let mut state = GroupAggState::new(kind, value_type);
            match &mut state {
                GroupAggState::Ordered(ordered) => ordered.restore_value(scalar, count(2 + 2 * i)),
                GroupAggState::Running { agg, non_null } => {
                    agg.restore_value(&scalar);
                    *non_null = count(2 + 2 * i);
                }
                GroupAggState::Extremes { extreme, .. } => {
                    if !scalar.is_null() {
                        *extreme = Some(MinMaxKey::from_scalar(&scalar));
                    }
                }
                GroupAggState::Distinct { live, .. } => {
                    if let ScalarValue::Int64(Some(value)) = scalar {
                        *live = value;
                    }
                }
                GroupAggState::DistinctRunning { agg, live, .. } => {
                    agg.restore_value(&scalar);
                    *live = count(2 + 2 * i);
                }
            }
            aggs.push(state);
        }
        Ok(GroupKeyState {
            aggs,
            records: count(0),
            last_output: None,
            last_output_bytes: 0,
            last_write_ms: 0,
        })
    }
    fn value_bytes(&self, state: &GroupKeyState) -> usize {
        group_key_state_bytes(state)
    }
    fn write_ms(&self, state: &GroupKeyState) -> i64 {
        state.last_write_ms
    }
    fn stamp_write_ms(&self, state: &mut GroupKeyState, ts_ms: i64) {
        state.last_write_ms = ts_ms;
    }
}

/// The Arrow type of each aggregate's persisted state scalar (equals the result type except AVG,
/// whose running sum is wider) — the persistent row schema and the checkpoint wire format agree.
pub(crate) fn group_state_types(kinds: &[i64], value_types: &[DataType]) -> Vec<DataType> {
    kinds
        .iter()
        .zip(value_types)
        .map(|(&kind, vt)| {
            if is_ordered_value(kind) {
                vt.clone()
            } else {
                RunningAgg::new(kind, vt).state_type()
            }
        })
        .collect()
}

/// A multiset's main-row slot carries only its running value and count. Its elements remain
/// in companion tables and are hydrated separately for the keys a bundle touches.
#[cfg(feature = "rocksdb-state")]
impl GroupAggState {
    fn persisted_scalar(&self, state_type: &DataType) -> ScalarValue {
        match self {
            Self::Ordered(state) => state.snapshot_value(),
            Self::Running { agg, .. } | Self::DistinctRunning { agg, .. } => agg.emit(),
            Self::Extremes { extreme, stale, .. } => {
                debug_assert!(!stale, "extremes persisted before the backend reseek");
                extreme
                    .as_ref()
                    .map_or_else(|| null_scalar(state_type), |key| key.scalar(state_type))
            }
            Self::Distinct { live, .. } => ScalarValue::Int64(Some(*live)),
        }
    }

    fn persisted_count(&self) -> i64 {
        match self {
            Self::Ordered(state) => state.count(),
            Self::Running { non_null, .. } => *non_null,
            Self::DistinctRunning { live, .. } => *live,
            Self::Extremes { .. } | Self::Distinct { .. } => 0,
        }
    }
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

struct DecimalViewBuffer {
    order: DecimalMapOrder,
    counts: HashMap<i128, i64>,
}

impl DecimalViewBuffer {
    fn new(scale: i8) -> Self {
        Self {
            order: DecimalMapOrder::new(scale),
            counts: HashMap::default(),
        }
    }

    fn merge(&mut self, value: Option<i128>, count: i64) {
        if let Some(value) = value {
            if count > 0 {
                match self.counts.entry(value) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        *entry.get_mut() += count;
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        self.order.insert_optional(Some(value));
                        entry.insert(count);
                    }
                }
                return;
            }
        }
        self.order.insert_optional(value);
    }

    fn bytes(&self) -> usize {
        let buckets = if self.counts.capacity() == 0 {
            0
        } else {
            (self.counts.capacity() * 8).div_ceil(7).next_power_of_two()
        };
        std::mem::size_of::<Self>() + self.order.bytes() - std::mem::size_of::<DecimalMapOrder>()
            + buckets * (std::mem::size_of::<(i128, i64)>() + 1)
    }
}

fn decimal_buffers_bytes(buffers: &[Option<DecimalViewBuffer>]) -> usize {
    buffers.len() * std::mem::size_of::<Option<DecimalViewBuffer>>()
        + buffers
            .iter()
            .flatten()
            .map(|buffer| buffer.bytes() - std::mem::size_of::<DecimalViewBuffer>())
            .sum::<usize>()
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
pub(crate) struct GroupAggregator<S: KeyedStateStore<GroupKeyState> = MemoryGroupStore> {
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
    // Idle-state retention millis (0 = off — Flink's default). With TTL on, a group expires
    // `ttl_ms` after its last write, and the no-change output suppression is disabled: Flink always
    // emits -U/+U under TTL to keep refreshing downstream state.
    ttl_ms: i64,
    ttl_emit_unchanged: bool,
    // When the last full expiry sweep ran; the sweep reclaims groups never touched again, once per
    // TTL period (expiry itself is enforced lazily at each touch).
    last_sweep_ms: i64,
    // Whether any aggregate keeps a retractable MIN/MAX multiset: a retraction can remove the
    // current extreme, which a partial-view backend must re-establish from its committed table
    // before the row's output (a no-op on the resident memory backend).
    has_retractable_extremes: bool,
    // The group store is keyed by Flink BinaryRow bytes. Besides giving equality the same
    // representation as the keyed exchange, this admits Arrow MAP values, which arrow-row cannot
    // encode.
    store: S,
    // Materialized once per checkpoint so the JVM can write one raw keyed-state payload per Flink
    // key group without repeatedly traversing the full native map.
    snapshot_cache: Option<GroupSnapshotCache>,
    mini_batch: bool,
    staged_order: Vec<ByteKey>,
    staged_changes: HashMap<ByteKey, StagedGroupChange>,
    staged_key_batches: Vec<RecordBatch>,
    staged_bytes: usize,
    decimal_views: HashMap<ByteKey, Vec<Option<DecimalViewBuffer>>>,
    deferred_batches: Vec<(RecordBatch, usize)>,
    deferred_keys: HashMap<ByteKey, ()>,
    deferred_bytes: usize,
    draining_deferred: bool,
    last_input_ms: i64,
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
        GroupAggState::Ordered(ordered) => {
            ordered.bytes() - std::mem::size_of::<OrderedValueState>()
        }
        GroupAggState::Running { .. } => 0,
        GroupAggState::Extremes { counts, .. } => counts.len() * MULTISET_ENTRY_BYTES,
        GroupAggState::Distinct { set, .. } => set.len() * MULTISET_ENTRY_BYTES,
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

struct GroupChanges {
    rows: Vec<u32>,
    results: Vec<Vec<ScalarValue>>,
    kinds: Vec<i8>,
}

impl GroupChanges {
    fn push(&mut self, kind: i8, row: usize, values: impl IntoIterator<Item = ScalarValue>) {
        self.rows.push(row as u32);
        for (column, value) in self.results.iter_mut().zip(values) {
            column.push(value);
        }
        self.kinds.push(kind);
    }
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
            .map(|(&kind, vt)| {
                if kind == 21 {
                    DataType::Int32
                } else if is_ordered_value(kind) {
                    vt.clone()
                } else {
                    RunningAgg::new(kind, vt).result_type()
                }
            })
            .collect();
        let state_types = group_state_types(&kinds, &value_types);
        let filter_columns = vec![-1; kinds.len()];
        let count_columns = vec![-1; kinds.len()];
        let distinct_view_columns = vec![-1; kinds.len()];
        let has_retractable_extremes = kinds.iter().any(|kind| matches!(kind, 1 | 2));
        GroupAggregator {
            has_retractable_extremes,
            kinds,
            value_types,
            result_types,
            state_types,
            value_columns,
            key_timestamp_precisions: vec![-1; key_columns.len()],
            key_columns,
            generate_update_before,
            ttl_ms: 0,
            ttl_emit_unchanged: true,
            last_sweep_ms: 0,
            store: MemoryGroupStore::default(),
            snapshot_cache: None,
            mini_batch: false,
            staged_order: Vec::new(),
            staged_changes: HashMap::default(),
            staged_key_batches: Vec::new(),
            staged_bytes: 0,
            decimal_views: HashMap::default(),
            deferred_batches: Vec::new(),
            deferred_keys: HashMap::default(),
            deferred_bytes: 0,
            draining_deferred: false,
            last_input_ms: 0,
            filter_columns,
            count_columns,
            distinct_view_columns,
            record_count_column: -1,
            memory: OperatorMemory::unaccounted(),
        }
    }

    /// Bounds this aggregator's state by the operator's task off-heap budget (negative =
    /// unaccounted), accounting any restored groups immediately.
    pub(crate) fn with_memory_budget(mut self, budget_bytes: i64) -> Result<Self, DataFusionError> {
        let state: usize = self
            .store
            .iter()
            .map(|(key, state)| byte_key_bytes(&key.0) + group_key_state_bytes(state))
            .sum();
        self.memory.attach("group-aggregate", budget_bytes, state)?;
        Ok(self)
    }

    /// Moves this freshly built (empty, memory-backed) aggregator's configuration onto another
    /// state backend. Construction goes through `new` + builders first so backend choice stays
    /// orthogonal to the many aggregate-shape builders.
    pub(crate) fn with_backend<T: KeyedStateStore<GroupKeyState>>(
        self,
        store: T,
    ) -> GroupAggregator<T> {
        GroupAggregator {
            has_retractable_extremes: self.has_retractable_extremes,
            kinds: self.kinds,
            value_types: self.value_types,
            result_types: self.result_types,
            state_types: self.state_types,
            value_columns: self.value_columns,
            filter_columns: self.filter_columns,
            count_columns: self.count_columns,
            distinct_view_columns: self.distinct_view_columns,
            record_count_column: self.record_count_column,
            key_columns: self.key_columns,
            key_timestamp_precisions: self.key_timestamp_precisions,
            generate_update_before: self.generate_update_before,
            ttl_ms: self.ttl_ms,
            ttl_emit_unchanged: self.ttl_emit_unchanged,
            last_sweep_ms: self.last_sweep_ms,
            store,
            snapshot_cache: None,
            mini_batch: self.mini_batch,
            staged_order: self.staged_order,
            staged_changes: self.staged_changes,
            staged_key_batches: self.staged_key_batches,
            staged_bytes: self.staged_bytes,
            decimal_views: self.decimal_views,
            deferred_batches: self.deferred_batches,
            deferred_keys: self.deferred_keys,
            deferred_bytes: self.deferred_bytes,
            draining_deferred: self.draining_deferred,
            last_input_ms: self.last_input_ms,
            memory: self.memory,
        }
    }
}

impl<S: KeyedStateStore<GroupKeyState>> GroupAggregator<S> {
    pub(crate) fn with_mini_batch(mut self) -> Self {
        self.mini_batch = true;
        self
    }

    pub(crate) fn staged_keys(&self) -> usize {
        self.staged_order.len() + self.deferred_keys.len()
    }

    /// The backing store, for backend-specific control paths (checkpointing a persistent store).
    pub(crate) fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// Attaches the task off-heap budget for a backend that starts with nothing resident (a
    /// read-through store hydrates on demand; there is no restored map to pre-account).
    pub(crate) fn with_read_through_budget(
        mut self,
        budget_bytes: i64,
    ) -> Result<Self, DataFusionError> {
        self.memory.attach("group-aggregate", budget_bytes, 0)?;
        Ok(self)
    }

    pub(crate) fn staging_bytes(&self) -> usize {
        self.staged_bytes + self.deferred_bytes
    }

    pub(crate) fn with_key_timestamp_precisions(
        mut self,
        key_timestamp_precisions: Vec<i32>,
    ) -> Self {
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
    pub(crate) fn with_count_columns(mut self, count_columns: Vec<i64>) -> Self {
        if !count_columns.is_empty() {
            self.count_columns = count_columns;
        }
        self
    }

    /// Sets the per-aggregate two-phase distinct-view columns (-1 = not a distinct merge). A builder
    /// for the same reason as {@link with_filter_columns}.
    pub(crate) fn with_distinct_view_columns(mut self, distinct_view_columns: Vec<i64>) -> Self {
        if !distinct_view_columns.is_empty() {
            self.distinct_view_columns = distinct_view_columns;
        }
        self
    }

    /// Sets the count1 record-counter partial column of a retracting two-phase merge (-1 = count
    /// rows ±1). A builder for the same reason as {@link with_filter_columns}.
    pub(crate) fn with_record_count_column(mut self, record_count_column: i64) -> Self {
        self.record_count_column = record_count_column;
        self
    }

    /// Sets the idle-state retention (`table.exec.state.ttl`) in millis; 0 (Flink's default)
    /// disables expiry. A builder for the same reason as {@link with_filter_columns}.
    pub(crate) fn with_ttl_emission(mut self, emit_unchanged: bool) -> Self {
        self.ttl_emit_unchanged = emit_unchanged;
        self
    }

    pub(crate) fn with_state_ttl(mut self, ttl_ms: i64) -> Self {
        self.ttl_ms = ttl_ms.max(0);
        self
    }

    /// Reclaims every group whose TTL elapsed with no further touch — the lazy per-touch expiry
    /// never sees such a key again. Silent, like Flink's background cleanup.
    fn sweep_expired(&mut self, ttl: StateTtl) {
        let track = self.memory.tracking();
        let mut reclaimed = 0isize;
        self.store.retain_live(&mut |key, state| {
            if ttl.expired(state.last_write_ms) {
                if track {
                    reclaimed += (group_key_state_bytes(state) + byte_key_bytes(key)) as isize;
                }
                false
            } else {
                true
            }
        });
        if reclaimed != 0 {
            self.memory.record(-reclaimed);
        }
    }

    /// Inserts a key's empty state on its first touch and returns it for folding.
    fn create(&mut self, key: ByteKey) -> &mut GroupKeyState {
        let state = GroupKeyState {
            aggs: self
                .kinds
                .iter()
                .zip(&self.value_types)
                .map(|(&kind, vt)| GroupAggState::new(kind, vt))
                .collect(),
            records: 0,
            last_output: None,
            last_output_bytes: 0,
            last_write_ms: 0,
        };
        self.store.insert(key, state)
    }

    /// Folds the batch's rows into per-key state in input order, honoring each row's `RowKind`, and
    /// returns the changelog rows produced, in emission order. `now_ms` is the host's wall-clock
    /// reading for this call (only read when state TTL is on).
    pub(crate) fn update(
        &mut self,
        batch: &RecordBatch,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        self.snapshot_cache = None;
        self.last_input_ms = now_ms;
        if self.mini_batch
            && self.ttl_ms > 0
            && !self.draining_deferred
            && self.kinds.iter().enumerate().any(|(i, kind)| {
                matches!(kind, 9 | 17)
                    && self.distinct_view_columns[i] >= 0
                    && matches!(self.value_types[i], DataType::Decimal128(p, _) if p > 19)
            })
        {
            // The host reads retained global state only when its temporary bundle is merged.
            let mut added = batch.get_array_memory_size() + std::mem::size_of::<RecordBatch>();
            let mut encoder = BinaryRowBatchEncoder::new(
                batch,
                &self.key_columns,
                &self.key_timestamp_precisions,
            );
            for row in 0..batch.num_rows() {
                let key = encoder.encode(row);
                if !self.deferred_keys.contains_key(key) {
                    added += byte_key_bytes(key);
                    self.deferred_keys.insert(ByteKey::from(key), ());
                }
            }
            self.deferred_bytes += added;
            self.deferred_batches.push((batch.clone(), added));
            if self.memory.tracking() {
                self.memory.record(added as isize);
            }
            self.memory.account()?;
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }
        let ttl = StateTtl::new(self.ttl_ms, now_ms);
        // The sweep reclaims groups no later row ever touches. Once per TTL period bounds its
        // amortized cost at one map walk per period; it must not run mid-bundle, where removing a
        // staged key's state would turn silent expiry into a spurious -D at the flush.
        if ttl.enabled()
            && self.staged_changes.is_empty()
            && now_ms >= self.last_sweep_ms + self.ttl_ms
        {
            self.sweep_expired(ttl);
            self.last_sweep_ms = now_ms;
        }
        // A read-through backend hydrates every key this batch can touch in one probe; point
        // accesses in the per-row loop below are then guaranteed resident (memory: no-op).
        self.store
            .begin_batch(batch, &self.key_columns, &self.key_timestamp_precisions)?;
        let n = batch.num_rows();
        let num_agg = self.kinds.len();
        let wide_views: Vec<Option<(u8, i8)>> = self
            .kinds
            .iter()
            .enumerate()
            .map(|(i, kind)| match self.value_types[i] {
                DataType::Decimal128(p, scale)
                    if p > 19 && matches!(kind, 9 | 17) && self.distinct_view_columns[i] >= 0 =>
                {
                    Some((p, scale))
                }
                _ => None,
            })
            .collect();
        assert!(
            self.mini_batch || wide_views.iter().all(Option::is_none),
            "wide decimal DISTINCT merge requires a logical mini-batch"
        );
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
        let view_values: Vec<_> = view_cols
            .iter()
            .map(|view| view.map(|(_, values, _)| DistinctColumn::new(values)))
            .collect();
        // Per aggregate, the value column index for a COUNT(DISTINCT) (kind 7) or SUM(DISTINCT)
        // (kind 9), else None — the single-phase per-row fold; view-merged aggregates take the
        // list path above instead. Captured before the per-row loop so the loop body reads no
        // `self` field while `state` is borrowed.
        let distinct_cols: Vec<Option<usize>> = (0..num_agg)
            .map(|i| {
                (matches!(self.kinds[i], 7 | 9 | 17 | 18) && self.distinct_view_columns[i] < 0)
                    .then_some(self.value_columns[i] as usize)
            })
            .collect();
        // Downcast fixed-width DISTINCT inputs once per batch.
        let distinct_columns: Vec<_> = distinct_cols
            .iter()
            .map(|c| c.map(|c| DistinctColumn::new(batch.column(c))))
            .collect();
        let ordered_columns: Vec<Option<usize>> = self
            .kinds
            .iter()
            .zip(&self.value_columns)
            .map(|(&kind, &column)| is_ordered_value(kind).then_some(column as usize))
            .collect();
        // Per aggregate, a nonnumeric MIN/MAX value column — folded as a scalar into
        // typed running state for insert-only inputs, or the Extremes multiset otherwise.
        let scalar_extreme_cols: Vec<Option<usize>> = (0..num_agg)
            .map(|i| {
                if matches!(self.kinds[i], 1 | 2 | 10 | 11) && self.value_columns[i] >= 0 {
                    let col = self.value_columns[i] as usize;
                    let data_type = batch.column(col).data_type();
                    (matches!(
                        data_type,
                        DataType::Utf8
                            | DataType::LargeUtf8
                            | DataType::Utf8View
                            | DataType::Date32
                            | DataType::Time32(arrow::datatypes::TimeUnit::Millisecond)
                            | DataType::Boolean
                    ) || streamfusion_bridge::timestamp::is_timestamp(data_type))
                    .then_some(col)
                } else {
                    None
                }
            })
            .collect();
        // Per aggregate, the two-phase AVG count-partial column (None = not a merge): the value
        // column is the pre-summed sum partial and the count folds from this column, not +1 per row.
        let single_count_cols: Vec<Option<&Int32Array>> = (0..num_agg)
            .map(|i| {
                (self.count_columns[i] >= 0 && self.kinds[i] == 14).then(|| {
                    batch
                        .column(self.count_columns[i] as usize)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("single-value count partial must be int")
                })
            })
            .collect();
        let merge_count_cols: Vec<Option<&Int64Array>> = (0..num_agg)
            .map(|i| {
                (self.count_columns[i] >= 0 && self.kinds[i] != 14).then(|| {
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

        // Output keys are gathered from the input row that caused each transition.
        let mut output = GroupChanges {
            rows: Vec::new(),
            results: vec![Vec::new(); num_agg],
            kinds: Vec::new(),
        };

        let track = self.memory.tracking();
        // One unfiltered SINGLE_VALUE accumulator already holds the complete result. Emit it
        // directly; filtered groups can have many noncontributing touches, so keep their cache.
        let single_value = self.kinds.as_slice() == [14] && self.filter_columns[0] < 0;
        let staged_key_batch = self.staged_key_batches.len();
        let mut retained_key_batch = false;
        let mut staged_delta = 0usize;
        for row in 0..n {
            let key = key_encoder.encode(row);
            // RowKind: 0 +I, 1 -U, 2 +U, 3 -D (absent column ⇒ INSERT). UB/delete retract; I/UA add.
            let kind = row_kinds.map_or(0, |kinds| kinds.value(row));
            let retract = kind == 1 || kind == 3;
            // An expired group is deleted on read and treated as never seen (Flink's
            // NeverReturnExpired): the next add re-enters through the fresh +I path below, and a
            // retraction falls into the no-accumulator skip. Nothing is emitted for the expiry.
            let mut expired_bytes = 0isize;
            let exists = if ttl.enabled() {
                match self.store.get(key) {
                    Some(state) if ttl.expired(state.last_write_ms) => {
                        expired_bytes =
                            (group_key_state_bytes(state) + byte_key_bytes(key)) as isize;
                        self.store.remove(key);
                        false
                    }
                    present => present.is_some(),
                }
            } else {
                self.store.contains(key)
            };
            if track && expired_bytes != 0 {
                self.memory.record(-expired_bytes);
            }
            let before = if track && exists {
                group_key_state_bytes(self.store.get(key).expect("key present")) as isize
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
                let state = self.store.get_mut(key).expect("key present");
                match state.last_output.take() {
                    Some(cached) => {
                        let bytes = std::mem::take(&mut state.last_output_bytes);
                        (Some(cached), bytes)
                    }
                    // Restored state or immediate, unfiltered SINGLE_VALUE has no cache.
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
                    StagedGroupChange {
                        old: prev.take(),
                        key_batch: staged_key_batch,
                        key_row: row,
                    },
                );
            }
            for (i, metadata) in wide_views.iter().enumerate() {
                let Some((_, scale)) = metadata else {
                    continue;
                };
                assert!(!retract, "wide decimal views are insert-only");
                let (list, values, counts) = view_cols[i].expect("wide decimal membership view");
                let values = values
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("decimal membership");
                if !self.decimal_views.contains_key(key) {
                    let owned = ByteKey::from(key);
                    let buffers = (0..num_agg).map(|_| None).collect::<Vec<_>>();
                    staged_delta += byte_key_bytes(key) + decimal_buffers_bytes(&buffers);
                    self.decimal_views.insert(owned, buffers);
                }
                let buffers = self.decimal_views.get_mut(key).unwrap();
                let before = buffers[i].as_ref().map_or(0, |buffer| {
                    buffer.bytes() - std::mem::size_of::<DecimalViewBuffer>()
                });
                let buffer = buffers[i].get_or_insert_with(|| DecimalViewBuffer::new(*scale));
                for entry in
                    list.value_offsets()[row] as usize..list.value_offsets()[row + 1] as usize
                {
                    buffer.merge(
                        (!values.is_null(entry)).then(|| values.value(entry)),
                        counts.value(entry),
                    );
                }
                staged_delta += buffer.bytes() - std::mem::size_of::<DecimalViewBuffer>() - before;
            }
            {
                let state = if exists {
                    self.store.get_mut(key).expect("key present")
                } else {
                    self.create(ByteKey::from(key))
                };
                for i in 0..num_agg {
                    // FILTER: fold into this aggregate only where its filter is TRUE (NULL/FALSE skip).
                    if let Some(filter) = filter_cols[i] {
                        if filter.is_null(row) || !filter.value(row) {
                            continue;
                        }
                    }
                    if let GroupAggState::Ordered(ordered) = &mut state.aggs[i] {
                        let column =
                            batch.column(ordered_columns[i].expect("ordered value column"));
                        let value = ScalarValue::try_from_array(column, row)?;
                        if let Some(counts) = single_count_cols[i] {
                            ordered.merge_single(value, counts.value(row))?;
                        } else {
                            ordered.update(value, retract)?;
                        }
                        continue;
                    }
                    // Two-phase AVG merge: fold the pre-summed sum partial and bump the count by the
                    // count partial. Empty/all-null bundles carry (0, 0). A NULL sum is a decimal
                    // overflow and must propagate even when retractions net the count to zero.
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
                                None => state.aggs[i].merge_overflowed(count, retract),
                            }
                        }
                        continue;
                    }
                    // String/timestamp MIN/MAX folds the value as a scalar into the Extremes multiset
                    // (skipping nulls — MIN/MAX ignore them), ordered by MinMaxKey.
                    if let Some(col_idx) = scalar_extreme_cols[i] {
                        let column = batch.column(col_idx);
                        if !column.is_null(row) {
                            if matches!(state.aggs[i], GroupAggState::Running { .. }) {
                                assert!(!retract, "running extrema require insert-only input");
                                state.aggs[i].accumulate_typed_extreme(column, row);
                                continue;
                            }
                            let scalar = ScalarValue::try_from_array(column, row)
                                .expect("non-numeric extreme scalar");
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
                    if let Some((list, _values, counts)) = view_cols[i] {
                        assert!(!retract, "distinct view partials are insert-only");
                        if wide_views[i].is_some() {
                            continue;
                        }
                        let start = list.value_offsets()[row] as usize;
                        let end = list.value_offsets()[row + 1] as usize;
                        let column = view_values[i].as_ref().expect("distinct view reader");
                        for e in start..end {
                            if counts.value(e) > 0 {
                                column.update(&mut state.aggs[i], e, false, counts.value(e));
                            }
                        }
                        continue;
                    }
                    // COUNT(DISTINCT x) (kind 7) folds the value itself, not a Num — read its scalar
                    // (skipping nulls, which DISTINCT ignores) and add/remove it from the value set.
                    if let Some(column) = &distinct_columns[i] {
                        column.update(&mut state.aggs[i], row, retract, 1);
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
                if ttl.enabled() {
                    // Every processed row is a state write (Flink's accState.update), so it
                    // refreshes the group's TTL. Reads never do.
                    state.last_write_ms = ttl.now();
                }
            }

            if self.has_retractable_extremes {
                self.store.resolve_multiset_extremes(key)?;
            }

            if self.mini_batch {
                if self.store.get(key).unwrap().records <= 0 {
                    self.store.remove(key);
                }
            } else if self.store.get(key).unwrap().records > 0 {
                let state = self.store.get_mut(key).expect("key present");
                if single_value {
                    let value = state.aggs[0].emit(&self.result_types[0]);
                    match prev {
                        None => output.push(0, row, [value]),
                        Some(prev) if value != prev[0] || ttl.enabled() => {
                            if self.generate_update_before {
                                output.push(1, row, prev);
                            }
                            output.push(2, row, [value]);
                        }
                        Some(_) => {}
                    }
                } else {
                    let new = output_of(state, &self.result_types);
                    match prev {
                        None => {
                            // +I — first row for the key; the emitted tuple seeds the cache.
                            state.last_output_bytes = scalar_row_bytes(&new);
                            output.push(0, row, new.iter().cloned());
                            state.last_output = Some(new);
                        }
                        // With TTL on the no-change suppression is disabled: Flink always emits -U/+U
                        // so downstream state keeps refreshing instead of expiring too early.
                        Some(prev) if new != prev || ttl.enabled() => {
                            state.last_output_bytes = scalar_row_bytes(&new);
                            if self.generate_update_before {
                                output.push(1, row, prev); // -U — moved out of the cache, not recomputed
                            }
                            output.push(2, row, new.iter().cloned()); // +U
                            state.last_output = Some(new);
                        }
                        Some(prev) => {
                            state.last_output_bytes = prev_bytes;
                            state.last_output = Some(prev); // unchanged result — suppressed
                        }
                    }
                }
            } else {
                // The last record for the key was retracted: delete the group, emitting -D only if
                // the key ever emitted (a new key whose first merged count1 nets to zero — Flink's
                // firstRow-and-empty case — is dropped silently).
                if let Some(prev) = prev {
                    output.push(3, row, prev); // -D
                }
                self.store.remove(key);
            }
            if track {
                // The touched group's footprint change, plus its key when created or deleted.
                let mut delta = -before;
                if let Some(state) = self.store.get(key) {
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
        let GroupChanges {
            rows: out_rows,
            results: mut out_results,
            kinds: out_kinds,
        } = output;
        self.staged_bytes += staged_delta;
        if track {
            self.memory.record(staged_delta as isize);
        }
        if !self.mini_batch {
            // Immediate mode: the batch is the whole bundle — the store may release what it
            // hydrated. Mini-batch bundles end in `flush_mini_batch` instead.
            self.store.end_bundle()?;
        }
        self.memory.record(self.store.footprint_delta());
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
        self.flush_mini_batch_at(self.last_input_ms)
    }

    pub(crate) fn flush_mini_batch_at(
        &mut self,
        now_ms: i64,
    ) -> Result<RecordBatch, DataFusionError> {
        let batches = std::mem::take(&mut self.deferred_batches);
        self.deferred_keys = HashMap::default();
        self.draining_deferred = true;
        let result = (|| {
            for (batch, bytes) in batches {
                let result = self.update(&batch, now_ms);
                drop(batch);
                self.deferred_bytes -= bytes;
                if self.memory.tracking() {
                    self.memory.forget(bytes);
                }
                result?;
                self.memory.account_shrink();
            }
            Ok::<(), DataFusionError>(())
        })();
        self.draining_deferred = false;
        if self.memory.tracking() {
            self.memory.forget(self.deferred_bytes);
        }
        self.deferred_bytes = 0;
        result?;
        self.finish_mini_batch()
    }

    fn finish_mini_batch(&mut self) -> Result<RecordBatch, DataFusionError> {
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

        // Staged keys were all written this bundle, so none can be expired here; a TTL shorter
        // than the bundle interval is degenerate and still only delays expiry to the next touch.
        let ttl_on = self.ttl_ms > 0 && self.ttl_emit_unchanged;
        let mut decimal_views = std::mem::take(&mut self.decimal_views);
        for key in order {
            if let Some(buffers) = decimal_views.remove(&key) {
                let state = self
                    .store
                    .get_mut(&key.0)
                    .expect("staged decimal key remains resident");
                let before = if self.memory.tracking() {
                    group_key_state_bytes(state)
                } else {
                    0
                };
                for (i, buffer) in buffers.into_iter().enumerate() {
                    if let Some(buffer) = buffer {
                        let DataType::Decimal128(precision, scale) = self.value_types[i] else {
                            unreachable!()
                        };
                        for value in buffer.order.iter().flatten() {
                            if let Some(&count) = buffer.counts.get(&value) {
                                state.aggs[i].merge_distinct(
                                    ScalarValue::Decimal128(Some(value), precision, scale),
                                    count,
                                );
                            }
                        }
                    }
                }
                if self.memory.tracking() {
                    self.memory
                        .record(group_key_state_bytes(state) as isize - before as isize);
                }
            }
            let staged = &changes[&key];
            let new = self
                .store
                .get(&key.0)
                .map(|state| output_of(state, &self.result_types));
            match (&staged.old, &new) {
                (None, Some(after)) => push(0, staged, after.clone()),
                (Some(before), None) => push(3, staged, before.clone()),
                // The same TTL rule as immediate mode: no-change transitions are suppressed only
                // with retention off (Flink's MiniBatchGroupAggFunction gate).
                (Some(before), Some(after)) if before != after || ttl_on => {
                    if self.generate_update_before {
                        push(1, staged, before.clone());
                    }
                    push(2, staged, after.clone());
                }
                _ => {}
            }
            if let (Some(state), Some(after)) = (self.store.get_mut(&key.0), new) {
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
            let data: Vec<_> = key_batches
                .iter()
                .map(|batch| batch.column(column).to_data())
                .collect();
            let refs: Vec<_> = data.iter().collect();
            let mut gathered = MutableArrayData::new(refs, false, key_rows.len());
            for &(batch, row) in &key_rows {
                gathered.extend(batch, row, row + 1);
            }
            columns.push(make_array(gathered.freeze()));
        }
        for (i, result_type) in self.result_types.iter().enumerate() {
            fields.push(Field::new(format!("result{i}"), result_type.clone(), true));
            columns.push(scalars_to_array(
                std::mem::take(&mut out_results[i]),
                result_type,
            ));
        }
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        self.memory.forget(self.staged_bytes);
        self.staged_bytes = 0;
        if self.memory.tracking() {
            self.memory.record(new_cache_bytes as isize);
        }
        self.store.end_bundle()?;
        self.memory.record(self.store.footprint_delta());
        self.memory.account()?;
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build mini-batch group-by changelog"))
    }
}

/// The raw keyed-state snapshot/restore surface. It walks the full resident map, so it exists only
/// on the memory backend — a persistent store checkpoints through its own commit path instead.
impl GroupAggregator {
    /// Serializes per-key state. A main batch carries `[key0.., records, state{i}, nonnull{i}…]` (the
    /// raw running value and non-null count for SUM/COUNT; a NULL placeholder for MIN/MAX), and a side
    /// batch per MIN/MAX aggregate carries its `[key0.., value, count]` multiset rows.
    #[cfg(test)]
    pub(crate) fn snapshot(&mut self) -> Vec<u8> {
        let selected: Vec<ByteKey> = self.store.keys().cloned().collect();
        self.snapshot_keys(&selected)
    }
}

impl<S: KeyedStateStore<GroupKeyState>> GroupAggregator<S> {
    fn snapshot_keys(&self, selected: &[ByteKey]) -> Vec<u8> {
        let num_agg = self.kinds.len();
        let mut encoded_keys: Vec<&[u8]> = Vec::new();
        let mut records: Vec<i64> = Vec::new();
        let mut write_timestamps: Vec<i64> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_agg];
        let mut non_null_columns: Vec<Vec<i64>> = vec![Vec::new(); num_agg];
        let mut multiset_keys: Vec<Vec<&[u8]>> = vec![Vec::new(); num_agg];
        let mut multiset_values: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_agg];
        let mut multiset_counts: Vec<Vec<i64>> = vec![Vec::new(); num_agg];
        for key in selected {
            let state = self
                .store
                .get(&key.0)
                .expect("snapshot key remains in group state");
            encoded_keys.push(&key.0);
            records.push(state.records);
            write_timestamps.push(state.last_write_ms);
            for i in 0..num_agg {
                match &state.aggs[i] {
                    GroupAggState::Ordered(ordered) => {
                        state_columns[i].push(ordered.snapshot_value());
                        non_null_columns[i].push(ordered.count());
                        for (value, count) in ordered.entries() {
                            multiset_keys[i].push(&key.0);
                            multiset_values[i].push(value.clone());
                            multiset_counts[i].push(count);
                        }
                    }
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
                    GroupAggState::Distinct { set, .. } => {
                        // The count is recomputed from the side batch on restore (placeholder here).
                        state_columns[i].push(null_scalar(&self.result_types[i]));
                        non_null_columns[i].push(0);
                        for (value, count) in set.scalar_entries() {
                            multiset_keys[i].push(&key.0);
                            multiset_values[i].push(value); // the distinct value itself
                            multiset_counts[i].push(count);
                        }
                    }
                    GroupAggState::DistinctRunning { counts, agg, live } => {
                        // Wide decimal SUM and AVG overflow depends on arrival order.
                        if self.kinds[i] == 17 || self.wide_distinct_sum(i) {
                            state_columns[i].push(agg.emit());
                            non_null_columns[i].push(*live);
                        } else {
                            state_columns[i].push(null_scalar(&self.result_types[i]));
                            non_null_columns[i].push(0);
                        }
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
            let mut field = Field::new(format!("state{i}"), self.state_types[i].clone(), true);
            if self.wide_distinct_sum(i) {
                field = field.with_metadata(std::collections::HashMap::from([(
                    "streamfusion.distinct-running-state".to_owned(),
                    "1".to_owned(),
                )]));
            }
            fields.push(field);
            columns.push(scalars_to_array(
                std::mem::take(&mut state_columns[i]),
                &self.state_types[i],
            ));
            fields.push(Field::new(format!("nonnull{i}"), DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(std::mem::take(
                &mut non_null_columns[i],
            ))));
        }
        // The TTL timestamps ride a trailing column only while TTL is on, so a TTL-off snapshot
        // stays byte-identical to the pre-TTL format (and disabling TTL sheds the timestamps).
        if self.ttl_ms > 0 {
            fields.push(Field::new(TTL_TS_COLUMN, DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(write_timestamps)));
        }
        let mut batches =
            vec![RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("main snapshot")];
        for i in 0..num_agg {
            // Kinds 10/11 write their (always empty) side batch too, so the frame layout matches
            // the retractable representation and a blob round-trips across the two.
            if matches!(
                self.kinds[i],
                1 | 2 | 7 | 9 | 10 | 11 | 15 | 16 | 17 | 18 | 19 | 20
            ) {
                let mut f = vec![Field::new("binary_key", DataType::Binary, false)];
                let mut c: Vec<ArrayRef> = vec![Arc::new(
                    arrow::array::BinaryArray::from_iter_values(multiset_keys[i].iter().copied()),
                )];
                // MIN/MAX values take the aggregate's result type; a distinct value keeps its own type
                // (a COUNT's bigint result type does not describe it), inferred from the scalars.
                let values = std::mem::take(&mut multiset_values[i]);
                let value_array: ArrayRef = if matches!(self.kinds[i], 7 | 9 | 17 | 18) {
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
}

impl GroupAggregator {
    /// Materializes every non-empty Flink key group once and transfers ownership to the caller.
    pub(crate) fn snapshot_partitions(
        &mut self,
        max_parallelism: usize,
        timestamp_precisions: &[i32],
    ) -> BTreeMap<i32, Vec<u8>> {
        self.materialize_raw_keyed_snapshots(max_parallelism, timestamp_precisions);
        self.snapshot_cache
            .take()
            .expect("raw keyed snapshot cache")
            .snapshots
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

        let selected: Vec<ByteKey> = self.store.keys().cloned().collect();
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
        restored_at_ms: i64,
    ) -> Self {
        let mut aggregator = GroupAggregator::new(
            kinds,
            value_types,
            value_columns,
            key_columns,
            generate_update_before,
        );
        aggregator.load_snapshot(bytes, restored_at_ms);
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
        restored_at_ms: i64,
    ) -> Self {
        let mut merged = GroupAggregator::new(
            kinds,
            value_types,
            value_columns,
            key_columns,
            generate_update_before,
        );
        for bytes in snapshots {
            merged.load_snapshot(bytes, restored_at_ms);
        }
        merged
    }
}

impl<S: KeyedStateStore<GroupKeyState>> GroupAggregator<S> {
    fn wide_distinct_sum(&self, i: usize) -> bool {
        self.kinds[i] == 9 && matches!(self.value_types[i], DataType::Decimal128(p, _) if p > 19)
    }

    /// Decodes one raw key-group snapshot blob into the backing store through the state seam, so
    /// the same decode serves the memory rebuild and the typed persistent import.
    fn load_snapshot(&mut self, bytes: &[u8], restored_at_ms: i64) {
        let num_agg = self.kinds.len();
        let batches = read_framed(bytes);
        if batches.is_empty() {
            return;
        }
        // Main batch: BinaryRow key, records, then (state, nonnull) per aggregate, then the TTL
        // timestamps when the writer had TTL on. A pre-TTL snapshot restored into a TTL'd operator
        // stamps every group with the restore time — a full retention from now, Flink's
        // enable-TTL migration — instead of 0, which would expire everything on first touch.
        let main = &batches[0];
        let preserved_distinct: Vec<_> = (0..num_agg)
            .map(|i| {
                self.kinds[i] == 17
                    || (self.wide_distinct_sum(i)
                        && main
                            .schema()
                            .field(2 + 2 * i)
                            .metadata()
                            .get("streamfusion.distinct-running-state")
                            .is_some_and(|version| version == "1"))
            })
            .collect();
        let write_timestamps = (main.num_columns() > 2 + 2 * num_agg).then(|| {
            assert_eq!(
                main.schema().field(2 + 2 * num_agg).name(),
                TTL_TS_COLUMN,
                "group snapshot schema"
            );
            column_i64(main, TTL_TS_COLUMN)
        });
        if write_timestamps.is_none() {
            assert_eq!(main.num_columns(), 2 + 2 * num_agg, "group snapshot schema");
        }
        let keys = main
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .expect("group snapshot binary keys");
        let records = column_i64(main, "records");
        for row in 0..main.num_rows() {
            let key = ByteKey::from(keys.value(row));
            let state = self.create(key);
            state.records = records.value(row);
            state.last_write_ms = write_timestamps
                .as_ref()
                .map_or(restored_at_ms, |ts| ts.value(row));
            for i in 0..num_agg {
                if let GroupAggState::Ordered(ordered) = &mut state.aggs[i] {
                    ordered.restore_value(
                        ScalarValue::try_from_array(main.column(2 + 2 * i), row)
                            .expect("ordered state scalar"),
                        main.column(3 + 2 * i)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("ordered count")
                            .value(row),
                    );
                }
                if preserved_distinct[i] {
                    if let GroupAggState::DistinctRunning { agg, live, .. } = &mut state.aggs[i] {
                        agg.restore_value(
                            &ScalarValue::try_from_array(main.column(2 + 2 * i), row)
                                .expect("distinct running sum"),
                        );
                        *live = main
                            .column(3 + 2 * i)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("distinct running count")
                            .value(row);
                    }
                }
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
            if !matches!(
                self.kinds[i],
                1 | 2 | 7 | 9 | 10 | 11 | 15 | 16 | 17 | 18 | 19 | 20
            ) {
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
                if let Some(state) = self.store.get_mut(key) {
                    if preserved_distinct[i] {
                        if let GroupAggState::DistinctRunning { counts: set, .. } =
                            &mut state.aggs[i]
                        {
                            set.insert_imported(value, counts.value(row));
                        }
                    } else {
                        state.aggs[i].import_multiset_entry(value, counts.value(row));
                    }
                }
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
impl GroupAggregator<RocksGroupStore> {
    /// Decodes restored blob key groups once at open and writes them through the typed store, so
    /// a canonical or raw restore continues on the direct persistent path.
    pub(crate) fn import_partitions(
        &mut self,
        snapshots: &[Vec<u8>],
        restored_at_ms: i64,
    ) -> Result<(), DataFusionError> {
        for bytes in snapshots {
            self.load_snapshot(bytes, restored_at_ms);
            self.store.end_bundle()?;
        }
        Ok(())
    }

    pub(crate) fn canonical_partitions(
        &mut self,
    ) -> Result<BTreeMap<i32, Vec<u8>>, DataFusionError> {
        let keys = self.store.canonical_keys_by_group()?;
        let partitions = keys
            .iter()
            .map(|(&group, selected)| (group, self.snapshot_keys(selected)))
            .collect();
        self.store.finish_canonical_scan();
        Ok(partitions)
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
/// counts rows. Wide decimal DISTINCT views preserve the host's group-map emission order;
/// other partials follow first appearance across buffered batches.
pub(crate) struct LocalGroupAggregator {
    kinds: Vec<i64>,
    value_types: Vec<DataType>,
    value_columns: Vec<i64>,
    // Per-aggregate FILTER column index (the boolean the host computes for `AGG(x) FILTER (WHERE p)`),
    // or -1 for an unfiltered aggregate. A row folds into aggregate i only when its filter is TRUE;
    // the global merge is filter-blind because the partials are already filtered here.
    filter_columns: Vec<i64>,
    key_columns: Vec<usize>,
    key_timestamp_precisions: Vec<i32>,
    result_types: Vec<DataType>,
    // Per distinct view column (trailing the partials, in Flink's declared order), the index of the
    // aggregate whose distinct set backs it — the flush emits that set's (value, count) entries as a
    // list column for the global to merge. Empty when no aggregate is distinct.
    distinct_view_sources: Vec<i64>,
    decimal_view_groups: Vec<Vec<usize>>,
    order: Vec<LocalGroupKey>,
    group_order: Option<JavaMapOrder<usize>>,
    states: HashMap<ByteKey, LocalGroupEntry>,
    scalar_states: HashMap<GroupKey, LocalGroupEntry>,
    key_converter: Option<RowConverter>,
    key_batches: Vec<RecordBatch>,
    scalar_key_mode: Option<bool>,
    key_types: Vec<DataType>,
    pub(crate) memory: OperatorMemory,
}

struct LocalGroupEntry {
    decimal_orders: Option<Box<Vec<DecimalMapOrder>>>,
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
fn decimal_order_bytes(orders: &Option<Box<Vec<DecimalMapOrder>>>) -> usize {
    orders.as_ref().map_or(0, |orders| {
        std::mem::size_of::<Vec<DecimalMapOrder>>()
            + orders.iter().map(DecimalMapOrder::bytes).sum::<usize>()
            + (orders.capacity() - orders.len()) * std::mem::size_of::<DecimalMapOrder>()
    })
}

fn local_entry_state_bytes(entry: &LocalGroupEntry) -> usize {
    std::mem::size_of::<LocalGroupEntry>()
        + decimal_order_bytes(&entry.decimal_orders)
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
            .map(|(&kind, vt)| {
                if kind == 21 {
                    DataType::Int32
                } else if is_ordered_value(kind) {
                    vt.clone()
                } else {
                    RunningAgg::new(kind, vt).result_type()
                }
            })
            .collect();
        let filter_columns = if filter_columns.is_empty() {
            vec![-1; kinds.len()]
        } else {
            filter_columns
        };
        let mut decimal_view_groups = Vec::new();
        let mut columns = Vec::new();
        for (i, &kind) in kinds.iter().enumerate() {
            if matches!(kind, 9 | 18)
                && matches!(value_types[i], DataType::Decimal128(p, _) if p > 19)
                && !columns.contains(&value_columns[i])
            {
                columns.push(value_columns[i]);
                let sources: Vec<usize> = distinct_view_sources
                    .iter()
                    .map(|&source| source as usize)
                    .filter(|&source| value_columns[source] == value_columns[i])
                    .collect();
                if !sources.is_empty() {
                    decimal_view_groups.push(sources);
                }
            }
        }
        LocalGroupAggregator {
            kinds,
            value_types,
            value_columns,
            filter_columns,
            key_timestamp_precisions: vec![-1; key_columns.len()],
            key_columns,
            result_types,
            distinct_view_sources,
            decimal_view_groups,
            order: Vec::new(),
            group_order: None,
            states: HashMap::default(),
            scalar_states: HashMap::default(),
            key_converter: None,
            key_batches: Vec::new(),
            scalar_key_mode: None,
            key_types: Vec::new(),
            memory: OperatorMemory::unaccounted(),
        }
    }

    fn new_decimal_orders(&self) -> Option<Box<Vec<DecimalMapOrder>>> {
        (!self.decimal_view_groups.is_empty()).then(|| {
            Box::new(
                self.decimal_view_groups
                    .iter()
                    .map(|sources| {
                        let DataType::Decimal128(_, scale) = self.value_types[sources[0]] else {
                            unreachable!()
                        };
                        DecimalMapOrder::new(scale)
                    })
                    .collect(),
            )
        })
    }

    pub(crate) fn with_key_timestamp_precisions(mut self, precisions: Vec<i32>) -> Self {
        self.key_timestamp_precisions = precisions;
        self
    }

    fn note_group_order(&mut self, hash: Option<i32>) {
        if let Some(hash) = hash {
            let before = self.group_order.as_ref().map_or(0, JavaMapOrder::bytes);
            let order = self
                .group_order
                .get_or_insert_with(|| JavaMapOrder::with_capacity(16));
            let hash = hash as u32;
            // BinaryRow tree ties use nondeterministic JVM identity. Insertion ranks choose
            // one valid identity ordering; distinct hashes preserve the deterministic order.
            order.insert(self.order.len(), (hash ^ (hash >> 16)) as i32);
            if self.memory.tracking() {
                self.memory.record((order.bytes() - before) as isize);
            }
        }
    }

    /// Bounds the buffered partials by a task off-heap budget (negative = unaccounted). The buffer
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
                .sum::<usize>()
            + self.group_order.as_ref().map_or(0, JavaMapOrder::bytes);
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
        let mut group_hashes = (!self.decimal_view_groups.is_empty()).then(|| {
            BinaryRowBatchEncoder::new(batch, &self.key_columns, &self.key_timestamp_precisions)
        });
        let key_batch = self.key_batches.len();
        let mut retained_key_batch = false;
        // Distinct aggregates fold the value itself into their per-bundle set, not a Num;
        // a BIGINT value column takes the primitive fast path.
        let distinct_cols: Vec<Option<usize>> = (0..num_agg)
            .map(|i| {
                matches!(self.kinds[i], 7 | 9 | 17 | 18).then_some(self.value_columns[i] as usize)
            })
            .collect();
        let distinct_columns: Vec<_> = distinct_cols
            .iter()
            .map(|c| c.map(|c| DistinctColumn::new(batch.column(c))))
            .collect();
        // Per aggregate, a string/timestamp MIN/MAX value column — folded as a scalar
        // into typed running state for insert-only inputs, or the Extremes multiset otherwise.
        let scalar_extreme_cols: Vec<Option<usize>> = (0..num_agg)
            .map(|i| {
                if matches!(self.kinds[i], 1 | 2 | 10 | 11) && self.value_columns[i] >= 0 {
                    let col = self.value_columns[i] as usize;
                    let data_type = batch.column(col).data_type();
                    (matches!(
                        data_type,
                        DataType::Utf8
                            | DataType::LargeUtf8
                            | DataType::Utf8View
                            | DataType::Date32
                            | DataType::Time32(arrow::datatypes::TimeUnit::Millisecond)
                            | DataType::Boolean
                    ) || streamfusion_bridge::timestamp::is_timestamp(data_type))
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
                    let decimal_orders = self.new_decimal_orders();
                    if track {
                        self.memory.record(
                            (group_key_bytes(&key) * 2
                                + std::mem::size_of::<LocalGroupEntry>()
                                + decimal_order_bytes(&decimal_orders))
                                as isize,
                        );
                    }
                    self.note_group_order(group_hashes.as_mut().map(|encoder| encoder.hash(row)));
                    self.order.push(LocalGroupKey::Scalar(key.clone()));
                    self.scalar_states.insert(
                        key.clone(),
                        LocalGroupEntry {
                            decimal_orders,
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
                    let decimal_orders = self.new_decimal_orders();
                    if track {
                        self.memory.record(
                            (byte_key_bytes(key) * 2
                                + std::mem::size_of::<LocalGroupEntry>()
                                + decimal_order_bytes(&decimal_orders))
                                as isize,
                        );
                    }
                    self.note_group_order(group_hashes.as_mut().map(|encoder| encoder.hash(row)));
                    self.order.push(LocalGroupKey::Byte(owned.clone()));
                    self.states.insert(
                        owned,
                        LocalGroupEntry {
                            decimal_orders,
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
                delta -= local_entry_state_bytes(entry) as isize;
            }
            if let Some(orders) = &mut entry.decimal_orders {
                assert!(!retract, "ordered decimal local views are insert-only");
                for (order, sources) in orders.iter_mut().zip(&self.decimal_view_groups) {
                    if sources.iter().any(|&source| {
                        filter_cols[source]
                            .map_or(true, |filter| !filter.is_null(row) && filter.value(row))
                    }) {
                        let values = batch
                            .column(self.value_columns[sources[0]] as usize)
                            .as_any()
                            .downcast_ref::<Decimal128Array>()
                            .expect("decimal membership");
                        order.insert_optional((!values.is_null(row)).then(|| values.value(row)));
                    }
                }
            }
            for i in 0..num_agg {
                if let Some(filter) = filter_cols[i] {
                    if filter.is_null(row) || !filter.value(row) {
                        continue;
                    }
                }
                if let GroupAggState::Ordered(ordered) = &mut entry.states[i] {
                    let value = ScalarValue::try_from_array(
                        batch.column(self.value_columns[i] as usize),
                        row,
                    )?;
                    ordered.update(value, retract)?;
                    continue;
                }
                if let Some(col_idx) = scalar_extreme_cols[i] {
                    let column = batch.column(col_idx);
                    if !column.is_null(row) {
                        if matches!(entry.states[i], GroupAggState::Running { .. }) {
                            assert!(!retract, "running local extrema require insert-only input");
                            entry.states[i].accumulate_typed_extreme(column, row);
                            continue;
                        }
                        let scalar = ScalarValue::try_from_array(column, row)
                            .expect("non-numeric extreme scalar");
                        if retract {
                            entry.states[i].retract_extreme(scalar);
                        } else {
                            entry.states[i].accumulate_extreme(scalar);
                        }
                    }
                    continue;
                }
                if let Some(column) = &distinct_columns[i] {
                    column.update(&mut entry.states[i], row, retract, 1);
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
                delta += local_entry_state_bytes(entry) as isize;
                self.memory.record(delta);
            }
        }
        if retained_key_batch && !scalar_key_mode {
            self.key_batches.push(batch.clone());
        }
        self.memory.account()
    }

    /// Emits the buffered partials (`[key0.., partial0.., distinct-view0..]`) and clears entries.
    /// Key types are retained so an empty flush still carries the right schema. Each distinct view column carries its bundle set's (value, count) entries as a
    /// list of structs — the wire form of Flink's serialized MapView partial — for the global to
    /// merge with multiplicities.
    #[cfg(test)]
    pub(crate) fn flush(&mut self) -> RecordBatch {
        self.try_flush().expect("local aggregate flush")
    }

    pub(crate) fn try_flush(&mut self) -> Result<RecordBatch, DataFusionError> {
        let mut temporary = self.memory.temporary_reservation();
        if let Some(reservation) = temporary
            .as_mut()
            .filter(|_| !self.decimal_view_groups.is_empty())
        {
            let mut entries = 0usize;
            let mut copy_peak = 0usize;
            for entry in self.states.values().chain(self.scalar_states.values()) {
                if let Some(orders) = &entry.decimal_orders {
                    for (order, sources) in orders.iter().zip(&self.decimal_view_groups) {
                        entries += order.len() * sources.len();
                        // One copied order at a time: node growth, old/new bucket arrays and
                        // resize partition vectors fit within four retained order footprints.
                        copy_peak = copy_peak.max(order.bytes() * 4);
                    }
                }
            }
            if copy_peak > 0 {
                // Bound geometric vector growth plus simultaneous scalar and Arrow output.
                let output = entries * 4 * (std::mem::size_of::<ScalarValue>() + 32)
                    + (self.order.len() + 1) * self.distinct_view_sources.len() * 8;
                reservation.try_grow(
                    copy_peak + output + self.order.len() * std::mem::size_of::<usize>(),
                )?;
            }
        }
        let mut order = std::mem::take(&mut self.order);
        if let Some(group_order) = &mut self.group_order {
            let mut destinations = vec![0; order.len()];
            for (destination, source) in group_order.iter().enumerate() {
                destinations[source] = destination;
            }
            for position in 0..order.len() {
                while destinations[position] != position {
                    let other = destinations[position];
                    order.swap(position, other);
                    destinations.swap(position, other);
                }
            }
            group_order.clear();
        }
        let states = std::mem::take(&mut self.states);
        let scalar_states = std::mem::take(&mut self.scalar_states);
        let key_batches = std::mem::take(&mut self.key_batches);
        let scalar_key_mode = self.scalar_key_mode.take().unwrap_or(false);
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
            let mut values = DistinctValues::new(value_type);
            let mut counts: Vec<i64> = Vec::new();
            for key in &order {
                let entry = match key {
                    LocalGroupKey::Byte(key) => &states[key],
                    LocalGroupKey::Scalar(key) => &scalar_states[key],
                };
                if let Some(group) = self
                    .decimal_view_groups
                    .iter()
                    .position(|sources| sources.contains(&source))
                {
                    let DataType::Decimal128(precision, scale) = value_type else {
                        unreachable!()
                    };
                    let original = &entry.decimal_orders.as_ref().unwrap()[group];
                    let copied = original.copy_if_reordered();
                    let transported = copied.as_ref().unwrap_or(original);
                    for value in transported.iter() {
                        let scalar = ScalarValue::Decimal128(value, *precision, *scale);
                        counts.push(entry.states[source].decimal_distinct_count(&scalar));
                        values.push(scalar);
                    }
                } else {
                    let set = match &entry.states[source] {
                        GroupAggState::Distinct { set, .. } => set,
                        GroupAggState::DistinctRunning { counts, .. } => counts,
                        _ => unreachable!("distinct view state"),
                    };
                    values.append(set, &mut counts);
                }
                offsets.push(counts.len() as i32);
            }
            let values = values.finish();
            let entries = arrow::array::StructArray::new(
                distinct_entry_fields(value_type),
                vec![values, Arc::new(Int64Array::from(counts))],
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
        // A keyless local aggregate may legitimately have no physical columns (for example a
        // count-only partial represented entirely by the downstream record-count contract). Arrow
        // cannot infer its row count from an empty column list, so preserve the number of groups
        // explicitly instead of rejecting the batch.
        let output = RecordBatch::try_new_with_options(
            Arc::new(Schema::new(fields)),
            columns,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(order.len())),
        )
        .expect("failed to build local group-by partial batch");
        drop((states, scalar_states, key_batches, order));
        self.memory
            .set(self.group_order.as_ref().map_or(0, JavaMapOrder::bytes));
        self.memory.account_shrink();
        Ok(output)
    }
}

state_bytes_getter!(
    Java_tech_streamfusion_Native_groupAggregatorStateBytes,
    GroupAggregator
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_groupAggregatorStagingBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let aggregator = unsafe { &*(handle as *const GroupAggregator) };
        aggregator.staging_bytes() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_groupAggregatorStagedKeys<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let aggregator = unsafe { &*(handle as *const GroupAggregator) };
        aggregator.staged_keys() as jlong
    })
}

state_bytes_getter!(
    Java_tech_streamfusion_Native_localGroupAggregatorStateBytes,
    LocalGroupAggregator
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_localGroupAggregatorStagedKeys<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let aggregator = unsafe { &*(handle as *const LocalGroupAggregator) };
        aggregator.order.len() as jlong
    })
}

/// Creates a buffering local two-phase GROUP BY pre-aggregate and returns an opaque handle. It
/// accumulates across batches in memory until flushed; the buffer is transient (drained before each
/// checkpoint on the JVM side), so there is no snapshot/restore.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createLocalGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    aggregate_kinds: JIntArray<'local>,
    value_types: JIntArray<'local>,
    value_columns: JIntArray<'local>,
    filter_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    distinct_view_sources: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    memory_budget_bytes: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
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
        .with_key_timestamp_precisions(read_i32_array(&env, &key_timestamp_precisions))
        .with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, aggregator)
    })
}

fn throw_group_update_error(env: &mut JNIEnv, error: DataFusionError) {
    match error {
        DataFusionError::Execution(message) => {
            if let Ok(message) = env.new_string(message) {
                let _ = env.call_static_method(
                    "tech/streamfusion/compat/TableErrors",
                    "fail",
                    "(Ljava/lang/String;)V",
                    &[jni::objects::JValue::Object(&message)],
                );
            }
        }
        other => throw_memory_limit(env, &other.to_string()),
    }
}

/// Folds an Arrow batch the JVM exported into the buffered per-key accumulators; emits nothing.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_updateLocalGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut LocalGroupAggregator) };
        // The batch must drop before a throw: its release callback upcalls into the JVM, which would
        // clear the pending exception (see updateTumblingAggregator).
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            aggregator.update(&batch)
        };
        if let Err(e) = result {
            throw_group_update_error(&mut env, e);
        }
    })
}

/// Emits the buffered partials (one row per key) and clears the buffer.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushLocalGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut LocalGroupAggregator) };
        match aggregator.try_flush() {
            Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
            Err(error) => throw_group_update_error(&mut env, error),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeLocalGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<LocalGroupAggregator>(handle));
    })
}

/// Creates a non-windowed `GROUP BY` aggregator and returns an opaque handle. The aggregate kinds
/// and per-aggregate value-type codes are positional; `generate_update_before` is the host's
/// per-node changelog flag. Grouping keys travel as `key0..` columns on each input batch.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createGroupAggregatorWithTtlEmission<
    'local,
>(
    env: JNIEnv<'local>,
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
    state_ttl_millis: jlong,
    memory_budget_bytes: jlong,
    emit_unchanged_with_ttl: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_types = read_int_array(&env, &value_types);
        let value_columns = read_int_array(&env, &value_columns);
        let filter_columns = read_int_array(&env, &filter_columns);
        let count_columns = read_int_array(&env, &count_columns);
        let distinct_view_columns = read_int_array(&env, &distinct_view_columns);
        let key_columns = read_columns(&env, &key_columns);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
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
        .with_record_count_column(record_count_column as i64)
        .with_state_ttl(state_ttl_millis)
        .with_ttl_emission(emit_unchanged_with_ttl != 0);
        if mini_batch != 0 {
            aggregator = aggregator.with_mini_batch();
        }
        let aggregator = aggregator.with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, aggregator)
    })
}

/// Folds an input batch into per-key state and exports the changelog rows it produces (the row kinds
/// ride the `$row_kind$` column of the result).
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_updateGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut GroupAggregator) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            aggregator.update(&batch, now_millis)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_group_update_error(&mut env, e),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut GroupAggregator) };
        match aggregator.flush_mini_batch_at(now_millis) {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Lists the non-empty Flink key groups represented by this group aggregator's current state.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotGroupAggregatorPartitions<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jni::sys::jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut GroupAggregator) };
        let precisions = read_i32_array(&env, &timestamp_precisions);
        keyed_state_partition_array(
            &mut env,
            aggregator.snapshot_partitions(max_parallelism as usize, &precisions),
            "group-aggregate",
        )
    })
}

/// Rebuilds a `GROUP BY` aggregator from all raw keyed-state partitions assigned to this subtask.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreGroupAggregatorPartitionsWithTtlEmission<
    'local,
>(
    env: JNIEnv<'local>,
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
    state_ttl_millis: jlong,
    now_millis: jlong,
    snapshots: JObjectArray<'local>,
    memory_budget_bytes: jlong,
    emit_unchanged_with_ttl: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_types = read_int_array(&env, &value_types);
        let value_columns = read_int_array(&env, &value_columns);
        let filter_columns = read_int_array(&env, &filter_columns);
        let count_columns = read_int_array(&env, &count_columns);
        let distinct_view_columns = read_int_array(&env, &distinct_view_columns);
        let key_columns = read_columns(&env, &key_columns);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
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
            now_millis,
        )
        .with_key_timestamp_precisions(key_timestamp_precisions)
        .with_filter_columns(filter_columns)
        .with_count_columns(count_columns)
        .with_distinct_view_columns(distinct_view_columns)
        .with_record_count_column(record_count_column as i64)
        .with_state_ttl(state_ttl_millis)
        .with_ttl_emission(emit_unchanged_with_ttl != 0);
        if mini_batch != 0 {
            aggregator = aggregator.with_mini_batch();
        }
        let aggregator = aggregator.with_memory_budget(memory_budget_bytes);
        boxed_or_throw(&mut env, aggregator)
    })
}

/// Releases the `GROUP BY` aggregator and its native state.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<GroupAggregator>(handle));
    })
}

#[cfg(all(test, feature = "rocksdb-state"))]
mod rocks_codec_tests {
    use super::*;
    use crate::state::RocksStateCodec;

    fn group(records: i64, aggs: Vec<GroupAggState>) -> GroupKeyState {
        GroupKeyState {
            aggs,
            records,
            last_output: None,
            last_output_bytes: 0,
            last_write_ms: 0,
        }
    }

    fn check_columns(codec: &GroupStateCodec, states: &[GroupKeyState], expected: Vec<ArrayRef>) {
        let refs: Vec<_> = states.iter().collect();
        let mut converter = RowConverter::new(
            codec
                .value_fields()
                .into_iter()
                .map(|(_, ty)| SortField::new(ty))
                .collect(),
        )
        .unwrap();
        // These columns describe the existing persisted schema independently of the new codec.
        let legacy_rows = converter.convert_columns(&expected).unwrap();
        let actual_rows = converter
            .convert_columns(&codec.encode_columns(&refs))
            .unwrap();
        assert_eq!(actual_rows.num_rows(), legacy_rows.num_rows());
        for (actual, expected) in actual_rows.iter().zip(legacy_rows.iter()) {
            assert_eq!(actual.data(), expected.data());
        }
        let columns = converter.convert_rows(legacy_rows.iter()).unwrap();
        for (row, original) in states.iter().enumerate() {
            let restored = codec.decode_row(&columns, row).unwrap();
            assert_eq!(restored.records, original.records);
            assert_eq!(restored.aggs.len(), original.aggs.len());
            for (i, (restored, original)) in restored.aggs.iter().zip(&original.aggs).enumerate() {
                assert_eq!(restored.persisted_count(), original.persisted_count());
                assert_eq!(
                    restored.persisted_scalar(&codec.state_types[i]),
                    original.persisted_scalar(&codec.state_types[i]),
                );
            }
        }
        assert_eq!(codec.encode_columns(&[]).len(), expected.len());
        assert!(codec
            .encode_columns(&[])
            .iter()
            .all(|column| column.is_empty()));
    }

    #[test]
    fn numeric_columns_preserve_legacy_rows_and_nulls() {
        let cases = [
            (
                DataType::Int8,
                vec![ScalarValue::Int8(Some(i8::MIN)), ScalarValue::Int8(None)],
            ),
            (
                DataType::Int16,
                vec![ScalarValue::Int16(Some(i16::MIN)), ScalarValue::Int16(None)],
            ),
            (
                DataType::Int32,
                vec![ScalarValue::Int32(Some(i32::MIN)), ScalarValue::Int32(None)],
            ),
            (
                DataType::Int64,
                vec![ScalarValue::Int64(Some(i64::MIN)), ScalarValue::Int64(None)],
            ),
            (
                DataType::Float32,
                vec![
                    ScalarValue::Float32(Some(-0.0)),
                    ScalarValue::Float32(None),
                    ScalarValue::Float32(Some(f32::NAN)),
                ],
            ),
            (
                DataType::Float64,
                vec![
                    ScalarValue::Float64(Some(-0.0)),
                    ScalarValue::Float64(None),
                    ScalarValue::Float64(Some(f64::NAN)),
                ],
            ),
        ];
        for (ty, values) in cases {
            let codec = GroupStateCodec::new(vec![0], vec![ty.clone()], vec![1], vec![]);
            let counts: Vec<i64> = values
                .iter()
                .map(|value| i64::from(!value.is_null()))
                .collect();
            let states: Vec<_> = values
                .iter()
                .zip(&counts)
                .map(|(value, &non_null)| {
                    let mut agg = RunningAgg::new(0, &ty);
                    agg.restore_value(value);
                    group(3, vec![GroupAggState::Running { agg, non_null }])
                })
                .collect();
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(vec![3; values.len()])),
                scalars_to_array(values, &ty),
                Arc::new(Int64Array::from(counts)),
            ];
            check_columns(&codec, &states, columns);
        }
    }

    #[test]
    fn widened_avg_decimal_and_multiset_columns_preserve_legacy_rows() {
        let kinds = vec![4, 4, 0, 1, 7, 9];
        let types = vec![
            DataType::Int16,
            DataType::Float32,
            DataType::Decimal128(12, 2),
            DataType::Utf8,
            DataType::Int64,
            DataType::Int64,
        ];
        let codec = GroupStateCodec::new(kinds.clone(), types.clone(), vec![1; 6], vec![]);
        let mut extreme = GroupAggState::new(1, &DataType::Utf8);
        if let GroupAggState::Extremes { extreme, .. } = &mut extreme {
            *extreme = Some(MinMaxKey::Str("\0\u{1f642}abcdefghijk".into()));
        }
        let first = group(
            10,
            vec![
                GroupAggState::Running {
                    agg: RunningAgg::AvgInt {
                        sum: -40000,
                        result: DataType::Int16,
                    },
                    non_null: 2,
                },
                GroupAggState::Running {
                    agg: RunningAgg::AvgFloat {
                        sum: 1.5,
                        result: DataType::Float32,
                    },
                    non_null: 3,
                },
                GroupAggState::Running {
                    agg: RunningAgg::SumDecimal {
                        sum: 12345,
                        scale: 2,
                        overflow: false,
                    },
                    non_null: 7,
                },
                extreme,
                GroupAggState::Distinct {
                    set: DistinctSet::new(&DataType::Int64),
                    live: 3,
                },
                GroupAggState::DistinctRunning {
                    counts: DistinctSet::new(&DataType::Int64),
                    agg: RunningAgg::SumI64(Some(17)),
                    live: 2,
                },
            ],
        );
        let mut empty = group(
            1,
            kinds
                .iter()
                .zip(&types)
                .map(|(&kind, ty)| GroupAggState::new(kind, ty))
                .collect(),
        );
        if let GroupAggState::Running {
            agg: RunningAgg::SumDecimal { overflow, .. },
            ..
        } = &mut empty.aggs[2]
        {
            *overflow = true;
        }
        let counts = |values: Vec<i64>| Arc::new(Int64Array::from(values)) as ArrayRef;
        let columns = vec![
            counts(vec![10, 1]),
            counts(vec![-40000, 0]),
            counts(vec![2, 0]),
            Arc::new(arrow::array::Float64Array::from(vec![1.5, 0.0])),
            counts(vec![3, 0]),
            Arc::new(
                Decimal128Array::from(vec![Some(12345), None])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
            counts(vec![7, 0]),
            Arc::new(StringArray::from(vec![
                Some("\0\u{1f642}abcdefghijk"),
                None,
            ])),
            counts(vec![0, 0]),
            counts(vec![3, 0]),
            counts(vec![0, 0]),
            Arc::new(Int64Array::from(vec![Some(17), None])),
            counts(vec![2, 0]),
        ];
        check_columns(&codec, &[first, empty], columns);
    }
}
