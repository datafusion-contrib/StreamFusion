use crate::*;

/// Builds a built-in aggregate over an int64 `value` column. SUM/MIN/MAX/COUNT all reduce an int64
/// column to a single int64 with single-scalar partial state, driven through DataFusion's
/// accumulator machinery.
/// Maps a value-type code (matching the JVM side) to the Arrow type of the value column.
pub(crate) fn value_data_type(code: i64) -> DataType {
    match code {
        0 => DataType::Int64,
        1 => DataType::Float64,
        2 => DataType::Int32,
        // 3 is a string value (MIN/MAX over a string, the Extremes multiset); 4/5/6 are narrow.
        3 => DataType::Utf8,
        4 => DataType::Int16,
        5 => DataType::Int8,
        6 => DataType::Float32,
        // Decimal packs precision/scale into the code (2000 + precision*100 + scale), matching the
        // JVM side, so the per-aggregate value type carries them without a wider signature.
        c if c >= 2000 => {
            let packed = c - 2000;
            DataType::Decimal128((packed / 100) as u8, (packed % 100) as i8)
        }
        other => panic!("unsupported value type: {other}"),
    }
}

pub(crate) fn build_builtin(kind: i64, value_type: &DataType) -> AggregateFunctionExpr {
    let function: Arc<AggregateUDF> = match kind {
        0 => sum_udaf(),
        1 => min_udaf(),
        2 => max_udaf(),
        3 => count_udaf(),
        other => panic!("unsupported builtin aggregate kind: {other}"),
    };
    let schema = Arc::new(Schema::new(vec![Field::new("value", value_type.clone(), true)]));
    let value = col("value", &schema).expect("value column");
    AggregateExprBuilder::new(function, vec![value])
        .schema(schema)
        .alias("result")
        .build()
        .expect("failed to build aggregate")
}

/// Average of an integer column matching the host engine's semantics: the sum accumulates in int64,
/// then integer division by the count truncates toward zero and the result is cast back to the
/// input integer type (Flink returns the integer type for AVG of integers, not a float). The
/// two-field partial state (sum, count) rides the general checkpoint path. `result_type` is the
/// input integer type (Int64 or Int32).
#[derive(Debug)]
pub(crate) struct IntegerAvgAccumulator {
    sum: i64,
    count: i64,
    result_type: DataType,
}

impl IntegerAvgAccumulator {
    fn new(result_type: DataType) -> Self {
        IntegerAvgAccumulator { sum: 0, count: 0, result_type }
    }
}

impl Accumulator for IntegerAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        // The value column is the input integer type; sum widens to int64 regardless (as Flink does).
        match self.result_type {
            DataType::Int32 => {
                let array = values[0].as_any().downcast_ref::<Int32Array>().expect("value int32");
                for value in array.iter().flatten() {
                    self.sum += i64::from(value);
                    self.count += 1;
                }
            }
            DataType::Int16 => {
                let array = values[0].as_any().downcast_ref::<Int16Array>().expect("value int16");
                for value in array.iter().flatten() {
                    self.sum += i64::from(value);
                    self.count += 1;
                }
            }
            DataType::Int8 => {
                let array = values[0].as_any().downcast_ref::<Int8Array>().expect("value int8");
                for value in array.iter().flatten() {
                    self.sum += i64::from(value);
                    self.count += 1;
                }
            }
            _ => {
                let array = values[0].as_any().downcast_ref::<Int64Array>().expect("value int64");
                for value in array.iter().flatten() {
                    self.sum += value;
                    self.count += 1;
                }
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let sums = states[0].as_any().downcast_ref::<Int64Array>().expect("sum state int64");
        let counts = states[1].as_any().downcast_ref::<Int64Array>().expect("count state int64");
        self.sum += sums.iter().flatten().sum::<i64>();
        self.count += counts.iter().flatten().sum::<i64>();
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.sum)), ScalarValue::Int64(Some(self.count))])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        // Truncating integer division, then a narrowing cast back to the input type (the host's
        // `cast(sum / count, <type>)`), which wraps the low bits exactly like Rust's `as`.
        let average = (self.count != 0).then(|| self.sum / self.count);
        Ok(match self.result_type {
            DataType::Int32 => ScalarValue::Int32(average.map(|a| a as i32)),
            DataType::Int16 => ScalarValue::Int16(average.map(|a| a as i16)),
            DataType::Int8 => ScalarValue::Int8(average.map(|a| a as i8)),
            _ => ScalarValue::Int64(average),
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Sum of an int32 column matching the host engine's semantics: the accumulator is itself int32 and
/// wraps on overflow (Flink keeps the input type and does not widen, unlike DataFusion's int64 sum),
/// and the result is null when no non-null value was seen. The two-field partial state (sum, count)
/// rides the general checkpoint path; the count distinguishes the empty case from a genuine zero.
#[derive(Debug, Default)]
pub(crate) struct WrappingIntSumAccumulator {
    sum: i32,
    count: i64,
}

impl Accumulator for WrappingIntSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let array = values[0].as_any().downcast_ref::<Int32Array>().expect("value must be int32");
        for value in array.iter().flatten() {
            self.sum = self.sum.wrapping_add(value);
            self.count += 1;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let sums = states[0].as_any().downcast_ref::<Int32Array>().expect("sum state int32");
        let counts = states[1].as_any().downcast_ref::<Int64Array>().expect("count state int64");
        for value in sums.iter().flatten() {
            self.sum = self.sum.wrapping_add(value);
        }
        self.count += counts.iter().flatten().sum::<i64>();
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int32(Some(self.sum)), ScalarValue::Int64(Some(self.count))])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(ScalarValue::Int32((self.count != 0).then_some(self.sum)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Decimal SUM/AVG window accumulator with Flink's semantics: an i128 running sum at the input scale
/// with overflow past DECIMAL(38, s) latched to NULL (Flink's fromBigDecimal, where DataFusion's
/// decimal sum errors instead), plus the non-null count. SUM evaluates to the sum as DECIMAL(38, s);
/// AVG divides sum by count with Flink's exact decimal division (38-significant-digit quotient, then
/// the HALF_UP rescale) at findAvgAggType's DECIMAL(38, max(6, s)). The snapshot state is the
/// (sum, count) pair; a NULL sum with a live count marks an overflowed accumulator on restore.
#[derive(Debug)]
pub(crate) struct DecimalWindowAccumulator {
    sum: i128,
    count: i64,
    overflow: bool,
    scale: i8,
    avg: bool,
}

impl DecimalWindowAccumulator {
    fn new(scale: i8, avg: bool) -> Self {
        DecimalWindowAccumulator { sum: 0, count: 0, overflow: false, scale, avg }
    }
}

impl Accumulator for DecimalWindowAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let array =
            values[0].as_any().downcast_ref::<Decimal128Array>().expect("value decimal128");
        for value in array.iter().flatten() {
            accumulate_decimal(&mut self.sum, &mut self.overflow, value);
            self.count += 1;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let sums = states[0].as_any().downcast_ref::<Decimal128Array>().expect("sum decimal128");
        let counts = states[1].as_any().downcast_ref::<Int64Array>().expect("count state int64");
        for row in 0..sums.len() {
            let count = if counts.is_null(row) { 0 } else { counts.value(row) };
            if sums.is_valid(row) {
                accumulate_decimal(&mut self.sum, &mut self.overflow, sums.value(row));
            } else if count > 0 {
                self.overflow = true; // a live count with a NULL sum is an overflowed partial
            }
            self.count += count;
        }
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        let sum = (!self.overflow && self.count != 0).then_some(self.sum);
        Ok(vec![
            ScalarValue::Decimal128(sum, 38, self.scale),
            ScalarValue::Int64(Some(self.count)),
        ])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        if !self.avg {
            let sum = (!self.overflow && self.count != 0).then_some(self.sum);
            return Ok(ScalarValue::Decimal128(sum, 38, self.scale));
        }
        let result_scale = self.scale.max(6);
        if self.overflow || self.count == 0 {
            return Ok(ScalarValue::Decimal128(None, 38, result_scale));
        }
        let (unscaled, qscale) = quotient_38_digits(self.sum, self.scale, self.count as i128, 0);
        Ok(ScalarValue::Decimal128(
            rescale_half_up(unscaled, qscale, 38, result_scale),
            38,
            result_scale,
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Sum of a narrow integer column (smallint/tinyint) matching the host's semantics: the host keeps
/// the input type and casts back each accumulate step, so the running sum wraps at the narrow width.
/// We fold at int64 but wrap to the width on every step, and emit the narrow type. Null when no
/// non-null value was seen. `data_type` is Int16 or Int8.
#[derive(Debug)]
pub(crate) struct WrappingNarrowSumAccumulator {
    sum: i64,
    count: i64,
    data_type: DataType,
}

impl WrappingNarrowSumAccumulator {
    fn new(data_type: DataType) -> Self {
        WrappingNarrowSumAccumulator { sum: 0, count: 0, data_type }
    }

    fn wrap(&self, value: i64) -> i64 {
        if self.data_type == DataType::Int16 {
            value as i16 as i64
        } else {
            value as i8 as i64
        }
    }

    fn fold_narrow(&mut self, array: &ArrayRef) {
        if self.data_type == DataType::Int16 {
            let array = array.as_any().downcast_ref::<Int16Array>().expect("value int16");
            for value in array.iter().flatten() {
                self.sum = self.wrap(self.sum + i64::from(value));
            }
        } else {
            let array = array.as_any().downcast_ref::<Int8Array>().expect("value int8");
            for value in array.iter().flatten() {
                self.sum = self.wrap(self.sum + i64::from(value));
            }
        }
    }
}

impl Accumulator for WrappingNarrowSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let non_null = values[0].len() - values[0].null_count();
        self.fold_narrow(&values[0]);
        self.count += non_null as i64;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        self.fold_narrow(&states[0]);
        let counts = states[1].as_any().downcast_ref::<Int64Array>().expect("count state int64");
        self.count += counts.iter().flatten().sum::<i64>();
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        let sum = (self.count != 0).then_some(self.sum);
        let sum = if self.data_type == DataType::Int16 {
            ScalarValue::Int16(sum.map(|s| s as i16))
        } else {
            ScalarValue::Int8(sum.map(|s| s as i8))
        };
        Ok(vec![sum, ScalarValue::Int64(Some(self.count))])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        let sum = (self.count != 0).then_some(self.sum);
        Ok(if self.data_type == DataType::Int16 {
            ScalarValue::Int16(sum.map(|s| s as i16))
        } else {
            ScalarValue::Int8(sum.map(|s| s as i8))
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Sum of a float (4-byte) column matching the host: the host keeps the input type and accumulates
/// the running sum in float, so it rounds to 4-byte precision on every step (unlike DataFusion's
/// sum, which widens to double). We accumulate in f32 in the same per-row fold order, so the result
/// is bit-identical. Null when no non-null value was seen.
#[derive(Debug, Default)]
pub(crate) struct FloatSumAccumulator {
    sum: f32,
    count: i64,
}

impl Accumulator for FloatSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let array = values[0].as_any().downcast_ref::<Float32Array>().expect("value float32");
        for value in array.iter().flatten() {
            self.sum += value;
            self.count += 1;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let sums = states[0].as_any().downcast_ref::<Float32Array>().expect("sum state float32");
        let counts = states[1].as_any().downcast_ref::<Int64Array>().expect("count state int64");
        for value in sums.iter().flatten() {
            self.sum += value;
        }
        self.count += counts.iter().flatten().sum::<i64>();
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float32((self.count != 0).then_some(self.sum)),
            ScalarValue::Int64(Some(self.count)),
        ])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(ScalarValue::Float32((self.count != 0).then_some(self.sum)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Average of a float column matching the host: the host accumulates the sum in double, divides by
/// the count in double, then narrows the result to float. The two-field partial state (sum, count)
/// rides the general checkpoint path.
#[derive(Debug, Default)]
pub(crate) struct FloatAvgAccumulator {
    sum: f64,
    count: i64,
}

impl Accumulator for FloatAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let array = values[0].as_any().downcast_ref::<Float32Array>().expect("value float32");
        for value in array.iter().flatten() {
            self.sum += f64::from(value);
            self.count += 1;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let sums = states[0].as_any().downcast_ref::<arrow::array::Float64Array>().expect("sum f64");
        let counts = states[1].as_any().downcast_ref::<Int64Array>().expect("count state int64");
        self.sum += sums.iter().flatten().sum::<f64>();
        self.count += counts.iter().flatten().sum::<i64>();
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float64(Some(self.sum)), ScalarValue::Int64(Some(self.count))])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        let average = (self.count != 0).then(|| (self.sum / self.count as f64) as f32);
        Ok(ScalarValue::Float32(average))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Average of a double column matching the host: the sum accumulates in double, divided by the
/// count in double (no narrowing). DataFusion's own avg matches this, but the project routes AVG
/// through custom accumulators (integer AVG truncates), so this keeps the path uniform.
#[derive(Debug, Default)]
pub(crate) struct DoubleAvgAccumulator {
    sum: f64,
    count: i64,
}

impl Accumulator for DoubleAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::common::Result<()> {
        let array = values[0].as_any().downcast_ref::<arrow::array::Float64Array>().expect("f64");
        for value in array.iter().flatten() {
            self.sum += value;
            self.count += 1;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::common::Result<()> {
        let sums = states[0].as_any().downcast_ref::<arrow::array::Float64Array>().expect("sum f64");
        let counts = states[1].as_any().downcast_ref::<Int64Array>().expect("count state int64");
        self.sum += sums.iter().flatten().sum::<f64>();
        self.count += counts.iter().flatten().sum::<i64>();
        Ok(())
    }

    fn state(&mut self) -> datafusion::common::Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Float64(Some(self.sum)), ScalarValue::Int64(Some(self.count))])
    }

    fn evaluate(&mut self) -> datafusion::common::Result<ScalarValue> {
        Ok(ScalarValue::Float64((self.count != 0).then(|| self.sum / self.count as f64)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// The per-window aggregate. Built-in aggregates come from DataFusion; the averages and the
/// narrow/int32/float wrapping sums are small custom accumulators so their results match the host
/// exactly. All expose mergeable partial state, so windows accumulate incrementally and checkpoint
/// uniformly.
pub(crate) enum WindowAggregate {
    Builtin(AggregateFunctionExpr),
    IntegerAvg(DataType),
    WrappingIntSum,
    WrappingNarrowSum(DataType),
    FloatSum,
    FloatAvg,
    DoubleAvg,
    // Decimal SUM/AVG carry Flink's semantics, which DataFusion's decimal aggregates don't: SUM is
    // an i128 running sum at the input scale reported as DECIMAL(38, s) with overflow → NULL (not an
    // error), and AVG divides that sum by the non-null count with Flink's exact decimal division,
    // reported as findAvgAggType's DECIMAL(38, max(6, s)).
    DecimalSum { scale: i8 },
    DecimalAvg { scale: i8 },
}

impl WindowAggregate {
    fn new(kind: i64, value_type: &DataType) -> Self {
        match (kind, value_type) {
            // SUM over a narrow int keeps the host's narrow, wrapping semantics rather than widening.
            (0, DataType::Int32) => WindowAggregate::WrappingIntSum,
            (0, DataType::Int16 | DataType::Int8) => {
                WindowAggregate::WrappingNarrowSum(value_type.clone())
            }
            // SUM over float keeps the host's 4-byte precision rather than widening to double.
            (0, DataType::Float32) => WindowAggregate::FloatSum,
            (0, DataType::Decimal128(_, s)) => WindowAggregate::DecimalSum { scale: *s },
            (0..=3, _) => WindowAggregate::Builtin(build_builtin(kind, value_type)),
            // Float AVG sums in double and narrows to float; double AVG stays double; integer AVG
            // truncates to its type.
            (4, DataType::Float32) => WindowAggregate::FloatAvg,
            (4, DataType::Float64) => WindowAggregate::DoubleAvg,
            (4, DataType::Decimal128(_, s)) => WindowAggregate::DecimalAvg { scale: *s },
            (4, _) => WindowAggregate::IntegerAvg(value_type.clone()),
            (other, _) => panic!("unsupported aggregate kind: {other}"),
        }
    }

    pub(crate) fn create_accumulator(&self) -> Box<dyn Accumulator> {
        match self {
            WindowAggregate::Builtin(aggregate) => {
                aggregate.create_accumulator().expect("failed to create accumulator")
            }
            WindowAggregate::IntegerAvg(result_type) => {
                Box::new(IntegerAvgAccumulator::new(result_type.clone()))
            }
            WindowAggregate::WrappingIntSum => Box::<WrappingIntSumAccumulator>::default(),
            WindowAggregate::WrappingNarrowSum(data_type) => {
                Box::new(WrappingNarrowSumAccumulator::new(data_type.clone()))
            }
            WindowAggregate::FloatSum => Box::<FloatSumAccumulator>::default(),
            WindowAggregate::FloatAvg => Box::<FloatAvgAccumulator>::default(),
            WindowAggregate::DoubleAvg => Box::<DoubleAvgAccumulator>::default(),
            WindowAggregate::DecimalSum { scale } => {
                Box::new(DecimalWindowAccumulator::new(*scale, false))
            }
            WindowAggregate::DecimalAvg { scale } => {
                Box::new(DecimalWindowAccumulator::new(*scale, true))
            }
        }
    }

    pub(crate) fn state_fields(&self) -> Vec<Field> {
        match self {
            WindowAggregate::Builtin(aggregate) => aggregate
                .state_fields()
                .expect("state fields")
                .iter()
                .map(|field| field.as_ref().clone())
                .collect(),
            WindowAggregate::IntegerAvg(_) => vec![
                Field::new("sum", DataType::Int64, true),
                Field::new("count", DataType::Int64, true),
            ],
            WindowAggregate::WrappingIntSum => vec![
                Field::new("sum", DataType::Int32, true),
                Field::new("count", DataType::Int64, true),
            ],
            WindowAggregate::WrappingNarrowSum(data_type) => vec![
                Field::new("sum", data_type.clone(), true),
                Field::new("count", DataType::Int64, true),
            ],
            WindowAggregate::FloatSum => vec![
                Field::new("sum", DataType::Float32, true),
                Field::new("count", DataType::Int64, true),
            ],
            WindowAggregate::FloatAvg | WindowAggregate::DoubleAvg => vec![
                Field::new("sum", DataType::Float64, true),
                Field::new("count", DataType::Int64, true),
            ],
            WindowAggregate::DecimalSum { scale } | WindowAggregate::DecimalAvg { scale } => vec![
                Field::new("sum", DataType::Decimal128(38, *scale), true),
                Field::new("count", DataType::Int64, true),
            ],
        }
    }

    /// The aggregate's output type (e.g. int64 for a sum of int64, float64 for a sum of float64).
    pub(crate) fn result_type(&self) -> DataType {
        match self {
            WindowAggregate::Builtin(aggregate) => aggregate.field().data_type().clone(),
            WindowAggregate::IntegerAvg(result_type) => result_type.clone(),
            WindowAggregate::WrappingIntSum => DataType::Int32,
            WindowAggregate::WrappingNarrowSum(data_type) => data_type.clone(),
            WindowAggregate::FloatSum | WindowAggregate::FloatAvg => DataType::Float32,
            WindowAggregate::DoubleAvg => DataType::Float64,
            WindowAggregate::DecimalSum { scale } => DataType::Decimal128(38, *scale),
            // Flink's findAvgAggType: DECIMAL(38, max(6, s)).
            WindowAggregate::DecimalAvg { scale } => DataType::Decimal128(38, (*scale).max(6)),
        }
    }
}

/// Builds one aggregate per (kind, value-type code) pair, positionally. Per-aggregate value types
/// let a single window aggregate compute over different value columns (e.g. `SUM(a), SUM(b)`).
pub(crate) fn build_aggregates(kinds: &[i64], value_types: &[i64]) -> Vec<WindowAggregate> {
    kinds
        .iter()
        .zip(value_types)
        .map(|(&kind, &code)| WindowAggregate::new(kind, &value_data_type(code)))
        .collect()
}

/// Builds an array from per-row scalars, using the given type for the empty case (where the
/// element type cannot be inferred from the values).
pub(crate) fn scalars_to_array(scalars: Vec<ScalarValue>, data_type: &DataType) -> ArrayRef {
    if scalars.is_empty() {
        return new_empty_array(data_type);
    }
    let array = ScalarValue::iter_to_array(scalars).expect("failed to build array");
    // For scalars the reconstructed type already equals `data_type` (no-op). For a nested type
    // (List/Struct/Map), ScalarValue reconstruction names the inner field generically (e.g. "item",
    // nullable) which need not match the declared column's field metadata; cast reconciles it so the
    // column's type matches the schema and RecordBatch assembly accepts it.
    if array.data_type() == data_type {
        array
    } else {
        arrow::compute::cast(&array, data_type)
            .expect("failed to cast reconstructed array to its column type")
    }
}

/// A typed NULL scalar of the given type (the value an aggregate reports when it has no live input).
pub(crate) fn null_scalar(data_type: &DataType) -> ScalarValue {
    ScalarValue::try_from(data_type).expect("null scalar of type")
}

/// Estimated heap footprint of one group's accumulators.
pub(crate) fn accumulators_bytes(accumulators: &[Box<dyn Accumulator>]) -> usize {
    accumulators.iter().map(|a| a.size()).sum()
}

/// Event-time `OVER (PARTITION BY … ORDER BY rt RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
/// aggregation: one never-evicting running accumulator set per partition key (an empty key, i.e. no
/// `PARTITION BY`, is the single-group case). Given a batch of already-complete rows (rowtime <= the
/// watermark), it folds them per key in rowtime order and returns each row's running aggregate. RANGE
/// means rows sharing a rowtime within a key share the post-fold value, so all rows of an rt group
/// are folded before any is read. Accumulators persist across calls (UNBOUNDED PRECEDING). Emits one
/// value per input row, in input order, so the caller can zip it back onto the passed-through columns.
/// One non-null value of the OVER value column, in the column's type.
#[derive(Clone, Copy)]
pub(crate) enum Num {
    I64(i64),
    I32(i32),
    I16(i16),
    I8(i8),
    F64(f64),
    F32(f32),
    /// A DECIMAL value as its unscaled i128 (the scale is fixed per aggregate).
    I128(i128),
}

/// The OVER value column downcast once, so the per-row fold reads a typed value without a per-row
/// downcast. `None` for a null row (which the aggregates skip).
pub(crate) enum ValueColumn<'a> {
    I64(&'a Int64Array),
    I32(&'a Int32Array),
    I16(&'a arrow::array::Int16Array),
    I8(&'a Int8Array),
    F64(&'a arrow::array::Float64Array),
    F32(&'a arrow::array::Float32Array),
    Decimal128(&'a Decimal128Array),
    // Any column read only for null-ness: COUNT over a non-numeric (e.g. ARRAY/MAP/ROW) value
    // column, where only "is this row non-null" matters. The dummy value is never folded (COUNT
    // ignores it), so the matcher admits this only for COUNT.
    NullOnly(&'a ArrayRef),
}

impl ValueColumn<'_> {
    pub(crate) fn at(&self, row: usize) -> Option<Num> {
        match self {
            ValueColumn::I64(a) => (!a.is_null(row)).then(|| Num::I64(a.value(row))),
            ValueColumn::I32(a) => (!a.is_null(row)).then(|| Num::I32(a.value(row))),
            ValueColumn::I16(a) => (!a.is_null(row)).then(|| Num::I16(a.value(row))),
            ValueColumn::I8(a) => (!a.is_null(row)).then(|| Num::I8(a.value(row))),
            ValueColumn::F64(a) => (!a.is_null(row)).then(|| Num::F64(a.value(row))),
            ValueColumn::F32(a) => (!a.is_null(row)).then(|| Num::F32(a.value(row))),
            ValueColumn::Decimal128(a) => (!a.is_null(row)).then(|| Num::I128(a.value(row))),
            ValueColumn::NullOnly(a) => (!a.is_null(row)).then_some(Num::I64(0)),
        }
    }
}

/// Per-key running state for one OVER aggregate. Folded directly per row — no per-row DataFusion
/// accumulator call — matching DataFusion's accumulators exactly: integer SUM wraps on overflow (as
/// `sum_udaf` and the int-sum accumulator do), and all four skip null values. The value type is
/// fixed per aggregator, so each variant pairs a kind with that type. See divergences/11.
pub(crate) enum RunningAgg {
    SumI64(Option<i64>),
    MinI64(Option<i64>),
    MaxI64(Option<i64>),
    SumI32(Option<i32>),
    MinI32(Option<i32>),
    MaxI32(Option<i32>),
    SumF64(Option<f64>),
    MinF64(Option<f64>),
    MaxF64(Option<f64>),
    Count(i64),
    // SUM over DECIMAL(p, s): accumulate the unscaled i128 at the input scale; the result is
    // DECIMAL(38, s) (Flink's findSumAggType). `overflow` latches once the running sum no longer
    // fits DECIMAL(38, s) (|value| >= 10^38), at which point Flink reports NULL.
    SumDecimal { sum: i128, scale: i8, overflow: bool },
    // AVG over DECIMAL(p, s): the running sum is SUM's DECIMAL(38, s) accumulator (overflow latches
    // NULL, like SUM); the count lives in GroupAggState's `non_null`. The emit divides sum by count
    // with Flink's exact decimal division and reports DECIMAL(38, max(6, s)) — findAvgAggType's
    // derivation, the type the planner declares for the call.
    AvgDecimal { sum: i128, scale: i8, overflow: bool },
    // MIN/MAX over DECIMAL(p, s): the value lives in the Extremes multiset (this variant is never
    // folded), so this carries only the precision/scale to report the result type DECIMAL(p, s).
    MinMaxDecimal { precision: u8, scale: i8 },
    // MIN/MAX over a string: likewise lives in the Extremes multiset; this only reports the result
    // type (the converter's Utf8). Never folded.
    MinMaxStr,
    // FIRST_VALUE / LAST_VALUE: hold the first / most-recent non-null value seen (None until one
    // arrives → emits NULL, matching Flink, which ignores nulls in these functions).
    FirstI64(Option<i64>),
    LastI64(Option<i64>),
    FirstI32(Option<i32>),
    LastI32(Option<i32>),
    FirstF64(Option<f64>),
    LastF64(Option<f64>),
    // Narrow ints (SMALLINT/TINYINT) and 4-byte float keep the host's narrow result type: integer
    // SUM wraps at the input width, float SUM keeps 4-byte precision (matching the tumbling
    // WrappingNarrowSum / FloatSum). MIN/MAX/FIRST/LAST keep the value in its narrow type.
    SumI16(Option<i16>),
    MinI16(Option<i16>),
    MaxI16(Option<i16>),
    FirstI16(Option<i16>),
    LastI16(Option<i16>),
    SumI8(Option<i8>),
    MinI8(Option<i8>),
    MaxI8(Option<i8>),
    FirstI8(Option<i8>),
    LastI8(Option<i8>),
    SumF32(Option<f32>),
    MinF32(Option<f32>),
    MaxF32(Option<f32>),
    FirstF32(Option<f32>),
    LastF32(Option<f32>),
    // AVG: a running sum plus the non-null count (the count lives in GroupAggState's `non_null`, so
    // the variant carries only the sum and the declared result type). Matching Flink's AvgAggFunction:
    // the sum widens to BIGINT for any integer input and DOUBLE for float/double, the result casts back
    // to the input type, and the value is `count == 0 ? NULL : sum / count` (integer division truncates
    // toward zero). Decimal AVG is not modelled (the matcher leaves it on the host).
    AvgInt { sum: i64, result: DataType },
    AvgFloat { sum: f64, result: DataType },
    // The LOCAL half of a two-phase AVG (kind 8): the widened running sum alone — the count rides a
    // separate COUNT partial state. Folds like AvgInt/AvgFloat (any integer / float input widens),
    // but emits the raw sum, not a quotient; the global half divides after merging. Matching Flink's
    // AvgAggFunction, the sum starts at 0 and stays non-NULL (an all-null bundle emits 0, and the
    // count partial 0 lets the global ignore it); the decimal variant alone can go NULL, when the
    // bundle's sum overflows DECIMAL(38, s).
    AvgPartialSumInt(i64),
    AvgPartialSumFloat(f64),
    AvgPartialSumDecimal { sum: i128, scale: i8, overflow: bool },
}

impl RunningAgg {
    pub(crate) fn new(kind: i64, value_type: &DataType) -> Self {
        use RunningAgg::*;
        if kind == 3 || kind == 7 {
            return Count(0); // COUNT and COUNT(DISTINCT) both report a bigint count
        }
        // SUM(DISTINCT) (kind 9) runs a plain SUM inside its distinct set (GroupAggState wraps it);
        // its running/result/state types are exactly SUM's.
        let kind = if kind == 9 { 0 } else { kind };
        // kind: 0=SUM, 1=MIN, 2=MAX, 5=FIRST_VALUE, 6=LAST_VALUE (3=COUNT handled above).
        match (kind, value_type) {
            (0, DataType::Int64) => SumI64(None),
            (1, DataType::Int64) => MinI64(None),
            (2, DataType::Int64) => MaxI64(None),
            (5, DataType::Int64) => FirstI64(None),
            (6, DataType::Int64) => LastI64(None),
            (0, DataType::Int32) => SumI32(None),
            (1, DataType::Int32) => MinI32(None),
            (2, DataType::Int32) => MaxI32(None),
            (5, DataType::Int32) => FirstI32(None),
            (6, DataType::Int32) => LastI32(None),
            (0, DataType::Float64) => SumF64(None),
            (1, DataType::Float64) => MinF64(None),
            (2, DataType::Float64) => MaxF64(None),
            (5, DataType::Float64) => FirstF64(None),
            (6, DataType::Float64) => LastF64(None),
            (0, DataType::Int16) => SumI16(None),
            (1, DataType::Int16) => MinI16(None),
            (2, DataType::Int16) => MaxI16(None),
            (5, DataType::Int16) => FirstI16(None),
            (6, DataType::Int16) => LastI16(None),
            (0, DataType::Int8) => SumI8(None),
            (1, DataType::Int8) => MinI8(None),
            (2, DataType::Int8) => MaxI8(None),
            (5, DataType::Int8) => FirstI8(None),
            (6, DataType::Int8) => LastI8(None),
            (0, DataType::Float32) => SumF32(None),
            (1, DataType::Float32) => MinF32(None),
            (2, DataType::Float32) => MaxF32(None),
            (5, DataType::Float32) => FirstF32(None),
            (6, DataType::Float32) => LastF32(None),
            // AVG(integer) — sum widens to BIGINT, result casts back to the input type on emit.
            (4, DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8) => {
                AvgInt { sum: 0, result: value_type.clone() }
            }
            // Two-phase AVG's local sum partial: the declared type is the WIDENED partial type
            // (BIGINT for integer inputs, DOUBLE for float/double — Flink's AvgAggFunction).
            (8, DataType::Int64) => AvgPartialSumInt(0),
            (8, DataType::Float64) => AvgPartialSumFloat(0.0),
            (8, DataType::Decimal128(_, s)) => {
                AvgPartialSumDecimal { sum: 0, scale: *s, overflow: false }
            }
            // AVG(float/double) — sum in DOUBLE, result casts back to the input type on emit.
            (4, DataType::Float64 | DataType::Float32) => {
                AvgFloat { sum: 0.0, result: value_type.clone() }
            }
            // SUM(DECIMAL(_, s)) — accumulate the unscaled i128 at scale s; result is DECIMAL(38, s).
            (0, DataType::Decimal128(_, s)) => SumDecimal { sum: 0, scale: *s, overflow: false },
            // AVG(DECIMAL(_, s)) — SUM's accumulator plus the exact divide on emit.
            (4, DataType::Decimal128(_, s)) => AvgDecimal { sum: 0, scale: *s, overflow: false },
            // MIN/MAX(DECIMAL(p, s)) — the extreme lives in the multiset; result is DECIMAL(p, s).
            (1 | 2, DataType::Decimal128(p, s)) => MinMaxDecimal { precision: *p, scale: *s },
            // MIN/MAX(string) — the extreme lives in the multiset; result is the converter's Utf8.
            (1 | 2, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => MinMaxStr,
            (k, other) => panic!("unsupported OVER aggregate kind {k} for value type {other:?}"),
        }
    }

    /// Folds one non-null value into the running state.
    pub(crate) fn fold(&mut self, value: Num) {
        use RunningAgg::*;
        match (self, value) {
            (SumI64(s), Num::I64(v)) => *s = Some(s.unwrap_or(0).wrapping_add(v)),
            (MinI64(m), Num::I64(v)) => *m = Some(m.map_or(v, |x| x.min(v))),
            (MaxI64(m), Num::I64(v)) => *m = Some(m.map_or(v, |x| x.max(v))),
            (SumI32(s), Num::I32(v)) => *s = Some(s.unwrap_or(0).wrapping_add(v)),
            (MinI32(m), Num::I32(v)) => *m = Some(m.map_or(v, |x| x.min(v))),
            (MaxI32(m), Num::I32(v)) => *m = Some(m.map_or(v, |x| x.max(v))),
            (SumF64(s), Num::F64(v)) => *s = Some(s.unwrap_or(0.0) + v),
            (MinF64(m), Num::F64(v)) => *m = Some(m.map_or(v, |x| x.min(v))),
            (MaxF64(m), Num::F64(v)) => *m = Some(m.map_or(v, |x| x.max(v))),
            (Count(c), _) => *c += 1,
            (SumDecimal { sum, overflow, .. }, Num::I128(v)) => accumulate_decimal(sum, overflow, v),
            (AvgDecimal { sum, overflow, .. }, Num::I128(v)) => accumulate_decimal(sum, overflow, v),
            // FIRST_VALUE keeps the earliest value (set once); LAST_VALUE takes the most recent.
            (FirstI64(f), Num::I64(v)) => *f = Some(f.unwrap_or(v)),
            (LastI64(l), Num::I64(v)) => *l = Some(v),
            (FirstI32(f), Num::I32(v)) => *f = Some(f.unwrap_or(v)),
            (LastI32(l), Num::I32(v)) => *l = Some(v),
            (FirstF64(f), Num::F64(v)) => *f = Some(f.unwrap_or(v)),
            (LastF64(l), Num::F64(v)) => *l = Some(v),
            (SumI16(s), Num::I16(v)) => *s = Some(s.unwrap_or(0).wrapping_add(v)),
            (MinI16(m), Num::I16(v)) => *m = Some(m.map_or(v, |x| x.min(v))),
            (MaxI16(m), Num::I16(v)) => *m = Some(m.map_or(v, |x| x.max(v))),
            (FirstI16(f), Num::I16(v)) => *f = Some(f.unwrap_or(v)),
            (LastI16(l), Num::I16(v)) => *l = Some(v),
            (SumI8(s), Num::I8(v)) => *s = Some(s.unwrap_or(0).wrapping_add(v)),
            (MinI8(m), Num::I8(v)) => *m = Some(m.map_or(v, |x| x.min(v))),
            (MaxI8(m), Num::I8(v)) => *m = Some(m.map_or(v, |x| x.max(v))),
            (FirstI8(f), Num::I8(v)) => *f = Some(f.unwrap_or(v)),
            (LastI8(l), Num::I8(v)) => *l = Some(v),
            (SumF32(s), Num::F32(v)) => *s = Some(s.unwrap_or(0.0) + v),
            (MinF32(m), Num::F32(v)) => *m = Some(m.map_or(v, |x| x.min(v))),
            (MaxF32(m), Num::F32(v)) => *m = Some(m.map_or(v, |x| x.max(v))),
            (FirstF32(f), Num::F32(v)) => *f = Some(f.unwrap_or(v)),
            (LastF32(l), Num::F32(v)) => *l = Some(v),
            // AVG: sum widens to the running type; the count is tracked by GroupAggState's `non_null`.
            (AvgInt { sum, .. }, Num::I64(v)) => *sum = sum.wrapping_add(v),
            (AvgInt { sum, .. }, Num::I32(v)) => *sum = sum.wrapping_add(v as i64),
            (AvgInt { sum, .. }, Num::I16(v)) => *sum = sum.wrapping_add(v as i64),
            (AvgInt { sum, .. }, Num::I8(v)) => *sum = sum.wrapping_add(v as i64),
            (AvgFloat { sum, .. }, Num::F64(v)) => *sum += v,
            (AvgFloat { sum, .. }, Num::F32(v)) => *sum += v as f64,
            (AvgPartialSumInt(sum), Num::I64(v)) => *sum = sum.wrapping_add(v),
            (AvgPartialSumInt(sum), Num::I32(v)) => *sum = sum.wrapping_add(v as i64),
            (AvgPartialSumInt(sum), Num::I16(v)) => *sum = sum.wrapping_add(v as i64),
            (AvgPartialSumInt(sum), Num::I8(v)) => *sum = sum.wrapping_add(v as i64),
            (AvgPartialSumFloat(sum), Num::F64(v)) => *sum += v,
            (AvgPartialSumFloat(sum), Num::F32(v)) => *sum += v as f64,
            (AvgPartialSumDecimal { sum, overflow, .. }, Num::I128(v)) => {
                accumulate_decimal(sum, overflow, v)
            }
            _ => unreachable!("OVER value type does not match aggregate state"),
        }
    }

    /// Reverses one value out of the running state — the changelog retraction of {@link #fold}. Only
    /// the additive aggregates support it: SUM subtracts, COUNT decrements. MIN/MAX cannot be
    /// retracted incrementally, so they are never admitted over a retracting input.
    pub(crate) fn retract(&mut self, value: Num) {
        use RunningAgg::*;
        match (self, value) {
            (SumI64(s), Num::I64(v)) => *s = Some(s.unwrap_or(0).wrapping_sub(v)),
            (SumI32(s), Num::I32(v)) => *s = Some(s.unwrap_or(0).wrapping_sub(v)),
            (SumF64(s), Num::F64(v)) => *s = Some(s.unwrap_or(0.0) - v),
            (SumI16(s), Num::I16(v)) => *s = Some(s.unwrap_or(0).wrapping_sub(v)),
            (SumI8(s), Num::I8(v)) => *s = Some(s.unwrap_or(0).wrapping_sub(v)),
            (SumF32(s), Num::F32(v)) => *s = Some(s.unwrap_or(0.0) - v),
            (Count(c), _) => *c -= 1,
            (SumDecimal { sum, overflow, .. }, Num::I128(v)) => {
                accumulate_decimal(sum, overflow, v.wrapping_neg())
            }
            (AvgDecimal { sum, overflow, .. }, Num::I128(v)) => {
                accumulate_decimal(sum, overflow, v.wrapping_neg())
            }
            (AvgInt { sum, .. }, Num::I64(v)) => *sum = sum.wrapping_sub(v),
            (AvgInt { sum, .. }, Num::I32(v)) => *sum = sum.wrapping_sub(v as i64),
            (AvgInt { sum, .. }, Num::I16(v)) => *sum = sum.wrapping_sub(v as i64),
            (AvgInt { sum, .. }, Num::I8(v)) => *sum = sum.wrapping_sub(v as i64),
            (AvgFloat { sum, .. }, Num::F64(v)) => *sum -= v,
            (AvgFloat { sum, .. }, Num::F32(v)) => *sum -= v as f64,
            (AvgPartialSumInt(sum), Num::I64(v)) => *sum = sum.wrapping_sub(v),
            (AvgPartialSumInt(sum), Num::I32(v)) => *sum = sum.wrapping_sub(v as i64),
            (AvgPartialSumFloat(sum), Num::F64(v)) => *sum -= v,
            _ => unreachable!("aggregate does not support retraction"),
        }
    }

    /// The current running value (also the checkpointed state).
    pub(crate) fn emit(&self) -> ScalarValue {
        use RunningAgg::*;
        match self {
            SumI64(v) | MinI64(v) | MaxI64(v) | FirstI64(v) | LastI64(v) => ScalarValue::Int64(*v),
            SumI32(v) | MinI32(v) | MaxI32(v) | FirstI32(v) | LastI32(v) => ScalarValue::Int32(*v),
            SumF64(v) | MinF64(v) | MaxF64(v) | FirstF64(v) | LastF64(v) => ScalarValue::Float64(*v),
            SumI16(v) | MinI16(v) | MaxI16(v) | FirstI16(v) | LastI16(v) => ScalarValue::Int16(*v),
            SumI8(v) | MinI8(v) | MaxI8(v) | FirstI8(v) | LastI8(v) => ScalarValue::Int8(*v),
            SumF32(v) | MinF32(v) | MaxF32(v) | FirstF32(v) | LastF32(v) => ScalarValue::Float32(*v),
            Count(c) => ScalarValue::Int64(Some(*c)),
            // Overflow past DECIMAL(38, s) reports NULL, matching Flink's fromBigDecimal.
            SumDecimal { sum, scale, overflow } => {
                ScalarValue::Decimal128((!*overflow).then_some(*sum), 38, *scale)
            }
            // Never folded — MIN/MAX state is the Extremes multiset, which emits via MinMaxKey::scalar.
            MinMaxDecimal { precision, scale } => ScalarValue::Decimal128(None, *precision, *scale),
            MinMaxStr => ScalarValue::Utf8(None),
            // The checkpointed state is the raw running sum (typed by state_type, wider than result);
            // the average itself is computed in GroupAggState::emit, where the count (non_null) lives.
            AvgInt { sum, .. } => ScalarValue::Int64(Some(*sum)),
            AvgFloat { sum, .. } => ScalarValue::Float64(Some(*sum)),
            AvgDecimal { sum, scale, overflow } => {
                ScalarValue::Decimal128((!*overflow).then_some(*sum), 38, *scale)
            }
            // The two-phase AVG's local sum partial IS the raw widened sum (the local is a transient
            // bundle — this is its flush emit, never a checkpoint). Only the decimal sum can be NULL
            // (a bundle whose sum overflowed DECIMAL(38, s)).
            AvgPartialSumInt(sum) => ScalarValue::Int64(Some(*sum)),
            AvgPartialSumFloat(sum) => ScalarValue::Float64(Some(*sum)),
            AvgPartialSumDecimal { sum, scale, overflow } => {
                ScalarValue::Decimal128((!*overflow).then_some(*sum), 38, *scale)
            }
        }
    }

    /// The Arrow type of the checkpointed state scalar from {@link #emit}. Equals {@link #result_type}
    /// except for AVG, whose state is the wider running sum (BIGINT / DOUBLE) rather than the result.
    pub(crate) fn state_type(&self) -> DataType {
        use RunningAgg::*;
        match self {
            AvgInt { .. } => DataType::Int64,
            AvgFloat { .. } => DataType::Float64,
            AvgDecimal { scale, .. } => DataType::Decimal128(38, *scale),
            _ => self.result_type(),
        }
    }

    pub(crate) fn result_type(&self) -> DataType {
        use RunningAgg::*;
        match self {
            SumI64(_) | MinI64(_) | MaxI64(_) | Count(_) | FirstI64(_) | LastI64(_) => {
                DataType::Int64
            }
            SumI32(_) | MinI32(_) | MaxI32(_) | FirstI32(_) | LastI32(_) => DataType::Int32,
            SumF64(_) | MinF64(_) | MaxF64(_) | FirstF64(_) | LastF64(_) => DataType::Float64,
            SumI16(_) | MinI16(_) | MaxI16(_) | FirstI16(_) | LastI16(_) => DataType::Int16,
            SumI8(_) | MinI8(_) | MaxI8(_) | FirstI8(_) | LastI8(_) => DataType::Int8,
            SumF32(_) | MinF32(_) | MaxF32(_) | FirstF32(_) | LastF32(_) => DataType::Float32,
            SumDecimal { scale, .. } => DataType::Decimal128(38, *scale),
            // Flink's findAvgAggType for DECIMAL(p, s): DECIMAL(38, max(6, s)).
            AvgDecimal { scale, .. } => DataType::Decimal128(38, (*scale).max(6)),
            MinMaxDecimal { precision, scale } => DataType::Decimal128(*precision, *scale),
            MinMaxStr => DataType::Utf8,
            AvgInt { result, .. } | AvgFloat { result, .. } => result.clone(),
            AvgPartialSumInt(_) => DataType::Int64,
            AvgPartialSumFloat(_) => DataType::Float64,
            AvgPartialSumDecimal { scale, .. } => DataType::Decimal128(38, *scale),
        }
    }

    pub(crate) fn restore_value(&mut self, scalar: &ScalarValue) {
        use RunningAgg::*;
        match (self, scalar) {
            (Count(c), ScalarValue::Int64(Some(v))) => *c = *v,
            (SumI64(s) | MinI64(s) | MaxI64(s) | FirstI64(s) | LastI64(s), ScalarValue::Int64(v)) => {
                *s = *v
            }
            (SumI32(s) | MinI32(s) | MaxI32(s) | FirstI32(s) | LastI32(s), ScalarValue::Int32(v)) => {
                *s = *v
            }
            (
                SumF64(s) | MinF64(s) | MaxF64(s) | FirstF64(s) | LastF64(s),
                ScalarValue::Float64(v),
            ) => *s = *v,
            (SumI16(s) | MinI16(s) | MaxI16(s) | FirstI16(s) | LastI16(s), ScalarValue::Int16(v)) => {
                *s = *v
            }
            (SumI8(s) | MinI8(s) | MaxI8(s) | FirstI8(s) | LastI8(s), ScalarValue::Int8(v)) => *s = *v,
            (SumF32(s) | MinF32(s) | MaxF32(s) | FirstF32(s) | LastF32(s), ScalarValue::Float32(v)) => {
                *s = *v
            }
            // A NULL snapshot value means the running sum had overflowed DECIMAL(38, s).
            (SumDecimal { sum, overflow, .. }, ScalarValue::Decimal128(v, _, _)) => match v {
                Some(x) => {
                    *sum = *x;
                    *overflow = false;
                }
                None => *overflow = true,
            },
            // AVG restores the running sum; the count is restored from the `non_null` column separately.
            (AvgInt { sum, .. }, ScalarValue::Int64(Some(v))) => *sum = *v,
            (AvgDecimal { sum, overflow, .. }, ScalarValue::Decimal128(v, _, _)) => match v {
                Some(x) => {
                    *sum = *x;
                    *overflow = false;
                }
                None => *overflow = true,
            },
            (AvgFloat { sum, .. }, ScalarValue::Float64(Some(v))) => *sum = *v,
            _ => panic!("OVER state type mismatch on restore"),
        }
    }
}

/// The exclusive bound for a `DECIMAL(38, _)` magnitude: a value fits iff `|value| < 10^38`.
pub(crate) const DECIMAL128_MAX: i128 = 10i128.pow(38);

/// Adds `delta` to a decimal running sum, latching `overflow` once it no longer fits DECIMAL(38, s)
/// (matching Flink's `fromBigDecimal`, which returns NULL past precision 38). Once overflowed it stays
/// overflowed — the lost magnitude can't be recovered by a later retraction.
pub(crate) fn accumulate_decimal(sum: &mut i128, overflow: &mut bool, delta: i128) {
    if *overflow {
        return;
    }
    match sum.checked_add(delta) {
        Some(t) if t > -DECIMAL128_MAX && t < DECIMAL128_MAX => *sum = t,
        _ => *overflow = true,
    }
}

/// A single OVER value as a typed Arrow scalar of the value column's type (null = no value).
pub(crate) fn num_to_scalar(value_type: &DataType, num: Option<Num>) -> ScalarValue {
    match (value_type, num) {
        (_, Some(Num::I64(v))) => ScalarValue::Int64(Some(v)),
        (_, Some(Num::I32(v))) => ScalarValue::Int32(Some(v)),
        (_, Some(Num::I16(v))) => ScalarValue::Int16(Some(v)),
        (_, Some(Num::I8(v))) => ScalarValue::Int8(Some(v)),
        (_, Some(Num::F64(v))) => ScalarValue::Float64(Some(v)),
        (_, Some(Num::F32(v))) => ScalarValue::Float32(Some(v)),
        (_, Some(Num::I128(_))) => unreachable!("decimal bounded OVER is not admitted"),
        (DataType::Int64, None) => ScalarValue::Int64(None),
        (DataType::Int32, None) => ScalarValue::Int32(None),
        (DataType::Int16, None) => ScalarValue::Int16(None),
        (DataType::Int8, None) => ScalarValue::Int8(None),
        (DataType::Float64, None) => ScalarValue::Float64(None),
        (DataType::Float32, None) => ScalarValue::Float32(None),
        (other, _) => panic!("unsupported bounded OVER value type: {other:?}"),
    }
}

/// Reads a typed Arrow scalar back into an OVER value (null → None, skipped by the aggregates).
pub(crate) fn num_from_scalar(scalar: &ScalarValue) -> Option<Num> {
    match scalar {
        ScalarValue::Int64(v) => v.map(Num::I64),
        ScalarValue::Int32(v) => v.map(Num::I32),
        ScalarValue::Int16(v) => v.map(Num::I16),
        ScalarValue::Int8(v) => v.map(Num::I8),
        ScalarValue::Float64(v) => v.map(Num::F64),
        ScalarValue::Float32(v) => v.map(Num::F32),
        other => panic!("unsupported bounded OVER value scalar: {other:?}"),
    }
}

/// The numeric fold value of a distinct-SUM scalar — the value types the matcher admits for SUM
/// (bigint/int/double, and DECIMAL as its unscaled i128).
pub(crate) fn distinct_num(value: &ScalarValue) -> Num {
    match value {
        ScalarValue::Int64(Some(v)) => Num::I64(*v),
        ScalarValue::Int32(Some(v)) => Num::I32(*v),
        ScalarValue::Float64(Some(v)) => Num::F64(*v),
        ScalarValue::Decimal128(Some(v), _, _) => Num::I128(*v),
        other => panic!("unsupported distinct SUM value {other:?}"),
    }
}

/// Casts an integer average (`sum / count`, already truncated toward zero in i64) back to the AVG
/// input type — Flink's `cast(div(sum, count), resultType)`.
pub(crate) fn avg_int_scalar(value: i64, result: &DataType) -> ScalarValue {
    match result {
        DataType::Int64 => ScalarValue::Int64(Some(value)),
        DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
        DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
        DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
        other => panic!("unsupported integer AVG result type {other:?}"),
    }
}

/// Casts a floating average back to the AVG input type (FLOAT narrows from the DOUBLE running sum).
pub(crate) fn avg_float_scalar(value: f64, result: &DataType) -> ScalarValue {
    match result {
        DataType::Float64 => ScalarValue::Float64(Some(value)),
        DataType::Float32 => ScalarValue::Float32(Some(value as f32)),
        other => panic!("unsupported float AVG result type {other:?}"),
    }
}
