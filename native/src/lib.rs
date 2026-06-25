use arrow::array::{
    make_array, new_empty_array, Array, ArrayRef, BinaryArray, BooleanArray, Float32Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    UInt32Array,
};
use arrow::compute::{concat_batches, filter_record_batch, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::catalog::memory::MemorySourceConfig;
use datafusion::common::{DFSchema, JoinSide, JoinType, NullEquality};
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::{Accumulator, AggregateUDF, Operator};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::expressions::{binary, col, lit, Column};
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::{HashJoinExec, JoinOn, PartitionMode};
use datafusion::prelude::{col as logical_col, lit as logical_lit, SessionContext};
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use jni::objects::{
    JByteArray, JClass, JDoubleArray, JIntArray, JLongArray, JObjectArray, JString,
};
use jni::sys::{jboolean, jbyteArray, jint, jlong, jstring};
use jni::JNIEnv;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

/// Takes ownership of a batch the JVM exported through the C Data Interface, swapping in released
/// placeholders so the producer's release callbacks fire once when the imported data drops.
fn import_record_batch(array_address: jlong, schema_address: jlong) -> RecordBatch {
    let ffi_array = unsafe {
        std::ptr::replace(array_address as *mut FFI_ArrowArray, FFI_ArrowArray::empty())
    };
    let ffi_schema = unsafe {
        std::ptr::replace(schema_address as *mut FFI_ArrowSchema, FFI_ArrowSchema::empty())
    };
    let mut data = unsafe { from_ffi(ffi_array, &ffi_schema) }.expect("failed to import Arrow batch");
    data.align_buffers();
    RecordBatch::from(StructArray::from(data))
}

/// Exports a batch into consumer-allocated C structs; the JVM owns and releases it after import.
fn export_record_batch(batch: RecordBatch, array_address: jlong, schema_address: jlong) {
    let out_data = StructArray::from(batch).to_data();
    let out_array = FFI_ArrowArray::new(&out_data);
    let out_schema =
        FFI_ArrowSchema::try_from(out_data.data_type()).expect("failed to export Arrow schema");
    unsafe {
        std::ptr::write(array_address as *mut FFI_ArrowArray, out_array);
        std::ptr::write(schema_address as *mut FFI_ArrowSchema, out_schema);
    }
}

/// Builds a built-in aggregate over an int64 `value` column. SUM/MIN/MAX/COUNT all reduce an int64
/// column to a single int64 with single-scalar partial state, driven through DataFusion's
/// accumulator machinery.
/// Maps a value-type code (matching the JVM side) to the Arrow type of the value column.
fn value_data_type(code: i64) -> DataType {
    match code {
        0 => DataType::Int64,
        1 => DataType::Float64,
        2 => DataType::Int32,
        // 3 is reserved for string keys (never a value type); 4/5/6 are the narrow value types.
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

fn build_builtin(kind: i64, value_type: &DataType) -> AggregateFunctionExpr {
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
struct IntegerAvgAccumulator {
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
struct WrappingIntSumAccumulator {
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

/// Sum of a narrow integer column (smallint/tinyint) matching the host's semantics: the host keeps
/// the input type and casts back each accumulate step, so the running sum wraps at the narrow width.
/// We fold at int64 but wrap to the width on every step, and emit the narrow type. Null when no
/// non-null value was seen. `data_type` is Int16 or Int8.
#[derive(Debug)]
struct WrappingNarrowSumAccumulator {
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
struct FloatSumAccumulator {
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
struct FloatAvgAccumulator {
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
struct DoubleAvgAccumulator {
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
enum WindowAggregate {
    Builtin(AggregateFunctionExpr),
    IntegerAvg(DataType),
    WrappingIntSum,
    WrappingNarrowSum(DataType),
    FloatSum,
    FloatAvg,
    DoubleAvg,
}

impl WindowAggregate {
    fn new(kind: i64, value_type: &DataType) -> Self {
        match kind {
            // SUM over a narrow int keeps the host's narrow, wrapping semantics rather than widening.
            0 if *value_type == DataType::Int32 => WindowAggregate::WrappingIntSum,
            0 if *value_type == DataType::Int16 || *value_type == DataType::Int8 => {
                WindowAggregate::WrappingNarrowSum(value_type.clone())
            }
            // SUM over float keeps the host's 4-byte precision rather than widening to double.
            0 if *value_type == DataType::Float32 => WindowAggregate::FloatSum,
            0..=3 => WindowAggregate::Builtin(build_builtin(kind, value_type)),
            // Float AVG sums in double and narrows to float; double AVG stays double; integer AVG
            // truncates to its type.
            4 if *value_type == DataType::Float32 => WindowAggregate::FloatAvg,
            4 if *value_type == DataType::Float64 => WindowAggregate::DoubleAvg,
            4 => WindowAggregate::IntegerAvg(value_type.clone()),
            other => panic!("unsupported aggregate kind: {other}"),
        }
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
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
        }
    }

    fn state_fields(&self) -> Vec<Field> {
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
        }
    }

    /// The aggregate's output type (e.g. int64 for a sum of int64, float64 for a sum of float64).
    fn result_type(&self) -> DataType {
        match self {
            WindowAggregate::Builtin(aggregate) => aggregate.field().data_type().clone(),
            WindowAggregate::IntegerAvg(result_type) => result_type.clone(),
            WindowAggregate::WrappingIntSum => DataType::Int32,
            WindowAggregate::WrappingNarrowSum(data_type) => data_type.clone(),
            WindowAggregate::FloatSum | WindowAggregate::FloatAvg => DataType::Float32,
            WindowAggregate::DoubleAvg => DataType::Float64,
        }
    }
}

/// Builds one aggregate per (kind, value-type code) pair, positionally. Per-aggregate value types
/// let a single window aggregate compute over different value columns (e.g. `SUM(a), SUM(b)`).
fn build_aggregates(kinds: &[i64], value_types: &[i64]) -> Vec<WindowAggregate> {
    kinds
        .iter()
        .zip(value_types)
        .map(|(&kind, &code)| WindowAggregate::new(kind, &value_data_type(code)))
        .collect()
}

/// Builds an array from per-row scalars, using the given type for the empty case (where the
/// element type cannot be inferred from the values).
fn scalars_to_array(scalars: Vec<ScalarValue>, data_type: &DataType) -> ArrayRef {
    if scalars.is_empty() {
        new_empty_array(data_type)
    } else {
        ScalarValue::iter_to_array(scalars).expect("failed to build array")
    }
}

/// A typed NULL scalar of the given type (the value an aggregate reports when it has no live input).
fn null_scalar(data_type: &DataType) -> ScalarValue {
    ScalarValue::try_from(data_type).expect("null scalar of type")
}

/// One open aligned window: its start, plus the per-key accumulators folding in matching rows. The
/// owning map keys windows by their *end*, which is unique even for cumulative windows that share a
/// start, so the start is carried here.
struct AlignedWindow {
    start: i64,
    keys: HashMap<GroupKey, Vec<Box<dyn Accumulator>>>,
}

/// Event-time aligned-window aggregation that holds open windows across batches: tumbling and
/// hopping (fixed-size windows at a slide interval) and cumulative (nested windows sharing a start,
/// growing by a step up to a max size). Mirrors the upstream streaming engine's window operator:
/// windows live in an ordered map keyed by end, each an incremental accumulator that folds in
/// matching rows, and a window is finalized and dropped once a watermark passes its end.
struct TumblingAggregator {
    window_millis: i64,
    slide_millis: i64,
    cumulative: bool,
    aggregates: Vec<WindowAggregate>,
    windows: BTreeMap<i64, AlignedWindow>,
    key_types: Vec<DataType>,
    // The highest watermark flushed so far; a row whose window ends at or before it is late (its
    // window already closed) and is dropped, matching the host's per-row late-data handling.
    current_watermark: i64,
}

impl TumblingAggregator {
    fn new(
        window_millis: i64,
        slide_millis: i64,
        cumulative: bool,
        value_types: Vec<i64>,
        kinds: Vec<i64>,
    ) -> Self {
        TumblingAggregator {
            window_millis,
            slide_millis,
            cumulative,
            aggregates: build_aggregates(&kinds, &value_types),
            windows: BTreeMap::new(),
            key_types: Vec::new(),
            current_watermark: i64::MIN,
        }
    }

    /// Every window a timestamp belongs to, as (start, end) pairs, appended to `windows` (cleared
    /// first) so the caller can reuse one buffer across rows. Tumbling yields one window; hopping
    /// yields the `size / slide` overlapping windows; cumulative yields the nested windows
    /// `[base, base + k*step)` whose end is past the timestamp, all sharing the bucket start.
    fn windows_for(&self, timestamp: i64, windows: &mut Vec<(i64, i64)>) {
        windows.clear();
        if self.cumulative {
            let base = timestamp - timestamp.rem_euclid(self.window_millis);
            let mut end = base + self.slide_millis;
            while end <= base + self.window_millis {
                if end > timestamp {
                    windows.push((base, end));
                }
                end += self.slide_millis;
            }
        } else {
            let mut start = timestamp - timestamp.rem_euclid(self.slide_millis);
            while start + self.window_millis > timestamp {
                windows.push((start, start + self.window_millis));
                start -= self.slide_millis;
            }
        }
    }

    /// The N accumulators (one per aggregate) for a (window, key), created on first touch. Windows
    /// are keyed by end; the start is stored on first creation. The group key is a composite of the
    /// (zero or more) grouping columns.
    fn accumulators(&mut self, start: i64, end: i64, key: GroupKey) -> &mut Vec<Box<dyn Accumulator>> {
        let aggregates = &self.aggregates;
        self.windows
            .entry(end)
            .or_insert_with(|| AlignedWindow { start, keys: HashMap::new() })
            .keys
            .entry(key)
            .or_insert_with(|| aggregates.iter().map(WindowAggregate::create_accumulator).collect())
    }

    /// Window ends at or before the watermark, in ascending order (the map is keyed by end).
    fn closed_windows(&self, watermark: i64) -> Vec<i64> {
        self.windows.keys().copied().take_while(|end| *end <= watermark).collect()
    }

    fn update(&mut self, batch: &RecordBatch) {
        let ts = column_i64(batch, "ts");
        // One value column per aggregate (value0, value1, …), so aggregates can read different
        // columns. Sliced by type-agnostic take, so each accumulator sees its column's own type.
        let values: Vec<&ArrayRef> = (0..self.aggregates.len())
            .map(|i| batch.column_by_name(&format!("value{i}")).expect("missing value column"))
            .collect();
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);

        // Group the row positions for each (window, key); the value columns are sliced by type-
        // agnostic take, so the accumulators see each column's own type (int, double, ...).
        let mut grouped: ahash::HashMap<(i64, i64, GroupKey), Vec<u32>> = ahash::HashMap::default();
        let mut windows = Vec::new();
        for row in 0..batch.num_rows() {
            let mut key = read_key(&key_arrays, row);
            self.windows_for(ts.value(row), &mut windows);
            // Drop windows already closed by the watermark — the row is late for them. The host's
            // per-row assigner drops such rows; the columnar assigner slices batches so a closing
            // watermark precedes any row it makes late, and this is where that row is discarded.
            windows.retain(|(_, end)| *end > self.current_watermark);
            // Move the row's key into its last window rather than cloning for every one; tumbling has
            // a single window, so this drops the per-row key clone entirely in the common case.
            let last = windows.len().saturating_sub(1);
            for index in 0..windows.len() {
                let (start, end) = windows[index];
                let owned = if index == last { std::mem::take(&mut key) } else { key.clone() };
                grouped.entry((start, end, owned)).or_default().push(row as u32);
            }
        }
        for ((start, end, key), rows) in grouped {
            let indices = UInt32Array::from(rows);
            let columns: Vec<ArrayRef> =
                values.iter().map(|v| take(v, &indices, None).expect("failed to take values")).collect();
            for (i, accumulator) in self.accumulators(start, end, key).iter_mut().enumerate() {
                accumulator.update_batch(std::slice::from_ref(&columns[i])).expect("failed to update");
            }
        }
    }

    /// Finalizes and removes closed windows, emitting
    /// `[key, window_start, window_end, result0..resultN-1]`. The end is carried explicitly since
    /// cumulative windows sharing a start differ by it; each result column takes the aggregate's
    /// own output type.
    fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.current_watermark = self.current_watermark.max(watermark);
        let n = self.aggregates.len();
        let mut keys: Vec<GroupKey> = Vec::new();
        let mut starts = Vec::new();
        let mut ends = Vec::new();
        let mut results: Vec<Vec<ScalarValue>> = vec![Vec::new(); n];
        for end in self.closed_windows(watermark) {
            let window = self.windows.remove(&end).expect("window present");
            let start = window.start;
            let mut group: Vec<(GroupKey, Vec<Box<dyn Accumulator>>)> =
                window.keys.into_iter().collect();
            group.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            for (key, mut accumulators) in group {
                keys.push(key);
                starts.push(start);
                ends.push(end);
                for (i, accumulator) in accumulators.iter_mut().enumerate() {
                    results[i].push(accumulator.evaluate().expect("failed to finalize"));
                }
            }
        }

        let mut fields = key_fields(&self.key_types);
        let mut columns = key_columns(&keys, &self.key_types);
        fields.push(Field::new("window_start", DataType::Int64, false));
        fields.push(Field::new("window_end", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(starts)));
        columns.push(Arc::new(Int64Array::from(ends)));
        for (i, scalars) in results.into_iter().enumerate() {
            fields.push(Field::new(format!("result{i}"), self.aggregates[i].result_type(), false));
            columns.push(scalars_to_array(scalars, &self.aggregates[i].result_type()));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build result batch")
    }

    /// Local half of two-phase aggregation: emits each closed window's per-aggregate partial state
    /// as `[key, partial0..partialN-1, slice_end]`. Single-field partials (sum/min/max/count).
    fn flush_partial(&mut self, watermark: i64) -> RecordBatch {
        self.current_watermark = self.current_watermark.max(watermark);
        let n = self.aggregates.len();
        let mut keys: Vec<GroupKey> = Vec::new();
        let mut slice_ends = Vec::new();
        let mut partials: Vec<Vec<ScalarValue>> = vec![Vec::new(); n];
        for end in self.closed_windows(watermark) {
            let window = self.windows.remove(&end).expect("window present");
            let mut group: Vec<(GroupKey, Vec<Box<dyn Accumulator>>)> =
                window.keys.into_iter().collect();
            group.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            for (key, mut accumulators) in group {
                keys.push(key);
                slice_ends.push(end);
                for (i, accumulator) in accumulators.iter_mut().enumerate() {
                    let state = accumulator.state().expect("state");
                    partials[i].push(state.into_iter().next().expect("single-field partial"));
                }
            }
        }

        let mut fields = key_fields(&self.key_types);
        let mut columns = key_columns(&keys, &self.key_types);
        for (i, scalars) in partials.into_iter().enumerate() {
            let partial_type = self.aggregates[i].state_fields()[0].data_type().clone();
            fields.push(Field::new(format!("partial{i}"), partial_type.clone(), false));
            columns.push(scalars_to_array(scalars, &partial_type));
        }
        fields.push(Field::new("slice_end", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(slice_ends)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build partial batch")
    }

    /// Global half of two-phase aggregation: merges incoming partials
    /// `[key, partial0..partialN-1, slice_end]` into the window each slice belongs to.
    fn update_partial(&mut self, batch: &RecordBatch) {
        let n = self.aggregates.len();
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let slice_ends = column_i64(batch, "slice_end");
        // Partials are read as whole columns and merged a row-slice at a time, so any partial type
        // (int64 sum/count, float64 sum, …) flows through without per-type handling here.
        let partials: Vec<&ArrayRef> =
            (0..n).map(|i| batch.column_by_name(&format!("partial{i}")).expect("partial")).collect();

        for row in 0..batch.num_rows() {
            let slice_end = slice_ends.value(row);
            let key = read_key(&key_arrays, row);
            for (start, end) in self.partial_windows(slice_end) {
                for (i, accumulator) in
                    self.accumulators(start, end, key.clone()).iter_mut().enumerate()
                {
                    accumulator
                        .merge_batch(&[partials[i].slice(row, 1)])
                        .expect("failed to merge partial");
                }
            }
        }
    }

    /// The `(start, end)` windows a slice ending at `slice_end` belongs to in the global merge.
    /// Tumbling/hopping: the `window_millis / slide_millis` fixed-size windows sharing this slice
    /// (one for tumbling, several for an overlapping hopping window). Cumulative: the nested windows
    /// of its bucket from this slice's end up to the bucket's max-size end, all sharing the bucket
    /// start — the slice (rows in `(slice_end - step, slice_end]`) contributes to every cumulative
    /// window that ends at or after it.
    fn partial_windows(&self, slice_end: i64) -> Vec<(i64, i64)> {
        if self.cumulative {
            let base = (slice_end - 1).div_euclid(self.window_millis) * self.window_millis;
            let mut windows = Vec::new();
            let mut end = slice_end;
            while end <= base + self.window_millis {
                windows.push((base, end));
                end += self.slide_millis;
            }
            windows
        } else {
            let num_windows = self.window_millis / self.slide_millis;
            (0..num_windows)
                .map(|j| {
                    let end = slice_end + j * self.slide_millis;
                    (end - self.window_millis, end)
                })
                .collect()
        }
    }

    /// Serializes every open window's accumulator state as an Arrow batch (one row per (window,
    /// key): window end, window start, key, then every accumulator's state fields in order), encoded
    /// with Arrow IPC. Carries arbitrary multi-aggregate, multi-field state through one path.
    fn snapshot(&mut self) -> Vec<u8> {
        let state_fields: Vec<Field> =
            self.aggregates.iter().flat_map(WindowAggregate::state_fields).collect();

        let mut ends: Vec<i64> = Vec::new();
        let mut starts: Vec<i64> = Vec::new();
        let mut keys: Vec<GroupKey> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); state_fields.len()];
        for (end, window) in self.windows.iter_mut() {
            for (key, accumulators) in window.keys.iter_mut() {
                ends.push(*end);
                starts.push(window.start);
                keys.push(key.clone());
                let mut column = 0;
                for accumulator in accumulators.iter_mut() {
                    for scalar in accumulator.state().expect("state") {
                        state_columns[column].push(scalar);
                        column += 1;
                    }
                }
            }
        }

        let mut fields = vec![
            Field::new("window_end", DataType::Int64, false),
            Field::new("window_start", DataType::Int64, false),
        ];
        let mut columns: Vec<ArrayRef> =
            vec![Arc::new(Int64Array::from(ends)), Arc::new(Int64Array::from(starts))];
        fields.extend(key_fields(&self.key_types));
        columns.extend(key_columns(&keys, &self.key_types));
        fields.extend(state_fields.iter().cloned());
        for (index, scalars) in state_columns.into_iter().enumerate() {
            columns.push(if scalars.is_empty() {
                new_empty_array(state_fields[index].data_type())
            } else {
                ScalarValue::iter_to_array(scalars).expect("state array")
            });
        }

        // Carry the watermark in schema metadata so late-data dropping survives a restore.
        let metadata = std::collections::HashMap::from([(
            "current_watermark".to_string(),
            self.current_watermark.to_string(),
        )]);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields).with_metadata(metadata)), columns)
            .expect("failed to build snapshot batch");

        let mut buffer = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &batch.schema())
            .expect("failed to open snapshot writer");
        writer.write(&batch).expect("failed to write snapshot");
        writer.finish().expect("failed to finish snapshot");
        drop(writer);
        buffer
    }

    fn restore(
        window_millis: i64,
        slide_millis: i64,
        cumulative: bool,
        value_types: Vec<i64>,
        kinds: Vec<i64>,
        bytes: &[u8],
    ) -> Self {
        let mut aggregator =
            TumblingAggregator::new(window_millis, slide_millis, cumulative, value_types, kinds);
        let field_counts: Vec<usize> =
            aggregator.aggregates.iter().map(|a| a.state_fields().len()).collect();
        let state_field_total: usize = field_counts.iter().sum();
        let reader = arrow::ipc::reader::StreamReader::try_new(bytes, None)
            .expect("failed to open snapshot reader");
        for batch in reader {
            let batch = batch.expect("failed to read snapshot");
            if let Some(watermark) = batch.schema().metadata().get("current_watermark") {
                aggregator.current_watermark = watermark.parse().expect("watermark");
            }
            // Columns are [window_end, window_start, key0..key{arity-1}, state fields...].
            let arity = batch.num_columns() - 2 - state_field_total;
            let ends =
                batch.column(0).as_any().downcast_ref::<Int64Array>().expect("window_end int64");
            let starts =
                batch.column(1).as_any().downcast_ref::<Int64Array>().expect("window_start int64");
            let key_arrays: Vec<&ArrayRef> = (0..arity).map(|j| batch.column(2 + j)).collect();
            aggregator.key_types = key_types(&key_arrays);
            for row in 0..batch.num_rows() {
                let key = read_key(&key_arrays, row);
                let mut column = 2 + arity;
                for (i, accumulator) in aggregator
                    .accumulators(starts.value(row), ends.value(row), key)
                    .iter_mut()
                    .enumerate()
                {
                    let count = field_counts[i];
                    let state: Vec<ArrayRef> =
                        (column..column + count).map(|c| batch.column(c).slice(row, 1)).collect();
                    accumulator.merge_batch(&state).expect("failed to restore window");
                    column += count;
                }
            }
        }
        aggregator
    }
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
enum Num {
    I64(i64),
    I32(i32),
    F64(f64),
}

/// The OVER value column downcast once, so the per-row fold reads a typed value without a per-row
/// downcast. `None` for a null row (which the aggregates skip).
enum ValueColumn<'a> {
    I64(&'a Int64Array),
    I32(&'a Int32Array),
    F64(&'a arrow::array::Float64Array),
}

impl ValueColumn<'_> {
    fn at(&self, row: usize) -> Option<Num> {
        match self {
            ValueColumn::I64(a) => (!a.is_null(row)).then(|| Num::I64(a.value(row))),
            ValueColumn::I32(a) => (!a.is_null(row)).then(|| Num::I32(a.value(row))),
            ValueColumn::F64(a) => (!a.is_null(row)).then(|| Num::F64(a.value(row))),
        }
    }
}

/// Per-key running state for one OVER aggregate. Folded directly per row — no per-row DataFusion
/// accumulator call — matching DataFusion's accumulators exactly: integer SUM wraps on overflow (as
/// `sum_udaf` and the int-sum accumulator do), and all four skip null values. The value type is
/// fixed per aggregator, so each variant pairs a kind with that type. See divergences/11.
enum RunningAgg {
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
}

impl RunningAgg {
    fn new(kind: i64, value_type: &DataType) -> Self {
        use RunningAgg::*;
        if kind == 3 {
            return Count(0);
        }
        match value_type {
            DataType::Int64 => [SumI64(None), MinI64(None), MaxI64(None)],
            DataType::Int32 => [SumI32(None), MinI32(None), MaxI32(None)],
            DataType::Float64 => [SumF64(None), MinF64(None), MaxF64(None)],
            other => panic!("unsupported OVER value type: {other:?}"),
        }
        .into_iter()
        .nth(kind as usize)
        .expect("kind 0=SUM, 1=MIN, 2=MAX")
    }

    /// Folds one non-null value into the running state.
    fn fold(&mut self, value: Num) {
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
            _ => unreachable!("OVER value type does not match aggregate state"),
        }
    }

    /// Reverses one value out of the running state — the changelog retraction of {@link #fold}. Only
    /// the additive aggregates support it: SUM subtracts, COUNT decrements. MIN/MAX cannot be
    /// retracted incrementally, so they are never admitted over a retracting input.
    fn retract(&mut self, value: Num) {
        use RunningAgg::*;
        match (self, value) {
            (SumI64(s), Num::I64(v)) => *s = Some(s.unwrap_or(0).wrapping_sub(v)),
            (SumI32(s), Num::I32(v)) => *s = Some(s.unwrap_or(0).wrapping_sub(v)),
            (SumF64(s), Num::F64(v)) => *s = Some(s.unwrap_or(0.0) - v),
            (Count(c), _) => *c -= 1,
            _ => unreachable!("aggregate does not support retraction"),
        }
    }

    /// The current running value (also the checkpointed state).
    fn emit(&self) -> ScalarValue {
        use RunningAgg::*;
        match self {
            SumI64(v) | MinI64(v) | MaxI64(v) => ScalarValue::Int64(*v),
            SumI32(v) | MinI32(v) | MaxI32(v) => ScalarValue::Int32(*v),
            SumF64(v) | MinF64(v) | MaxF64(v) => ScalarValue::Float64(*v),
            Count(c) => ScalarValue::Int64(Some(*c)),
        }
    }

    fn result_type(&self) -> DataType {
        use RunningAgg::*;
        match self {
            SumI64(_) | MinI64(_) | MaxI64(_) | Count(_) => DataType::Int64,
            SumI32(_) | MinI32(_) | MaxI32(_) => DataType::Int32,
            SumF64(_) | MinF64(_) | MaxF64(_) => DataType::Float64,
        }
    }

    fn restore_value(&mut self, scalar: &ScalarValue) {
        use RunningAgg::*;
        match (self, scalar) {
            (Count(c), ScalarValue::Int64(Some(v))) => *c = *v,
            (SumI64(s) | MinI64(s) | MaxI64(s), ScalarValue::Int64(v)) => *s = *v,
            (SumI32(s) | MinI32(s) | MaxI32(s), ScalarValue::Int32(v)) => *s = *v,
            (SumF64(s) | MinF64(s) | MaxF64(s), ScalarValue::Float64(v)) => *s = *v,
            _ => panic!("OVER state type mismatch on restore"),
        }
    }
}

struct OverAggregator {
    kinds: Vec<i64>,
    value_type: DataType,
    keys: HashMap<GroupKey, Vec<RunningAgg>>,
    key_types: Vec<DataType>,
}

impl OverAggregator {
    fn new(value_type: i64, kinds: Vec<i64>) -> Self {
        OverAggregator {
            kinds,
            value_type: value_data_type(value_type),
            keys: HashMap::new(),
            key_types: Vec::new(),
        }
    }

    /// The running aggregate state for a key, created on first touch.
    fn states(&mut self, key: GroupKey) -> &mut Vec<RunningAgg> {
        let (kinds, value_type) = (&self.kinds, &self.value_type);
        self.keys
            .entry(key)
            .or_insert_with(|| kinds.iter().map(|&kind| RunningAgg::new(kind, value_type)).collect())
    }

    /// Folds the batch (`rt` i64, `value`, optional `key0..`) into the per-key running state in
    /// rowtime order and returns `[result0..resultN-1]` per input row, in input order.
    fn update(&mut self, batch: &RecordBatch) -> RecordBatch {
        let rt = column_i64(batch, "rt");
        let value = batch.column_by_name("value").expect("missing value column");
        let value_column = match self.value_type {
            DataType::Int64 => ValueColumn::I64(value.as_any().downcast_ref().expect("int64 value")),
            DataType::Int32 => ValueColumn::I32(value.as_any().downcast_ref().expect("int32 value")),
            DataType::Float64 => {
                ValueColumn::F64(value.as_any().downcast_ref().expect("float64 value"))
            }
            ref other => panic!("unsupported OVER value type: {other:?}"),
        };
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let n = batch.num_rows();
        let num_agg = self.kinds.len();
        let row_keys: Vec<GroupKey> = (0..n).map(|row| read_key(&key_arrays, row)).collect();

        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&row| rt.value(row));
        let mut results: Vec<Vec<ScalarValue>> = vec![vec![ScalarValue::Null; n]; num_agg];
        let mut start = 0;
        while start < n {
            let mut end = start;
            while end < n && rt.value(order[end]) == rt.value(order[start]) {
                end += 1;
            }
            // Fold every row of this rt group into its key before reading any (RANGE: tied rows of a
            // key share the post-fold value); a null value is skipped, but the key's state is touched
            // so the row still emits the running value.
            for &row in &order[start..end] {
                let value = value_column.at(row);
                let states = self.states(row_keys[row].clone());
                if let Some(num) = value {
                    for state in states.iter_mut() {
                        state.fold(num);
                    }
                }
            }
            for &row in &order[start..end] {
                let states = self.keys.get(&row_keys[row]).expect("key present");
                for (a, state) in states.iter().enumerate() {
                    results[a][row] = state.emit();
                }
            }
            start = end;
        }

        let mut fields = Vec::with_capacity(num_agg);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_agg);
        for (a, &kind) in self.kinds.iter().enumerate() {
            let result_type = RunningAgg::new(kind, &self.value_type).result_type();
            fields.push(Field::new(format!("result{a}"), result_type.clone(), true));
            columns.push(scalars_to_array(std::mem::take(&mut results[a]), &result_type));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build over result batch")
    }

    /// Serializes the per-key running state (`[key0.., state0..]`, one row per key, one scalar per
    /// aggregate — the running value is itself the checkpointed state).
    fn snapshot(&mut self) -> Vec<u8> {
        let result_types: Vec<DataType> =
            self.kinds.iter().map(|&k| RunningAgg::new(k, &self.value_type).result_type()).collect();
        let mut keys: Vec<GroupKey> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); self.kinds.len()];
        for (key, states) in self.keys.iter() {
            keys.push(key.clone());
            for (i, state) in states.iter().enumerate() {
                state_columns[i].push(state.emit());
            }
        }
        let mut fields = key_fields(&self.key_types);
        let mut columns = key_columns(&keys, &self.key_types);
        for (index, result_type) in result_types.iter().enumerate() {
            fields.push(Field::new(format!("state{index}"), result_type.clone(), true));
        }
        for (index, scalars) in state_columns.into_iter().enumerate() {
            columns.push(scalars_to_array(scalars, &result_types[index]));
        }
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build over snapshot batch"))
    }

    fn restore(value_type: i64, kinds: Vec<i64>, bytes: &[u8]) -> Self {
        let mut aggregator = OverAggregator::new(value_type, kinds);
        let num_agg = aggregator.kinds.len();
        for batch in read_ipc(bytes) {
            let arity = batch.num_columns() - num_agg;
            let key_arrays: Vec<&ArrayRef> = (0..arity).map(|j| batch.column(j)).collect();
            aggregator.key_types = key_types(&key_arrays);
            for row in 0..batch.num_rows() {
                let key = read_key(&key_arrays, row);
                for (i, state) in aggregator.states(key).iter_mut().enumerate() {
                    let scalar = ScalarValue::try_from_array(batch.column(arity + i), row)
                        .expect("over state scalar");
                    state.restore_value(&scalar);
                }
            }
        }
        aggregator
    }
}

/// The hidden column carrying a changelog row's `RowKind` as a byte across the row/Arrow boundary
/// (must match the JVM converter's column name). See divergences/13.
const ROW_KIND_COLUMN: &str = "$row_kind$";

/// The `$row_kind$` byte column if the batch carries one. A columnar batch from an insert-only
/// producer (a native source/exchange) has none — every row is then an INSERT.
fn row_kind_column(batch: &RecordBatch) -> Option<&Int8Array> {
    batch.column_by_name(ROW_KIND_COLUMN).map(|column| {
        column.as_any().downcast_ref::<Int8Array>().expect("row kind must be int8")
    })
}

/// The number of data columns: every column except a trailing `$row_kind$`, if present.
fn data_arity(batch: &RecordBatch) -> usize {
    batch.num_columns() - if row_kind_column(batch).is_some() { 1 } else { 0 }
}

/// Total ordering over f64 so a MIN/MAX value multiset can be a `BTreeMap` (floats compared by
/// `total_cmp`); a given aggregate's column has no NaN in practice, so the tie-break is moot.
#[derive(Clone, Copy, PartialEq)]
struct OrdF64(f64);
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
enum MinMaxKey {
    I64(i64),
    I32(i32),
    F64(OrdF64),
}

impl MinMaxKey {
    fn of(num: Num) -> Self {
        match num {
            Num::I64(v) => MinMaxKey::I64(v),
            Num::I32(v) => MinMaxKey::I32(v),
            Num::F64(v) => MinMaxKey::F64(OrdF64(v)),
        }
    }

    fn from_scalar(scalar: &ScalarValue) -> Self {
        match scalar {
            ScalarValue::Int64(Some(v)) => MinMaxKey::I64(*v),
            ScalarValue::Int32(Some(v)) => MinMaxKey::I32(*v),
            ScalarValue::Float64(Some(v)) => MinMaxKey::F64(OrdF64(*v)),
            other => panic!("unexpected MIN/MAX value scalar: {other:?}"),
        }
    }

    fn scalar(&self) -> ScalarValue {
        match self {
            MinMaxKey::I64(v) => ScalarValue::Int64(Some(*v)),
            MinMaxKey::I32(v) => ScalarValue::Int32(Some(*v)),
            MinMaxKey::F64(v) => ScalarValue::Float64(Some(v.0)),
        }
    }
}

/// Per-(key, aggregate) state. SUM and COUNT fold/retract a single running value (SUM also keeps a
/// non-null count so it reports NULL once fully retracted). MIN/MAX cannot be retracted from a single
/// value, so they keep a value→count multiset and read the extreme off its ends — what makes them
/// retractable (Flink's `*WithRetractAccumulator` uses a `MapView`; Arroyo calls this the batch state).
enum GroupAggState {
    Running { agg: RunningAgg, non_null: i64 },
    Extremes { is_min: bool, counts: BTreeMap<MinMaxKey, i64> },
}

impl GroupAggState {
    fn new(kind: i64, value_type: &DataType) -> Self {
        match kind {
            1 => GroupAggState::Extremes { is_min: true, counts: BTreeMap::new() }, // MIN
            2 => GroupAggState::Extremes { is_min: false, counts: BTreeMap::new() }, // MAX
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
        }
    }

    /// The current output value; SUM and MIN/MAX report NULL when they hold no live non-null input.
    fn emit(&self, result_type: &DataType) -> ScalarValue {
        match self {
            GroupAggState::Running { agg, non_null } => match agg {
                RunningAgg::Count(_) => agg.emit(),
                _ if *non_null == 0 => null_scalar(result_type),
                _ => agg.emit(),
            },
            GroupAggState::Extremes { is_min, counts } => {
                let extreme = if *is_min { counts.keys().next() } else { counts.keys().next_back() };
                extreme.map_or_else(|| null_scalar(result_type), MinMaxKey::scalar)
            }
        }
    }
}

/// Per-key state for a `GROUP BY` group: the per-aggregate state and the live record count (reaching
/// zero deletes the group).
struct GroupKeyState {
    aggs: Vec<GroupAggState>,
    records: i64,
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
struct GroupAggregator {
    kinds: Vec<i64>,
    value_types: Vec<DataType>,
    result_types: Vec<DataType>,
    value_columns: Vec<i64>,
    key_columns: Vec<usize>,
    generate_update_before: bool,
    keys: HashMap<GroupKey, GroupKeyState>,
    key_types: Vec<DataType>,
}

impl GroupAggregator {
    fn new(
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
        GroupAggregator {
            kinds,
            value_types,
            result_types,
            value_columns,
            key_columns,
            generate_update_before,
            keys: HashMap::new(),
            key_types: Vec::new(),
        }
    }

    /// The per-key state, created (empty) on first touch.
    fn state(&mut self, key: GroupKey) -> &mut GroupKeyState {
        let (kinds, value_types) = (&self.kinds, &self.value_types);
        self.keys.entry(key).or_insert_with(|| GroupKeyState {
            aggs: kinds.iter().zip(value_types).map(|(&kind, vt)| GroupAggState::new(kind, vt)).collect(),
            records: 0,
        })
    }

    /// A key's current output tuple (each aggregate reports NULL while it has no live input).
    fn output_values(&self, key: &GroupKey) -> Vec<ScalarValue> {
        let state = self.keys.get(key).expect("key present");
        state.aggs.iter().zip(&self.result_types).map(|(agg, rt)| agg.emit(rt)).collect()
    }

    /// Folds the batch's rows into per-key state in input order, honoring each row's `RowKind`, and
    /// returns the changelog rows produced, in emission order.
    fn update(&mut self, batch: &RecordBatch) -> RecordBatch {
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
                Some(match self.value_types[i] {
                    DataType::Int64 => {
                        ValueColumn::I64(column.as_any().downcast_ref().expect("int64 value"))
                    }
                    DataType::Int32 => {
                        ValueColumn::I32(column.as_any().downcast_ref().expect("int32 value"))
                    }
                    DataType::Float64 => {
                        ValueColumn::F64(column.as_any().downcast_ref().expect("float64 value"))
                    }
                    ref other => panic!("unsupported group-by value type: {other:?}"),
                })
            })
            .collect();
        let key_arrays: Vec<&ArrayRef> = self.key_columns.iter().map(|&i| batch.column(i)).collect();
        self.key_types = key_types(&key_arrays);
        let row_kinds = row_kind_column(batch);

        let mut out_keys: Vec<GroupKey> = Vec::new();
        let mut out_results: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_agg];
        let mut out_kinds: Vec<i8> = Vec::new();
        let mut push = |kind: i8, key: &GroupKey, values: &[ScalarValue]| {
            out_keys.push(key.clone());
            for (i, v) in values.iter().enumerate() {
                out_results[i].push(v.clone());
            }
            out_kinds.push(kind);
        };

        for row in 0..n {
            let key = read_key(&key_arrays, row);
            // RowKind: 0 +I, 1 -U, 2 +U, 3 -D (absent column ⇒ INSERT). UB/delete retract; I/UA add.
            let kind = row_kinds.map_or(0, |kinds| kinds.value(row));
            let retract = kind == 1 || kind == 3;
            let exists = self.keys.contains_key(&key);
            if !exists && retract {
                continue; // no accumulator for a key's first message being a retraction (host skips it)
            }
            let prev = if exists { Some(self.output_values(&key)) } else { None };
            {
                // Clone the key into the map only when inserting a new group; an existing group
                // (the steady state) is reached by reference, avoiding a per-row key allocation.
                let state = if exists {
                    self.keys.get_mut(&key).expect("key present")
                } else {
                    self.state(key.clone())
                };
                for i in 0..num_agg {
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
                let new = self.output_values(&key);
                match prev {
                    None => push(0, &key, &new), // +I — first row for the key
                    Some(prev) if new != prev => {
                        if self.generate_update_before {
                            push(1, &key, &prev); // -U
                        }
                        push(2, &key, &new); // +U
                    }
                    Some(_) => {} // unchanged result — suppressed (state retention off)
                }
            } else {
                // The last record for the key was retracted: delete the group.
                push(3, &key, &prev.expect("a retraction implies the key existed")); // -D
                self.keys.remove(&key);
            }
        }
        drop(push);

        let mut fields = key_fields(&self.key_types);
        let mut columns = key_columns(&out_keys, &self.key_types);
        for (i, rt) in self.result_types.iter().enumerate() {
            fields.push(Field::new(format!("result{i}"), rt.clone(), true));
            columns.push(scalars_to_array(std::mem::take(&mut out_results[i]), rt));
        }
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build group-by changelog batch")
    }

    /// Serializes per-key state. A main batch carries `[key0.., records, state{i}, nonnull{i}…]` (the
    /// raw running value and non-null count for SUM/COUNT; a NULL placeholder for MIN/MAX), and a side
    /// batch per MIN/MAX aggregate carries its `[key0.., value, count]` multiset rows.
    fn snapshot(&mut self) -> Vec<u8> {
        let num_agg = self.kinds.len();
        let mut keys: Vec<GroupKey> = Vec::new();
        let mut records: Vec<i64> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); num_agg];
        let mut non_null_columns: Vec<Vec<i64>> = vec![Vec::new(); num_agg];
        let mut multiset_keys: Vec<Vec<GroupKey>> = vec![Vec::new(); num_agg];
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
                            multiset_values[i].push(value.scalar());
                            multiset_counts[i].push(*count);
                        }
                    }
                }
            }
        }

        let mut fields = key_fields(&self.key_types);
        let mut columns = key_columns(&keys, &self.key_types);
        fields.push(Field::new("records", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(records)));
        for i in 0..num_agg {
            fields.push(Field::new(format!("state{i}"), self.result_types[i].clone(), true));
            columns.push(scalars_to_array(std::mem::take(&mut state_columns[i]), &self.result_types[i]));
            fields.push(Field::new(format!("nonnull{i}"), DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(std::mem::take(&mut non_null_columns[i]))));
        }
        let mut batches =
            vec![RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("main snapshot")];
        for i in 0..num_agg {
            if matches!(self.kinds[i], 1 | 2) {
                let mut f = key_fields(&self.key_types);
                let mut c = key_columns(&multiset_keys[i], &self.key_types);
                f.push(Field::new("value", self.result_types[i].clone(), true));
                c.push(scalars_to_array(std::mem::take(&mut multiset_values[i]), &self.result_types[i]));
                f.push(Field::new("count", DataType::Int64, false));
                c.push(Arc::new(Int64Array::from(std::mem::take(&mut multiset_counts[i]))));
                batches.push(RecordBatch::try_new(Arc::new(Schema::new(f)), c).expect("multiset snapshot"));
            }
        }
        write_framed(&batches)
    }

    fn restore(
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
        let records = column_i64(main, "records");
        for row in 0..main.num_rows() {
            let key = read_key(&key_arrays, row);
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
        // One side batch per MIN/MAX aggregate, in aggregate order: key0.., value, count.
        let mut frame = 1;
        for i in 0..num_agg {
            if !matches!(aggregator.kinds[i], 1 | 2) {
                continue;
            }
            let side = &batches[frame];
            frame += 1;
            let side_arity = side.num_columns() - 2;
            let side_keys: Vec<&ArrayRef> = (0..side_arity).map(|j| side.column(j)).collect();
            let values = side.column(side_arity);
            let counts = column_i64(side, "count");
            for row in 0..side.num_rows() {
                let key = read_key(&side_keys, row);
                let value = ScalarValue::try_from_array(values, row).expect("multiset value");
                if let Some(GroupAggState::Extremes { counts: map, .. }) =
                    aggregator.keys.get_mut(&key).map(|s| &mut s.aggs[i])
                {
                    map.insert(MinMaxKey::from_scalar(&value), counts.value(row));
                }
            }
        }
        aggregator
    }
}

/// Window-function code: a SQL `OVER` analytic function that is *not* a mergeable aggregate.
fn is_window_function_kind(kind: i64) -> bool {
    kind >= 10
}

/// Per-key running state for one OVER window function. Unlike the aggregate path these are not
/// DataFusion accumulators (DataFusion's window evaluators expose no serializable state); we own the
/// small running state so it checkpoints, computing it incrementally in rowtime order like Flink's
/// own `OverAggregate` (see divergences/11).
enum WindowFnState {
    /// `ROW_NUMBER()` over `ROWS UNBOUNDED PRECEDING` — a per-partition counter (1-based).
    RowNumber(i64),
    /// `RANK()` — `count` rows seen, `rank` of the current order-value group, `last` order value.
    /// Tied order values share a rank; the next value's rank jumps to its row position (gaps).
    Rank { count: i64, rank: i64, last: Option<i64> },
    /// `DENSE_RANK()` — increments only when the order value changes, so ranks are gap-free.
    DenseRank { dense: i64, last: Option<i64> },
}

impl WindowFnState {
    fn new(kind: i64) -> Self {
        match kind {
            10 => WindowFnState::RowNumber(0),
            11 => WindowFnState::Rank { count: 0, rank: 0, last: None },
            12 => WindowFnState::DenseRank { dense: 0, last: None },
            other => panic!("unsupported window function kind: {other}"),
        }
    }

    /// Advances the state for the current row (whose ORDER BY value is `rt`) and returns its value.
    /// Rows are fed in ascending order, so tied order values arrive consecutively.
    fn next(&mut self, rt: i64) -> ScalarValue {
        match self {
            WindowFnState::RowNumber(n) => {
                *n += 1;
                ScalarValue::Int64(Some(*n))
            }
            WindowFnState::Rank { count, rank, last } => {
                *count += 1;
                if *last != Some(rt) {
                    *rank = *count;
                    *last = Some(rt);
                }
                ScalarValue::Int64(Some(*rank))
            }
            WindowFnState::DenseRank { dense, last } => {
                if *last != Some(rt) {
                    *dense += 1;
                    *last = Some(rt);
                }
                ScalarValue::Int64(Some(*dense))
            }
        }
    }

    fn result_type(&self) -> DataType {
        DataType::Int64
    }

    /// The checkpointable running state, as scalars (one or more per function).
    fn state(&self) -> Vec<ScalarValue> {
        let i = |v: i64| ScalarValue::Int64(Some(v));
        match self {
            WindowFnState::RowNumber(n) => vec![i(*n)],
            WindowFnState::Rank { count, rank, last } => {
                vec![i(*count), i(*rank), ScalarValue::Int64(*last)]
            }
            WindowFnState::DenseRank { dense, last } => vec![i(*dense), ScalarValue::Int64(*last)],
        }
    }

    fn state_types(&self) -> Vec<DataType> {
        match self {
            WindowFnState::RowNumber(_) => vec![DataType::Int64],
            WindowFnState::Rank { .. } => vec![DataType::Int64; 3],
            WindowFnState::DenseRank { .. } => vec![DataType::Int64; 2],
        }
    }

    fn restore_state(&mut self, state: &[ScalarValue]) {
        let int = |scalar: &ScalarValue| match scalar {
            ScalarValue::Int64(value) => *value,
            _ => None,
        };
        match self {
            WindowFnState::RowNumber(n) => *n = int(&state[0]).unwrap_or(0),
            WindowFnState::Rank { count, rank, last } => {
                *count = int(&state[0]).unwrap_or(0);
                *rank = int(&state[1]).unwrap_or(0);
                *last = int(&state[2]);
            }
            WindowFnState::DenseRank { dense, last } => {
                *dense = int(&state[0]).unwrap_or(0);
                *last = int(&state[1]);
            }
        }
    }
}

/// OVER window functions (ROW_NUMBER today; RANK/DENSE_RANK/FIRST_VALUE/LAST_VALUE to follow),
/// computed incrementally per partition key in rowtime order. The sibling of {@link OverAggregator}
/// for the non-aggregate `OVER` functions: same `[rt, key0..]` sub-batch in, one result column per
/// function out, but driven by per-key {@link WindowFnState} rather than DataFusion accumulators.
struct WindowFunctionOver {
    kinds: Vec<i64>,
    keys: HashMap<GroupKey, Vec<WindowFnState>>,
    key_types: Vec<DataType>,
}

impl WindowFunctionOver {
    fn new(kinds: Vec<i64>) -> Self {
        WindowFunctionOver { kinds, keys: HashMap::new(), key_types: Vec::new() }
    }

    fn states(&mut self, key: GroupKey) -> &mut Vec<WindowFnState> {
        let kinds = &self.kinds;
        self.keys.entry(key).or_insert_with(|| kinds.iter().map(|&k| WindowFnState::new(k)).collect())
    }

    /// Advances each function per row in rowtime order and returns `[result0..]` in input order.
    fn update(&mut self, batch: &RecordBatch) -> RecordBatch {
        let rt = column_i64(batch, "rt");
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let n = batch.num_rows();
        let num = self.kinds.len();
        let row_keys: Vec<GroupKey> = (0..n).map(|row| read_key(&key_arrays, row)).collect();
        // Stable sort by rowtime: rows of equal rowtime keep input (arrival) order, matching Flink's
        // ROWS-frame tie order.
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&row| rt.value(row));
        let mut results: Vec<Vec<ScalarValue>> = vec![vec![ScalarValue::Null; n]; num];
        for &row in &order {
            for (i, state) in self.states(row_keys[row].clone()).iter_mut().enumerate() {
                results[i][row] = state.next(rt.value(row));
            }
        }
        let mut fields = Vec::with_capacity(num);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num);
        for (i, &kind) in self.kinds.iter().enumerate() {
            let result_type = WindowFnState::new(kind).result_type();
            fields.push(Field::new(format!("result{i}"), result_type.clone(), true));
            columns.push(scalars_to_array(std::mem::take(&mut results[i]), &result_type));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build window-function result batch")
    }

    /// Serializes the per-key running state (`[key0.., state…]`, one row per key).
    fn snapshot(&mut self) -> Vec<u8> {
        let state_types: Vec<DataType> =
            self.kinds.iter().flat_map(|&k| WindowFnState::new(k).state_types()).collect();
        let mut keys: Vec<GroupKey> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); state_types.len()];
        for (key, states) in self.keys.iter() {
            keys.push(key.clone());
            let mut column = 0;
            for state in states {
                for scalar in state.state() {
                    state_columns[column].push(scalar);
                    column += 1;
                }
            }
        }
        let mut fields = key_fields(&self.key_types);
        let mut columns = key_columns(&keys, &self.key_types);
        for (index, state_type) in state_types.iter().enumerate() {
            fields.push(Field::new(format!("state{index}"), state_type.clone(), true));
        }
        for (index, scalars) in state_columns.into_iter().enumerate() {
            columns.push(scalars_to_array(scalars, &state_types[index]));
        }
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build window-function snapshot batch"))
    }

    fn restore(kinds: Vec<i64>, bytes: &[u8]) -> Self {
        let mut over = WindowFunctionOver::new(kinds);
        let state_counts: Vec<usize> =
            over.kinds.iter().map(|&k| WindowFnState::new(k).state_types().len()).collect();
        let state_total: usize = state_counts.iter().sum();
        for batch in read_ipc(bytes) {
            let arity = batch.num_columns() - state_total;
            let key_arrays: Vec<&ArrayRef> = (0..arity).map(|j| batch.column(j)).collect();
            over.key_types = key_types(&key_arrays);
            for row in 0..batch.num_rows() {
                let key = read_key(&key_arrays, row);
                let mut column = arity;
                for (i, state) in over.states(key).iter_mut().enumerate() {
                    let count = state_counts[i];
                    let scalars: Vec<ScalarValue> = (column..column + count)
                        .map(|c| ScalarValue::try_from_array(batch.column(c), row).expect("state scalar"))
                        .collect();
                    state.restore_state(&scalars);
                    column += count;
                }
            }
        }
        over
    }
}

/// The inner per-key computation of a columnar OVER: mergeable aggregates (DataFusion accumulators)
/// or non-aggregate window functions ({@link WindowFunctionOver}). Both take a `[rt, value?, key0..]`
/// sub-batch and return one result column per output, in input row order.
enum OverInner {
    Aggregates(OverAggregator),
    WindowFunctions(WindowFunctionOver),
}

impl OverInner {
    fn new(value_type: i64, kinds: Vec<i64>) -> Self {
        if kinds.iter().all(|&k| is_window_function_kind(k)) {
            OverInner::WindowFunctions(WindowFunctionOver::new(kinds))
        } else {
            OverInner::Aggregates(OverAggregator::new(value_type, kinds))
        }
    }

    fn update(&mut self, batch: &RecordBatch) -> RecordBatch {
        match self {
            OverInner::Aggregates(inner) => inner.update(batch),
            OverInner::WindowFunctions(inner) => inner.update(batch),
        }
    }

    fn snapshot(&mut self) -> Vec<u8> {
        match self {
            OverInner::Aggregates(inner) => inner.snapshot(),
            OverInner::WindowFunctions(inner) => inner.snapshot(),
        }
    }

    fn restore(value_type: i64, kinds: Vec<i64>, bytes: &[u8]) -> Self {
        if kinds.iter().all(|&k| is_window_function_kind(k)) {
            OverInner::WindowFunctions(WindowFunctionOver::restore(kinds, bytes))
        } else {
            OverInner::Aggregates(OverAggregator::restore(value_type, kinds, bytes))
        }
    }
}

/// Columnar OVER: buffers whole input batches, and on a watermark emits the rows it has completed
/// (rowtime <= watermark) with the running aggregate / window-function column(s) appended — the input
/// columns pass straight through, so the data stays Arrow end to end. The {@link OverInner} does the
/// per-key running fold; this layer adds the buffering, the complete/pending split, the rowtime→millis
/// conversion the inner expects, and the passthrough.
struct OverWindowAggregator {
    inner: OverInner,
    rt_column: usize,
    /// The value column the inner reads, or `None` for functions with no argument (e.g. ROW_NUMBER).
    value_column: Option<usize>,
    key_columns: Vec<usize>,
    buffered: Vec<RecordBatch>,
    input_schema: Option<SchemaRef>,
}

impl OverWindowAggregator {
    fn new(
        value_type: i64,
        kinds: Vec<i64>,
        rt_column: usize,
        value_column: Option<usize>,
        key_columns: Vec<usize>,
    ) -> Self {
        OverWindowAggregator {
            inner: OverInner::new(value_type, kinds),
            rt_column,
            value_column,
            key_columns,
            buffered: Vec::new(),
            input_schema: None,
        }
    }

    fn push(&mut self, batch: RecordBatch) {
        self.input_schema = Some(batch.schema());
        self.buffered.push(batch);
    }

    /// Emits the rows the watermark has completed (input columns + running aggregates) and keeps the
    /// rest buffered. Returns an empty batch when nothing is complete.
    fn flush(&mut self, watermark: i64) -> RecordBatch {
        let schema = match &self.input_schema {
            Some(schema) => schema.clone(),
            None => return RecordBatch::new_empty(Arc::new(Schema::empty())),
        };
        let all = concat_batches(&schema, &self.buffered).expect("failed to concat over buffer");
        let rt_millis = rt_to_millis(all.column(self.rt_column));
        let complete_mask: BooleanArray = rt_millis.iter().map(|v| Some(v.unwrap() <= watermark)).collect();
        let complete = filter_record_batch(&all, &complete_mask).expect("failed to filter complete");
        let pending_mask = arrow::compute::not(&complete_mask).expect("failed to negate mask");
        let pending = filter_record_batch(&all, &pending_mask).expect("failed to filter pending");
        self.buffered = if pending.num_rows() > 0 { vec![pending] } else { Vec::new() };
        if complete.num_rows() == 0 {
            return RecordBatch::new_empty(Arc::new(Schema::empty()));
        }

        let aggregates = self.inner.update(&self.keyed_subbatch(&complete));
        let mut fields: Vec<Field> =
            complete.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = complete.columns().to_vec();
        for (i, field) in aggregates.schema().fields().iter().enumerate() {
            fields.push(field.as_ref().clone());
            columns.push(aggregates.column(i).clone());
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build over output batch")
    }

    /// The `[rt(ms i64), value, key0..]` batch the inner per-key fold reads, projected from the
    /// completed rows (rowtime converted from its timestamp unit to epoch millis).
    fn keyed_subbatch(&self, complete: &RecordBatch) -> RecordBatch {
        let mut fields = vec![Field::new("rt", DataType::Int64, false)];
        let mut columns: Vec<ArrayRef> = vec![Arc::new(rt_to_millis(complete.column(self.rt_column)))];
        if let Some(value_column) = self.value_column {
            fields.push(Field::new("value", complete.column(value_column).data_type().clone(), true));
            columns.push(complete.column(value_column).clone());
        }
        for (j, &key) in self.key_columns.iter().enumerate() {
            fields.push(Field::new(format!("key{j}"), complete.column(key).data_type().clone(), false));
            columns.push(complete.column(key).clone());
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build over sub-batch")
    }

    fn snapshot(&mut self) -> Vec<u8> {
        let accumulators = self.inner.snapshot();
        let buffer = match (&self.input_schema, self.buffered.is_empty()) {
            (Some(schema), false) => {
                let all = concat_batches(schema, &self.buffered).expect("concat over buffer");
                let mut bytes = Vec::new();
                let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &all.schema())
                    .expect("over buffer writer");
                writer.write(&all).expect("write over buffer");
                writer.finish().expect("finish over buffer");
                drop(writer);
                bytes
            }
            _ => Vec::new(),
        };
        let mut out = (accumulators.len() as u32).to_le_bytes().to_vec();
        out.extend_from_slice(&accumulators);
        out.extend_from_slice(&buffer);
        out
    }

    fn restore(
        value_type: i64,
        kinds: Vec<i64>,
        rt_column: usize,
        value_column: Option<usize>,
        key_columns: Vec<usize>,
        bytes: &[u8],
    ) -> Self {
        let accumulators_len = u32::from_le_bytes(bytes[0..4].try_into().expect("len")) as usize;
        let inner = OverInner::restore(value_type, kinds, &bytes[4..4 + accumulators_len]);
        let mut aggregator = OverWindowAggregator {
            inner,
            rt_column,
            value_column,
            key_columns,
            buffered: Vec::new(),
            input_schema: None,
        };
        let buffer = &bytes[4 + accumulators_len..];
        if !buffer.is_empty() {
            let reader =
                arrow::ipc::reader::StreamReader::try_new(buffer, None).expect("over buffer reader");
            for batch in reader {
                let batch = batch.expect("read over buffer");
                aggregator.input_schema = Some(batch.schema());
                aggregator.buffered.push(batch);
            }
        }
        aggregator
    }
}

/// Reads a timestamp column as epoch millis, regardless of its stored unit.
fn rt_to_millis(array: &ArrayRef) -> Int64Array {
    use arrow::datatypes::TimeUnit;
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let ts = array.as_any().downcast_ref::<TimestampNanosecondArray>().expect("ts ns");
            ts.iter().map(|v| v.map(|x| x / 1_000_000)).collect()
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let ts = array.as_any().downcast_ref::<TimestampMicrosecondArray>().expect("ts us");
            ts.iter().map(|v| v.map(|x| x / 1_000)).collect()
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let ts = array.as_any().downcast_ref::<TimestampMillisecondArray>().expect("ts ms");
            ts.iter().map(|v| v.map(|x| x)).collect()
        }
        DataType::Int64 => array.as_any().downcast_ref::<Int64Array>().expect("i64").clone(),
        other => panic!("unsupported rowtime column type: {other:?}"),
    }
}

/// Serializes a batch to a one-shot Arrow IPC stream (used to checkpoint buffered join state).
fn write_ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &batch.schema())
        .expect("failed to open ipc writer");
    writer.write(batch).expect("failed to write ipc batch");
    writer.finish().expect("failed to finish ipc stream");
    drop(writer);
    bytes
}

/// Reads the batches of a one-shot Arrow IPC stream back.
fn read_ipc(bytes: &[u8]) -> Vec<RecordBatch> {
    arrow::ipc::reader::StreamReader::try_new(bytes, None)
        .expect("failed to open ipc reader")
        .map(|batch| batch.expect("failed to read ipc batch"))
        .collect()
}

/// Serializes several batches of differing schemas into one buffer, each length-prefixed, so a
/// snapshot can carry side tables (e.g. a per-key multiset) alongside the main per-key state.
fn write_framed(batches: &[RecordBatch]) -> Vec<u8> {
    let mut out = Vec::new();
    for batch in batches {
        let bytes = write_ipc(batch);
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&bytes);
    }
    out
}

/// Reads back the length-prefixed batches written by {@link write_framed}, in order.
fn read_framed(bytes: &[u8]) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    let mut pos = 0;
    while pos + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        batches.extend(read_ipc(&bytes[pos..pos + len]));
        pos += len;
    }
    batches
}

/// Event-time INNER interval join, Flink's
/// `a JOIN b ON a.k = b.k AND a.rt BETWEEN b.rt + lower AND b.rt + upper`.
///
/// Buffers both inputs as batches. When a batch arrives on one side it is joined against the other
/// side's buffered rows — an INNER hash join on the equi-keys with the interval as a residual filter
/// (`lower <= left.rt - right.rt <= upper`) — so each matched pair is emitted exactly once, when the
/// second of its two rows arrives. This is the insert-then-join-the-other-side structure of Arroyo's
/// `JoinWithExpiration`, which likewise runs a DataFusion join over the batches it has buffered. A
/// row is evicted once the watermark passes the point beyond which no future row of the other side
/// could match it. Output columns are the left input columns followed by the right input columns.
struct IntervalJoiner {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    left_time: usize,
    right_time: usize,
    lower: i64,
    upper: i64,
    left_schema: Option<SchemaRef>,
    right_schema: Option<SchemaRef>,
    left_buffered: Vec<RecordBatch>,
    right_buffered: Vec<RecordBatch>,
}

impl IntervalJoiner {
    fn new(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_time: usize,
        right_time: usize,
        lower: i64,
        upper: i64,
    ) -> Self {
        IntervalJoiner {
            left_keys,
            right_keys,
            left_time,
            right_time,
            lower,
            upper,
            left_schema: None,
            right_schema: None,
            left_buffered: Vec::new(),
            right_buffered: Vec::new(),
        }
    }

    fn key_pairs(&self) -> Vec<(usize, usize)> {
        self.left_keys.iter().zip(&self.right_keys).map(|(&l, &r)| (l, r)).collect()
    }

    /// Joins an incoming left batch against the buffered right rows (equi-key + interval filter),
    /// then buffers it. Empty until the right side has rows.
    fn push_left(&mut self, batch: RecordBatch) -> RecordBatch {
        self.left_schema = Some(batch.schema());
        let result = match &self.right_schema {
            Some(right_schema) if !self.right_buffered.is_empty() => {
                let right = concat_batches(right_schema, self.right_buffered.iter())
                    .expect("concat right interval buffer");
                let filter = interval_filter(
                    &batch.schema(),
                    right_schema,
                    self.left_time,
                    self.right_time,
                    self.lower,
                    self.upper,
                );
                hash_join_inner(batch.clone(), right, &self.key_pairs(), Some(filter))
            }
            _ => RecordBatch::new_empty(Arc::new(Schema::empty())),
        };
        self.left_buffered.push(batch);
        result
    }

    /// Joins an incoming right batch against the buffered left rows, then buffers it.
    fn push_right(&mut self, batch: RecordBatch) -> RecordBatch {
        self.right_schema = Some(batch.schema());
        let result = match &self.left_schema {
            Some(left_schema) if !self.left_buffered.is_empty() => {
                let left = concat_batches(left_schema, self.left_buffered.iter())
                    .expect("concat left interval buffer");
                let filter = interval_filter(
                    left_schema,
                    &batch.schema(),
                    self.left_time,
                    self.right_time,
                    self.lower,
                    self.upper,
                );
                hash_join_inner(left, batch.clone(), &self.key_pairs(), Some(filter))
            }
            _ => RecordBatch::new_empty(Arc::new(Schema::empty())),
        };
        self.right_buffered.push(batch);
        result
    }

    /// Drops the rows the watermark has made dead. A left row can no longer match once
    /// `left.rt - lower <= watermark` (a future right row has rt > watermark, but matching it needs
    /// `right.rt <= left.rt - lower`); a right row once `right.rt + upper <= watermark`.
    fn advance(&mut self, watermark: i64) {
        let (lower, upper) = (self.lower, self.upper);
        Self::evict(&mut self.left_buffered, &self.left_schema, self.left_time, |rt| {
            rt - lower > watermark
        });
        Self::evict(&mut self.right_buffered, &self.right_schema, self.right_time, |rt| {
            rt + upper > watermark
        });
    }

    /// Keeps only the buffered rows whose rowtime (column `time`) satisfies `keep`.
    fn evict(
        buffered: &mut Vec<RecordBatch>,
        schema: &Option<SchemaRef>,
        time: usize,
        keep: impl Fn(i64) -> bool,
    ) {
        let Some(schema) = schema.as_ref() else {
            return;
        };
        if buffered.is_empty() {
            return;
        }
        let all = concat_batches(schema, buffered.iter()).expect("concat interval buffer");
        let rt = rt_to_millis(all.column(time));
        let mask: BooleanArray = rt.iter().map(|v| Some(keep(v.unwrap()))).collect();
        let kept = filter_record_batch(&all, &mask).expect("filter interval buffer");
        *buffered = if kept.num_rows() > 0 { vec![kept] } else { Vec::new() };
    }

    /// Serializes both buffers (`[u32 left_len][left ipc][right ipc]`) for a checkpoint.
    fn snapshot(&self) -> Vec<u8> {
        let serialize = |schema: &Option<SchemaRef>, buffered: &[RecordBatch]| match schema {
            Some(schema) if !buffered.is_empty() => {
                write_ipc(&concat_batches(schema, buffered.iter()).expect("concat interval buffer"))
            }
            _ => Vec::new(),
        };
        let left = serialize(&self.left_schema, &self.left_buffered);
        let right = serialize(&self.right_schema, &self.right_buffered);
        let mut out = (left.len() as u32).to_le_bytes().to_vec();
        out.extend_from_slice(&left);
        out.extend_from_slice(&right);
        out
    }

    #[allow(clippy::too_many_arguments)]
    fn restore(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_time: usize,
        right_time: usize,
        lower: i64,
        upper: i64,
        bytes: &[u8],
    ) -> Self {
        let mut joiner =
            IntervalJoiner::new(left_keys, right_keys, left_time, right_time, lower, upper);
        let left_len = u32::from_le_bytes(bytes[0..4].try_into().expect("snapshot len")) as usize;
        for batch in read_ipc_if_present(&bytes[4..4 + left_len]) {
            joiner.left_schema = Some(batch.schema());
            joiner.left_buffered.push(batch);
        }
        for batch in read_ipc_if_present(&bytes[4 + left_len..]) {
            joiner.right_schema = Some(batch.schema());
            joiner.right_buffered.push(batch);
        }
        joiner
    }
}

/// Reads IPC batches, treating empty bytes (a side that never saw a row) as no batches.
fn read_ipc_if_present(bytes: &[u8]) -> Vec<RecordBatch> {
    if bytes.is_empty() {
        Vec::new()
    } else {
        read_ipc(bytes)
    }
}

/// Renames a batch's fields to `c0..` (positional) so a joined batch with duplicate input field
/// names round-trips the C Data Interface cleanly — the downstream conversion is positional.
fn rename_positional(batch: &RecordBatch) -> RecordBatch {
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
fn hash_join_inner(
    left: RecordBatch,
    right: RecordBatch,
    key_pairs: &[(usize, usize)],
    filter: Option<JoinFilter>,
) -> RecordBatch {
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
    let batches = runtime()
        .block_on(collect(Arc::new(join), SessionContext::new().task_ctx()))
        .expect("failed to run hash join");
    if batches.iter().all(|batch| batch.num_rows() == 0) {
        return RecordBatch::new_empty(Arc::new(Schema::empty()));
    }
    let schema = batches[0].schema();
    let joined = concat_batches(&schema, &batches).expect("failed to concat join output");
    rename_positional(&joined)
}

/// Builds an interval-join residual filter for `lower <= left.rt - right.rt <= upper`, expressed as
/// `left.rt >= right.rt + lower AND left.rt <= right.rt + upper` over the two rowtime columns so it
/// works directly on the timestamp type (no subtraction to a duration). `column_indices` maps the
/// filter's intermediate schema `[left.rt, right.rt]` back to the join inputs.
fn interval_filter(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    left_rt: usize,
    right_rt: usize,
    lower: i64,
    upper: i64,
) -> JoinFilter {
    use arrow::datatypes::TimeUnit;
    let left_type = left_schema.field(left_rt).data_type().clone();
    let right_type = right_schema.field(right_rt).data_type().clone();
    // The bound is added to the rowtime in its own type and unit (arrow rejects a timestamp plus a
    // duration of a different unit): a plain millis offset for an int64 rowtime, else a Duration in
    // the timestamp's own unit (the millis offset scaled to it).
    let offset = |millis: i64| -> ScalarValue {
        match &right_type {
            DataType::Int64 => ScalarValue::Int64(Some(millis)),
            DataType::Timestamp(TimeUnit::Second, _) => ScalarValue::DurationSecond(Some(millis / 1_000)),
            DataType::Timestamp(TimeUnit::Millisecond, _) => ScalarValue::DurationMillisecond(Some(millis)),
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                ScalarValue::DurationMicrosecond(Some(millis * 1_000))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                ScalarValue::DurationNanosecond(Some(millis * 1_000_000))
            }
            other => panic!("unsupported interval-join rowtime type: {other:?}"),
        }
    };
    let intermediate = Arc::new(Schema::new(vec![
        Field::new("left_rt", left_type, true),
        Field::new("right_rt", right_type.clone(), true),
    ]));
    let left_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("left_rt", 0));
    let right_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("right_rt", 1));
    let bound = |millis: i64| -> Arc<dyn PhysicalExpr> {
        binary(right_col.clone(), Operator::Plus, lit(offset(millis)), &intermediate)
            .expect("failed to build interval bound")
    };
    let ge = binary(left_col.clone(), Operator::GtEq, bound(lower), &intermediate)
        .expect("failed to build lower bound");
    let le = binary(left_col.clone(), Operator::LtEq, bound(upper), &intermediate)
        .expect("failed to build upper bound");
    let expression = binary(ge, Operator::And, le, &intermediate).expect("failed to build interval and");
    let column_indices = vec![
        ColumnIndex { index: left_rt, side: JoinSide::Left },
        ColumnIndex { index: right_rt, side: JoinSide::Right },
    ];
    JoinFilter::new(expression, column_indices, intermediate)
}

/// Buffered rows of one side of a window join, grouped by window then by equi-join key.
/// Event-time INNER window join: the join of two windowing-TVF inputs on their equi-join key within
/// the same window — `a JOIN b ON a.k = b.k` where both sides carry matching `window_start` /
/// `window_end` columns (assigned upstream by identical `TUMBLE`/`HOP`/`CUMULATE` windows).
///
/// Input batches are buffered per side; on a watermark, the rows whose window has closed (its end at
/// or before the watermark) are joined and evicted. The window equality is folded into the equi-keys
/// — `window_start` and `window_end` join alongside the user key — so a single hash join over the
/// closed rows matches only within a window. Late rows for an already-closed window produce no
/// further output, matching Flink's watermark semantics.
struct WindowJoiner {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    left_wstart: usize,
    left_wend: usize,
    right_wstart: usize,
    right_wend: usize,
    left_schema: Option<SchemaRef>,
    right_schema: Option<SchemaRef>,
    left_buffered: Vec<RecordBatch>,
    right_buffered: Vec<RecordBatch>,
}

impl WindowJoiner {
    #[allow(clippy::too_many_arguments)]
    fn new(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_wstart: usize,
        left_wend: usize,
        right_wstart: usize,
        right_wend: usize,
    ) -> Self {
        WindowJoiner {
            left_keys,
            right_keys,
            left_wstart,
            left_wend,
            right_wstart,
            right_wend,
            left_schema: None,
            right_schema: None,
            left_buffered: Vec::new(),
            right_buffered: Vec::new(),
        }
    }

    fn push_left(&mut self, batch: RecordBatch) {
        self.left_schema = Some(batch.schema());
        self.left_buffered.push(batch);
    }

    fn push_right(&mut self, batch: RecordBatch) {
        self.right_schema = Some(batch.schema());
        self.right_buffered.push(batch);
    }

    /// Splits a side's buffer into the rows whose window has closed (`window_end <= watermark`,
    /// returned) and the rest (kept buffered). `None` if the side has not seen any rows.
    fn split_closed(
        buffered: &mut Vec<RecordBatch>,
        schema: &Option<SchemaRef>,
        wend: usize,
        watermark: i64,
    ) -> Option<RecordBatch> {
        let schema = schema.as_ref()?;
        if buffered.is_empty() {
            return None;
        }
        let all = concat_batches(schema, buffered.iter()).expect("concat window-join buffer");
        let ends = rt_to_millis(all.column(wend));
        let closed_mask: BooleanArray = ends.iter().map(|v| Some(v.unwrap() <= watermark)).collect();
        let closed = filter_record_batch(&all, &closed_mask).expect("filter closed windows");
        let pending_mask = arrow::compute::not(&closed_mask).expect("negate window mask");
        let pending = filter_record_batch(&all, &pending_mask).expect("filter pending windows");
        *buffered = if pending.num_rows() > 0 { vec![pending] } else { Vec::new() };
        Some(closed)
    }

    /// Joins and evicts the windows the watermark has closed (empty batch if nothing matches).
    fn flush(&mut self, watermark: i64) -> RecordBatch {
        let left = Self::split_closed(&mut self.left_buffered, &self.left_schema, self.left_wend, watermark);
        let right =
            Self::split_closed(&mut self.right_buffered, &self.right_schema, self.right_wend, watermark);
        match (left, right) {
            (Some(left), Some(right)) if left.num_rows() > 0 && right.num_rows() > 0 => {
                // Join on the user keys plus the window bounds, so only rows of the same window match.
                let mut on: Vec<(usize, usize)> =
                    self.left_keys.iter().zip(&self.right_keys).map(|(&l, &r)| (l, r)).collect();
                on.push((self.left_wstart, self.right_wstart));
                on.push((self.left_wend, self.right_wend));
                hash_join_inner(left, right, &on, None)
            }
            _ => RecordBatch::new_empty(Arc::new(Schema::empty())),
        }
    }

    /// Serializes both buffers (`[u32 left_len][left ipc][right ipc]`) for a checkpoint.
    fn snapshot(&self) -> Vec<u8> {
        let serialize = |schema: &Option<SchemaRef>, buffered: &[RecordBatch]| match schema {
            Some(schema) if !buffered.is_empty() => {
                write_ipc(&concat_batches(schema, buffered.iter()).expect("concat window-join buffer"))
            }
            _ => Vec::new(),
        };
        let left = serialize(&self.left_schema, &self.left_buffered);
        let right = serialize(&self.right_schema, &self.right_buffered);
        let mut out = (left.len() as u32).to_le_bytes().to_vec();
        out.extend_from_slice(&left);
        out.extend_from_slice(&right);
        out
    }

    #[allow(clippy::too_many_arguments)]
    fn restore(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_wstart: usize,
        left_wend: usize,
        right_wstart: usize,
        right_wend: usize,
        bytes: &[u8],
    ) -> Self {
        let mut joiner =
            WindowJoiner::new(left_keys, right_keys, left_wstart, left_wend, right_wstart, right_wend);
        let left_len = u32::from_le_bytes(bytes[0..4].try_into().expect("snapshot len")) as usize;
        for batch in read_ipc_if_present(&bytes[4..4 + left_len]) {
            joiner.left_schema = Some(batch.schema());
            joiner.left_buffered.push(batch);
        }
        for batch in read_ipc_if_present(&bytes[4 + left_len..]) {
            joiner.right_schema = Some(batch.schema());
            joiner.right_buffered.push(batch);
        }
        joiner
    }
}

/// A full input row carried as scalars, used as a multiset key in the updating join's state.
type JoinRow = Vec<ScalarValue>;

/// Regular (non-windowed) INNER equi-join over a changelog, the "updating join". Unlike the
/// time-bounded interval/window joins — which buffer a batch and delegate the match to a DataFusion
/// hash join — this keeps a per-side keyed multiset of live rows and probes it incrementally per
/// input row, because retract correctness needs per-row counts a batch join does not give. This is
/// how the standalone streaming engines do it (RisingWave's `JoinHashMap`, Proton's `MemoryHashJoin`;
/// see divergences/14); INNER needs no match-degree table (only outer/semi/anti would).
///
/// On each input row the arriving side emits one output per matching row on the *other* side
/// (repeated by that row's multiset count), carrying the input row's `RowKind` — so a `+I`/`+U`
/// produces inserts/update-afters and a `-U`/`-D` produces the matching retractions — then folds the
/// row into its own side's multiset. State grows until rows are retracted (no time eviction). The
/// emitted batch is `[left cols.., right cols..]` plus the `$row_kind$` byte column.
struct UpdatingJoiner {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    left_schema: Option<SchemaRef>,
    right_schema: Option<SchemaRef>,
    left_state: HashMap<GroupKey, HashMap<JoinRow, i64>>,
    right_state: HashMap<GroupKey, HashMap<JoinRow, i64>>,
}

impl UpdatingJoiner {
    fn new(left_keys: Vec<usize>, right_keys: Vec<usize>) -> Self {
        UpdatingJoiner {
            left_keys,
            right_keys,
            left_schema: None,
            right_schema: None,
            left_state: HashMap::new(),
            right_state: HashMap::new(),
        }
    }

    /// Reads a batch's data schema (every column except a trailing `$row_kind$`, if present).
    fn data_schema(batch: &RecordBatch) -> SchemaRef {
        let arity = data_arity(batch);
        Arc::new(Schema::new(
            batch.schema().fields().iter().take(arity).map(|f| f.as_ref().clone()).collect::<Vec<_>>(),
        ))
    }

    /// Folds an input batch into its side and emits the join changelog it produces.
    fn push(&mut self, batch: &RecordBatch, is_left: bool) -> RecordBatch {
        let arity = data_arity(batch);
        let schema = Self::data_schema(batch);
        if is_left {
            self.left_schema = Some(schema);
        } else {
            self.right_schema = Some(schema);
        }
        let key_indices = if is_left { &self.left_keys } else { &self.right_keys };
        let key_arrays: Vec<&ArrayRef> = key_indices.iter().map(|&i| batch.column(i)).collect();
        let data_arrays: Vec<&ArrayRef> = (0..arity).map(|i| batch.column(i)).collect();
        let row_kinds = row_kind_column(batch);

        let mut out_rows: Vec<JoinRow> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();

        for row in 0..batch.num_rows() {
            // Absent `$row_kind$` (insert-only columnar input) ⇒ every row is an INSERT.
            let kind = row_kinds.map_or(0, |kinds| kinds.value(row));
            let retract = kind == 1 || kind == 3;
            let key = read_key(&key_arrays, row);
            // INNER equi-join: a null in the join key can never satisfy `a.k = b.k`, so the row
            // neither joins nor is stored (the host's null-filtering INNER semantics).
            if key.iter().any(ScalarValue::is_null) {
                continue;
            }
            let full: JoinRow = data_arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a, row).expect("join row scalar"))
                .collect();
            // Emit against the other side's current multiset for this key (it is unaffected by the
            // update to our own side below, so order does not matter).
            {
                let other = if is_left { &self.right_state } else { &self.left_state };
                if let Some(matches) = other.get(&key) {
                    for (other_row, &count) in matches.iter() {
                        let output: JoinRow = if is_left {
                            full.iter().chain(other_row).cloned().collect()
                        } else {
                            other_row.iter().chain(&full).cloned().collect()
                        };
                        for _ in 0..count.max(0) {
                            out_rows.push(output.clone());
                            out_kinds.push(kind);
                        }
                    }
                }
            }
            // Fold the row into our own side's multiset.
            let own = if is_left { &mut self.left_state } else { &mut self.right_state };
            let bucket = own.entry(key.clone()).or_default();
            let next = bucket.get(&full).copied().unwrap_or(0) + if retract { -1 } else { 1 };
            if next <= 0 {
                bucket.remove(&full);
            } else {
                bucket.insert(full, next);
            }
            if bucket.is_empty() {
                own.remove(&key);
            }
        }

        if out_rows.is_empty() {
            return RecordBatch::new_empty(Arc::new(Schema::empty()));
        }
        let left_schema = self.left_schema.as_ref().expect("left schema once a pair matched");
        let right_schema = self.right_schema.as_ref().expect("right schema once a pair matched");
        let types: Vec<DataType> = left_schema
            .fields()
            .iter()
            .chain(right_schema.fields().iter())
            .map(|f| f.data_type().clone())
            .collect();
        let mut fields: Vec<Field> = (0..types.len())
            .map(|j| Field::new(format!("c{j}"), types[j].clone(), true))
            .collect();
        let mut columns: Vec<ArrayRef> = (0..types.len())
            .map(|j| scalars_to_array(out_rows.iter().map(|r| r[j].clone()).collect(), &types[j]))
            .collect();
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build updating-join changelog batch")
    }

    /// Serializes one side's multiset as `[data cols.., __count__]` (one row per distinct live row),
    /// or no bytes when the side has no schema or no rows yet.
    fn serialize_side(&self, is_left: bool) -> Vec<u8> {
        let (schema, state) = if is_left {
            (&self.left_schema, &self.left_state)
        } else {
            (&self.right_schema, &self.right_state)
        };
        let Some(schema) = schema else { return Vec::new() };
        let mut rows: Vec<&JoinRow> = Vec::new();
        let mut counts: Vec<i64> = Vec::new();
        for bucket in state.values() {
            for (row, &count) in bucket.iter() {
                rows.push(row);
                counts.push(count);
            }
        }
        if rows.is_empty() {
            return Vec::new();
        }
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| {
                scalars_to_array(rows.iter().map(|r| r[j].clone()).collect(), fields[j].data_type())
            })
            .collect();
        fields.push(Field::new("__count__", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(counts)));
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("join side"))
    }

    fn snapshot(&self) -> Vec<u8> {
        let left = self.serialize_side(true);
        let right = self.serialize_side(false);
        let mut out = (left.len() as u32).to_le_bytes().to_vec();
        out.extend_from_slice(&left);
        out.extend_from_slice(&right);
        out
    }

    fn restore(left_keys: Vec<usize>, right_keys: Vec<usize>, bytes: &[u8]) -> Self {
        let mut joiner = UpdatingJoiner::new(left_keys, right_keys);
        let left_len = u32::from_le_bytes(bytes[0..4].try_into().expect("snapshot len")) as usize;
        joiner.load_side(true, &bytes[4..4 + left_len]);
        joiner.load_side(false, &bytes[4 + left_len..]);
        joiner
    }

    fn load_side(&mut self, is_left: bool, bytes: &[u8]) {
        for batch in read_ipc_if_present(bytes) {
            // The snapshot side batch is `[data cols.., __count__]`; the data schema is all but the
            // trailing count (not a `$row_kind$` column, so `data_schema` does not apply here).
            let arity = batch.num_columns() - 1;
            let schema = Arc::new(Schema::new(
                batch.schema().fields().iter().take(arity).map(|f| f.as_ref().clone()).collect::<Vec<_>>(),
            ));
            let key_indices = if is_left { &self.left_keys } else { &self.right_keys };
            let key_arrays: Vec<&ArrayRef> = key_indices.iter().map(|&i| batch.column(i)).collect();
            let counts = column_i64(&batch, "__count__");
            let state = if is_left { &mut self.left_state } else { &mut self.right_state };
            for row in 0..batch.num_rows() {
                let key = read_key(&key_arrays, row);
                let full: JoinRow = (0..arity)
                    .map(|i| ScalarValue::try_from_array(batch.column(i), row).expect("join row scalar"))
                    .collect();
                state.entry(key).or_default().insert(full, counts.value(row));
            }
            if is_left {
                self.left_schema = Some(schema);
            } else {
                self.right_schema = Some(schema);
            }
        }
    }
}

/// One ORDER BY column for the Top-N comparator: which column, ascending vs descending, and whether
/// nulls sort first (independent of direction, as in SQL `NULLS FIRST`/`LAST`).
struct SortColumn {
    index: usize,
    ascending: bool,
    nulls_first: bool,
}

/// Orders two rows by the sort columns, returning the first column's decision. Null placement
/// follows `nulls_first` and is not flipped by `ascending`; the value comparison is.
fn compare_rows(a: &[ScalarValue], b: &[ScalarValue], sort: &[SortColumn]) -> std::cmp::Ordering {
    use std::cmp::Ordering::Equal;
    for s in sort {
        let (x, y) = (&a[s.index], &b[s.index]);
        let ord = match (x.is_null(), y.is_null()) {
            (true, true) => Equal,
            (true, false) => {
                if s.nulls_first {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (false, true) => {
                if s.nulls_first {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            (false, false) => {
                let c = x.partial_cmp(y).unwrap_or(Equal);
                if s.ascending {
                    c
                } else {
                    c.reverse()
                }
            }
        };
        if ord != Equal {
            return ord;
        }
    }
    Equal
}

/// Append-only streaming Top-N (`ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) <= N`, rank number
/// not projected). Per partition it keeps the top `limit` rows sorted by the order keys (ties in
/// arrival order), exactly the host's append-only bounded buffer. On each input row it inserts into
/// the buffer; if that overflows the limit it drops the last (rank N+1) — emitting nothing if the
/// new row is the one dropped, else a DELETE of the displaced row — and otherwise emits the new row
/// as an INSERT. Output is the input columns (no rank column) plus the `$row_kind$` byte.
struct TopNRanker {
    partition_columns: Vec<usize>,
    sort_columns: Vec<SortColumn>,
    limit: i64,
    schema: Option<SchemaRef>,
    groups: HashMap<GroupKey, Vec<JoinRow>>,
}

impl TopNRanker {
    fn new(partition_columns: Vec<usize>, sort_columns: Vec<SortColumn>, limit: i64) -> Self {
        TopNRanker {
            partition_columns,
            sort_columns,
            limit,
            schema: None,
            groups: HashMap::new(),
        }
    }

    fn push(&mut self, batch: &RecordBatch) -> RecordBatch {
        let arity = data_arity(batch);
        self.schema = Some(UpdatingJoiner::data_schema(batch));
        let partition_arrays: Vec<&ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i)).collect();
        let data_arrays: Vec<&ArrayRef> = (0..arity).map(|i| batch.column(i)).collect();

        let mut out_rows: Vec<JoinRow> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();

        for row in 0..batch.num_rows() {
            let key = read_key(&partition_arrays, row);
            let full: JoinRow = data_arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a, row).expect("top-n row scalar"))
                .collect();
            let sort = &self.sort_columns;
            let buffer = self.groups.entry(key).or_default();
            // Insert after any rows that order equal-or-before, preserving arrival order for ties.
            let pos = buffer.partition_point(|r| compare_rows(r, &full, sort) != std::cmp::Ordering::Greater);
            buffer.insert(pos, full.clone());
            if buffer.len() as i64 > self.limit {
                let evicted = buffer.pop().expect("buffer over limit is non-empty");
                if evicted == full {
                    continue; // the new row was itself rank N+1 — it never entered the top-N
                }
                out_rows.push(evicted);
                out_kinds.push(3); // -D the displaced row
            }
            out_rows.push(full);
            out_kinds.push(0); // +I the new row
        }
        self.emit(out_rows, out_kinds)
    }

    fn emit(&self, out_rows: Vec<JoinRow>, out_kinds: Vec<i8>) -> RecordBatch {
        if out_rows.is_empty() {
            return RecordBatch::new_empty(Arc::new(Schema::empty()));
        }
        let schema = self.schema.as_ref().expect("schema set once a row was processed");
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| {
                scalars_to_array(out_rows.iter().map(|r| r[j].clone()).collect(), fields[j].data_type())
            })
            .collect();
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build top-n changelog batch")
    }

    /// Serializes the buffered rows in per-partition buffer order (partition derivable from the row).
    fn snapshot(&self) -> Vec<u8> {
        let Some(schema) = &self.schema else { return Vec::new() };
        let rows: Vec<&JoinRow> = self.groups.values().flatten().collect();
        if rows.is_empty() {
            return Vec::new();
        }
        let fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| {
                scalars_to_array(rows.iter().map(|r| r[j].clone()).collect(), fields[j].data_type())
            })
            .collect();
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("top-n snapshot"))
    }

    fn restore(
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        bytes: &[u8],
    ) -> Self {
        let mut ranker = TopNRanker::new(partition_columns, sort_columns, limit);
        for batch in read_ipc_if_present(bytes) {
            ranker.schema = Some(batch.schema());
            let partition_arrays: Vec<&ArrayRef> =
                ranker.partition_columns.iter().map(|&i| batch.column(i)).collect();
            for row in 0..batch.num_rows() {
                let key = read_key(&partition_arrays, row);
                let full: JoinRow = (0..batch.num_columns())
                    .map(|i| ScalarValue::try_from_array(batch.column(i), row).expect("top-n scalar"))
                    .collect();
                ranker.groups.entry(key).or_default().push(full);
            }
        }
        ranker
    }
}

/// One open session for a key: its end (the latest element's timestamp plus the gap) and the
/// incremental accumulators folding in its rows. The start is the map key that holds it.
struct Session {
    end: i64,
    accumulators: Vec<Box<dyn Accumulator>>,
}

/// Folds the state of one accumulator set into another, used when two sessions merge into one.
fn merge_into(into: &mut [Box<dyn Accumulator>], mut from: Vec<Box<dyn Accumulator>>) {
    for (target, source) in into.iter_mut().zip(from.iter_mut()) {
        let state: Vec<ArrayRef> =
            source.state().expect("state").into_iter().map(|s| s.to_array().expect("scalar")).collect();
        target.merge_batch(&state).expect("failed to merge session");
    }
}

/// Event-time session-window aggregation. Unlike the fixed-bin tumbling/hopping windows, sessions
/// are dynamic and per key: each element opens a `[ts, ts + gap)` window that merges with any
/// existing session it intersects, so a single element can bridge two sessions into one. A session
/// is finalized once a watermark passes its end. The connected-components result this produces is
/// order-independent, matching the host's merging window assigner.
struct SessionAggregator {
    gap_millis: i64,
    aggregates: Vec<WindowAggregate>,
    sessions: HashMap<GroupKey, BTreeMap<i64, Session>>,
    key_types: Vec<DataType>,
}

impl SessionAggregator {
    fn new(gap_millis: i64, value_types: Vec<i64>, kinds: Vec<i64>) -> Self {
        SessionAggregator {
            gap_millis,
            aggregates: build_aggregates(&kinds, &value_types),
            sessions: HashMap::new(),
            key_types: Vec::new(),
        }
    }

    fn update(&mut self, batch: &RecordBatch) {
        let ts = column_i64(batch, "ts");
        // One value column per aggregate (value0, value1, …); each accumulator reads its own.
        let values: Vec<&ArrayRef> = (0..self.aggregates.len())
            .map(|i| batch.column_by_name(&format!("value{i}")).expect("missing value column"))
            .collect();
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);

        for row in 0..batch.num_rows() {
            let key = read_key(&key_arrays, row);
            let candidate_start = ts.value(row);
            let candidate_end = candidate_start + self.gap_millis;
            let row_indices = UInt32Array::from(vec![row as u32]);
            let row_values: Vec<ArrayRef> =
                values.iter().map(|v| take(v, &row_indices, None).expect("take value")).collect();

            let map = self.sessions.entry(key).or_default();
            // Existing sessions are maximal and pairwise separated, but a `gap`-wide candidate can
            // still straddle more than one, so absorb every session it intersects. Intersection is
            // inclusive at the bounds (a gap of exactly `gap` still merges), matching the host's
            // `TimeWindow.intersects`.
            let overlapping: Vec<i64> = map
                .iter()
                .filter(|(start, session)| **start <= candidate_end && candidate_start <= session.end)
                .map(|(start, _)| *start)
                .collect();

            let mut start = candidate_start;
            let mut end = candidate_end;
            let mut accumulators: Vec<Box<dyn Accumulator>> =
                self.aggregates.iter().map(WindowAggregate::create_accumulator).collect();
            for overlap in overlapping {
                let session = map.remove(&overlap).expect("session present");
                start = start.min(overlap);
                end = end.max(session.end);
                merge_into(&mut accumulators, session.accumulators);
            }
            for (i, accumulator) in accumulators.iter_mut().enumerate() {
                accumulator.update_batch(std::slice::from_ref(&row_values[i])).expect("update");
            }
            map.insert(start, Session { end, accumulators });
        }
    }

    /// Finalizes and removes sessions the watermark has closed, emitting
    /// `[key, window_start, window_end, result0..resultN-1]`. The end is the session's own bound,
    /// not a fixed offset, so it travels as its own column.
    fn flush(&mut self, watermark: i64) -> RecordBatch {
        let n = self.aggregates.len();
        let mut rows: Vec<(GroupKey, i64, i64, Vec<ScalarValue>)> = Vec::new();
        for (key, map) in self.sessions.iter_mut() {
            let closed: Vec<i64> =
                map.iter().filter(|(_, s)| s.end <= watermark).map(|(start, _)| *start).collect();
            for start in closed {
                let mut session = map.remove(&start).expect("session present");
                let results = session
                    .accumulators
                    .iter_mut()
                    .map(|a| a.evaluate().expect("failed to finalize"))
                    .collect();
                rows.push((key.clone(), start, session.end, results));
            }
        }
        self.sessions.retain(|_, map| !map.is_empty());
        rows.sort_by(|a, b| (&a.0, a.1).partial_cmp(&(&b.0, b.1)).unwrap_or(std::cmp::Ordering::Equal));

        let keys: Vec<GroupKey> = rows.iter().map(|(key, ..)| key.clone()).collect();
        let starts: Vec<i64> = rows.iter().map(|(_, start, ..)| *start).collect();
        let ends: Vec<i64> = rows.iter().map(|(_, _, end, _)| *end).collect();
        let mut fields = key_fields(&self.key_types);
        let mut columns = key_columns(&keys, &self.key_types);
        fields.push(Field::new("window_start", DataType::Int64, false));
        fields.push(Field::new("window_end", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(starts)));
        columns.push(Arc::new(Int64Array::from(ends)));
        for i in 0..n {
            let scalars: Vec<ScalarValue> = rows.iter().map(|(_, _, _, r)| r[i].clone()).collect();
            fields.push(Field::new(format!("result{i}"), self.aggregates[i].result_type(), false));
            columns.push(scalars_to_array(scalars, &self.aggregates[i].result_type()));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build result batch")
    }

    /// Serializes every open session (one row per (key, session): key, start, end, then each
    /// accumulator's state fields) with Arrow IPC, mirroring the tumbling checkpoint path.
    fn snapshot(&mut self) -> Vec<u8> {
        let state_fields: Vec<Field> =
            self.aggregates.iter().flat_map(WindowAggregate::state_fields).collect();

        let mut keys: Vec<GroupKey> = Vec::new();
        let mut starts: Vec<i64> = Vec::new();
        let mut ends: Vec<i64> = Vec::new();
        let mut state_columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); state_fields.len()];
        for (key, map) in self.sessions.iter_mut() {
            for (start, session) in map.iter_mut() {
                keys.push(key.clone());
                starts.push(*start);
                ends.push(session.end);
                let mut column = 0;
                for accumulator in session.accumulators.iter_mut() {
                    for scalar in accumulator.state().expect("state") {
                        state_columns[column].push(scalar);
                        column += 1;
                    }
                }
            }
        }

        let mut fields = key_fields(&self.key_types);
        let mut columns = key_columns(&keys, &self.key_types);
        fields.push(Field::new("window_start", DataType::Int64, false));
        fields.push(Field::new("window_end", DataType::Int64, false));
        columns.push(Arc::new(Int64Array::from(starts)));
        columns.push(Arc::new(Int64Array::from(ends)));
        fields.extend(state_fields.iter().cloned());
        for (index, scalars) in state_columns.into_iter().enumerate() {
            columns.push(if scalars.is_empty() {
                new_empty_array(state_fields[index].data_type())
            } else {
                ScalarValue::iter_to_array(scalars).expect("state array")
            });
        }

        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build snapshot batch");
        let mut buffer = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &batch.schema())
            .expect("failed to open snapshot writer");
        writer.write(&batch).expect("failed to write snapshot");
        writer.finish().expect("failed to finish snapshot");
        drop(writer);
        buffer
    }

    fn restore(gap_millis: i64, value_types: Vec<i64>, kinds: Vec<i64>, bytes: &[u8]) -> Self {
        let mut aggregator = SessionAggregator::new(gap_millis, value_types, kinds);
        let field_counts: Vec<usize> =
            aggregator.aggregates.iter().map(|a| a.state_fields().len()).collect();
        let state_field_total: usize = field_counts.iter().sum();
        let reader = arrow::ipc::reader::StreamReader::try_new(bytes, None)
            .expect("failed to open snapshot reader");
        for batch in reader {
            let batch = batch.expect("failed to read snapshot");
            // Columns are [key0..key{arity-1}, window_start, window_end, state fields...].
            let arity = batch.num_columns() - 2 - state_field_total;
            let key_arrays: Vec<&ArrayRef> = (0..arity).map(|j| batch.column(j)).collect();
            aggregator.key_types = key_types(&key_arrays);
            let starts = batch
                .column(arity)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("window_start int64");
            let ends = batch
                .column(arity + 1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("window_end int64");
            for row in 0..batch.num_rows() {
                let mut accumulators: Vec<Box<dyn Accumulator>> =
                    aggregator.aggregates.iter().map(WindowAggregate::create_accumulator).collect();
                let mut column = arity + 2;
                for (i, accumulator) in accumulators.iter_mut().enumerate() {
                    let count = field_counts[i];
                    let state: Vec<ArrayRef> =
                        (column..column + count).map(|c| batch.column(c).slice(row, 1)).collect();
                    accumulator.merge_batch(&state).expect("failed to restore session");
                    column += count;
                }
                aggregator
                    .sessions
                    .entry(read_key(&key_arrays, row))
                    .or_default()
                    .insert(starts.value(row), Session { end: ends.value(row), accumulators });
            }
        }
        aggregator
    }
}

/// A composite grouping key: the typed values of the zero or more grouping columns for a row.
/// Scalars (rather than int64s) so the key can hold any column type — int/bigint/string/….
type GroupKey = Vec<ScalarValue>;

/// The partition a key maps to, by a consistent hash of the key values. The hash is internal — it
/// only distributes rows across partitions and need not match Flink's, because the downstream keyed
/// consumer is our own native operator that re-groups internally (see todo 10). A fixed-seed hasher
/// keeps the mapping identical across subtasks/processes.
fn partition_for_key(key: &GroupKey, num_partitions: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % num_partitions as u64) as usize
}

/// Splits a batch into up to `num_partitions` sub-batches by a consistent hash of the given key
/// columns, so every row with the same key lands in the same partition. Each sub-batch keeps the
/// full input schema (a row subset); empty partitions are omitted. This is the by-key split the
/// columnar shuffle routes to channels.
fn partition_batch(
    batch: &RecordBatch,
    key_columns: &[usize],
    num_partitions: usize,
) -> Vec<(usize, RecordBatch)> {
    let key_arrays: Vec<&ArrayRef> = key_columns.iter().map(|&i| batch.column(i)).collect();
    let mut rows_by_partition: Vec<Vec<u32>> = vec![Vec::new(); num_partitions];
    for row in 0..batch.num_rows() {
        let partition = partition_for_key(&read_key(&key_arrays, row), num_partitions);
        rows_by_partition[partition].push(row as u32);
    }
    let mut out = Vec::new();
    for (partition, rows) in rows_by_partition.into_iter().enumerate() {
        if rows.is_empty() {
            continue;
        }
        let indices = UInt32Array::from(rows);
        let columns: Vec<ArrayRef> =
            batch.columns().iter().map(|c| take(c, &indices, None).expect("take")).collect();
        out.push((partition, RecordBatch::try_new(batch.schema(), columns).expect("sub batch")));
    }
    out
}

/// The key columns (`key0`, `key1`, …) present in a batch, in order, as generic arrays. Their count
/// is the grouping arity; an unkeyed (window-only) aggregation has none.
fn key_arrays<'a>(batch: &'a RecordBatch) -> Vec<&'a ArrayRef> {
    let mut arrays = Vec::new();
    while let Some(column) = batch.column_by_name(&format!("key{}", arrays.len())) {
        arrays.push(column);
    }
    arrays
}

/// The Arrow types of the key columns, in order (used to build emitted key columns by position).
fn key_types(arrays: &[&ArrayRef]) -> Vec<DataType> {
    arrays.iter().map(|array| array.data_type().clone()).collect()
}

/// Reads one row's composite key from the gathered key columns.
fn read_key(arrays: &[&ArrayRef], row: usize) -> GroupKey {
    arrays
        .iter()
        .map(|array| ScalarValue::try_from_array(array.as_ref(), row).expect("read key scalar"))
        .collect()
}

/// The `key0..key{n-1}` schema fields for an emitted batch, one per stored key type.
fn key_fields(types: &[DataType]) -> Vec<Field> {
    types.iter().enumerate().map(|(j, t)| Field::new(format!("key{j}"), t.clone(), false)).collect()
}

/// Transposes per-row composite keys into one typed column per key position.
fn key_columns(keys: &[GroupKey], types: &[DataType]) -> Vec<ArrayRef> {
    (0..types.len())
        .map(|j| {
            let scalars: Vec<ScalarValue> = keys.iter().map(|key| key[j].clone()).collect();
            if scalars.is_empty() {
                new_empty_array(&types[j])
            } else {
                ScalarValue::iter_to_array(scalars).expect("key column array")
            }
        })
        .collect()
}

/// Downcasts a named int64 column, with a clear message if it is missing or the wrong type.
fn column_i64<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("column {name} must be int64"))
}

/// The native data plane runs stateful operators as asynchronous plans, so the work is driven on a
/// shared multi-threaded runtime that outlives any single call rather than spun up per batch.
fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build the native runtime")
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_version<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    env.new_string(env!("CARGO_PKG_VERSION"))
        .expect("failed to allocate Java string for version")
        .into_raw()
}

/// Drives a trivial asynchronous computation to completion on the shared runtime, proving the
/// blocking pull bridge a JVM thread will use to await native plan execution.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_blockingAnswer<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    runtime().block_on(async { 42 })
}

/// Imports a single Arrow array exported by the JVM through the C Data Interface and returns the
/// sum of its values, proving the columnar buffers cross the boundary zero-copy and intact.
///
/// Takes ownership of the producer-allocated C structs by swapping in released placeholders, so the
/// release callbacks fire exactly once when the imported data and schema drop here.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_sumInt<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    array_address: jlong,
    schema_address: jlong,
) -> jlong {
    let ffi_array =
        unsafe { std::ptr::replace(array_address as *mut FFI_ArrowArray, FFI_ArrowArray::empty()) };
    let ffi_schema = unsafe {
        std::ptr::replace(schema_address as *mut FFI_ArrowSchema, FFI_ArrowSchema::empty())
    };

    let mut data = unsafe { from_ffi(ffi_array, &ffi_schema) }.expect("failed to import Arrow array");
    data.align_buffers();

    let array = make_array(data);
    let ints = array
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("expected an int32 array");
    ints.iter().flatten().map(i64::from).sum()
}

/// Imports an int32 column from the JVM, rebuilds it into native-owned buffers, and exports the
/// result back through the C Data Interface into consumer-allocated C structs.
///
/// The rebuilt array owns its buffers, so the inbound import is released when this call returns
/// while the outbound array stays alive until the JVM imports and releases it. This is the shape a
/// real operator takes: read an input batch, produce a new one.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_roundTrip<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let ffi_array = unsafe {
        std::ptr::replace(in_array_address as *mut FFI_ArrowArray, FFI_ArrowArray::empty())
    };
    let ffi_schema = unsafe {
        std::ptr::replace(in_schema_address as *mut FFI_ArrowSchema, FFI_ArrowSchema::empty())
    };

    let mut data = unsafe { from_ffi(ffi_array, &ffi_schema) }.expect("failed to import Arrow array");
    data.align_buffers();

    let array = make_array(data);
    let ints = array
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("expected an int32 array");
    let rebuilt = ints.iter().collect::<Int32Array>().to_data();

    let out_array = FFI_ArrowArray::new(&rebuilt);
    let out_schema =
        FFI_ArrowSchema::try_from(rebuilt.data_type()).expect("failed to export Arrow schema");
    unsafe {
        std::ptr::write(out_array_address as *mut FFI_ArrowArray, out_array);
        std::ptr::write(out_schema_address as *mut FFI_ArrowSchema, out_schema);
    }
}

/// Runs the first stateless operator over a batch from the JVM: a projection that doubles a single
/// int32 column, evaluated through DataFusion's expression engine exactly as Arroyo drives one
/// (evaluate the physical expression against the batch, then realize the column).
///
/// The fixed `column * 2` expression stands in until the planner feeds real expressions; the point
/// is that genuine engine logic now executes over JVM-owned columnar data and produces a batch back
/// across the same boundary.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_doubleColumn<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let ffi_array = unsafe {
        std::ptr::replace(in_array_address as *mut FFI_ArrowArray, FFI_ArrowArray::empty())
    };
    let ffi_schema = unsafe {
        std::ptr::replace(in_schema_address as *mut FFI_ArrowSchema, FFI_ArrowSchema::empty())
    };

    let field = Field::try_from(&ffi_schema).expect("failed to import Arrow field");
    let schema = Arc::new(Schema::new(vec![field]));

    let mut data = unsafe { from_ffi(ffi_array, &ffi_schema) }.expect("failed to import Arrow array");
    data.align_buffers();
    let batch =
        RecordBatch::try_new(schema.clone(), vec![make_array(data)]).expect("failed to build batch");

    let column = col(schema.field(0).name(), &schema).expect("failed to resolve column");
    let expr = binary(column, Operator::Multiply, lit(2i32), &schema).expect("failed to build expr");
    let projected = expr
        .evaluate(&batch)
        .expect("failed to evaluate projection")
        .into_array(batch.num_rows())
        .expect("failed to realize projected column");

    let out_data = projected.to_data();
    let out_array = FFI_ArrowArray::new(&out_data);
    let out_schema =
        FFI_ArrowSchema::try_from(out_data.data_type()).expect("failed to export Arrow schema");
    unsafe {
        std::ptr::write(out_array_address as *mut FFI_ArrowArray, out_array);
        std::ptr::write(out_schema_address as *mut FFI_ArrowSchema, out_schema);
    }
}

/// Imports a whole multi-column batch from the JVM and exports it back. A batch crosses the
/// boundary as a single struct, so importing it yields one struct array that unwraps into a record
/// batch and re-wraps on the way out. This is the columnar shape operators that read several
/// columns at once need, beyond the single-column path.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_echoBatch<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let ffi_array = unsafe {
        std::ptr::replace(in_array_address as *mut FFI_ArrowArray, FFI_ArrowArray::empty())
    };
    let ffi_schema = unsafe {
        std::ptr::replace(in_schema_address as *mut FFI_ArrowSchema, FFI_ArrowSchema::empty())
    };

    let mut data = unsafe { from_ffi(ffi_array, &ffi_schema) }.expect("failed to import Arrow batch");
    data.align_buffers();
    let batch = RecordBatch::from(StructArray::from(data));

    let out_data = StructArray::from(batch).to_data();
    let out_array = FFI_ArrowArray::new(&out_data);
    let out_schema =
        FFI_ArrowSchema::try_from(out_data.data_type()).expect("failed to export Arrow schema");
    unsafe {
        std::ptr::write(out_array_address as *mut FFI_ArrowArray, out_array);
        std::ptr::write(out_schema_address as *mut FFI_ArrowSchema, out_schema);
    }
}

/// Runs a filter as a full DataFusion plan over a batch from the JVM, keeping rows whose int32
/// column exceeds `threshold`, and exports the surviving column back.
///
/// Unlike a bare expression, a plan executes asynchronously and yields a stream of batches, so a
/// control-plane thread blocks on the shared runtime and pulls that stream to completion. This is
/// the drive model every stateful operator will use; a filter is the simplest plan that exercises
/// it because it actually changes the row count.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_filterGreaterThan<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    threshold: jint,
) {
    let ffi_array = unsafe {
        std::ptr::replace(in_array_address as *mut FFI_ArrowArray, FFI_ArrowArray::empty())
    };
    let ffi_schema = unsafe {
        std::ptr::replace(in_schema_address as *mut FFI_ArrowSchema, FFI_ArrowSchema::empty())
    };

    let field = Field::try_from(&ffi_schema).expect("failed to import Arrow field");
    let schema = Arc::new(Schema::new(vec![field]));
    let column_name = schema.field(0).name().clone();

    let mut data = unsafe { from_ffi(ffi_array, &ffi_schema) }.expect("failed to import Arrow array");
    data.align_buffers();
    let batch =
        RecordBatch::try_new(schema.clone(), vec![make_array(data)]).expect("failed to build batch");

    let result = runtime().block_on(async move {
        let ctx = SessionContext::new();
        let frame = ctx
            .read_batch(batch)
            .expect("failed to read batch")
            .filter(logical_col(&column_name).gt(logical_lit(threshold)))
            .expect("failed to build filter");
        let mut stream = frame.execute_stream().await.expect("failed to execute plan");
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
}

/// Builds a DataFusion expression from the JVM's pre-order encoding (ticket 19): `kinds`, `payload`,
/// and `child_counts` describe each node, with literals drawn from the typed pools by `payload`.
fn build_expr(
    schema: &SchemaRef,
    kinds: &[i64],
    payload: &[i64],
    child_counts: &[i64],
    longs: &[i64],
    doubles: &[f64],
    strings: &[Option<String>],
    cursor: &mut usize,
) -> datafusion::prelude::Expr {
    let node = *cursor;
    *cursor += 1;
    let arg = payload[node] as usize;
    match kinds[node] {
        0 => logical_col(schema.field(arg).name()),
        1 => logical_lit(longs[arg]),
        2 => logical_lit(doubles[arg]),
        3 => logical_lit(strings[arg].clone().expect("string literal")),
        4 => logical_lit(longs[arg] != 0),
        // An untyped NULL; the surrounding expression's coercion (e.g. a CASE branch) types it.
        5 => datafusion::prelude::Expr::Literal(ScalarValue::Null, None),
        // Narrow integer literals carry their declared width so arithmetic evaluates in the same
        // type as the host (e.g. `int * 2` stays int32 and wraps), not a widened type.
        7 => logical_lit(longs[arg] as i32),
        8 => logical_lit(longs[arg] as i16),
        9 => logical_lit(longs[arg] as i8),
        11 => {
            // A widening numeric cast: build the single child, then wrap it. `arg` is the target code.
            let child = build_expr(
                schema, kinds, payload, child_counts, longs, doubles, strings, cursor,
            );
            datafusion::prelude::Expr::Cast(datafusion::logical_expr::Cast::new(
                Box::new(child),
                cast_data_type(arg),
            ))
        }
        6 => {
            let op = payload[node];
            let count = child_counts[node] as usize;
            let mut args = Vec::with_capacity(count);
            for _ in 0..count {
                args.push(build_expr(
                    schema,
                    kinds,
                    payload,
                    child_counts,
                    longs,
                    doubles,
                    strings,
                    cursor,
                ));
            }
            build_call(op, args)
        }
        other => panic!("unsupported expression kind: {other}"),
    }
}

/// The Arrow type for a cast target code (mirrors the JVM encoder's widening cast targets).
fn cast_data_type(code: usize) -> DataType {
    match code {
        0 => DataType::Int8,
        1 => DataType::Int16,
        2 => DataType::Int32,
        3 => DataType::Int64,
        4 => DataType::Float32,
        5 => DataType::Float64,
        other => panic!("unsupported cast target: {other}"),
    }
}

/// Combines decoded operands by op code: arithmetic, the six comparisons, AND/OR/NOT, the null
/// predicates, and searched CASE.
fn build_call(op: i64, args: Vec<datafusion::prelude::Expr>) -> datafusion::prelude::Expr {
    if op == 40 {
        // Searched CASE: [when1, then1, …, else]. The trailing else is the odd operand out.
        let mut args = args;
        let else_expr = (args.len() % 2 == 1).then(|| Box::new(args.pop().expect("case else")));
        let mut when_then = Vec::with_capacity(args.len() / 2);
        let mut iter = args.into_iter();
        while let (Some(when), Some(then)) = (iter.next(), iter.next()) {
            when_then.push((Box::new(when), Box::new(then)));
        }
        return datafusion::prelude::Expr::Case(datafusion::logical_expr::Case::new(
            None, when_then, else_expr,
        ));
    }
    if op == 58 {
        // REPLACE(s, from, to): replace every occurrence of `from` with `to`.
        let mut a = args.into_iter();
        return datafusion::functions::string::expr_fn::replace(
            a.next().expect("replace string"),
            a.next().expect("replace from"),
            a.next().expect("replace to"),
        );
    }
    if op == 84 {
        // ROUND(x) or ROUND(x, scale): opt-in (allowIncompatible) — see the Java encoder.
        return datafusion::functions::math::expr_fn::round(args);
    }
    if op == 82 || op == 83 {
        // LPAD/RPAD yield a Utf8View; cast back to Utf8 for the JVM converter.
        let padded = if op == 82 {
            datafusion::functions::unicode::expr_fn::lpad(args)
        } else {
            datafusion::functions::unicode::expr_fn::rpad(args)
        };
        return datafusion::prelude::Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(padded),
            DataType::Utf8,
        ));
    }
    if op == 57 {
        // POSITION(sub IN s): operands arrive [sub, s]; strpos takes (string, substring).
        let mut a = args.into_iter();
        let substring = a.next().expect("position substring");
        let string = a.next().expect("position string");
        return datafusion::functions::unicode::expr_fn::strpos(string, substring);
    }
    if op == 55 {
        // SUBSTRING: 2-arg substr(s, pos) or 3-arg substring(s, pos, len). DataFusion's substr
        // yields a Utf8View; cast back to Utf8 so the result is a plain VarChar vector the JVM
        // converter reads (same string content, just the non-view representation).
        let mut a = args.into_iter();
        let source = a.next().expect("substring source");
        let position = a.next().expect("substring position");
        let result = match a.next() {
            Some(length) => {
                datafusion::functions::unicode::expr_fn::substring(source, position, length)
            }
            None => datafusion::functions::unicode::expr_fn::substr(source, position),
        };
        return datafusion::prelude::Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(result),
            DataType::Utf8,
        ));
    }
    let mut it = args.into_iter();
    let mut next = || it.next().expect("missing operand");
    match op {
        0 => next() + next(),
        1 => next() - next(),
        2 => next() * next(),
        3 => next() / next(),
        4 => next() % next(),
        10 => next().gt(next()),
        11 => next().gt_eq(next()),
        12 => next().lt(next()),
        13 => next().lt_eq(next()),
        14 => next().eq(next()),
        15 => next().not_eq(next()),
        20 => next().and(next()),
        21 => next().or(next()),
        22 => !next(),
        30 => next().is_null(),
        31 => next().is_not_null(),
        52 => datafusion::functions::unicode::expr_fn::character_length(next()),
        54 => datafusion::functions::string::expr_fn::btrim(vec![next()]),
        60 => datafusion::functions::string::expr_fn::ltrim(vec![next()]),
        61 => datafusion::functions::string::expr_fn::rtrim(vec![next()]),
        62 => datafusion::functions::math::expr_fn::abs(next()),
        63 => datafusion::functions::math::expr_fn::floor(next()),
        64 => datafusion::functions::math::expr_fn::ceil(next()),
        65 => datafusion::functions::math::expr_fn::signum(next()),
        66 => datafusion::functions::string::expr_fn::repeat(next(), next()),
        67 => datafusion::functions::string::expr_fn::ascii(next()),
        81 => datafusion::functions::string::expr_fn::chr(next()),
        // Opt-in (allowIncompatible) functions: native results may differ from the host. The Java
        // encoder admits these only under the per-function flag — see NativeConfig.
        50 => datafusion::functions::string::expr_fn::upper(next()),
        51 => datafusion::functions::string::expr_fn::lower(next()),
        71 => datafusion::functions::math::expr_fn::power(next(), next()),
        72 => datafusion::functions::math::expr_fn::exp(next()),
        73 => datafusion::functions::math::expr_fn::ln(next()),
        74 => datafusion::functions::math::expr_fn::sin(next()),
        75 => datafusion::functions::math::expr_fn::cos(next()),
        76 => datafusion::functions::math::expr_fn::tan(next()),
        77 => datafusion::functions::math::expr_fn::asin(next()),
        78 => datafusion::functions::math::expr_fn::acos(next()),
        79 => datafusion::functions::math::expr_fn::atan(next()),
        80 => datafusion::functions::math::expr_fn::log10(next()),
        // LEFT/RIGHT yield a Utf8View; cast back to Utf8 for the JVM converter.
        69 => datafusion::prelude::Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(datafusion::functions::unicode::expr_fn::left(next(), next())),
            DataType::Utf8,
        )),
        70 => datafusion::prelude::Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(datafusion::functions::unicode::expr_fn::right(next(), next())),
            DataType::Utf8,
        )),
        56 => datafusion::prelude::Expr::Like(datafusion::logical_expr::Like::new(
            false,
            Box::new(next()),
            Box::new(next()),
            None,
            false,
        )),
        // REVERSE yields a Utf8View (like substr); cast back to Utf8 for the JVM converter.
        59 => datafusion::prelude::Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(datafusion::functions::unicode::expr_fn::reverse(next())),
            DataType::Utf8,
        )),
        other => panic!("unsupported expression op: {other}"),
    }
}

/// A compiled filter predicate held across batches: the decoded expression tree plus the physical
/// expression, which is built once against the first batch's schema and reused for every later
/// batch. This follows Comet — the plan is compiled once at operator construction, not re-planned
/// per batch — and evaluates the predicate directly (no per-batch `SessionContext` or async plan).
struct FilterExpression {
    kinds: Vec<i64>,
    payload: Vec<i64>,
    child_counts: Vec<i64>,
    longs: Vec<i64>,
    doubles: Vec<f64>,
    strings: Vec<Option<String>>,
    compiled: Option<Arc<dyn PhysicalExpr>>,
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
        let context = SimplifyContext::default().with_schema(df_schema.clone());
        let coerced = ExprSimplifier::new(context)
            .coerce(logical, df_schema.as_ref())
            .expect("failed to coerce predicate");
        let physical = create_physical_expr(&coerced, df_schema.as_ref(), &ExecutionProps::new())
            .expect("failed to compile predicate");
        self.compiled = Some(physical.clone());
        physical
    }

    /// Keeps the rows for which the predicate is true; a null result drops the row, as `WHERE` requires.
    fn filter(&mut self, batch: RecordBatch) -> RecordBatch {
        let predicate = self.predicate(&batch.schema());
        let evaluated = predicate
            .evaluate(&batch)
            .expect("failed to evaluate predicate")
            .into_array(batch.num_rows())
            .expect("failed to materialize predicate");
        let mask =
            evaluated.as_any().downcast_ref::<BooleanArray>().expect("predicate must be boolean");
        filter_record_batch(&batch, mask).expect("failed to filter batch")
    }
}

/// Compiles a general predicate expression (the JVM's encoded tree) into a reusable handle. The
/// handle owns the compiled plan and must be released with `closeFilterExpression`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createFilterExpression<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    kinds: JIntArray<'local>,
    payload: JIntArray<'local>,
    child_counts: JIntArray<'local>,
    longs: JLongArray<'local>,
    doubles: JDoubleArray<'local>,
    strings: JObjectArray<'local>,
) -> jlong {
    let expression = FilterExpression {
        kinds: read_int_array(&env, &kinds),
        payload: read_int_array(&env, &payload),
        child_counts: read_int_array(&env, &child_counts),
        longs: read_longs(&env, &longs),
        doubles: read_doubles(&env, &doubles),
        strings: read_strings(&mut env, &strings),
        compiled: None,
    };
    Box::into_raw(Box::new(expression)) as jlong
}

/// Filters a batch from the JVM through a compiled predicate handle, exporting the surviving rows.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_filterExpression<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let expression = unsafe { &mut *(handle as *mut FilterExpression) };
    let batch = import_record_batch(in_array_address, in_schema_address);
    let result = expression.filter(batch);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases a compiled predicate handle and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeFilterExpression<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut FilterExpression));
    }
}

/// A compiled expression tree against a schema, coerced like the planner's logical pipeline so the
/// physical expression sees the operand types the host would.
fn compile_expr(
    schema: &SchemaRef,
    df_schema: &DFSchema,
    kinds: &[i64],
    payload: &[i64],
    child_counts: &[i64],
    longs: &[i64],
    doubles: &[f64],
    strings: &[Option<String>],
    root: usize,
) -> Arc<dyn PhysicalExpr> {
    let mut cursor = root;
    let logical =
        build_expr(schema, kinds, payload, child_counts, longs, doubles, strings, &mut cursor);
    let context = SimplifyContext::default().with_schema(Arc::new(df_schema.clone()));
    let coerced =
        ExprSimplifier::new(context).coerce(logical, df_schema).expect("failed to coerce expr");
    create_physical_expr(&coerced, df_schema, &ExecutionProps::new()).expect("failed to compile expr")
}

/// The compiled form of a Calc: an optional filter predicate plus the projection expressions.
struct CompiledCalc {
    condition: Option<Arc<dyn PhysicalExpr>>,
    projections: Vec<Arc<dyn PhysicalExpr>>,
}

/// A compiled Calc held across batches: an optional condition and a list of projection expressions
/// (each an encoded tree rooted in the shared pools), built once against the first batch's schema.
/// It filters rows by the condition, then evaluates each projection over the survivors to form the
/// output batch — the general form of the filter-plus-column-subset path, also covering computed
/// columns and constants.
struct CalcExpression {
    kinds: Vec<i64>,
    payload: Vec<i64>,
    child_counts: Vec<i64>,
    longs: Vec<i64>,
    doubles: Vec<f64>,
    strings: Vec<Option<String>>,
    projection_roots: Vec<usize>,
    condition_root: i64,
    output_names: Vec<String>,
    compiled: Option<CompiledCalc>,
}

impl CalcExpression {
    fn compiled(&mut self, schema: &SchemaRef) -> &CompiledCalc {
        if self.compiled.is_none() {
            let df_schema = DFSchema::try_from(schema.as_ref().clone()).expect("failed to build schema");
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
            let condition = (self.condition_root >= 0).then(|| compile(self.condition_root as usize));
            let projections = self.projection_roots.iter().map(|&r| compile(r)).collect();
            self.compiled = Some(CompiledCalc { condition, projections });
        }
        self.compiled.as_ref().unwrap()
    }

    fn evaluate(&mut self, batch: RecordBatch) -> RecordBatch {
        let (condition, projections) = {
            let compiled = self.compiled(&batch.schema());
            (compiled.condition.clone(), compiled.projections.clone())
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
                filter_record_batch(&batch, mask).expect("failed to filter batch")
            }
            None => batch,
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
            fields.push(Field::new(&self.output_names[i], array.data_type().clone(), true));
            columns.push(array);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("failed to build output")
    }
}

/// Compiles an encoded Calc (optional condition + projection trees) into a reusable handle, released
/// with `closeCalcExpression`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createCalcExpression<'local>(
    mut env: JNIEnv<'local>,
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
    let expression = CalcExpression {
        kinds: read_int_array(&env, &kinds),
        payload: read_int_array(&env, &payload),
        child_counts: read_int_array(&env, &child_counts),
        longs: read_longs(&env, &longs),
        doubles: read_doubles(&env, &doubles),
        strings: read_strings(&mut env, &strings),
        projection_roots: read_int_array(&env, &projection_roots).into_iter().map(|r| r as usize).collect(),
        condition_root: condition_root as i64,
        output_names: read_strings(&mut env, &output_names)
            .into_iter()
            .map(|s| s.expect("output name"))
            .collect(),
        compiled: None,
    };
    Box::into_raw(Box::new(expression)) as jlong
}

/// Runs a batch from the JVM through a compiled Calc handle, exporting the projected output batch.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_calcExpression<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let expression = unsafe { &mut *(handle as *mut CalcExpression) };
    let batch = import_record_batch(in_array_address, in_schema_address);
    let result = expression.evaluate(batch);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases a compiled Calc handle and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeCalcExpression<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut CalcExpression));
    }
}

/// Encodes a single Arrow batch to a Parquet file. The core of the native columnar sink: the batch
/// is written in its columnar form directly, skipping the host's row-to-Parquet encoding.
fn write_parquet(batch: &RecordBatch, path: &str) {
    let file = std::fs::File::create(path).expect("failed to create parquet file");
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
        .expect("failed to create parquet writer");
    writer.write(batch).expect("failed to write batch");
    writer.close().expect("failed to close parquet writer");
}

/// Writes an Arrow batch the JVM exported to a Parquet file at `path`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_writeParquet<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    path: JString<'local>,
) {
    let batch = import_record_batch(in_array_address, in_schema_address);
    let path: String = env.get_string(&path).expect("failed to read path").into();
    write_parquet(&batch, &path);
}

/// A Parquet sink that holds one open `ArrowWriter` across many batches, so the output file count is
/// decoupled from the input batch size: the JVM rolls a file by closing this handle when its row
/// target is reached (and on checkpoint), not once per batch. The writer is created lazily from the
/// first batch's schema; an empty file (closed before any batch) writes a valid header-only Parquet.
struct ParquetSink {
    path: String,
    writer: Option<parquet::arrow::ArrowWriter<std::fs::File>>,
}

impl ParquetSink {
    fn new(path: String) -> ParquetSink {
        ParquetSink { path, writer: None }
    }

    fn write(&mut self, batch: RecordBatch) {
        if self.writer.is_none() {
            let file = std::fs::File::create(&self.path).expect("failed to create parquet file");
            self.writer = Some(
                parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
                    .expect("failed to create parquet writer"),
            );
        }
        self.writer.as_mut().unwrap().write(&batch).expect("failed to write batch");
    }

    fn close(self) {
        if let Some(writer) = self.writer {
            writer.close().expect("failed to close parquet writer");
        }
    }
}

/// Opens a Parquet file for writing and returns an opaque handle. Batches are appended with
/// `parquetWriterWrite`; the file is finalized (and the handle released) by `closeParquetWriter`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createParquetWriter<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
) -> jlong {
    let path: String = env.get_string(&path).expect("failed to read path").into();
    Box::into_raw(Box::new(ParquetSink::new(path))) as jlong
}

/// Appends an Arrow batch the JVM exported to the open file behind `handle`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_parquetWriterWrite<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let sink = unsafe { &mut *(handle as *mut ParquetSink) };
    let batch = import_record_batch(in_array_address, in_schema_address);
    sink.write(batch);
}

/// Finalizes the Parquet file (writes its footer) and releases the writer handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeParquetWriter<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    let sink = unsafe { Box::from_raw(handle as *mut ParquetSink) };
    sink.close();
}

/// Reorders/selects a batch's columns to match the requested projection (by name); identity when no
/// projection was requested. Shared by the file sources so projection pushdown behaves identically.
/// A native source the JVM pulls one Arrow batch at a time, until the split is exhausted. The concrete
/// reader is hidden behind this trait so the file formats share one open/next/close bridge — the bytes
/// are read and decoded directly in the engine, never crossing into the row world.
trait BatchSource {
    fn next_batch(&mut self) -> Option<RecordBatch>;
}

/// A scan of one file split, driven by DataFusion's file-scan execution — the same path
/// datafusion-comet uses. DataFusion selects the row groups (Parquet) / stripes (ORC) whose start
/// falls in the split's byte range, pushes the projection into the decode (only the wanted columns are
/// read), and yields Arrow batches, which we pull synchronously on the shared runtime. Parquet and ORC
/// differ only in the `FileSource` constructed.
struct FileScan {
    stream: datafusion::physical_plan::SendableRecordBatchStream,
}

impl FileScan {
    fn open(
        file_source: Arc<dyn datafusion::datasource::physical_plan::FileSource>,
        path: &str,
        schema: SchemaRef,
        projection: &[String],
        range_start: i64,
        range_length: i64,
    ) -> FileScan {
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::physical_plan::FileScanConfigBuilder;
        use datafusion::datasource::source::DataSourceExec;
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::physical_plan::ExecutionPlan;

        let size = std::fs::metadata(path).expect("failed to stat source file").len();
        let file =
            PartitionedFile::new_with_range(path.to_string(), size, range_start, range_start + range_length);
        // Projection as column indices in plan order; an empty projection reads every column.
        let indices: Vec<usize> = if projection.is_empty() {
            (0..schema.fields().len()).collect()
        } else {
            projection
                .iter()
                .map(|name| schema.index_of(name).expect("projected column not in source file"))
                .collect()
        };
        let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file(file)
            .with_projection_indices(Some(indices))
            .expect("failed to set scan projection")
            .build();
        let stream = DataSourceExec::from_data_source(config)
            .execute(0, SessionContext::new().task_ctx())
            .expect("failed to start file scan");
        FileScan { stream }
    }
}

impl BatchSource for FileScan {
    fn next_batch(&mut self) -> Option<RecordBatch> {
        runtime()
            .block_on(async { self.stream.next().await })
            .map(|batch| batch.expect("failed to read file batch"))
    }
}

/// The Arrow schema of a Parquet file, read from its footer to build the scan's file source.
fn parquet_file_schema(path: &str) -> SchemaRef {
    let file = std::fs::File::open(path).expect("failed to open parquet file");
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("failed to read parquet metadata")
        .schema()
        .clone()
}

/// The Arrow schema of an ORC file, read from its footer.
fn orc_file_schema(path: &str) -> SchemaRef {
    let file = std::fs::File::open(path).expect("failed to open orc file");
    orc_rust::ArrowReaderBuilder::try_new(file)
        .expect("failed to read orc metadata")
        .schema()
}

/// Decodes a column of raw JSON message bodies — one complete document per row, as a source hands
/// them off untouched — into a typed Arrow batch matching `schema`. This replaces Flink's per-record
/// `byte[] -> tree -> RowData` materialization with a single batched decode straight to columnar
/// form, so the row representation never exists on the hot ingest path. The body column may arrive as
/// binary or string (whichever the source-edge transpose produced for the message bytes).
struct JsonDecoder {
    schema: SchemaRef,
}

impl JsonDecoder {
    fn new(schema: SchemaRef) -> JsonDecoder {
        JsonDecoder { schema }
    }

    /// Decodes the single body column of `bodies` into a batch of the target schema. Each row is a
    /// complete document, so feeding them one at a time keeps the decoder's record boundaries aligned
    /// with the input rows; a null body contributes no row.
    fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        let column = bodies.column(0);
        let row_count = bodies.num_rows();
        let body = |row: usize| -> Option<&[u8]> {
            match column.data_type() {
                DataType::Binary => {
                    let array = column.as_any().downcast_ref::<BinaryArray>().unwrap();
                    array.is_valid(row).then(|| array.value(row))
                }
                DataType::LargeBinary => {
                    let array = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                    array.is_valid(row).then(|| array.value(row))
                }
                DataType::Utf8 => {
                    let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                    array.is_valid(row).then(|| array.value(row).as_bytes())
                }
                other => panic!("unsupported JSON body column type {other:?}"),
            }
        };

        let mut decoder = arrow::json::ReaderBuilder::new(self.schema.clone())
            .with_batch_size(row_count.max(1))
            .build_decoder()
            .expect("failed to build JSON decoder");
        for row in 0..row_count {
            if let Some(bytes) = body(row) {
                let consumed = decoder.decode(bytes).expect("failed to decode JSON record");
                assert_eq!(consumed, bytes.len(), "JSON body was not a single complete document");
            }
        }
        decoder
            .flush()
            .expect("failed to flush JSON batch")
            .unwrap_or_else(|| RecordBatch::new_empty(self.schema.clone()))
    }
}

/// Opens one Parquet split — the row groups of `path` within `[range_start, range_start +
/// range_length)` — and returns an opaque handle, released with `closeSource`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_openParquet<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    projection: JObjectArray<'local>,
    range_start: jlong,
    range_length: jlong,
) -> jlong {
    let path: String = env.get_string(&path).expect("failed to read path").into();
    let projection = read_strings(&mut env, &projection)
        .into_iter()
        .map(|name| name.expect("projection column name was null"))
        .collect::<Vec<_>>();
    let schema = parquet_file_schema(&path);
    let file_source = Arc::new(
        datafusion::datasource::physical_plan::ParquetSource::new(schema.clone()),
    ) as Arc<dyn datafusion::datasource::physical_plan::FileSource>;
    let source: Box<dyn BatchSource> =
        Box::new(FileScan::open(file_source, &path, schema, &projection, range_start, range_length));
    Box::into_raw(Box::new(source)) as jlong
}

/// Exports the next Arrow batch from the source into the consumer-allocated C structs, returning
/// true if a batch was produced and false once the directory is exhausted. Shared by every native
/// file source.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_nextBatch<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jboolean {
    let source = unsafe { &mut *(handle as *mut Box<dyn BatchSource>) };
    match source.next_batch() {
        Some(batch) => {
            export_record_batch(batch, out_array_address, out_schema_address);
            1
        }
        None => 0,
    }
}

/// Releases a native file source handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeSource<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut Box<dyn BatchSource>));
    }
}

/// Opens one ORC split — the stripes of `path` within `[range_start, range_start + range_length)` —
/// and returns an opaque handle, released with `closeSource`. Driven by datafusion-orc's file source,
/// which maps the split's byte range to the stripes it covers.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_openOrc<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
    projection: JObjectArray<'local>,
    range_start: jlong,
    range_length: jlong,
) -> jlong {
    let path: String = env.get_string(&path).expect("failed to read path").into();
    let projection = read_strings(&mut env, &projection)
        .into_iter()
        .map(|name| name.expect("projection column name was null"))
        .collect::<Vec<_>>();
    let schema = orc_file_schema(&path);
    let table_schema = datafusion::datasource::table_schema::TableSchema::from(schema.clone());
    let file_source = Arc::new(datafusion_orc::OrcSource::new(table_schema))
        as Arc<dyn datafusion::datasource::physical_plan::FileSource>;
    let source: Box<dyn BatchSource> =
        Box::new(FileScan::open(file_source, &path, schema, &projection, range_start, range_length));
    Box::into_raw(Box::new(source)) as jlong
}

/// Holds the per-partition sub-batches of one split, pulled out one at a time by the JVM.
struct SplitState {
    partitions: Vec<(usize, RecordBatch)>,
    cursor: usize,
}

/// Splits a batch from the JVM by key into per-partition sub-batches and returns a handle to pull
/// them with `nextSplit`; released with `closeSplit`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_splitByKey<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    key_columns: JIntArray<'local>,
    num_partitions: jint,
) -> jlong {
    let batch = import_record_batch(in_array_address, in_schema_address);
    let keys: Vec<usize> = read_int_array(&env, &key_columns).into_iter().map(|k| k as usize).collect();
    let partitions = partition_batch(&batch, &keys, num_partitions as usize);
    Box::into_raw(Box::new(SplitState { partitions, cursor: 0 })) as jlong
}

/// Exports the next sub-batch into the consumer-allocated C structs and returns its partition, or
/// -1 once the split is exhausted.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_nextSplit<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jint {
    let state = unsafe { &mut *(handle as *mut SplitState) };
    if state.cursor >= state.partitions.len() {
        return -1;
    }
    let (partition, batch) = state.partitions[state.cursor].clone();
    state.cursor += 1;
    export_record_batch(batch, out_array_address, out_schema_address);
    partition as jint
}

/// Releases a split handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeSplit<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut SplitState));
    }
}

/// Runs an event-time tumbling-window sum over a batch from the JVM: rows are bucketed by the start
/// of the `window_millis`-wide window their `ts` falls in, and `value` is summed per bucket. This
/// is the first aggregating operator and the core of the initial target envelope.
///
/// The window assignment and grouped aggregation run as a DataFusion plan on the shared runtime,
/// and the per-window result batch is exported back. Aggregation across batch boundaries, where the
/// operator must hold partial windows until a watermark closes them, is a later step.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_tumblingSum<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    window_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let ffi_array = unsafe {
        std::ptr::replace(in_array_address as *mut FFI_ArrowArray, FFI_ArrowArray::empty())
    };
    let ffi_schema = unsafe {
        std::ptr::replace(in_schema_address as *mut FFI_ArrowSchema, FFI_ArrowSchema::empty())
    };

    let mut data = unsafe { from_ffi(ffi_array, &ffi_schema) }.expect("failed to import Arrow batch");
    data.align_buffers();
    let batch = RecordBatch::from(StructArray::from(data));

    let window = format!("ts - (ts % {window_millis})");
    let query = format!(
        "SELECT {window} AS window_start, SUM(value) AS total \
         FROM events GROUP BY {window} ORDER BY window_start"
    );

    let result = runtime().block_on(async move {
        let ctx = SessionContext::new();
        ctx.register_batch("events", batch).expect("failed to register batch");
        let frame = ctx.sql(&query).await.expect("failed to plan aggregation");
        let mut stream = frame.execute_stream().await.expect("failed to execute plan");
        let schema = stream.schema();
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.expect("failed to pull batch"));
        }
        concat_batches(&schema, &batches).expect("failed to assemble result")
    });

    let out_data = StructArray::from(result).to_data();
    let out_array = FFI_ArrowArray::new(&out_data);
    let out_schema =
        FFI_ArrowSchema::try_from(out_data.data_type()).expect("failed to export Arrow schema");
    unsafe {
        std::ptr::write(out_array_address as *mut FFI_ArrowArray, out_array);
        std::ptr::write(out_schema_address as *mut FFI_ArrowSchema, out_schema);
    }
}

/// Creates a stateful tumbling-window aggregator and returns an opaque handle to it. The handle
/// owns native state that lives across calls; the JVM must release it with the matching close.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createTumblingAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    window_millis: jlong,
    slide_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    Box::into_raw(Box::new(TumblingAggregator::new(
        window_millis,
        slide_millis,
        false,
        value_types,
        kinds,
    ))) as jlong
}

/// Creates a stateful cumulative-window aggregator (nested windows of `step` up to `max_size`) and
/// returns an opaque handle. It shares the aligned-window engine and every other call (update,
/// flush, snapshot, close) with the tumbling handle; only the window assignment differs.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createCumulativeAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    max_size_millis: jlong,
    step_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    Box::into_raw(Box::new(TumblingAggregator::new(
        max_size_millis,
        step_millis,
        true,
        value_types,
        kinds,
    ))) as jlong
}

/// Reads a JVM int[] (aggregate kinds or per-aggregate value-type codes) into a Vec.
fn read_int_array(env: &JNIEnv, array: &JIntArray) -> Vec<i64> {
    let length = env.get_array_length(array).expect("failed to read int array length");
    let mut buffer = vec![0i32; length as usize];
    env.get_int_array_region(array, 0, &mut buffer).expect("failed to read int array");
    buffer.into_iter().map(i64::from).collect()
}

/// Reads a JVM double[] into a Vec.
fn read_doubles(env: &JNIEnv, values: &JDoubleArray) -> Vec<f64> {
    let length = env.get_array_length(values).expect("failed to read doubles length");
    let mut buffer = vec![0f64; length as usize];
    env.get_double_array_region(values, 0, &mut buffer).expect("failed to read doubles");
    buffer
}

/// Reads a JVM long[] into a Vec.
fn read_longs(env: &JNIEnv, values: &JLongArray) -> Vec<i64> {
    let length = env.get_array_length(values).expect("failed to read longs length");
    let mut buffer = vec![0i64; length as usize];
    env.get_long_array_region(values, 0, &mut buffer).expect("failed to read longs");
    buffer
}

/// Reads a JVM String[] into a Vec, mapping a Java null element to None (a numeric comparison).
fn read_strings(env: &mut JNIEnv, values: &JObjectArray) -> Vec<Option<String>> {
    let length = env.get_array_length(values).expect("failed to read strings length");
    let mut out = Vec::with_capacity(length as usize);
    for index in 0..length {
        let element = env.get_object_array_element(values, index).expect("failed to read string");
        if element.is_null() {
            out.push(None);
        } else {
            let text: String =
                env.get_string(&JString::from(element)).expect("failed to decode string").into();
            out.push(Some(text));
        }
    }
    out
}

/// Folds a batch from the JVM into the aggregator's open windows. Produces no output; results are
/// emitted later when a watermark closes windows.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updateTumblingAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
    let batch = import_record_batch(in_array_address, in_schema_address);
    aggregator.update(&batch);
}

/// Emits the windows the given watermark has closed as a batch and drops them from state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushTumblingAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
    let result = aggregator.flush(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Local two-phase half: merges a batch of partials `[key, partial, slice_end]` into the windows.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updatePartialTumblingAggregator<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
    let batch = import_record_batch(in_array_address, in_schema_address);
    aggregator.update_partial(&batch);
}

/// Local two-phase half: emits the partial state of the windows the watermark has closed.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushPartialTumblingAggregator<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
    let result = aggregator.flush_partial(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases the aggregator and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeTumblingAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut TumblingAggregator));
    }
}

/// Serializes the aggregator's open windows so the JVM can store them in a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotTumblingAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
    env.byte_array_from_slice(&aggregator.snapshot())
        .expect("failed to allocate snapshot array")
        .into_raw()
}

/// Rebuilds an aggregator from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreTumblingAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    window_millis: jlong,
    slide_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    snapshot: JByteArray<'local>,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read snapshot");
    Box::into_raw(Box::new(TumblingAggregator::restore(
        window_millis,
        slide_millis,
        false,
        value_types,
        kinds,
        &bytes,
    ))) as jlong
}

/// Rebuilds a cumulative-window aggregator from a snapshot taken by a prior run.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreCumulativeAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    max_size_millis: jlong,
    step_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    snapshot: JByteArray<'local>,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read snapshot");
    Box::into_raw(Box::new(TumblingAggregator::restore(
        max_size_millis,
        step_millis,
        true,
        value_types,
        kinds,
        &bytes,
    ))) as jlong
}

/// Reads a JVM int[] of column indices into a Vec.
fn read_columns(env: &JNIEnv, columns: &JIntArray) -> Vec<usize> {
    read_int_array(env, columns).into_iter().map(|c| c as usize).collect()
}

/// Creates a columnar OVER aggregator (event-time RANGE unbounded preceding); it buffers input
/// batches and flushes completed rows with the running aggregates appended. The rt/value/key column
/// indices locate those columns within the buffered input batch.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    value_type: jint,
    aggregate_kinds: JIntArray<'local>,
    rt_column: jint,
    value_column: jint,
    key_columns: JIntArray<'local>,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let keys = read_columns(&env, &key_columns);
    Box::into_raw(Box::new(OverWindowAggregator::new(
        value_type as i64,
        kinds,
        rt_column as usize,
        if value_column < 0 { None } else { Some(value_column as usize) },
        keys,
    ))) as jlong
}

/// Buffers an input batch (no output); the rows are emitted later when a watermark completes them.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushOverAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
    aggregator.push(import_record_batch(in_array_address, in_schema_address));
}

/// Exports the rows the watermark has completed (input columns + running aggregates).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushOverAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
    let result = aggregator.flush(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases the OVER aggregator and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeOverAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut OverWindowAggregator));
    }
}

/// Serializes the OVER aggregator's running state and buffered rows for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
    env.byte_array_from_slice(&aggregator.snapshot())
        .expect("failed to allocate over snapshot array")
        .into_raw()
}

/// Rebuilds an OVER aggregator from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    value_type: jint,
    aggregate_kinds: JIntArray<'local>,
    rt_column: jint,
    value_column: jint,
    key_columns: JIntArray<'local>,
    snapshot: JByteArray<'local>,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let keys = read_columns(&env, &key_columns);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read over snapshot");
    Box::into_raw(Box::new(OverWindowAggregator::restore(
        value_type as i64,
        kinds,
        rt_column as usize,
        if value_column < 0 { None } else { Some(value_column as usize) },
        keys,
        &bytes,
    ))) as jlong
}

/// Creates a non-windowed `GROUP BY` aggregator and returns an opaque handle. The aggregate kinds
/// and per-aggregate value-type codes are positional; `generate_update_before` is the host's
/// per-node changelog flag. Grouping keys travel as `key0..` columns on each input batch.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    aggregate_kinds: JIntArray<'local>,
    value_types: JIntArray<'local>,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    generate_update_before: jboolean,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let value_columns = read_int_array(&env, &value_columns);
    let key_columns = read_columns(&env, &key_columns);
    Box::into_raw(Box::new(GroupAggregator::new(
        kinds,
        value_types,
        value_columns,
        key_columns,
        generate_update_before != 0,
    ))) as jlong
}

/// Folds an input batch into per-key state and exports the changelog rows it produces (the row kinds
/// ride the `$row_kind$` column of the result).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updateGroupAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut GroupAggregator) };
    let result = aggregator.update(&import_record_batch(in_array_address, in_schema_address));
    export_record_batch(result, out_array_address, out_schema_address);
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
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    aggregate_kinds: JIntArray<'local>,
    value_types: JIntArray<'local>,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    generate_update_before: jboolean,
    snapshot: JByteArray<'local>,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let value_columns = read_int_array(&env, &value_columns);
    let key_columns = read_columns(&env, &key_columns);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read group-by snapshot");
    Box::into_raw(Box::new(GroupAggregator::restore(
        kinds,
        value_types,
        value_columns,
        key_columns,
        generate_update_before != 0,
        &bytes,
    ))) as jlong
}

/// Releases the `GROUP BY` aggregator and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeGroupAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut GroupAggregator));
    }
}

/// Creates a JSON decode operator and returns an opaque handle, released with `closeJsonDecoder`. The
/// target schema is taken from an empty batch the JVM exports (built from the plan's row type); the
/// operator turns a batch of one binary column of raw JSON message bodies into a typed batch of that
/// schema. This is the format-decode core both the shallow and native Kafka paths feed bytes into.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createJsonDecoder<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema_array_address: jlong,
    schema_address: jlong,
) -> jlong {
    let template = import_record_batch(schema_array_address, schema_address);
    Box::into_raw(Box::new(JsonDecoder::new(template.schema()))) as jlong
}

/// Decodes one input batch (a single binary column of JSON bodies) into a typed batch, exporting it
/// into the consumer-allocated C structs.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_decodeJson<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let decoder = unsafe { &*(handle as *mut JsonDecoder) };
    let bodies = import_record_batch(in_array_address, in_schema_address);
    export_record_batch(decoder.decode(&bodies), out_array_address, out_schema_address);
}

/// Releases a JSON decode operator handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeJsonDecoder<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut JsonDecoder));
    }
}

/// Reads a JVM String[] into a Vec<String>.
#[cfg(feature = "kafka")]
fn read_string_array(env: &mut JNIEnv, array: &JObjectArray) -> Vec<String> {
    let length = env.get_array_length(array).expect("failed to read string[] length");
    let mut out = Vec::with_capacity(length as usize);
    for i in 0..length {
        let element = env.get_object_array_element(array, i).expect("failed to read string[] element");
        let string: String = env.get_string(&JString::from(element)).expect("failed to read string").into();
        out.push(string);
    }
    out
}

/// The production native Kafka consumer for one Flink subtask: a single rdkafka `BaseConsumer` that
/// multiplexes all of the subtask's assigned partitions (Flink-parity — one consumer, not one per
/// split). It manually `assign()`s (topic, partition) at explicit offsets and re-assigns as the
/// enumerator hands over more splits — never `subscribe()`/group-rebalance, matching Flink's
/// `KafkaPartitionSplitReader`. Each poll buckets messages **by partition** and decodes one typed Arrow
/// batch per partition, so every split's checkpointed offset advances independently; the decoded
/// batches sit in `pending` until the JVM drains them into its per-split `RecordsWithSplitIds`. Payloads
/// go straight from librdkafka into an Arrow builder (no JVM heap byte[], no per-record JNI). The
/// librdkafka config is the map from the JVM-side `KafkaConfigTranslator`; this side is a dumb applier.
#[cfg(feature = "kafka")]
struct KafkaSplitReader {
    consumer: rdkafka::consumer::BaseConsumer,
    decoder: JsonDecoder,
    body_schema: SchemaRef,
    /// Next offset to consume per assigned partition — the split's checkpoint position. Re-assigns seek
    /// existing partitions back here (a no-op) and new ones to their start offset.
    next_offsets: HashMap<(String, i32), i64>,
    /// Per-partition decoded batches from the last poll, drained one split at a time by the JVM.
    pending: Vec<(String, i32, i64, RecordBatch)>,
}

#[cfg(feature = "kafka")]
impl KafkaSplitReader {
    fn open(config: &[(String, String)], output_schema: SchemaRef) -> KafkaSplitReader {
        use rdkafka::config::ClientConfig;

        let mut client = ClientConfig::new();
        for (key, value) in config {
            client.set(key, value);
        }
        let consumer: rdkafka::consumer::BaseConsumer =
            client.create().expect("failed to create kafka consumer");
        let body_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Binary, true)]));
        KafkaSplitReader {
            consumer,
            decoder: JsonDecoder::new(output_schema),
            body_schema,
            next_offsets: HashMap::new(),
            pending: Vec::new(),
        }
    }

    /// Adds splits (idempotent) and re-assigns the whole set: each newly added partition seeks to its
    /// given start offset, each existing one stays at its tracked next offset. assign() with explicit
    /// offsets both assigns and seeks, so no subscribe/rebalance is involved.
    ///
    /// A negative start offset is one of Flink's `KafkaPartitionSplit` markers, which the enumerator
    /// leaves for the reader to resolve: -2 EARLIEST -> beginning, -1 LATEST -> end, -3 COMMITTED ->
    /// the group's stored offset. A concrete (>= 0) offset seeks to exactly there.
    fn assign_splits(&mut self, topics: &[String], partitions: &[i64], offsets: &[i64]) {
        use rdkafka::consumer::Consumer;
        use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

        for i in 0..topics.len() {
            self.next_offsets
                .entry((topics[i].clone(), partitions[i] as i32))
                .or_insert(offsets[i]);
        }
        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), &offset) in &self.next_offsets {
            let position = match offset {
                -2 => Offset::Beginning,
                -1 => Offset::End,
                -3 => Offset::Stored,
                concrete if concrete >= 0 => Offset::Offset(concrete),
                _ => Offset::Beginning,
            };
            tpl.add_partition_offset(topic, *partition, position)
                .expect("failed to add partition offset");
        }
        self.consumer.assign(&tpl).expect("failed to assign partitions");
    }

    /// Polls up to `max_records` messages, buckets them by partition, and decodes one typed Arrow batch
    /// per partition into `pending`, advancing each split's next offset. Returns the number of
    /// per-partition batches now pending (0 on a poll timeout).
    fn poll(&mut self, max_records: usize, timeout: std::time::Duration) -> usize {
        use arrow::array::BinaryBuilder;
        use rdkafka::message::Message;

        let mut builders: HashMap<(String, i32), (BinaryBuilder, i64)> = HashMap::new();
        let mut buffered = 0usize;
        while buffered < max_records {
            match self.consumer.poll(timeout) {
                Some(Ok(message)) => {
                    let key = (message.topic().to_string(), message.partition());
                    let entry = builders.entry(key).or_insert_with(|| (BinaryBuilder::new(), 0));
                    entry.0.append_value(message.payload().unwrap_or(&[]));
                    entry.1 = message.offset() + 1;
                    buffered += 1;
                }
                // librdkafka surfaces transient connectivity events (e.g. AllBrokersDown while still
                // connecting) as discrete poll errors; librdkafka reconnects internally, so log and
                // keep polling (the next poll returns None once drained) — the same log-and-continue
                // the rdkafka examples use, rather than failing the way a panic across the FFI would.
                Some(Err(error)) => eprintln!("[streamfusion-kafka] poll error: {error}"),
                None => break, // poll timeout with nothing pending
            }
        }
        self.pending.clear();
        for ((topic, partition), (mut builder, next_offset)) in builders {
            let body = RecordBatch::try_new(self.body_schema.clone(), vec![Arc::new(builder.finish())])
                .expect("failed to build kafka body batch");
            let decoded = self.decoder.decode(&body);
            self.next_offsets.insert((topic.clone(), partition), next_offset);
            self.pending.push((topic, partition, next_offset, decoded));
        }
        self.pending.len()
    }
}

/// Opens a native Kafka split reader for one subtask and returns an opaque handle, released with
/// `closeKafkaConsumer`. `configKeys`/`configValues` are the translated librdkafka config (applied
/// verbatim); the schema C structs carry the decoder's output schema. Splits are added later via
/// `assignKafkaSplits` as the enumerator assigns them.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_openKafkaConsumer<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    schema_array_address: jlong,
    schema_address: jlong,
) -> jlong {
    let keys = read_string_array(&mut env, &config_keys);
    let values = read_string_array(&mut env, &config_values);
    let config: Vec<(String, String)> = keys.into_iter().zip(values).collect();
    let schema = import_record_batch(schema_array_address, schema_address).schema();
    let reader = KafkaSplitReader::open(&config, schema);
    Box::into_raw(Box::new(reader)) as jlong
}

/// Adds splits to the reader and re-assigns: `topics`/`partitions`/`startOffsets` are index-aligned;
/// new partitions seek to their start offset, existing ones keep their tracked position.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_assignKafkaSplits<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topics: JObjectArray<'local>,
    partitions: JLongArray<'local>,
    start_offsets: JLongArray<'local>,
) {
    let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
    let topics = read_string_array(&mut env, &topics);
    let partitions = read_longs(&env, &partitions);
    let offsets = read_longs(&env, &start_offsets);
    reader.assign_splits(&topics, &partitions, &offsets);
}

/// Polls one cycle, decoding one Arrow batch per partition that had messages. Returns the number of
/// per-partition batches now pending; the JVM drains each with `drainKafkaSplit`.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pollKafkaBatch<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_records: jint,
    timeout_ms: jlong,
) -> jint {
    let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
    reader.poll(
        max_records as usize,
        std::time::Duration::from_millis(timeout_ms as u64),
    ) as jint
}

/// Drains one pending per-partition batch: exports the decoded typed Arrow into the consumer C structs,
/// writes `[partition, nextOffset]` into `splitMeta`, and the topic into `outTopic[0]`, so the JVM can
/// form the split id and advance that split's checkpoint offset. Returns the decoded row count; call it
/// `pollKafkaBatch`'s return-value times.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_drainKafkaSplit<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    split_meta: JLongArray<'local>,
    out_topic: JObjectArray<'local>,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jint {
    let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
    let (topic, partition, next_offset, batch) =
        reader.pending.pop().expect("drainKafkaSplit called with no pending batch");
    let rows = batch.num_rows() as jint;
    env.set_long_array_region(&split_meta, 0, &[partition as i64, next_offset])
        .expect("failed to write split meta");
    let topic_jstr = env.new_string(&topic).expect("failed to make topic string");
    env.set_object_array_element(&out_topic, 0, &topic_jstr)
        .expect("failed to write topic");
    export_record_batch(batch, out_array_address, out_schema_address);
    rows
}

/// Releases a native Kafka split reader, dropping the rdkafka consumer (which closes its connections).
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeKafkaConsumer<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut KafkaSplitReader));
    }
}

/// Benchmark-only: consume an entire topic with a native (rdkafka) consumer and decode it to typed
/// Arrow, all in Rust — message payloads go straight from librdkafka into an Arrow binary builder (one
/// copy, no JVM heap byte[] and no per-record JNI crossing), then through the same `JsonDecoder` the
/// shallow path uses. Returns the decoded row count; the JVM times this single call to compare native
/// consume+decode against the shallow path. This is the fast path's measurement, not yet the
/// production FLIP-27 source (no enumerator/offset/config-fidelity work — see ticket 33).
#[cfg(feature = "kafka-bench")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_benchmarkKafkaConsume<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    brokers: JString<'local>,
    topic: JString<'local>,
    schema_array_address: jlong,
    schema_address: jlong,
    max_messages: jlong,
) -> jlong {
    use arrow::array::BinaryBuilder;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::message::Message;

    let brokers: String = env.get_string(&brokers).expect("failed to read brokers").into();
    let topic: String = env.get_string(&topic).expect("failed to read topic").into();
    let decoder = JsonDecoder::new(import_record_batch(schema_array_address, schema_address).schema());

    // A fresh group reading from the beginning each run; offsets are not committed (the consumer is
    // throwaway). This mirrors the manual, non-committing consumption the production source would do.
    let group = format!("streamfusion-bench-{}", std::process::id());
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("failed to create kafka consumer");
    consumer.subscribe(&[&topic]).expect("failed to subscribe");

    let body_field = Field::new("body", DataType::Binary, true);
    let body_schema = Arc::new(Schema::new(vec![body_field]));
    let mut builder = BinaryBuilder::new();
    let mut buffered = 0usize;
    let mut seen: i64 = 0;
    let mut rows: i64 = 0;
    let mut decode = |builder: &mut BinaryBuilder| -> i64 {
        let batch = RecordBatch::try_new(body_schema.clone(), vec![Arc::new(builder.finish())])
            .expect("failed to build kafka body batch");
        decoder.decode(&batch).num_rows() as i64
    };

    while seen < max_messages {
        match consumer.poll(std::time::Duration::from_secs(5)) {
            Some(Ok(message)) => {
                builder.append_value(message.payload().unwrap_or(&[]));
                buffered += 1;
                seen += 1;
                if buffered >= 8192 {
                    rows += decode(&mut builder);
                    buffered = 0;
                }
            }
            Some(Err(error)) => panic!("kafka consume error: {error}"),
            None => break, // poll timeout: the produced messages are exhausted
        }
    }
    if buffered > 0 {
        rows += decode(&mut builder);
    }
    rows
}

/// Creates an event-time INNER interval joiner and returns an opaque handle. The key/time column
/// indices locate the equi-join key and rowtime within each side's input batch; `lower`/`upper` are
/// the inclusive bounds (millis) on `left.rt - right.rt`. The JVM owns the handle across calls.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_time: jint,
    right_time: jint,
    lower: jlong,
    upper: jlong,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    Box::into_raw(Box::new(IntervalJoiner::new(
        left,
        right,
        left_time as usize,
        right_time as usize,
        lower,
        upper,
    ))) as jlong
}

/// Pushes a left batch, probing the buffered right rows and exporting the matched pairs.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushLeftIntervalJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
    let batch = import_record_batch(in_array_address, in_schema_address);
    let result = joiner.push_left(batch);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Pushes a right batch, probing the buffered left rows and exporting the matched pairs.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushRightIntervalJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
    let batch = import_record_batch(in_array_address, in_schema_address);
    let result = joiner.push_right(batch);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Advances the combined watermark, evicting rows no future arrival can match.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_advanceIntervalJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
    joiner.advance(watermark_millis);
}

/// Releases the interval joiner and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeIntervalJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut IntervalJoiner));
    }
}

/// Serializes the joiner's buffered rows for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let joiner = unsafe { &*(handle as *mut IntervalJoiner) };
    env.byte_array_from_slice(&joiner.snapshot())
        .expect("failed to allocate join snapshot array")
        .into_raw()
}

/// Rebuilds an interval joiner from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_time: jint,
    right_time: jint,
    lower: jlong,
    upper: jlong,
    snapshot: JByteArray<'local>,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read join snapshot");
    Box::into_raw(Box::new(IntervalJoiner::restore(
        left,
        right,
        left_time as usize,
        right_time as usize,
        lower,
        upper,
        &bytes,
    ))) as jlong
}

/// Creates a regular (non-windowed) INNER updating joiner and returns an opaque handle. The key
/// column indices locate the equi-join key within each side's input batch (whose trailing column is
/// the `$row_kind$` byte). The JVM owns the handle and must release it with the matching close.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createUpdatingJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    Box::into_raw(Box::new(UpdatingJoiner::new(left, right))) as jlong
}

/// Folds a left batch into state and exports the join changelog it produces (left cols, right cols,
/// then `$row_kind$`).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushLeftUpdatingJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut UpdatingJoiner) };
    let result = joiner.push(&import_record_batch(in_array_address, in_schema_address), true);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Folds a right batch into state and exports the join changelog it produces.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushRightUpdatingJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut UpdatingJoiner) };
    let result = joiner.push(&import_record_batch(in_array_address, in_schema_address), false);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Serializes the updating joiner's per-side state for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotUpdatingJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let joiner = unsafe { &mut *(handle as *mut UpdatingJoiner) };
    env.byte_array_from_slice(&joiner.snapshot())
        .expect("failed to allocate updating-join snapshot array")
        .into_raw()
}

/// Rebuilds an updating joiner from a snapshot and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreUpdatingJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    snapshot: JByteArray<'local>,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read updating-join snapshot");
    Box::into_raw(Box::new(UpdatingJoiner::restore(left, right, &bytes))) as jlong
}

/// Releases an updating joiner handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeUpdatingJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut UpdatingJoiner));
    }
}

/// Builds the sort-column comparator config from three parallel arrays (column index, ascending,
/// nulls-first), as the JVM passes the resolved ORDER BY spec.
fn read_sort_columns(
    env: &JNIEnv,
    indices: &JIntArray,
    ascending: &JIntArray,
    nulls_first: &JIntArray,
) -> Vec<SortColumn> {
    let indices = read_columns(env, indices);
    let ascending = read_int_array(env, ascending);
    let nulls_first = read_int_array(env, nulls_first);
    indices
        .into_iter()
        .enumerate()
        .map(|(i, index)| SortColumn {
            index,
            ascending: ascending[i] != 0,
            nulls_first: nulls_first[i] != 0,
        })
        .collect()
}

/// Creates an append-only streaming Top-N ranker (`ROW_NUMBER ... <= limit`, no rank-number output)
/// and returns an opaque handle. The JVM owns it and must release it with the matching close.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    limit: jlong,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
    Box::into_raw(Box::new(TopNRanker::new(partitions, sort, limit))) as jlong
}

/// Folds an input batch into the per-partition top-N and exports the changelog it produces (the
/// input columns plus `$row_kind$`).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushTopNRanker<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let ranker = unsafe { &mut *(handle as *mut TopNRanker) };
    let result = ranker.push(&import_record_batch(in_array_address, in_schema_address));
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Serializes the ranker's bounded per-partition buffers for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let ranker = unsafe { &mut *(handle as *mut TopNRanker) };
    env.byte_array_from_slice(&ranker.snapshot())
        .expect("failed to allocate top-n snapshot array")
        .into_raw()
}

/// Rebuilds a Top-N ranker from a snapshot and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    limit: jlong,
    snapshot: JByteArray<'local>,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read top-n snapshot");
    Box::into_raw(Box::new(TopNRanker::restore(partitions, sort, limit, &bytes))) as jlong
}

/// Releases a Top-N ranker handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeTopNRanker<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut TopNRanker));
    }
}

/// Creates an event-time INNER window joiner and returns an opaque handle. The key/window column
/// indices locate the equi-join key and the `window_start`/`window_end` columns within each side's
/// input batch. The JVM owns the handle across calls and must release it with the matching close.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_window_start: jint,
    left_window_end: jint,
    right_window_start: jint,
    right_window_end: jint,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    Box::into_raw(Box::new(WindowJoiner::new(
        left,
        right,
        left_window_start as usize,
        left_window_end as usize,
        right_window_start as usize,
        right_window_end as usize,
    ))) as jlong
}

/// Buffers a left batch (no output); its rows are joined later when the watermark closes their window.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushLeftWindowJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
    joiner.push_left(import_record_batch(in_array_address, in_schema_address));
}

/// Buffers a right batch (no output).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushRightWindowJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
    joiner.push_right(import_record_batch(in_array_address, in_schema_address));
}

/// Exports the INNER matches of every window the watermark has closed (then evicts those windows).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushWindowJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
    let result = joiner.flush(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases the window joiner and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeWindowJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut WindowJoiner));
    }
}

/// Serializes the window joiner's buffered rows for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let joiner = unsafe { &*(handle as *mut WindowJoiner) };
    env.byte_array_from_slice(&joiner.snapshot())
        .expect("failed to allocate window-join snapshot array")
        .into_raw()
}

/// Rebuilds a window joiner from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_window_start: jint,
    left_window_end: jint,
    right_window_start: jint,
    right_window_end: jint,
    snapshot: JByteArray<'local>,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read window-join snapshot");
    Box::into_raw(Box::new(WindowJoiner::restore(
        left,
        right,
        left_window_start as usize,
        left_window_end as usize,
        right_window_start as usize,
        right_window_end as usize,
        &bytes,
    ))) as jlong
}

/// Creates a stateful session-window aggregator and returns an opaque handle. As with the tumbling
/// handle, the JVM owns the native state across calls and must release it with the matching close.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    gap_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    Box::into_raw(Box::new(SessionAggregator::new(gap_millis, value_types, kinds))) as jlong
}

/// Folds a batch from the JVM into the session aggregator, merging sessions as elements bridge them.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updateSessionAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
    let batch = import_record_batch(in_array_address, in_schema_address);
    aggregator.update(&batch);
}

/// Emits the sessions the given watermark has closed as a batch and drops them from state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushSessionAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
    let result = aggregator.flush(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases the session aggregator and its native state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeSessionAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut SessionAggregator));
    }
}

/// Serializes the aggregator's open sessions so the JVM can store them in a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
    env.byte_array_from_slice(&aggregator.snapshot())
        .expect("failed to allocate snapshot array")
        .into_raw()
}

/// Rebuilds a session aggregator from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    gap_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    snapshot: JByteArray<'local>,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read snapshot");
    Box::into_raw(Box::new(SessionAggregator::restore(gap_millis, value_types, kinds, &bytes)))
        as jlong
}

/// Thin wrappers exposing the engine hot paths to the Criterion benchmark harness, without leaking
/// the JNI internals or the Arrow-FFI plumbing. Not used by the JVM bridge.
pub mod bench {
    use super::*;

    /// A filter predicate compiled once (on the first `run`) and reused, as the operator uses it.
    pub struct Filter(FilterExpression);

    impl Filter {
        pub fn new(
            kinds: Vec<i64>,
            payload: Vec<i64>,
            child_counts: Vec<i64>,
            longs: Vec<i64>,
            doubles: Vec<f64>,
            strings: Vec<Option<String>>,
        ) -> Self {
            Filter(FilterExpression {
                kinds,
                payload,
                child_counts,
                longs,
                doubles,
                strings,
                compiled: None,
            })
        }

        pub fn run(&mut self, batch: RecordBatch) -> RecordBatch {
            self.0.filter(batch)
        }
    }

    /// A tumbling-window aggregator driven by `update`/`flush`, as the stateful operator drives it.
    pub struct Tumbling(TumblingAggregator);

    impl Tumbling {
        pub fn new(window_millis: i64, value_type: i64, kinds: Vec<i64>) -> Self {
            let value_types = vec![value_type; kinds.len()];
            Tumbling(TumblingAggregator::new(window_millis, window_millis, false, value_types, kinds))
        }

        pub fn update(&mut self, batch: &RecordBatch) {
            self.0.update(batch);
        }

        pub fn flush(&mut self, watermark: i64) -> RecordBatch {
            self.0.flush(watermark)
        }
    }

    /// A session-window aggregator driven by `update`/`flush`.
    pub struct Session(SessionAggregator);

    impl Session {
        pub fn new(gap_millis: i64, value_type: i64, kinds: Vec<i64>) -> Self {
            let value_types = vec![value_type; kinds.len()];
            Session(SessionAggregator::new(gap_millis, value_types, kinds))
        }

        pub fn update(&mut self, batch: &RecordBatch) {
            self.0.update(batch);
        }

        pub fn flush(&mut self, watermark: i64) -> RecordBatch {
            self.0.flush(watermark)
        }
    }

    /// A columnar OVER operator driven by push/flush, as the stateful operator drives it.
    pub struct Over(OverWindowAggregator);

    impl Over {
        pub fn new(
            value_type: i64,
            kinds: Vec<i64>,
            rt_column: usize,
            value_column: Option<usize>,
            key_columns: Vec<usize>,
        ) -> Self {
            Over(OverWindowAggregator::new(value_type, kinds, rt_column, value_column, key_columns))
        }

        pub fn push(&mut self, batch: RecordBatch) {
            self.0.push(batch);
        }

        pub fn flush(&mut self, watermark: i64) -> RecordBatch {
            self.0.flush(watermark)
        }
    }

    /// A source-edge JSON decoder (one document per input row -> a typed columnar batch).
    pub struct JsonDecode(JsonDecoder);

    impl JsonDecode {
        pub fn new(schema: SchemaRef) -> Self {
            JsonDecode(JsonDecoder::new(schema))
        }

        pub fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
            self.0.decode(bodies)
        }
    }

    /// A non-windowed GROUP BY aggregator (update emits the changelog), as the operator drives it.
    pub struct GroupBy(GroupAggregator);

    impl GroupBy {
        pub fn new(
            kinds: Vec<i64>,
            value_types: Vec<i64>,
            value_columns: Vec<i64>,
            key_columns: Vec<usize>,
        ) -> Self {
            GroupBy(GroupAggregator::new(kinds, value_types, value_columns, key_columns, true))
        }

        pub fn update(&mut self, batch: &RecordBatch) -> RecordBatch {
            self.0.update(batch)
        }
    }

    /// An event-time interval joiner (push emits matches immediately), as the operator drives it.
    pub struct IntervalJoin(IntervalJoiner);

    impl IntervalJoin {
        pub fn new(
            left_keys: Vec<usize>,
            right_keys: Vec<usize>,
            left_time: usize,
            right_time: usize,
            lower: i64,
            upper: i64,
        ) -> Self {
            IntervalJoin(IntervalJoiner::new(left_keys, right_keys, left_time, right_time, lower, upper))
        }

        pub fn push_left(&mut self, batch: RecordBatch) -> RecordBatch {
            self.0.push_left(batch)
        }

        pub fn push_right(&mut self, batch: RecordBatch) -> RecordBatch {
            self.0.push_right(batch)
        }
    }

    /// An event-time window joiner (buffer on push, join on flush), as the operator drives it.
    pub struct WindowJoin(WindowJoiner);

    impl WindowJoin {
        #[allow(clippy::too_many_arguments)]
        pub fn new(
            left_keys: Vec<usize>,
            right_keys: Vec<usize>,
            left_window_start: usize,
            left_window_end: usize,
            right_window_start: usize,
            right_window_end: usize,
        ) -> Self {
            WindowJoin(WindowJoiner::new(
                left_keys,
                right_keys,
                left_window_start,
                left_window_end,
                right_window_start,
                right_window_end,
            ))
        }

        pub fn push_left(&mut self, batch: RecordBatch) {
            self.0.push_left(batch);
        }

        pub fn push_right(&mut self, batch: RecordBatch) {
            self.0.push_right(batch);
        }

        pub fn flush(&mut self, watermark: i64) -> RecordBatch {
            self.0.flush(watermark)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_batch() -> RecordBatch {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 6, 3, 9]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 0, 8, 2]));
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Int64, true),
            ])),
            vec![a, b],
        )
        .unwrap()
    }

    fn values(batch: &RecordBatch, column: usize) -> Vec<i64> {
        batch.column(column).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()
    }

    fn json_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]))
    }

    fn bodies(docs: Vec<Option<&[u8]>>) -> RecordBatch {
        let column: ArrayRef = Arc::new(BinaryArray::from(docs));
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("body", DataType::Binary, true)])),
            vec![column],
        )
        .unwrap()
    }

    // Each input row is one complete JSON document; the decoder emits one typed row per document,
    // matching the target schema's columns and order.
    #[test]
    fn json_decode_emits_one_row_per_document() {
        let batch = bodies(vec![
            Some(br#"{"id": 1, "name": "a", "score": 1.5}"#),
            Some(br#"{"id": 2, "name": "b", "score": 2.5}"#),
        ]);
        let out = JsonDecoder::new(json_schema()).decode(&batch);
        assert_eq!(out.num_rows(), 2);
        assert_eq!(values(&out, 0), vec![1, 2]);
        let names = out.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!((names.value(0), names.value(1)), ("a", "b"));
    }

    // Fields absent from a document and a null body both yield SQL NULLs, not failures.
    #[test]
    fn json_decode_tolerates_missing_fields_and_null_bodies() {
        let batch = bodies(vec![
            Some(br#"{"id": 1}"#),
            None,
            Some(br#"{"id": 3, "name": "c", "score": 9.0}"#),
        ]);
        let out = JsonDecoder::new(json_schema()).decode(&batch);
        // A null body contributes no row; the present documents decode in order.
        assert_eq!(out.num_rows(), 2);
        assert_eq!(values(&out, 0), vec![1, 3]);
        assert!(out.column(1).is_null(0));
    }

    // An empty input batch flushes to an empty batch of the target schema, not a panic.
    #[test]
    fn json_decode_empty_batch_yields_empty() {
        let out = JsonDecoder::new(json_schema()).decode(&bodies(vec![]));
        assert_eq!(out.num_rows(), 0);
        assert_eq!(out.schema(), json_schema());
    }

    // OVER (ORDER BY rt RANGE UNBOUNDED PRECEDING) running SUM: ties in rt share the post-fold value,
    // and the running total persists across update calls.
    #[test]
    fn over_running_sum_shares_range_ties() {
        let rt: ArrayRef = Arc::new(Int64Array::from(vec![0i64, 1000, 1000, 2000]));
        let value: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30, 40]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("rt", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![rt, value]).unwrap();
        let mut over = OverAggregator::new(0, vec![0]); // bigint value, SUM
        // rt 1000 ties (20,30) both see 10+20+30=60; emitted in input order.
        assert_eq!(values(&over.update(&batch), 0), vec![10, 60, 60, 100]);

        // A later complete batch continues the running total (UNBOUNDED PRECEDING).
        let rt2: ArrayRef = Arc::new(Int64Array::from(vec![3000i64]));
        let value2: ArrayRef = Arc::new(Int64Array::from(vec![5i64]));
        let batch2 = RecordBatch::try_new(schema, vec![rt2, value2]).unwrap();
        assert_eq!(values(&over.update(&batch2), 0), vec![105]);
    }

    // PARTITION BY: each key has its own running SUM; rt ties within a key share the value.
    #[test]
    fn over_running_sum_per_partition_key() {
        let rt: ArrayRef = Arc::new(Int64Array::from(vec![0i64, 0, 1000, 1000, 2000]));
        let value: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 100, 20, 30, 40]));
        let key0: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 1, 1, 2]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("rt", DataType::Int64, false),
                Field::new("value", DataType::Int64, true),
                Field::new("key0", DataType::Int64, false),
            ])),
            vec![rt, value, key0],
        )
        .unwrap();
        let mut over = OverAggregator::new(0, vec![0]);
        // key 1: 10, then (20,30) tie -> 60, 60; key 2: 100, then 140.
        assert_eq!(values(&over.update(&batch), 0), vec![10, 100, 60, 60, 140]);
    }

    // The columnar (buffering) OVER passes input columns through and appends the running aggregate,
    // emitting only the rows the watermark has completed.
    #[test]
    fn over_window_buffers_and_passes_through() {
        let k: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 2, 1]));
        let v: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 100, 40]));
        // rowtime in nanoseconds (millis 0, 1000, 500, 9000).
        let rt: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            0i64,
            1_000_000_000,
            500_000_000,
            9_000_000_000,
        ]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("rt", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![k, v, rt]).unwrap();
        let mut over = OverWindowAggregator::new(0, vec![0], 2, Some(1), vec![0]);
        over.push(batch);
        // Watermark 2000ms completes the first three rows (rt 0/1000/500); the rt=9000 row stays.
        let out = over.flush(2000);
        assert_eq!(out.num_rows(), 3);
        assert_eq!(values(&out, 0), vec![1, 1, 2]); // k passed through
        assert_eq!(values(&out, 1), vec![10, 20, 100]); // v passed through
        // running SUM per key: key 1 -> 10, 30; key 2 -> 100 (result is the last column).
        assert_eq!(values(&out, 3), vec![10, 30, 100]);
        // The pending row flushes once the watermark passes it.
        let rest = over.flush(10_000);
        assert_eq!(rest.num_rows(), 1);
        assert_eq!(values(&rest, 1), vec![40]); // v
        assert_eq!(values(&rest, 3), vec![70]); // key 1 running sum 10+20+40
    }

    // Two-phase cumulative: per-slice SUM partials merge into the nested windows of their bucket.
    #[test]
    fn cumulative_two_phase_merges_nested_windows() {
        // max size 3 s, step 1 s, cumulative, bigint value, SUM.
        let mut agg = TumblingAggregator::new(3000, 1000, true, vec![0], vec![0]);
        let partial = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key0", DataType::Int64, false),
                Field::new("partial0", DataType::Int64, true),
                Field::new("slice_end", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 1, 1])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
                Arc::new(Int64Array::from(vec![1000i64, 2000, 3000])),
            ],
        )
        .unwrap();
        agg.update_partial(&partial);
        let out = agg.flush(3000);
        // Nested windows share the bucket start 0; each accumulates the slices up to its end:
        // (0,1000]=10, (0,2000]=10+20=30, (0,3000]=10+20+30=60.
        assert_eq!(values(&out, 1), vec![0, 0, 0]); // window_start
        assert_eq!(values(&out, 2), vec![1000, 2000, 3000]); // window_end
        assert_eq!(values(&out, 3), vec![10, 30, 60]); // running SUM
    }

    // A `[key0, value0, $row_kind$]` changelog batch (key/value bigint) for the GROUP BY tests;
    // `kinds` is the RowKind byte per row (0 +I, 1 -U, 2 +U, 3 -D).
    fn group_changelog(keys: Vec<i64>, values: Vec<Option<i64>>, kinds: Vec<i8>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key0", DataType::Int64, false),
                Field::new("value0", DataType::Int64, true),
                Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(values)),
                Arc::new(Int8Array::from(kinds)),
            ],
        )
        .unwrap()
    }

    // All-INSERT convenience for the append-only tests.
    fn group_batch(keys: Vec<i64>, values: Vec<i64>) -> RecordBatch {
        let kinds = vec![0i8; keys.len()];
        group_changelog(keys, values.into_iter().map(Some).collect(), kinds)
    }

    fn row_kinds(batch: &RecordBatch) -> Vec<i8> {
        batch
            .column_by_name(ROW_KIND_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    // GROUP BY changelog: a key's first row emits INSERT(0); a later row that changes the result
    // emits UPDATE_BEFORE(1)+UPDATE_AFTER(2); a row that leaves the result unchanged emits nothing.
    #[test]
    fn group_by_emits_insert_then_update_changelog() {
        // SUM(bigint) over value column 1, grouping on key column 0, emitting -U.
        let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
        // keys a,a,b,a with values 1,2,5,0 — the last adds 0, leaving a's sum at 3 (suppressed).
        let out = agg.update(&group_batch(vec![1, 1, 2, 1], vec![1, 2, 5, 0]));
        assert_eq!(row_kinds(&out), vec![0, 1, 2, 0]);
        assert_eq!(values(&out, 0), vec![1, 1, 1, 2]); // key
        assert_eq!(values(&out, 1), vec![1, 1, 3, 5]); // running sum (prev on -U, new on +U)
    }

    // COUNT(*) (no argument column) counts every row, alongside a SUM over a value column.
    #[test]
    fn group_by_counts_every_row_for_count_star() {
        // kinds COUNT(*), SUM; COUNT(*) has no column (-1), SUM reads column 1; group on column 0.
        let mut agg = GroupAggregator::new(vec![3, 0], vec![0, 0], vec![-1, 1], vec![0], true);
        let out = agg.update(&group_batch(vec![1, 1], vec![10, 5]));
        assert_eq!(row_kinds(&out), vec![0, 1, 2]); // +I, then -U/+U
        assert_eq!(values(&out, 1), vec![1, 1, 2]); // COUNT(*): 1, then 1->2
        assert_eq!(values(&out, 2), vec![10, 10, 15]); // SUM: 10, then 10->15
    }

    // A columnar input from an insert-only producer has no `$row_kind$` column; every row is then an
    // INSERT (so the GROUP BY still emits its +I / -U / +U changelog).
    #[test]
    fn group_by_treats_absent_row_kind_as_insert() {
        let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key0", DataType::Int64, false),
                Field::new("value0", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 1])),
                Arc::new(Int64Array::from(vec![10i64, 20])),
            ],
        )
        .unwrap();
        let out = agg.update(&batch);
        assert_eq!(row_kinds(&out), vec![0, 1, 2]); // +I(10); -U(10)/+U(30)
        assert_eq!(values(&out, 1), vec![10, 10, 30]);
    }

    // With the host's update-before flag off, an update emits only the UPDATE_AFTER row.
    #[test]
    fn group_by_omits_update_before_when_disabled() {
        let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], false);
        let out = agg.update(&group_batch(vec![1, 1], vec![10, 5]));
        assert_eq!(row_kinds(&out), vec![0, 2]); // +I(10), +U(15)
        assert_eq!(values(&out, 1), vec![10, 15]);
    }

    // A checkpoint preserves per-key state: a restored key is not "first", so a new row updates
    // rather than re-inserting.
    #[test]
    fn group_by_survives_snapshot_restore() {
        let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
        agg.update(&group_batch(vec![1], vec![10]));
        let snapshot = agg.snapshot();
        let mut restored =
            GroupAggregator::restore(vec![0], vec![0], vec![1], vec![0], true, &snapshot);
        let out = restored.update(&group_batch(vec![1], vec![5]));
        assert_eq!(row_kinds(&out), vec![1, 2]); // -U(10), +U(15) — continues from 10
        assert_eq!(values(&out, 1), vec![10, 15]);
    }

    // Consuming a changelog: a -U input retracts a prior value, updating the running SUM.
    #[test]
    fn group_by_retracts_changelog_input() {
        let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
        // +I 10, +I 20 (sum 30), then -U 10 (retract -> sum 20), all key 1.
        let out = agg.update(&group_changelog(
            vec![1, 1, 1],
            vec![Some(10), Some(20), Some(10)],
            vec![0, 0, 1],
        ));
        assert_eq!(row_kinds(&out), vec![0, 1, 2, 1, 2]);
        assert_eq!(values(&out, 1), vec![10, 10, 30, 30, 20]); // +I10; -U10/+U30; -U30/+U20
    }

    // Retracting a key's last record empties the group and emits a DELETE.
    #[test]
    fn group_by_deletes_when_last_record_retracted() {
        let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
        let out = agg.update(&group_changelog(vec![1, 1], vec![Some(10), Some(10)], vec![0, 3]));
        assert_eq!(row_kinds(&out), vec![0, 3]); // +I(10), then -D(10)
        assert_eq!(values(&out, 1), vec![10, 10]);
    }

    // A SUM reports NULL once its last non-null value is retracted while a null-valued row keeps the
    // group alive — matching the host's sum-with-retract.
    #[test]
    fn group_by_sum_is_null_after_last_value_retracted() {
        let mut agg = GroupAggregator::new(vec![0], vec![0], vec![1], vec![0], true);
        // +I 5, +I NULL (sum still 5, suppressed), -U 5 (no non-null left -> SUM NULL, group alive).
        let out = agg.update(&group_changelog(
            vec![1, 1, 1],
            vec![Some(5), None, Some(5)],
            vec![0, 0, 1],
        ));
        assert_eq!(row_kinds(&out), vec![0, 1, 2]); // +I(5); -U(5)/+U(NULL)
        let result = out.column(1);
        assert_eq!(result.len(), 3);
        assert!(!result.is_null(0) && result.as_any().downcast_ref::<Int64Array>().unwrap().value(0) == 5);
        assert!(result.is_null(2)); // the +U carries a NULL sum
    }

    // MIN over a changelog: retracting the current minimum reveals the next-smallest from the
    // per-key value multiset (what a single running value could not do).
    #[test]
    fn group_by_min_recovers_next_after_retract() {
        // kind MIN (1) over value column 1, group on column 0, emit -U.
        let mut agg = GroupAggregator::new(vec![1], vec![0], vec![1], vec![0], true);
        // +I 5, +I 3, +I 8 (min 3), then -U 3 (min back to 5).
        let out = agg.update(&group_changelog(
            vec![1, 1, 1, 1],
            vec![Some(5), Some(3), Some(8), Some(3)],
            vec![0, 0, 0, 1],
        ));
        assert_eq!(row_kinds(&out), vec![0, 1, 2, 1, 2]);
        // min: 5; 5->3; (8 leaves min 3, suppressed); 3->5 after retracting the 3.
        assert_eq!(values(&out, 1), vec![5, 5, 3, 3, 5]);
    }

    // The MIN/MAX value multiset survives a checkpoint, so a post-restore retract still recovers the
    // next extreme.
    #[test]
    fn group_by_min_multiset_survives_snapshot_restore() {
        let mut agg = GroupAggregator::new(vec![1], vec![0], vec![1], vec![0], true);
        agg.update(&group_changelog(vec![1, 1], vec![Some(5), Some(3)], vec![0, 0])); // min 3
        let snapshot = agg.snapshot();
        let mut restored =
            GroupAggregator::restore(vec![1], vec![0], vec![1], vec![0], true, &snapshot);
        // Retract the 3 — the restored multiset still holds the 5, so the min becomes 5.
        let out = restored.update(&group_changelog(vec![1], vec![Some(3)], vec![1]));
        assert_eq!(row_kinds(&out), vec![1, 2]); // -U(3), +U(5)
        assert_eq!(values(&out, 1), vec![3, 5]);
    }

    // A `[p, s, $row_kind$]` insert-only batch (partition p at col 0, sort key s at col 1) for the
    // Top-N tests.
    fn topn_batch(p: Vec<i64>, s: Vec<i64>) -> RecordBatch {
        let kinds = vec![0i8; p.len()];
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("p", DataType::Int64, false),
                Field::new("s", DataType::Int64, true),
                Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(p)),
                Arc::new(Int64Array::from(s)),
                Arc::new(Int8Array::from(kinds)),
            ],
        )
        .unwrap()
    }

    fn asc(index: usize) -> SortColumn {
        SortColumn { index, ascending: true, nulls_first: false }
    }

    // Top-2 by ascending sort key, one partition: a row entering the top-2 inserts and displaces the
    // current 2nd (a DELETE); a row that would rank 3rd emits nothing.
    #[test]
    fn topn_keeps_smallest_n_per_partition() {
        // partition col 0, ORDER BY col 1 ASC, limit 2.
        let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2);
        // s = 5, 3, 8, 1 for partition 1.
        let out = ranker.push(&topn_batch(vec![1, 1, 1, 1], vec![5, 3, 8, 1]));
        // 5: +I5. 3: +I3 (top2 = {3,5}). 8: rank 3 -> nothing. 1: +I1, -D5 (top2 = {1,3}).
        assert_eq!(row_kinds(&out), vec![0, 0, 3, 0]);
        assert_eq!(values(&out, 1), vec![5, 3, 5, 1]); // the sort-key column of each emitted row
    }

    // Partitions are independent: each keeps its own top-N.
    #[test]
    fn topn_is_per_partition() {
        let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 1);
        let out = ranker.push(&topn_batch(vec![1, 2, 1], vec![5, 7, 3]));
        // p1: +I5; p2: +I7; p1 sees 3 < 5 -> -D5 then +I3 (delete first, as the host emits).
        assert_eq!(row_kinds(&out), vec![0, 0, 3, 0]);
        assert_eq!(values(&out, 0), vec![1, 2, 1, 1]); // partition of each emitted row
        assert_eq!(values(&out, 1), vec![5, 7, 5, 3]);
    }

    // The bounded buffer survives a checkpoint, so post-restore ranking continues correctly.
    #[test]
    fn topn_buffer_survives_snapshot_restore() {
        let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2);
        ranker.push(&topn_batch(vec![1, 1], vec![5, 3])); // top2 = {3, 5}
        let snapshot = ranker.snapshot();
        let mut restored = TopNRanker::restore(vec![0], vec![asc(1)], 2, &snapshot);
        // A 1 enters the restored top-2 and displaces the 5.
        let out = restored.push(&topn_batch(vec![1], vec![1]));
        assert_eq!(row_kinds(&out), vec![3, 0]); // -D5, +I1
        assert_eq!(values(&out, 1), vec![5, 1]);
    }

    // A `[k, v, $row_kind$]` changelog batch (k join key at col 0) for the updating-join tests.
    fn changelog_join_batch(k: Vec<i64>, v: Vec<i64>, kinds: Vec<i8>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, true),
                Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(k)),
                Arc::new(Int64Array::from(v)),
                Arc::new(Int8Array::from(kinds)),
            ],
        )
        .unwrap()
    }

    // INNER updating join on column 0: a matched pair is emitted when the second side's row arrives,
    // carrying the arriving row's kind; the output is left columns then right columns.
    #[test]
    fn updating_join_emits_matches_with_arriving_kind() {
        let mut joiner = UpdatingJoiner::new(vec![0], vec![0]);
        // Buffer a left row (k=1, v=10); no right yet, so nothing emits.
        assert_eq!(
            joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true).num_rows(),
            0
        );
        // A right row (k=1, v=100) matches it: emit +I (left ++ right).
        let out = joiner.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false);
        assert_eq!(row_kinds(&out), vec![0]);
        assert_eq!(values(&out, 0), vec![1]); // left k
        assert_eq!(values(&out, 1), vec![10]); // left v
        assert_eq!(values(&out, 2), vec![1]); // right k
        assert_eq!(values(&out, 3), vec![100]); // right v
        // Retracting the left row emits the matching pair as a retraction.
        let retract = joiner.push(&changelog_join_batch(vec![1], vec![10], vec![3]), true);
        assert_eq!(row_kinds(&retract), vec![3]); // -D
        assert_eq!(values(&retract, 1), vec![10]);
        assert_eq!(values(&retract, 3), vec![100]);
    }

    // A left row matches every buffered right row of its key (cartesian per key); different keys
    // never match.
    #[test]
    fn updating_join_is_cartesian_per_key() {
        let mut joiner = UpdatingJoiner::new(vec![0], vec![0]);
        joiner.push(&changelog_join_batch(vec![1, 1, 2], vec![100, 200, 300], vec![0, 0, 0]), false);
        let out = joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true);
        assert_eq!(out.num_rows(), 2); // matches both k=1 right rows, not the k=2 one
        let mut right_vs = values(&out, 3);
        right_vs.sort();
        assert_eq!(right_vs, vec![100, 200]);
    }

    // A null join key never matches (INNER `a.k = b.k` null semantics): the row is neither joined
    // nor stored.
    #[test]
    fn updating_join_drops_null_keys() {
        let mut joiner = UpdatingJoiner::new(vec![0], vec![0]);
        // A right row with a null key, then a left row with a null key — no match either way.
        let right = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
                Field::new(ROW_KIND_COLUMN, DataType::Int8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![None, Some(1)])),
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(Int8Array::from(vec![0, 0])),
            ],
        )
        .unwrap();
        joiner.push(&right, false);
        // Left null key matches nothing; left key=1 matches the stored right (1, 200).
        let left = RecordBatch::try_new(
            right.schema(),
            vec![
                Arc::new(Int64Array::from(vec![None, Some(1)])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int8Array::from(vec![0, 0])),
            ],
        )
        .unwrap();
        let out = joiner.push(&left, true);
        assert_eq!(out.num_rows(), 1); // only key=1 pair, not the null-key rows
        assert_eq!(values(&out, 1), vec![20]); // left v
        assert_eq!(values(&out, 3), vec![200]); // right v
    }

    // The per-side multiset survives a checkpoint, so a post-restore arrival still finds its match.
    #[test]
    fn updating_join_state_survives_snapshot_restore() {
        let mut joiner = UpdatingJoiner::new(vec![0], vec![0]);
        joiner.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false); // buffer right
        let snapshot = joiner.snapshot();
        let mut restored = UpdatingJoiner::restore(vec![0], vec![0], &snapshot);
        let out = restored.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true);
        assert_eq!(out.num_rows(), 1);
        assert_eq!(values(&out, 1), vec![10]);
        assert_eq!(values(&out, 3), vec![100]);
    }

    // A `[k, v, rt]` batch with int64 rowtime (epoch millis) for the interval-join tests.
    fn join_batch(k: Vec<i64>, v: Vec<i64>, rt: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, true),
                Field::new("rt", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(k)),
                Arc::new(Int64Array::from(v)),
                Arc::new(Int64Array::from(rt)),
            ],
        )
        .unwrap()
    }

    // INNER interval join: a left row matches a buffered right row of the same key whose rowtime is
    // within [rt + lower, rt + upper]; output columns are left ++ right.
    #[test]
    fn interval_join_emits_matched_pairs() {
        // a.rt BETWEEN b.rt - 1000 AND b.rt + 1000, single equi-key on column 0, rt is column 2.
        let mut joiner = IntervalJoiner::new(vec![0], vec![0], 2, 2, -1000, 1000);
        // Buffer two right rows for key 1 (rt 5500 in range of left 5000, rt 7000 out of range).
        assert_eq!(joiner.push_right(join_batch(vec![1, 1], vec![100, 200], vec![5500, 7000])).num_rows(), 0);
        // A left row (k=1, rt=5000): matches the rt=5500 right row only (delta -500 in [-1000,1000]).
        let out = joiner.push_left(join_batch(vec![1], vec![10], vec![5000]));
        assert_eq!(out.num_rows(), 1);
        assert_eq!(values(&out, 0), vec![1]); // left k
        assert_eq!(values(&out, 1), vec![10]); // left v
        assert_eq!(values(&out, 2), vec![5000]); // left rt
        assert_eq!(values(&out, 3), vec![1]); // right k
        assert_eq!(values(&out, 4), vec![100]); // right v
        assert_eq!(values(&out, 5), vec![5500]); // right rt
    }

    // Different keys never match, and a pair is emitted once — when its second side arrives —
    // regardless of which side arrived first.
    #[test]
    fn interval_join_matches_on_key_and_emits_once() {
        let mut joiner = IntervalJoiner::new(vec![0], vec![0], 2, 2, -1000, 1000);
        // Left first: buffer a left row, no right yet.
        assert_eq!(joiner.push_left(join_batch(vec![1], vec![10], vec![5000])).num_rows(), 0);
        // A right row with a different key does not match.
        assert_eq!(joiner.push_right(join_batch(vec![2], vec![100], vec![5000])).num_rows(), 0);
        // A matching right row emits the pair exactly once.
        let out = joiner.push_right(join_batch(vec![1], vec![100], vec![5500]));
        assert_eq!(out.num_rows(), 1);
        assert_eq!(values(&out, 1), vec![10]);
        assert_eq!(values(&out, 4), vec![100]);
    }

    // The watermark evicts rows past their last useful rowtime, so a later arrival can no longer
    // match an evicted row.
    #[test]
    fn interval_join_evicts_dead_rows_on_watermark() {
        let mut joiner = IntervalJoiner::new(vec![0], vec![0], 2, 2, -1000, 1000);
        joiner.push_left(join_batch(vec![1], vec![10], vec![5000]));
        // Watermark 6000: left.rt - lower = 5000 - (-1000) = 6000, not > 6000, so the row is evicted.
        joiner.advance(6000);
        // A right row that would otherwise match (delta -500) finds nothing buffered.
        assert_eq!(joiner.push_right(join_batch(vec![1], vec![100], vec![5500])).num_rows(), 0);
    }

    // A `[k, v, window_start, window_end]` batch (window bounds as int64 millis) for window-join tests.
    fn window_batch(k: Vec<i64>, v: Vec<i64>, ws: Vec<i64>, we: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, true),
                Field::new("window_start", DataType::Int64, false),
                Field::new("window_end", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(k)),
                Arc::new(Int64Array::from(v)),
                Arc::new(Int64Array::from(ws)),
                Arc::new(Int64Array::from(we)),
            ],
        )
        .unwrap()
    }

    // The matched (left v, right v) pairs of a join output, sorted (the hash join does not promise
    // an output order; parity is over the result set).
    fn left_right_values(batch: &RecordBatch) -> Vec<(i64, i64)> {
        let mut pairs: Vec<(i64, i64)> =
            values(batch, 1).into_iter().zip(values(batch, 5)).collect();
        pairs.sort_unstable();
        pairs
    }

    // INNER window join: left and right rows of the same key in the same window join (their cross
    // product) once the watermark closes the window; other windows/keys do not match.
    #[test]
    fn window_join_emits_matches_when_window_closes() {
        // keys col 0; window_start col 2, window_end col 3 on both sides.
        let mut joiner = WindowJoiner::new(vec![0], vec![0], 2, 3, 2, 3);
        // Window [0,1000): left k=1 (two rows) and k=2; right k=1 and k=3.
        joiner.push_left(window_batch(vec![1, 1, 2], vec![10, 11, 20], vec![0, 0, 0], vec![1000, 1000, 1000]));
        joiner.push_right(window_batch(vec![1, 3], vec![100, 300], vec![0, 0], vec![1000, 1000]));
        // A later window [1000,2000) for k=1 on both sides (should not mix with [0,1000)).
        joiner.push_left(window_batch(vec![1], vec![40], vec![1000], vec![2000]));
        joiner.push_right(window_batch(vec![1], vec![400], vec![1000], vec![2000]));

        // Watermark 1000 closes only [0,1000): k=1 matches (2 left × 1 right = 2 rows), k=2/k=3 don't.
        let out = joiner.flush(1000);
        assert_eq!(left_right_values(&out), vec![(10, 100), (11, 100)]);

        // Watermark 2000 closes [1000,2000): k=1 matches once.
        let rest = joiner.flush(2000);
        assert_eq!(left_right_values(&rest), vec![(40, 400)]);
    }

    // Buffered window-join rows survive a snapshot/restore round trip.
    #[test]
    fn window_join_restores_buffered_rows() {
        let mut joiner = WindowJoiner::new(vec![0], vec![0], 2, 3, 2, 3);
        joiner.push_left(window_batch(vec![1], vec![10], vec![0], vec![1000]));
        joiner.push_right(window_batch(vec![1], vec![100], vec![0], vec![1000]));
        let snapshot = joiner.snapshot();
        let mut restored = WindowJoiner::restore(vec![0], vec![0], 2, 3, 2, 3, &snapshot);
        let out = restored.flush(1000);
        assert_eq!(left_right_values(&out), vec![(10, 100)]);
    }

    // Buffered rows survive a snapshot/restore round trip and still match afterward.
    #[test]
    fn interval_join_restores_buffered_rows() {
        let mut joiner = IntervalJoiner::new(vec![0], vec![0], 2, 2, -1000, 1000);
        joiner.push_right(join_batch(vec![1], vec![100], vec![5500]));
        let snapshot = joiner.snapshot();
        let mut restored =
            IntervalJoiner::restore(vec![0], vec![0], 2, 2, -1000, 1000, &snapshot);
        let out = restored.push_left(join_batch(vec![1], vec![10], vec![5000]));
        assert_eq!(out.num_rows(), 1);
        assert_eq!(values(&out, 4), vec![100]);
    }

    // ROW_NUMBER over (PARTITION BY key0 ORDER BY rt): a per-key counter in rowtime order, surviving
    // across update calls (the unbounded frame).
    #[test]
    fn window_function_row_number_counts_per_key() {
        let batch = |rt: Vec<i64>, key0: Vec<i64>| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("rt", DataType::Int64, false),
                    Field::new("key0", DataType::Int64, false),
                ])),
                vec![Arc::new(Int64Array::from(rt)), Arc::new(Int64Array::from(key0))],
            )
            .unwrap()
        };
        let mut over = WindowFunctionOver::new(vec![10]); // ROW_NUMBER
        // Out of rowtime order within the batch: ROW_NUMBER follows rowtime, emitted in input order.
        assert_eq!(values(&over.update(&batch(vec![0, 1000, 0], vec![1, 1, 2])), 0), vec![1, 2, 1]);
        // The counter continues per key across calls.
        assert_eq!(values(&over.update(&batch(vec![2000, 1000], vec![1, 2])), 0), vec![3, 2]);
    }

    // RANK and DENSE_RANK over (ORDER BY rt): tied rowtimes share a rank; RANK leaves gaps after a
    // tie (next jumps to the row position), DENSE_RANK does not.
    #[test]
    fn window_function_rank_and_dense_rank_handle_ties() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("rt", DataType::Int64, false),
                Field::new("key0", DataType::Int64, false),
            ])),
            // One key, rowtimes 10, 10 (tie), 20, 30.
            vec![
                Arc::new(Int64Array::from(vec![10i64, 10, 20, 30])),
                Arc::new(Int64Array::from(vec![1i64, 1, 1, 1])),
            ],
        )
        .unwrap();
        let mut rank = WindowFunctionOver::new(vec![11]); // RANK
        assert_eq!(values(&rank.update(&batch), 0), vec![1, 1, 3, 4]);
        let mut dense = WindowFunctionOver::new(vec![12]); // DENSE_RANK
        assert_eq!(values(&dense.update(&batch), 0), vec![1, 1, 2, 3]);
    }

    // Decoder over the pre-order encoding: CALL gt ( INPUT_REF a , LIT_LONG 5 ).
    #[test]
    fn filters_column_greater_than_literal() {
        let mut expression = FilterExpression {
            kinds: vec![6, 0, 1],
            payload: vec![10, 0, 0],
            child_counts: vec![2, 0, 0],
            longs: vec![5],
            doubles: vec![],
            strings: vec![],
            compiled: None,
        };
        let out = expression.filter(sample_batch());
        assert_eq!(values(&out, 0), vec![6, 9]);
    }

    // Arithmetic inside the predicate: CALL gt ( CALL plus ( INPUT_REF a , INPUT_REF b ) , LIT 10 ).
    #[test]
    fn filters_arithmetic_predicate() {
        let mut expression = FilterExpression {
            kinds: vec![6, 6, 0, 0, 1],
            payload: vec![10, 0, 0, 1, 0],
            child_counts: vec![2, 2, 0, 0, 0],
            longs: vec![10],
            doubles: vec![],
            strings: vec![],
            compiled: None,
        };
        let out = expression.filter(sample_batch());
        assert_eq!(values(&out, 0), vec![1, 3, 9]);
    }

    // An int32 literal keeps the arithmetic in int32, so `v * 2` wraps on overflow like the host
    // rather than widening: CALL gt ( CALL times ( INPUT_REF v , LIT_INT 2 ) , LIT_INT 50 ).
    #[test]
    fn integer_arithmetic_wraps_in_declared_width() {
        let v: ArrayRef = Arc::new(Int32Array::from(vec![30i32, 2_000_000_000]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)])),
            vec![v],
        )
        .unwrap();
        let mut expression = FilterExpression {
            kinds: vec![6, 6, 0, 7, 7],
            payload: vec![10, 2, 0, 0, 1],
            child_counts: vec![2, 2, 0, 0, 0],
            longs: vec![2, 50],
            doubles: vec![],
            strings: vec![],
            compiled: None,
        };
        let out = expression.filter(batch);
        let kept = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        // 30*2=60 > 50 keeps 30; 2e9*2 overflows int32 to a negative value, excluded.
        assert_eq!(kept.values(), &[30]);
    }

    // The native sink writes a batch to Parquet; reading it back yields the same rows.
    #[test]
    fn writes_and_reads_parquet() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let batch = sample_batch();
        let path = std::env::temp_dir().join("streamfusion_parquet_roundtrip.parquet");
        let path = path.to_str().unwrap();
        write_parquet(&batch, path);

        let file = std::fs::File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
        let mut rows = 0usize;
        let mut first = Vec::new();
        for read in reader {
            let read = read.unwrap();
            let column = read.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            first.extend_from_slice(column.values());
            rows += read.num_rows();
        }
        assert_eq!(rows, batch.num_rows());
        assert_eq!(first, values(&batch, 0));
    }

    // Pure-native read→write ceiling: time copying a directory of Parquet with no JVM in the loop.
    // Run with: cargo test -- --ignored --nocapture profile_parquet_copy
    #[test]
    #[ignore]
    fn profile_parquet_copy() {
        let rows: i64 = 5_000_000;
        let k: ArrayRef = Arc::new(Int64Array::from((0..rows).collect::<Vec<i64>>()));
        let v: ArrayRef = Arc::new(Int64Array::from((0..rows).map(|i| i % 100).collect::<Vec<i64>>()));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
            ])),
            vec![k, v],
        )
        .unwrap();
        let in_dir = std::env::temp_dir().join("sf_profile_in");
        let out_dir = std::env::temp_dir().join("sf_profile_out");
        let _ = std::fs::remove_dir_all(&in_dir);
        let _ = std::fs::remove_dir_all(&out_dir);
        std::fs::create_dir_all(&in_dir).unwrap();
        std::fs::create_dir_all(&out_dir).unwrap();
        write_parquet(&batch, in_dir.join("part-0.parquet").to_str().unwrap());

        let start = std::time::Instant::now();
        let mut source = ParquetSource::open(in_dir.to_str().unwrap(), vec![], 0, 1);
        let mut i = 0;
        let mut read = std::time::Duration::ZERO;
        let mut write = std::time::Duration::ZERO;
        loop {
            let t = std::time::Instant::now();
            let next = source.next();
            read += t.elapsed();
            match next {
                Some(b) => {
                    let t = std::time::Instant::now();
                    write_parquet(&b, out_dir.join(format!("part-{i}.parquet")).to_str().unwrap());
                    write += t.elapsed();
                    i += 1;
                }
                None => break,
            }
        }
        let elapsed = start.elapsed();
        eprintln!(
            "native copy: {} rows, {} batches in {:?} ({:.2} M rows/s); read {:?}, write {:?}",
            rows,
            i,
            elapsed,
            rows as f64 / elapsed.as_secs_f64() / 1e6,
            read,
            write
        );
    }

    fn ab_batch() -> RecordBatch {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30]));
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Int64, true),
            ])),
            vec![a, b],
        )
        .unwrap()
    }

    // A Calc with no condition projects computed columns: [a + b, a].
    #[test]
    fn calc_projects_computed_columns() {
        let mut calc = CalcExpression {
            kinds: vec![6, 0, 0, 0],
            payload: vec![0, 0, 1, 0], // CALL(+), col a, col b; col a
            child_counts: vec![2, 0, 0, 0],
            longs: vec![],
            doubles: vec![],
            strings: vec![],
            projection_roots: vec![0, 3],
            condition_root: -1,
            output_names: vec!["sum".to_string(), "a".to_string()],
            compiled: None,
        };
        let out = calc.evaluate(ab_batch());
        assert_eq!(out.schema().field(0).name(), "sum");
        assert_eq!(out.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values(), &[11, 22, 33]);
        assert_eq!(out.column(1).as_any().downcast_ref::<Int64Array>().unwrap().values(), &[1, 2, 3]);
    }

    // A Calc filters by the condition (a > 2), then projects the survivors.
    #[test]
    fn calc_filters_then_projects() {
        let mut calc = CalcExpression {
            kinds: vec![6, 0, 1, 0],
            payload: vec![10, 0, 0, 0], // CALL(>), col a, lit; col a
            child_counts: vec![2, 0, 0, 0],
            longs: vec![2],
            doubles: vec![],
            strings: vec![],
            projection_roots: vec![3],
            condition_root: 0,
            output_names: vec!["a".to_string()],
            compiled: None,
        };
        let out = calc.evaluate(ab_batch());
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values(), &[3]);
    }

    // The by-key split sends every row with the same key to the same partition and preserves all
    // rows, for any partition count.
    #[test]
    fn partitions_a_batch_by_key() {
        use std::collections::HashMap;
        let n = 1000usize;
        let key: ArrayRef = Arc::new(Int64Array::from((0..n as i64).map(|i| i % 37).collect::<Vec<_>>()));
        let value: ArrayRef = Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>()));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
            ])),
            vec![key, value],
        )
        .unwrap();

        for num_partitions in [1usize, 3, 8] {
            let parts = partition_batch(&batch, &[0], num_partitions);
            let mut rows = 0usize;
            let mut key_to_partition: HashMap<i64, usize> = HashMap::new();
            for (partition, sub) in &parts {
                assert!(*partition < num_partitions);
                let keys = sub.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..sub.num_rows() {
                    // Each key is consistently assigned to one partition.
                    let prev = key_to_partition.insert(keys.value(i), *partition);
                    if let Some(p) = prev {
                        assert_eq!(p, *partition, "key {} split across partitions", keys.value(i));
                    }
                }
                rows += sub.num_rows();
            }
            assert_eq!(rows, n, "all rows preserved for {num_partitions} partitions");
        }
    }

    // The Parquet source reads back every batch written across a directory's files.
    #[test]
    fn reads_a_parquet_directory() {
        let dir = std::env::temp_dir().join("streamfusion_source_dir");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        write_parquet(&sample_batch(), dir.join("part-0.parquet").to_str().unwrap());
        write_parquet(&sample_batch(), dir.join("part-1.parquet").to_str().unwrap());

        let mut source = ParquetSource::open(dir.to_str().unwrap(), vec![], 0, 1);
        let mut rows = 0usize;
        let mut batches = 0usize;
        while let Some(batch) = source.next() {
            rows += batch.num_rows();
            batches += 1;
        }
        assert_eq!(rows, 2 * sample_batch().num_rows());
        assert!(batches >= 2);
    }

    // The compiled predicate is cached after the first batch and reused.
    #[test]
    fn compiles_once_and_reuses() {
        let mut expression = FilterExpression {
            kinds: vec![6, 0, 1],
            payload: vec![12, 0, 0],
            child_counts: vec![2, 0, 0],
            longs: vec![5],
            doubles: vec![],
            strings: vec![],
            compiled: None,
        };
        let first = expression.filter(sample_batch());
        assert!(expression.compiled.is_some());
        let second = expression.filter(sample_batch());
        assert_eq!(values(&first, 0), values(&second, 0));
        assert_eq!(values(&first, 0), vec![1, 3]);
    }
}

