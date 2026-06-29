use arrow::array::{
    make_array, new_empty_array, new_null_array, Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array,
    Float32Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, ListArray, MapArray,
    RecordBatch, StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, UInt32Array,
};
use arrow::compute::{concat_batches, filter_record_batch, take};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
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
use std::collections::{BTreeMap, HashMap, HashSet};
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

/// Takes ownership of a schema the JVM exported through the C Data Interface (schema only, no data),
/// swapping in a released placeholder so the producer's release callback fires once.
fn import_schema(schema_address: jlong) -> SchemaRef {
    let ffi_schema = unsafe {
        std::ptr::replace(schema_address as *mut FFI_ArrowSchema, FFI_ArrowSchema::empty())
    };
    Arc::new(Schema::try_from(&ffi_schema).expect("failed to import Arrow schema"))
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
fn null_scalar(data_type: &DataType) -> ScalarValue {
    ScalarValue::try_from(data_type).expect("null scalar of type")
}

/// Every aligned window a timestamp (millis) belongs to, as (start, end) millis pairs, appended to
/// `windows` (cleared first) so the caller can reuse one buffer. Tumbling yields one window; hopping
/// yields the `size / slide` overlapping windows; cumulative yields the nested windows
/// `[base, base + k*step)` whose end is past the timestamp, all sharing the bucket start. Shared by
/// the window aggregate and the windowing TVF so their assignment is byte-for-byte identical.
fn windows_for(
    timestamp: i64,
    window_millis: i64,
    slide_millis: i64,
    cumulative: bool,
    windows: &mut Vec<(i64, i64)>,
) {
    windows.clear();
    if cumulative {
        let base = timestamp - timestamp.rem_euclid(window_millis);
        let mut end = base + slide_millis;
        while end <= base + window_millis {
            if end > timestamp {
                windows.push((base, end));
            }
            end += slide_millis;
        }
    } else {
        let mut start = timestamp - timestamp.rem_euclid(slide_millis);
        while start + window_millis > timestamp {
            windows.push((start, start + window_millis));
            start -= slide_millis;
        }
    }
}

/// Stateless windowing table function (Flink's `WindowTableFunctionOperator`): assigns each input
/// row to its window(s) and emits the input columns — fanned out, one copy per window for
/// hopping/cumulative — with `window_start`, `window_end`, and `window_time` (= `window_end - 1ms`)
/// appended. The downstream window join/aggregate does the event-time buffering; this is a pure
/// per-row map, so watermarks pass straight through (the wrapper operator forwards them).
///
/// Timestamps are nanosecond / no time zone — the unit `ArrowConversion` pins both `TIMESTAMP` and
/// `TIMESTAMP_LTZ` to — while the window math runs in millis, identical to the window aggregate
/// (shared [`windows_for`]). A trailing `$row_kind$` changelog tag stays the last output column, so
/// the window columns land at the input-column count (the indices the join/aggregate expect).
fn assign_windows(
    input: &RecordBatch,
    time_col: usize,
    window_millis: i64,
    slide_millis: i64,
    cumulative: bool,
) -> RecordBatch {
    let schema = input.schema();
    let row_kind_idx = schema.fields().iter().position(|f| f.name() == ROW_KIND_COLUMN);
    let data_end = row_kind_idx.unwrap_or_else(|| schema.fields().len());

    let times = input
        .column(time_col)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("windowing TVF time column must be timestamp(ns)");

    // One take index per output row (the input row it copies), plus that row's window bounds in nanos.
    let mut take_indices: Vec<u32> = Vec::with_capacity(input.num_rows());
    let mut starts: Vec<i64> = Vec::new();
    let mut ends: Vec<i64> = Vec::new();
    let mut windows: Vec<(i64, i64)> = Vec::new();
    for row in 0..input.num_rows() {
        windows_for(times.value(row) / 1_000_000, window_millis, slide_millis, cumulative, &mut windows);
        for &(start, end) in &windows {
            take_indices.push(row as u32);
            starts.push(start * 1_000_000);
            ends.push(end * 1_000_000);
        }
    }
    let indices = UInt32Array::from(take_indices);
    let timestamp = DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None);

    let mut fields: Vec<Field> = Vec::with_capacity(data_end + 4);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(data_end + 4);
    for i in 0..data_end {
        fields.push(schema.field(i).as_ref().clone());
        columns.push(take(input.column(i), &indices, None).expect("failed to fan out column"));
    }
    let window_time: Vec<i64> = ends.iter().map(|end| end - 1_000_000).collect();
    for name in ["window_start", "window_end", "window_time"] {
        fields.push(Field::new(name, timestamp.clone(), false));
    }
    columns.push(Arc::new(TimestampNanosecondArray::from(starts)));
    columns.push(Arc::new(TimestampNanosecondArray::from(ends)));
    columns.push(Arc::new(TimestampNanosecondArray::from(window_time)));
    if let Some(idx) = row_kind_idx {
        fields.push(schema.field(idx).as_ref().clone());
        columns.push(take(input.column(idx), &indices, None).expect("failed to fan out row kind"));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("failed to build TVF batch")
}

/// Stateless GROUPING SETS / CUBE / ROLLUP expansion (Flink's `ExpandFunction`): each input row is
/// fanned out to `num_expand_rows` output rows, one per grouping set. Per output column `c` and
/// expand row `r`, `copy_indices[r*num_out_cols + c]` is either the input column index to copy
/// (an `InputRef` cell) or `-1` for a literal — the expand-id column (`expand_id_index`) takes the
/// per-row grouping id `expand_id_values[r]` (Int32 or Int64), every other literal cell is a typed
/// NULL (a grouped-out key). Built block by block (all input rows for expand row 0, then expand row
/// 1, …) and concatenated; the host downstream GROUP BY is order-insensitive, so this multiset
/// matches Flink's per-input-row interleaving. The `$row_kind$` tag rides through (repeated per
/// block), so the expansion is changelog-transparent.
fn expand(
    input: &RecordBatch,
    num_expand_rows: usize,
    num_out_cols: usize,
    expand_id_index: usize,
    expand_id_is_long: bool,
    copy_indices: &[i64],
    expand_id_values: &[i64],
) -> RecordBatch {
    let schema = input.schema();
    let row_kind_idx = schema.fields().iter().position(|f| f.name() == ROW_KIND_COLUMN);
    let n = input.num_rows();
    let id_type = if expand_id_is_long { DataType::Int64 } else { DataType::Int32 };

    // Each non-expand-id output column is an InputRef in at least one expand row (a grouped-out key
    // is NULL elsewhere but InputRef where it is grouped-in); take its type/name from that copy row.
    let mut out_types: Vec<DataType> = Vec::with_capacity(num_out_cols);
    let mut out_names: Vec<String> = Vec::with_capacity(num_out_cols);
    for c in 0..num_out_cols {
        if c == expand_id_index {
            out_types.push(id_type.clone());
            out_names.push("$e".to_string());
        } else {
            let src = (0..num_expand_rows)
                .map(|r| copy_indices[r * num_out_cols + c])
                .find(|&s| s >= 0)
                .expect("a non-expand-id column must be an InputRef in some expand row");
            out_types.push(input.column(src as usize).data_type().clone());
            out_names.push(schema.field(src as usize).name().to_string());
        }
    }

    let mut blocks: Vec<Vec<ArrayRef>> = vec![Vec::with_capacity(num_expand_rows); num_out_cols];
    let mut row_kind_blocks: Vec<ArrayRef> = Vec::with_capacity(num_expand_rows);
    for r in 0..num_expand_rows {
        for c in 0..num_out_cols {
            let arr: ArrayRef = if c == expand_id_index {
                if expand_id_is_long {
                    Arc::new(Int64Array::from(vec![expand_id_values[r]; n]))
                } else {
                    Arc::new(Int32Array::from(vec![expand_id_values[r] as i32; n]))
                }
            } else {
                let src = copy_indices[r * num_out_cols + c];
                if src >= 0 {
                    input.column(src as usize).clone()
                } else {
                    new_null_array(&out_types[c], n)
                }
            };
            blocks[c].push(arr);
        }
        if let Some(idx) = row_kind_idx {
            row_kind_blocks.push(input.column(idx).clone());
        }
    }

    let mut fields: Vec<Field> = Vec::with_capacity(num_out_cols + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_out_cols + 1);
    for c in 0..num_out_cols {
        let refs: Vec<&dyn Array> = blocks[c].iter().map(|a| a.as_ref()).collect();
        // A grouped-out key carries nulls, so every non-expand-id column is nullable.
        fields.push(Field::new(&out_names[c], out_types[c].clone(), c != expand_id_index));
        columns.push(arrow::compute::concat(&refs).expect("failed to concat expand column"));
    }
    if row_kind_idx.is_some() {
        let refs: Vec<&dyn Array> = row_kind_blocks.iter().map(|a| a.as_ref()).collect();
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(arrow::compute::concat(&refs).expect("failed to concat row kind"));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("failed to build expand batch")
}

/// Stateless INNER UNNEST of an ARRAY or MAP column (Flink's `$UNNEST_ROWS$` / `Correlate`): each
/// input row is fanned out to one output row per element of its collection column `array_col`, the
/// input columns repeated and the element appended. A scalar `ARRAY` element appends one column; an
/// `ARRAY<ROW>` element flattens to one column per struct field; a `MAP` element appends two columns
/// (key, value). A NULL or empty collection yields no rows (INNER); Flink keeps a null *scalar*
/// element (a null row) but drops a null *ROW* element, so null elements are skipped only for a
/// struct child. The `$row_kind$` tag rides through (repeated per element), so it is
/// changelog-transparent. The same take-based fan-out as the windowing TVF / Expand — see
/// divergences/15 for why we don't drive DataFusion's UnnestExec. With `with_ordinality`, a trailing
/// 1-based INTEGER ordinality column (the element's position in its collection) is appended. With
/// `is_left` (a LEFT/outer unnest), a row whose collection produces no element emits one row with the
/// appended columns null, rather than no rows.
fn unnest_array(
    input: &RecordBatch,
    array_col: usize,
    with_ordinality: bool,
    is_left: bool,
    is_multiset: bool,
) -> RecordBatch {
    let schema = input.schema();
    let row_kind_idx = schema.fields().iter().position(|f| f.name() == ROW_KIND_COLUMN);
    let data_end = row_kind_idx.unwrap_or_else(|| schema.fields().len());
    let column = input.column(array_col);

    // Per source type: the per-row offsets, the array whose null drops an element (only ARRAY<ROW>:
    // Flink drops a null ROW element but keeps a null scalar/value), the flattened child arrays to
    // append (each taken by element index below), and a per-entry repeat count (only MULTISET, which
    // is a MAP<element, count> emitting each element `count` times). ARRAY<scalar> appends one column,
    // ARRAY<ROW> one per struct field, MAP a key and a value, MULTISET just the element.
    let (offsets, null_check, children, repeat): (
        &[i32],
        Option<ArrayRef>,
        Vec<(Field, ArrayRef)>,
        Option<ArrayRef>,
    ) = match column.data_type() {
        DataType::List(_) => {
            let list = column.as_any().downcast_ref::<ListArray>().expect("list");
            let values = list.values().clone();
            match values.data_type() {
                DataType::Struct(sfields) => {
                    let sa = values.as_any().downcast_ref::<StructArray>().expect("struct");
                    let children = sfields
                        .iter()
                        .enumerate()
                        .map(|(i, f)| (f.as_ref().clone(), sa.column(i).clone()))
                        .collect();
                    (list.value_offsets(), Some(values.clone()), children, None)
                }
                elem => (
                    list.value_offsets(),
                    None,
                    vec![(Field::new("f0", elem.clone(), true), values.clone())],
                    None,
                ),
            }
        }
        DataType::Map(..) => {
            let map = column.as_any().downcast_ref::<MapArray>().expect("map");
            let entries = map.entries();
            if is_multiset {
                // MULTISET<T> is MAP<T, count>: append only the element (column 0), repeated `count`.
                let element = (entries.fields()[0].as_ref().clone(), entries.column(0).clone());
                (map.value_offsets(), None, vec![element], Some(entries.column(1).clone()))
            } else {
                let children = entries
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (f.as_ref().clone(), entries.column(i).clone()))
                    .collect();
                (map.value_offsets(), None, children, None)
            }
        }
        other => panic!("UNNEST column must be a List or Map, got {other:?}"),
    };
    let counts = repeat
        .as_ref()
        .map(|c| c.as_any().downcast_ref::<Int32Array>().expect("multiset count int32"));

    // One take index per output row: the input row it copies (passthrough) and the child element it
    // carries. A null/empty collection contributes nothing (INNER); a null struct element is dropped;
    // a MULTISET entry is repeated by its count. take_elems carries the child index per output row, or
    // NULL for a LEFT null-pad row (a null take index makes `take` emit a null). ordinals likewise.
    let mut take_rows: Vec<u32> = Vec::new();
    let mut take_elems: Vec<Option<u32>> = Vec::new();
    let mut ordinals: Vec<Option<i32>> = Vec::new();
    for row in 0..input.num_rows() {
        let mut emitted = false;
        if !column.is_null(row) {
            let mut ord = 0i32; // running 1-based position among this row's emitted elements
            for k in offsets[row]..offsets[row + 1] {
                if let Some(child) = &null_check {
                    if child.is_null(k as usize) {
                        continue;
                    }
                }
                let copies = counts.map_or(1, |c| c.value(k as usize).max(0));
                for _ in 0..copies {
                    ord += 1;
                    take_rows.push(row as u32);
                    take_elems.push(Some(k as u32));
                    ordinals.push(Some(ord));
                    emitted = true;
                }
            }
        }
        if !emitted && is_left {
            // LEFT/outer: a row that produced no element is kept with the appended columns null.
            take_rows.push(row as u32);
            take_elems.push(None);
            ordinals.push(None);
        }
    }
    let rows_idx = UInt32Array::from(take_rows);
    let elems_idx = UInt32Array::from(take_elems);

    let mut fields: Vec<Field> = Vec::with_capacity(data_end + children.len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(data_end + children.len() + 1);
    for i in 0..data_end {
        fields.push(schema.field(i).as_ref().clone());
        columns.push(take(input.column(i), &rows_idx, None).expect("failed to fan out column"));
    }
    for (field, child) in &children {
        // A LEFT/outer null-pad makes every appended column nullable (even a map key or a
        // non-nullable struct field), so relax nullability there.
        fields.push(if is_left { field.clone().with_nullable(true) } else { field.clone() });
        columns.push(take(child.as_ref(), &elems_idx, None).expect("failed to take unnest element"));
    }
    if with_ordinality {
        fields.push(Field::new("ordinality", DataType::Int32, is_left));
        columns.push(Arc::new(Int32Array::from(ordinals)));
    }
    if let Some(idx) = row_kind_idx {
        fields.push(schema.field(idx).as_ref().clone());
        columns.push(take(input.column(idx), &rows_idx, None).expect("failed to fan out row kind"));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("failed to build unnest batch")
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
        windows_for(timestamp, self.window_millis, self.slide_millis, self.cumulative, windows);
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
    /// A DECIMAL value as its unscaled i128 (the scale is fixed per aggregate).
    I128(i128),
}

/// The OVER value column downcast once, so the per-row fold reads a typed value without a per-row
/// downcast. `None` for a null row (which the aggregates skip).
enum ValueColumn<'a> {
    I64(&'a Int64Array),
    I32(&'a Int32Array),
    F64(&'a arrow::array::Float64Array),
    Decimal128(&'a Decimal128Array),
    // Any column read only for null-ness: COUNT over a non-numeric (e.g. ARRAY/MAP/ROW) value
    // column, where only "is this row non-null" matters. The dummy value is never folded (COUNT
    // ignores it), so the matcher admits this only for COUNT.
    NullOnly(&'a ArrayRef),
}

impl ValueColumn<'_> {
    fn at(&self, row: usize) -> Option<Num> {
        match self {
            ValueColumn::I64(a) => (!a.is_null(row)).then(|| Num::I64(a.value(row))),
            ValueColumn::I32(a) => (!a.is_null(row)).then(|| Num::I32(a.value(row))),
            ValueColumn::F64(a) => (!a.is_null(row)).then(|| Num::F64(a.value(row))),
            ValueColumn::Decimal128(a) => (!a.is_null(row)).then(|| Num::I128(a.value(row))),
            ValueColumn::NullOnly(a) => (!a.is_null(row)).then_some(Num::I64(0)),
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
    // SUM over DECIMAL(p, s): accumulate the unscaled i128 at the input scale; the result is
    // DECIMAL(38, s) (Flink's findSumAggType). `overflow` latches once the running sum no longer
    // fits DECIMAL(38, s) (|value| >= 10^38), at which point Flink reports NULL.
    SumDecimal { sum: i128, scale: i8, overflow: bool },
    // MIN/MAX over DECIMAL(p, s): the value lives in the Extremes multiset (this variant is never
    // folded), so this carries only the precision/scale to report the result type DECIMAL(p, s).
    MinMaxDecimal { precision: u8, scale: i8 },
    // FIRST_VALUE / LAST_VALUE: hold the first / most-recent non-null value seen (None until one
    // arrives → emits NULL, matching Flink, which ignores nulls in these functions).
    FirstI64(Option<i64>),
    LastI64(Option<i64>),
    FirstI32(Option<i32>),
    LastI32(Option<i32>),
    FirstF64(Option<f64>),
    LastF64(Option<f64>),
}

impl RunningAgg {
    fn new(kind: i64, value_type: &DataType) -> Self {
        use RunningAgg::*;
        if kind == 3 || kind == 7 {
            return Count(0); // COUNT and COUNT(DISTINCT) both report a bigint count
        }
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
            // SUM(DECIMAL(_, s)) — accumulate the unscaled i128 at scale s; result is DECIMAL(38, s).
            (0, DataType::Decimal128(_, s)) => SumDecimal { sum: 0, scale: *s, overflow: false },
            // MIN/MAX(DECIMAL(p, s)) — the extreme lives in the multiset; result is DECIMAL(p, s).
            (1 | 2, DataType::Decimal128(p, s)) => MinMaxDecimal { precision: *p, scale: *s },
            (k, other) => panic!("unsupported OVER aggregate kind {k} for value type {other:?}"),
        }
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
            (SumDecimal { sum, overflow, .. }, Num::I128(v)) => accumulate_decimal(sum, overflow, v),
            // FIRST_VALUE keeps the earliest value (set once); LAST_VALUE takes the most recent.
            (FirstI64(f), Num::I64(v)) => *f = Some(f.unwrap_or(v)),
            (LastI64(l), Num::I64(v)) => *l = Some(v),
            (FirstI32(f), Num::I32(v)) => *f = Some(f.unwrap_or(v)),
            (LastI32(l), Num::I32(v)) => *l = Some(v),
            (FirstF64(f), Num::F64(v)) => *f = Some(f.unwrap_or(v)),
            (LastF64(l), Num::F64(v)) => *l = Some(v),
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
            (SumDecimal { sum, overflow, .. }, Num::I128(v)) => {
                accumulate_decimal(sum, overflow, v.wrapping_neg())
            }
            _ => unreachable!("aggregate does not support retraction"),
        }
    }

    /// The current running value (also the checkpointed state).
    fn emit(&self) -> ScalarValue {
        use RunningAgg::*;
        match self {
            SumI64(v) | MinI64(v) | MaxI64(v) | FirstI64(v) | LastI64(v) => ScalarValue::Int64(*v),
            SumI32(v) | MinI32(v) | MaxI32(v) | FirstI32(v) | LastI32(v) => ScalarValue::Int32(*v),
            SumF64(v) | MinF64(v) | MaxF64(v) | FirstF64(v) | LastF64(v) => ScalarValue::Float64(*v),
            Count(c) => ScalarValue::Int64(Some(*c)),
            // Overflow past DECIMAL(38, s) reports NULL, matching Flink's fromBigDecimal.
            SumDecimal { sum, scale, overflow } => {
                ScalarValue::Decimal128((!*overflow).then_some(*sum), 38, *scale)
            }
            // Never folded — MIN/MAX state is the Extremes multiset, which emits via MinMaxKey::scalar.
            MinMaxDecimal { precision, scale } => ScalarValue::Decimal128(None, *precision, *scale),
        }
    }

    fn result_type(&self) -> DataType {
        use RunningAgg::*;
        match self {
            SumI64(_) | MinI64(_) | MaxI64(_) | Count(_) | FirstI64(_) | LastI64(_) => {
                DataType::Int64
            }
            SumI32(_) | MinI32(_) | MaxI32(_) | FirstI32(_) | LastI32(_) => DataType::Int32,
            SumF64(_) | MinF64(_) | MaxF64(_) | FirstF64(_) | LastF64(_) => DataType::Float64,
            SumDecimal { scale, .. } => DataType::Decimal128(38, *scale),
            MinMaxDecimal { precision, scale } => DataType::Decimal128(*precision, *scale),
        }
    }

    fn restore_value(&mut self, scalar: &ScalarValue) {
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
            // A NULL snapshot value means the running sum had overflowed DECIMAL(38, s).
            (SumDecimal { sum, overflow, .. }, ScalarValue::Decimal128(v, _, _)) => match v {
                Some(x) => {
                    *sum = *x;
                    *overflow = false;
                }
                None => *overflow = true,
            },
            _ => panic!("OVER state type mismatch on restore"),
        }
    }
}

/// The exclusive bound for a `DECIMAL(38, _)` magnitude: a value fits iff `|value| < 10^38`.
const DECIMAL128_MAX: i128 = 10i128.pow(38);

/// Adds `delta` to a decimal running sum, latching `overflow` once it no longer fits DECIMAL(38, s)
/// (matching Flink's `fromBigDecimal`, which returns NULL past precision 38). Once overflowed it stays
/// overflowed — the lost magnitude can't be recovered by a later retraction.
fn accumulate_decimal(sum: &mut i128, overflow: &mut bool, delta: i128) {
    if *overflow {
        return;
    }
    match sum.checked_add(delta) {
        Some(t) if t > -DECIMAL128_MAX && t < DECIMAL128_MAX => *sum = t,
        _ => *overflow = true,
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

/// One buffered row of a bounded-frame OVER partition: its rowtime and the single OVER value (None =
/// null, which the aggregates skip). Held until it can no longer fall inside any future row's frame.
#[derive(Clone, Copy)]
struct BufferedRow {
    rt: i64,
    value: Option<Num>,
}

/// Bounded-frame event-time OVER (`ROWS BETWEEN n PRECEDING AND CURRENT ROW`, or `RANGE BETWEEN
/// INTERVAL x PRECEDING AND CURRENT ROW`). Unlike the unbounded {@link OverAggregator}, which folds a
/// single persistent accumulator per key, a bounded frame drops rows off its trailing edge — so the
/// running value cannot be maintained incrementally for MIN/MAX (they would need a retractable
/// multiset). Instead this keeps a per-key sorted buffer of the rows still reachable by some future
/// frame and **recomputes** each emitted row's aggregate over its frame slice with a fresh
/// {@link RunningAgg}. The result is byte-identical to Flink's `*BoundedPrecedingFunction` (both
/// aggregate over the same frame) and sidesteps MIN/MAX retraction entirely. See divergences/11.
struct BoundedOverAggregator {
    kinds: Vec<i64>,
    value_type: DataType,
    /// true = ROWS (count of rows), false = RANGE (rowtime interval).
    rows_frame: bool,
    /// n preceding rows (ROWS) or the preceding interval in millis (RANGE).
    offset: i64,
    /// Per key, the buffered rows sorted ascending by rowtime (stable for ties).
    keys: HashMap<GroupKey, Vec<BufferedRow>>,
    key_types: Vec<DataType>,
}

impl BoundedOverAggregator {
    fn new(value_type: i64, kinds: Vec<i64>, rows_frame: bool, offset: i64) -> Self {
        BoundedOverAggregator {
            kinds,
            value_type: value_data_type(value_type),
            rows_frame,
            offset,
            keys: HashMap::new(),
            key_types: Vec::new(),
        }
    }

    /// Folds the batch (`rt` i64, `value`, optional `key0..`) into the per-key buffer and returns
    /// `[result0..]` per input row, each computed by recomputing the aggregate over that row's frame.
    fn update(&mut self, batch: &RecordBatch) -> RecordBatch {
        let rt = column_i64(batch, "rt");
        let value = batch.column_by_name("value").expect("missing value column");
        let value_column = match self.value_type {
            DataType::Int64 => ValueColumn::I64(value.as_any().downcast_ref().expect("int64 value")),
            DataType::Int32 => ValueColumn::I32(value.as_any().downcast_ref().expect("int32 value")),
            DataType::Float64 => {
                ValueColumn::F64(value.as_any().downcast_ref().expect("float64 value"))
            }
            ref other => panic!("unsupported bounded OVER value type: {other:?}"),
        };
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);
        let n = batch.num_rows();
        let num_agg = self.kinds.len();
        let row_keys: Vec<GroupKey> = (0..n).map(|row| read_key(&key_arrays, row)).collect();

        // Append the new rows to their per-key buffers in rowtime order (stable for ties). Every new
        // row's rowtime is past the prior watermark, hence at or after all already-buffered rows, so
        // appending in this order keeps each buffer sorted. Record where each input row landed.
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&row| rt.value(row));
        let mut buffer_index = vec![0usize; n];
        let mut max_rt = i64::MIN;
        for &row in &order {
            let row_rt = rt.value(row);
            max_rt = max_rt.max(row_rt);
            let buffer = self.keys.entry(row_keys[row].clone()).or_default();
            buffer.push(BufferedRow { rt: row_rt, value: value_column.at(row) });
            buffer_index[row] = buffer.len() - 1;
        }

        let mut results: Vec<Vec<ScalarValue>> = vec![vec![ScalarValue::Null; n]; num_agg];
        for row in 0..n {
            let buffer = &self.keys[&row_keys[row]];
            let i = buffer_index[row];
            let cur_rt = buffer[i].rt;
            // ROWS counts physical rows up to and including this one; RANGE covers all rows within the
            // rowtime interval and shares one frame across rows of equal rowtime (ending at the last).
            let (lower, upper) = if self.rows_frame {
                (i.saturating_sub(self.offset as usize), i)
            } else {
                let lo = buffer.partition_point(|r| r.rt < cur_rt - self.offset);
                let hi = buffer.partition_point(|r| r.rt <= cur_rt) - 1;
                (lo, hi)
            };
            let mut aggs: Vec<RunningAgg> =
                self.kinds.iter().map(|&k| RunningAgg::new(k, &self.value_type)).collect();
            for r in &buffer[lower..=upper] {
                if let Some(v) = r.value {
                    for agg in aggs.iter_mut() {
                        agg.fold(v);
                    }
                }
            }
            for (a, agg) in aggs.iter().enumerate() {
                results[a][row] = agg.emit();
            }
        }

        self.evict(max_rt);

        let mut fields = Vec::with_capacity(num_agg);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_agg);
        for (a, &kind) in self.kinds.iter().enumerate() {
            let result_type = RunningAgg::new(kind, &self.value_type).result_type();
            fields.push(Field::new(format!("result{a}"), result_type.clone(), true));
            columns.push(scalars_to_array(std::mem::take(&mut results[a]), &result_type));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build bounded over result batch")
    }

    /// Drops buffered rows that can no longer fall inside any future row's frame. A future row's
    /// rowtime exceeds `max_rt` (it has not completed yet), so for RANGE keep rows whose rowtime is at
    /// or after `max_rt - offset`; for ROWS keep the most recent `offset` rows (the deepest any future
    /// frame can reach back). Empty partitions are removed to bound memory.
    fn evict(&mut self, max_rt: i64) {
        let (rows_frame, offset) = (self.rows_frame, self.offset);
        self.keys.retain(|_, buffer| {
            if rows_frame {
                let keep = offset as usize;
                if buffer.len() > keep {
                    buffer.drain(0..buffer.len() - keep);
                }
            } else {
                let bound = max_rt - offset;
                let cut = buffer.partition_point(|r| r.rt < bound);
                if cut > 0 {
                    buffer.drain(0..cut);
                }
            }
            !buffer.is_empty()
        });
    }

    /// Serializes the per-key buffer (`[key0.., rt, value]`, one row per buffered row).
    fn snapshot(&mut self) -> Vec<u8> {
        let mut keys: Vec<GroupKey> = Vec::new();
        let mut rts: Vec<ScalarValue> = Vec::new();
        let mut values: Vec<ScalarValue> = Vec::new();
        for (key, buffer) in self.keys.iter() {
            for row in buffer {
                keys.push(key.clone());
                rts.push(ScalarValue::Int64(Some(row.rt)));
                values.push(num_to_scalar(&self.value_type, row.value));
            }
        }
        let mut fields = key_fields(&self.key_types);
        let mut columns = key_columns(&keys, &self.key_types);
        fields.push(Field::new("rt", DataType::Int64, false));
        columns.push(scalars_to_array(rts, &DataType::Int64));
        fields.push(Field::new("value", self.value_type.clone(), true));
        columns.push(scalars_to_array(values, &self.value_type));
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build bounded over snapshot batch"))
    }

    fn restore(value_type: i64, kinds: Vec<i64>, rows_frame: bool, offset: i64, bytes: &[u8]) -> Self {
        let mut aggregator = BoundedOverAggregator::new(value_type, kinds, rows_frame, offset);
        for batch in read_ipc(bytes) {
            let arity = batch.num_columns() - 2; // trailing rt + value columns
            let key_arrays: Vec<&ArrayRef> = (0..arity).map(|j| batch.column(j)).collect();
            aggregator.key_types = key_types(&key_arrays);
            let rt = column_i64(&batch, "rt");
            let value = batch.column_by_name("value").expect("missing value column");
            for row in 0..batch.num_rows() {
                let key = read_key(&key_arrays, row);
                let scalar = ScalarValue::try_from_array(value, row).expect("bounded over value");
                aggregator
                    .keys
                    .entry(key)
                    .or_default()
                    .push(BufferedRow { rt: rt.value(row), value: num_from_scalar(&scalar) });
            }
        }
        aggregator
    }
}

/// A single OVER value as a typed Arrow scalar of the value column's type (null = no value).
fn num_to_scalar(value_type: &DataType, num: Option<Num>) -> ScalarValue {
    match (value_type, num) {
        (_, Some(Num::I64(v))) => ScalarValue::Int64(Some(v)),
        (_, Some(Num::I32(v))) => ScalarValue::Int32(Some(v)),
        (_, Some(Num::F64(v))) => ScalarValue::Float64(Some(v)),
        (DataType::Int64, None) => ScalarValue::Int64(None),
        (DataType::Int32, None) => ScalarValue::Int32(None),
        (DataType::Float64, None) => ScalarValue::Float64(None),
        (other, _) => panic!("unsupported bounded OVER value type: {other:?}"),
    }
}

/// Reads a typed Arrow scalar back into an OVER value (null → None, skipped by the aggregates).
fn num_from_scalar(scalar: &ScalarValue) -> Option<Num> {
    match scalar {
        ScalarValue::Int64(v) => v.map(Num::I64),
        ScalarValue::Int32(v) => v.map(Num::I32),
        ScalarValue::Float64(v) => v.map(Num::F64),
        other => panic!("unsupported bounded OVER value scalar: {other:?}"),
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
    // A DECIMAL extreme as its unscaled i128. All values of one aggregate share a scale, so ordering
    // the raw i128 is the decimal ordering; the precision/scale are restored from the result type on
    // emit (the key carries only the value, since the fold path has no precision/scale).
    Decimal128(i128),
}

impl MinMaxKey {
    fn of(num: Num) -> Self {
        match num {
            Num::I64(v) => MinMaxKey::I64(v),
            Num::I32(v) => MinMaxKey::I32(v),
            Num::F64(v) => MinMaxKey::F64(OrdF64(v)),
            Num::I128(v) => MinMaxKey::Decimal128(v),
        }
    }

    fn from_scalar(scalar: &ScalarValue) -> Self {
        match scalar {
            ScalarValue::Int64(Some(v)) => MinMaxKey::I64(*v),
            ScalarValue::Int32(Some(v)) => MinMaxKey::I32(*v),
            ScalarValue::Float64(Some(v)) => MinMaxKey::F64(OrdF64(*v)),
            ScalarValue::Decimal128(Some(v), _, _) => MinMaxKey::Decimal128(*v),
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
    // COUNT(DISTINCT x): a value→multiplicity map (Flink's DistinctAccumulator MapView). The count is
    // the number of live entries; a value's multiplicity tracks how many input rows carry it so a
    // retraction removes it only when the last one is retracted. Nulls are never inserted.
    Distinct(HashMap<ScalarValue, i64>),
}

impl GroupAggState {
    fn new(kind: i64, value_type: &DataType) -> Self {
        match kind {
            1 => GroupAggState::Extremes { is_min: true, counts: BTreeMap::new() }, // MIN
            2 => GroupAggState::Extremes { is_min: false, counts: BTreeMap::new() }, // MAX
            7 => GroupAggState::Distinct(HashMap::new()), // COUNT(DISTINCT)
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
            GroupAggState::Distinct(_) => unreachable!("distinct folds a scalar, not a Num"),
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
            GroupAggState::Distinct(_) => unreachable!("distinct retracts a scalar, not a Num"),
        }
    }

    /// Adds one occurrence of a distinct value (COUNT(DISTINCT)); a new value grows the count.
    fn accumulate_distinct(&mut self, value: ScalarValue) {
        match self {
            GroupAggState::Distinct(counts) => *counts.entry(value).or_insert(0) += 1,
            _ => unreachable!("accumulate_distinct on a non-distinct aggregate"),
        }
    }

    /// Removes one occurrence; the value leaves the distinct set when its last occurrence is retracted.
    fn retract_distinct(&mut self, value: ScalarValue) {
        match self {
            GroupAggState::Distinct(counts) => {
                if let Some(count) = counts.get_mut(&value) {
                    *count -= 1;
                    if *count <= 0 {
                        counts.remove(&value);
                    }
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
                _ => agg.emit(),
            },
            GroupAggState::Extremes { is_min, counts } => {
                let extreme = if *is_min { counts.keys().next() } else { counts.keys().next_back() };
                extreme.map_or_else(|| null_scalar(result_type), |k| k.scalar(result_type))
            }
            // COUNT(DISTINCT) is the number of live distinct values (never NULL — empty is 0).
            GroupAggState::Distinct(counts) => ScalarValue::Int64(Some(counts.len() as i64)),
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
        let row_kinds = row_kind_column(batch);
        // Per aggregate, the value column index for a COUNT(DISTINCT) (kind 7), else None. Captured
        // before the per-row loop so the loop body reads no `self` field while `state` is borrowed.
        let distinct_cols: Vec<Option<usize>> = (0..num_agg)
            .map(|i| (self.kinds[i] == 7).then_some(self.value_columns[i] as usize))
            .collect();

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
                    // COUNT(DISTINCT x) (kind 7) folds the value itself, not a Num — read its scalar
                    // (skipping nulls, which DISTINCT ignores) and add/remove it from the value set.
                    if let Some(col_idx) = distinct_cols[i] {
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
                            multiset_values[i].push(value.scalar(&self.result_types[i]));
                            multiset_counts[i].push(*count);
                        }
                    }
                    GroupAggState::Distinct(counts) => {
                        // The count is recomputed from the side batch on restore (placeholder here).
                        state_columns[i].push(null_scalar(&self.result_types[i]));
                        non_null_columns[i].push(0);
                        for (value, count) in counts.iter() {
                            multiset_keys[i].push(key.clone());
                            multiset_values[i].push(value.clone()); // the distinct value itself
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
            if matches!(self.kinds[i], 1 | 2 | 7) {
                let mut f = key_fields(&self.key_types);
                let mut c = key_columns(&multiset_keys[i], &self.key_types);
                // MIN/MAX values take the aggregate's result type; a distinct value keeps its own type
                // (the count's bigint result type does not describe it), inferred from the scalars.
                let values = std::mem::take(&mut multiset_values[i]);
                let value_array: ArrayRef = if self.kinds[i] == 7 {
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
        // One side batch per MIN/MAX or DISTINCT aggregate, in aggregate order: key0.., value, count.
        let mut frame = 1;
        for i in 0..num_agg {
            if !matches!(aggregator.kinds[i], 1 | 2 | 7) {
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
                match aggregator.keys.get_mut(&key).map(|s| &mut s.aggs[i]) {
                    Some(GroupAggState::Extremes { counts: map, .. }) => {
                        map.insert(MinMaxKey::from_scalar(&value), counts.value(row));
                    }
                    Some(GroupAggState::Distinct(map)) => {
                        map.insert(value, counts.value(row));
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
struct LocalGroupAggregator {
    kinds: Vec<i64>,
    value_types: Vec<DataType>,
    value_columns: Vec<i64>,
    key_columns: Vec<usize>,
    result_types: Vec<DataType>,
    order: Vec<GroupKey>,
    states: HashMap<GroupKey, Vec<GroupAggState>>,
    key_types: Vec<DataType>,
}

impl LocalGroupAggregator {
    fn new(
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
            states: HashMap::new(),
            key_types: Vec::new(),
        }
    }

    /// Folds the batch's rows into the buffered per-key accumulators (append-only — the local's input
    /// is insert-only, so there is no retraction). Nothing is emitted until a flush.
    fn update(&mut self, batch: &RecordBatch) {
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
        for row in 0..n {
            let key = read_key(&key_arrays, row);
            if !self.states.contains_key(&key) {
                let init: Vec<GroupAggState> = self
                    .kinds
                    .iter()
                    .zip(&self.value_types)
                    .map(|(&kind, vt)| GroupAggState::new(kind, vt))
                    .collect();
                self.order.push(key.clone());
                self.states.insert(key.clone(), init);
            }
            let entry = self.states.get_mut(&key).expect("key present");
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
        }
    }

    /// Emits the buffered partials (`[key0.., partial0..]`, in first-appearance order) and clears the
    /// buffer; the key types are retained so an empty flush still carries the right schema.
    fn flush(&mut self) -> RecordBatch {
        let order = std::mem::take(&mut self.order);
        let states = std::mem::take(&mut self.states);
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
    Bounded(BoundedOverAggregator),
    WindowFunctions(WindowFunctionOver),
}

impl OverInner {
    fn new(value_type: i64, kinds: Vec<i64>, frame_kind: i64, frame_offset: i64) -> Self {
        if kinds.iter().all(|&k| is_window_function_kind(k)) {
            OverInner::WindowFunctions(WindowFunctionOver::new(kinds))
        } else if frame_kind == 0 {
            OverInner::Aggregates(OverAggregator::new(value_type, kinds))
        } else {
            // frame_kind 1 = bounded ROWS, 2 = bounded RANGE.
            OverInner::Bounded(BoundedOverAggregator::new(
                value_type,
                kinds,
                frame_kind == 1,
                frame_offset,
            ))
        }
    }

    fn update(&mut self, batch: &RecordBatch) -> RecordBatch {
        match self {
            OverInner::Aggregates(inner) => inner.update(batch),
            OverInner::Bounded(inner) => inner.update(batch),
            OverInner::WindowFunctions(inner) => inner.update(batch),
        }
    }

    fn snapshot(&mut self) -> Vec<u8> {
        match self {
            OverInner::Aggregates(inner) => inner.snapshot(),
            OverInner::Bounded(inner) => inner.snapshot(),
            OverInner::WindowFunctions(inner) => inner.snapshot(),
        }
    }

    fn restore(value_type: i64, kinds: Vec<i64>, frame_kind: i64, frame_offset: i64, bytes: &[u8]) -> Self {
        if kinds.iter().all(|&k| is_window_function_kind(k)) {
            OverInner::WindowFunctions(WindowFunctionOver::restore(kinds, bytes))
        } else if frame_kind == 0 {
            OverInner::Aggregates(OverAggregator::restore(value_type, kinds, bytes))
        } else {
            OverInner::Bounded(BoundedOverAggregator::restore(
                value_type,
                kinds,
                frame_kind == 1,
                frame_offset,
                bytes,
            ))
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
        frame_kind: i64,
        frame_offset: i64,
    ) -> Self {
        OverWindowAggregator {
            inner: OverInner::new(value_type, kinds, frame_kind, frame_offset),
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
        frame_kind: i64,
        frame_offset: i64,
        bytes: &[u8],
    ) -> Self {
        let accumulators_len = u32::from_le_bytes(bytes[0..4].try_into().expect("len")) as usize;
        let inner =
            OverInner::restore(value_type, kinds, frame_kind, frame_offset, &bytes[4..4 + accumulators_len]);
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

/// Event-time sort (Flink's `RowTimeSortOperator`): buffers input rows and, on a watermark, emits
/// the rows whose rowtime is at or before it in ascending rowtime order, keeping the rest. Insert-
/// only — the watermark guarantees no earlier-rowtime row can still arrive, so the emitted order is
/// final. Ties at the same rowtime keep arrival order (a stable sort), matching the host. There is no
/// key (a single distribution gathers the stream), so this is the columnar analog of the host's
/// single-input event-time sort.
struct TemporalSorter {
    rt_column: usize,
    buffered: Vec<RecordBatch>,
    input_schema: Option<SchemaRef>,
}

impl TemporalSorter {
    fn new(rt_column: usize) -> Self {
        TemporalSorter { rt_column, buffered: Vec::new(), input_schema: None }
    }

    fn push(&mut self, batch: RecordBatch) {
        self.input_schema = Some(batch.schema());
        self.buffered.push(batch);
    }

    /// Emits the rows the watermark has completed, sorted ascending by rowtime, and keeps the rest
    /// buffered. Returns an empty batch when nothing is complete.
    fn flush(&mut self, watermark: i64) -> RecordBatch {
        let schema = match &self.input_schema {
            Some(schema) => schema.clone(),
            None => return RecordBatch::new_empty(Arc::new(Schema::empty())),
        };
        let all = concat_batches(&schema, &self.buffered).expect("failed to concat sort buffer");
        let rt_millis = rt_to_millis(all.column(self.rt_column));
        let complete_mask: BooleanArray =
            rt_millis.iter().map(|v| Some(v.unwrap() <= watermark)).collect();
        let complete = filter_record_batch(&all, &complete_mask).expect("failed to filter complete");
        let pending_mask = arrow::compute::not(&complete_mask).expect("failed to negate mask");
        let pending = filter_record_batch(&all, &pending_mask).expect("failed to filter pending");
        self.buffered = if pending.num_rows() > 0 { vec![pending] } else { Vec::new() };
        if complete.num_rows() == 0 {
            return RecordBatch::new_empty(schema);
        }
        // Stable ascending sort by rowtime: Rust's sort_by_key is stable, so equal-rowtime rows keep
        // their arrival order, as the host's sort does.
        let rt_complete = rt_to_millis(complete.column(self.rt_column));
        let values = rt_complete.values();
        let mut order: Vec<u32> = (0..complete.num_rows() as u32).collect();
        order.sort_by_key(|&i| values[i as usize]);
        let indices = UInt32Array::from(order);
        let columns: Vec<ArrayRef> = complete
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).expect("failed to sort column"))
            .collect();
        RecordBatch::try_new(complete.schema(), columns).expect("failed to build sorted batch")
    }

    fn snapshot(&mut self) -> Vec<u8> {
        match (&self.input_schema, self.buffered.is_empty()) {
            (Some(schema), false) => {
                let all = concat_batches(schema, &self.buffered).expect("concat sort buffer");
                let mut bytes = Vec::new();
                let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &all.schema())
                    .expect("sort buffer writer");
                writer.write(&all).expect("write sort buffer");
                writer.finish().expect("finish sort buffer");
                drop(writer);
                bytes
            }
            _ => Vec::new(),
        }
    }

    fn restore(rt_column: usize, bytes: &[u8]) -> Self {
        let mut sorter = TemporalSorter::new(rt_column);
        if !bytes.is_empty() {
            let reader =
                arrow::ipc::reader::StreamReader::try_new(bytes, None).expect("sort buffer reader");
            for batch in reader {
                let batch = batch.expect("read sort buffer");
                sorter.input_schema = Some(batch.schema());
                sorter.buffered.push(batch);
            }
        }
        sorter
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
    predicate: Option<JoinPredicate>,
    join_type: JoinKind,
    // Eager data schemas (no `$rowid$`), seeded at construction so an outer join can type the
    // null-padding for a side before that side's first batch arrives.
    left_data_schema: SchemaRef,
    right_data_schema: SchemaRef,
    // Buffered rows: data-only for an INNER join, data + a trailing `__rowid__` for an outer join (so
    // a matched buffered row can be identified to set its match flag, and an unmatched one null-padded
    // at eviction).
    left_buffered: Vec<RecordBatch>,
    right_buffered: Vec<RecordBatch>,
    // Outer only: the row-ids that have matched at least once, and the per-side id counters.
    left_matched: HashSet<i64>,
    right_matched: HashSet<i64>,
    left_next_id: i64,
    right_next_id: i64,
}

impl IntervalJoiner {
    #[allow(clippy::too_many_arguments)]
    fn new(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        left_time: usize,
        right_time: usize,
        lower: i64,
        upper: i64,
        predicate: Option<JoinPredicate>,
        join_type: JoinKind,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
    ) -> Self {
        IntervalJoiner {
            left_keys,
            right_keys,
            left_time,
            right_time,
            lower,
            upper,
            predicate,
            join_type,
            left_data_schema,
            right_data_schema,
            left_buffered: Vec::new(),
            right_buffered: Vec::new(),
            left_matched: HashSet::new(),
            right_matched: HashSet::new(),
            left_next_id: 0,
            right_next_id: 0,
        }
    }

    fn key_pairs(&self) -> Vec<(usize, usize)> {
        self.left_keys.iter().zip(&self.right_keys).map(|(&l, &r)| (l, r)).collect()
    }

    /// The schema of one side's buffered batches: data, plus a trailing `__rowid__` for an outer join.
    fn buf_schema(&self, is_left: bool) -> SchemaRef {
        let data = if is_left { &self.left_data_schema } else { &self.right_data_schema };
        if self.join_type == JoinKind::Inner {
            data.clone()
        } else {
            with_rowid_schema(data)
        }
    }

    /// Joins an incoming left batch against the buffered right rows (equi-key + interval bounds and
    /// the residual non-equi predicate), then buffers it. Empty until the right side has rows.
    fn push_left(&mut self, batch: RecordBatch) -> RecordBatch {
        let interval = Some((self.left_time, self.right_time, self.lower, self.upper));
        let filter = residual_filter(
            &self.left_data_schema,
            &self.right_data_schema,
            interval,
            self.predicate.as_mut(),
        );
        if self.join_type == JoinKind::Inner {
            let result = if self.right_buffered.is_empty() {
                empty_batch()
            } else {
                let right = concat_batches(&self.buf_schema(false), self.right_buffered.iter())
                    .expect("concat right interval buffer");
                hash_join_inner(batch.clone(), right, &self.key_pairs(), filter)
            };
            self.left_buffered.push(batch);
            return result;
        }
        // Outer: tag with row-ids, join, record matches on both sides, then buffer the tagged batch.
        let tagged = append_rowids(&batch, &mut self.left_next_id);
        let result = if self.right_buffered.is_empty() {
            empty_batch()
        } else {
            let right = concat_batches(&self.buf_schema(false), self.right_buffered.iter())
                .expect("concat right interval buffer");
            self.join_tagged(tagged.clone(), right, filter)
        };
        self.left_buffered.push(tagged);
        result
    }

    /// Joins an incoming right batch against the buffered left rows, then buffers it.
    fn push_right(&mut self, batch: RecordBatch) -> RecordBatch {
        let interval = Some((self.left_time, self.right_time, self.lower, self.upper));
        let filter = residual_filter(
            &self.left_data_schema,
            &self.right_data_schema,
            interval,
            self.predicate.as_mut(),
        );
        if self.join_type == JoinKind::Inner {
            let result = if self.left_buffered.is_empty() {
                empty_batch()
            } else {
                let left = concat_batches(&self.buf_schema(true), self.left_buffered.iter())
                    .expect("concat left interval buffer");
                hash_join_inner(left, batch.clone(), &self.key_pairs(), filter)
            };
            self.right_buffered.push(batch);
            return result;
        }
        let tagged = append_rowids(&batch, &mut self.right_next_id);
        let result = if self.left_buffered.is_empty() {
            empty_batch()
        } else {
            let left = concat_batches(&self.buf_schema(true), self.left_buffered.iter())
                .expect("concat left interval buffer");
            self.join_tagged(left, tagged.clone(), filter)
        };
        self.right_buffered.push(tagged);
        result
    }

    /// Runs the (always INNER) hash join of two row-id-tagged operands `[left data.., left __rowid__]`
    /// and `[right data.., right __rowid__]`, records the matched row-ids on both sides, and returns
    /// the matched pairs projected back to `[left data.., right data..]` (the row-ids dropped).
    fn join_tagged(
        &mut self,
        left_tagged: RecordBatch,
        right_tagged: RecordBatch,
        filter: Option<JoinFilter>,
    ) -> RecordBatch {
        let left_arity = self.left_data_schema.fields().len();
        let joined = hash_join_inner(left_tagged, right_tagged, &self.key_pairs(), filter);
        if joined.num_rows() == 0 {
            return empty_batch();
        }
        // Layout after the join (renamed c0..): [left data.., left rid, right data.., right rid].
        let total = joined.num_columns();
        let left_rid = joined.column(left_arity).as_any().downcast_ref::<Int64Array>().expect("left rid");
        let right_rid = joined.column(total - 1).as_any().downcast_ref::<Int64Array>().expect("right rid");
        for i in 0..joined.num_rows() {
            self.left_matched.insert(left_rid.value(i));
            self.right_matched.insert(right_rid.value(i));
        }
        // Project out the two row-id columns, leaving [left data.., right data..].
        let keep: Vec<usize> =
            (0..left_arity).chain(left_arity + 1..total - 1).collect();
        let fields: Vec<Field> = keep
            .iter()
            .enumerate()
            .map(|(j, &i)| Field::new(format!("c{j}"), joined.schema().field(i).data_type().clone(), true))
            .collect();
        let columns: Vec<ArrayRef> = keep.iter().map(|&i| joined.column(i).clone()).collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("project interval pairs")
    }

    /// Drops the rows the watermark has made dead and, for an outer join, returns the null-padded
    /// rows for the evicted outer-side rows that never matched (append-only — emitted once). A left
    /// row can no longer match once `left.rt - lower <= watermark` (a future right row has rt >
    /// watermark, but matching it needs `right.rt <= left.rt - lower`); a right row once
    /// `right.rt + upper <= watermark`. Because an outer row is evicted only once no future other-side
    /// row could match it, all its potential matches have been seen, so its match flag is final.
    fn advance(&mut self, watermark: i64) -> RecordBatch {
        let (lower, upper) = (self.lower, self.upper);
        if self.join_type == JoinKind::Inner {
            Self::evict_inner(&mut self.left_buffered, &self.left_data_schema, self.left_time, |rt| {
                rt - lower > watermark
            });
            Self::evict_inner(&mut self.right_buffered, &self.right_data_schema, self.right_time, |rt| {
                rt + upper > watermark
            });
            return empty_batch();
        }
        let left_pads = self.evict_outer(true, |rt| rt - lower > watermark);
        let right_pads = self.evict_outer(false, |rt| rt + upper > watermark);
        match (left_pads, right_pads) {
            (None, None) => empty_batch(),
            (Some(b), None) | (None, Some(b)) => b,
            (Some(l), Some(r)) => {
                concat_batches(&l.schema(), [l, r].iter()).expect("concat interval null-pads")
            }
        }
    }

    /// INNER eviction: keeps only the buffered rows whose rowtime (column `time`) satisfies `keep`.
    fn evict_inner(
        buffered: &mut Vec<RecordBatch>,
        schema: &SchemaRef,
        time: usize,
        keep: impl Fn(i64) -> bool,
    ) {
        if buffered.is_empty() {
            return;
        }
        let all = concat_batches(schema, buffered.iter()).expect("concat interval buffer");
        let rt = rt_to_millis(all.column(time));
        let mask: BooleanArray = rt.iter().map(|v| Some(keep(v.unwrap()))).collect();
        let kept = filter_record_batch(&all, &mask).expect("filter interval buffer");
        *buffered = if kept.num_rows() > 0 { vec![kept] } else { Vec::new() };
    }

    /// Outer eviction for one side: keeps the live rows, drops the dead ones' match flags, and returns
    /// the null-padded rows for evicted rows that never matched (only when this side is outer).
    fn evict_outer(&mut self, is_left: bool, keep: impl Fn(i64) -> bool) -> Option<RecordBatch> {
        let buffered = std::mem::take(if is_left { &mut self.left_buffered } else { &mut self.right_buffered });
        if buffered.is_empty() {
            return None;
        }
        let time = if is_left { self.left_time } else { self.right_time };
        let buf_schema = self.buf_schema(is_left);
        let all = concat_batches(&buf_schema, buffered.iter()).expect("concat interval buffer");
        let rt = rt_to_millis(all.column(time));
        let keep_mask: BooleanArray = rt.iter().map(|v| Some(keep(v.unwrap()))).collect();
        let kept = filter_record_batch(&all, &keep_mask).expect("filter interval kept");
        let evicted =
            filter_record_batch(&all, &arrow::compute::not(&keep_mask).expect("negate keep mask"))
                .expect("filter interval evicted");
        let target = if is_left { &mut self.left_buffered } else { &mut self.right_buffered };
        *target = if kept.num_rows() > 0 { vec![kept] } else { Vec::new() };

        let rid_index = all.num_columns() - 1;
        let evicted_rids = evicted.column(rid_index).as_any().downcast_ref::<Int64Array>().expect("rid");
        let this_outer =
            if is_left { self.join_type.left_is_outer() } else { self.join_type.right_is_outer() };
        let matched = if is_left { &mut self.left_matched } else { &mut self.right_matched };
        // Read match flags before dropping them.
        let unmatched_mask: BooleanArray = (0..evicted.num_rows())
            .map(|i| Some(this_outer && !matched.contains(&evicted_rids.value(i))))
            .collect();
        for i in 0..evicted.num_rows() {
            matched.remove(&evicted_rids.value(i));
        }
        if !this_outer {
            return None;
        }
        let unmatched = filter_record_batch(&evicted, &unmatched_mask).expect("filter unmatched evicted");
        if unmatched.num_rows() == 0 {
            return None;
        }
        Some(self.null_pad(&unmatched, is_left))
    }

    /// Builds the null-padded output `[left data.., right data..]` (columns `c0..`) for unmatched rows
    /// of one side: the side's data columns (the trailing `__rowid__` dropped) beside all-null columns
    /// for the other side.
    fn null_pad(&self, rows: &RecordBatch, is_left: bool) -> RecordBatch {
        let left_types: Vec<DataType> =
            self.left_data_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let right_types: Vec<DataType> =
            self.right_data_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        build_null_pad(rows, &left_types, &right_types, is_left)
    }

    /// Serializes both buffers and (for an outer join) the per-side matched row-id sets, length-framed.
    fn snapshot(&self) -> Vec<u8> {
        let buf = |is_left: bool, buffered: &[RecordBatch]| -> Vec<u8> {
            if buffered.is_empty() {
                Vec::new()
            } else {
                write_ipc(&concat_batches(&self.buf_schema(is_left), buffered.iter()).expect("concat buf"))
            }
        };
        let mut out = Vec::new();
        for section in [
            buf(true, &self.left_buffered),
            buf(false, &self.right_buffered),
            serialize_id_set(&self.left_matched),
            serialize_id_set(&self.right_matched),
        ] {
            out.extend_from_slice(&(section.len() as u32).to_le_bytes());
            out.extend_from_slice(&section);
        }
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
        predicate: Option<JoinPredicate>,
        join_type: JoinKind,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
        bytes: &[u8],
    ) -> Self {
        let mut joiner = IntervalJoiner::new(
            left_keys,
            right_keys,
            left_time,
            right_time,
            lower,
            upper,
            predicate,
            join_type,
            left_data_schema,
            right_data_schema,
        );
        let sections = read_framed_sections(bytes);
        joiner.left_buffered = read_ipc_if_present(&sections[0]);
        joiner.right_buffered = read_ipc_if_present(&sections[1]);
        joiner.left_matched = deserialize_id_set(&sections[2]);
        joiner.right_matched = deserialize_id_set(&sections[3]);
        // Resume the id counters past any live buffered row (evicted ids are gone, so reuse is safe).
        joiner.left_next_id = max_rowid(&joiner.left_buffered) + 1;
        joiner.right_next_id = max_rowid(&joiner.right_buffered) + 1;
        joiner
    }
}

/// An empty batch (no schema), the joiner's "nothing to emit" result.
fn empty_batch() -> RecordBatch {
    RecordBatch::new_empty(Arc::new(Schema::empty()))
}

const ROW_ID_COLUMN: &str = "__rowid__";

/// A data schema plus a trailing non-null `__rowid__` Int64 field.
fn with_rowid_schema(data: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = data.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(ROW_ID_COLUMN, DataType::Int64, false));
    Arc::new(Schema::new(fields))
}

/// Appends a monotonic Int64 `__rowid__` column to a data batch, advancing the counter.
fn append_rowids(batch: &RecordBatch, next_id: &mut i64) -> RecordBatch {
    let n = batch.num_rows() as i64;
    let ids = Int64Array::from((0..n).map(|i| *next_id + i).collect::<Vec<_>>());
    *next_id += n;
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(ids));
    RecordBatch::try_new(with_rowid_schema(&batch.schema()), columns).expect("append rowids")
}

/// All-null columns of the given types, `n` rows each — to null-pad the absent side of an outer row.
fn null_columns(types: &[DataType], n: usize) -> Vec<ArrayRef> {
    types.iter().map(|t| new_null_array(t, n)).collect()
}

/// Null-pads the rows of a closed window-join side whose transient row-id (== row index) is not in
/// `matched`, or None when every row matched. Used by the window join, where a window's rows all close
/// together so match state is known within the flush.
fn unmatched_null_pad(
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
fn build_null_pad(
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
fn max_rowid(buffered: &[RecordBatch]) -> i64 {
    buffered
        .iter()
        .flat_map(|b| {
            let rid = b.column(b.num_columns() - 1).as_any().downcast_ref::<Int64Array>().expect("rid");
            (0..rid.len()).map(|i| rid.value(i)).collect::<Vec<_>>()
        })
        .max()
        .unwrap_or(-1)
}

/// Serializes a set of row-ids as an IPC batch of one Int64 `id` column (empty bytes when empty).
fn serialize_id_set(ids: &HashSet<i64>) -> Vec<u8> {
    if ids.is_empty() {
        return Vec::new();
    }
    let array = Int64Array::from(ids.iter().copied().collect::<Vec<_>>());
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    write_ipc(&RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("id-set batch"))
}

fn deserialize_id_set(bytes: &[u8]) -> HashSet<i64> {
    let mut set = HashSet::new();
    for batch in read_ipc_if_present(bytes) {
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().expect("id column");
        for i in 0..ids.len() {
            set.insert(ids.value(i));
        }
    }
    set
}

/// Reads length-framed byte sections (`[u32 len][bytes]` repeated) into a vector.
fn read_framed_sections(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut sections = Vec::new();
    let mut cursor = 0usize;
    while cursor + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().expect("section len")) as usize;
        cursor += 4;
        sections.push(bytes[cursor..cursor + len].to_vec());
        cursor += len;
    }
    sections
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

/// Builds the residual `JoinFilter` for a time-bounded join: the interval bounds (if any) AND the
/// residual non-equi predicate (if any), over the full joined `[left.., right..]` schema. Returns
/// None when neither is present. The intermediate schema is the joined schema (columns `c0..`), so a
/// predicate compiled against that schema and the interval bounds (referencing the two rowtime
/// columns by their joined index) share one filter; `column_indices` maps each joined column back to
/// its side. The interval bounds are `left.rt >= right.rt + lower AND left.rt <= right.rt + upper`,
/// expressed over the rowtime columns directly so the comparison works on the timestamp type.
fn residual_filter(
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    interval: Option<(usize, usize, i64, i64)>,
    predicate: Option<&mut JoinPredicate>,
) -> Option<JoinFilter> {
    let left_n = left_schema.fields().len();
    let right_n = right_schema.fields().len();
    let intermediate = UpdatingJoiner::joined_schema(left_schema, right_schema);
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

/// The interval-bounds conjunct `joined[left_rt] BETWEEN joined[right_rt] + lower AND + upper`, built
/// against the joined intermediate schema. `right_type` is the right rowtime's type, which the bound
/// offset must match (arrow rejects a timestamp plus a duration of a different unit).
fn interval_bounds_expr(
    intermediate: &SchemaRef,
    left_rt: usize,
    right_rt: usize,
    right_type: &DataType,
    lower: i64,
    upper: i64,
) -> Arc<dyn PhysicalExpr> {
    use arrow::datatypes::TimeUnit;
    let offset = |millis: i64| -> ScalarValue {
        match right_type {
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
    let left_col: Arc<dyn PhysicalExpr> =
        Arc::new(Column::new(intermediate.field(left_rt).name(), left_rt));
    let right_col: Arc<dyn PhysicalExpr> =
        Arc::new(Column::new(intermediate.field(right_rt).name(), right_rt));
    let bound = |millis: i64| -> Arc<dyn PhysicalExpr> {
        binary(right_col.clone(), Operator::Plus, lit(offset(millis)), intermediate)
            .expect("failed to build interval bound")
    };
    let ge = binary(left_col.clone(), Operator::GtEq, bound(lower), intermediate)
        .expect("failed to build lower bound");
    let le = binary(left_col.clone(), Operator::LtEq, bound(upper), intermediate)
        .expect("failed to build upper bound");
    binary(ge, Operator::And, le, intermediate).expect("failed to build interval and")
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
    predicate: Option<JoinPredicate>,
    join_type: JoinKind,
    // Eager data schemas, seeded at construction so an outer join can type the null-padding for a side
    // that never saw a row.
    left_data_schema: SchemaRef,
    right_data_schema: SchemaRef,
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
        predicate: Option<JoinPredicate>,
        join_type: JoinKind,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
    ) -> Self {
        WindowJoiner {
            left_keys,
            right_keys,
            left_wstart,
            left_wend,
            right_wstart,
            right_wend,
            predicate,
            join_type,
            left_data_schema,
            right_data_schema,
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

    /// Joins and evicts the windows the watermark has closed. For an outer join the unmatched rows of
    /// the closed windows are null-padded here too: because a window's rows on both sides close in the
    /// same flush, the INNER join over the closed rows sees every potential match, so a closed row that
    /// does not appear in it never matched. Empty batch when nothing is emitted.
    fn flush(&mut self, watermark: i64) -> RecordBatch {
        let left = Self::split_closed(&mut self.left_buffered, &self.left_schema, self.left_wend, watermark);
        let right =
            Self::split_closed(&mut self.right_buffered, &self.right_schema, self.right_wend, watermark);
        // Join on the user keys plus the window bounds, so only rows of the same window match.
        let mut on: Vec<(usize, usize)> =
            self.left_keys.iter().zip(&self.right_keys).map(|(&l, &r)| (l, r)).collect();
        on.push((self.left_wstart, self.right_wstart));
        on.push((self.left_wend, self.right_wend));
        let filter =
            residual_filter(&self.left_data_schema, &self.right_data_schema, None, self.predicate.as_mut());

        if self.join_type == JoinKind::Inner {
            return match (left, right) {
                (Some(left), Some(right)) if left.num_rows() > 0 && right.num_rows() > 0 => {
                    hash_join_inner(left, right, &on, filter)
                }
                _ => empty_batch(),
            };
        }

        // Outer: tag the closed rows with transient row-ids (== row index), join, and from the matched
        // row-ids null-pad the closed rows of each outer side that never appeared in a pair.
        let left_closed = left.filter(|b| b.num_rows() > 0);
        let right_closed = right.filter(|b| b.num_rows() > 0);
        let left_types: Vec<DataType> =
            self.left_data_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let right_types: Vec<DataType> =
            self.right_data_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let mut outputs: Vec<RecordBatch> = Vec::new();
        let mut matched_left: HashSet<i64> = HashSet::new();
        let mut matched_right: HashSet<i64> = HashSet::new();
        if let (Some(left), Some(right)) = (&left_closed, &right_closed) {
            let (mut lc, mut rc) = (0i64, 0i64);
            let joined = hash_join_inner(
                append_rowids(left, &mut lc),
                append_rowids(right, &mut rc),
                &on,
                filter,
            );
            if joined.num_rows() > 0 {
                let total = joined.num_columns();
                let lrid = joined.column(left_types.len()).as_any().downcast_ref::<Int64Array>().expect("lrid");
                let rrid = joined.column(total - 1).as_any().downcast_ref::<Int64Array>().expect("rrid");
                for i in 0..joined.num_rows() {
                    matched_left.insert(lrid.value(i));
                    matched_right.insert(rrid.value(i));
                }
                let keep: Vec<usize> =
                    (0..left_types.len()).chain(left_types.len() + 1..total - 1).collect();
                let fields: Vec<Field> = keep
                    .iter()
                    .enumerate()
                    .map(|(j, &i)| Field::new(format!("c{j}"), joined.schema().field(i).data_type().clone(), true))
                    .collect();
                let columns: Vec<ArrayRef> = keep.iter().map(|&i| joined.column(i).clone()).collect();
                outputs.push(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("window pairs"));
            }
        }
        if self.join_type.left_is_outer() {
            if let Some(left) = &left_closed {
                if let Some(pad) = unmatched_null_pad(left, &matched_left, &left_types, &right_types, true) {
                    outputs.push(pad);
                }
            }
        }
        if self.join_type.right_is_outer() {
            if let Some(right) = &right_closed {
                if let Some(pad) = unmatched_null_pad(right, &matched_right, &left_types, &right_types, false) {
                    outputs.push(pad);
                }
            }
        }
        match outputs.len() {
            0 => empty_batch(),
            1 => outputs.pop().expect("one output"),
            _ => concat_batches(&outputs[0].schema(), outputs.iter()).expect("concat window outputs"),
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
        predicate: Option<JoinPredicate>,
        join_type: JoinKind,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
        bytes: &[u8],
    ) -> Self {
        let mut joiner = WindowJoiner::new(
            left_keys,
            right_keys,
            left_wstart,
            left_wend,
            right_wstart,
            right_wend,
            predicate,
            join_type,
            left_data_schema,
            right_data_schema,
        );
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

/// Reads a batch's data schema (every column except a trailing `$row_kind$`, if present).
fn data_schema(batch: &RecordBatch) -> SchemaRef {
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
struct RowMeta {
    count: i64,
    num_assoc: i32,
}

/// The join family the updating joiner runs. INNER carries no degree; LEFT/RIGHT/FULL maintain a
/// per-row degree on the outer side(s); SEMI/ANTI maintain a degree on the left (probe) side.
#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Semi,
    Anti,
}

impl JoinKind {
    fn from_code(code: i32) -> Self {
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

    fn left_is_outer(self) -> bool {
        matches!(self, JoinKind::LeftOuter | JoinKind::FullOuter)
    }

    fn right_is_outer(self) -> bool {
        matches!(self, JoinKind::RightOuter | JoinKind::FullOuter)
    }

    fn is_semi_anti(self) -> bool {
        matches!(self, JoinKind::Semi | JoinKind::Anti)
    }
}

/// One associated row gathered from the other side when probing for matches: a clone of the stored
/// row and the match-degree it carried at probe time (`-1` when the other side keeps no degree). The
/// degree is captured once per distinct row and shared across its copies, exactly as Flink's
/// `OuterJoinRecordStateViews` iterator reuses one `numOfAssociations` for a record's appear-times.
struct OuterRecord {
    record: JoinRow,
    num_assoc: i32,
}

/// A residual non-equi join predicate (Flink's `joinSpec.getNonEquiCondition()`), encoded in the same
/// pre-order form as the filter engine and evaluated over candidate `[left.., right..]` pairs. A pair
/// is a match only when the predicate is true (null ⇒ not a match, as a join condition treats it), so
/// it gates which rows feed the degree and the emitted output — mirroring Flink's `condition.apply`
/// filter inside the associated-records iterator. The compiled expression is built once against the
/// joined schema and cached.
struct JoinPredicate {
    kinds: Vec<i64>,
    payload: Vec<i64>,
    child_counts: Vec<i64>,
    longs: Vec<i64>,
    doubles: Vec<f64>,
    strings: Vec<Option<String>>,
    compiled: Option<Arc<dyn PhysicalExpr>>,
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

    /// Evaluates the predicate over the candidate `[left.., right..]` rows (laid out by `schema`),
    /// returning one boolean per row (a null result is `false` — not a match).
    fn evaluate(&mut self, schema: &SchemaRef, rows: &[JoinRow]) -> Vec<bool> {
        let types: Vec<DataType> = schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let columns: Vec<ArrayRef> = (0..types.len())
            .map(|j| scalars_to_array(rows.iter().map(|r| r[j].clone()).collect(), &types[j]))
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .expect("failed to build join-predicate batch");
        let predicate = self.compiled(schema);
        let evaluated = predicate
            .evaluate(&batch)
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
struct UpdatingJoiner {
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    kind: JoinKind,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    predicate: Option<JoinPredicate>,
    left_state: HashMap<GroupKey, HashMap<JoinRow, RowMeta>>,
    right_state: HashMap<GroupKey, HashMap<JoinRow, RowMeta>>,
}

impl UpdatingJoiner {
    fn new(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        kind: JoinKind,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        predicate: Option<JoinPredicate>,
    ) -> Self {
        UpdatingJoiner {
            left_keys,
            right_keys,
            kind,
            left_schema,
            right_schema,
            predicate,
            left_state: HashMap::new(),
            right_state: HashMap::new(),
        }
    }

    /// The joined `[left fields.., right fields..]` schema (columns named `c0..`) the non-equi
    /// predicate's input refs index into.
    fn joined_schema(left_schema: &SchemaRef, right_schema: &SchemaRef) -> SchemaRef {
        let fields: Vec<Field> = left_schema
            .fields()
            .iter()
            .chain(right_schema.fields().iter())
            .enumerate()
            .map(|(j, f)| Field::new(format!("c{j}"), f.data_type().clone(), true))
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// Drops the candidate matches whose `[left.., right..]` pair fails the residual non-equi
    /// predicate, so only condition-satisfying rows feed the degree and the emitted output (Flink's
    /// `condition.apply` filter). A no-op when there is no predicate.
    fn filter_associated(&mut self, full: &JoinRow, is_left: bool, associated: &mut Vec<OuterRecord>) {
        if associated.is_empty() || self.predicate.is_none() {
            return;
        }
        let joined = Self::joined_schema(&self.left_schema, &self.right_schema);
        let pairs: Vec<JoinRow> = associated
            .iter()
            .map(|other| {
                if is_left {
                    full.iter().chain(&other.record).cloned().collect()
                } else {
                    other.record.iter().chain(full).cloned().collect()
                }
            })
            .collect();
        let mask = self.predicate.as_mut().expect("predicate present").evaluate(&joined, &pairs);
        let mut keep = mask.into_iter();
        associated.retain(|_| keep.next().unwrap_or(false));
    }

    fn left_types(&self) -> Vec<DataType> {
        self.left_schema.fields().iter().map(|f| f.data_type().clone()).collect()
    }

    fn right_types(&self) -> Vec<DataType> {
        self.right_schema.fields().iter().map(|f| f.data_type().clone()).collect()
    }

    /// A null row sized for one side (every column a typed NULL), used to null-pad the absent side.
    fn null_row(types: &[DataType]) -> JoinRow {
        types.iter().map(null_scalar).collect()
    }

    /// Gathers the matching rows on `other_state` for `key`, expanding each distinct row by its
    /// appear-times (so multiplicity is preserved) and capturing its degree once per distinct row. A
    /// null in the equi-key matches nothing (Flink's null-filtering equi semantics), so an empty key
    /// match means "no associated rows".
    fn associated(
        other_state: &HashMap<GroupKey, HashMap<JoinRow, RowMeta>>,
        key: &GroupKey,
    ) -> Vec<OuterRecord> {
        if key.iter().any(ScalarValue::is_null) {
            return Vec::new();
        }
        let mut out = Vec::new();
        if let Some(bucket) = other_state.get(key) {
            for (row, meta) in bucket.iter() {
                for _ in 0..meta.count.max(0) {
                    out.push(OuterRecord { record: row.clone(), num_assoc: meta.num_assoc });
                }
            }
        }
        out
    }

    /// `state.addRecord(record, num_assoc)` — bumps appear-times and (re)sets the degree, as Flink's
    /// no-unique-key `OuterJoinRecordStateView.addRecord`.
    fn add_record(
        state: &mut HashMap<GroupKey, HashMap<JoinRow, RowMeta>>,
        key: &GroupKey,
        row: &JoinRow,
        num_assoc: i32,
    ) {
        let bucket = state.entry(key.clone()).or_default();
        bucket
            .entry(row.clone())
            .and_modify(|m| {
                m.count += 1;
                m.num_assoc = num_assoc;
            })
            .or_insert(RowMeta { count: 1, num_assoc });
    }

    /// `state.updateNumOfAssociations(record, num_assoc)` — sets the degree of an existing row.
    fn update_num_assoc(
        state: &mut HashMap<GroupKey, HashMap<JoinRow, RowMeta>>,
        key: &GroupKey,
        row: &JoinRow,
        num_assoc: i32,
    ) {
        let bucket = state.entry(key.clone()).or_default();
        bucket
            .entry(row.clone())
            .and_modify(|m| m.num_assoc = num_assoc)
            .or_insert(RowMeta { count: 1, num_assoc });
    }

    /// `state.retractRecord(record)` — drops one appear-time, removing the row (and emptied key) at 0.
    fn retract_record(
        state: &mut HashMap<GroupKey, HashMap<JoinRow, RowMeta>>,
        key: &GroupKey,
        row: &JoinRow,
    ) {
        if let Some(bucket) = state.get_mut(key) {
            if let Some(meta) = bucket.get_mut(row) {
                meta.count -= 1;
                if meta.count <= 0 {
                    bucket.remove(row);
                }
            }
            if bucket.is_empty() {
                state.remove(key);
            }
        }
    }

    /// Folds an input batch into its side and emits the join changelog it produces.
    fn push(&mut self, batch: &RecordBatch, is_left: bool) -> RecordBatch {
        let arity = data_arity(batch);
        let key_indices = if is_left { &self.left_keys } else { &self.right_keys };
        let key_arrays: Vec<&ArrayRef> = key_indices.iter().map(|&i| batch.column(i)).collect();
        let data_arrays: Vec<&ArrayRef> = (0..arity).map(|i| batch.column(i)).collect();
        let row_kinds = row_kind_column(batch);

        let mut out_rows: Vec<JoinRow> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();

        for row in 0..batch.num_rows() {
            // Absent `$row_kind$` (insert-only columnar input) ⇒ every row is an INSERT.
            let kind = row_kinds.map_or(0, |kinds| kinds.value(row));
            let key = read_key(&key_arrays, row);
            let full: JoinRow = data_arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a, row).expect("join row scalar"))
                .collect();
            if self.kind.is_semi_anti() {
                self.process_semi_anti(&key, &full, kind, is_left, &mut out_rows, &mut out_kinds);
            } else {
                self.process_inner_outer(&key, &full, kind, is_left, &mut out_rows, &mut out_kinds);
            }
        }

        if out_rows.is_empty() {
            return RecordBatch::new_empty(Arc::new(Schema::empty()));
        }
        let types: Vec<DataType> = if self.kind.is_semi_anti() {
            self.left_types()
        } else {
            self.left_types().into_iter().chain(self.right_types()).collect()
        };
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

    /// INNER/LEFT/RIGHT/FULL — a faithful port of `StreamingJoinOperator.processElement`. `is_left`
    /// is whether the input arrived on the left; `kind` is the input row's `RowKind` byte
    /// (0=+I,1=-U,2=+U,3=-D). Output rows are `[left cols.., right cols..]`.
    fn process_inner_outer(
        &mut self,
        key: &GroupKey,
        full: &JoinRow,
        kind: i8,
        is_left: bool,
        out_rows: &mut Vec<JoinRow>,
        out_kinds: &mut Vec<i8>,
    ) {
        let accumulate = kind == 0 || kind == 2;
        let input_is_outer = if is_left { self.kind.left_is_outer() } else { self.kind.right_is_outer() };
        let other_is_outer = if is_left { self.kind.right_is_outer() } else { self.kind.left_is_outer() };
        let right_nulls = Self::null_row(&self.right_types());
        let left_nulls = Self::null_row(&self.left_types());
        // `output(input, other)` builds `[left.., right..]` placing the input on its side.
        let paired = |other: &JoinRow| -> JoinRow {
            if is_left {
                full.iter().chain(other).cloned().collect()
            } else {
                other.iter().chain(full).cloned().collect()
            }
        };
        // `outputNullPadding(input)` — the input row with the other side nulled.
        let input_padded: JoinRow = if is_left {
            full.iter().chain(&right_nulls).cloned().collect()
        } else {
            left_nulls.iter().chain(full).cloned().collect()
        };
        // `outputNullPadding(other)` — an other-side row with the input side nulled.
        let other_padded = |other: &JoinRow| -> JoinRow {
            if is_left {
                left_nulls.iter().chain(other).cloned().collect()
            } else {
                other.iter().chain(&right_nulls).cloned().collect()
            }
        };

        // Gather the matching other-side rows (immutable read), then drop those failing the residual
        // non-equi predicate — Flink's `condition.apply` filter inside the associated iterator. Done
        // before the per-side mutations below so no state borrow is held across the predicate eval.
        let mut associated = Self::associated(
            if is_left { &self.right_state } else { &self.left_state },
            key,
        );
        self.filter_associated(full, is_left, &mut associated);

        if accumulate {
            if input_is_outer {
                if associated.is_empty() {
                    out_rows.push(input_padded);
                    out_kinds.push(0); // +I[record+null]
                    Self::add_record(self.input_state(is_left), key, full, 0);
                } else {
                    let num = associated.len() as i32;
                    for other in &associated {
                        if other_is_outer {
                            if other.num_assoc == 0 {
                                out_rows.push(other_padded(&other.record));
                                out_kinds.push(3); // -D[null+other]
                            }
                            Self::update_num_assoc(self.other_state(is_left), key, &other.record, other.num_assoc + 1);
                        }
                        out_rows.push(paired(&other.record));
                        out_kinds.push(0); // +I[record+other]
                    }
                    Self::add_record(self.input_state(is_left), key, full, num);
                }
            } else {
                Self::add_record(self.input_state(is_left), key, full, -1);
                for other in &associated {
                    if other_is_outer {
                        if other.num_assoc == 0 {
                            out_rows.push(other_padded(&other.record));
                            out_kinds.push(3); // -D[null+other]
                        }
                        Self::update_num_assoc(self.other_state(is_left), key, &other.record, other.num_assoc + 1);
                        out_rows.push(paired(&other.record));
                        out_kinds.push(0); // +I[record+other]
                    } else {
                        out_rows.push(paired(&other.record));
                        out_kinds.push(kind); // +I/+U[record+other] (input RowKind)
                    }
                }
            }
        } else {
            Self::retract_record(self.input_state(is_left), key, full);
            if associated.is_empty() {
                if input_is_outer {
                    out_rows.push(input_padded);
                    out_kinds.push(3); // -D[record+null]
                }
            } else {
                for other in &associated {
                    out_rows.push(paired(&other.record));
                    out_kinds.push(if input_is_outer { 3 } else { kind }); // -D / -D|-U (input RowKind)
                    if other_is_outer {
                        if other.num_assoc == 1 {
                            out_rows.push(other_padded(&other.record));
                            out_kinds.push(0); // +I[null+other]
                        }
                        Self::update_num_assoc(self.other_state(is_left), key, &other.record, other.num_assoc - 1);
                    }
                }
            }
        }
    }

    /// The state map for the arriving (input) side.
    fn input_state(&mut self, is_left: bool) -> &mut HashMap<GroupKey, HashMap<JoinRow, RowMeta>> {
        if is_left { &mut self.left_state } else { &mut self.right_state }
    }

    /// The state map for the side opposite the arriving one.
    fn other_state(&mut self, is_left: bool) -> &mut HashMap<GroupKey, HashMap<JoinRow, RowMeta>> {
        if is_left { &mut self.right_state } else { &mut self.left_state }
    }

    /// SEMI/ANTI — a faithful port of `StreamingSemiAntiJoinOperator`. The left side carries the
    /// degree (it is the side whose rows are emitted); the right side is plain. Output rows are the
    /// left columns only.
    fn process_semi_anti(
        &mut self,
        key: &GroupKey,
        full: &JoinRow,
        kind: i8,
        is_left: bool,
        out_rows: &mut Vec<JoinRow>,
        out_kinds: &mut Vec<i8>,
    ) {
        let accumulate = kind == 0 || kind == 2;
        let is_anti = self.kind == JoinKind::Anti;
        if is_left {
            // processElement1: emit the input row when it has (semi) / lacks (anti) a match, then
            // record it with its current match count as its degree.
            let mut associated = Self::associated(&self.right_state, key);
            self.filter_associated(full, true, &mut associated);
            let matched = !associated.is_empty();
            if matched != is_anti {
                out_rows.push(full.clone());
                out_kinds.push(kind); // forward input RowKind
            }
            if accumulate {
                Self::add_record(&mut self.left_state, key, full, associated.len() as i32);
            } else {
                Self::retract_record(&mut self.left_state, key, full);
            }
        } else {
            // processElement2: a right row flips associated left rows' degree across 0↔1, emitting or
            // retracting them (semi) or the inverse (anti).
            let mut associated = Self::associated(&self.left_state, key);
            self.filter_associated(full, false, &mut associated);
            if accumulate {
                Self::add_record(&mut self.right_state, key, full, -1);
                for other in &associated {
                    if other.num_assoc == 0 {
                        // anti: -D[left]; semi: +I/+U[left] (input RowKind)
                        out_rows.push(other.record.clone());
                        out_kinds.push(if is_anti { 3 } else { kind });
                    }
                    Self::update_num_assoc(&mut self.left_state, key, &other.record, other.num_assoc + 1);
                }
            } else {
                Self::retract_record(&mut self.right_state, key, full);
                for other in &associated {
                    if other.num_assoc == 1 {
                        // semi: -D/-U[left] (input RowKind); anti: +I[left]
                        out_rows.push(other.record.clone());
                        out_kinds.push(if is_anti { 0 } else { kind });
                    }
                    Self::update_num_assoc(&mut self.left_state, key, &other.record, other.num_assoc - 1);
                }
            }
        }
    }

    /// Serializes one side's multiset as `[data cols.., __count__, __assoc__]` (one row per distinct
    /// live row), or no bytes when the side has no rows yet.
    fn serialize_side(&self, is_left: bool) -> Vec<u8> {
        let (schema, state) =
            if is_left { (&self.left_schema, &self.left_state) } else { (&self.right_schema, &self.right_state) };
        let mut rows: Vec<&JoinRow> = Vec::new();
        let mut counts: Vec<i64> = Vec::new();
        let mut assocs: Vec<i32> = Vec::new();
        for bucket in state.values() {
            for (row, meta) in bucket.iter() {
                rows.push(row);
                counts.push(meta.count);
                assocs.push(meta.num_assoc);
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
        fields.push(Field::new("__assoc__", DataType::Int32, false));
        columns.push(Arc::new(Int32Array::from(assocs)));
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

    fn restore(
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        kind: JoinKind,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        predicate: Option<JoinPredicate>,
        bytes: &[u8],
    ) -> Self {
        let mut joiner =
            UpdatingJoiner::new(left_keys, right_keys, kind, left_schema, right_schema, predicate);
        let left_len = u32::from_le_bytes(bytes[0..4].try_into().expect("snapshot len")) as usize;
        joiner.load_side(true, &bytes[4..4 + left_len]);
        joiner.load_side(false, &bytes[4 + left_len..]);
        joiner
    }

    fn load_side(&mut self, is_left: bool, bytes: &[u8]) {
        for batch in read_ipc_if_present(bytes) {
            // The snapshot side batch is `[data cols.., __count__, __assoc__]`; the data columns are
            // all but the two trailing bookkeeping columns.
            let arity = batch.num_columns() - 2;
            let key_indices = if is_left { &self.left_keys } else { &self.right_keys };
            let key_arrays: Vec<&ArrayRef> = key_indices.iter().map(|&i| batch.column(i)).collect();
            let counts = column_i64(&batch, "__count__");
            let assocs = column_i32(&batch, "__assoc__");
            let state = if is_left { &mut self.left_state } else { &mut self.right_state };
            for row in 0..batch.num_rows() {
                let key = read_key(&key_arrays, row);
                let full: JoinRow = (0..arity)
                    .map(|i| ScalarValue::try_from_array(batch.column(i), row).expect("join row scalar"))
                    .collect();
                state.entry(key).or_default().insert(
                    full,
                    RowMeta { count: counts.value(row), num_assoc: assocs.value(row) },
                );
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

/// Append-only streaming Top-N (`ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) <= N`). Per partition
/// it keeps the top `limit` rows sorted by the order keys (ties in arrival order), exactly the host's
/// append-only bounded buffer.
///
/// With the rank number **not** projected (`output_rank_number = false`): on each input row it
/// inserts into the buffer; if that overflows the limit it drops the last (rank N+1) — emitting
/// nothing if the new row is the one dropped, else a DELETE of the displaced row — and otherwise
/// emits the new row as an INSERT. Output is the input columns plus the `$row_kind$` byte.
///
/// With the rank number projected (`output_rank_number = true`): a row entering at rank `r` shifts
/// everyone below it down by one, so the operator emits the cascade Flink's `AppendOnlyTopNFunction`
/// does — for each rank from `r` to the buffer end, UPDATE_BEFORE(old occupant)/UPDATE_AFTER(new
/// occupant), and an INSERT for the row taking a brand-new rank; a row pushed past `limit` is
/// retracted by the UPDATE_BEFORE at the last rank (no separate delete). Output appends the rank
/// (a bigint) before the `$row_kind$` byte.
struct TopNRanker {
    partition_columns: Vec<usize>,
    sort_columns: Vec<SortColumn>,
    limit: i64,
    output_rank_number: bool,
    schema: Option<SchemaRef>,
    groups: HashMap<GroupKey, Vec<JoinRow>>,
}

impl TopNRanker {
    fn new(
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        output_rank_number: bool,
    ) -> Self {
        TopNRanker {
            partition_columns,
            sort_columns,
            limit,
            output_rank_number,
            schema: None,
            groups: HashMap::new(),
        }
    }

    fn push(&mut self, batch: &RecordBatch) -> RecordBatch {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        let partition_arrays: Vec<&ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i)).collect();
        let data_arrays: Vec<&ArrayRef> = (0..arity).map(|i| batch.column(i)).collect();

        let mut out_rows: Vec<JoinRow> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        let mut out_ranks: Vec<i64> = Vec::new();

        for row in 0..batch.num_rows() {
            let key = read_key(&partition_arrays, row);
            let full: JoinRow = data_arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a, row).expect("top-n row scalar"))
                .collect();
            let sort = &self.sort_columns;
            let limit = self.limit as usize;
            let buffer = self.groups.entry(key).or_default();
            // Insert after any rows that order equal-or-before, preserving arrival order for ties.
            let pos = buffer.partition_point(|r| compare_rows(r, &full, sort) != std::cmp::Ordering::Greater);

            if self.output_rank_number {
                if pos >= limit {
                    continue; // beyond rank N — the new row never enters the top-N
                }
                let old_len = buffer.len();
                buffer.insert(pos, full.clone());
                // Cascade from the new row's rank to the buffer end (capped at the limit): each rank's
                // occupant changes, so retract the old and append the new; a brand-new rank inserts.
                let upper = (old_len + 1).min(limit); // highest 1-based rank to emit
                for rank in (pos + 1)..=upper {
                    let new_occupant = buffer[rank - 1].clone();
                    if rank <= old_len {
                        out_rows.push(buffer[rank].clone()); // old occupant (shifted down by one)
                        out_kinds.push(1); // -U
                        out_ranks.push(rank as i64);
                        out_rows.push(new_occupant);
                        out_kinds.push(2); // +U
                        out_ranks.push(rank as i64);
                    } else {
                        out_rows.push(new_occupant);
                        out_kinds.push(0); // +I a brand-new rank
                        out_ranks.push(rank as i64);
                    }
                }
                if buffer.len() > limit {
                    buffer.truncate(limit); // the row past N was retracted by the -U at rank=limit
                }
            } else {
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
        }
        self.emit(out_rows, out_kinds, out_ranks)
    }

    fn emit(&self, out_rows: Vec<JoinRow>, out_kinds: Vec<i8>, out_ranks: Vec<i64>) -> RecordBatch {
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
        if self.output_rank_number {
            fields.push(Field::new("w0$o0", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(out_ranks)));
        }
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
        output_rank_number: bool,
        bytes: &[u8],
    ) -> Self {
        let mut ranker = TopNRanker::new(partition_columns, sort_columns, limit, output_rank_number);
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

/// Retracting streaming Top-N — Flink's `RetractableTopNFunction`: a `ROW_NUMBER() OVER (PARTITION BY
/// … ORDER BY …) <= N` over a **changelog** input (e.g. a Top-N of a GROUP BY result). Unlike the
/// append-only ranker it keeps the **full** sorted buffer per key (never truncated to N), so when a
/// top-N row is retracted the row that was at rank N+1 can be promoted into the top-N.
///
/// Each input row accumulates (`+I`/`+U`) by inserting into the sorted buffer or retracts (`-U`/`-D`)
/// by removing the first full-row-equal match. The emitted changelog is then the **diff of the top-N
/// before vs after** the mutation: with the rank number projected, compared by rank position (a
/// changed occupant → `-U`(old)/`+U`(new), a newly-occupied rank → `+I`, a vacated rank → `-D`);
/// without it, compared as a row multiset (rows that left → `-D`, rows that entered → `+I`). This
/// single diff covers insert and retract and collapses to the same materialized result as Flink's
/// per-case cascade.
struct RetractableTopNRanker {
    partition_columns: Vec<usize>,
    sort_columns: Vec<SortColumn>,
    /// Rank window: output ranks `[offset+1, limit]` (1-based), i.e. buffer indices `[offset, limit)`.
    /// `offset = rankStart - 1` (0 for the common no-`OFFSET` case); `limit = rankEnd`.
    offset: i64,
    limit: i64,
    output_rank_number: bool,
    schema: Option<SchemaRef>,
    groups: HashMap<GroupKey, Vec<JoinRow>>,
}

impl RetractableTopNRanker {
    fn new(
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        offset: i64,
        limit: i64,
        output_rank_number: bool,
    ) -> Self {
        RetractableTopNRanker {
            partition_columns,
            sort_columns,
            offset,
            limit,
            output_rank_number,
            schema: None,
            groups: HashMap::new(),
        }
    }

    fn push(&mut self, batch: &RecordBatch) -> RecordBatch {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        let partition_arrays: Vec<&ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i)).collect();
        let data_arrays: Vec<&ArrayRef> = (0..arity).map(|i| batch.column(i)).collect();
        let row_kinds = row_kind_column(batch);
        // Output window: buffer indices [offset, limit) = ranks [offset+1, limit], clamped to len.
        let (offset, limit) = (self.offset as usize, self.limit as usize);

        let mut out_rows: Vec<JoinRow> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        let mut out_ranks: Vec<i64> = Vec::new();

        for row in 0..batch.num_rows() {
            let key = read_key(&partition_arrays, row);
            let full: JoinRow = data_arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a, row).expect("retract top-n row scalar"))
                .collect();
            // +I(0)/+U(2) accumulate; -U(1)/-D(3) retract.
            let retract = matches!(row_kinds.map(|k| k.value(row)).unwrap_or(0), 1 | 3);
            let sort = &self.sort_columns;
            let buffer = self.groups.entry(key).or_default();
            let old_top: Vec<JoinRow> =
                buffer[offset.min(buffer.len())..limit.min(buffer.len())].to_vec();
            if retract {
                if let Some(pos) = buffer.iter().position(|r| *r == full) {
                    buffer.remove(pos);
                }
            } else {
                let pos = buffer
                    .partition_point(|r| compare_rows(r, &full, sort) != std::cmp::Ordering::Greater);
                buffer.insert(pos, full.clone());
            }
            let new_top: Vec<JoinRow> =
                buffer[offset.min(buffer.len())..limit.min(buffer.len())].to_vec();
            self.diff(&old_top, &new_top, &mut out_rows, &mut out_kinds, &mut out_ranks);
        }
        self.emit(out_rows, out_kinds, out_ranks)
    }

    /// Appends the changelog transitioning the top-N from `old_top` to `new_top` (see the struct doc).
    fn diff(
        &self,
        old_top: &[JoinRow],
        new_top: &[JoinRow],
        out_rows: &mut Vec<JoinRow>,
        out_kinds: &mut Vec<i8>,
        out_ranks: &mut Vec<i64>,
    ) {
        if self.output_rank_number {
            for i in 0..old_top.len().max(new_top.len()) {
                let rank = self.offset + i as i64 + 1; // window position i is rank offset+i+1
                match (old_top.get(i), new_top.get(i)) {
                    (Some(o), Some(n)) if o != n => {
                        out_rows.push(o.clone());
                        out_kinds.push(1); // -U the old occupant of this rank
                        out_ranks.push(rank);
                        out_rows.push(n.clone());
                        out_kinds.push(2); // +U the new occupant
                        out_ranks.push(rank);
                    }
                    (Some(_), Some(_)) => {} // rank unchanged
                    (Some(o), None) => {
                        out_rows.push(o.clone());
                        out_kinds.push(3); // -D a rank that lost its occupant
                        out_ranks.push(rank);
                    }
                    (None, Some(n)) => {
                        out_rows.push(n.clone());
                        out_kinds.push(0); // +I a newly-occupied rank
                        out_ranks.push(rank);
                    }
                    (None, None) => {}
                }
            }
        } else {
            // No rank column — only membership matters; diff the two row multisets.
            let mut old_counts: HashMap<&JoinRow, i32> = HashMap::new();
            for r in old_top {
                *old_counts.entry(r).or_insert(0) += 1;
            }
            for r in new_top {
                match old_counts.get_mut(r) {
                    Some(c) if *c > 0 => *c -= 1, // still present — no change
                    _ => {
                        out_rows.push(r.clone());
                        out_kinds.push(0); // +I a row that entered the top-N
                    }
                }
            }
            for (r, count) in old_counts {
                for _ in 0..count {
                    out_rows.push(r.clone());
                    out_kinds.push(3); // -D a row that left the top-N
                }
            }
        }
    }

    fn emit(&self, out_rows: Vec<JoinRow>, out_kinds: Vec<i8>, out_ranks: Vec<i64>) -> RecordBatch {
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
        if self.output_rank_number {
            fields.push(Field::new("w0$o0", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(out_ranks)));
        }
        fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
        columns.push(Arc::new(Int8Array::from(out_kinds)));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .expect("failed to build retract top-n changelog batch")
    }

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
        write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("retract top-n snapshot"),
        )
    }

    fn restore(
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        offset: i64,
        limit: i64,
        output_rank_number: bool,
        bytes: &[u8],
    ) -> Self {
        let mut ranker =
            RetractableTopNRanker::new(partition_columns, sort_columns, offset, limit, output_rank_number);
        for batch in read_ipc_if_present(bytes) {
            ranker.schema = Some(batch.schema());
            let partition_arrays: Vec<&ArrayRef> =
                ranker.partition_columns.iter().map(|&i| batch.column(i)).collect();
            for row in 0..batch.num_rows() {
                let key = read_key(&partition_arrays, row);
                let full: JoinRow = (0..batch.num_columns())
                    .map(|i| ScalarValue::try_from_array(batch.column(i), row).expect("retract top-n scalar"))
                    .collect();
                ranker.groups.entry(key).or_default().push(full); // stored in buffer (sorted) order
            }
        }
        ranker
    }
}

/// The Top-N handle the JVM holds: append-only (insert-only input, bounded buffer) or retracting
/// (changelog input, full buffer). Both push a batch and return a changelog, snapshot, and restore.
enum TopNHandle {
    Append(TopNRanker),
    Retract(RetractableTopNRanker),
}

impl TopNHandle {
    fn push(&mut self, batch: &RecordBatch) -> RecordBatch {
        match self {
            TopNHandle::Append(r) => r.push(batch),
            TopNHandle::Retract(r) => r.push(batch),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        match self {
            TopNHandle::Append(r) => r.snapshot(),
            TopNHandle::Retract(r) => r.snapshot(),
        }
    }
}

/// Append-only keep-first deduplication on a rowtime order — Flink's
/// `RowTimeDeduplicateKeepFirstRowFunction`. Per partition key it keeps the row with the minimum
/// rowtime and emits it exactly once, when a watermark reaches that rowtime; every later row for the
/// key is then ignored, and a row arriving with a rowtime already below the watermark is dropped as
/// late. Insert-only: once a key's candidate fires, no smaller-rowtime row can still arrive (it would
/// be late), so the emitted row is final and never retracted.
///
/// Columnar: the per-key candidates live as a single Arrow batch — one row per pending key — and row
/// data moves only through `filter`/`take`/`concat` kernels, never materialized into scalars. Each
/// batch is reduced to its per-key minimum-rowtime row and merged with the standing candidates; only
/// the key (for grouping) and the rowtime (i64) are read per row, as any keyed reduction must.
struct KeepFirstDeduplicator {
    partition_columns: Vec<usize>,
    rt_column: usize,
    current_watermark: i64,
    /// One row per pending key — that key's minimum-rowtime candidate — awaiting its release.
    pending: Option<RecordBatch>,
    /// Keys whose first row has already been emitted; later rows for them are ignored.
    emitted: std::collections::HashSet<GroupKey>,
    schema: Option<SchemaRef>,
}

impl KeepFirstDeduplicator {
    fn new(partition_columns: Vec<usize>, rt_column: usize) -> Self {
        KeepFirstDeduplicator {
            partition_columns,
            rt_column,
            current_watermark: i64::MIN,
            pending: None,
            emitted: std::collections::HashSet::new(),
            schema: None,
        }
    }

    fn push(&mut self, batch: &RecordBatch) {
        let schema = batch.schema();
        self.schema = Some(schema.clone());
        // Drop late rows (rowtime already below the watermark) with a columnar filter.
        let rt = rt_to_millis(batch.column(self.rt_column));
        let live_mask: BooleanArray =
            rt.iter().map(|v| Some(v.unwrap() >= self.current_watermark)).collect();
        let live = filter_record_batch(batch, &live_mask).expect("dedup late filter");
        // Merge with the standing candidates and reduce to one minimum-rowtime row per pending key.
        let combined = match self.pending.take() {
            Some(prev) => concat_batches(&schema, [&prev, &live]).expect("dedup concat"),
            None => live,
        };
        let reduced = self.min_per_key(&combined);
        self.pending = (reduced.num_rows() > 0).then_some(reduced);
    }

    /// Reduces a batch to one row per non-emitted key: the row with the minimum rowtime, ties going to
    /// the earlier position (candidates precede new rows in `combined`, so a tie keeps the incumbent —
    /// Flink's keep-first rule of replacing only on a strictly smaller rowtime). The winning rows are
    /// gathered with `take`; the row data is never materialized into scalars.
    fn min_per_key(&self, batch: &RecordBatch) -> RecordBatch {
        let key_arrays: Vec<&ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i)).collect();
        let rt = rt_to_millis(batch.column(self.rt_column));
        let mut best: HashMap<GroupKey, (i64, u32)> = HashMap::new();
        for row in 0..batch.num_rows() {
            let key = read_key(&key_arrays, row);
            if self.emitted.contains(&key) {
                continue; // this key's first row already emitted
            }
            let rowtime = rt.value(row);
            match best.get(&key) {
                Some((existing, _)) if *existing <= rowtime => {}
                _ => {
                    best.insert(key, (rowtime, row as u32));
                }
            }
        }
        let mut indices: Vec<u32> = best.into_values().map(|(_, idx)| idx).collect();
        indices.sort_unstable();
        let idx = UInt32Array::from(indices);
        let columns: Vec<ArrayRef> =
            batch.columns().iter().map(|c| take(c, &idx, None).expect("dedup take")).collect();
        RecordBatch::try_new(batch.schema(), columns).expect("dedup compacted batch")
    }

    /// Emits each pending key's candidate whose rowtime the watermark has now reached (insert-only),
    /// records those keys as emitted, and keeps the rest. Both partitions are columnar filters.
    fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.current_watermark = watermark;
        let Some(pending) = self.pending.take() else {
            return self.empty();
        };
        let rt = rt_to_millis(pending.column(self.rt_column));
        let ready_mask: BooleanArray = rt.iter().map(|v| Some(v.unwrap() <= watermark)).collect();
        let ready = filter_record_batch(&pending, &ready_mask).expect("dedup ready filter");
        let not_ready =
            filter_record_batch(&pending, &arrow::compute::not(&ready_mask).expect("dedup not"))
                .expect("dedup keep filter");
        self.pending = (not_ready.num_rows() > 0).then_some(not_ready);
        if ready.num_rows() > 0 {
            let key_arrays: Vec<&ArrayRef> =
                self.partition_columns.iter().map(|&i| ready.column(i)).collect();
            for row in 0..ready.num_rows() {
                self.emitted.insert(read_key(&key_arrays, row));
            }
        }
        ready
    }

    fn empty(&self) -> RecordBatch {
        match &self.schema {
            Some(schema) => RecordBatch::new_empty(schema.clone()),
            None => RecordBatch::new_empty(Arc::new(Schema::empty())),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let mut out = self.current_watermark.to_le_bytes().to_vec();
        let pending = match &self.pending {
            Some(batch) if batch.num_rows() > 0 => write_ipc(batch),
            _ => Vec::new(),
        };
        out.extend_from_slice(&(pending.len() as u32).to_le_bytes());
        out.extend_from_slice(&pending);
        out.extend_from_slice(&self.snapshot_emitted());
        out
    }

    /// The emitted keys as an IPC batch of just the key columns (their types taken from the scalars).
    fn snapshot_emitted(&self) -> Vec<u8> {
        if self.emitted.is_empty() {
            return Vec::new();
        }
        let keys: Vec<&GroupKey> = self.emitted.iter().collect();
        let arity = keys[0].len();
        let fields: Vec<Field> = (0..arity)
            .map(|j| Field::new(format!("key{j}"), keys[0][j].data_type(), true))
            .collect();
        let columns: Vec<ArrayRef> = (0..arity)
            .map(|j| scalars_to_array(keys.iter().map(|k| k[j].clone()).collect(), fields[j].data_type()))
            .collect();
        write_ipc(&RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("dedup emitted"))
    }

    fn restore(partition_columns: Vec<usize>, rt_column: usize, bytes: &[u8]) -> Self {
        let mut dedup = KeepFirstDeduplicator::new(partition_columns, rt_column);
        if bytes.len() < 8 {
            return dedup;
        }
        dedup.current_watermark = i64::from_le_bytes(bytes[0..8].try_into().expect("watermark"));
        let pending_len = u32::from_le_bytes(bytes[8..12].try_into().expect("pending len")) as usize;
        for batch in read_ipc_if_present(&bytes[12..12 + pending_len]) {
            dedup.schema = Some(batch.schema());
            dedup.pending = Some(batch);
        }
        for batch in read_ipc_if_present(&bytes[12 + pending_len..]) {
            let key_arrays: Vec<&ArrayRef> = (0..batch.num_columns()).map(|i| batch.column(i)).collect();
            for row in 0..batch.num_rows() {
                dedup.emitted.insert(read_key(&key_arrays, row));
            }
        }
        dedup
    }
}

/// Keep-last deduplication on a rowtime order — Flink's `RowTimeDeduplicateFunction` (keep-last). Per
/// partition key it keeps the row with the **maximum** rowtime and emits a retract changelog eagerly,
/// per input row (no watermark buffering, unlike keep-first): the first row for a key emits `+I`; a
/// later row whose rowtime is `>=` the stored one replaces it, emitting `-U`(previous, gated on
/// `generate_update_before`) then `+U`(new); a row with a smaller rowtime is ignored (it is older).
/// Insert-only input (every row is a put), changelog output. The stored full row per key lives as
/// scalars and is rebuilt with `scalars_to_array` on emit, like the changelog normalizer below.
struct KeepLastDeduplicator {
    partition_columns: Vec<usize>,
    rt_column: usize,
    generate_update_before: bool,
    schema: Option<SchemaRef>,
    /// Per key: the stored row's rowtime (millis) and its full row.
    rows: HashMap<GroupKey, (i64, JoinRow)>,
}

impl KeepLastDeduplicator {
    fn new(partition_columns: Vec<usize>, rt_column: usize, generate_update_before: bool) -> Self {
        KeepLastDeduplicator {
            partition_columns,
            rt_column,
            generate_update_before,
            schema: None,
            rows: HashMap::new(),
        }
    }

    fn push(&mut self, batch: &RecordBatch) -> RecordBatch {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        let key_arrays: Vec<&ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i)).collect();
        let data_arrays: Vec<&ArrayRef> = (0..arity).map(|i| batch.column(i)).collect();
        let rt = rt_to_millis(batch.column(self.rt_column));

        let mut out_rows: Vec<JoinRow> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();
        for row in 0..batch.num_rows() {
            let key = read_key(&key_arrays, row);
            let rowtime = rt.value(row);
            let current: JoinRow = data_arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a, row).expect("keep-last row scalar"))
                .collect();
            match self.rows.get(&key) {
                None => {
                    out_rows.push(current.clone());
                    out_kinds.push(0); // +I — first row for the key
                }
                Some((stored_rt, _)) if rowtime < *stored_rt => {
                    continue; // an older row by rowtime — keep-last ignores it
                }
                Some((_, prev)) => {
                    if self.generate_update_before {
                        out_rows.push(prev.clone());
                        out_kinds.push(1); // -U the previous row
                    }
                    out_rows.push(current.clone());
                    out_kinds.push(2); // +U the new (later) row
                }
            }
            self.rows.insert(key, (rowtime, current));
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
            .expect("failed to build keep-last dedup batch")
    }

    /// Serializes the stored last-row-per-key set; the rowtime is re-derived from each row on restore.
    fn snapshot(&self) -> Vec<u8> {
        let Some(schema) = &self.schema else { return Vec::new() };
        let rows: Vec<&JoinRow> = self.rows.values().map(|(_, row)| row).collect();
        if rows.is_empty() {
            return Vec::new();
        }
        let fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| {
                scalars_to_array(rows.iter().map(|r| r[j].clone()).collect(), fields[j].data_type())
            })
            .collect();
        write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("keep-last snapshot"),
        )
    }

    fn restore(
        partition_columns: Vec<usize>,
        rt_column: usize,
        generate_update_before: bool,
        bytes: &[u8],
    ) -> Self {
        let mut dedup =
            KeepLastDeduplicator::new(partition_columns, rt_column, generate_update_before);
        for batch in read_ipc_if_present(bytes) {
            dedup.schema = Some(batch.schema());
            let key_arrays: Vec<&ArrayRef> =
                dedup.partition_columns.iter().map(|&i| batch.column(i)).collect();
            let rt = rt_to_millis(batch.column(dedup.rt_column));
            let arity = batch.num_columns();
            for row in 0..batch.num_rows() {
                let key = read_key(&key_arrays, row);
                let stored: JoinRow = (0..arity)
                    .map(|i| ScalarValue::try_from_array(batch.column(i), row).expect("restore scalar"))
                    .collect();
                dedup.rows.insert(key, (rt.value(row), stored));
            }
        }
        dedup
    }
}

/// Changelog normalization (Flink's `ChangelogNormalize` / `ProcTimeDeduplicateKeepLastRowFunction`,
/// keep-last on a changelog): turns an upsert or duplicate-bearing changelog into a regular
/// INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE changelog with no duplication, keyed by the unique key.
/// It keeps the last full row per key (stored as INSERT) and, on each input row:
///   * a "put" (`+I`/`+U`): first row → emit `+I`; an unchanged row → suppress (no emit); a changed
///     row → emit `-U`(previous) if `generate_update_before`, then `+U`(new).
///   * a "remove" (`-D`/`-U`): emit `-D`(the stored full row, since a tombstone may carry only the
///     key) and clear the key; a remove of an absent key emits nothing.
/// Proctime — it emits synchronously per input row, so there is no watermark buffering.
struct ChangelogNormalizer {
    key_columns: Vec<usize>,
    generate_update_before: bool,
    schema: Option<SchemaRef>,
    rows: HashMap<GroupKey, JoinRow>,
}

impl ChangelogNormalizer {
    fn new(key_columns: Vec<usize>, generate_update_before: bool) -> Self {
        ChangelogNormalizer {
            key_columns,
            generate_update_before,
            schema: None,
            rows: HashMap::new(),
        }
    }

    fn push(&mut self, batch: &RecordBatch) -> RecordBatch {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        let key_arrays: Vec<&ArrayRef> =
            self.key_columns.iter().map(|&i| batch.column(i)).collect();
        let data_arrays: Vec<&ArrayRef> = (0..arity).map(|i| batch.column(i)).collect();
        let row_kinds = row_kind_column(batch);

        let mut out_rows: Vec<JoinRow> = Vec::new();
        let mut out_kinds: Vec<i8> = Vec::new();

        for row in 0..batch.num_rows() {
            let kind = row_kinds.map(|k| k.value(row)).unwrap_or(0);
            let key = read_key(&key_arrays, row);
            let current: JoinRow = data_arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a, row).expect("changelog-normalize row scalar"))
                .collect();
            // INSERT(0)/UPDATE_AFTER(2) put; UPDATE_BEFORE(1)/DELETE(3) remove.
            if kind == 0 || kind == 2 {
                match self.rows.get(&key) {
                    None => {
                        out_rows.push(current.clone());
                        out_kinds.push(0); // +I
                    }
                    Some(prev) if *prev == current => {
                        continue; // unchanged — emit nothing (no state TTL)
                    }
                    Some(prev) => {
                        if self.generate_update_before {
                            out_rows.push(prev.clone());
                            out_kinds.push(1); // -U the previous row
                        }
                        out_rows.push(current.clone());
                        out_kinds.push(2); // +U the new row
                    }
                }
                self.rows.insert(key, current);
            } else if let Some(prev) = self.rows.remove(&key) {
                out_rows.push(prev); // emit the stored full row, not the (maybe key-only) tombstone
                out_kinds.push(3); // -D
            }
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
            .expect("failed to build changelog-normalize batch")
    }

    /// Serializes the stored last-row-per-key set (keys are re-derived from each row on restore).
    fn snapshot(&self) -> Vec<u8> {
        let Some(schema) = &self.schema else { return Vec::new() };
        let rows: Vec<&JoinRow> = self.rows.values().collect();
        if rows.is_empty() {
            return Vec::new();
        }
        let fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| {
                scalars_to_array(rows.iter().map(|r| r[j].clone()).collect(), fields[j].data_type())
            })
            .collect();
        write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .expect("changelog-normalize snapshot"),
        )
    }

    fn restore(key_columns: Vec<usize>, generate_update_before: bool, bytes: &[u8]) -> Self {
        let mut normalizer = ChangelogNormalizer::new(key_columns, generate_update_before);
        for batch in read_ipc_if_present(bytes) {
            normalizer.schema = Some(batch.schema());
            let key_arrays: Vec<&ArrayRef> =
                normalizer.key_columns.iter().map(|&i| batch.column(i)).collect();
            let arity = batch.num_columns();
            for row in 0..batch.num_rows() {
                let key = read_key(&key_arrays, row);
                let stored: JoinRow = (0..arity)
                    .map(|i| ScalarValue::try_from_array(batch.column(i), row).expect("restore scalar"))
                    .collect();
                normalizer.rows.insert(key, stored);
            }
        }
        normalizer
    }
}

/// Window Top-N / window deduplication over a windowing-TVF input (Flink's `WindowRank` /
/// `WindowDeduplicate`): within each window (the attached `window_start`/`window_end` columns) and
/// partition key, rank rows by the sort key and keep the top N, emitting them once the watermark
/// closes the window. Append-only — a closed window's rows are emitted exactly once. Window
/// deduplication is the `limit = 1` case (keep-first = sort by rowtime ascending, keep-last =
/// descending). Late rows (whose window already closed) are dropped, matching the host.
struct WindowRanker {
    window_start_col: usize,
    window_end_col: usize,
    partition_columns: Vec<usize>,
    sort_columns: Vec<SortColumn>,
    limit: i64,
    output_rank_number: bool,
    current_watermark: i64,
    /// Bounded, sorted top-N buffer per (window_end, window_start, partition key).
    groups: HashMap<(i64, i64, GroupKey), Vec<JoinRow>>,
    schema: Option<SchemaRef>,
}

impl WindowRanker {
    fn new(
        window_start_col: usize,
        window_end_col: usize,
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        output_rank_number: bool,
    ) -> Self {
        WindowRanker {
            window_start_col,
            window_end_col,
            partition_columns,
            sort_columns,
            limit,
            output_rank_number,
            current_watermark: i64::MIN,
            groups: HashMap::new(),
            schema: None,
        }
    }

    fn push(&mut self, batch: &RecordBatch) {
        let arity = data_arity(batch);
        self.schema = Some(data_schema(batch));
        let ws = rt_to_millis(batch.column(self.window_start_col));
        let we = rt_to_millis(batch.column(self.window_end_col));
        let partition_arrays: Vec<&ArrayRef> =
            self.partition_columns.iter().map(|&i| batch.column(i)).collect();
        let data_arrays: Vec<&ArrayRef> = (0..arity).map(|i| batch.column(i)).collect();
        for row in 0..batch.num_rows() {
            let window_end = we.value(row);
            if window_end <= self.current_watermark {
                continue; // late: the window already closed and emitted
            }
            let window_start = ws.value(row);
            let key = read_key(&partition_arrays, row);
            let full: JoinRow = data_arrays
                .iter()
                .map(|a| ScalarValue::try_from_array(a, row).expect("window-rank row scalar"))
                .collect();
            let buffer = self.groups.entry((window_end, window_start, key)).or_default();
            // Insert after rows ordering equal-or-before, preserving arrival order for ties (the
            // ROW_NUMBER tie-break), then drop anything past rank N.
            let pos = buffer
                .partition_point(|r| compare_rows(r, &full, &self.sort_columns) != std::cmp::Ordering::Greater);
            buffer.insert(pos, full);
            if buffer.len() as i64 > self.limit {
                buffer.truncate(self.limit as usize);
            }
        }
    }

    /// Emits the top-N rows of every window the watermark has closed, in rank order (with the rank
    /// number appended when the host projects it), and evicts those windows.
    fn flush(&mut self, watermark: i64) -> RecordBatch {
        self.current_watermark = watermark;
        let mut ready: Vec<(i64, i64, GroupKey)> =
            self.groups.keys().filter(|(we, _, _)| *we <= watermark).cloned().collect();
        // Evict in (window_end, window_start) order for a deterministic emission sequence.
        ready.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        let mut rows: Vec<JoinRow> = Vec::new();
        let mut ranks: Vec<i64> = Vec::new();
        for group in ready {
            let buffer = self.groups.remove(&group).expect("ready group present");
            for (rank, row) in buffer.into_iter().enumerate() {
                rows.push(row);
                ranks.push(rank as i64 + 1);
            }
        }
        self.emit(rows, ranks)
    }

    fn emit(&self, rows: Vec<JoinRow>, ranks: Vec<i64>) -> RecordBatch {
        let schema = match &self.schema {
            Some(schema) => schema.clone(),
            None => return RecordBatch::new_empty(Arc::new(Schema::empty())),
        };
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let mut columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| scalars_to_array(rows.iter().map(|r| r[j].clone()).collect(), fields[j].data_type()))
            .collect();
        if self.output_rank_number {
            fields.push(Field::new("w0$o0", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(ranks)));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("window-rank output batch")
    }

    fn snapshot(&self) -> Vec<u8> {
        let mut out = self.current_watermark.to_le_bytes().to_vec();
        let Some(schema) = &self.schema else { return out };
        let rows: Vec<&JoinRow> = self.groups.values().flatten().collect();
        if rows.is_empty() {
            return out;
        }
        let fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        let columns: Vec<ArrayRef> = (0..fields.len())
            .map(|j| scalars_to_array(rows.iter().map(|r| r[j].clone()).collect(), fields[j].data_type()))
            .collect();
        out.extend_from_slice(&write_ipc(
            &RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("window-rank snapshot"),
        ));
        out
    }

    fn restore(
        window_start_col: usize,
        window_end_col: usize,
        partition_columns: Vec<usize>,
        sort_columns: Vec<SortColumn>,
        limit: i64,
        output_rank_number: bool,
        bytes: &[u8],
    ) -> Self {
        let mut ranker = WindowRanker::new(
            window_start_col,
            window_end_col,
            partition_columns,
            sort_columns,
            limit,
            output_rank_number,
        );
        if bytes.len() < 8 {
            return ranker;
        }
        ranker.current_watermark = i64::from_le_bytes(bytes[0..8].try_into().expect("watermark"));
        // Re-inserting through push reproduces each group's sorted, truncated buffer; buffered rows
        // have window_end > the watermark, so none are dropped as late.
        for batch in read_ipc_if_present(&bytes[8..]) {
            ranker.push(&batch);
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

/// The `key0..key{n-1}` schema fields for an emitted batch, one per stored key type. Nullable: a
/// group key can be NULL (Flink groups nulls as their own key), and GROUPING SETS/CUBE/ROLLUP makes a
/// grouped-out key NULL routinely — so the emitted column may carry nulls.
fn key_fields(types: &[DataType]) -> Vec<Field> {
    types.iter().enumerate().map(|(j, t)| Field::new(format!("key{j}"), t.clone(), true)).collect()
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

/// Downcasts a named int32 column, with a clear message if it is missing or the wrong type.
fn column_i32<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int32Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap_or_else(|| panic!("column {name} must be int32"))
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
        // Carry the changelog tag through: a Calc transforms each row independently (per-row
        // projection, optional deterministic filter), so a `$row_kind$` column rides through unchanged
        // — filtered alongside the rows by the condition above. This makes the Calc changelog-safe,
        // matching the host's per-row Calc over a retracting stream.
        if let Some(kind) = filtered.column_by_name(ROW_KIND_COLUMN) {
            fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
            columns.push(kind.clone());
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

/// Runs a batch through the stateless windowing table function, exporting the fanned-out batch with
/// window_start/window_end/window_time appended. Stateless, so there is no handle to create or close.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_assignWindows<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    time_col: jint,
    window_millis: jlong,
    slide_millis: jlong,
    cumulative: jboolean,
) {
    let batch = import_record_batch(in_array_address, in_schema_address);
    let result = assign_windows(
        &batch,
        time_col as usize,
        window_millis,
        slide_millis,
        cumulative != 0,
    );
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Stateless GROUPING SETS / CUBE / ROLLUP expansion over an Arrow batch the JVM exported.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_expand<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    num_expand_rows: jint,
    num_out_cols: jint,
    expand_id_index: jint,
    expand_id_is_long: jboolean,
    copy_indices: JIntArray<'local>,
    expand_id_values: JLongArray<'local>,
) {
    let batch = import_record_batch(in_array_address, in_schema_address);
    let copy = read_int_array(&env, &copy_indices);
    let ids = read_longs(&env, &expand_id_values);
    let result = expand(
        &batch,
        num_expand_rows as usize,
        num_out_cols as usize,
        expand_id_index as usize,
        expand_id_is_long != 0,
        &copy,
        &ids,
    );
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Creates a buffering local two-phase GROUP BY pre-aggregate and returns an opaque handle. It
/// accumulates across batches in memory until flushed; the buffer is transient (drained before each
/// checkpoint on the JVM side), so there is no snapshot/restore.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createLocalGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    aggregate_kinds: JIntArray<'local>,
    value_types: JIntArray<'local>,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_types = read_int_array(&env, &value_types);
    let value_columns = read_int_array(&env, &value_columns);
    let key_cols = read_columns(&env, &key_columns);
    Box::into_raw(Box::new(LocalGroupAggregator::new(kinds, value_types, value_columns, key_cols)))
        as jlong
}

/// Folds an Arrow batch the JVM exported into the buffered per-key accumulators; emits nothing.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updateLocalGroupAggregator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut LocalGroupAggregator) };
    aggregator.update(&import_record_batch(in_array_address, in_schema_address));
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
        drop(Box::from_raw(handle as *mut LocalGroupAggregator));
    }
}

/// Stateless INNER UNNEST of an ARRAY column over an Arrow batch the JVM exported.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_unnest<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    array_col: jint,
    with_ordinality: jboolean,
    is_left: jboolean,
    is_multiset: jboolean,
) {
    let batch = import_record_batch(in_array_address, in_schema_address);
    let result = unnest_array(
        &batch,
        array_col as usize,
        with_ordinality != 0,
        is_left != 0,
        is_multiset != 0,
    );
    export_record_batch(result, out_array_address, out_schema_address);
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
    frame_kind: jint,
    frame_offset: jlong,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let keys = read_columns(&env, &key_columns);
    Box::into_raw(Box::new(OverWindowAggregator::new(
        value_type as i64,
        kinds,
        rt_column as usize,
        if value_column < 0 { None } else { Some(value_column as usize) },
        keys,
        frame_kind as i64,
        frame_offset,
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
    frame_kind: jint,
    frame_offset: jlong,
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
        frame_kind as i64,
        frame_offset,
        &bytes,
    ))) as jlong
}

/// Creates an event-time sorter over the given rowtime column and returns an opaque handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createTemporalSorter<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    rt_column: jint,
) -> jlong {
    Box::into_raw(Box::new(TemporalSorter::new(rt_column as usize))) as jlong
}

/// Buffers an input batch (no output); the rows are emitted later, in rowtime order, as watermarks
/// complete them.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushTemporalSorter<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let sorter = unsafe { &mut *(handle as *mut TemporalSorter) };
    sorter.push(import_record_batch(in_array_address, in_schema_address));
}

/// Exports the rows the watermark has completed, sorted ascending by rowtime.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushTemporalSorter<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let sorter = unsafe { &mut *(handle as *mut TemporalSorter) };
    let result = sorter.flush(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases the event-time sorter and its buffered rows.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeTemporalSorter<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut TemporalSorter));
    }
}

/// Serializes the sorter's buffered rows for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let sorter = unsafe { &mut *(handle as *mut TemporalSorter) };
    env.byte_array_from_slice(&sorter.snapshot())
        .expect("failed to allocate sort snapshot array")
        .into_raw()
}

/// Rebuilds an event-time sorter from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    rt_column: jint,
    snapshot: JByteArray<'local>,
) -> jlong {
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read sort snapshot");
    Box::into_raw(Box::new(TemporalSorter::restore(rt_column as usize, &bytes))) as jlong
}

/// Creates a keep-first deduplicator over the given partition-key columns and rowtime column.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createKeepFirstDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    rt_column: jint,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    Box::into_raw(Box::new(KeepFirstDeduplicator::new(partitions, rt_column as usize))) as jlong
}

/// Buffers an input batch (no output); each key's minimum-rowtime row is emitted later, on the
/// watermark that reaches its rowtime.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushKeepFirstDeduplicator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
    dedup.push(&import_record_batch(in_array_address, in_schema_address));
}

/// Exports each key's first (minimum-rowtime) row whose rowtime the watermark has reached.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushKeepFirstDeduplicator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
    let result = dedup.flush(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases the deduplicator and its per-key state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeKeepFirstDeduplicator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut KeepFirstDeduplicator));
    }
}

/// Serializes the deduplicator's pending candidates, emitted keys, and watermark for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotKeepFirstDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
    env.byte_array_from_slice(&dedup.snapshot())
        .expect("failed to allocate dedup snapshot array")
        .into_raw()
}

/// Rebuilds a keep-first deduplicator from a snapshot taken by a prior run and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreKeepFirstDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    rt_column: jint,
    snapshot: JByteArray<'local>,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read dedup snapshot");
    Box::into_raw(Box::new(KeepFirstDeduplicator::restore(partitions, rt_column as usize, &bytes)))
        as jlong
}

/// Creates a keep-last (rowtime) deduplicator and returns an opaque handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createKeepLastDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    rt_column: jint,
    generate_update_before: jboolean,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    Box::into_raw(Box::new(KeepLastDeduplicator::new(
        partitions,
        rt_column as usize,
        generate_update_before != 0,
    ))) as jlong
}

/// Folds an input batch and returns the retract changelog it produces (emitted eagerly per row).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushKeepLastDeduplicator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let dedup = unsafe { &mut *(handle as *mut KeepLastDeduplicator) };
    let result = dedup.push(&import_record_batch(in_array_address, in_schema_address));
    export_record_batch(result, out_array_address, out_schema_address);
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeKeepLastDeduplicator<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut KeepLastDeduplicator));
    }
}

/// Serializes the keep-last deduplicator's per-key stored rows for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotKeepLastDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let dedup = unsafe { &mut *(handle as *mut KeepLastDeduplicator) };
    env.byte_array_from_slice(&dedup.snapshot())
        .expect("failed to allocate dedup snapshot array")
        .into_raw()
}

/// Rebuilds a keep-last deduplicator from a snapshot and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreKeepLastDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    rt_column: jint,
    generate_update_before: jboolean,
    snapshot: JByteArray<'local>,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read dedup snapshot");
    Box::into_raw(Box::new(KeepLastDeduplicator::restore(
        partitions,
        rt_column as usize,
        generate_update_before != 0,
        &bytes,
    ))) as jlong
}

/// Creates a window-rank ranker (window Top-N / window deduplication) over the attached
/// window_start/window_end columns and returns an opaque handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createWindowRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    window_start_col: jint,
    window_end_col: jint,
    partition_columns: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    limit: jlong,
    output_rank_number: jboolean,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
    Box::into_raw(Box::new(WindowRanker::new(
        window_start_col as usize,
        window_end_col as usize,
        partitions,
        sort,
        limit,
        output_rank_number != 0,
    ))) as jlong
}

/// Buffers an input batch (no output); each window's top-N rows are emitted when the watermark
/// closes the window.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushWindowRanker<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    let ranker = unsafe { &mut *(handle as *mut WindowRanker) };
    ranker.push(&import_record_batch(in_array_address, in_schema_address));
}

/// Exports the top-N rows of every window the watermark has closed (with the rank number appended
/// when the host projects it).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushWindowRanker<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let ranker = unsafe { &mut *(handle as *mut WindowRanker) };
    let result = ranker.flush(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Releases the window-rank ranker and its per-window state.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeWindowRanker<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut WindowRanker));
    }
}

/// Serializes the ranker's per-window buffers and watermark for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotWindowRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let ranker = unsafe { &mut *(handle as *mut WindowRanker) };
    env.byte_array_from_slice(&ranker.snapshot())
        .expect("failed to allocate window-rank snapshot array")
        .into_raw()
}

/// Rebuilds a window-rank ranker from a snapshot and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreWindowRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    window_start_col: jint,
    window_end_col: jint,
    partition_columns: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    limit: jlong,
    output_rank_number: jboolean,
    snapshot: JByteArray<'local>,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read window-rank snapshot");
    Box::into_raw(Box::new(WindowRanker::restore(
        window_start_col as usize,
        window_end_col as usize,
        partitions,
        sort,
        limit,
        output_rank_number != 0,
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

/// Decodes a binary "body" batch (one bare protobuf message per row) into typed Arrow, matching Flink's
/// `protobuf` format: each message is the *whole* serialized protobuf (no Confluent framing), parsed
/// against a descriptor the JVM serialized off the generated message class into a `FileDescriptorSet`.
/// `prost-reflect` builds the descriptor pool at open time; `ptars` walks the wire format straight into
/// Arrow arrays (no per-row `DynamicMessage`), deriving the batch schema from the message descriptor.
struct ProtobufDecoder {
    message: prost_reflect::MessageDescriptor,
    config: ptars::PtarsConfig,
}

impl ProtobufDecoder {
    /// `descriptor_set` is an encoded protobuf `FileDescriptorSet` (the message's file + its transitive
    /// dependencies); `message_name` is the fully-qualified message type to decode each body as.
    fn new(descriptor_set: &[u8], message_name: &str) -> ProtobufDecoder {
        let pool = prost_reflect::DescriptorPool::decode(descriptor_set)
            .expect("failed to decode protobuf FileDescriptorSet");
        let message = pool
            .get_message_by_name(message_name)
            .unwrap_or_else(|| panic!("protobuf message {message_name} not found in descriptor"));
        // ConfluentWirePolicy::Raw (the default) = bare protobuf bytes, which is what Flink's `protobuf`
        // format carries; the Confluent variant (strip magic+id+message-index) would set it here.
        ProtobufDecoder { message, config: ptars::PtarsConfig::default() }
    }

    /// Decodes the single binary body column into a typed batch (schema derived from the descriptor).
    /// A null body decodes to a null row (ptars keeps the batch 1:1 with the input column).
    fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        use arrow::array::BinaryArray;
        let column = bodies.column(0).as_any().downcast_ref::<BinaryArray>().expect("binary body");
        ptars::binary_array_to_record_batch_direct(column, &self.message, &self.config)
            .expect("failed to decode protobuf batch")
    }
}

/// Decodes a binary "body" batch (one CSV record per row, no header) into a batch of the target schema
/// via `arrow-csv`, matching Flink's `csv` format. Records are fed newline-terminated so the streaming
/// decoder sees each message as a complete row; a null body contributes no row (like JSON).
struct CsvDecoder {
    schema: SchemaRef,
}

impl CsvDecoder {
    fn new(schema: SchemaRef) -> CsvDecoder {
        CsvDecoder { schema }
    }

    fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        let column = bodies.column(0);
        let row_count = bodies.num_rows();
        let body = |row: usize| -> Option<&[u8]> { binary_body(column, row) };
        // Join the rows into one newline-delimited buffer (each Kafka message is one record with no
        // trailing newline), then stream it through the CSV decoder in one flush.
        let mut buf = Vec::new();
        for row in 0..row_count {
            if let Some(bytes) = body(row) {
                buf.extend_from_slice(bytes);
                if !bytes.ends_with(b"\n") {
                    buf.push(b'\n');
                }
            }
        }
        let mut decoder = arrow::csv::ReaderBuilder::new(self.schema.clone())
            .with_header(false)
            .with_batch_size(row_count.max(1))
            .build_decoder();
        let mut offset = 0;
        while offset < buf.len() {
            let consumed = decoder.decode(&buf[offset..]).expect("failed to decode CSV record");
            if consumed == 0 {
                break;
            }
            offset += consumed;
        }
        decoder
            .flush()
            .expect("failed to flush CSV batch")
            .unwrap_or_else(|| RecordBatch::new_empty(self.schema.clone()))
    }
}

/// Decodes Flink's `raw` format: the message bytes pass through as a single column. The body is already
/// a binary column, so this just casts it to the target column's type (Binary passthrough, or Binary →
/// Utf8 for a STRING column) and renames it. 1:1 with the input rows (a null stays null).
struct RawDecoder {
    schema: SchemaRef,
}

impl RawDecoder {
    fn new(schema: SchemaRef) -> RawDecoder {
        RawDecoder { schema }
    }

    fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        let target = self.schema.field(0).data_type();
        let column = arrow::compute::cast(bodies.column(0), target).expect("failed to cast raw column");
        RecordBatch::try_new(self.schema.clone(), vec![column]).expect("failed to build raw batch")
    }
}

/// Reads row `row` of a binary "body" column as bytes, or `None` if the column is null there. Shared by
/// the JSON/CSV decoders, which accept a Binary, LargeBinary, or Utf8 body column.
fn binary_body(column: &ArrayRef, row: usize) -> Option<&[u8]> {
    use arrow::array::{Array, BinaryArray, LargeBinaryArray, StringArray};
    match column.data_type() {
        DataType::Binary => {
            let a = column.as_any().downcast_ref::<BinaryArray>().unwrap();
            a.is_valid(row).then(|| a.value(row))
        }
        DataType::LargeBinary => {
            let a = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            a.is_valid(row).then(|| a.value(row))
        }
        DataType::Utf8 => {
            let a = column.as_any().downcast_ref::<StringArray>().unwrap();
            a.is_valid(row).then(|| a.value(row).as_bytes())
        }
        other => panic!("unsupported body column type {other:?}"),
    }
}

/// Which CDC changelog JSON dialect an envelope is in. The wire codec is plain JSON either way; the
/// dialect fixes the two image fields, the operation field, the op-code → action mapping, and how the
/// pre-image is recovered (see `CdcShape`). Debezium/OGG and Maxwell are scalar (one image per message);
/// Canal (`data`/`old` arrays — a message fans out per element) is a follow-up.
#[derive(Clone, Copy)]
enum CdcDialect {
    /// Debezium JSON: `{before, after, op}`, op ∈ {`c`/`r` → insert, `u` → update, `d` → delete}.
    /// Mirrors `DebeziumJsonDeserializationSchema` (`r` is a snapshot read, treated as an insert).
    Debezium,
    /// Oracle GoldenGate JSON: `{before, after, op_type}`, op ∈ {`I` → insert, `U` → update,
    /// `D` → delete, `T` truncate → skipped}. Mirrors `OggJsonDeserializationSchema`.
    Ogg,
    /// Maxwell JSON: `{data, old, type}`, type ∈ {`insert`, `update`, `delete`}. `data` is the full
    /// post-image, `old` a *partial* pre-image (only changed fields); delete carries the row in `data`.
    /// Mirrors `MaxwellJsonDeserializationSchema`.
    Maxwell,
    /// Canal JSON: `{data, old, type}` where `data`/`old` are *arrays* of rows (one message fans out
    /// per element), type ∈ {`INSERT`, `UPDATE`, `DELETE`, `CREATE` (DDL → skipped)}. Same partial-`old`
    /// merge as Maxwell, applied per element pair. Mirrors `CanalJsonDeserializationSchema`.
    Canal,
}

/// A CDC envelope's change action, before fanning out to physical rows. An update emits two rows
/// (UPDATE_BEFORE + UPDATE_AFTER); insert/delete emit one.
enum CdcAction {
    Insert,
    Update,
    Delete,
}

/// What to do with one envelope row's operation. `Skip` is a deliberate no-op Flink also drops (Canal's
/// `CREATE` DDL); `Unknown` is an unrecognized op, which Flink *fails the job* on by default — we match
/// that (rather than silently dropping the row) so the result is identical, and only the planner's
/// fallback gate lets `ignore-parse-errors` tables (which skip) run on Flink instead.
enum CdcOp {
    Change(CdcAction),
    Skip,
    Unknown,
}

/// How a dialect lays out its pre/post images, which determines how UPDATE_BEFORE and DELETE rows are
/// built.
#[derive(Clone, Copy, PartialEq)]
enum CdcShape {
    /// Debezium/OGG: `before` is the full pre-image and `after` the full post-image. DELETE reads
    /// `before`; an update's UPDATE_BEFORE is `before` verbatim — a null `before` skips the record
    /// (Flink throws; we match `ignore-parse-errors`).
    BeforeAfter,
    /// Maxwell/Canal: `data` is the full post-image and `old` a *partial* pre-image (only changed
    /// fields). DELETE reads `data` (it holds the deleted row); an update's UPDATE_BEFORE is
    /// `coalesce(old, data)` per field — a field absent from `old` is unchanged, so it falls back to
    /// `data`. (Divergence: a field deliberately changed *to* null can't be told apart from an absent
    /// one once decoded, so it falls back to `data`; Flink keeps the null. Rare; documented.)
    DataOld,
}

/// The fixed per-dialect envelope layout the decoder reads.
struct CdcSpec {
    /// JSON field holding the pre-image (`before` / `old`) — envelope column 0.
    before_field: &'static str,
    /// JSON field holding the post-image (`after` / `data`) — envelope column 1.
    after_field: &'static str,
    /// JSON field holding the operation — envelope column 2.
    op_field: &'static str,
    shape: CdcShape,
    /// Whether the images are JSON *arrays* of rows (Canal) rather than single rows: one message then
    /// fans out per element, pairing `data[i]` with `old[i]`.
    arrays: bool,
}

impl CdcDialect {
    fn spec(self) -> CdcSpec {
        match self {
            CdcDialect::Debezium => CdcSpec {
                before_field: "before",
                after_field: "after",
                op_field: "op",
                shape: CdcShape::BeforeAfter,
                arrays: false,
            },
            CdcDialect::Ogg => CdcSpec {
                before_field: "before",
                after_field: "after",
                op_field: "op_type",
                shape: CdcShape::BeforeAfter,
                arrays: false,
            },
            CdcDialect::Maxwell => CdcSpec {
                before_field: "old",
                after_field: "data",
                op_field: "type",
                shape: CdcShape::DataOld,
                arrays: false,
            },
            CdcDialect::Canal => CdcSpec {
                before_field: "old",
                after_field: "data",
                op_field: "type",
                shape: CdcShape::DataOld,
                arrays: true,
            },
        }
    }

    /// Classifies an op string. An unrecognized op is `Unknown` (Flink throws on it by default — see
    /// `CdcOp`); Canal's `CREATE` is a `Skip` (Flink drops DDL). Mirrors each `*JsonDeserializationSchema`.
    fn classify(self, op: &str) -> CdcOp {
        match self {
            CdcDialect::Debezium => match op {
                "c" | "r" => CdcOp::Change(CdcAction::Insert),
                "u" => CdcOp::Change(CdcAction::Update),
                "d" => CdcOp::Change(CdcAction::Delete),
                _ => CdcOp::Unknown,
            },
            CdcDialect::Ogg => match op {
                "I" => CdcOp::Change(CdcAction::Insert),
                "U" => CdcOp::Change(CdcAction::Update),
                "D" => CdcOp::Change(CdcAction::Delete),
                _ => CdcOp::Unknown, // including "T" truncate, which Flink treats as an unknown op
            },
            CdcDialect::Maxwell => match op {
                "insert" => CdcOp::Change(CdcAction::Insert),
                "update" => CdcOp::Change(CdcAction::Update),
                "delete" => CdcOp::Change(CdcAction::Delete),
                _ => CdcOp::Unknown,
            },
            CdcDialect::Canal => match op {
                "INSERT" => CdcOp::Change(CdcAction::Insert),
                "UPDATE" => CdcOp::Change(CdcAction::Update),
                "DELETE" => CdcOp::Change(CdcAction::Delete),
                "CREATE" => CdcOp::Skip, // a DDL change event Flink drops
                _ => CdcOp::Unknown,
            },
        }
    }
}

/// Appends the output row(s) for one change-event unit (one envelope row, or one array element for
/// Canal): `before_idx`/`after_idx` are the rows to read in the pre/post-image struct arrays (equal for
/// scalar dialects; distinct flattened indices for Canal). An update fans out to UPDATE_BEFORE +
/// UPDATE_AFTER; a `BeforeAfter` dialect with a null pre-image skips the update/delete (Flink throws).
fn cdc_emit(
    action: &CdcAction,
    before_idx: usize,
    after_idx: usize,
    shape: CdcShape,
    before: &StructArray,
    out: &mut Vec<(i8, usize, usize, RowSource)>,
) {
    // Debezium/OGG fail the job on a null pre-image for an update/delete (Flink's REPLICA_IDENTITY
    // error); we match that rather than silently dropping the row, so the result is identical.
    match action {
        CdcAction::Insert => out.push((0, before_idx, after_idx, RowSource::After)),
        CdcAction::Update => {
            let before_source = match shape {
                CdcShape::BeforeAfter if !before.is_valid(before_idx) => {
                    panic!("CDC UPDATE has a null \"before\"/pre-image (REPLICA IDENTITY not FULL?)")
                }
                CdcShape::BeforeAfter => RowSource::Before,
                CdcShape::DataOld => RowSource::Coalesce,
            };
            out.push((1, before_idx, after_idx, before_source));
            out.push((2, before_idx, after_idx, RowSource::After));
        }
        CdcAction::Delete => match shape {
            CdcShape::BeforeAfter if !before.is_valid(before_idx) => {
                panic!("CDC DELETE has a null \"before\"/pre-image (REPLICA IDENTITY not FULL?)")
            }
            CdcShape::BeforeAfter => out.push((3, before_idx, after_idx, RowSource::Before)),
            // Maxwell/Canal: the deleted row lives in the post-image (`data`).
            CdcShape::DataOld => out.push((3, before_idx, after_idx, RowSource::After)),
        },
    }
}

/// Which image an output row reads its columns from. `Coalesce` (Maxwell/Canal UPDATE_BEFORE) reads the
/// pre-image where that field is present and falls back to the post-image otherwise — a per-field choice,
/// so it can't share one gather index across columns.
#[derive(Clone, Copy)]
enum RowSource {
    /// The pre-image (`before` / `old`), envelope column 0.
    Before,
    /// The post-image (`after` / `data`), envelope column 1.
    After,
    /// Per field: pre-image where present, else post-image.
    Coalesce,
}

/// Decodes a scalar CDC changelog JSON format (Debezium/OGG/Maxwell) straight to a columnar changelog
/// batch: the physical columns plus a trailing `$row_kind$` byte, with one input message fanning out to
/// 0–2 output rows (an update becomes UPDATE_BEFORE + UPDATE_AFTER; a tombstone/empty message, zero).
/// An unknown op or a null pre-image on an update/delete *fails* (Flink's default throw), never a silent
/// drop — the planner only routes here when `ignore-parse-errors` is off, so failing matches Flink.
/// This mirrors Flink's `*JsonDeserializationSchema` — decode the envelope to a row, then emit the
/// physical row(s) by op with a `RowKind` — but vectorized: every body's envelope is decoded in one
/// `arrow-json` pass, then each physical column is gathered with a single `interleave` choosing the
/// right pre/post-image struct child per output row. RisingWave's row-at-a-time `DebeziumChangeEvent`
/// (`access_field(before/after)` + an `Ops` array) is the reference; this is its batch form, where
/// `$row_kind$` is our columnar `RowKind` (divergences/13). It feeds the existing native changelog
/// operators, so a CDC → GROUP BY/join/Top-N pipeline materializes zero rows end to end.
struct CdcJsonDecoder {
    /// The envelope `arrow-json` decodes into: the pre/post images as nested structs of the physical
    /// columns (made nullable, since the absent side / unchanged fields are null), plus the op field as
    /// Utf8. Envelope fields not in this schema (`source`, `ts_ms`, `database`, …) are ignored by arrow-json.
    envelope: SchemaRef,
    /// Output schema: the physical columns (nullable) + trailing `$row_kind$` Int8.
    output: SchemaRef,
    /// Number of physical columns (envelope/output arity excludes op and `$row_kind$`).
    arity: usize,
    dialect: CdcDialect,
}

impl CdcJsonDecoder {
    fn new(physical: SchemaRef, dialect: CdcDialect) -> CdcJsonDecoder {
        let spec = dialect.spec();
        // The images are null on the absent side / for unchanged fields, so the nested physical fields
        // must be nullable regardless of the table's declared nullability.
        let nullable: Fields = physical
            .fields()
            .iter()
            .map(|f| Arc::new(f.as_ref().clone().with_nullable(true)))
            .collect();
        let image = DataType::Struct(nullable.clone());
        // Canal wraps each image in a JSON array of rows.
        let image = if spec.arrays {
            DataType::List(Arc::new(Field::new("item", image, true)))
        } else {
            image
        };
        // Column 0 = pre-image, 1 = post-image, 2 = op (arrow-json matches the JSON keys by name).
        let envelope = Arc::new(Schema::new(vec![
            Field::new(spec.before_field, image.clone(), true),
            Field::new(spec.after_field, image, true),
            Field::new(spec.op_field, DataType::Utf8, true),
        ]));
        let mut output_fields: Vec<FieldRef> = nullable.iter().cloned().collect();
        output_fields.push(Arc::new(Field::new(ROW_KIND_COLUMN, DataType::Int8, false)));
        let output = Arc::new(Schema::new(output_fields));
        CdcJsonDecoder { envelope, output, arity: nullable.len(), dialect }
    }

    fn decode(&self, bodies: &RecordBatch) -> RecordBatch {
        use arrow::array::ListArray;
        let column = bodies.column(0);
        let mut decoder = arrow::json::ReaderBuilder::new(self.envelope.clone())
            .with_batch_size(bodies.num_rows().max(1))
            .build_decoder()
            .expect("failed to build CDC JSON decoder");
        for row in 0..bodies.num_rows() {
            if let Some(bytes) = binary_body(column, row) {
                let consumed = decoder.decode(bytes).expect("failed to decode CDC envelope");
                assert_eq!(consumed, bytes.len(), "CDC body was not a single complete document");
            }
        }
        let envelope = match decoder.flush().expect("failed to flush CDC envelope batch") {
            Some(batch) => batch,
            None => return RecordBatch::new_empty(self.output.clone()),
        };

        let spec = self.dialect.spec();
        let ops = envelope.column(2).as_any().downcast_ref::<StringArray>().expect("op string");

        // The pre/post images as struct arrays the gather reads from. For Canal they are the *flattened*
        // values of the `old`/`data` list columns, and a list's element pairs `old[i]` with `data[i]`;
        // for scalar dialects each envelope row is itself the single unit (pre/post index = the row).
        let (before, after) = if spec.arrays {
            let before_list = envelope.column(0).as_any().downcast_ref::<ListArray>().expect("old list");
            let after_list = envelope.column(1).as_any().downcast_ref::<ListArray>().expect("data list");
            (before_list.values().clone(), after_list.values().clone())
        } else {
            (envelope.column(0).clone(), envelope.column(1).clone())
        };
        let before = before.as_any().downcast_ref::<StructArray>().expect("pre-image struct");
        let after = after.as_any().downcast_ref::<StructArray>().expect("post-image struct");

        // Per output row: its RowKind byte (0 +I, 1 -U, 2 +U, 3 -D — `RowKind.toByteValue()`), and the
        // rows to read in the pre/post-image struct arrays, and which image to read each column from.
        let mut out_rows: Vec<(i8, usize, usize, RowSource)> = Vec::with_capacity(envelope.num_rows());
        for row in 0..envelope.num_rows() {
            // A missing op field is malformed; Flink fails on it (NPE caught → rethrown). Match that.
            let op = if ops.is_valid(row) {
                ops.value(row)
            } else {
                panic!("CDC message has no operation field");
            };
            let action = match self.dialect.classify(op) {
                CdcOp::Change(action) => action,
                CdcOp::Skip => continue,
                // Flink throws on an unrecognized op by default; we fail too (never drop it silently).
                CdcOp::Unknown => panic!("unknown CDC operation \"{op}\""),
            };
            if spec.arrays {
                let after_list = envelope.column(1).as_any().downcast_ref::<ListArray>().unwrap();
                let before_list = envelope.column(0).as_any().downcast_ref::<ListArray>().unwrap();
                let (after_off, after_len) =
                    (after_list.value_offsets()[row] as usize, after_list.value_length(row) as usize);
                let (before_off, before_len) =
                    (before_list.value_offsets()[row] as usize, before_list.value_length(row) as usize);
                for i in 0..after_len {
                    // Canal pairs data[i] with old[i]; if `old` is shorter (or absent) for this element,
                    // there is no paired pre-image, so fall back to the post-image (coalesce → no change).
                    let before_idx = if i < before_len { before_off + i } else { after_off + i };
                    cdc_emit(&action, before_idx, after_off + i, spec.shape, before, &mut out_rows);
                }
            } else {
                cdc_emit(&action, row, row, spec.shape, before, &mut out_rows);
            }
        }

        // Gather each physical column, choosing the pre/post-image child per output row. The source is
        // the same across columns except for `Coalesce`, which picks per field by the pre-image's
        // validity there — so the gather index is built per field.
        const BEFORE: usize = 0;
        const AFTER: usize = 1;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.arity + 1);
        for field in 0..self.arity {
            let before_child = before.column(field);
            let after_child = after.column(field);
            let indices: Vec<(usize, usize)> = out_rows
                .iter()
                .map(|&(_, before_idx, after_idx, source)| match source {
                    RowSource::Before => (BEFORE, before_idx),
                    RowSource::After => (AFTER, after_idx),
                    RowSource::Coalesce => {
                        if before_child.is_valid(before_idx) {
                            (BEFORE, before_idx)
                        } else {
                            (AFTER, after_idx)
                        }
                    }
                })
                .collect();
            let sources = [before_child.as_ref(), after_child.as_ref()];
            columns.push(
                arrow::compute::interleave(&sources, &indices).expect("failed to gather CDC column"),
            );
        }
        columns.push(Arc::new(Int8Array::from(
            out_rows.iter().map(|&(kind, _, _, _)| kind).collect::<Vec<i8>>(),
        )));
        RecordBatch::try_new(self.output.clone(), columns).expect("failed to build CDC batch")
    }
}

/// The single, format-dispatched decode core shared by every ingest path: it turns a batch of one
/// binary column — raw message bodies, one per row — into a typed Arrow batch. JSON goes through
/// `arrow-json`, CSV through `arrow-csv`, Avro (bare or Confluent-framed) through `arrow-avro` against a
/// local schema-id store, protobuf through `prost-reflect`/`ptars`, the CDC changelog formats through
/// `CdcJsonDecoder`, and `raw` is a passthrough. Both the shallow path (Flink polls bytes, hands them
/// here) and the native source (rdkafka polls bytes, hands them here) feed the *same* `MessageDecoder`;
/// only who produces the body batch differs.
enum MessageDecoder {
    Json(JsonDecoder),
    Csv(CsvDecoder),
    Raw(RawDecoder),
    /// Confluent-framed Avro: each message is `0x00` + 4-byte BE schema id + datum, resolved by id.
    Avro(arrow_avro::schema::SchemaStore),
    /// Bare Avro (Flink's `avro`): each message is just the datum, decoded against the one reader schema
    /// registered at synthetic id 0 — we prepend the 5-byte id-0 header so the framed decoder applies.
    BareAvro(arrow_avro::schema::SchemaStore),
    Protobuf(ProtobufDecoder),
    /// CDC changelog JSON (Debezium/OGG): envelope → physical rows + `$row_kind$`, fanning out updates.
    Cdc(CdcJsonDecoder),
}

impl MessageDecoder {
    /// `format`: 0 = JSON, 2 = CSV, 3 = `raw`, 6 = debezium-json, 7 = ogg-json, 8 = maxwell-json,
    /// 9 = canal-json — all decoded against `output_schema` (CDC treats it as the physical columns);
    /// 1 = Confluent-Avro
    /// (`avro_schema` registered at `schema_id`); 4 = bare Avro (`avro_schema` as the reader schema,
    /// registered at synthetic id 0). (Protobuf is built via `createProtobufDecoder`, not here.)
    fn new(format: i32, output_schema: SchemaRef, avro_schema: &str, schema_id: i32) -> MessageDecoder {
        match format {
            1 => MessageDecoder::Avro(avro_store(avro_schema, schema_id as u32)),
            4 => MessageDecoder::BareAvro(avro_store(avro_schema, 0)),
            2 => MessageDecoder::Csv(CsvDecoder::new(output_schema)),
            3 => MessageDecoder::Raw(RawDecoder::new(output_schema)),
            6 => MessageDecoder::Cdc(CdcJsonDecoder::new(output_schema, CdcDialect::Debezium)),
            7 => MessageDecoder::Cdc(CdcJsonDecoder::new(output_schema, CdcDialect::Ogg)),
            8 => MessageDecoder::Cdc(CdcJsonDecoder::new(output_schema, CdcDialect::Maxwell)),
            9 => MessageDecoder::Cdc(CdcJsonDecoder::new(output_schema, CdcDialect::Canal)),
            _ => MessageDecoder::Json(JsonDecoder::new(output_schema)),
        }
    }

    fn decode(&self, body: &RecordBatch) -> RecordBatch {
        match self {
            MessageDecoder::Json(decoder) => decoder.decode(body),
            MessageDecoder::Csv(decoder) => decoder.decode(body),
            MessageDecoder::Raw(decoder) => decoder.decode(body),
            MessageDecoder::Avro(store) => decode_avro_body(store, body, false),
            MessageDecoder::BareAvro(store) => decode_avro_body(store, body, true),
            MessageDecoder::Protobuf(decoder) => decoder.decode(body),
            MessageDecoder::Cdc(decoder) => decoder.decode(body),
        }
    }
}

/// A single-schema arrow-avro writer store keyed by integer id (the Confluent / id-framing layout).
fn avro_store(avro_schema: &str, id: u32) -> arrow_avro::schema::SchemaStore {
    use arrow_avro::schema::{AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore};
    let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
    store
        .set(Fingerprint::Id(id), AvroSchema::new(avro_schema.to_string()))
        .expect("failed to register avro schema");
    store
}

/// Creates a format-dispatched message decoder and returns an opaque handle, released with
/// `closeDecoder`. Formats 0/2/3 (JSON/CSV/raw) decode against the target schema the JVM exports as an
/// empty batch; formats 1/4 (Confluent/bare Avro) derive their schema from `avroSchema` (registered
/// under `schemaId` for Confluent, synthetic id 0 for bare) and ignore the schema C structs.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createDecoder<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    format: jint,
    schema_array_address: jlong,
    schema_address: jlong,
    avro_schema: JString<'local>,
    schema_id: jint,
) -> jlong {
    // Avro (1, 4) derives its own schema from the writer schema, so those callers pass 0/0 for the
    // schema C structs; JSON/CSV/raw (0, 2, 3) decode against the exported target schema.
    let schema = if format == 1 || format == 4 {
        Arc::new(Schema::empty())
    } else {
        import_record_batch(schema_array_address, schema_address).schema()
    };
    let avro_schema: String = env.get_string(&avro_schema).map(Into::into).unwrap_or_default();
    Box::into_raw(Box::new(MessageDecoder::new(format, schema, &avro_schema, schema_id))) as jlong
}

/// Creates a protobuf message decoder (Flink's `protobuf` format: bare message bytes, no framing) and
/// returns an opaque `MessageDecoder` handle, released with `closeDecoder` like any other decoder.
/// `descriptor` is an encoded `FileDescriptorSet` the JVM serialized off the generated message class
/// (the message's `.proto` file + transitive dependencies); `messageName` is the fully-qualified type
/// to decode each body as. The Arrow batch schema is derived from the descriptor by ptars (no schema
/// C-structs needed, unlike JSON).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createProtobufDecoder<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    descriptor: JByteArray<'local>,
    message_name: JString<'local>,
) -> jlong {
    let descriptor = env.convert_byte_array(&descriptor).expect("failed to read proto descriptor");
    let message_name: String = env.get_string(&message_name).expect("failed to read message name").into();
    let decoder = MessageDecoder::Protobuf(ProtobufDecoder::new(&descriptor, &message_name));
    Box::into_raw(Box::new(decoder)) as jlong
}

/// Decodes one body batch into a typed batch, exporting it into the consumer-allocated C structs.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_decodeInto<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let decoder = unsafe { &*(handle as *mut MessageDecoder) };
    let bodies = import_record_batch(in_array_address, in_schema_address);
    export_record_batch(decoder.decode(&bodies), out_array_address, out_schema_address);
}

/// Benchmark-only: decode a body batch and return the decoded row count without exporting the result —
/// so the shallow path can terminate with Arrow in Rust (counted in Rust), symmetric with the native
/// consumer, for an apples-to-apples comparison.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_decodeCount<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) -> jlong {
    let decoder = unsafe { &*(handle as *mut MessageDecoder) };
    let bodies = import_record_batch(in_array_address, in_schema_address);
    decoder.decode(&bodies).num_rows() as jlong
}

/// Releases a message decoder handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeDecoder<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut MessageDecoder));
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
/// One partition's raw message bytes for a poll cycle: a single binary "body" column, sent from the
/// fetcher thread to the decode thread.
#[cfg(feature = "kafka")]
struct RawWork {
    topic: String,
    partition: i32,
    next_offset: i64,
    body: RecordBatch,
}

/// A decoded typed-Arrow batch coming back from the decode thread.
#[cfg(feature = "kafka")]
struct Decoded {
    topic: String,
    partition: i32,
    next_offset: i64,
    batch: RecordBatch,
}

/// Decodes a binary "body" batch into typed Arrow via arrow-avro against the local schema-id store. When
/// `bare`, each message is a raw datum (Flink's `avro`) and we prepend the 5-byte id-0 Confluent header
/// (`0x00` + 4-byte 0) so arrow-avro's framed decoder resolves it against the schema at id 0; otherwise
/// each message already carries its `0x00` + id prefix (Confluent `avro-confluent`). A null body is
/// skipped. Used by `MessageDecoder` for both Avro variants.
fn decode_avro_body(
    store: &arrow_avro::schema::SchemaStore,
    body: &RecordBatch,
    bare: bool,
) -> RecordBatch {
    use arrow::array::{Array, BinaryArray};
    let column = body.column(0).as_any().downcast_ref::<BinaryArray>().expect("binary body");
    let mut decoder = arrow_avro::reader::ReaderBuilder::new()
        .with_writer_schema_store(store.clone())
        .with_batch_size(column.len().max(1))
        .build_decoder()
        .expect("failed to build avro decoder");
    let mut framed = Vec::new();
    for i in 0..column.len() {
        if !column.is_valid(i) {
            continue;
        }
        let bytes = if bare {
            framed.clear();
            framed.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00]); // id-0 Confluent header
            framed.extend_from_slice(column.value(i));
            &framed[..]
        } else {
            column.value(i)
        };
        decoder.decode(bytes).expect("avro decode failed");
    }
    decoder.flush().expect("avro flush failed").expect("empty avro batch")
}

/// The production native Kafka consumer for one Flink subtask: a single rdkafka `BaseConsumer` that
/// multiplexes all of the subtask's assigned partitions (Flink-parity — one consumer, not one per
/// split). The fetcher thread (driving `poll`) only consumes bytes into per-partition binary batches and
/// hands them to a **background decode thread**, so polling and decoding pipeline across cores instead
/// of serializing on one thread; the JVM drains the decoded batches the thread produces. Manual
/// `assign()`+seek, never `subscribe()`/rebalance.
#[cfg(feature = "kafka")]
struct KafkaSplitReader {
    consumer: rdkafka::consumer::BaseConsumer,
    /// The consumer's message queue, batch-drained with `rd_kafka_consume_batch_queue` so we pull many
    /// messages per FFI call instead of one (rust-rdkafka's `poll()` allocates an op + `Arc` per
    /// message — the per-message poll path the flame graph showed dominating the fetcher thread).
    consumer_queue: *mut rdkafka::bindings::rd_kafka_queue_t,
    body_schema: SchemaRef,
    /// Next offset to consume per assigned partition — the split's checkpoint position.
    next_offsets: HashMap<(String, i32), i64>,
    /// Decoded batches ready for the JVM to drain one split at a time, in arrival (offset) order so a
    /// split's offset never goes backwards when several of its batches are drained in one cycle.
    pending: std::collections::VecDeque<(String, i32, i64, RecordBatch)>,
    /// Fetcher -> decode thread (raw bytes); dropping it shuts the thread down.
    raw_tx: Option<std::sync::mpsc::Sender<RawWork>>,
    /// Decode thread -> here (typed Arrow).
    ready_rx: std::sync::mpsc::Receiver<Decoded>,
    decode_thread: Option<std::thread::JoinHandle<()>>,
    /// Batches submitted to the decode thread but not yet drained back.
    in_flight: usize,
}

#[cfg(feature = "kafka")]
impl Drop for KafkaSplitReader {
    fn drop(&mut self) {
        self.raw_tx.take(); // closing the channel ends the decode loop
        if let Some(handle) = self.decode_thread.take() {
            let _ = handle.join();
        }
        if !self.consumer_queue.is_null() {
            unsafe { rdkafka::bindings::rd_kafka_queue_destroy(self.consumer_queue) };
        }
    }
}

#[cfg(feature = "kafka")]
impl KafkaSplitReader {
    /// `format` selects the decoder: JSON (0) decodes against `output_schema`; Avro (1) builds a
    /// Confluent schema store mapping `schema_id` → `avro_schema` (the writer schema JSON). The decoder
    /// is built and owned by the background decode thread.
    fn open(
        config: &[(String, String)],
        format: i32,
        output_schema: SchemaRef,
        avro_schema: &str,
        schema_id: i32,
    ) -> KafkaSplitReader {
        use rdkafka::config::ClientConfig;

        let mut client = ClientConfig::new();
        for (key, value) in config {
            client.set(key, value);
        }
        let consumer: rdkafka::consumer::BaseConsumer =
            client.create().expect("failed to create kafka consumer");
        // The consumer's queue, for batch draining. (assign/seek still go through the BaseConsumer.)
        let consumer_queue = unsafe {
            use rdkafka::consumer::Consumer;
            rdkafka::bindings::rd_kafka_queue_get_consumer(consumer.client().native_ptr())
        };

        let (raw_tx, raw_rx) = std::sync::mpsc::channel::<RawWork>();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Decoded>();
        let avro_schema = avro_schema.to_string();
        let decode_thread = std::thread::spawn(move || {
            // The same format-dispatched decoder the shallow path uses; built here so its state never
            // crosses threads. Only who produces the body batch (rdkafka vs Flink) differs.
            let decoder = MessageDecoder::new(format, output_schema, &avro_schema, schema_id);
            while let Ok(work) = raw_rx.recv() {
                let decoded = Decoded {
                    topic: work.topic,
                    partition: work.partition,
                    next_offset: work.next_offset,
                    batch: decoder.decode(&work.body),
                };
                if ready_tx.send(decoded).is_err() {
                    break;
                }
            }
        });

        KafkaSplitReader {
            consumer,
            consumer_queue,
            body_schema: Arc::new(Schema::new(vec![Field::new("body", DataType::Binary, true)])),
            next_offsets: HashMap::new(),
            pending: std::collections::VecDeque::new(),
            raw_tx: Some(raw_tx),
            ready_rx,
            decode_thread: Some(decode_thread),
            in_flight: 0,
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
        for i in 0..topics.len() {
            self.next_offsets
                .entry((topics[i].clone(), partitions[i] as i32))
                .or_insert(offsets[i]);
        }
        self.reassign();
    }

    /// Removes the given splits (which reached their stopping offset) from the assignment so the
    /// consumer no longer fetches or blocks on them — mirroring the connector's `unassignPartitions`.
    /// Without this a finished partition makes `poll` block for the timeout at the bounded tail.
    fn unassign_splits(&mut self, topics: &[String], partitions: &[i64]) {
        for i in 0..topics.len() {
            self.next_offsets.remove(&(topics[i].clone(), partitions[i] as i32));
        }
        self.reassign();
    }

    /// (Re)assigns the consumer to exactly the currently-tracked partitions, each seeked to its tracked
    /// offset (or start marker). assign() with explicit offsets replaces the whole assignment.
    fn reassign(&mut self) {
        use rdkafka::consumer::Consumer;
        use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

        if self.next_offsets.is_empty() {
            self.consumer.unassign().expect("failed to unassign");
            return;
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
        use rdkafka::bindings as rdsys;

        // Fetcher thread: batch-drain the consumer queue in ONE FFI call (vs one op+Arc allocation per
        // message via BaseConsumer::poll), copy each payload into a per-partition binary builder, and
        // hand each partition's batch to the decode thread. Decoding overlaps the next poll.
        let mut builders: HashMap<i32, (String, BinaryBuilder, i64)> = HashMap::new();
        let mut messages: Vec<*mut rdsys::rd_kafka_message_t> = vec![std::ptr::null_mut(); max_records];
        let received = unsafe {
            rdsys::rd_kafka_consume_batch_queue(
                self.consumer_queue,
                timeout.as_millis() as std::os::raw::c_int,
                messages.as_mut_ptr(),
                max_records,
            )
        };
        let mut buffered = 0usize;
        if received > 0 {
            for &message_ptr in &messages[..received as usize] {
                let message = unsafe { &*message_ptr };
                if message.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
                    && !message.payload.is_null()
                {
                    let payload =
                        unsafe { std::slice::from_raw_parts(message.payload as *const u8, message.len) };
                    let entry = builders.entry(message.partition).or_insert_with(|| {
                        // Topic resolved once per partition (not per message); pre-size so the binary
                        // buffers don't reallocate as the batch fills.
                        let topic = unsafe {
                            std::ffi::CStr::from_ptr(rdsys::rd_kafka_topic_name(message.rkt))
                        }
                        .to_string_lossy()
                        .into_owned();
                        (topic, BinaryBuilder::with_capacity(max_records, max_records * 64), 0)
                    });
                    entry.1.append_value(payload);
                    entry.2 = message.offset + 1;
                    buffered += 1;
                }
                // Errors here are queue events (e.g. transient connectivity); skip them. Every message
                // (data or event) must be destroyed to release librdkafka's reference.
                unsafe { rdsys::rd_kafka_message_destroy(message_ptr) };
            }
        }
        let tx = self.raw_tx.as_ref().expect("reader is closed");
        for (partition, (topic, mut builder, next_offset)) in builders {
            let body = RecordBatch::try_new(self.body_schema.clone(), vec![Arc::new(builder.finish())])
                .expect("failed to build kafka body batch");
            self.next_offsets.insert((topic.clone(), partition), next_offset);
            tx.send(RawWork { topic, partition, next_offset, body }).expect("decode thread gone");
            self.in_flight += 1;
        }

        // Drain whatever the decode thread has finished (from this and prior polls).
        self.pending.clear();
        while let Ok(decoded) = self.ready_rx.try_recv() {
            self.in_flight -= 1;
            self.pending
                .push_back((decoded.topic, decoded.partition, decoded.next_offset, decoded.batch));
        }
        // When the consumer is drained (nothing polled), block until the decode pipeline empties so the
        // JVM's bounded-finish logic sees every offset before it concludes the split is done.
        if buffered == 0 {
            while self.in_flight > 0 {
                match self.ready_rx.recv() {
                    Ok(decoded) => {
                        self.in_flight -= 1;
                        self.pending.push_back((
                            decoded.topic,
                            decoded.partition,
                            decoded.next_offset,
                            decoded.batch,
                        ));
                    }
                    Err(_) => break,
                }
            }
        }
        self.pending.len()
    }
}

/// Opens a native Kafka split reader for one subtask and returns an opaque handle, released with
/// `closeKafkaConsumer`. `configKeys`/`configValues` are the translated librdkafka config (applied
/// verbatim). `format` is 0 for JSON (decoded against the schema in the C structs) or 1 for Confluent
/// Avro (decoded against `avroSchema` registered at `schemaId`). Splits are added later via
/// `assignKafkaSplits` as the enumerator assigns them.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_openKafkaConsumer<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    format: jint,
    schema_array_address: jlong,
    schema_address: jlong,
    avro_schema: JString<'local>,
    schema_id: jint,
) -> jlong {
    let keys = read_string_array(&mut env, &config_keys);
    let values = read_string_array(&mut env, &config_values);
    let config: Vec<(String, String)> = keys.into_iter().zip(values).collect();
    let schema = import_record_batch(schema_array_address, schema_address).schema();
    let avro_schema: String =
        env.get_string(&avro_schema).map(Into::into).unwrap_or_default();
    let reader =
        KafkaSplitReader::open(&config, format, schema, &avro_schema, schema_id);
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

/// Removes finished splits (reached their bounded stopping offset) from the assignment so the consumer
/// stops fetching/blocking on them. Index-aligned `topics`/`partitions`.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_unassignKafkaSplits<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    topics: JObjectArray<'local>,
    partitions: JLongArray<'local>,
) {
    let reader = unsafe { &mut *(handle as *mut KafkaSplitReader) };
    let topics = read_string_array(&mut env, &topics);
    let partitions = read_longs(&env, &partitions);
    reader.unassign_splits(&topics, &partitions);
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
        reader.pending.pop_front().expect("drainKafkaSplit called with no pending batch");
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

/// Benchmark-only: drive the **production** split reader (poll + the background decode thread) over a
/// whole topic and count the decoded rows **entirely in Rust** — the decoded Arrow batches are consumed
/// in Rust and never exported to the JVM, exactly as they would feed a downstream native operator in a
/// fused pipeline. This is the honest "fastest way to get Arrow batches in Rust" measurement: it
/// excludes the per-batch JVM export that the FLIP-27 DataStream wrapper forces. Returns the row count.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_benchmarkNativeConsume<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    topic: JString<'local>,
    format: jint,
    schema_array_address: jlong,
    schema_address: jlong,
    avro_schema: JString<'local>,
    schema_id: jint,
    max_messages: jlong,
) -> jlong {
    let keys = read_string_array(&mut env, &config_keys);
    let values = read_string_array(&mut env, &config_values);
    let config: Vec<(String, String)> = keys.into_iter().zip(values).collect();
    let topic: String = env.get_string(&topic).expect("failed to read topic").into();
    let schema = import_record_batch(schema_array_address, schema_address).schema();
    let avro_schema: String = env.get_string(&avro_schema).map(Into::into).unwrap_or_default();

    let mut reader = KafkaSplitReader::open(&config, format, schema, &avro_schema, schema_id);
    reader.assign_splits(&[topic], &[0], &[-2]); // partition 0, earliest

    let timeout = std::time::Duration::from_millis(250);
    let mut rows: i64 = 0;
    let mut idle = 0;
    // The topic holds exactly `max_messages`; loop until we've decoded them all. A generous idle guard
    // (≈10s of empty polls) only trips if the broker truly stops delivering, avoiding a hang.
    while rows < max_messages && idle < 40 {
        let count = reader.poll(65536, timeout);
        if count == 0 {
            idle += 1;
            continue;
        }
        idle = 0;
        for (_topic, _partition, _next_offset, batch) in reader.pending.drain(..) {
            rows += batch.num_rows() as i64; // consumed in Rust; no JVM export
        }
    }
    rows
}

/// Benchmark-only: measure librdkafka's raw delivery rate — batch-consume the whole topic and count
/// messages with NO decode and no decode thread, isolating the consumer from everything downstream.
/// Compared against the Java client's raw poll to answer "is librdkafka delivery actually slower here".
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_benchmarkConsumeOnly<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    topic: JString<'local>,
    max_messages: jlong,
) -> jlong {
    use rdkafka::bindings as rdsys;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

    let keys = read_string_array(&mut env, &config_keys);
    let values = read_string_array(&mut env, &config_values);
    let topic: String = env.get_string(&topic).expect("failed to read topic").into();
    let mut client = ClientConfig::new();
    for (key, value) in keys.iter().zip(&values) {
        client.set(key, value);
    }
    let consumer: BaseConsumer = client.create().expect("failed to create kafka consumer");
    // Assign every partition at the beginning — librdkafka fetches them all (one FetchRequest per
    // broker) and merges them onto the single consumer queue this loop drains.
    let metadata = consumer
        .fetch_metadata(Some(&topic), std::time::Duration::from_secs(10))
        .expect("fetch metadata");
    let partitions = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .expect("topic in metadata")
        .partitions();
    let mut tpl = TopicPartitionList::new();
    for partition in partitions {
        tpl.add_partition_offset(&topic, partition.id(), Offset::Beginning).expect("add partition");
    }
    consumer.assign(&tpl).expect("assign");
    let queue = unsafe { rdsys::rd_kafka_queue_get_consumer(consumer.client().native_ptr()) };

    let mut messages: Vec<*mut rdsys::rd_kafka_message_t> = vec![std::ptr::null_mut(); 65536];
    let mut count: i64 = 0;
    let mut idle = 0;
    while count < max_messages && idle < 40 {
        let received =
            unsafe { rdsys::rd_kafka_consume_batch_queue(queue, 250, messages.as_mut_ptr(), 65536) };
        if received <= 0 {
            idle += 1;
            continue;
        }
        idle = 0;
        for &message_ptr in &messages[..received as usize] {
            let message = unsafe { &*message_ptr };
            if message.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
                && !message.payload.is_null()
            {
                count += 1; // no decode — raw delivery only
            }
            unsafe { rdsys::rd_kafka_message_destroy(message_ptr) };
        }
    }
    unsafe { rdsys::rd_kafka_queue_destroy(queue) };
    count
}

/// Benchmark-only: the SERIAL counterpart to `benchmarkNativeConsume` — same rdkafka batch consume and
/// the same `MessageDecoder`, but decode runs INLINE on the consume thread (no decode thread, no channel
/// handoff). Isolates whether the pipelining helps or whether the per-batch handoff is overhead: for a
/// cheap decode (Avro) serial should match or beat the pipelined path; for an expensive decode (JSON)
/// the pipeline should win by overlapping decode with the next poll. Returns the decoded row count.
#[cfg(feature = "kafka")]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_benchmarkNativeConsumeSerial<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_keys: JObjectArray<'local>,
    config_values: JObjectArray<'local>,
    topic: JString<'local>,
    format: jint,
    schema_array_address: jlong,
    schema_address: jlong,
    avro_schema: JString<'local>,
    schema_id: jint,
    max_messages: jlong,
) -> jlong {
    use arrow::array::BinaryBuilder;
    use rdkafka::bindings as rdsys;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

    let keys = read_string_array(&mut env, &config_keys);
    let values = read_string_array(&mut env, &config_values);
    let topic: String = env.get_string(&topic).expect("failed to read topic").into();
    let schema = import_record_batch(schema_array_address, schema_address).schema();
    let avro_schema: String = env.get_string(&avro_schema).map(Into::into).unwrap_or_default();

    let mut client = ClientConfig::new();
    for (key, value) in keys.iter().zip(&values) {
        client.set(key, value);
    }
    let consumer: BaseConsumer = client.create().expect("failed to create kafka consumer");
    let metadata = consumer
        .fetch_metadata(Some(&topic), std::time::Duration::from_secs(10))
        .expect("fetch metadata");
    let mut tpl = TopicPartitionList::new();
    for partition in metadata.topics().iter().find(|t| t.name() == topic).expect("topic").partitions() {
        tpl.add_partition_offset(&topic, partition.id(), Offset::Beginning).expect("add partition");
    }
    consumer.assign(&tpl).expect("assign");
    let queue = unsafe { rdsys::rd_kafka_queue_get_consumer(consumer.client().native_ptr()) };

    // The same decoder the pipelined path builds, just driven inline.
    let decoder = MessageDecoder::new(format, schema, &avro_schema, schema_id);
    let body_schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Binary, true)]));

    let mut messages: Vec<*mut rdsys::rd_kafka_message_t> = vec![std::ptr::null_mut(); 65536];
    let mut rows: i64 = 0;
    let mut idle = 0;
    while rows < max_messages && idle < 40 {
        let received =
            unsafe { rdsys::rd_kafka_consume_batch_queue(queue, 250, messages.as_mut_ptr(), 65536) };
        if received <= 0 {
            idle += 1;
            continue;
        }
        idle = 0;
        let mut builder = BinaryBuilder::with_capacity(received as usize, received as usize * 64);
        for &message_ptr in &messages[..received as usize] {
            let message = unsafe { &*message_ptr };
            if message.err == rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR
                && !message.payload.is_null()
            {
                let payload =
                    unsafe { std::slice::from_raw_parts(message.payload as *const u8, message.len) };
                builder.append_value(payload);
            }
            unsafe { rdsys::rd_kafka_message_destroy(message_ptr) };
        }
        // Decode INLINE — the next batch is not fetched until this returns.
        let body = RecordBatch::try_new(body_schema.clone(), vec![Arc::new(builder.finish())])
            .expect("failed to build kafka body batch");
        rows += decoder.decode(&body).num_rows() as i64;
    }
    unsafe { rdsys::rd_kafka_queue_destroy(queue) };
    rows
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
    // Unique group per call so each timed run re-reads the whole topic from the beginning (a fixed
    // group would leave the warm-up run's position at the end and the timed run would read nothing).
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let group = format!("streamfusion-bench-{}-{}", std::process::id(), nonce);
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
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createIntervalJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_time: jint,
    right_time: jint,
    lower: jlong,
    upper: jlong,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let left_schema = import_schema(left_schema_address);
    let right_schema = import_schema(right_schema_address);
    let predicate = read_join_predicate(
        &mut env,
        &pred_kinds,
        &pred_payload,
        &pred_child_counts,
        &pred_longs,
        &pred_doubles,
        &pred_strings,
    );
    Box::into_raw(Box::new(IntervalJoiner::new(
        left,
        right,
        left_time as usize,
        right_time as usize,
        lower,
        upper,
        predicate,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
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

/// Advances the combined watermark, evicting rows no future arrival can match, and exporting the
/// null-padded rows for evicted outer rows that never matched (empty for an INNER join).
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_advanceIntervalJoiner<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
    let result = joiner.advance(watermark_millis);
    export_record_batch(result, out_array_address, out_schema_address);
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
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreIntervalJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_time: jint,
    right_time: jint,
    lower: jlong,
    upper: jlong,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
    snapshot: JByteArray<'local>,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let left_schema = import_schema(left_schema_address);
    let right_schema = import_schema(right_schema_address);
    let predicate = read_join_predicate(
        &mut env,
        &pred_kinds,
        &pred_payload,
        &pred_child_counts,
        &pred_longs,
        &pred_doubles,
        &pred_strings,
    );
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read join snapshot");
    Box::into_raw(Box::new(IntervalJoiner::restore(
        left,
        right,
        left_time as usize,
        right_time as usize,
        lower,
        upper,
        predicate,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
        &bytes,
    ))) as jlong
}

/// Reads the encoded residual non-equi join predicate (empty `kinds` ⇒ no predicate). It compiles
/// lazily against the joined `[left.., right..]` schema supplied at evaluation time.
fn read_join_predicate(
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

/// Creates a regular (non-windowed) updating joiner and returns an opaque handle. The key column
/// indices locate the equi-join key within each side's input batch (whose trailing column is the
/// `$row_kind$` byte); the join type selects INNER/outer/semi-anti; the two schema addresses seed the
/// per-side data schemas (so outer null-padding can be typed); the encoded arrays carry the optional
/// residual non-equi predicate. The JVM owns the handle and must release it with the matching close.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createUpdatingJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let left_schema = import_schema(left_schema_address);
    let right_schema = import_schema(right_schema_address);
    let predicate = read_join_predicate(
        &mut env,
        &pred_kinds,
        &pred_payload,
        &pred_child_counts,
        &pred_longs,
        &pred_doubles,
        &pred_strings,
    );
    Box::into_raw(Box::new(UpdatingJoiner::new(
        left,
        right,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
        predicate,
    ))) as jlong
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
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreUpdatingJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
    snapshot: JByteArray<'local>,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let left_schema = import_schema(left_schema_address);
    let right_schema = import_schema(right_schema_address);
    let predicate = read_join_predicate(
        &mut env,
        &pred_kinds,
        &pred_payload,
        &pred_child_counts,
        &pred_longs,
        &pred_doubles,
        &pred_strings,
    );
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read updating-join snapshot");
    Box::into_raw(Box::new(UpdatingJoiner::restore(
        left,
        right,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
        predicate,
        &bytes,
    ))) as jlong
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
    offset: jlong,
    limit: jlong,
    output_rank_number: jboolean,
    retracting: jboolean,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
    let handle = if retracting != 0 {
        TopNHandle::Retract(RetractableTopNRanker::new(
            partitions,
            sort,
            offset,
            limit,
            output_rank_number != 0,
        ))
    } else {
        // The append-only ranker is the no-OFFSET path (offset always 0).
        TopNHandle::Append(TopNRanker::new(partitions, sort, limit, output_rank_number != 0))
    };
    Box::into_raw(Box::new(handle)) as jlong
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
    let ranker = unsafe { &mut *(handle as *mut TopNHandle) };
    let result = ranker.push(&import_record_batch(in_array_address, in_schema_address));
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Serializes the ranker's per-partition buffers for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let ranker = unsafe { &mut *(handle as *mut TopNHandle) };
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
    offset: jlong,
    limit: jlong,
    output_rank_number: jboolean,
    retracting: jboolean,
    snapshot: JByteArray<'local>,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read top-n snapshot");
    let handle = if retracting != 0 {
        TopNHandle::Retract(RetractableTopNRanker::restore(
            partitions,
            sort,
            offset,
            limit,
            output_rank_number != 0,
            &bytes,
        ))
    } else {
        TopNHandle::Append(TopNRanker::restore(partitions, sort, limit, output_rank_number != 0, &bytes))
    };
    Box::into_raw(Box::new(handle)) as jlong
}

/// Releases a Top-N ranker handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeTopNRanker<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut TopNHandle));
    }
}

/// Creates a changelog normalizer (keep-last per unique key) and returns an opaque handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createChangelogNormalizer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    key_columns: JIntArray<'local>,
    generate_update_before: jboolean,
) -> jlong {
    let keys = read_columns(&env, &key_columns);
    Box::into_raw(Box::new(ChangelogNormalizer::new(keys, generate_update_before != 0))) as jlong
}

/// Folds an input changelog batch into the keep-last state and exports the normalized changelog.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushChangelogNormalizer<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let normalizer = unsafe { &mut *(handle as *mut ChangelogNormalizer) };
    let result = normalizer.push(&import_record_batch(in_array_address, in_schema_address));
    export_record_batch(result, out_array_address, out_schema_address);
}

/// Serializes the normalizer's per-key last rows for a checkpoint.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_snapshotChangelogNormalizer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    let normalizer = unsafe { &mut *(handle as *mut ChangelogNormalizer) };
    env.byte_array_from_slice(&normalizer.snapshot())
        .expect("failed to allocate changelog-normalize snapshot array")
        .into_raw()
}

/// Rebuilds a changelog normalizer from a snapshot and returns a fresh handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreChangelogNormalizer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    key_columns: JIntArray<'local>,
    generate_update_before: jboolean,
    snapshot: JByteArray<'local>,
) -> jlong {
    let keys = read_columns(&env, &key_columns);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read changelog-normalize snapshot");
    Box::into_raw(Box::new(ChangelogNormalizer::restore(keys, generate_update_before != 0, &bytes)))
        as jlong
}

/// Releases a changelog normalizer handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeChangelogNormalizer<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(Box::from_raw(handle as *mut ChangelogNormalizer));
    }
}

/// Creates an event-time INNER window joiner and returns an opaque handle. The key/window column
/// indices locate the equi-join key and the `window_start`/`window_end` columns within each side's
/// input batch. The JVM owns the handle across calls and must release it with the matching close.
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createWindowJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_window_start: jint,
    left_window_end: jint,
    right_window_start: jint,
    right_window_end: jint,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let left_schema = import_schema(left_schema_address);
    let right_schema = import_schema(right_schema_address);
    let predicate = read_join_predicate(
        &mut env,
        &pred_kinds,
        &pred_payload,
        &pred_child_counts,
        &pred_longs,
        &pred_doubles,
        &pred_strings,
    );
    Box::into_raw(Box::new(WindowJoiner::new(
        left,
        right,
        left_window_start as usize,
        left_window_end as usize,
        right_window_start as usize,
        right_window_end as usize,
        predicate,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
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
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_restoreWindowJoiner<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_window_start: jint,
    left_window_end: jint,
    right_window_start: jint,
    right_window_end: jint,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
    snapshot: JByteArray<'local>,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let left_schema = import_schema(left_schema_address);
    let right_schema = import_schema(right_schema_address);
    let predicate = read_join_predicate(
        &mut env,
        &pred_kinds,
        &pred_payload,
        &pred_child_counts,
        &pred_longs,
        &pred_doubles,
        &pred_strings,
    );
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read window-join snapshot");
    Box::into_raw(Box::new(WindowJoiner::restore(
        left,
        right,
        left_window_start as usize,
        left_window_end as usize,
        right_window_start as usize,
        right_window_end as usize,
        predicate,
        JoinKind::from_code(join_type),
        left_schema,
        right_schema,
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
            Over(OverWindowAggregator::new(value_type, kinds, rt_column, value_column, key_columns, 0, 0))
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
        #[allow(clippy::too_many_arguments)]
        pub fn new(
            left_keys: Vec<usize>,
            right_keys: Vec<usize>,
            left_time: usize,
            right_time: usize,
            lower: i64,
            upper: i64,
            left_schema: SchemaRef,
            right_schema: SchemaRef,
        ) -> Self {
            IntervalJoin(IntervalJoiner::new(
                left_keys,
                right_keys,
                left_time,
                right_time,
                lower,
                upper,
                None,
                JoinKind::Inner,
                left_schema,
                right_schema,
            ))
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
        #[allow(clippy::too_many_arguments)]
        pub fn new(
            left_keys: Vec<usize>,
            right_keys: Vec<usize>,
            left_window_start: usize,
            left_window_end: usize,
            right_window_start: usize,
            right_window_end: usize,
            left_schema: SchemaRef,
            right_schema: SchemaRef,
        ) -> Self {
            WindowJoin(WindowJoiner::new(
                left_keys,
                right_keys,
                left_window_start,
                left_window_end,
                right_window_start,
                right_window_end,
                None,
                JoinKind::Inner,
                left_schema,
                right_schema,
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

    /// A hand-built `FileDescriptorSet` for `bench.Row { int64 id=1; string name=2; double score=3; }`
    /// — what the JVM would serialize off the generated message class for Flink's `protobuf` format.
    fn proto_descriptor_set() -> Vec<u8> {
        use prost_reflect::prost::Message;
        use prost_reflect::prost_types::{
            field_descriptor_proto::{Label, Type},
            DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        };
        let field = |name: &str, number: i32, ty: Type| FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(number),
            label: Some(Label::Optional as i32),
            r#type: Some(ty as i32),
            ..Default::default()
        };
        let message = DescriptorProto {
            name: Some("Row".to_string()),
            field: vec![
                field("id", 1, Type::Int64),
                field("name", 2, Type::String),
                field("score", 3, Type::Double),
            ],
            ..Default::default()
        };
        let file = FileDescriptorProto {
            name: Some("bench.proto".to_string()),
            package: Some("bench".to_string()),
            message_type: vec![message],
            syntax: Some("proto3".to_string()),
            ..Default::default()
        };
        FileDescriptorSet { file: vec![file] }.encode_to_vec()
    }

    // Each body is one bare protobuf message (no framing); ptars decodes the wire format straight into
    // Arrow arrays, deriving the batch schema from the descriptor (columns named by proto field).
    #[test]
    fn protobuf_decode_emits_one_row_per_message() {
        use prost_reflect::prost::Message;
        use prost_reflect::{DescriptorPool, DynamicMessage, Value};

        let descriptor = proto_descriptor_set();
        let message = DescriptorPool::decode(descriptor.as_ref())
            .unwrap()
            .get_message_by_name("bench.Row")
            .unwrap();
        let encode = |id: i64, name: &str, score: f64| {
            let mut m = DynamicMessage::new(message.clone());
            m.set_field_by_name("id", Value::I64(id));
            m.set_field_by_name("name", Value::String(name.to_string()));
            m.set_field_by_name("score", Value::F64(score));
            m.encode_to_vec()
        };
        let row0 = encode(1, "a", 1.5);
        let row1 = encode(2, "b", 2.5);
        let body = bodies(vec![Some(row0.as_slice()), Some(row1.as_slice())]);

        let out = ProtobufDecoder::new(&descriptor, "bench.Row").decode(&body);

        assert_eq!(out.num_rows(), 2);
        let id = out.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.values(), &[1, 2]);
        let names =
            out.column_by_name("name").unwrap().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!((names.value(0), names.value(1)), ("a", "b"));
        let scores =
            out.column_by_name("score").unwrap().as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        assert_eq!(scores.values(), &[1.5, 2.5]);
    }

    // Each body is one CSV record (no header); CSV decode (format 2) emits one typed row per record.
    #[test]
    fn csv_decode_emits_one_row_per_record() {
        let body = bodies(vec![Some(b"1,a,1.5"), Some(b"2,b,2.5")]);
        let out = MessageDecoder::new(2, json_schema(), "", 0).decode(&body);
        assert_eq!(out.num_rows(), 2);
        let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.values(), &[1, 2]);
        let names = out.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!((names.value(0), names.value(1)), ("a", "b"));
        let scores = out.column(2).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        assert_eq!(scores.values(), &[1.5, 2.5]);
    }

    // `raw` (format 3): the body bytes pass through as the single column, cast to the declared type.
    #[test]
    fn raw_decode_passes_bytes_through() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("payload", DataType::Utf8, true)]));
        let body = bodies(vec![Some(b"hello"), Some(b"world")]);
        let out = MessageDecoder::new(3, schema, "", 0).decode(&body);
        assert_eq!(out.num_rows(), 2);
        let col = out.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!((col.value(0), col.value(1)), ("hello", "world"));
    }

    // Bare Avro (format 4): each body is a raw datum (no Confluent framing), decoded against the reader
    // schema we register at synthetic id 0 (the decoder prepends the id-0 header internally).
    #[test]
    fn bare_avro_decode_emits_one_row_per_datum() {
        // Avro binary datum for record { long id; string name; double score }, no framing.
        fn zigzag_varint(n: i64) -> Vec<u8> {
            let mut zz = ((n << 1) ^ (n >> 63)) as u64;
            let mut out = Vec::new();
            loop {
                let mut b = (zz & 0x7f) as u8;
                zz >>= 7;
                if zz != 0 {
                    b |= 0x80;
                }
                out.push(b);
                if zz == 0 {
                    break;
                }
            }
            out
        }
        fn datum(id: i64, name: &str, score: f64) -> Vec<u8> {
            let mut v = zigzag_varint(id);
            v.extend(zigzag_varint(name.len() as i64));
            v.extend_from_slice(name.as_bytes());
            v.extend_from_slice(&score.to_le_bytes());
            v
        }
        let reader_schema = r#"{"type":"record","name":"Row","fields":[
            {"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"score","type":"double"}]}"#;
        let m0 = datum(1, "a", 1.5);
        let m1 = datum(2, "b", 2.5);
        let body = bodies(vec![Some(m0.as_slice()), Some(m1.as_slice())]);

        let out = MessageDecoder::new(4, Arc::new(Schema::empty()), reader_schema, 0).decode(&body);

        assert_eq!(out.num_rows(), 2);
        let id = out.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.values(), &[1, 2]);
        let names =
            out.column_by_name("name").unwrap().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!((names.value(0), names.value(1)), ("a", "b"));
    }

    // Debezium JSON (format 6): the `{before, after, op}` envelope fans out to a columnar changelog —
    // c/r → one INSERT row from `after`, u → UPDATE_BEFORE (from `before`) + UPDATE_AFTER (from `after`),
    // d → one DELETE row from `before` — with each row's `RowKind` on the trailing `$row_kind$` column.
    #[test]
    fn cdc_debezium_decode_emits_changelog() {
        let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op":"c","ts_ms":7}"#;
        let update =
            br#"{"before":{"id":2,"name":"b","score":2.5},"after":{"id":2,"name":"b2","score":3.5},"op":"u"}"#;
        let delete = br#"{"before":{"id":3,"name":"c","score":4.5},"after":null,"op":"d"}"#;
        let body = bodies(vec![Some(insert.as_slice()), Some(update), Some(delete)]);

        let out = MessageDecoder::new(6, json_schema(), "", 0).decode(&body);

        // 1 (insert) + 2 (update) + 1 (delete) physical rows.
        assert_eq!(out.num_rows(), 4);
        let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.values(), &[1, 2, 2, 3]);
        let names = out.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(
            (0..4).map(|i| names.value(i)).collect::<Vec<_>>(),
            vec!["a", "b", "b2", "c"]
        );
        let scores = out.column(2).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        assert_eq!(scores.values(), &[1.5, 2.5, 3.5, 4.5]);
        // INSERT(0), UPDATE_BEFORE(1), UPDATE_AFTER(2), DELETE(3) — Flink's RowKind byte values.
        let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(kinds.values(), &[0, 1, 2, 3]);
        assert_eq!(out.schema().field(3).name(), ROW_KIND_COLUMN);
    }

    // A tombstone (null body) is dropped, leaving the valid records — matching Flink, which skips
    // empty/null messages regardless of error handling.
    #[test]
    fn cdc_debezium_skips_tombstone() {
        let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op":"r"}"#;
        let body = bodies(vec![None, Some(insert.as_slice())]);

        let out = MessageDecoder::new(6, json_schema(), "", 0).decode(&body);

        assert_eq!(out.num_rows(), 1);
        let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.values(), &[1]);
        let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(kinds.values(), &[0]); // "r" snapshot read → INSERT
    }

    // An unrecognized op fails the decode rather than silently dropping the row — Flink throws on it by
    // default, so failing keeps the result identical (the planner routes here only when the table does
    // not set ignore-parse-errors, i.e. Flink is in throw mode too).
    #[test]
    #[should_panic(expected = "unknown CDC operation")]
    fn cdc_unknown_op_fails() {
        let unknown = br#"{"before":null,"after":{"id":9,"name":"z","score":9.5},"op":"x"}"#;
        MessageDecoder::new(6, json_schema(), "", 0).decode(&bodies(vec![Some(unknown.as_slice())]));
    }

    // A null "before" on an update fails (Flink's REPLICA_IDENTITY error), not a silent drop.
    #[test]
    #[should_panic(expected = "null \"before\"")]
    fn cdc_debezium_null_before_update_fails() {
        let update = br#"{"before":null,"after":{"id":2,"name":"b","score":2.5},"op":"u"}"#;
        MessageDecoder::new(6, json_schema(), "", 0).decode(&bodies(vec![Some(update.as_slice())]));
    }

    // OGG JSON (format 7): same nested before/after layout as Debezium, but the op field is `op_type`
    // with I/U/D codes.
    #[test]
    fn cdc_ogg_dialect_uses_op_type() {
        let insert = br#"{"before":null,"after":{"id":1,"name":"a","score":1.5},"op_type":"I"}"#;
        let update =
            br#"{"before":{"id":2,"name":"b","score":2.5},"after":{"id":2,"name":"b2","score":3.5},"op_type":"U"}"#;
        let delete = br#"{"before":{"id":3,"name":"c","score":4.5},"after":null,"op_type":"D"}"#;
        let body = bodies(vec![Some(insert.as_slice()), Some(update), Some(delete)]);

        let out = MessageDecoder::new(7, json_schema(), "", 0).decode(&body);

        assert_eq!(out.num_rows(), 4); // insert + (update→2) + delete
        let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.values(), &[1, 2, 2, 3]);
        let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(kinds.values(), &[0, 1, 2, 3]);
    }

    // Maxwell JSON (format 8): `{data, old, type}` — `data` is the full post-image, `old` only the
    // changed fields. An update's UPDATE_BEFORE is coalesce(old, data) per field (unchanged fields fall
    // back to `data`); a delete reads the row from `data`, not `old`.
    #[test]
    fn cdc_maxwell_merges_partial_old_image() {
        let insert = br#"{"data":{"id":1,"name":"a","score":1.5},"type":"insert"}"#;
        // Only `name` changed (b → b2): `old` carries just `name`; id/score must come from `data`.
        let update = br#"{"data":{"id":2,"name":"b2","score":2.5},"old":{"name":"b"},"type":"update"}"#;
        let delete = br#"{"data":{"id":3,"name":"c","score":3.5},"type":"delete"}"#;
        let body = bodies(vec![Some(insert.as_slice()), Some(update), Some(delete)]);

        let out = MessageDecoder::new(8, json_schema(), "", 0).decode(&body);

        assert_eq!(out.num_rows(), 4);
        let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.values(), &[1, 2, 2, 3]);
        let names = out.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        // UPDATE_BEFORE keeps the old name "b"; the unchanged id/score are pulled from `data`.
        assert_eq!((0..4).map(|i| names.value(i)).collect::<Vec<_>>(), vec!["a", "b", "b2", "c"]);
        let scores = out.column(2).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        assert_eq!(scores.values(), &[1.5, 2.5, 2.5, 3.5]);
        let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(kinds.values(), &[0, 1, 2, 3]);
    }

    // Canal JSON (format 9): `data`/`old` are arrays, so one message fans out per element. An INSERT
    // with a two-row `data` emits two INSERTs; an UPDATE pairs `data[i]` with `old[i]` and merges the
    // partial `old` like Maxwell (UPDATE_BEFORE coalesces old over data).
    #[test]
    fn cdc_canal_fans_out_arrays_and_merges_old() {
        // One INSERT message carrying two rows.
        let insert = br#"{"data":[{"id":1,"name":"a","score":1.5},{"id":2,"name":"b","score":2.5}],"type":"INSERT"}"#;
        // One UPDATE message, one element: only `score` changed (3.5 → 3.75); id/name come from data.
        let update =
            br#"{"data":[{"id":3,"name":"c","score":3.75}],"old":[{"score":3.5}],"type":"UPDATE"}"#;
        // A CREATE (DDL) message is skipped entirely.
        let ddl = br#"{"data":null,"type":"CREATE"}"#;
        let body = bodies(vec![Some(insert.as_slice()), Some(update), Some(ddl)]);

        let out = MessageDecoder::new(9, json_schema(), "", 0).decode(&body);

        // 2 inserts + (update → UB + UA); CREATE dropped.
        assert_eq!(out.num_rows(), 4);
        let id = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id.values(), &[1, 2, 3, 3]);
        let names = out.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!((0..4).map(|i| names.value(i)).collect::<Vec<_>>(), vec!["a", "b", "c", "c"]);
        let scores = out.column(2).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        // UPDATE_BEFORE keeps the old score 3.5; UPDATE_AFTER has the new 3.75.
        assert_eq!(scores.values(), &[1.5, 2.5, 3.5, 3.75]);
        let kinds = out.column(3).as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(kinds.values(), &[0, 0, 1, 2]);
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
        let mut over = OverWindowAggregator::new(0, vec![0], 2, Some(1), vec![0], 0, 0);
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

    // Bounded ROWS frame (1 PRECEDING): each row's SUM covers only itself and the row before it
    // within its partition, recomputed over the frame slice — and the trailing edge drops older rows.
    #[test]
    fn bounded_rows_over_sums_the_frame_slice() {
        let k: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 1, 2]));
        let v: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30, 100]));
        let rt: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            0i64,
            1_000_000_000,
            2_000_000_000,
            500_000_000,
        ]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("rt", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![k, v, rt]).unwrap();
        // frame_kind 1 = bounded ROWS, offset 1 = one preceding row.
        let mut over = OverWindowAggregator::new(0, vec![0], 2, Some(1), vec![0], 1, 1);
        over.push(batch);
        let out = over.flush(2000);
        assert_eq!(out.num_rows(), 4);
        // SUM over {self, prev}: key 1 -> 10, 10+20, 20+30; key 2 (lone row) -> 100.
        assert_eq!(values(&out, 1), vec![10, 20, 30, 100]); // v passed through
        assert_eq!(values(&out, 3), vec![10, 30, 50, 100]);
    }

    // Bounded RANGE frame (1 SECOND PRECEDING): each row's SUM covers the rows within 1000ms of it,
    // by rowtime interval rather than a physical row count.
    #[test]
    fn bounded_range_over_sums_the_time_interval() {
        let k: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 1]));
        let v: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30]));
        let rt: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            0i64,
            1_000_000_000,
            2_000_000_000,
        ]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("rt", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![k, v, rt]).unwrap();
        // frame_kind 2 = bounded RANGE, offset 1000 = a 1000ms preceding interval.
        let mut over = OverWindowAggregator::new(0, vec![0], 2, Some(1), vec![0], 2, 1000);
        over.push(batch);
        let out = over.flush(2000);
        assert_eq!(out.num_rows(), 3);
        // SUM over rows within 1000ms: rt0 -> {10}, rt1000 -> {10,20}, rt2000 -> {20,30}.
        assert_eq!(values(&out, 3), vec![10, 30, 50]);
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
        let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, false);
        // s = 5, 3, 8, 1 for partition 1.
        let out = ranker.push(&topn_batch(vec![1, 1, 1, 1], vec![5, 3, 8, 1]));
        // 5: +I5. 3: +I3 (top2 = {3,5}). 8: rank 3 -> nothing. 1: +I1, -D5 (top2 = {1,3}).
        assert_eq!(row_kinds(&out), vec![0, 0, 3, 0]);
        assert_eq!(values(&out, 1), vec![5, 3, 5, 1]); // the sort-key column of each emitted row
    }

    // Top-2 with the rank number projected: a row entering shifts the rows below it, emitting the
    // UPDATE_BEFORE/UPDATE_AFTER cascade Flink does, and an INSERT for a brand-new rank.
    #[test]
    fn topn_with_rank_number_emits_cascade() {
        let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, true);
        let out = ranker.push(&topn_batch(vec![1, 1, 1, 1], vec![5, 3, 8, 1]));
        // 5: +I(5,1). 3: -U(5,1) +U(3,1) +I(5,2). 8: rank 3 -> nothing.
        // 1: -U(3,1) +U(1,1) -U(5,2) +U(3,2)  [5 pushed past rank 2, retracted by the -U].
        assert_eq!(row_kinds(&out), vec![0, 1, 2, 0, 1, 2, 1, 2]);
        assert_eq!(values(&out, 1), vec![5, 5, 3, 5, 3, 1, 5, 3]); // sort-key column
        assert_eq!(values(&out, 2), vec![1, 1, 1, 2, 1, 1, 2, 2]); // appended rank (w0$o0)
    }

    // Partitions are independent: each keeps its own top-N.
    #[test]
    fn topn_is_per_partition() {
        let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 1, false);
        let out = ranker.push(&topn_batch(vec![1, 2, 1], vec![5, 7, 3]));
        // p1: +I5; p2: +I7; p1 sees 3 < 5 -> -D5 then +I3 (delete first, as the host emits).
        assert_eq!(row_kinds(&out), vec![0, 0, 3, 0]);
        assert_eq!(values(&out, 0), vec![1, 2, 1, 1]); // partition of each emitted row
        assert_eq!(values(&out, 1), vec![5, 7, 5, 3]);
    }

    // The bounded buffer survives a checkpoint, so post-restore ranking continues correctly.
    #[test]
    fn topn_buffer_survives_snapshot_restore() {
        let mut ranker = TopNRanker::new(vec![0], vec![asc(1)], 2, false);
        ranker.push(&topn_batch(vec![1, 1], vec![5, 3])); // top2 = {3, 5}
        let snapshot = ranker.snapshot();
        let mut restored = TopNRanker::restore(vec![0], vec![asc(1)], 2, false, &snapshot);
        // A 1 enters the restored top-2 and displaces the 5.
        let out = restored.push(&topn_batch(vec![1], vec![1]));
        assert_eq!(row_kinds(&out), vec![3, 0]); // -D5, +I1
        assert_eq!(values(&out, 1), vec![5, 1]);
    }

    // The `[k, v]` data schema (no `$row_kind$`) both sides carry in the updating-join tests.
    fn kv_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ]))
    }

    fn inner_joiner() -> UpdatingJoiner {
        UpdatingJoiner::new(vec![0], vec![0], JoinKind::Inner, kv_schema(), kv_schema(), None)
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
        let mut joiner = inner_joiner();
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
        let mut joiner = inner_joiner();
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
        let mut joiner = inner_joiner();
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
        let mut joiner = inner_joiner();
        joiner.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false); // buffer right
        let snapshot = joiner.snapshot();
        let mut restored =
            UpdatingJoiner::restore(vec![0], vec![0], JoinKind::Inner, kv_schema(), kv_schema(), None, &snapshot);
        let out = restored.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true);
        assert_eq!(out.num_rows(), 1);
        assert_eq!(values(&out, 1), vec![10]);
        assert_eq!(values(&out, 3), vec![100]);
    }

    // LEFT OUTER: a left row with no right match emits a null-padded row immediately; when a right
    // row later matches, the null-pad is retracted (-D) and the matched pair emitted (+I).
    #[test]
    fn updating_join_left_outer_null_pads_then_retracts() {
        let mut joiner =
            UpdatingJoiner::new(vec![0], vec![0], JoinKind::LeftOuter, kv_schema(), kv_schema(), None);
        // Left row k=1, v=10: no right match → +I[left + null].
        let out = joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true);
        assert_eq!(row_kinds(&out), vec![0]);
        assert_eq!(values(&out, 1), vec![10]); // left v
        assert!(out.column(3).is_null(0)); // right v nulled
        // Right row k=1, v=100 arrives: -D[left + null], +I[left + right].
        let out = joiner.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false);
        assert_eq!(row_kinds(&out), vec![3, 0]);
        assert!(out.column(3).is_null(0)); // the retracted null-pad's right v
        assert!(!out.column(3).is_null(1)); // the matched pair's right v is present
        assert_eq!(values(&out, 1), vec![10, 10]); // both rows carry the left v
    }

    // LEFT OUTER on a left key that never matches: the null-pad is emitted once and retracted when
    // the left row is deleted — net materialized result is empty.
    #[test]
    fn updating_join_left_outer_unmatched_retract() {
        let mut joiner =
            UpdatingJoiner::new(vec![0], vec![0], JoinKind::LeftOuter, kv_schema(), kv_schema(), None);
        let out = joiner.push(&changelog_join_batch(vec![7], vec![70], vec![0]), true);
        assert_eq!(row_kinds(&out), vec![0]); // +I[left + null]
        let out = joiner.push(&changelog_join_batch(vec![7], vec![70], vec![3]), true);
        assert_eq!(row_kinds(&out), vec![3]); // -D[left + null]
        assert!(out.column(3).is_null(0));
    }

    // SEMI: a left row is emitted once it has a right match; ANTI would emit it while unmatched.
    #[test]
    fn updating_join_semi_emits_on_match() {
        let mut joiner = UpdatingJoiner::new(vec![0], vec![0], JoinKind::Semi, kv_schema(), kv_schema(), None);
        // Left row with no right match → nothing (semi).
        assert_eq!(joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true).num_rows(), 0);
        // Right row arrives → emit the left row (+I), one column-set (left only).
        let out = joiner.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false);
        assert_eq!(row_kinds(&out), vec![0]);
        assert_eq!(out.num_columns(), 3); // left k, left v, $row_kind$ (no right columns)
        assert_eq!(values(&out, 1), vec![10]);
    }

    // ANTI: a left row is emitted while it has no match, and retracted (-D) once a match arrives.
    #[test]
    fn updating_join_anti_retracts_on_match() {
        let mut joiner = UpdatingJoiner::new(vec![0], vec![0], JoinKind::Anti, kv_schema(), kv_schema(), None);
        let out = joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true);
        assert_eq!(row_kinds(&out), vec![0]); // +I[left] (no match yet)
        let out = joiner.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false);
        assert_eq!(row_kinds(&out), vec![3]); // -D[left] (now matched)
        assert_eq!(values(&out, 1), vec![10]);
    }

    // A residual non-equi predicate gates which same-key pairs are matches. `left.v > right.v`
    // (cols [k, lv, k0, rv] = indices [0,1,2,3]) over an INNER join: of two buffered right rows only
    // the one whose v is below the left's v matches.
    #[test]
    fn updating_join_applies_non_equi_predicate() {
        let predicate = JoinPredicate {
            kinds: vec![6, 0, 0],      // CALL(>), input_ref, input_ref
            payload: vec![10, 1, 3],   // op GREATER_THAN; left.v (col 1) > right.v (col 3)
            child_counts: vec![2, 0, 0],
            longs: vec![],
            doubles: vec![],
            strings: vec![],
            compiled: None,
        };
        let mut joiner = UpdatingJoiner::new(
            vec![0],
            vec![0],
            JoinKind::Inner,
            kv_schema(),
            kv_schema(),
            Some(predicate),
        );
        // Buffer two right rows for k=1: v=5 and v=20.
        joiner.push(&changelog_join_batch(vec![1, 1], vec![5, 20], vec![0, 0]), false);
        // Left row k=1, v=10 → matches only the right v=5 (10 > 5), not v=20.
        let out = joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true);
        assert_eq!(out.num_rows(), 1);
        assert_eq!(values(&out, 3), vec![5]); // the one right row passing left.v > right.v
    }

    // The degree survives a checkpoint: a restored LEFT OUTER joiner still retracts the null-pad when
    // the first match arrives post-restore.
    #[test]
    fn updating_join_outer_degree_survives_snapshot_restore() {
        let mut joiner =
            UpdatingJoiner::new(vec![0], vec![0], JoinKind::LeftOuter, kv_schema(), kv_schema(), None);
        joiner.push(&changelog_join_batch(vec![1], vec![10], vec![0]), true); // +I[left+null], degree 0
        let snapshot = joiner.snapshot();
        let mut restored = UpdatingJoiner::restore(
            vec![0],
            vec![0],
            JoinKind::LeftOuter,
            kv_schema(),
            kv_schema(),
            None,
            &snapshot,
        );
        let out = restored.push(&changelog_join_batch(vec![1], vec![100], vec![0]), false);
        assert_eq!(row_kinds(&out), vec![3, 0]); // -D[left+null], +I[left+right]
    }

    // The `[k, v, rt]` data schema both sides of the interval-join tests carry.
    fn interval_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("rt", DataType::Int64, false),
        ]))
    }

    // An INNER interval joiner over the `[k, v, rt]` schema (key col 0, rowtime col 2).
    fn inner_interval_joiner(lower: i64, upper: i64) -> IntervalJoiner {
        IntervalJoiner::new(
            vec![0],
            vec![0],
            2,
            2,
            lower,
            upper,
            None,
            JoinKind::Inner,
            interval_schema(),
            interval_schema(),
        )
    }

    // A `[k, v, rt]` batch with int64 rowtime (epoch millis) for the interval-join tests.
    fn join_batch(k: Vec<i64>, v: Vec<i64>, rt: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(interval_schema(), vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(v)),
            Arc::new(Int64Array::from(rt)),
        ])
        .unwrap()
    }

    // INNER interval join: a left row matches a buffered right row of the same key whose rowtime is
    // within [rt + lower, rt + upper]; output columns are left ++ right.
    #[test]
    fn interval_join_emits_matched_pairs() {
        // a.rt BETWEEN b.rt - 1000 AND b.rt + 1000, single equi-key on column 0, rt is column 2.
        let mut joiner = inner_interval_joiner(-1000, 1000);
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
        let mut joiner = inner_interval_joiner(-1000, 1000);
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
        let mut joiner = inner_interval_joiner(-1000, 1000);
        joiner.push_left(join_batch(vec![1], vec![10], vec![5000]));
        // Watermark 6000: left.rt - lower = 5000 - (-1000) = 6000, not > 6000, so the row is evicted.
        joiner.advance(6000);
        // A right row that would otherwise match (delta -500) finds nothing buffered.
        assert_eq!(joiner.push_right(join_batch(vec![1], vec![100], vec![5500])).num_rows(), 0);
    }

    // The `[k, v, window_start, window_end]` data schema the window-join tests carry.
    fn window_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
        ]))
    }

    // A window joiner of the given kind (key col 0, window bounds cols 2/3) over `window_schema`.
    fn window_joiner(kind: JoinKind) -> WindowJoiner {
        WindowJoiner::new(vec![0], vec![0], 2, 3, 2, 3, None, kind, window_schema(), window_schema())
    }

    // A `[k, v, window_start, window_end]` batch (window bounds as int64 millis) for window-join tests.
    fn window_batch(k: Vec<i64>, v: Vec<i64>, ws: Vec<i64>, we: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(window_schema(), vec![
            Arc::new(Int64Array::from(k)),
            Arc::new(Int64Array::from(v)),
            Arc::new(Int64Array::from(ws)),
            Arc::new(Int64Array::from(we)),
        ])
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
        let mut joiner = window_joiner(JoinKind::Inner);
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
        let mut joiner = window_joiner(JoinKind::Inner);
        joiner.push_left(window_batch(vec![1], vec![10], vec![0], vec![1000]));
        joiner.push_right(window_batch(vec![1], vec![100], vec![0], vec![1000]));
        let snapshot = joiner.snapshot();
        let mut restored = WindowJoiner::restore(
            vec![0],
            vec![0],
            2,
            3,
            2,
            3,
            None,
            JoinKind::Inner,
            window_schema(),
            window_schema(),
            &snapshot,
        );
        let out = restored.flush(1000);
        assert_eq!(left_right_values(&out), vec![(10, 100)]);
    }

    // LEFT window join: a left row whose window has no matching right row is null-padded when the
    // window closes (append-only — emitted once at flush).
    #[test]
    fn window_left_join_null_pads_unmatched() {
        let mut joiner = window_joiner(JoinKind::LeftOuter);
        // Window [0,1000): left k=1 (matches right) and k=2 (no right match); right k=1 only.
        joiner.push_left(window_batch(vec![1, 2], vec![10, 20], vec![0, 0], vec![1000, 1000]));
        joiner.push_right(window_batch(vec![1], vec![100], vec![0], vec![1000]));
        let out = joiner.flush(1000);
        // k=1 emits the matched pair [10,100]; k=2 emits [20, null].
        assert_eq!(out.num_rows(), 2);
        let mut left_vs = values(&out, 1);
        left_vs.sort_unstable();
        assert_eq!(left_vs, vec![10, 20]);
        // Exactly one row (k=2) has a null right v (column 5).
        let null_right = (0..out.num_rows()).filter(|&i| out.column(5).is_null(i)).count();
        assert_eq!(null_right, 1);
    }

    // Buffered rows survive a snapshot/restore round trip and still match afterward.
    #[test]
    fn interval_join_restores_buffered_rows() {
        let mut joiner = inner_interval_joiner(-1000, 1000);
        joiner.push_right(join_batch(vec![1], vec![100], vec![5500]));
        let snapshot = joiner.snapshot();
        let mut restored = IntervalJoiner::restore(
            vec![0],
            vec![0],
            2,
            2,
            -1000,
            1000,
            None,
            JoinKind::Inner,
            interval_schema(),
            interval_schema(),
            &snapshot,
        );
        let out = restored.push_left(join_batch(vec![1], vec![10], vec![5000]));
        assert_eq!(out.num_rows(), 1);
        assert_eq!(values(&out, 4), vec![100]);
    }

    fn left_interval_joiner(lower: i64, upper: i64) -> IntervalJoiner {
        IntervalJoiner::new(
            vec![0],
            vec![0],
            2,
            2,
            lower,
            upper,
            None,
            JoinKind::LeftOuter,
            interval_schema(),
            interval_schema(),
        )
    }

    // LEFT interval join: a left row that never matches is null-padded once its interval is evicted by
    // the watermark (append-only — emitted once). A left row evicts when `rt - lower <= watermark`.
    #[test]
    fn interval_left_join_null_pads_unmatched_on_eviction() {
        let mut joiner = left_interval_joiner(-1000, 1000);
        // Left row k=1, v=10, rt=5000; no right buffered → no immediate match.
        assert_eq!(joiner.push_left(join_batch(vec![1], vec![10], vec![5000])).num_rows(), 0);
        // Watermark below the eviction point: not yet evicted, nothing emitted.
        assert_eq!(joiner.advance(5000).num_rows(), 0);
        // Watermark at/above 5000 - (-1000) = 6000: the left row is evicted unmatched → [left+null]
        // (append-only, so no $row_kind$ column — just the padded row).
        let out = joiner.advance(6000);
        assert_eq!(out.num_rows(), 1);
        assert_eq!(values(&out, 1), vec![10]); // left v
        assert!(out.column(3).is_null(0)); // right k nulled
        assert!(out.column(4).is_null(0)); // right v nulled
    }

    // LEFT interval join: a left row that matches a right row is emitted as a pair and not
    // null-padded at eviction.
    #[test]
    fn interval_left_join_matched_row_not_padded() {
        let mut joiner = left_interval_joiner(-1000, 1000);
        joiner.push_left(join_batch(vec![1], vec![10], vec![5000]));
        // Right row k=1, rt=5000 within [rt-1000, rt+1000] of the left → emits the matched pair.
        let out = joiner.push_right(join_batch(vec![1], vec![100], vec![5000]));
        assert_eq!(out.num_rows(), 1);
        assert_eq!(values(&out, 4), vec![100]);
        // Evict the left row: it matched, so no null-pad.
        assert_eq!(joiner.advance(10000).num_rows(), 0);
    }

    // The match flags survive a checkpoint: a restored LEFT interval joiner does not re-pad a left
    // row that matched before the snapshot.
    #[test]
    fn interval_left_join_match_flags_survive_restore() {
        let mut joiner = left_interval_joiner(-1000, 1000);
        joiner.push_left(join_batch(vec![1], vec![10], vec![5000]));
        joiner.push_right(join_batch(vec![1], vec![100], vec![5000])); // marks the left row matched
        let snapshot = joiner.snapshot();
        let mut restored = IntervalJoiner::restore(
            vec![0],
            vec![0],
            2,
            2,
            -1000,
            1000,
            None,
            JoinKind::LeftOuter,
            interval_schema(),
            interval_schema(),
            &snapshot,
        );
        // Evicting the (matched) left row post-restore must emit no null-pad.
        assert_eq!(restored.advance(10000).num_rows(), 0);
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

