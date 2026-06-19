use arrow::array::{
    make_array, new_empty_array, Array, ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch,
    StructArray, UInt32Array,
};
use arrow::compute::{concat_batches, filter_record_batch, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::common::DFSchema;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::{Accumulator, AggregateUDF, Operator};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::expressions::{binary, col, lit};
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::prelude::{col as logical_col, lit as logical_lit, SessionContext};
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use jni::objects::{
    JByteArray, JClass, JDoubleArray, JIntArray, JLongArray, JObjectArray, JString,
};
use jni::sys::{jbyteArray, jint, jlong, jstring};
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
        if self.result_type == DataType::Int32 {
            let array = values[0].as_any().downcast_ref::<Int32Array>().expect("value must be int32");
            for value in array.iter().flatten() {
                self.sum += i64::from(value);
                self.count += 1;
            }
        } else {
            let array = values[0].as_any().downcast_ref::<Int64Array>().expect("value must be int64");
            for value in array.iter().flatten() {
                self.sum += value;
                self.count += 1;
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
        let average = (self.count != 0).then(|| self.sum / self.count);
        Ok(if self.result_type == DataType::Int32 {
            ScalarValue::Int32(average.map(|a| a as i32))
        } else {
            ScalarValue::Int64(average)
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

/// The per-window aggregate. Built-in aggregates come from DataFusion; integer average and int32 sum
/// are small custom accumulators so their results match the host exactly. All expose mergeable
/// partial state, so windows accumulate incrementally and checkpoint uniformly.
enum WindowAggregate {
    Builtin(AggregateFunctionExpr),
    IntegerAvg(DataType),
    WrappingIntSum,
}

impl WindowAggregate {
    fn new(kind: i64, value_type: &DataType) -> Self {
        match kind {
            // SUM over int32 keeps the host's narrow, wrapping semantics rather than widening.
            0 if *value_type == DataType::Int32 => WindowAggregate::WrappingIntSum,
            0..=3 => WindowAggregate::Builtin(build_builtin(kind, value_type)),
            // Integer AVG returns the input integer type (truncating); carry it for the result.
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
        }
    }

    /// The aggregate's output type (e.g. int64 for a sum of int64, float64 for a sum of float64).
    fn result_type(&self) -> DataType {
        match self {
            WindowAggregate::Builtin(aggregate) => aggregate.field().data_type().clone(),
            WindowAggregate::IntegerAvg(result_type) => result_type.clone(),
            WindowAggregate::WrappingIntSum => DataType::Int32,
        }
    }
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
}

impl TumblingAggregator {
    fn new(
        window_millis: i64,
        slide_millis: i64,
        cumulative: bool,
        value_type: i64,
        kinds: Vec<i64>,
    ) -> Self {
        let value_type = value_data_type(value_type);
        TumblingAggregator {
            window_millis,
            slide_millis,
            cumulative,
            aggregates: kinds.into_iter().map(|kind| WindowAggregate::new(kind, &value_type)).collect(),
            windows: BTreeMap::new(),
            key_types: Vec::new(),
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
        let value = batch.column_by_name("value").expect("missing value column");
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);

        // Group the row positions for each (window, key); the value column is sliced by type-
        // agnostic take, so the accumulators see the value column's own type (int, double, ...).
        let mut grouped: ahash::HashMap<(i64, i64, GroupKey), Vec<u32>> = ahash::HashMap::default();
        let mut windows = Vec::new();
        for row in 0..batch.num_rows() {
            let mut key = read_key(&key_arrays, row);
            self.windows_for(ts.value(row), &mut windows);
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
            let column = take(value, &UInt32Array::from(rows), None).expect("failed to take values");
            for accumulator in self.accumulators(start, end, key) {
                accumulator.update_batch(std::slice::from_ref(&column)).expect("failed to update");
            }
        }
    }

    /// Finalizes and removes closed windows, emitting
    /// `[key, window_start, window_end, result0..resultN-1]`. The end is carried explicitly since
    /// cumulative windows sharing a start differ by it; each result column takes the aggregate's
    /// own output type.
    fn flush(&mut self, watermark: i64) -> RecordBatch {
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

        // A slice belongs to `window_millis / slide_millis` windows — one for a tumbling global
        // (slide == size), several for a hopping global where slices are shared across the
        // overlapping windows. Merge each slice partial into every window that contains it.
        let num_windows = self.window_millis / self.slide_millis;
        for row in 0..batch.num_rows() {
            let slice_end = slice_ends.value(row);
            let key = read_key(&key_arrays, row);
            for j in 0..num_windows {
                let end = slice_end + j * self.slide_millis;
                let start = end - self.window_millis;
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

    fn restore(
        window_millis: i64,
        slide_millis: i64,
        cumulative: bool,
        value_type: i64,
        kinds: Vec<i64>,
        bytes: &[u8],
    ) -> Self {
        let mut aggregator =
            TumblingAggregator::new(window_millis, slide_millis, cumulative, value_type, kinds);
        let field_counts: Vec<usize> =
            aggregator.aggregates.iter().map(|a| a.state_fields().len()).collect();
        let state_field_total: usize = field_counts.iter().sum();
        let reader = arrow::ipc::reader::StreamReader::try_new(bytes, None)
            .expect("failed to open snapshot reader");
        for batch in reader {
            let batch = batch.expect("failed to read snapshot");
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
    fn new(gap_millis: i64, value_type: i64, kinds: Vec<i64>) -> Self {
        let value_type = value_data_type(value_type);
        SessionAggregator {
            gap_millis,
            aggregates: kinds.into_iter().map(|kind| WindowAggregate::new(kind, &value_type)).collect(),
            sessions: HashMap::new(),
            key_types: Vec::new(),
        }
    }

    fn update(&mut self, batch: &RecordBatch) {
        let ts = column_i64(batch, "ts");
        let value = batch.column_by_name("value").expect("missing value column");
        let key_arrays = key_arrays(batch);
        self.key_types = key_types(&key_arrays);

        for row in 0..batch.num_rows() {
            let key = read_key(&key_arrays, row);
            let candidate_start = ts.value(row);
            let candidate_end = candidate_start + self.gap_millis;
            let value = take(value, &UInt32Array::from(vec![row as u32]), None).expect("take value");

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
            for accumulator in accumulators.iter_mut() {
                accumulator.update_batch(std::slice::from_ref(&value)).expect("failed to update");
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

    fn restore(gap_millis: i64, value_type: i64, kinds: Vec<i64>, bytes: &[u8]) -> Self {
        let mut aggregator = SessionAggregator::new(gap_millis, value_type, kinds);
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
        // Narrow integer literals carry their declared width so arithmetic evaluates in the same
        // type as the host (e.g. `int * 2` stays int32 and wraps), not a widened type.
        7 => logical_lit(longs[arg] as i32),
        8 => logical_lit(longs[arg] as i16),
        9 => logical_lit(longs[arg] as i8),
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

/// Combines decoded operands by op code: arithmetic, the six comparisons, and AND/OR/NOT.
fn build_call(op: i64, args: Vec<datafusion::prelude::Expr>) -> datafusion::prelude::Expr {
    let mut it = args.into_iter();
    let mut next = || it.next().expect("missing operand");
    match op {
        0 => next() + next(),
        1 => next() - next(),
        2 => next() * next(),
        10 => next().gt(next()),
        11 => next().gt_eq(next()),
        12 => next().lt(next()),
        13 => next().lt_eq(next()),
        14 => next().eq(next()),
        15 => next().not_eq(next()),
        20 => next().and(next()),
        21 => next().or(next()),
        22 => !next(),
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
        kinds: read_kinds(&env, &kinds),
        payload: read_kinds(&env, &payload),
        child_counts: read_kinds(&env, &child_counts),
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

/// Copies a directory of Parquet files to another, entirely in Arrow: DataFusion reads the input as
/// columnar batches and writes them straight back to Parquet, with no row materialization. This is a
/// whole `INSERT INTO parquet SELECT * FROM parquet` running natively as one columnar job — the data
/// is never transposed to rows, so the pipeline stays columnar from source to sink.
fn copy_parquet(in_dir: &str, out_dir: &str) {
    runtime().block_on(async {
        let ctx = SessionContext::new();
        let frame = ctx
            .read_parquet(in_dir, datafusion::prelude::ParquetReadOptions::default())
            .await
            .expect("failed to read parquet");
        frame
            .write_parquet(out_dir, datafusion::dataframe::DataFrameWriteOptions::new(), None)
            .await
            .expect("failed to write parquet");
    });
}

/// Runs a native Parquet-to-Parquet copy: reads the input directory and writes the output directory.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_copyParquet<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_dir: JString<'local>,
    out_dir: JString<'local>,
) {
    let in_dir: String = env.get_string(&in_dir).expect("failed to read in_dir").into();
    let out_dir: String = env.get_string(&out_dir).expect("failed to read out_dir").into();
    copy_parquet(&in_dir, &out_dir);
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
    value_type: jint,
    aggregate_kinds: JIntArray<'local>,
) -> jlong {
    let kinds = read_kinds(&env, &aggregate_kinds);
    Box::into_raw(Box::new(TumblingAggregator::new(
        window_millis,
        slide_millis,
        false,
        value_type as i64,
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
    value_type: jint,
    aggregate_kinds: JIntArray<'local>,
) -> jlong {
    let kinds = read_kinds(&env, &aggregate_kinds);
    Box::into_raw(Box::new(TumblingAggregator::new(
        max_size_millis,
        step_millis,
        true,
        value_type as i64,
        kinds,
    ))) as jlong
}

/// Reads a JVM int[] of aggregate kinds into a Vec.
fn read_kinds(env: &JNIEnv, kinds: &JIntArray) -> Vec<i64> {
    let length = env.get_array_length(kinds).expect("failed to read kinds length");
    let mut buffer = vec![0i32; length as usize];
    env.get_int_array_region(kinds, 0, &mut buffer).expect("failed to read kinds");
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
    value_type: jint,
    aggregate_kinds: JIntArray<'local>,
    snapshot: JByteArray<'local>,
) -> jlong {
    let kinds = read_kinds(&env, &aggregate_kinds);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read snapshot");
    Box::into_raw(Box::new(TumblingAggregator::restore(
        window_millis,
        slide_millis,
        false,
        value_type as i64,
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
    value_type: jint,
    aggregate_kinds: JIntArray<'local>,
    snapshot: JByteArray<'local>,
) -> jlong {
    let kinds = read_kinds(&env, &aggregate_kinds);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read snapshot");
    Box::into_raw(Box::new(TumblingAggregator::restore(
        max_size_millis,
        step_millis,
        true,
        value_type as i64,
        kinds,
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
    value_type: jint,
    aggregate_kinds: JIntArray<'local>,
) -> jlong {
    let kinds = read_kinds(&env, &aggregate_kinds);
    Box::into_raw(Box::new(SessionAggregator::new(gap_millis, value_type as i64, kinds))) as jlong
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
    value_type: jint,
    aggregate_kinds: JIntArray<'local>,
    snapshot: JByteArray<'local>,
) -> jlong {
    let kinds = read_kinds(&env, &aggregate_kinds);
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read snapshot");
    Box::into_raw(Box::new(SessionAggregator::restore(gap_millis, value_type as i64, kinds, &bytes)))
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
            Tumbling(TumblingAggregator::new(window_millis, window_millis, false, value_type, kinds))
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
            Session(SessionAggregator::new(gap_millis, value_type, kinds))
        }

        pub fn update(&mut self, batch: &RecordBatch) {
            self.0.update(batch);
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

    // A native Parquet-to-Parquet copy reads an input directory and writes the rows to the output.
    #[test]
    fn copies_parquet_directory() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let batch = sample_batch();
        let in_dir = std::env::temp_dir().join("streamfusion_copy_in");
        let out_dir = std::env::temp_dir().join("streamfusion_copy_out");
        let _ = std::fs::remove_dir_all(&in_dir);
        let _ = std::fs::remove_dir_all(&out_dir);
        std::fs::create_dir_all(&in_dir).unwrap();
        write_parquet(&batch, in_dir.join("data.parquet").to_str().unwrap());

        copy_parquet(in_dir.to_str().unwrap(), out_dir.to_str().unwrap());

        let mut rows = 0usize;
        for entry in std::fs::read_dir(&out_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                let file = std::fs::File::open(&path).unwrap();
                let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
                for read in reader {
                    rows += read.unwrap().num_rows();
                }
            }
        }
        assert_eq!(rows, batch.num_rows());
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
