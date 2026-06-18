use arrow::array::{make_array, Array, Int32Array, Int64Array, RecordBatch, StructArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{binary, col, lit};
use datafusion::prelude::{col as logical_col, lit as logical_lit, SessionContext};
use futures::StreamExt;
use jni::objects::{JByteArray, JClass};
use jni::sys::{jbyteArray, jint, jlong, jstring};
use jni::JNIEnv;
use std::collections::BTreeMap;
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

/// Event-time tumbling-window sum that holds open windows across batches. Mirrors the upstream
/// streaming engine's window operator: windows are keyed by their start in an ordered map, each
/// incoming batch folds into the running per-window totals, and a window is emitted and dropped
/// only once a watermark guarantees no earlier data can still arrive.
struct TumblingAggregator {
    window_millis: i64,
    totals: BTreeMap<i64, i64>,
}

impl TumblingAggregator {
    fn new(window_millis: i64) -> Self {
        TumblingAggregator { window_millis, totals: BTreeMap::new() }
    }

    fn update(&mut self, batch: &RecordBatch) {
        let ts = batch
            .column_by_name("ts")
            .expect("missing ts column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ts must be int64");
        let value = batch
            .column_by_name("value")
            .expect("missing value column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value must be int64");
        for row in 0..batch.num_rows() {
            let window_start = ts.value(row) - ts.value(row).rem_euclid(self.window_millis);
            *self.totals.entry(window_start).or_insert(0) += value.value(row);
        }
    }

    /// Emits and removes every window whose end is at or before the watermark.
    fn flush(&mut self, watermark: i64) -> RecordBatch {
        let closed: Vec<i64> = self
            .totals
            .keys()
            .copied()
            .take_while(|start| start + self.window_millis <= watermark)
            .collect();

        let mut starts = Vec::with_capacity(closed.len());
        let mut totals = Vec::with_capacity(closed.len());
        for start in closed {
            starts.push(start);
            totals.push(self.totals.remove(&start).expect("window present"));
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("window_start", DataType::Int64, false),
            Field::new("total", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(starts)), Arc::new(Int64Array::from(totals))],
        )
        .expect("failed to build result batch")
    }

    /// Serializes the open windows as a flat sequence of (start, total) little-endian i64 pairs.
    fn snapshot(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.totals.len() * 16);
        for (start, total) in &self.totals {
            bytes.extend_from_slice(&start.to_le_bytes());
            bytes.extend_from_slice(&total.to_le_bytes());
        }
        bytes
    }

    fn restore(window_millis: i64, bytes: &[u8]) -> Self {
        let mut totals = BTreeMap::new();
        for pair in bytes.chunks_exact(16) {
            let start = i64::from_le_bytes(pair[0..8].try_into().expect("8 bytes"));
            let total = i64::from_le_bytes(pair[8..16].try_into().expect("8 bytes"));
            totals.insert(start, total);
        }
        TumblingAggregator { window_millis, totals }
    }
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
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    window_millis: jlong,
) -> jlong {
    Box::into_raw(Box::new(TumblingAggregator::new(window_millis))) as jlong
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
    let aggregator = unsafe { &*(handle as *const TumblingAggregator) };
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
    snapshot: JByteArray<'local>,
) -> jlong {
    let bytes = env.convert_byte_array(&snapshot).expect("failed to read snapshot");
    Box::into_raw(Box::new(TumblingAggregator::restore(window_millis, &bytes))) as jlong
}
