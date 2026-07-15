use crate::*;

/// Takes ownership of a batch the JVM exported through the C Data Interface, swapping in released
/// placeholders so the producer's release callbacks fire once when the imported data drops.
/// The JVM handle, captured on the first JNI entry that may build a UDF-bearing expression, so a native
/// `JvmUdf` node can attach the (already JVM-owned) task thread and upcall the UDF bridge. Set once.
pub(crate) static JVM: OnceLock<jni::JavaVM> = OnceLock::new();

pub(crate) fn capture_jvm(env: &JNIEnv) {
    if JVM.get().is_none() {
        if let Ok(vm) = env.get_java_vm() {
            let _ = JVM.set(vm);
        }
    }
}

/// Anchors libmimalloc-sys's object file into the link: nothing references the crate by symbol
/// until build.rs's link aliases resolve, so without this the archive member they alias into would
/// be dropped. See the `mimalloc` feature in Cargo.toml.
#[cfg(feature = "mimalloc")]
#[used]
pub(crate) static FORCE_LINK_MIMALLOC: unsafe extern "C" fn(*mut std::os::raw::c_void) =
    libmimalloc_sys::mi_free;

/// Raises the managed-memory-limit exception on the calling JVM thread; the native call returns
/// immediately after, so the task fails with the budget message instead of the container OOM-killing
/// the process.
pub(crate) fn throw_memory_limit(env: &mut JNIEnv, message: &str) {
    let _ = env.throw_new(
        "io/github/jordepic/streamfusion/NativeMemoryLimitException",
        message,
    );
}

/// Frames every raw keyed-state payload with its big-endian Flink key-group id and returns the
/// partitions as one JNI object array. This keeps checkpoint partitioning to one native pass and
/// lets Java stream each payload directly into Flink's keyed-state output.
pub(crate) fn keyed_state_partition_array(
    env: &mut JNIEnv,
    partitions: BTreeMap<i32, Vec<u8>>,
    operator: &str,
) -> jni::sys::jobjectArray {
    let output = env
        .new_object_array(
            partitions.len() as i32,
            "[B",
            jni::objects::JObject::null(),
        )
        .unwrap_or_else(|_| panic!("allocate {operator} raw keyed-state partitions"));
    for (index, (key_group, payload)) in partitions.into_iter().enumerate() {
        let mut framed = Vec::with_capacity(std::mem::size_of::<i32>() + payload.len());
        framed.extend_from_slice(&key_group.to_be_bytes());
        framed.extend_from_slice(&payload);
        let framed = env
            .byte_array_from_slice(&framed)
            .unwrap_or_else(|_| panic!("allocate {operator} raw keyed-state partition"));
        env.set_object_array_element(&output, index as i32, framed)
            .unwrap_or_else(|_| panic!("store {operator} raw keyed-state partition"));
    }
    output.into_raw()
}

pub(crate) fn import_record_batch(array_address: jlong, schema_address: jlong) -> RecordBatch {
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
pub(crate) fn import_schema(schema_address: jlong) -> SchemaRef {
    let ffi_schema = unsafe {
        std::ptr::replace(schema_address as *mut FFI_ArrowSchema, FFI_ArrowSchema::empty())
    };
    Arc::new(Schema::try_from(&ffi_schema).expect("failed to import Arrow schema"))
}

/// Exports a batch into consumer-allocated C structs; the JVM owns and releases it after import.
pub(crate) fn export_record_batch(batch: RecordBatch, array_address: jlong, schema_address: jlong) {
    let out_data = StructArray::from(batch).to_data();
    let out_array = FFI_ArrowArray::new(&out_data);
    let out_schema =
        FFI_ArrowSchema::try_from(out_data.data_type()).expect("failed to export Arrow schema");
    unsafe {
        std::ptr::write(array_address as *mut FFI_ArrowArray, out_array);
        std::ptr::write(schema_address as *mut FFI_ArrowSchema, out_schema);
    }
}

/// Live native handles by type — the leak sentinel the test harness polls. Every handle the JVM
/// creates increments its type's count and every close decrements it, so a non-empty breakdown
/// after all operators have closed pinpoints a missing close call: memory the JVM's own tooling
/// cannot see. (Comet only logs a warning on this condition; tests can afford to fail hard.)
pub(crate) static LIVE_HANDLES: OnceLock<Mutex<BTreeMap<&'static str, i64>>> = OnceLock::new();

pub(crate) fn live_handles() -> &'static Mutex<BTreeMap<&'static str, i64>> {
    LIVE_HANDLES.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Boxes a value into an opaque JVM handle, counting it in the live-handle registry.
pub(crate) fn into_handle<T>(value: T) -> jlong {
    *live_handles().lock().unwrap().entry(std::any::type_name::<T>()).or_insert(0) += 1;
    Box::into_raw(Box::new(value)) as jlong
}

/// Reclaims a handle created by [`into_handle`], draining it from the live-handle registry. The
/// caller owns the returned box; most close paths drop it immediately.
///
/// # Safety
/// `handle` must have come from [`into_handle`] with the same `T` and not have been reclaimed yet.
pub(crate) unsafe fn from_handle<T>(handle: jlong) -> Box<T> {
    let mut live = live_handles().lock().unwrap();
    if let Some(count) = live.get_mut(std::any::type_name::<T>()) {
        *count -= 1;
        if *count == 0 {
            live.remove(std::any::type_name::<T>());
        }
    }
    Box::from_raw(handle as *mut T)
}

/// `std::any::type_name` output with every module path stripped, so the breakdown reads
/// `Box<dyn BatchSource>` rather than `alloc::boxed::Box<dyn streamfusion::BatchSource>`.
pub(crate) fn short_type_name(full: &str) -> String {
    let mut out = String::new();
    let mut ident = String::new();
    for c in full.chars() {
        if c.is_alphanumeric() || c == '_' || c == ':' {
            ident.push(c);
        } else {
            out.push_str(ident.rsplit("::").next().unwrap_or(&ident));
            ident.clear();
            out.push(c);
        }
    }
    out.push_str(ident.rsplit("::").next().unwrap_or(&ident));
    out
}

/// The native data plane runs stateful operators as asynchronous plans, so the work is driven on a
/// shared multi-threaded runtime that outlives any single call rather than spun up per batch.
pub(crate) fn runtime() -> &'static Runtime {
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

/// The live-handle breakdown, e.g. `SessionAggregator=1,MessageDecoder=2` — empty once every
/// handle has been closed. The test harness asserts it drains to empty after each job, so a
/// missing close call fails the test naming the leaking type instead of slowly growing RSS.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_liveNativeHandles<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    let live = live_handles().lock().unwrap();
    let breakdown = live
        .iter()
        .filter(|(_, count)| **count != 0)
        .map(|(name, count)| format!("{}={count}", short_type_name(name)))
        .collect::<Vec<_>>()
        .join(",");
    drop(live);
    env.new_string(breakdown)
        .expect("failed to allocate Java string for the live-handle breakdown")
        .into_raw()
}

/// Generates a per-type JNI getter for an operator's tracked native state footprint in bytes (zero
/// when unaccounted). Must be called on the task thread between batches — handles are not
/// thread-safe — so the Java side samples it per batch into an atomic its metrics thread reads.
macro_rules! state_bytes_getter {
    ($fn_name:ident, $ty:ty) => {
        #[no_mangle]
        pub extern "system" fn $fn_name<'local>(
            _env: JNIEnv<'local>,
            _class: JClass<'local>,
            handle: jlong,
        ) -> jlong {
            let operator = unsafe { &*(handle as *const $ty) };
            operator.memory.state_bytes as jlong
        }
    };
}
pub(crate) use state_bytes_getter;

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

/// Boxes a built operator into an opaque handle, or raises the memory-limit exception and returns
/// a null handle when its budget was already exceeded (a restore larger than the budget).
pub(crate) fn boxed_or_throw<T>(env: &mut JNIEnv, operator: Result<T, DataFusionError>) -> jlong {
    match operator {
        Ok(operator) => into_handle(operator),
        Err(e) => {
            throw_memory_limit(env, &e.to_string());
            0
        }
    }
}

/// Reads a JVM int[] (aggregate kinds or per-aggregate value-type codes) into a Vec.
pub(crate) fn read_int_array(env: &JNIEnv, array: &JIntArray) -> Vec<i64> {
    let length = env.get_array_length(array).expect("failed to read int array length");
    let mut buffer = vec![0i32; length as usize];
    env.get_int_array_region(array, 0, &mut buffer).expect("failed to read int array");
    buffer.into_iter().map(i64::from).collect()
}

/// Reads a JVM double[] into a Vec.
pub(crate) fn read_doubles(env: &JNIEnv, values: &JDoubleArray) -> Vec<f64> {
    let length = env.get_array_length(values).expect("failed to read doubles length");
    let mut buffer = vec![0f64; length as usize];
    env.get_double_array_region(values, 0, &mut buffer).expect("failed to read doubles");
    buffer
}

/// Reads a JVM long[] into a Vec.
pub(crate) fn read_longs(env: &JNIEnv, values: &JLongArray) -> Vec<i64> {
    let length = env.get_array_length(values).expect("failed to read longs length");
    let mut buffer = vec![0i64; length as usize];
    env.get_long_array_region(values, 0, &mut buffer).expect("failed to read longs");
    buffer
}

/// Reads a JVM String[] into a Vec, mapping a Java null element to None (a numeric comparison).
pub(crate) fn read_strings(env: &mut JNIEnv, values: &JObjectArray) -> Vec<Option<String>> {
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

/// Reads a JVM int[] of column indices into a Vec.
pub(crate) fn read_columns(env: &JNIEnv, columns: &JIntArray) -> Vec<usize> {
    read_int_array(env, columns).into_iter().map(|c| c as usize).collect()
}

/// Reads a JVM String[] into a Vec<String>.
#[cfg(any(feature = "kafka", feature = "fluss"))]
pub(crate) fn read_string_array(env: &mut JNIEnv, array: &JObjectArray) -> Vec<String> {
    let length = env.get_array_length(array).expect("failed to read string[] length");
    let mut out = Vec::with_capacity(length as usize);
    for i in 0..length {
        let element = env.get_object_array_element(array, i).expect("failed to read string[] element");
        let string: String = env.get_string(&JString::from(element)).expect("failed to read string").into();
        out.push(string);
    }
    out
}
