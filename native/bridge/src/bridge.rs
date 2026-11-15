use crate::*;
use arrow::array::ArrayData;
use arrow::buffer::{BooleanBuffer, NullBuffer};

/// Takes ownership of a batch the JVM exported through the C Data Interface, swapping in released
/// placeholders so the producer's release callbacks fire once when the imported data drops.
/// The JVM handle, captured on the first JNI entry that may build a UDF-bearing expression, so a native
/// `JvmUdf` node can attach the (already JVM-owned) task thread and upcall the UDF bridge. Set once.
pub static JVM: OnceLock<jni::JavaVM> = OnceLock::new();

/// Keeps a callback's original throwable alive while Arrow release callbacks run during unwind.
pub struct JavaException(pub jni::objects::GlobalRef);

/// A host SQL exception carried through an engine error without losing its Java type.
#[derive(Debug, Clone)]
pub struct FlinkException {
    class: &'static str,
    message: String,
}

impl FlinkException {
    pub fn illegal_argument(message: String) -> Self {
        Self {
            class: "java/lang/IllegalArgumentException",
            message,
        }
    }
    pub fn table(message: String) -> Self {
        Self {
            class: "org/apache/flink/table/api/TableException",
            message,
        }
    }

    pub fn number_format(message: String) -> Self {
        Self {
            class: "java/lang/NumberFormatException",
            message,
        }
    }

    pub fn null_pointer(message: String) -> Self {
        Self {
            class: "java/lang/NullPointerException",
            message,
        }
    }

    pub fn java_runtime(message: String) -> Self {
        Self {
            class: "java/lang/RuntimeException",
            message,
        }
    }

    pub fn table_runtime(message: String) -> Self {
        Self {
            class: "org/apache/flink/table/api/TableRuntimeException",
            message,
        }
    }

    pub fn flink_runtime(message: String) -> Self {
        Self {
            class: "org/apache/flink/util/FlinkRuntimeException",
            message,
        }
    }

    pub fn class_cast(source: &str, target: &str) -> Self {
        Self {
            class: "java/lang/ClassCastException",
            message: format!(
                "class {source} cannot be cast to class {target} ({source} and {target} \
                 are in module java.base of loader 'bootstrap')"
            ),
        }
    }
}

impl std::fmt::Display for FlinkException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for FlinkException {}

/// Keeps typed SQL errors intact until the guarded JNI entry can raise their Java exception.
pub trait FlinkResultExt<T> {
    fn expect_flink(self, context: &str) -> T;
}

impl<T, E: std::error::Error + 'static> FlinkResultExt<T> for Result<T, E> {
    fn expect_flink(self, context: &str) -> T {
        self.unwrap_or_else(|error| {
            let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
            while let Some(current) = cause {
                if let Some(host) = current.downcast_ref::<FlinkException>() {
                    std::panic::resume_unwind(Box::new(host.clone()));
                }
                cause = current.source();
            }
            panic!("{context}: {error}");
        })
    }
}

pub fn capture_jvm_raw(vm: *mut jni::sys::JavaVM) {
    if JVM.get().is_none() {
        if let Ok(vm) = unsafe { jni::JavaVM::from_raw(vm) } {
            let _ = JVM.set(vm);
        }
    }
}

pub fn capture_jvm(env: &JNIEnv) {
    if JVM.get().is_none() {
        if let Ok(vm) = env.get_java_vm() {
            let _ = JVM.set(vm);
        }
    }
}

/// Raises the task-off-heap-limit exception on the calling JVM thread; the native call returns
/// immediately after, so the task fails with the budget message instead of the container OOM-killing
/// the process.
pub fn throw_memory_limit(env: &mut JNIEnv, message: &str) {
    let _ = env.throw_new("tech/streamfusion/NativeMemoryLimitException", message);
}

/// The value a guarded entrypoint returns after a panic has been converted into a pending Java
/// exception. The JVM ignores the return value once an exception is pending, but the ABI still
/// demands one, so every JNI return type names its zero here.
pub trait JniDefault {
    fn jni_default() -> Self;
}

impl JniDefault for () {
    fn jni_default() {}
}

impl JniDefault for jlong {
    fn jni_default() -> Self {
        0
    }
}

impl JniDefault for jint {
    fn jni_default() -> Self {
        0
    }
}

impl JniDefault for jni::sys::jboolean {
    fn jni_default() -> Self {
        0 // JNI_FALSE
    }
}

// Covers every JNI reference return (jobject and its jbyteArray/jobjectArray/jstring aliases).
impl<T> JniDefault for *mut T {
    fn jni_default() -> Self {
        std::ptr::null_mut()
    }
}

/// Contains a Rust panic at the JNI boundary and re-raises it as an exception on the calling task
/// thread.
///
/// A panic that unwinds out of an `extern "system"` frame aborts the process, so an unguarded
/// entrypoint turns one bad record into a dead TaskManager — every co-located task included — and
/// gives Flink's restart strategy nothing to act on. Catching here downgrades that to an ordinary
/// task failure, which is what the restart strategy is for. This is the same boundary discipline
/// Comet applies: no panic may cross a JVM native frame.
pub fn jni_guard<'local, T, F>(mut env: JNIEnv<'local>, f: F) -> T
where
    T: JniDefault,
    F: FnOnce(&mut JNIEnv<'local>) -> T,
{
    capture_jvm(&env);
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&mut env))) {
        Ok(value) => value,
        Err(payload) => {
            if let Some(JavaException(throwable)) = payload.downcast_ref::<JavaException>() {
                let _ = env.throw(<&jni::objects::JThrowable>::from(throwable.as_obj()));
                return T::jni_default();
            }
            // Don't stack a second exception on a frame that already has one pending — the first
            // is the real cause and `throw_new` would replace it.
            if !matches!(env.exception_check(), Ok(true)) {
                if let Some(host) = payload.downcast_ref::<FlinkException>() {
                    let _ = env.throw_new(host.class, &host.message);
                } else {
                    let _ = env.throw_new(
                        "tech/streamfusion/NativeException",
                        format!("native panic: {}", panic_message(payload)),
                    );
                }
            }
            T::jni_default()
        }
    }
}

/// Best-effort text of a caught panic payload; `panic!` produces one of these two shapes.
pub fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Contains native connector failures at the JNI boundary and turns them into the checked
/// `java.io.IOException` the FLIP-27 split-reader contract expects — an explicit `Err` as-is, a
/// panic prefixed with the connector's name. Same boundary discipline as `jni_guard`: no Rust
/// panic may unwind through a JVM native frame.
pub fn connector_jni<T, F>(env: &mut JNIEnv, default: T, prefix: &str, f: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> Result<T, String>,
{
    capture_jvm(env);
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(env))) {
        Ok(Ok(value)) => value,
        Ok(Err(message)) => {
            let _ = env.throw_new("java/io/IOException", message);
            default
        }
        Err(payload) => {
            let _ = env.throw_new(
                "java/io/IOException",
                format!("{prefix}: {}", panic_message(payload)),
            );
            default
        }
    }
}

/// Frames every raw keyed-state payload with its big-endian Flink key-group id and returns the
/// partitions as one JNI object array. This keeps checkpoint partitioning to one native pass and
/// lets Java stream each payload directly into Flink's keyed-state output.
pub fn keyed_state_partition_array(
    env: &mut JNIEnv,
    partitions: BTreeMap<i32, Vec<u8>>,
    operator: &str,
) -> jni::sys::jobjectArray {
    let output = env
        .new_object_array(partitions.len() as i32, "[B", jni::objects::JObject::null())
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

pub fn import_record_batch(array_address: jlong, schema_address: jlong) -> RecordBatch {
    let ffi_array = unsafe {
        std::ptr::replace(
            array_address as *mut FFI_ArrowArray,
            FFI_ArrowArray::empty(),
        )
    };
    let ffi_schema = unsafe {
        std::ptr::replace(
            schema_address as *mut FFI_ArrowSchema,
            FFI_ArrowSchema::empty(),
        )
    };
    let metadata = ffi_schema
        .metadata()
        .expect("failed to import Arrow schema metadata");
    let mut data =
        unsafe { from_ffi(ffi_array, &ffi_schema) }.expect("failed to import Arrow batch");
    data.align_buffers();
    let mut batch = RecordBatch::from(StructArray::from(data));
    *batch.schema_metadata_mut() = metadata;
    preserve_actual_nulls(batch)
}

/// Takes ownership of a batch whose schema was imported on an earlier call. Arrow's C Data array
/// carries buffers and child layout but deliberately not field names or logical metadata; the
/// cached struct type supplies that immutable information without rebuilding an `ArrowSchema` for
/// every batch.
pub fn import_record_batch_with_schema(array_address: jlong, schema: &SchemaRef) -> RecordBatch {
    let ffi_array = unsafe {
        std::ptr::replace(
            array_address as *mut FFI_ArrowArray,
            FFI_ArrowArray::empty(),
        )
    };
    let array_children = ffi_array.num_children();
    let array_buffers = ffi_array.num_buffers();
    let data_type = DataType::Struct(schema.fields().clone());
    let mut data =
        unsafe { from_ffi_and_data_type(ffi_array, data_type) }.unwrap_or_else(|error| {
            panic!(
                "failed to import Arrow batch with cached schema {schema:?} \
             (array children={array_children}, buffers={array_buffers}): {error}"
            )
        });
    data.align_buffers();
    let mut batch = RecordBatch::from(StructArray::from(data));
    *batch.schema_metadata_mut() = schema.metadata().clone();
    preserve_actual_nulls(batch)
}

// Flink source nullability is a logical hint; actual NULL slots still reach its operators.
// Widen only physical fields that contain NULLs, retaining every buffer and logical type.
fn preserve_actual_nulls(batch: RecordBatch) -> RecordBatch {
    let schema = batch.schema();
    if !schema
        .fields()
        .iter()
        .zip(batch.columns())
        .any(|(field, column)| !field.is_nullable() && column.null_count() != 0)
    {
        return batch;
    }
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| {
            field
                .as_ref()
                .clone()
                .with_nullable(field.is_nullable() || column.null_count() != 0)
        })
        .collect();
    batch
        .with_schema(Arc::new(Schema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        )))
        .expect("actual NULL fields widen the imported physical schema")
}

/// Reads only the top-level child count from a still JVM-owned C Data array. Callers use this to
/// select an already-cached physical schema before [`import_record_batch_with_schema`] takes
/// ownership of the array and replaces its C struct with a released placeholder.
pub fn record_batch_column_count(array_address: jlong) -> usize {
    unsafe { (&*(array_address as *const FFI_ArrowArray)).num_children() }
}

/// Takes ownership of a schema the JVM exported through the C Data Interface (schema only, no data),
/// swapping in a released placeholder so the producer's release callback fires once.
pub fn import_schema(schema_address: jlong) -> SchemaRef {
    let ffi_schema = unsafe {
        std::ptr::replace(
            schema_address as *mut FFI_ArrowSchema,
            FFI_ArrowSchema::empty(),
        )
    };
    Arc::new(Schema::try_from(&ffi_schema).expect("failed to import Arrow schema"))
}

/// Exports a schema (no data) into a consumer-allocated C struct; the JVM owns and releases it.
pub fn export_schema(schema: &Schema, schema_address: jlong) {
    let ffi_schema = FFI_ArrowSchema::try_from(schema).expect("failed to export Arrow schema");
    unsafe {
        std::ptr::write(schema_address as *mut FFI_ArrowSchema, ffi_schema);
    }
}

/// Exports only buffers when the consumer already owns the immutable output schema.
pub fn export_record_batch_array(batch: RecordBatch, array_address: jlong) {
    let data = StructArray::from(batch).to_data();
    let out_data = rebase_boolean_slices(&data).unwrap_or(data);
    unsafe {
        std::ptr::write(
            array_address as *mut FFI_ArrowArray,
            FFI_ArrowArray::new(&out_data),
        );
    }
}

/// Exports a batch into consumer-allocated C structs; the JVM owns and releases it after import.
/// A zero schema address reuses an immutable schema already held by the consumer.
pub fn export_record_batch(batch: RecordBatch, array_address: jlong, schema_address: jlong) {
    if schema_address == 0 {
        export_record_batch_array(batch, array_address);
        return;
    }
    let data = StructArray::from(batch).to_data();
    let out_data = rebase_boolean_slices(&data).unwrap_or(data);
    let out_array = FFI_ArrowArray::new(&out_data);
    let out_schema =
        FFI_ArrowSchema::try_from(out_data.data_type()).expect("failed to export Arrow schema");
    unsafe {
        std::ptr::write(array_address as *mut FFI_ArrowArray, out_array);
        std::ptr::write(schema_address as *mut FFI_ArrowSchema, out_schema);
    }
}

// Arrow Java's C Data importer loads Boolean buffers from bit zero, ignoring the C array offset.
// Rebase only sliced Boolean bitmaps; byte-aligned slices remain shared buffer views.
fn rebase_boolean_slices(data: &ArrayData) -> Option<ArrayData> {
    if data.data_type() == &DataType::Boolean && data.offset() != 0 {
        let array = BooleanArray::from(data.clone());
        let values = BooleanBuffer::new(array.values().sliced(), 0, array.len());
        let nulls = array.nulls().map(|nulls| {
            NullBuffer::new(BooleanBuffer::new(nulls.inner().sliced(), 0, nulls.len()))
        });
        return Some(BooleanArray::new(values, nulls).to_data());
    }

    let mut children = None;
    for (index, child) in data.child_data().iter().enumerate() {
        if let Some(rebased) = rebase_boolean_slices(child) {
            children.get_or_insert_with(|| data.child_data().to_vec())[index] = rebased;
        }
    }
    children.map(|children| {
        data.clone()
            .into_builder()
            .child_data(children)
            .build()
            .expect("failed to rebuild Java-compatible Boolean children")
    })
}

#[cfg(test)]
mod cached_schema_tests {
    use super::*;

    #[test]
    fn imported_null_slots_override_only_physical_hints_and_keep_owned_buffers() {
        let fields = vec![
            Field::new("date", DataType::Date32, false)
                .with_metadata([("origin".into(), "flink-hint".into())].into()),
            Field::new(
                "time",
                DataType::Time32(arrow::datatypes::TimeUnit::Millisecond),
                false,
            ),
            Field::new("key", DataType::Int32, false),
        ];
        let declared = Arc::new(Schema::new_with_metadata(
            fields.clone(),
            [("source".into(), "nonnull".into())].into(),
        ));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(arrow::array::Date32Array::from(vec![
                Some(-1),
                None,
                Some(1),
            ])),
            Arc::new(arrow::array::Time32MillisecondArray::from(vec![
                Some(1),
                None,
                Some(86_399_999),
            ])),
            Arc::new(Int32Array::from(vec![0, 1, 2])),
        ];
        let valid = RecordBatch::try_new(
            Arc::new(Schema::new(
                fields
                    .into_iter()
                    .map(|field| field.with_nullable(true))
                    .collect::<Vec<_>>(),
            )),
            columns,
        )
        .unwrap();
        let mut retained = Vec::new();
        for cached in [false, true] {
            let mut array = FFI_ArrowArray::empty();
            export_record_batch(valid.clone(), &mut array as *mut _ as jlong, 0);
            let mut schema = FFI_ArrowSchema::try_from(declared.as_ref()).unwrap();
            let output = if cached {
                import_record_batch_with_schema(&mut array as *mut _ as jlong, &declared)
            } else {
                import_record_batch(
                    &mut array as *mut _ as jlong,
                    &mut schema as *mut _ as jlong,
                )
            };
            assert!(output.schema().field(0).is_nullable());
            assert!(output.schema().field(1).is_nullable());
            assert!(!output.schema().field(2).is_nullable());
            assert_eq!(output.schema().metadata(), declared.metadata());
            assert_eq!(
                output.schema().field(0).metadata(),
                declared.field(0).metadata()
            );
            for column in 0..3 {
                assert_eq!(
                    output.column(column).to_data().buffers()[0].as_ptr(),
                    valid.column(column).to_data().buffers()[0].as_ptr()
                );
            }
            // The normalized batch must satisfy the checks used by shuffle, concat and IPC.
            RecordBatch::try_new(output.schema(), output.columns().to_vec()).unwrap();
            retained.push(output);
        }
        assert!(
            !declared.field(0).is_nullable(),
            "cached logical hints stay immutable"
        );
        drop(valid);
        for output in retained {
            assert_eq!(output.num_rows(), 3);
            assert_eq!(output.column(0).null_count(), 1);
            assert_eq!(output.column(1).null_count(), 1);
            assert_eq!(
                output
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Date32Array>()
                    .unwrap()
                    .value(0),
                -1
            );
        }
    }

    #[test]
    fn imports_without_actual_nulls_retain_the_declared_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![7, 9]))])
                .unwrap();
        let mut array = FFI_ArrowArray::empty();
        export_record_batch(batch, &mut array as *mut _ as jlong, 0);
        let output = import_record_batch_with_schema(&mut array as *mut _ as jlong, &schema);
        assert_eq!(output.schema(), schema);
    }

    #[test]
    fn boolean_exports_rebase_bits_and_preserve_other_buffers() {
        let mut retained = Vec::new();
        for offset in [0, 1, 7, 8, 9, 63, 64, 65, 127] {
            for length in [0, 1, 17] {
                for nullable in [false, true] {
                    let values = BooleanBuffer::collect_bool(offset + length, |i| i % 3 == 0)
                        .slice(offset, length);
                    // Validity can have a different bit offset from the values.
                    let nulls = nullable.then(|| {
                        NullBuffer::new(
                            BooleanBuffer::collect_bool(offset + length + 5, |i| i % 7 != 2)
                                .slice(offset + 5, length),
                        )
                    });
                    let flags: ArrayRef = Arc::new(BooleanArray::new(values, nulls));
                    let nested: ArrayRef = Arc::new(StructArray::from(vec![(
                        Arc::new(
                            Field::new("bit", DataType::Boolean, true)
                                .with_metadata([("source".into(), "boolean-slice".into())].into()),
                        ),
                        flags.clone(),
                    )]));
                    let integers: ArrayRef = Arc::new(
                        Int64Array::from_iter_values(0..(offset + length) as i64)
                            .slice(offset, length),
                    );
                    let input = RecordBatch::try_from_iter(vec![
                        ("flag", flags),
                        ("nested", nested),
                        ("id", integers),
                    ])
                    .unwrap();
                    for cached in [false, true] {
                        let mut array = FFI_ArrowArray::empty();
                        let mut schema = FFI_ArrowSchema::empty();
                        export_record_batch(
                            input.clone(),
                            &mut array as *mut _ as jlong,
                            if cached {
                                0
                            } else {
                                &mut schema as *mut _ as jlong
                            },
                        );
                        assert_eq!(array.child(0).offset(), 0);
                        assert_eq!(array.child(1).child(0).offset(), 0);
                        if length > 0 {
                            assert_eq!(
                                array.child(2).buffer(1),
                                input.column(2).to_data().buffers()[0].as_ptr(),
                            );
                            if offset % 8 == 0 {
                                assert_eq!(
                                    array.child(0).buffer(1),
                                    input.column(0).to_data().buffers()[0]
                                        .as_ptr()
                                        .wrapping_add(offset / 8),
                                );
                            }
                        }
                        let output = if cached {
                            import_record_batch_with_schema(
                                &mut array as *mut _ as jlong,
                                &input.schema(),
                            )
                        } else {
                            import_record_batch(
                                &mut array as *mut _ as jlong,
                                &mut schema as *mut _ as jlong,
                            )
                        };
                        assert_eq!(output, input);
                        retained.push((output, input.clone()));
                    }
                }
            }
        }
        for (output, expected) in retained {
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn array_only_exports_preserve_slices_nulls_metadata_and_retained_buffers() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(-11)]));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )
        .with_metadata(std::collections::HashMap::from([(
            "source".to_string(),
            "retained".to_string(),
        )]))]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        let mut retained = Vec::new();
        for (offset, length) in [(0, 0), (1, 2), (0, 3), (3, 0)] {
            let input = batch.slice(offset, length);
            let mut array = FFI_ArrowArray::empty();
            export_record_batch(input.clone(), &mut array as *mut _ as jlong, 0);
            let output = import_record_batch_with_schema(&mut array as *mut _ as jlong, &schema);
            assert_eq!(output, input);
            if length > 0 {
                assert_eq!(
                    output.column(0).to_data().buffers()[0].as_ptr(),
                    input.column(0).to_data().buffers()[0].as_ptr()
                );
            }
            retained.push(output);
        }
        drop(batch);
        assert_eq!(
            retained[1]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![None, Some(-11)])
        );
        assert_eq!(retained[2].schema(), schema);
    }
}

/// Live native handles by type — the leak sentinel the test harness polls. Every handle the JVM
/// creates increments its type's count and every close decrements it, so a non-empty breakdown
/// after all operators have closed pinpoints a missing close call: memory the JVM's own tooling
/// cannot see. (Comet only logs a warning on this condition; tests can afford to fail hard.)
pub static LIVE_HANDLES: OnceLock<Mutex<BTreeMap<&'static str, i64>>> = OnceLock::new();

pub fn live_handles() -> &'static Mutex<BTreeMap<&'static str, i64>> {
    LIVE_HANDLES.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Boxes a value into an opaque JVM handle, counting it in the live-handle registry.
pub fn into_handle<T>(value: T) -> jlong {
    *live_handles()
        .lock()
        .unwrap()
        .entry(std::any::type_name::<T>())
        .or_insert(0) += 1;
    Box::into_raw(Box::new(value)) as jlong
}

/// Reclaims a handle created by [`into_handle`], draining it from the live-handle registry. The
/// caller owns the returned box; most close paths drop it immediately.
///
/// # Safety
/// `handle` must have come from [`into_handle`] with the same `T` and not have been reclaimed yet.
pub unsafe fn from_handle<T>(handle: jlong) -> Box<T> {
    let mut live = live_handles().lock().unwrap();
    if let Some(count) = live.get_mut(std::any::type_name::<T>()) {
        *count -= 1;
        if *count == 0 {
            live.remove(std::any::type_name::<T>());
        }
    }
    Box::from_raw(handle as *mut T)
}

/// `std::any::type_name` output with every module path stripped, so handle diagnostics stay compact.
pub fn short_type_name(full: &str) -> String {
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

/// Reports handles owned by the calling library's statically linked bridge.
pub fn live_handles_probe(env: JNIEnv) -> jstring {
    jni_guard(env, |env| {
        let live = live_handles().lock().unwrap();
        let breakdown = live
            .iter()
            .filter(|(_, count)| **count != 0)
            .map(|(name, count)| format!("{}={count}", short_type_name(name)))
            .collect::<Vec<_>>()
            .join(",");
        drop(live);
        env.new_string(breakdown)
            .expect("failed to allocate the live-handle breakdown")
            .into_raw()
    })
}

/// The StreamFusion deployment version this library was built as. Cargo and Maven use the same
/// release version; reporting Cargo's package version avoids a second native-only version stamp.
pub fn build_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// The build version as a Java string: the core class's probe below and every extension class's
/// own probe, since a native method binds to whichever loaded library exports its symbol and only a
/// probe named after the owning class is guaranteed to read that class's library.
pub fn version_probe<'local>(env: JNIEnv<'local>) -> jstring {
    jni_guard(env, move |env| {
        env.new_string(build_version())
            .expect("failed to allocate Java string for version")
            .into_raw()
    })
}

/// Reads a JVM int[] into a Vec.
pub fn read_i32_array(env: &JNIEnv, array: &JIntArray) -> Vec<i32> {
    let length = env
        .get_array_length(array)
        .expect("failed to read int array length");
    let mut buffer = vec![0i32; length as usize];
    env.get_int_array_region(array, 0, &mut buffer)
        .expect("failed to read int array");
    buffer
}

/// Reads a JVM int[] (aggregate kinds or per-aggregate value-type codes) widened into a Vec<i64>.
pub fn read_int_array(env: &JNIEnv, array: &JIntArray) -> Vec<i64> {
    read_i32_array(env, array)
        .into_iter()
        .map(i64::from)
        .collect()
}

/// Reads a JVM double[] into a Vec.
pub fn read_doubles(env: &JNIEnv, values: &JDoubleArray) -> Vec<f64> {
    let length = env
        .get_array_length(values)
        .expect("failed to read doubles length");
    let mut buffer = vec![0f64; length as usize];
    env.get_double_array_region(values, 0, &mut buffer)
        .expect("failed to read doubles");
    buffer
}

/// Reads a JVM float[] into a Vec.
#[allow(dead_code)]
pub fn read_floats(env: &JNIEnv, values: &JFloatArray) -> Vec<f32> {
    let length = env
        .get_array_length(values)
        .expect("failed to read floats length");
    let mut buffer = vec![0f32; length as usize];
    env.get_float_array_region(values, 0, &mut buffer)
        .expect("failed to read floats");
    buffer
}

/// Reads a JVM long[] into a Vec.
pub fn read_longs(env: &JNIEnv, values: &JLongArray) -> Vec<i64> {
    let length = env
        .get_array_length(values)
        .expect("failed to read longs length");
    let mut buffer = vec![0i64; length as usize];
    env.get_long_array_region(values, 0, &mut buffer)
        .expect("failed to read longs");
    buffer
}

/// Reads a JVM String[] into a Vec, mapping a Java null element to None (a numeric comparison).
pub fn read_strings(env: &mut JNIEnv, values: &JObjectArray) -> Vec<Option<String>> {
    let length = env
        .get_array_length(values)
        .expect("failed to read strings length");
    let mut out = Vec::with_capacity(length as usize);
    for index in 0..length {
        let element = env
            .get_object_array_element(values, index)
            .expect("failed to read string");
        if element.is_null() {
            out.push(None);
        } else {
            let text: String = env
                .get_string(&JString::from(element))
                .expect("failed to decode string")
                .into();
            out.push(Some(text));
        }
    }
    out
}

/// Reads a JVM int[] of column indices into a Vec.
pub fn read_columns(env: &JNIEnv, columns: &JIntArray) -> Vec<usize> {
    read_i32_array(env, columns)
        .into_iter()
        .map(|c| c as usize)
        .collect()
}

/// Reads a JVM String[] into a Vec<String>.
pub fn read_string_array(env: &mut JNIEnv, array: &JObjectArray) -> Vec<String> {
    let length = env
        .get_array_length(array)
        .expect("failed to read string[] length");
    let mut out = Vec::with_capacity(length as usize);
    for i in 0..length {
        let element = env
            .get_object_array_element(array, i)
            .expect("failed to read string[] element");
        let string: String = env
            .get_string(&JString::from(element))
            .expect("failed to read string")
            .into();
        out.push(string);
    }
    out
}
