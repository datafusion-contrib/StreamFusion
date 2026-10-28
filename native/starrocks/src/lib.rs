//! Native Stream Load row encoding for the released StarRocks Flink connector.
//!
//! The connector keeps HTTP transport, buffering, labels, transactions and recovery; this library
//! only turns a sink's Arrow batch into the CSV or JSON rows its serializers would have produced.

use arrow::array::RecordBatch;
use jni::objects::{JByteArray, JClass, JIntArray, JObjectArray};
use jni::sys::{jboolean, jbyteArray, jlong, jstring};
use jni::JNIEnv;
use streamfusion_bridge::{
    connector_jni, from_handle, import_record_batch, into_handle, jni_guard, live_handles_probe,
    read_i32_array, read_string_array, version_probe,
};

streamfusion_bridge::link_allocator!();

mod encoder;
mod fastjson;
mod java_text;

pub use encoder::{Encoder, Format};

/// Rows cross JNI in slabs of about this many bytes, so the JVM never holds a whole batch's rows.
pub const SLAB_BYTES: usize = 1 << 20;

/// A task-local encoder and the batch whose rows it is handing to the JVM slab by slab.
struct TaskEncoder {
    encoder: Encoder,
    batch: Option<RecordBatch>,
    next_row: usize,
    output: Vec<u8>,
    ends: Vec<usize>,
    offsets: Vec<i32>,
}

fn starrocks_jni<T, F>(env: &mut JNIEnv, default: T, f: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> Result<T, String>,
{
    connector_jni(env, default, "native StarRocks encoding panic", f)
}

/// The extension library's StreamFusion build stamp for the loader compatibility check.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_starrocks_NativeStarRocks_nativeBuildVersion<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    version_probe(env)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_starrocks_NativeStarRocks_liveNativeHandles<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    live_handles_probe(env)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_starrocks_NativeStarRocks_isLoaded<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jboolean {
    jni_guard(env, move |_env| true as jboolean)
}

/// Builds a task-local encoder from the planner's resolved connector options.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_tech_streamfusion_starrocks_NativeStarRocks_createEncoder<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    json: jboolean,
    column_separator: JByteArray<'local>,
    logical_types: JObjectArray<'local>,
    field_names: JObjectArray<'local>,
    json_order: JIntArray<'local>,
    upsert_delete: jboolean,
    ignore_update_before: jboolean,
) -> jlong {
    starrocks_jni(&mut env, 0, |env| {
        let format = if json != 0 {
            Format::Json
        } else {
            Format::Csv {
                column_separator: env
                    .convert_byte_array(&column_separator)
                    .map_err(|error| format!("failed to read the CSV column separator: {error}"))?,
            }
        };
        let logical_types = read_string_array(env, &logical_types);
        let field_names = read_string_array(env, &field_names);
        let json_order = read_i32_array(env, &json_order);
        let encoder = Encoder::new(
            format,
            &logical_types,
            &field_names,
            &json_order,
            upsert_delete != 0,
            ignore_update_before != 0,
        )?;
        Ok(into_handle(TaskEncoder {
            encoder,
            batch: None,
            next_row: 0,
            output: Vec::new(),
            ends: Vec::new(),
            offsets: Vec::new(),
        }))
    })
}

/// Imports one Arrow batch whose rows the following `next` calls hand over.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_starrocks_NativeStarRocks_begin<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    array_address: jlong,
    schema_address: jlong,
) {
    starrocks_jni(&mut env, (), |_env| {
        let task = unsafe { &mut *(handle as *mut TaskEncoder) };
        let batch = import_record_batch(array_address, schema_address);
        task.batch = None;
        task.encoder.check(&batch)?;
        task.batch = Some(batch);
        task.next_row = 0;
        Ok(())
    })
}

/// Encodes the current batch's next slab: rows back to back, with each row's end offset in `ends`
/// and the row count in `rows[0]`. Returns null once every row has been handed over.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_starrocks_NativeStarRocks_next<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    ends: JIntArray<'local>,
    rows: JIntArray<'local>,
) -> jbyteArray {
    starrocks_jni(&mut env, std::ptr::null_mut(), |env| {
        let task = unsafe { &mut *(handle as *mut TaskEncoder) };
        let Some(batch) = task.batch.as_ref() else {
            return Ok(std::ptr::null_mut());
        };
        let capacity = env
            .get_array_length(&ends)
            .map_err(|error| format!("failed to size the StarRocks row offsets: {error}"))?;
        if capacity < 1 {
            return Err("the StarRocks row offsets array is empty".to_string());
        }
        task.output.clear();
        task.ends.clear();
        task.next_row = task.encoder.encode(
            batch,
            task.next_row,
            SLAB_BYTES,
            capacity as usize,
            &mut task.output,
            &mut task.ends,
        )?;
        if task.next_row == batch.num_rows() {
            task.batch = None;
        }
        if task.ends.is_empty() {
            return Ok(std::ptr::null_mut());
        }
        task.offsets.clear();
        for &end in &task.ends {
            task.offsets.push(i32::try_from(end).map_err(|_| {
                "a StarRocks row exceeds the 2 GiB a Java array can hold".to_string()
            })?);
        }
        env.set_int_array_region(&ends, 0, &task.offsets)
            .map_err(|error| format!("failed to report the StarRocks row offsets: {error}"))?;
        env.set_int_array_region(&rows, 0, &[task.offsets.len() as i32])
            .map_err(|error| format!("failed to report the StarRocks row count: {error}"))?;
        Ok(env
            .byte_array_from_slice(&task.output)
            .map_err(|error| format!("failed to materialize the StarRocks rows: {error}"))?
            .into_raw())
    })
}

/// Releases the current batch, also when the JVM stopped consuming its rows part-way.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_starrocks_NativeStarRocks_reset<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(env, move |_env| {
        let task = unsafe { &mut *(handle as *mut TaskEncoder) };
        task.batch = None;
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_starrocks_NativeStarRocks_closeEncoder<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(env, move |_env| unsafe {
        drop(from_handle::<TaskEncoder>(handle));
    })
}
