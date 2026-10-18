use iceberg::io::OutputFile;
use jni::objects::{JByteArray, JClass, JIntArray, JObject, JString};
use jni::sys::{jlong, jstring};
use jni::JNIEnv;
use std::sync::Arc;
use streamfusion_bridge::{
    from_handle, import_record_batch_with_schema, import_schema, into_handle, jni_guard,
};

streamfusion_bridge::link_allocator!();
mod input;
mod reader;
mod routing;
mod scan_range;
mod storage;
mod task_metadata;
mod writer;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_liveNativeHandles(
    env: JNIEnv,
    _: JClass,
) -> jstring {
    streamfusion_bridge::live_handles_probe(env)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_createWriter(
    env: JNIEnv,
    _: JClass,
    schema_address: jlong,
    schema_json: JString,
    settings_json: JString,
    location: JString,
    output: JObject,
    chunk: JByteArray,
) -> jlong {
    jni_guard(env, |env| {
        let schema: String = env
            .get_string(&schema_json)
            .expect("Iceberg schema JSON")
            .into();
        let settings: String = env
            .get_string(&settings_json)
            .expect("Iceberg settings JSON")
            .into();
        let location: String = env
            .get_string(&location)
            .expect("Iceberg file location")
            .into();
        let storage = storage::OutputStorage::new(env, output, chunk);
        let encoder = futures::executor::block_on(writer::Encoder::new(
            import_schema(schema_address),
            serde_json::from_str(&schema).expect("Iceberg schema"),
            serde_json::from_str(&settings).expect("Iceberg writer settings"),
            OutputFile::new(Arc::new(storage), location),
        ));
        into_handle(encoder)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_retainWriteBatch(
    env: JNIEnv,
    _: JClass,
    writer: jlong,
    array: jlong,
) -> jlong {
    jni_guard(env, |_| {
        let encoder = unsafe { &*(writer as *const writer::Encoder) };
        into_handle(import_record_batch_with_schema(
            array,
            &encoder.input_schema,
        ))
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_releaseWriteBatch(
    env: JNIEnv,
    _: JClass,
    batch: jlong,
) {
    jni_guard(env, |_| {
        drop(unsafe { from_handle::<arrow::array::RecordBatch>(batch) });
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_write(
    env: JNIEnv,
    _: JClass,
    handle: jlong,
    batch: jlong,
    rows: JIntArray,
    count: i32,
) -> jlong {
    jni_guard(env, |env| {
        let encoder = unsafe { &mut *(handle as *mut writer::Encoder) };
        let batch = unsafe { &*(batch as *const arrow::array::RecordBatch) };
        assert!(
            count >= 0 && count <= env.get_array_length(&rows).expect("Iceberg selection size")
        );
        let mut rows_buffer = vec![0i32; count as usize];
        env.get_int_array_region(&rows, 0, &mut rows_buffer)
            .expect("Iceberg selected rows");
        let indices = rows_buffer
            .into_iter()
            .map(|row| {
                assert!(
                    row >= 0 && (row as usize) < batch.num_rows(),
                    "Iceberg selection out of bounds"
                );
                row as u32
            })
            .collect::<Vec<_>>();
        futures::executor::block_on(encoder.write(batch, indices));
        encoder.memory_bytes() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_estimatedBytes(
    env: JNIEnv,
    _: JClass,
    handle: jlong,
) -> jlong {
    jni_guard(env, |_| unsafe {
        (&*(handle as *const writer::Encoder)).size() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_finish(
    env: JNIEnv,
    _: JClass,
    handle: jlong,
) -> jstring {
    jni_guard(env, |env| {
        let encoder = unsafe { &mut *(handle as *mut writer::Encoder) };
        let result = futures::executor::block_on(encoder.finish());
        env.new_string(serde_json::to_string(&result).expect("Iceberg statistics JSON"))
            .expect("Iceberg statistics string")
            .into_raw()
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_closeWriter(
    env: JNIEnv,
    _: JClass,
    handle: jlong,
) {
    jni_guard(env, |_| {
        drop(unsafe { from_handle::<writer::Encoder>(handle) });
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_nativeBuildVersion(
    env: JNIEnv,
    _: JClass,
) -> jstring {
    streamfusion_bridge::version_probe(env)
}

pub mod bench;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_route(
    env: JNIEnv,
    _: JClass,
    array: jlong,
    schema: jlong,
    destinations: JIntArray,
) -> jlong {
    jni_guard(env, |env| {
        let batch = streamfusion_bridge::import_record_batch(array, schema);
        let mut labels = vec![
            0i32;
            env.get_array_length(&destinations)
                .expect("Iceberg destinations") as usize
        ];
        env.get_int_array_region(&destinations, 0, &mut labels)
            .expect("Iceberg routing labels");
        into_handle(routing::Routing::new(batch, &labels))
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_nextRoute(
    env: JNIEnv,
    _: JClass,
    handle: jlong,
    array: jlong,
    schema: jlong,
) -> i32 {
    jni_guard(env, |_| {
        let route = unsafe { &mut *(handle as *mut routing::Routing) };
        match route.batches.pop_front() {
            None => -1,
            Some((destination, batch)) => {
                streamfusion_bridge::export_record_batch(batch, array, schema);
                destination
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_closeRoute(
    env: JNIEnv,
    _: JClass,
    handle: jlong,
) {
    jni_guard(env, |_| {
        drop(unsafe { from_handle::<routing::Routing>(handle) });
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_createReader(
    env: JNIEnv,
    _: JClass,
    task_json: JString,
    files: JObject,
    batch_rows: i32,
    skip: jlong,
    output_schema: jlong,
) -> jlong {
    jni_guard(env, |env| {
        assert!(
            batch_rows > 0 && skip >= 0,
            "Iceberg reader batch size/offset"
        );
        let json: String = env
            .get_string(&task_json)
            .expect("Iceberg task metadata")
            .into();
        let task: reader::ScanTask = serde_json::from_str(&json).expect("Iceberg scan task");
        let io = input::InputStorage::file_io(env, files);
        into_handle(
            reader::Reader::new(io, task, batch_rows as usize, skip as u64)
                .expect("Iceberg task reader")
                .with_output_schema(import_schema(output_schema)),
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_nextRead(
    env: JNIEnv,
    _: JClass,
    handle: jlong,
    array: jlong,
) -> jni::sys::jboolean {
    jni_guard(env, |_| {
        let decoder = unsafe { &mut *(handle as *mut reader::Reader) };
        match reader::runtime()
            .block_on(decoder.next())
            .expect("Iceberg Parquet task read")
        {
            Some(batch) => {
                streamfusion_bridge::export_record_batch_array(batch, array);
                1
            }
            None => 0,
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_iceberg_NativeIceberg_closeReader(
    env: JNIEnv,
    _: JClass,
    handle: jlong,
) {
    jni_guard(env, |_| {
        drop(unsafe { from_handle::<reader::Reader>(handle) });
    })
}
