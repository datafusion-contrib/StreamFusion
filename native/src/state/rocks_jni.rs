//! JNI lifecycle for Rust-owned RocksDB state. Java controls Flink lifecycle and file upload;
//! every state read, write, flush, and compaction stays inside Rust/RocksDB.

use crate::*;
use jni::objects::{JByteArray, JClass, JIntArray, JObject, JObjectArray, JString};
use jni::sys::{jboolean, jint, jlong, jobjectArray};
use jni::JNIEnv;

type RocksGroupAggregator = GroupAggregator<RocksGroupStore>;

fn read_string(env: &mut JNIEnv, value: &JString) -> String {
    env.get_string(value).expect("jni string").into()
}

fn read_strings(env: &mut JNIEnv, values: &JObjectArray) -> Vec<Option<String>> {
    (0..env.get_array_length(values).expect("array length"))
        .map(|i| {
            let value = env
                .get_object_array_element(values, i)
                .expect("array element");
            if value.is_null() {
                None
            } else {
                Some(read_string(env, &JString::from(value)))
            }
        })
        .collect()
}

fn manifest_array<'local>(
    env: &mut JNIEnv<'local>,
    manifest: &RocksCheckpointManifest,
) -> jobjectArray {
    let mut lines = Vec::with_capacity(1 + manifest.data_files.len() + manifest.meta_files.len());
    lines.push(if manifest.snapshot_id < 0 {
        String::new()
    } else {
        manifest.snapshot_id.to_string()
    });
    lines.extend(manifest.data_files.iter().map(|f| format!("d:{f}")));
    lines.extend(manifest.meta_files.iter().map(|f| format!("m:{f}")));
    let array = env
        .new_object_array(lines.len() as i32, "java/lang/String", JObject::null())
        .expect("manifest array");
    for (i, line) in lines.iter().enumerate() {
        let value = env.new_string(line).expect("manifest line");
        env.set_object_array_element(&array, i as i32, value)
            .expect("manifest element");
    }
    array.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbStateAvailable<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jboolean {
    crate::bridge::jni_guard(env, move |_env| 1)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbGroupAggregatorSupported<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    kinds: JIntArray<'local>,
    values: JIntArray<'local>,
) -> jboolean {
    crate::bridge::jni_guard(env, move |env| {
        let kinds = read_int_array(&env, &kinds);
        let values: Vec<_> = read_int_array(&env, &values)
            .into_iter()
            .map(value_data_type)
            .collect();
        rocks_group_supported(&kinds, &group_state_types(&kinds, &values)) as jboolean
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBGroupAggregator<'local>(
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
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    options_json: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_type_codes = read_int_array(&env, &value_types);
        let values: Vec<_> = value_type_codes
            .iter()
            .map(|&code| value_data_type(code))
            .collect();
        let codec = GroupStateCodec {
            kinds: kinds.clone(),
            value_types: values.clone(),
            state_types: group_state_types(&kinds, &values),
        };
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: state_ttl_millis.max(0),
        };
        let source_dirs: Vec<_> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let source_tokens: Vec<_> = read_strings(&mut env, &source_snapshot_tokens)
            .into_iter()
            .flatten()
            .map(|token| token.parse::<i64>().expect("RocksDB checkpoint generation"))
            .collect();
        let store = if source_dirs.is_empty() {
            RocksGroupStore::create(config, codec)
        } else {
            RocksGroupStore::open_merged(
                config,
                codec,
                &source_dirs
                    .into_iter()
                    .zip(source_tokens)
                    .collect::<Vec<_>>(),
                key_group_start..=key_group_end,
                aligned != 0,
                now_millis,
            )
        };
        let aggregator = store.and_then(|store| {
            let mut base = GroupAggregator::new(
                kinds,
                value_type_codes,
                read_int_array(&env, &value_columns),
                read_columns(&env, &key_columns),
                generate_update_before != 0,
            )
            .with_key_timestamp_precisions(read_i32_array(&env, &key_timestamp_precisions))
            .with_filter_columns(read_int_array(&env, &filter_columns))
            .with_count_columns(read_int_array(&env, &count_columns))
            .with_record_count_column(record_count_column as i64)
            .with_distinct_view_columns(read_int_array(&env, &distinct_view_columns))
            .with_state_ttl(state_ttl_millis);
            if mini_batch != 0 {
                base = base.with_mini_batch();
            }
            base.with_backend(store)
                .with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, aggregator)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_updateRocksDBGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
    now: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut RocksGroupAggregator) };
        aggregator.store_mut().set_clock(now);
        let batch = import_record_batch(in_array, in_schema);
        match aggregator.update(&batch, now) {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        match unsafe { &mut *(handle as *mut RocksGroupAggregator) }.flush_mini_batch() {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        match unsafe { &mut *(handle as *mut RocksGroupAggregator) }
            .store_mut()
            .checkpoint(&snapshot_directory)
        {
            Ok(m) => manifest_array(&mut env, &m),
            Err(e) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("RocksDB checkpoint failed: {e}"),
                );
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbGroupAggregatorStateBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| unsafe { &*(handle as *const RocksGroupAggregator) }.memory.state_bytes as jlong)
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbGroupAggregatorStagingBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksGroupAggregator) }.staging_bytes() as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbGroupAggregatorStagedKeys<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksGroupAggregator) }.staged_keys() as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBGroupAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<RocksGroupAggregator>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBSnapshotStore<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    table_directory: JString<'local>,
    max_parallelism: jint,
    options_json: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: 0,
        };
        let source_dirs: Vec<_> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let source_tokens: Vec<_> = read_strings(&mut env, &source_snapshot_tokens)
            .into_iter()
            .flatten()
            .map(|token| token.parse::<i64>().expect("RocksDB checkpoint generation"))
            .collect();
        let sources: Vec<_> = source_dirs.into_iter().zip(source_tokens).collect();
        boxed_or_throw(
            &mut env,
            RocksSnapshotStore::open_merged(
                config,
                &sources,
                key_group_start..=key_group_end,
                aligned != 0,
            ),
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_restoreRocksDBSnapshotStorePartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |env| {
        let store = unsafe { &*(handle as *const RocksSnapshotStore) };
        match store.partitions() {
            Ok(partitions) => {
                let output = env
                    .new_object_array(partitions.len() as i32, "[B", JObject::null())
                    .expect("snapshot partition array");
                for (index, partition) in partitions.iter().enumerate() {
                    let bytes = env
                        .byte_array_from_slice(partition)
                        .expect("snapshot partition");
                    env.set_object_array_element(&output, index as i32, bytes)
                        .expect("snapshot partition element");
                }
                output.into_raw()
            }
            Err(error) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("RocksDB restore failed: {error}"),
                );
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbSnapshotStoreTimerDeadline<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksSnapshotStore) }.timer_deadline() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBSnapshotStore<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    snapshots: JObjectArray<'local>,
    timer_deadline: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        let count = env.get_array_length(&snapshots).expect("snapshot count");
        let mut partitions = Vec::with_capacity(count as usize);
        for index in 0..count {
            let object = env
                .get_object_array_element(&snapshots, index)
                .expect("snapshot element");
            partitions.push(
                env.convert_byte_array(&JByteArray::from(object))
                    .expect("snapshot bytes"),
            );
        }
        match unsafe { &mut *(handle as *mut RocksSnapshotStore) }.checkpoint(
            &partitions,
            timer_deadline,
            &snapshot_directory,
        ) {
            Ok(manifest) => manifest_array(&mut env, &manifest),
            Err(error) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("RocksDB checkpoint failed: {error}"),
                );
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBSnapshotStore<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<RocksSnapshotStore>(handle));
    })
}
