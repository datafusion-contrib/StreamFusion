//! JNI lifecycle for Rust-owned RocksDB state. Java controls Flink lifecycle and file upload;
//! every state read, write, flush, and compaction stays inside Rust/RocksDB.

use crate::*;
use jni::objects::{
    JByteArray, JClass, JDoubleArray, JIntArray, JLongArray, JObject, JObjectArray, JString,
};
use jni::sys::{jboolean, jdouble, jint, jlong, jobjectArray};
use jni::JNIEnv;

type RocksGroupAggregator = GroupAggregator<RocksGroupStore>;
type RocksChangelogNormalizer = ChangelogNormalizer<RocksNormalizerStore>;
type RocksKeepLastDeduplicator = KeepLastDeduplicator<RocksDedupStore>;
type RocksUpdatingJoiner = UpdatingJoiner<RocksJoinStore>;

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

/// Opens an operator's typed store: fresh when no restored sources exist, otherwise merged from
/// the restored checkpoint directories with this subtask's key-group range.
#[allow(clippy::too_many_arguments)]
fn open_store<C: RocksStateCodec>(
    env: &mut JNIEnv,
    config: RocksStoreConfig,
    codec: C,
    source_directories: &JObjectArray,
    source_snapshot_tokens: &JObjectArray,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
    now_millis: jlong,
) -> Result<RocksStore<C>, DataFusionError> {
    let source_dirs: Vec<_> = read_strings(env, source_directories)
        .into_iter()
        .flatten()
        .collect();
    let source_tokens: Vec<_> = read_strings(env, source_snapshot_tokens)
        .into_iter()
        .flatten()
        .map(|token| token.parse::<i64>().expect("RocksDB checkpoint generation"))
        .collect();
    if source_dirs.is_empty() {
        RocksStore::create(config, codec)
    } else {
        RocksStore::open_merged(
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
    }
}

/// [`open_store`] for two side stores sharing one DB (the updating join's left and right states):
/// fresh when no restored sources exist, otherwise merged once for both tables.
#[allow(clippy::too_many_arguments)]
fn open_store_pair<C: RocksStateCodec>(
    env: &mut JNIEnv,
    config: RocksStoreConfig,
    second_ttl_ms: i64,
    codecs: (C, C),
    source_directories: &JObjectArray,
    source_snapshot_tokens: &JObjectArray,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
    now_millis: jlong,
) -> Result<(RocksStore<C>, RocksStore<C>), DataFusionError> {
    let source_dirs: Vec<_> = read_strings(env, source_directories)
        .into_iter()
        .flatten()
        .collect();
    let source_tokens: Vec<_> = read_strings(env, source_snapshot_tokens)
        .into_iter()
        .flatten()
        .map(|token| token.parse::<i64>().expect("RocksDB checkpoint generation"))
        .collect();
    if source_dirs.is_empty() {
        RocksStore::create_pair(config, second_ttl_ms, codecs)
    } else {
        RocksStore::open_merged_pair(
            config,
            second_ttl_ms,
            codecs,
            &source_dirs
                .into_iter()
                .zip(source_tokens)
                .collect::<Vec<_>>(),
            key_group_start..=key_group_end,
            aligned != 0,
            now_millis,
        )
    }
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
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBSharedResources<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    total_bytes: jlong,
    write_buffer_ratio: jdouble,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        Box::into_raw(Box::new(
            crate::state::rocks_config::RocksSharedResources::new(total_bytes, write_buffer_ratio),
        )) as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_releaseRocksDBSharedResources<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<
            crate::state::rocks_config::RocksSharedResources,
        >(handle));
    })
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
    shared_resources: jlong,
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
            shared_resources,
        };
        let store = open_store(
            &mut env,
            config,
            codec,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            now_millis,
        );
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
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBGroupAggregatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut RocksGroupAggregator) };
        match aggregator.canonical_partitions() {
            Ok(partitions) => {
                keyed_state_partition_array(&mut env, partitions, "rocksdb-group-aggregate")
            }
            Err(error) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("RocksDB canonical snapshot failed: {error}"),
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
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBChangelogNormalizer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    key_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    generate_update_before: jboolean,
    mini_batch: jboolean,
    state_ttl_millis: jlong,
    now_millis: jlong,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    options_json: JString<'local>,
    shared_resources: jlong,
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
            ttl_ms: state_ttl_millis.max(0),
            shared_resources,
        };
        let store = open_store(
            &mut env,
            config,
            NormalizerStateCodec,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            now_millis,
        );
        let normalizer = store.and_then(|store| {
            ChangelogNormalizer::new(
                read_columns(&env, &key_columns),
                generate_update_before != 0,
            )
            .with_mini_batch(mini_batch != 0)
            .with_key_timestamp_precisions(read_i32_array(&env, &key_timestamp_precisions))
            .with_state_ttl(state_ttl_millis)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, normalizer)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRocksDBChangelogNormalizer<'local>(
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
        let normalizer = unsafe { &mut *(handle as *mut RocksChangelogNormalizer) };
        normalizer.store_mut().set_clock(now);
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            normalizer.push(&batch, now)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBChangelogNormalizer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        match unsafe { &mut *(handle as *mut RocksChangelogNormalizer) }.flush_mini_batch() {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBChangelogNormalizer<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        match unsafe { &mut *(handle as *mut RocksChangelogNormalizer) }
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
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBChangelogNormalizerPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let normalizer = unsafe { &mut *(handle as *mut RocksChangelogNormalizer) };
        match normalizer.canonical_partitions() {
            Ok(partitions) => {
                keyed_state_partition_array(&mut env, partitions, "rocksdb-changelog-normalizer")
            }
            Err(error) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("RocksDB canonical snapshot failed: {error}"),
                );
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbChangelogNormalizerStateBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksChangelogNormalizer) }
            .memory
            .state_bytes as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbChangelogNormalizerStagingBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksChangelogNormalizer) }.staging_bytes() as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbChangelogNormalizerStagedKeys<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksChangelogNormalizer) }.staged_keys() as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBChangelogNormalizer<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<RocksChangelogNormalizer>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBKeepLastDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
    generate_update_before: jboolean,
    generate_insert: jboolean,
    rowtime_ordered: jboolean,
    keep_first: jboolean,
    mini_batch: jboolean,
    compact_changes: jboolean,
    state_ttl_millis: jlong,
    now_millis: jlong,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    options_json: JString<'local>,
    shared_resources: jlong,
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
            ttl_ms: state_ttl_millis.max(0),
            shared_resources,
        };
        let store = open_store(
            &mut env,
            config,
            DedupStateCodec,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            now_millis,
        );
        let dedup = store.and_then(|store| {
            KeepLastDeduplicator::new(
                read_columns(&env, &partition_columns),
                rt_column as usize,
                generate_update_before != 0,
                rowtime_ordered != 0,
                keep_first != 0,
            )
            .with_generate_insert(generate_insert != 0)
            .with_mini_batch(mini_batch != 0)
            .with_compact_changes(compact_changes != 0)
            .with_key_timestamp_precisions(read_i32_array(&env, &key_timestamp_precisions))
            .with_state_ttl(state_ttl_millis)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, dedup)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRocksDBKeepLastDeduplicator<'local>(
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
        let dedup = unsafe { &mut *(handle as *mut RocksKeepLastDeduplicator) };
        dedup.store_mut().set_clock(now);
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            dedup.push(&batch, now)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBKeepLastDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        match unsafe { &mut *(handle as *mut RocksKeepLastDeduplicator) }.flush_mini_batch() {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBKeepLastDeduplicator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        match unsafe { &mut *(handle as *mut RocksKeepLastDeduplicator) }
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
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBKeepLastDeduplicatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut RocksKeepLastDeduplicator) };
        match dedup.canonical_partitions() {
            Ok(partitions) => {
                keyed_state_partition_array(&mut env, partitions, "rocksdb-keep-last-dedup")
            }
            Err(error) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("RocksDB canonical snapshot failed: {error}"),
                );
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbKeepLastDeduplicatorStateBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksKeepLastDeduplicator) }
            .memory
            .state_bytes as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbKeepLastDeduplicatorStagingBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksKeepLastDeduplicator) }.staging_bytes() as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbKeepLastDeduplicatorStagedKeys<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksKeepLastDeduplicator) }.staged_keys() as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBKeepLastDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<RocksKeepLastDeduplicator>(handle));
    })
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBUpdatingJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
    left_join_key_unique: jboolean,
    right_join_key_unique: jboolean,
    mini_batch: jboolean,
    left_state_ttl_millis: jlong,
    right_state_ttl_millis: jlong,
    now_millis: jlong,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    options_json: JString<'local>,
    shared_resources: jlong,
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
            ttl_ms: left_state_ttl_millis.max(0),
            shared_resources,
        };
        let stores = open_store_pair(
            &mut env,
            config,
            right_state_ttl_millis.max(0),
            (JoinStateCodec, JoinStateCodec),
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            now_millis,
        );
        let joiner = stores.and_then(|(left_store, right_store)| {
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
            UpdatingJoiner::new(
                left,
                right,
                JoinKind::from_code(join_type),
                left_schema,
                right_schema,
                predicate,
            )
            .with_key_timestamp_precisions(read_i32_array(&env, &key_timestamp_precisions))
            .with_unique_join_keys(left_join_key_unique != 0, right_join_key_unique != 0)
            .with_mini_batch(mini_batch != 0)
            .with_state_ttl(left_state_ttl_millis, right_state_ttl_millis)
            .with_backend(left_store, right_store)
            .with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, joiner)
    })
}

fn push_rocksdb_updating_joiner(
    env: JNIEnv,
    handle: jlong,
    is_left: bool,
    in_array: jlong,
    in_schema: jlong,
    now: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut RocksUpdatingJoiner) };
        let (left_store, right_store) = joiner.stores_mut();
        left_store.set_clock(now);
        right_store.set_clock(now);
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            joiner.push(&batch, is_left, now)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushLeftRocksDBUpdatingJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
    now: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    push_rocksdb_updating_joiner(
        env, handle, true, in_array, in_schema, now, out_array, out_schema,
    )
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRightRocksDBUpdatingJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
    now: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    push_rocksdb_updating_joiner(
        env, handle, false, in_array, in_schema, now, out_array, out_schema,
    )
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBUpdatingJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        match unsafe { &mut *(handle as *mut RocksUpdatingJoiner) }.flush_mini_batch() {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBUpdatingJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        let joiner = unsafe { &mut *(handle as *mut RocksUpdatingJoiner) };
        let (left_store, right_store) = joiner.stores_mut();
        match RocksStore::checkpoint_pair(left_store, right_store, &snapshot_directory) {
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
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBUpdatingJoinerPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut RocksUpdatingJoiner) };
        match joiner.canonical_partitions() {
            Ok(partitions) => {
                keyed_state_partition_array(&mut env, partitions, "rocksdb-updating-join")
            }
            Err(error) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("RocksDB canonical snapshot failed: {error}"),
                );
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbUpdatingJoinerStateBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksUpdatingJoiner) }
            .memory
            .state_bytes as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbUpdatingJoinerStagingBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksUpdatingJoiner) }.staging_bytes() as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbUpdatingJoinerStagedKeys<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksUpdatingJoiner) }.staged_keys() as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbUpdatingJoinerStagedRecords<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    left: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksUpdatingJoiner) }.staged_records(left != 0) as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBUpdatingJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<RocksUpdatingJoiner>(handle));
    })
}

/// [`open_store_pair`] for the window join's bespoke row buffer: fresh when no restored sources
/// exist, otherwise merged once for both side tables.
#[allow(clippy::too_many_arguments)]
fn open_window_buffer(
    env: &mut JNIEnv,
    config: RocksStoreConfig,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    source_directories: &JObjectArray,
    source_snapshot_tokens: &JObjectArray,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> Result<RocksWindowBuffer, DataFusionError> {
    let source_dirs: Vec<_> = read_strings(env, source_directories)
        .into_iter()
        .flatten()
        .collect();
    let source_tokens: Vec<_> = read_strings(env, source_snapshot_tokens)
        .into_iter()
        .flatten()
        .map(|token| token.parse::<i64>().expect("RocksDB checkpoint generation"))
        .collect();
    if source_dirs.is_empty() {
        RocksWindowBuffer::create(config, left_schema, right_schema)
    } else {
        RocksWindowBuffer::open_merged(
            config,
            left_schema,
            right_schema,
            &source_dirs
                .into_iter()
                .zip(source_tokens)
                .collect::<Vec<_>>(),
            key_group_start..=key_group_end,
            aligned != 0,
        )
    }
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
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
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    options_json: JString<'local>,
    shared_resources: jlong,
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
            shared_resources,
        };
        let left_schema = import_schema(left_schema_address);
        let right_schema = import_schema(right_schema_address);
        let store = open_window_buffer(
            &mut env,
            config,
            left_schema.clone(),
            right_schema.clone(),
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
        );
        let joiner = store.and_then(|store| {
            let left = read_columns(&env, &left_keys);
            let right = read_columns(&env, &right_keys);
            let predicate = read_join_predicate(
                &mut env,
                &pred_kinds,
                &pred_payload,
                &pred_child_counts,
                &pred_longs,
                &pred_doubles,
                &pred_strings,
            );
            WindowJoiner::new(
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
            )
            .with_key_timestamp_precisions(read_i32_array(&env, &key_timestamp_precisions))
            .with_store(store)
            .with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, joiner)
    })
}

fn push_rocksdb_window_joiner(
    env: JNIEnv,
    handle: jlong,
    is_left: bool,
    in_array: jlong,
    in_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            if is_left {
                joiner.push_left(batch)
            } else {
                joiner.push_right(batch)
            }
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushLeftRocksDBWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
) {
    push_rocksdb_window_joiner(env, handle, true, in_array, in_schema)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRightRocksDBWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
) {
    push_rocksdb_window_joiner(env, handle, false, in_array, in_schema)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
        match joiner.flush(watermark_millis) {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
        match joiner.store_mut().checkpoint(&snapshot_directory) {
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
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBWindowJoinerPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
        match joiner.canonical_partitions() {
            Ok(partitions) => {
                keyed_state_partition_array(&mut env, partitions, "rocksdb-window-join")
            }
            Err(error) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("RocksDB canonical snapshot failed: {error}"),
                );
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbWindowJoinerStateBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const WindowJoiner) }
            .memory
            .state_bytes as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBWindowJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<WindowJoiner>(handle));
    })
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    offset: jlong,
    limit: jlong,
    output_rank_number: jboolean,
    retracting: jboolean,
    net_diff: jboolean,
    state_ttl_millis: jlong,
    now_millis: jlong,
    memory_budget_bytes: jlong,
    schema_address: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    options_json: JString<'local>,
    shared_resources: jlong,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
        let schema = import_schema(schema_address);
        let converters = TopNConverters::from_declared(&schema, &partitions, &sort);
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: state_ttl_millis.max(0),
            shared_resources,
        };
        let store = open_store(
            &mut env,
            config,
            TopNStateCodec::new(&converters),
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            now_millis,
        );
        let ranker = store.and_then(|store| {
            if retracting != 0 {
                RetractableTopNRanker::new(partitions, sort, offset, limit, output_rank_number != 0)
                    .with_key_timestamp_precisions(timestamp_precisions)
                    .with_net_diff(net_diff != 0)
                    .with_state_ttl(state_ttl_millis)
                    .with_converters(converters)
                    .with_payload_schema(schema)
                    .with_backend(store)
                    .with_read_through_budget(memory_budget_bytes)
                    .map(RocksTopNHandle::Retract)
            } else {
                TopNRanker::new(
                    partitions,
                    sort,
                    limit,
                    output_rank_number != 0,
                    net_diff != 0,
                )
                .with_key_timestamp_precisions(timestamp_precisions)
                .with_state_ttl(state_ttl_millis)
                .with_converters(converters)
                .with_payload_schema(schema)
                .with_backend(store)
                .with_read_through_budget(memory_budget_bytes)
                .map(RocksTopNHandle::Append)
            }
        });
        boxed_or_throw(&mut env, ranker)
    })
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBUpdateFastTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    row_key_columns: JIntArray<'local>,
    row_key_timestamp_precisions: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    limit: jlong,
    output_rank_number: jboolean,
    generate_update_before: jboolean,
    state_ttl_millis: jlong,
    now_millis: jlong,
    memory_budget_bytes: jlong,
    schema_address: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    options_json: JString<'local>,
    shared_resources: jlong,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let partition_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let row_keys = read_columns(&env, &row_key_columns);
        let row_key_precisions = read_i32_array(&env, &row_key_timestamp_precisions);
        let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
        let schema = import_schema(schema_address);
        let converters = TopNConverters::from_declared(&schema, &partitions, &sort);
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: state_ttl_millis.max(0),
            shared_resources,
        };
        let store = open_store(
            &mut env,
            config,
            UpdatableTopNStateCodec::new(&converters),
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            now_millis,
        );
        let ranker = store.and_then(|store| {
            UpdatableTopNRanker::new(
                partitions,
                partition_precisions,
                row_keys,
                row_key_precisions,
                sort,
                limit,
                output_rank_number != 0,
                generate_update_before != 0,
            )
            .with_state_ttl(state_ttl_millis)
            .with_converters(converters)
            .with_payload_schema(schema)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)
            .map(RocksTopNHandle::UpdateFast)
        });
        boxed_or_throw(&mut env, ranker)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRocksDBTopNRanker<'local>(
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
        let ranker = unsafe { &mut *(handle as *mut RocksTopNHandle) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            ranker.push(&batch, now)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let ranker = unsafe { &mut *(handle as *mut RocksTopNHandle) };
        export_record_batch(ranker.flush(), out_array, out_schema);
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        match unsafe { &mut *(handle as *mut RocksTopNHandle) }.checkpoint(&snapshot_directory) {
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
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBTopNRankerPartitions<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut RocksTopNHandle) };
        match ranker.canonical_partitions() {
            Ok(partitions) => keyed_state_partition_array(&mut env, partitions, "rocksdb-top-n"),
            Err(error) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("RocksDB canonical snapshot failed: {error}"),
                );
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbTopNRankerStateBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksTopNHandle) }.state_bytes() as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbTopNRankerStagingBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksTopNHandle) }.staging_bytes() as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbTopNRankerStagedPartitions<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksTopNHandle) }.staged_partitions() as jlong
    })
}
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<RocksTopNHandle>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBSnapshotStore<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    table_directory: JString<'local>,
    max_parallelism: jint,
    options_json: JString<'local>,
    shared_resources: jlong,
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
            shared_resources,
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
