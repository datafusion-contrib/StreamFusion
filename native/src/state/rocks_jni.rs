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

/// The restored blob key groups a create receives when a canonical savepoint or raw keyed state
/// (rather than a RocksDB checkpoint) is being restored; empty on a fresh start or native restore.
fn read_restored_partitions(env: &mut JNIEnv, values: &JObjectArray) -> Vec<Vec<u8>> {
    (0..env.get_array_length(values).expect("array length"))
        .map(|i| {
            let value = env
                .get_object_array_element(values, i)
                .expect("array element");
            env.convert_byte_array(&JByteArray::from(value))
                .expect("restored partition bytes")
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
        rocks_group_supported(&kinds, &values, &group_state_types(&kinds, &values)) as jboolean
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
    restored_partitions: JObjectArray<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_type_codes = read_int_array(&env, &value_types);
        let values: Vec<_> = value_type_codes
            .iter()
            .map(|&code| value_data_type(code))
            .collect();
        let codec = GroupStateCodec::new(
            kinds.clone(),
            values,
            read_int_array(&env, &value_columns),
            read_int_array(&env, &distinct_view_columns),
        );
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
        let aggregator = aggregator.and_then(|mut aggregator| {
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            aggregator.import_partitions(&restored, now_millis)?;
            Ok(aggregator)
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
    restored_partitions: JObjectArray<'local>,
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
        let normalizer = normalizer.and_then(|mut normalizer| {
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            normalizer.import_partitions(&restored, now_millis)?;
            Ok(normalizer)
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
    restored_partitions: JObjectArray<'local>,
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
        let dedup = dedup.and_then(|mut dedup| {
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            dedup.import_partitions(&restored, now_millis)?;
            Ok(dedup)
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
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbKeepFirstDeduplicatorSupported<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema_address: jlong,
) -> jboolean {
    crate::bridge::jni_guard(env, move |_env| {
        let schema = import_schema(schema_address);
        let row_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        rocks_row_supported(&row_types) as jboolean
    })
}

/// [`open_store`] for the keep-first deduplicator's two-table store: fresh when no restored
/// sources exist, otherwise merged once with this subtask's key-group range.
#[allow(clippy::too_many_arguments)]
fn open_keep_first_dedup_store(
    env: &mut JNIEnv,
    config: RocksStoreConfig,
    schema: SchemaRef,
    partition_columns: &[usize],
    source_directories: &JObjectArray,
    source_snapshot_tokens: &JObjectArray,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> Result<RocksKeepFirstDedupStore, DataFusionError> {
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
        RocksKeepFirstDedupStore::create(config, schema, partition_columns)
    } else {
        RocksKeepFirstDedupStore::open_merged(
            config,
            schema,
            partition_columns,
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
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBKeepFirstDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
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
    restored_partitions: JObjectArray<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let schema = import_schema(schema_address);
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: state_ttl_millis.max(0),
            shared_resources,
        };
        let store = open_keep_first_dedup_store(
            &mut env,
            config,
            schema,
            &partitions,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
        );
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let dedup = store.and_then(|store| {
            let mut dedup = KeepFirstDeduplicator::new(partitions, rt_column as usize)
                .with_key_timestamp_precisions(read_i32_array(&env, &key_timestamp_precisions))
                .with_state_ttl(state_ttl_millis)
                .with_store(store);
            dedup.import_partitions(&restored)?;
            dedup.adopt_store_ttl(now_millis)?;
            dedup.with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, dedup)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRocksDBKeepFirstDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
    now_millis: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            dedup.push(&batch, now_millis)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBKeepFirstDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    now_millis: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
        match dedup.flush(watermark_millis, now_millis) {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBKeepFirstDeduplicator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
        match dedup.store_mut().checkpoint(&snapshot_directory) {
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
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBKeepFirstDeduplicatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &*(handle as *const KeepFirstDeduplicator) };
        match dedup.canonical_partitions() {
            Ok(partitions) => {
                keyed_state_partition_array(&mut env, partitions, "rocksdb-keep-first-dedup")
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

state_bytes_getter!(
    Java_tech_streamfusion_Native_rocksdbKeepFirstDeduplicatorStateBytes,
    KeepFirstDeduplicator
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBKeepFirstDeduplicator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<KeepFirstDeduplicator>(handle));
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
    restored_partitions: JObjectArray<'local>,
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
        let joiner = joiner.and_then(|mut joiner| {
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            joiner.import_partitions(&restored, now_millis)?;
            Ok(joiner)
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
    restored_partitions: JObjectArray<'local>,
    restored_timer_deadline: jlong,
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
        let joiner = joiner.and_then(|mut joiner| {
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            joiner.import_partitions(&restored, restored_timer_deadline)?;
            Ok(joiner)
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
    timer_deadline: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        let joiner = unsafe { &mut *(handle as *mut WindowJoiner) };
        match joiner
            .store_mut()
            .checkpoint(timer_deadline, &snapshot_directory)
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
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbIntervalJoinerSupported<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_schema_address: jlong,
    right_schema_address: jlong,
) -> jboolean {
    crate::bridge::jni_guard(env, move |_env| {
        let row_types = |schema: SchemaRef| -> Vec<DataType> {
            schema
                .fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect()
        };
        (rocks_row_supported(&row_types(import_schema(left_schema_address)))
            && rocks_row_supported(&row_types(import_schema(right_schema_address))))
            as jboolean
    })
}

/// [`open_store`] for the interval join's two-table row buffer: fresh when no restored sources
/// exist, otherwise merged once with this subtask's key-group range.
#[allow(clippy::too_many_arguments)]
fn open_interval_buffer(
    env: &mut JNIEnv,
    config: RocksStoreConfig,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    source_directories: &JObjectArray,
    source_snapshot_tokens: &JObjectArray,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> Result<RocksIntervalBuffer, DataFusionError> {
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
        RocksIntervalBuffer::create(config, left_schema, right_schema)
    } else {
        RocksIntervalBuffer::open_merged(
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
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
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
    restored_partitions: JObjectArray<'local>,
    restored_timer_deadline: jlong,
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
        let store = open_interval_buffer(
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
            IntervalJoiner::new(
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
            )
            .with_key_timestamp_precisions(read_i32_array(&env, &key_timestamp_precisions))
            .with_store(store)
            .with_read_through_budget(memory_budget_bytes)
        });
        let joiner = joiner.and_then(|mut joiner| {
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            joiner.import_partitions(&restored, restored_timer_deadline)?;
            Ok(joiner)
        });
        boxed_or_throw(&mut env, joiner)
    })
}

fn push_rocksdb_interval_joiner(
    env: JNIEnv,
    handle: jlong,
    is_left: bool,
    in_array: jlong,
    in_schema: jlong,
    out_array: jlong,
    out_schema: jlong,
    proctime: jboolean,
    proctime_now_millis: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            let now = (proctime != 0).then_some(proctime_now_millis);
            if is_left {
                joiner.push_left(batch, now)
            } else {
                joiner.push_right(batch, now)
            }
        };
        match result {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_tech_streamfusion_Native_pushLeftRocksDBIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
    out_array: jlong,
    out_schema: jlong,
    proctime: jboolean,
    proctime_now_millis: jlong,
) {
    push_rocksdb_interval_joiner(
        env,
        handle,
        true,
        in_array,
        in_schema,
        out_array,
        out_schema,
        proctime,
        proctime_now_millis,
    )
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_tech_streamfusion_Native_pushRightRocksDBIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
    out_array: jlong,
    out_schema: jlong,
    proctime: jboolean,
    proctime_now_millis: jlong,
) {
    push_rocksdb_interval_joiner(
        env,
        handle,
        false,
        in_array,
        in_schema,
        out_array,
        out_schema,
        proctime,
        proctime_now_millis,
    )
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_advanceRocksDBIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
        match joiner.advance(watermark_millis) {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    timer_deadline: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
        match joiner
            .store_mut()
            .checkpoint(timer_deadline, &snapshot_directory)
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
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBIntervalJoinerPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &*(handle as *const IntervalJoiner) };
        match joiner.canonical_partitions() {
            Ok(partitions) => {
                keyed_state_partition_array(&mut env, partitions, "rocksdb-interval-join")
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

state_bytes_getter!(
    Java_tech_streamfusion_Native_rocksdbIntervalJoinerStateBytes,
    IntervalJoiner
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBIntervalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<IntervalJoiner>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbTemporalJoinerSupported<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    left_schema_address: jlong,
    right_schema_address: jlong,
) -> jboolean {
    crate::bridge::jni_guard(env, move |env| {
        let left_schema = import_schema(left_schema_address);
        let right_schema = import_schema(right_schema_address);
        let row_types = |schema: &SchemaRef| -> Vec<DataType> {
            schema
                .fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect()
        };
        let key_types = |schema: &SchemaRef, keys: &[usize]| -> Vec<DataType> {
            keys.iter()
                .map(|&column| schema.field(column).data_type().clone())
                .collect()
        };
        let left = read_columns(&env, &left_keys);
        let right = read_columns(&env, &right_keys);
        (rocks_row_supported(&row_types(&left_schema))
            && rocks_row_supported(&row_types(&right_schema))
            && key_types(&left_schema, &left) == key_types(&right_schema, &right))
            as jboolean
    })
}

/// [`open_store`] for the temporal join's three-table store: fresh when no restored sources
/// exist, otherwise merged once with this subtask's key-group range.
#[allow(clippy::too_many_arguments)]
fn open_temporal_join_store(
    env: &mut JNIEnv,
    config: RocksStoreConfig,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    left_keys: &[usize],
    source_directories: &JObjectArray,
    source_snapshot_tokens: &JObjectArray,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> Result<RocksTemporalJoinStore, DataFusionError> {
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
        RocksTemporalJoinStore::create(config, left_schema, right_schema, left_keys)
    } else {
        RocksTemporalJoinStore::open_merged(
            config,
            left_schema,
            right_schema,
            left_keys,
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
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBTemporalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    left_time: jint,
    right_time: jint,
    join_type: jint,
    left_schema_address: jlong,
    right_schema_address: jlong,
    pred_kinds: JIntArray<'local>,
    pred_payload: JIntArray<'local>,
    pred_child_counts: JIntArray<'local>,
    pred_longs: JLongArray<'local>,
    pred_doubles: JDoubleArray<'local>,
    pred_strings: JObjectArray<'local>,
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
    restored_partitions: JObjectArray<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: 0,
            shared_resources,
        };
        let left = read_columns(&env, &left_keys);
        let right = read_columns(&env, &right_keys);
        let left_schema = import_schema(left_schema_address);
        let right_schema = import_schema(right_schema_address);
        let store = open_temporal_join_store(
            &mut env,
            config,
            left_schema.clone(),
            right_schema.clone(),
            &left,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
        );
        let joiner = store.and_then(|store| {
            let predicate = read_join_predicate(
                &mut env,
                &pred_kinds,
                &pred_payload,
                &pred_child_counts,
                &pred_longs,
                &pred_doubles,
                &pred_strings,
            );
            let mut joiner = TemporalJoiner::new(
                left,
                right,
                left_time as usize,
                right_time as usize,
                JoinKind::from_code(join_type),
                left_schema,
                right_schema,
                predicate,
            )
            .with_state_retention(state_ttl_millis)
            .with_key_timestamp_precisions(read_i32_array(&env, &key_timestamp_precisions))
            .with_store(store);
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            joiner.import_partitions(&restored)?;
            joiner.adopt_store_retention(now_millis)?;
            joiner.with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, joiner)
    })
}

fn push_rocksdb_temporal_joiner(
    env: JNIEnv,
    handle: jlong,
    is_left: bool,
    in_array: jlong,
    in_schema: jlong,
    now_millis: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            if is_left {
                joiner.push_left(&batch, now_millis)
            } else {
                joiner.push_right(&batch, now_millis)
            }
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushLeftRocksDBTemporalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
    now_millis: jlong,
) {
    push_rocksdb_temporal_joiner(env, handle, true, in_array, in_schema, now_millis)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRightRocksDBTemporalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
    now_millis: jlong,
) {
    push_rocksdb_temporal_joiner(env, handle, false, in_array, in_schema, now_millis)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_advanceRocksDBTemporalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    now_millis: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
        match joiner.advance(watermark_millis, now_millis) {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBTemporalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        let joiner = unsafe { &mut *(handle as *mut TemporalJoiner) };
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
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBTemporalJoinerPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &*(handle as *const TemporalJoiner) };
        match joiner.canonical_partitions() {
            Ok(partitions) => {
                keyed_state_partition_array(&mut env, partitions, "rocksdb-temporal-join")
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

state_bytes_getter!(
    Java_tech_streamfusion_Native_rocksdbTemporalJoinerStateBytes,
    TemporalJoiner
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBTemporalJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<TemporalJoiner>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbTemporalSorterSupported<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema_address: jlong,
) -> jboolean {
    crate::bridge::jni_guard(env, move |_env| {
        let schema = import_schema(schema_address);
        let row_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        rocks_row_supported(&row_types) as jboolean
    })
}

/// [`open_store`] for the temporal sort's unkeyed row buffer: fresh when no restored sources
/// exist, otherwise merged once with this subtask's key-group range (always group zero).
#[allow(clippy::too_many_arguments)]
fn open_temporal_sort_buffer(
    env: &mut JNIEnv,
    config: RocksStoreConfig,
    schema: SchemaRef,
    source_directories: &JObjectArray,
    source_snapshot_tokens: &JObjectArray,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> Result<RocksTemporalSortBuffer, DataFusionError> {
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
        RocksTemporalSortBuffer::create(config, schema)
    } else {
        RocksTemporalSortBuffer::open_merged(
            config,
            schema,
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
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    rt_column: jint,
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
    restored_partitions: JObjectArray<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: 0,
            shared_resources,
        };
        let store = open_temporal_sort_buffer(
            &mut env,
            config,
            import_schema(schema_address),
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
        );
        let sorter = store.and_then(|store| {
            let mut sorter = TemporalSorter::new(rt_column as usize)
                .with_store(store)
                .with_read_through_budget(memory_budget_bytes)?;
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            sorter.import_snapshots(&restored)?;
            Ok(sorter)
        });
        boxed_or_throw(&mut env, sorter)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRocksDBTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let sorter = unsafe { &mut *(handle as *mut TemporalSorter) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            sorter.push(batch)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let sorter = unsafe { &mut *(handle as *mut TemporalSorter) };
        match sorter.flush(watermark_millis) {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        let sorter = unsafe { &mut *(handle as *mut TemporalSorter) };
        match sorter.store_mut().checkpoint(&snapshot_directory) {
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

/// The persistent buffer as the memory snapshot's own plain-IPC blob; the host frames it into its
/// singleton key group exactly as it frames the memory snapshot.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jbyteArray {
    crate::bridge::jni_guard(env, move |env| {
        let sorter = unsafe { &*(handle as *const TemporalSorter) };
        match sorter.store_snapshot() {
            Ok(bytes) => env
                .byte_array_from_slice(&bytes)
                .expect("failed to allocate sort snapshot array")
                .into_raw(),
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

state_bytes_getter!(
    Java_tech_streamfusion_Native_rocksdbTemporalSorterStateBytes,
    TemporalSorter
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBTemporalSorter<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<TemporalSorter>(handle));
    })
}

/// The Arrow carriage type of a window grouping-key column, from the JVM key-type code: timestamps
/// ride as int64 nanoseconds and int widens to int64 (the aggregator's existing key carriage), so
/// only string, boolean, date, and decimal keys keep a distinct Arrow type.
fn window_key_data_type(code: i64) -> DataType {
    match code {
        3 => DataType::Utf8,
        7 => DataType::Boolean,
        8 => DataType::Date32,
        code if code >= 2000 => {
            let packed = code - 2000;
            DataType::Decimal128((packed / 100) as u8, (packed % 100) as i8)
        }
        _ => DataType::Int64,
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbWindowAggregatorSupported<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    key_types: JIntArray<'local>,
) -> jboolean {
    crate::bridge::jni_guard(env, move |env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_types = read_int_array(&env, &value_types);
        let key_types: Vec<DataType> = read_int_array(&env, &key_types)
            .into_iter()
            .map(window_key_data_type)
            .collect();
        (rocks_row_supported(&key_types)
            && rocks_row_supported(&window_state_types(&kinds, &value_types))) as jboolean
    })
}

/// [`open_store`] for the aligned-window aggregate's composite-key store: fresh when no restored
/// sources exist, otherwise merged once with this subtask's key-group range.
#[allow(clippy::too_many_arguments)]
fn open_window_agg_store(
    env: &mut JNIEnv,
    config: RocksStoreConfig,
    state_types: &[DataType],
    source_directories: &JObjectArray,
    source_snapshot_tokens: &JObjectArray,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> Result<RocksWindowAggStore, DataFusionError> {
    let source_dirs: Vec<_> = read_strings(env, source_directories)
        .into_iter()
        .flatten()
        .collect();
    let source_tokens: Vec<_> = read_strings(env, source_snapshot_tokens)
        .into_iter()
        .flatten()
        .map(|token| token.parse::<i64>().expect("RocksDB checkpoint generation"))
        .collect();
    let key_groups = key_group_start..=key_group_end;
    if source_dirs.is_empty() {
        RocksWindowAggStore::create(config, state_types, key_groups)
    } else {
        RocksWindowAggStore::open_merged(
            config,
            state_types,
            key_groups,
            &source_dirs
                .into_iter()
                .zip(source_tokens)
                .collect::<Vec<_>>(),
            aligned != 0,
        )
    }
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBWindowAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    window_millis: jlong,
    slide_millis: jlong,
    cumulative: jboolean,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    key_types: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
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
    restored_partitions: JObjectArray<'local>,
    restored_timer_deadline: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_types = read_int_array(&env, &value_types);
        let key_types: Vec<DataType> = read_int_array(&env, &key_types)
            .into_iter()
            .map(window_key_data_type)
            .collect();
        let state_types = window_state_types(&kinds, &value_types);
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: 0,
            shared_resources,
        };
        let store = if rocks_row_supported(&key_types) {
            open_window_agg_store(
                &mut env,
                config,
                &state_types,
                &source_directories,
                &source_snapshot_tokens,
                key_group_start,
                key_group_end,
                aligned,
            )
        } else {
            Err(DataFusionError::Plan(
                "window key shape not supported by RocksDB".into(),
            ))
        };
        let aggregator = store.and_then(|store| {
            let mut aggregator = TumblingAggregator::new(
                window_millis,
                slide_millis,
                cumulative != 0,
                value_types,
                kinds,
            )
            .with_key_timestamp_precisions(read_i32_array(&env, &key_timestamp_precisions))
            .with_store(store, key_types)
            .with_read_through_budget(memory_budget_bytes)?;
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            aggregator.import_partitions(&restored, restored_timer_deadline)?;
            Ok(aggregator)
        });
        boxed_or_throw(&mut env, aggregator)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRocksDBWindowAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            aggregator.update(&batch)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushPartialRocksDBWindowAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            aggregator.update_partial(&batch)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBWindowAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
        match aggregator.flush(watermark_millis) {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBWindowAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    timer_deadline: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
        match aggregator.checkpoint_store(timer_deadline, &snapshot_directory) {
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
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBWindowAggregatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut TumblingAggregator) };
        let precisions = read_i32_array(&env, &timestamp_precisions);
        match aggregator.canonical_partitions(max_parallelism as usize, &precisions) {
            Ok(partitions) => {
                keyed_state_partition_array(&mut env, partitions, "rocksdb-fixed-window")
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

state_bytes_getter!(
    Java_tech_streamfusion_Native_rocksdbWindowAggregatorStateBytes,
    TumblingAggregator
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBWindowAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<TumblingAggregator>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbSessionAggregatorSupported<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    key_types: JIntArray<'local>,
) -> jboolean {
    crate::bridge::jni_guard(env, move |env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_types = read_int_array(&env, &value_types);
        let key_types: Vec<DataType> = read_int_array(&env, &key_types)
            .into_iter()
            .map(window_key_data_type)
            .collect();
        (rocks_row_supported(&key_types)
            && rocks_row_supported(&window_state_types(&kinds, &value_types))) as jboolean
    })
}

/// [`open_store`] for the session aggregate's key-major store: fresh when no restored sources
/// exist, otherwise merged once with this subtask's key-group range.
#[allow(clippy::too_many_arguments)]
fn open_session_agg_store(
    env: &mut JNIEnv,
    config: RocksStoreConfig,
    state_types: &[DataType],
    source_directories: &JObjectArray,
    source_snapshot_tokens: &JObjectArray,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> Result<RocksSessionAggStore, DataFusionError> {
    let source_dirs: Vec<_> = read_strings(env, source_directories)
        .into_iter()
        .flatten()
        .collect();
    let source_tokens: Vec<_> = read_strings(env, source_snapshot_tokens)
        .into_iter()
        .flatten()
        .map(|token| token.parse::<i64>().expect("RocksDB checkpoint generation"))
        .collect();
    let key_groups = key_group_start..=key_group_end;
    if source_dirs.is_empty() {
        RocksSessionAggStore::create(config, state_types, key_groups)
    } else {
        RocksSessionAggStore::open_merged(
            config,
            state_types,
            key_groups,
            &source_dirs
                .into_iter()
                .zip(source_tokens)
                .collect::<Vec<_>>(),
            aligned != 0,
        )
    }
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    gap_millis: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    key_types: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
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
    restored_partitions: JObjectArray<'local>,
    restored_timer_deadline: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_types = read_int_array(&env, &value_types);
        let key_types: Vec<DataType> = read_int_array(&env, &key_types)
            .into_iter()
            .map(window_key_data_type)
            .collect();
        let state_types = window_state_types(&kinds, &value_types);
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: 0,
            shared_resources,
        };
        let store = if rocks_row_supported(&key_types) {
            open_session_agg_store(
                &mut env,
                config,
                &state_types,
                &source_directories,
                &source_snapshot_tokens,
                key_group_start,
                key_group_end,
                aligned,
            )
        } else {
            Err(DataFusionError::Plan(
                "session key shape not supported by RocksDB".into(),
            ))
        };
        let aggregator = store.and_then(|store| {
            let mut aggregator = SessionAggregator::new(gap_millis, value_types, kinds)
                .with_key_timestamp_precisions(read_i32_array(&env, &key_timestamp_precisions))
                .with_store(store, key_types)
                .with_read_through_budget(memory_budget_bytes)?;
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            aggregator.import_partitions(&restored, restored_timer_deadline)?;
            Ok(aggregator)
        });
        boxed_or_throw(&mut env, aggregator)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRocksDBSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            aggregator.update(&batch)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        match aggregator.flush(watermark_millis) {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    timer_deadline: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        match aggregator.checkpoint_store(timer_deadline, &snapshot_directory) {
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
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBSessionAggregatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut SessionAggregator) };
        let precisions = read_i32_array(&env, &timestamp_precisions);
        match aggregator.canonical_partitions(max_parallelism as usize, &precisions) {
            Ok(partitions) => {
                keyed_state_partition_array(&mut env, partitions, "rocksdb-session-window")
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

state_bytes_getter!(
    Java_tech_streamfusion_Native_rocksdbSessionAggregatorStateBytes,
    SessionAggregator
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBSessionAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<SessionAggregator>(handle));
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbOverAggregatorSupported<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    frame_kind: jint,
    proctime: jboolean,
    in_schema: jlong,
) -> jboolean {
    crate::bridge::jni_guard(env, move |env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_types = read_int_array(&env, &value_types);
        let schema = import_schema(in_schema);
        let payload_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let supported =
            match rocks_over_state_types(&value_types, &kinds, frame_kind as i64, proctime != 0) {
                Some(state_types) => {
                    rocks_row_supported(&state_types) && rocks_row_supported(&payload_types)
                }
                None => false,
            };
        supported as jboolean
    })
}

/// [`open_store`] for the OVER aggregate's two-table store: fresh when no restored sources
/// exist, otherwise merged once with this subtask's key-group range.
#[allow(clippy::too_many_arguments)]
fn open_over_agg_store(
    env: &mut JNIEnv,
    config: RocksStoreConfig,
    state_types: &[DataType],
    payload_schema: SchemaRef,
    source_directories: &JObjectArray,
    source_snapshot_tokens: &JObjectArray,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> Result<RocksOverAggStore, DataFusionError> {
    let source_dirs: Vec<_> = read_strings(env, source_directories)
        .into_iter()
        .flatten()
        .collect();
    let source_tokens: Vec<_> = read_strings(env, source_snapshot_tokens)
        .into_iter()
        .flatten()
        .map(|token| token.parse::<i64>().expect("RocksDB checkpoint generation"))
        .collect();
    let key_groups = key_group_start..=key_group_end;
    if source_dirs.is_empty() {
        RocksOverAggStore::create(config, state_types, payload_schema, key_groups)
    } else {
        RocksOverAggStore::open_merged(
            config,
            state_types,
            payload_schema,
            key_groups,
            &source_dirs
                .into_iter()
                .zip(source_tokens)
                .collect::<Vec<_>>(),
            aligned != 0,
        )
    }
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    rt_column: jint,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    frame_kind: jint,
    frame_offset: jlong,
    proctime: jboolean,
    key_timestamp_precisions: JIntArray<'local>,
    state_ttl_millis: jlong,
    now_millis: jlong,
    memory_budget_bytes: jlong,
    in_schema: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    options_json: JString<'local>,
    shared_resources: jlong,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
    restored_partitions: JObjectArray<'local>,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_types = read_int_array(&env, &value_types);
        let values = read_columns(&env, &value_columns);
        let keys = read_columns(&env, &key_columns);
        let schema = import_schema(in_schema);
        let key_types: Vec<DataType> = keys
            .iter()
            .map(|&column| schema.field(column).data_type().clone())
            .collect();
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: 0,
            shared_resources,
        };
        let store =
            match rocks_over_state_types(&value_types, &kinds, frame_kind as i64, proctime != 0) {
                Some(state_types) => open_over_agg_store(
                    &mut env,
                    config,
                    &state_types,
                    schema,
                    &source_directories,
                    &source_snapshot_tokens,
                    key_group_start,
                    key_group_end,
                    aligned,
                ),
                None => Err(DataFusionError::Plan(
                    "over shape not supported by RocksDB".into(),
                )),
            };
        let aggregator = store.and_then(|store| {
            let mut aggregator = OverWindowAggregator::new(
                value_types,
                kinds,
                rt_column as usize,
                values,
                keys,
                frame_kind as i64,
                frame_offset,
                proctime != 0,
            )
            .with_state_retention(state_ttl_millis)
            .with_key_timestamp_precisions(read_i32_array(&env, &key_timestamp_precisions))
            .with_store(store, key_types);
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            aggregator.import_partitions(&restored)?;
            aggregator.adopt_store_retention(now_millis)?;
            aggregator.with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, aggregator)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRocksDBOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
    now_millis: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            aggregator.push(batch, now_millis)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    now_millis: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        match aggregator.flush(watermark_millis, now_millis) {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        match aggregator.checkpoint_store(&snapshot_directory) {
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
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBOverAggregatorPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    max_parallelism: jint,
    timestamp_precisions: JIntArray<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut OverWindowAggregator) };
        let precisions = read_i32_array(&env, &timestamp_precisions);
        match aggregator.canonical_partitions(max_parallelism as usize, &precisions) {
            Ok(partitions) => keyed_state_partition_array(&mut env, partitions, "rocksdb-over"),
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

state_bytes_getter!(
    Java_tech_streamfusion_Native_rocksdbOverAggregatorStateBytes,
    OverWindowAggregator
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBOverAggregator<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<OverWindowAggregator>(handle));
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
    restored_partitions: JObjectArray<'local>,
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
        let ranker = ranker.and_then(|mut ranker| {
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            ranker.import_partitions(&restored, now_millis)?;
            Ok(ranker)
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
    restored_partitions: JObjectArray<'local>,
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
        let ranker = ranker.and_then(|mut ranker| {
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            ranker.import_partitions(&restored, now_millis)?;
            Ok(ranker)
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
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbWindowRankerSupported<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    schema_address: jlong,
) -> jboolean {
    crate::bridge::jni_guard(env, move |_env| {
        let schema = import_schema(schema_address);
        let row_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        rocks_row_supported(&row_types) as jboolean
    })
}

/// [`open_store`] for the window ranker's composite-key store: fresh when no restored sources
/// exist, otherwise merged once with this subtask's key-group range.
#[allow(clippy::too_many_arguments)]
fn open_window_rank_store(
    env: &mut JNIEnv,
    config: RocksStoreConfig,
    row_types: &[DataType],
    source_directories: &JObjectArray,
    source_snapshot_tokens: &JObjectArray,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> Result<RocksWindowRankStore, DataFusionError> {
    let source_dirs: Vec<_> = read_strings(env, source_directories)
        .into_iter()
        .flatten()
        .collect();
    let source_tokens: Vec<_> = read_strings(env, source_snapshot_tokens)
        .into_iter()
        .flatten()
        .map(|token| token.parse::<i64>().expect("RocksDB checkpoint generation"))
        .collect();
    let key_groups = key_group_start..=key_group_end;
    if source_dirs.is_empty() {
        RocksWindowRankStore::create(config, row_types, key_groups)
    } else {
        RocksWindowRankStore::open_merged(
            config,
            row_types,
            key_groups,
            &source_dirs
                .into_iter()
                .zip(source_tokens)
                .collect::<Vec<_>>(),
            aligned != 0,
        )
    }
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBWindowRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    window_start_col: jint,
    window_end_col: jint,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    limit: jlong,
    output_rank_number: jboolean,
    schema_address: jlong,
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
    restored_partitions: JObjectArray<'local>,
    restored_timer_deadline: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let schema = import_schema(schema_address);
        let row_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: 0,
            shared_resources,
        };
        let store = open_window_rank_store(
            &mut env,
            config,
            &row_types,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
        );
        let ranker = store.and_then(|store| {
            let partitions = read_columns(&env, &partition_columns);
            let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
            WindowRanker::new(
                window_start_col as usize,
                window_end_col as usize,
                partitions,
                sort,
                limit,
                output_rank_number != 0,
            )
            .with_key_timestamp_precisions(read_i32_array(&env, &key_timestamp_precisions))
            .with_store(store, schema)
            .with_memory_budget(memory_budget_bytes)
        });
        let ranker = ranker.and_then(|mut ranker| {
            let restored = read_restored_partitions(&mut env, &restored_partitions);
            ranker.import_partitions(&restored, restored_timer_deadline)?;
            Ok(ranker)
        });
        boxed_or_throw(&mut env, ranker)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRocksDBWindowRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut WindowRanker) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            ranker.push(&batch)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBWindowRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut WindowRanker) };
        match ranker.flush(watermark_millis) {
            Ok(out) => export_record_batch(out, out_array, out_schema),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBWindowRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    timer_deadline: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        let ranker = unsafe { &mut *(handle as *mut WindowRanker) };
        match ranker.checkpoint_store(timer_deadline, &snapshot_directory) {
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
pub extern "system" fn Java_tech_streamfusion_Native_snapshotRocksDBWindowRankerPartitions<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &*(handle as *const WindowRanker) };
        match ranker.canonical_partitions() {
            Ok(partitions) => {
                keyed_state_partition_array(&mut env, partitions, "rocksdb-window-rank")
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

state_bytes_getter!(
    Java_tech_streamfusion_Native_rocksdbWindowRankerStateBytes,
    WindowRanker
);

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeRocksDBWindowRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<WindowRanker>(handle));
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

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbWindowAggregatorTimerDeadline<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const TumblingAggregator) }.store_timer_deadline() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbSessionAggregatorTimerDeadline<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const SessionAggregator) }.store_timer_deadline() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbWindowRankerTimerDeadline<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const WindowRanker) }.store_timer_deadline() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbWindowJoinerTimerDeadline<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &mut *(handle as *mut WindowJoiner) }
            .store_mut()
            .timer_deadline() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbIntervalJoinerTimerDeadline<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &mut *(handle as *mut IntervalJoiner) }
            .store_mut()
            .timer_deadline() as jlong
    })
}
