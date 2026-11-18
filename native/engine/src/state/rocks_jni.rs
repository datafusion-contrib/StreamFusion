//! JNI lifecycle for Rust-owned RocksDB state. Java controls Flink lifecycle and file upload;
//! every state read, write, flush, and compaction stays inside Rust/RocksDB.

use super::rocks_lifecycle::{self as lifecycle, *};
use super::rocks_store::{retained_rows::RetainedRows, window_memberships::WindowStateCodec};
use crate::*;
use jni::objects::{
    JByteArray, JClass, JDoubleArray, JIntArray, JLongArray, JObject, JObjectArray, JString,
};
use jni::sys::{jboolean, jdouble, jint, jlong, jobjectArray};
use jni::JNIEnv;

pub(crate) fn read_string(env: &mut JNIEnv, value: &JString) -> String {
    env.get_string(value).expect("jni string").into()
}

pub(crate) fn read_strings(env: &mut JNIEnv, values: &JObjectArray) -> Vec<Option<String>> {
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
pub(crate) fn read_restored_partitions(env: &mut JNIEnv, values: &JObjectArray) -> Vec<Vec<u8>> {
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

fn read_restore(
    env: &mut JNIEnv,
    source_directories: &JObjectArray,
    source_snapshot_tokens: &JObjectArray,
    key_group_start: i32,
    key_group_end: i32,
    aligned: jboolean,
    partitions: Vec<Vec<u8>>,
) -> Restore {
    let directories = read_strings(env, source_directories)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let tokens = read_strings(env, source_snapshot_tokens)
        .into_iter()
        .flatten()
        .map(|token| token.parse::<i64>().expect("RocksDB checkpoint generation"))
        .collect::<Vec<_>>();
    Restore {
        sources: directories.into_iter().zip(tokens).collect(),
        key_groups: key_group_start..=key_group_end,
        aligned: aligned != 0,
        partitions,
    }
}

fn runtime_result<'local, T, R: crate::bridge::JniDefault>(
    env: &mut JNIEnv<'local>,
    result: Result<T, DataFusionError>,
    context: &str,
    convert: impl FnOnce(&mut JNIEnv<'local>, T) -> R,
) -> R {
    match result {
        Ok(value) => convert(env, value),
        Err(error) => {
            let _ = env.throw_new("java/lang/RuntimeException", format!("{context}: {error}"));
            R::jni_default()
        }
    }
}

pub(crate) fn manifest_array<'local>(
    env: &mut JNIEnv<'local>,
    manifest: &RocksCheckpointManifest,
) -> jobjectArray {
    let lines = lifecycle::manifest_lines(manifest);
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
        if kinds
            .iter()
            .any(|kind| kind & crate::group_agg::RETAINED_DISTINCT != 0)
        {
            return 0;
        }
        let values: Vec<_> = read_int_array(&env, &values)
            .into_iter()
            .map(value_data_type)
            .collect();
        rocks_group_supported(&kinds, &values, &group_state_types(&kinds, &values)) as jboolean
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBGroupAggregatorWithTtlEmission<
    'local,
>(
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
    emit_unchanged_with_ttl: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: state_ttl_millis.max(0),
            shared_resources,
        };
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let aggregate_kinds = read_int_array(&env, &aggregate_kinds);
        let value_types = read_int_array(&env, &value_types);
        let value_columns = read_int_array(&env, &value_columns);
        let key_columns = read_columns(&env, &key_columns);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let filter_columns = read_int_array(&env, &filter_columns);
        let count_columns = read_int_array(&env, &count_columns);
        let distinct_view_columns = read_int_array(&env, &distinct_view_columns);
        let generate_update_before = generate_update_before != 0;
        let mini_batch = mini_batch != 0;
        let emit_unchanged_with_ttl = emit_unchanged_with_ttl != 0;
        let result = lifecycle::create_group_aggregator_with_ttl_emission(
            Storage { config, restore },
            GroupOptions {
                aggregate_kinds,
                value_types,
                value_columns,
                key_columns,
                key_timestamp_precisions,
                filter_columns,
                count_columns,
                distinct_view_columns,
                record_count_column,
                generate_update_before,
                mini_batch,
                state_ttl_millis,
                now_millis,
                memory_budget_bytes,
                emit_unchanged_with_ttl,
            },
        );
        boxed_or_throw(&mut env, result)
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
    now_millis: jlong,
    out_array: jlong,
    out_schema: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut RocksGroupAggregator) };
        aggregator.store_mut().set_clock(now_millis);
        match aggregator.flush_mini_batch_at(now_millis) {
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
        let result = lifecycle::checkpoint_group_aggregator(
            unsafe { &mut *(handle as *mut RocksGroupAggregator) },
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let result = aggregator.canonical_partitions();
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, partitions| {
                keyed_state_partition_array(env, partitions, "rocksdb-group-aggregate")
            },
        )
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbGroupAggregatorStateBytes<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe { &*(handle as *const RocksGroupAggregator) }
            .memory
            .state_bytes() as jlong
    })
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
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let key_columns = read_columns(&env, &key_columns);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let generate_update_before = generate_update_before != 0;
        let mini_batch = mini_batch != 0;
        let result = lifecycle::create_changelog_normalizer(
            Storage { config, restore },
            NormalizerOptions {
                key_columns,
                key_timestamp_precisions,
                generate_update_before,
                mini_batch,
                state_ttl_millis,
                now_millis,
                memory_budget_bytes,
            },
        );
        boxed_or_throw(&mut env, result)
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
        let result = lifecycle::checkpoint_changelog_normalizer(
            unsafe { &mut *(handle as *mut RocksChangelogNormalizer) },
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let result = normalizer.canonical_partitions();
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, partitions| {
                keyed_state_partition_array(env, partitions, "rocksdb-changelog-normalizer")
            },
        )
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
pub extern "system" fn Java_tech_streamfusion_Native_createRocksDBFirstN<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partitions: JIntArray<'local>,
    precisions: JIntArray<'local>,
    limit: jint,
    output_rank: jboolean,
    ttl: jlong,
    now: jlong,
    budget: jlong,
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
            ttl_ms: ttl.max(0),
            shared_resources,
        };
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let partitions = read_columns(&env, &partitions);
        let precisions = read_i32_array(&env, &precisions);
        let output_rank = output_rank != 0;
        let result = lifecycle::create_first_n(
            Storage { config, restore },
            FirstNOptions {
                partitions,
                precisions,
                limit,
                output_rank,
                ttl,
                now,
                budget,
            },
        );
        boxed_or_throw(&mut env, result)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointRocksDBFirstN<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    snapshot_directory: JString<'local>,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let snapshot_directory = read_string(&mut env, &snapshot_directory);
        let result = lifecycle::checkpoint_first_n(
            unsafe { &mut *(handle as *mut FirstNHandle) },
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "first-N checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let partition_columns = read_columns(&env, &partition_columns);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let generate_update_before = generate_update_before != 0;
        let generate_insert = generate_insert != 0;
        let rowtime_ordered = rowtime_ordered != 0;
        let keep_first = keep_first != 0;
        let mini_batch = mini_batch != 0;
        let compact_changes = compact_changes != 0;
        let result = lifecycle::create_keep_last_deduplicator(
            Storage { config, restore },
            KeepLastOptions {
                partition_columns,
                key_timestamp_precisions,
                rt_column,
                generate_update_before,
                generate_insert,
                rowtime_ordered,
                keep_first,
                mini_batch,
                compact_changes,
                state_ttl_millis,
                now_millis,
                memory_budget_bytes,
            },
        );
        boxed_or_throw(&mut env, result)
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
        let result = lifecycle::checkpoint_keep_last_deduplicator(
            unsafe { &mut *(handle as *mut RocksKeepLastDeduplicator) },
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let result = dedup.canonical_partitions();
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, partitions| {
                keyed_state_partition_array(env, partitions, "rocksdb-keep-last-dedup")
            },
        )
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
        RetainedRows::supported(&row_types) as jboolean
    })
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
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: state_ttl_millis.max(0),
            shared_resources,
        };
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let partition_columns = read_columns(&env, &partition_columns);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let schema = import_schema(schema_address);
        let result = lifecycle::create_keep_first_deduplicator(
            Storage { config, restore },
            KeepFirstOptions {
                partition_columns,
                key_timestamp_precisions,
                rt_column,
                state_ttl_millis,
                now_millis,
                memory_budget_bytes,
                schema,
            },
        );
        boxed_or_throw(&mut env, result)
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
        let result = lifecycle::checkpoint_keep_first_deduplicator(
            unsafe { &mut *(handle as *mut KeepFirstDeduplicator) },
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let result = dedup.canonical_partitions();
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, partitions| {
                keyed_state_partition_array(env, partitions, "rocksdb-keep-first-dedup")
            },
        )
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
    filter_nulls: JIntArray<'local>,
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
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let predicate = read_join_predicate(
            &mut env,
            &pred_kinds,
            &pred_payload,
            &pred_child_counts,
            &pred_longs,
            &pred_doubles,
            &pred_strings,
        );
        let left_keys = read_columns(&env, &left_keys);
        let right_keys = read_columns(&env, &right_keys);
        let filter_nulls = read_i32_array(&env, &filter_nulls);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let left_schema = import_schema(left_schema_address);
        let right_schema = import_schema(right_schema_address);
        let left_join_key_unique = left_join_key_unique != 0;
        let right_join_key_unique = right_join_key_unique != 0;
        let mini_batch = mini_batch != 0;
        let result = lifecycle::create_updating_joiner(
            Storage { config, restore },
            UpdatingJoinOptions {
                predicate,
                left_keys,
                right_keys,
                filter_nulls,
                key_timestamp_precisions,
                join_type,
                left_schema,
                right_schema,
                left_join_key_unique,
                right_join_key_unique,
                mini_batch,
                left_state_ttl_millis,
                right_state_ttl_millis,
                now_millis,
                memory_budget_bytes,
            },
        );
        boxed_or_throw(&mut env, result)
    })
}

fn push_rocksdb_updating_joiner(
    env: JNIEnv,
    handle: jlong,
    is_left: bool,
    in_array: jlong,
    in_schema: jlong,
    now: jlong,
    receiver: jni::objects::JObject,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut RocksUpdatingJoiner) };
        let (left_store, right_store) = joiner.stores_mut();
        left_store.set_clock(now);
        right_store.set_clock(now);
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array, in_schema);
            joiner.push_to(&batch, is_left, now, &mut |out| {
                crate::bridge::emit_record_batch(&mut env, &receiver, out)
            })
        };
        match result {
            Ok(()) => (),
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
    receiver: jni::objects::JObject,
) {
    push_rocksdb_updating_joiner(env, handle, true, in_array, in_schema, now, receiver)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRightRocksDBUpdatingJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array: jlong,
    in_schema: jlong,
    now: jlong,
    receiver: jni::objects::JObject,
) {
    push_rocksdb_updating_joiner(env, handle, false, in_array, in_schema, now, receiver)
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBUpdatingJoiner<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    receiver: jni::objects::JObject,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        match unsafe { &mut *(handle as *mut RocksUpdatingJoiner) }.flush_mini_batch_to(
            &mut |out| crate::bridge::emit_record_batch(&mut env, &receiver, out),
        ) {
            Ok(()) => (),
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
        let result = lifecycle::checkpoint_updating_joiner(
            unsafe { &mut *(handle as *mut RocksUpdatingJoiner) },
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let result = joiner.canonical_partitions();
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, partitions| keyed_state_partition_array(env, partitions, "rocksdb-updating-join"),
        )
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
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let predicate = read_join_predicate(
            &mut env,
            &pred_kinds,
            &pred_payload,
            &pred_child_counts,
            &pred_longs,
            &pred_doubles,
            &pred_strings,
        );
        let left_keys = read_columns(&env, &left_keys);
        let right_keys = read_columns(&env, &right_keys);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let left_schema = import_schema(left_schema_address);
        let right_schema = import_schema(right_schema_address);
        let result = lifecycle::create_window_joiner(
            Storage { config, restore },
            WindowJoinOptions {
                predicate,
                left_keys,
                right_keys,
                key_timestamp_precisions,
                left_window_start,
                left_window_end,
                right_window_start,
                right_window_end,
                join_type,
                left_schema,
                right_schema,
                memory_budget_bytes,
                restored_timer_deadline,
            },
        );
        boxed_or_throw(&mut env, result)
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
        let result = lifecycle::checkpoint_window_joiner(
            unsafe { &mut *(handle as *mut WindowJoiner) },
            timer_deadline,
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let result = joiner.canonical_partitions();
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, partitions| keyed_state_partition_array(env, partitions, "rocksdb-window-join"),
        )
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
        (RetainedRows::supported(&row_types(import_schema(left_schema_address)))
            && RetainedRows::supported(&row_types(import_schema(right_schema_address))))
            as jboolean
    })
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
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let predicate = read_join_predicate(
            &mut env,
            &pred_kinds,
            &pred_payload,
            &pred_child_counts,
            &pred_longs,
            &pred_doubles,
            &pred_strings,
        );
        let left_keys = read_columns(&env, &left_keys);
        let right_keys = read_columns(&env, &right_keys);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let left_schema = import_schema(left_schema_address);
        let right_schema = import_schema(right_schema_address);
        let result = lifecycle::create_interval_joiner(
            Storage { config, restore },
            IntervalJoinOptions {
                predicate,
                left_keys,
                right_keys,
                key_timestamp_precisions,
                left_time,
                right_time,
                lower,
                upper,
                join_type,
                left_schema,
                right_schema,
                memory_budget_bytes,
                restored_timer_deadline,
            },
        );
        boxed_or_throw(&mut env, result)
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
        let result = lifecycle::checkpoint_interval_joiner(
            unsafe { &mut *(handle as *mut IntervalJoiner) },
            timer_deadline,
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let joiner = unsafe { &mut *(handle as *mut IntervalJoiner) };
        let result = lifecycle::snapshot_interval_joiner(joiner);
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, partitions| keyed_state_partition_array(env, partitions, "rocksdb-interval-join"),
        )
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
        (RetainedRows::supported(&row_types(&left_schema))
            && RetainedRows::supported(&row_types(&right_schema))
            && rocks_row_supported(&key_types(&left_schema, &left))
            && key_types(&left_schema, &left) == key_types(&right_schema, &right))
            as jboolean
    })
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
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let predicate = read_join_predicate(
            &mut env,
            &pred_kinds,
            &pred_payload,
            &pred_child_counts,
            &pred_longs,
            &pred_doubles,
            &pred_strings,
        );
        let left_keys = read_columns(&env, &left_keys);
        let right_keys = read_columns(&env, &right_keys);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let left_schema = import_schema(left_schema_address);
        let right_schema = import_schema(right_schema_address);
        let result = lifecycle::create_temporal_joiner(
            Storage { config, restore },
            TemporalJoinOptions {
                predicate,
                left_keys,
                right_keys,
                key_timestamp_precisions,
                left_time,
                right_time,
                join_type,
                left_schema,
                right_schema,
                state_ttl_millis,
                now_millis,
                memory_budget_bytes,
            },
        );
        boxed_or_throw(&mut env, result)
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
        let result = lifecycle::checkpoint_temporal_joiner(
            unsafe { &mut *(handle as *mut TemporalJoiner) },
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let result = joiner.canonical_partitions();
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, partitions| keyed_state_partition_array(env, partitions, "rocksdb-temporal-join"),
        )
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
        RetainedRows::supported(&row_types) as jboolean
    })
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
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let schema = import_schema(schema_address);
        let result = lifecycle::create_temporal_sorter(
            Storage { config, restore },
            TemporalSortOptions {
                rt_column,
                memory_budget_bytes,
                schema,
            },
        );
        boxed_or_throw(&mut env, result)
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
        let result = lifecycle::checkpoint_temporal_sorter(
            unsafe { &mut *(handle as *mut TemporalSorter) },
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
    crate::bridge::jni_guard(env, move |mut env| {
        let sorter = unsafe { &*(handle as *const TemporalSorter) };
        let result = sorter.store_snapshot();
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, bytes| {
                env.byte_array_from_slice(&bytes)
                    .expect("failed to allocate sort snapshot array")
                    .into_raw()
            },
        )
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
/// keep their existing codec and legacy INT widens to int64. Newly admitted narrow integers,
/// floating point, TIME, binary and month-interval keys retain their natural Arrow carriage.
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
            && WindowStateCodec::new(&build_aggregates(&kinds, &value_types)).is_ok())
            as jboolean
    })
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
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: 0,
            shared_resources,
        };
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let cumulative = cumulative != 0;
        let value_types = read_int_array(&env, &value_types);
        let aggregate_kinds = read_int_array(&env, &aggregate_kinds);
        let key_types = read_int_array(&env, &key_types);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let result = lifecycle::create_window_aggregator(
            Storage { config, restore },
            WindowOptions {
                window_millis,
                slide_millis,
                cumulative,
                value_types,
                aggregate_kinds,
                key_types,
                key_timestamp_precisions,
                memory_budget_bytes,
                restored_timer_deadline,
            },
        );
        boxed_or_throw(&mut env, result)
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
        let result = lifecycle::checkpoint_window_aggregator(
            unsafe { &mut *(handle as *mut TumblingAggregator) },
            timer_deadline,
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let result = aggregator.canonical_partitions(max_parallelism as usize, &precisions);
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, partitions| keyed_state_partition_array(env, partitions, "rocksdb-fixed-window"),
        )
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
            && WindowStateCodec::new(&build_aggregates(&kinds, &value_types)).is_ok())
            as jboolean
    })
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
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: 0,
            shared_resources,
        };
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let value_types = read_int_array(&env, &value_types);
        let aggregate_kinds = read_int_array(&env, &aggregate_kinds);
        let key_types = read_int_array(&env, &key_types);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let result = lifecycle::create_session_aggregator(
            Storage { config, restore },
            SessionOptions {
                gap_millis,
                value_types,
                aggregate_kinds,
                key_types,
                key_timestamp_precisions,
                memory_budget_bytes,
                restored_timer_deadline,
            },
        );
        boxed_or_throw(&mut env, result)
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
        let result = lifecycle::checkpoint_session_aggregator(
            unsafe { &mut *(handle as *mut SessionAggregator) },
            timer_deadline,
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let result = aggregator.canonical_partitions(max_parallelism as usize, &precisions);
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, partitions| {
                keyed_state_partition_array(env, partitions, "rocksdb-session-window")
            },
        )
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
                    rocks_row_supported(&state_types)
                        && rocks_row_supported(&rocks_over_frame_value_types(
                            &value_types,
                            &kinds,
                            frame_kind as i64,
                        ))
                        && rocks_row_supported(&rocks_over_distinct_element_types(
                            &value_types,
                            &kinds,
                            frame_kind as i64,
                        ))
                        && RetainedRows::supported(&payload_types)
                }
                None => false,
            };
        supported as jboolean
    })
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
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: 0,
            shared_resources,
        };
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let value_types = read_int_array(&env, &value_types);
        let aggregate_kinds = read_int_array(&env, &aggregate_kinds);
        let value_columns = read_columns(&env, &value_columns);
        let key_columns = read_columns(&env, &key_columns);
        let proctime = proctime != 0;
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let schema = import_schema(in_schema);
        let result = lifecycle::create_over_aggregator(
            Storage { config, restore },
            OverOptions {
                value_types,
                aggregate_kinds,
                rt_column,
                value_columns,
                key_columns,
                frame_kind,
                frame_offset,
                proctime,
                key_timestamp_precisions,
                state_ttl_millis,
                now_millis,
                memory_budget_bytes,
                schema,
            },
        );
        boxed_or_throw(&mut env, result)
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
        let result = lifecycle::checkpoint_over_aggregator(
            unsafe { &mut *(handle as *mut OverWindowAggregator) },
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let result = aggregator.canonical_partitions(max_parallelism as usize, &precisions);
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, partitions| keyed_state_partition_array(env, partitions, "rocksdb-over"),
        )
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
    rank_end_column: jint,
    output_rank_number: jboolean,
    retracting: jboolean,
    generate_update_before: jboolean,
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
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: state_ttl_millis.max(0),
            shared_resources,
        };
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
        let partition_columns = read_columns(&env, &partition_columns);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let output_rank_number = output_rank_number != 0;
        let retracting = retracting != 0;
        let generate_update_before = generate_update_before != 0;
        let net_diff = net_diff != 0;
        let schema = import_schema(schema_address);
        let result = lifecycle::create_top_n_ranker(
            Storage { config, restore },
            TopNOptions {
                sort,
                partition_columns,
                key_timestamp_precisions,
                offset,
                limit,
                rank_end_column,
                output_rank_number,
                retracting,
                generate_update_before,
                net_diff,
                state_ttl_millis,
                now_millis,
                memory_budget_bytes,
                schema,
            },
        );
        boxed_or_throw(&mut env, result)
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
    offset: jlong,
    limit: jlong,
    rank_end_column: jint,
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
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: state_ttl_millis.max(0),
            shared_resources,
        };
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
        let partition_columns = read_columns(&env, &partition_columns);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let row_key_columns = read_columns(&env, &row_key_columns);
        let row_key_timestamp_precisions = read_i32_array(&env, &row_key_timestamp_precisions);
        let output_rank_number = output_rank_number != 0;
        let generate_update_before = generate_update_before != 0;
        let schema = import_schema(schema_address);
        let result = lifecycle::create_update_fast_top_n_ranker(
            Storage { config, restore },
            UpdateFastOptions {
                sort,
                partition_columns,
                key_timestamp_precisions,
                row_key_columns,
                row_key_timestamp_precisions,
                offset,
                limit,
                rank_end_column,
                output_rank_number,
                generate_update_before,
                state_ttl_millis,
                now_millis,
                memory_budget_bytes,
                schema,
            },
        );
        boxed_or_throw(&mut env, result)
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
) -> jlong {
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
        ranker.take_invalid_top_size() as jlong
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
        let result = lifecycle::checkpoint_top_n_ranker(
            unsafe { &mut *(handle as *mut RocksTopNHandle) },
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let result = ranker.canonical_partitions();
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, partitions| keyed_state_partition_array(env, partitions, "rocksdb-top-n"),
        )
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
pub extern "system" fn Java_tech_streamfusion_Native_rocksdbTopNRankerCacheStats<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jlongArray {
    crate::bridge::jni_guard(env, move |env| {
        let stats = unsafe { &*(handle as *const RocksTopNHandle) }.cache_stats();
        let output = env.new_long_array(4).expect("top-1 cache stats array");
        env.set_long_array_region(&output, 0, &stats)
            .expect("top-1 cache stats values");
        output.into_raw()
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
        RetainedRows::supported(&row_types) as jboolean
    })
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
        let config = RocksStoreConfig {
            table_dir: read_string(&mut env, &table_directory),
            max_parallelism: max_parallelism as usize,
            options_json: read_string(&mut env, &options_json),
            ttl_ms: 0,
            shared_resources,
        };
        let restored = read_restored_partitions(&mut env, &restored_partitions);
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            restored,
        );
        let sort = read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
        let partition_columns = read_columns(&env, &partition_columns);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let output_rank_number = output_rank_number != 0;
        let schema = import_schema(schema_address);
        let result = lifecycle::create_window_ranker(
            Storage { config, restore },
            WindowRankOptions {
                sort,
                window_start_col,
                window_end_col,
                partition_columns,
                key_timestamp_precisions,
                limit,
                output_rank_number,
                schema,
                memory_budget_bytes,
                restored_timer_deadline,
            },
        );
        boxed_or_throw(&mut env, result)
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
        let result = lifecycle::checkpoint_window_ranker(
            unsafe { &mut *(handle as *mut WindowRanker) },
            timer_deadline,
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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
        let result = ranker.canonical_partitions();
        runtime_result(
            &mut env,
            result,
            "RocksDB canonical snapshot failed",
            |env, partitions| keyed_state_partition_array(env, partitions, "rocksdb-window-rank"),
        )
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
        let restore = read_restore(
            &mut env,
            &source_directories,
            &source_snapshot_tokens,
            key_group_start,
            key_group_end,
            aligned,
            Vec::new(),
        );
        boxed_or_throw(
            &mut env,
            lifecycle::create_snapshot_store(Storage { config, restore }),
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
    crate::bridge::jni_guard(env, move |mut env| {
        let store = unsafe { &*(handle as *const RocksSnapshotStore) };
        let result = store.partitions();
        runtime_result(
            &mut env,
            result,
            "RocksDB restore failed",
            |env, partitions| {
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
            },
        )
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
        let result = lifecycle::checkpoint_snapshot_store(
            unsafe { &mut *(handle as *mut RocksSnapshotStore) },
            &partitions,
            timer_deadline,
            &snapshot_directory,
        );
        runtime_result(
            &mut env,
            result,
            "RocksDB checkpoint failed",
            |env, manifest| manifest_array(env, &manifest),
        )
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

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBWindowAggregatorBatches<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark: jlong,
    receiver: jni::objects::JObject<'local>,
) {
    crate::window_agg::Java_tech_streamfusion_Native_flushTumblingAggregatorBatches(
        env, _class, handle, watermark, receiver,
    )
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushRocksDBSessionAggregatorBatches<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark: jlong,
    receiver: jni::objects::JObject<'local>,
) {
    crate::session_agg::Java_tech_streamfusion_Native_flushSessionAggregatorBatches(
        env, _class, handle, watermark, receiver,
    )
}
