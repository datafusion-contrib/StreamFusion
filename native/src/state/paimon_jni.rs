//! JNI surface of the Paimon-backed group aggregate. Mirrors the memory-backed entry points; the
//! handle is a distinct Rust type (`GroupAggregator<PaimonGroupStore>`), so the two families never
//! share a symbol. Checkpointing replaces the raw keyed-state snapshot family: the barrier calls
//! `checkpointPaimonGroupAggregator`, which commits the table's snapshot and hands back the file
//! manifest the host uploads.

use crate::*;
use jni::objects::{JClass, JIntArray, JObject, JObjectArray, JString};
use jni::sys::{jboolean, jint, jlong, jobjectArray};
use jni::JNIEnv;

type PaimonGroupAggregator = GroupAggregator<PaimonGroupStore>;

fn read_string(env: &mut JNIEnv, value: &JString) -> String {
    env.get_string(value).expect("jni string").into()
}

fn throw_runtime(env: &mut JNIEnv, message: &str) {
    let _ = env.throw_new("java/lang/RuntimeException", message);
}

/// Serializes a checkpoint manifest as the host-facing string array —
/// `["<snapshot token>", "d:<data file>"…, "m:<meta file>"…]`, paths relative to the table root.
/// The token is opaque to the host (a single-table store uses its decimal Paimon snapshot id);
/// an empty token means no state was ever committed.
fn manifest_array<'local>(
    env: &mut JNIEnv<'local>,
    manifest: &PaimonCheckpointManifest,
) -> jobjectArray {
    let mut lines =
        Vec::with_capacity(1 + manifest.data_files.len() + manifest.meta_files.len());
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
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonGroupAggregator<
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
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_type_codes = read_int_array(&env, &value_types);
        let value_columns = read_int_array(&env, &value_columns);
        let filter_columns = read_int_array(&env, &filter_columns);
        let count_columns = read_int_array(&env, &count_columns);
        let distinct_view_columns = read_int_array(&env, &distinct_view_columns);
        let key_columns = read_columns(&env, &key_columns);
        let key_timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let source_snapshots: Vec<i64> = read_strings(&mut env, &source_snapshot_tokens)
            .into_iter()
            .flatten()
            .map(|token| token.parse::<i64>().expect("single-table paimon snapshot token"))
            .collect();

        let arrow_value_types: Vec<DataType> =
            value_type_codes.iter().map(|&code| value_data_type(code)).collect();
        let state_types = group_state_types(&kinds, &arrow_value_types);
        let codec = GroupStateCodec {
            kinds: kinds.clone(),
            value_types: arrow_value_types,
            state_types,
        };
        let config = PaimonStoreConfig {
            table_dir,
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format,
            file_compression: compression,
            deletion_vectors: true,
            ttl_ms: state_ttl_millis.max(0),
        };
        let store = if source_dirs.is_empty() {
            PaimonGroupStore::create(config, codec)
        } else {
            let sources: Vec<(String, i64)> =
                source_dirs.into_iter().zip(source_snapshots).collect();
            PaimonGroupStore::open_merged(config, codec, &sources, key_group_start..=key_group_end, aligned != 0, now_millis)
        };
        let aggregator = store.and_then(|store| {
            let mut base = GroupAggregator::new(
                kinds,
                value_type_codes,
                value_columns,
                key_columns,
                generate_update_before != 0,
            )
            .with_key_timestamp_precisions(key_timestamp_precisions)
            .with_filter_columns(filter_columns)
            .with_count_columns(count_columns)
            .with_record_count_column(record_count_column as i64)
            .with_distinct_view_columns(distinct_view_columns)
            .with_state_ttl(state_ttl_millis);
            if mini_batch != 0 {
                base = base.with_mini_batch();
            }
            base.with_backend(store).with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, aggregator)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_updatePaimonGroupAggregator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut PaimonGroupAggregator) };
        aggregator.store_mut().set_clock(now_millis);
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            aggregator.update(&batch, now_millis)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushPaimonGroupAggregator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut PaimonGroupAggregator) };
        match aggregator.flush_mini_batch() {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Checkpoint sync phase (task thread, at the barrier): commit and hand back the file manifest —
/// `["<snapshot id>", "d:<data file>"…, "m:<meta file>"…]`, paths relative to the table root.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointPaimonGroupAggregator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut PaimonGroupAggregator) };
        match aggregator.store_mut().checkpoint() {
            Ok(manifest) => manifest_array(&mut env, &manifest),
            Err(e) => {
                throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonGroupAggregatorStateBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let aggregator = unsafe { &*(handle as *const PaimonGroupAggregator) };
        aggregator.memory.state_bytes as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonGroupAggregatorStagingBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let aggregator = unsafe { &*(handle as *const PaimonGroupAggregator) };
        aggregator.staging_bytes() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonGroupAggregatorStagedKeys<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let aggregator = unsafe { &*(handle as *const PaimonGroupAggregator) };
        aggregator.staged_keys() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closePaimonGroupAggregator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<PaimonGroupAggregator>(handle));
        }
    })
}

type PaimonKeepLastDeduplicator = KeepLastDeduplicator<PaimonDedupStore>;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonKeepLastDeduplicator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
    row_schema_address: jlong,
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
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let row_types: Vec<DataType> = import_schema(row_schema_address)
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let source_snapshots: Vec<i64> = read_strings(&mut env, &source_snapshot_tokens)
            .into_iter()
            .flatten()
            .map(|token| token.parse::<i64>().expect("single-table paimon snapshot token"))
            .collect();

        let codec = DedupStateCodec::new(row_types, rt_column as usize, rowtime_ordered != 0);
        let config = PaimonStoreConfig {
            table_dir,
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format,
            file_compression: compression,
            deletion_vectors: true,
            ttl_ms: state_ttl_millis.max(0),
        };
        let store = if source_dirs.is_empty() {
            PaimonDedupStore::create(config, codec)
        } else {
            let sources: Vec<(String, i64)> =
                source_dirs.into_iter().zip(source_snapshots).collect();
            PaimonDedupStore::open_merged(config, codec, &sources, key_group_start..=key_group_end, aligned != 0, now_millis)
        };
        let dedup = store.and_then(|store| {
            KeepLastDeduplicator::new(
                partitions,
                rt_column as usize,
                generate_update_before != 0,
                rowtime_ordered != 0,
                keep_first != 0,
            )
            .with_generate_insert(generate_insert != 0)
            .with_mini_batch(mini_batch != 0)
            .with_compact_changes(compact_changes != 0)
            .with_key_timestamp_precisions(timestamp_precisions)
            .with_state_ttl(state_ttl_millis)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, dedup)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushPaimonKeepLastDeduplicator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut PaimonKeepLastDeduplicator) };
        dedup.store_mut().set_clock(now_millis);
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            dedup.push(&batch, now_millis)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushPaimonKeepLastDeduplicator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut PaimonKeepLastDeduplicator) };
        match dedup.flush_mini_batch() {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Checkpoint sync phase (task thread, at the barrier); see `checkpointPaimonGroupAggregator`.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointPaimonKeepLastDeduplicator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut PaimonKeepLastDeduplicator) };
        match dedup.store_mut().checkpoint() {
            Ok(manifest) => manifest_array(&mut env, &manifest),
            Err(e) => {
                throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonKeepLastDeduplicatorStateBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let dedup = unsafe { &*(handle as *const PaimonKeepLastDeduplicator) };
        dedup.memory.state_bytes as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonKeepLastDeduplicatorStagingBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let dedup = unsafe { &*(handle as *const PaimonKeepLastDeduplicator) };
        dedup.staging_bytes() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonKeepLastDeduplicatorStagedKeys<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let dedup = unsafe { &*(handle as *const PaimonKeepLastDeduplicator) };
        dedup.staged_keys() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closePaimonKeepLastDeduplicator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<PaimonKeepLastDeduplicator>(handle));
        }
    })
}

// ---------------------------------------------------------------------------------------------
// Watermark-driven keep-first dedup on the keep-first time store (dirty region + range overlay).
// ---------------------------------------------------------------------------------------------

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonKeepFirstDeduplicator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
    row_schema_address: jlong,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let row_types: Vec<DataType> = import_schema(row_schema_address)
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let source_snapshots: Vec<i64> = read_strings(&mut env, &source_snapshot_tokens)
            .into_iter()
            .flatten()
            .map(|token| token.parse::<i64>().expect("single-table paimon snapshot token"))
            .collect();

        let config = PaimonStoreConfig {
            table_dir,
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format,
            file_compression: compression,
            deletion_vectors: true,
            ttl_ms: 0,
        };
        let store = if source_dirs.is_empty() {
            PaimonKeepFirstStore::create(config, row_types)
        } else {
            let sources: Vec<(String, i64)> =
                source_dirs.into_iter().zip(source_snapshots).collect();
            PaimonKeepFirstStore::open_merged(
                config,
                row_types,
                &sources,
                key_group_start..=key_group_end,
                aligned != 0,
            )
        };
        let dedup = store.and_then(|store| {
            KeepFirstDeduplicator::new(partitions, rt_column as usize)
                .with_key_timestamp_precisions(timestamp_precisions)
                .with_backend(store)
                .with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, dedup)
    })
}

/// Buffers an input batch (no output); emission is watermark-driven (`flush`).
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushPaimonKeepFirstDeduplicator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            // No TTL clock: a TTL'd keep-first deduplicator never takes the Paimon route.
            dedup.push(&batch, 0)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Exports each key's first (minimum-rowtime) row whose rowtime the watermark has reached — the
/// overlay range read over the write buffer and the committed table.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushPaimonKeepFirstDeduplicator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
        match dedup.flush(watermark_millis, 0) {
            Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Checkpoint sync phase (task thread, at the barrier); see `checkpointPaimonGroupAggregator`.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointPaimonKeepFirstDeduplicator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let dedup = unsafe { &mut *(handle as *mut KeepFirstDeduplicator) };
        match dedup.store_mut().checkpoint() {
            Ok(manifest) => manifest_array(&mut env, &manifest),
            Err(e) => {
                throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonKeepFirstDeduplicatorStateBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let dedup = unsafe { &*(handle as *const KeepFirstDeduplicator) };
        dedup.memory.state_bytes as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closePaimonKeepFirstDeduplicator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<KeepFirstDeduplicator>(handle));
        }
    })
}

type PaimonChangelogNormalizer = ChangelogNormalizer<PaimonNormalizerStore>;

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonChangelogNormalizer<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    key_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    row_schema_address: jlong,
    generate_update_before: jboolean,
    mini_batch: jboolean,
    state_ttl_millis: jlong,
    now_millis: jlong,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let keys = read_columns(&env, &key_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let row_types: Vec<DataType> = import_schema(row_schema_address)
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let source_snapshots: Vec<i64> = read_strings(&mut env, &source_snapshot_tokens)
            .into_iter()
            .flatten()
            .map(|token| token.parse::<i64>().expect("single-table paimon snapshot token"))
            .collect();

        let codec = NormalizerStateCodec::new(row_types);
        let config = PaimonStoreConfig {
            table_dir,
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format,
            file_compression: compression,
            deletion_vectors: true,
            ttl_ms: state_ttl_millis.max(0),
        };
        let store = if source_dirs.is_empty() {
            PaimonNormalizerStore::create(config, codec)
        } else {
            let sources: Vec<(String, i64)> =
                source_dirs.into_iter().zip(source_snapshots).collect();
            PaimonNormalizerStore::open_merged(config, codec, &sources, key_group_start..=key_group_end, aligned != 0, now_millis)
        };
        let normalizer = store.and_then(|store| {
            ChangelogNormalizer::new(keys, generate_update_before != 0)
                .with_mini_batch(mini_batch != 0)
                .with_key_timestamp_precisions(timestamp_precisions)
                .with_state_ttl(state_ttl_millis)
                .with_backend(store)
                .with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, normalizer)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushPaimonChangelogNormalizer<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let normalizer = unsafe { &mut *(handle as *mut PaimonChangelogNormalizer) };
        normalizer.store_mut().set_clock(now_millis);
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            normalizer.push(&batch, now_millis)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushPaimonChangelogNormalizer<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let normalizer = unsafe { &mut *(handle as *mut PaimonChangelogNormalizer) };
        match normalizer.flush_mini_batch() {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Checkpoint sync phase (task thread, at the barrier); see `checkpointPaimonGroupAggregator`.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointPaimonChangelogNormalizer<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let normalizer = unsafe { &mut *(handle as *mut PaimonChangelogNormalizer) };
        match normalizer.store_mut().checkpoint() {
            Ok(manifest) => manifest_array(&mut env, &manifest),
            Err(e) => {
                throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonChangelogNormalizerStateBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let normalizer = unsafe { &*(handle as *const PaimonChangelogNormalizer) };
        normalizer.memory.state_bytes as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonChangelogNormalizerStagingBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let normalizer = unsafe { &*(handle as *const PaimonChangelogNormalizer) };
        normalizer.staging_bytes() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonChangelogNormalizerStagedKeys<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let normalizer = unsafe { &*(handle as *const PaimonChangelogNormalizer) };
        normalizer.staged_keys() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closePaimonChangelogNormalizer<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<PaimonChangelogNormalizer>(handle));
        }
    })
}

/// The Paimon-backed Top-N handle: append-only (bounded buffer) or retracting (full buffer, so a
/// retracted top row's successor can be promoted), both on the same list store and codec — the
/// buffer shape is identical, only its growth policy differs — plus update-fast (unique-keyed
/// changelog without retractions) on the row-keyed map shape.
enum PaimonTopNRanker {
    Append(TopNRanker<PaimonTopNStore>),
    Retract(RetractableTopNRanker<PaimonTopNStore>),
    UpdateFast(UpdatableTopNRanker<PaimonUpdatableTopNStore>),
}

impl PaimonTopNRanker {
    fn set_clock(&mut self, now_ms: i64) {
        match self {
            PaimonTopNRanker::Append(r) => r.store_mut().set_clock(now_ms),
            PaimonTopNRanker::Retract(r) => r.store_mut().set_clock(now_ms),
            PaimonTopNRanker::UpdateFast(r) => r.store_mut().set_clock(now_ms),
        }
    }

    fn push(&mut self, batch: &RecordBatch, now_ms: i64) -> Result<RecordBatch, DataFusionError> {
        match self {
            PaimonTopNRanker::Append(r) => r.push(batch, now_ms),
            PaimonTopNRanker::Retract(r) => r.push(batch, now_ms),
            PaimonTopNRanker::UpdateFast(r) => r.push(batch, now_ms),
        }
    }

    fn flush(&mut self) -> RecordBatch {
        match self {
            PaimonTopNRanker::Append(r) => r.flush_net_diff(),
            PaimonTopNRanker::Retract(r) => r.flush_net_diff(),
            // The update-fast ranker has no net-diff staging; every push emitted its diff already.
            PaimonTopNRanker::UpdateFast(_) => RecordBatch::new_empty(Arc::new(Schema::empty())),
        }
    }

    fn checkpoint(&mut self) -> Result<PaimonCheckpointManifest, DataFusionError> {
        match self {
            PaimonTopNRanker::Append(r) => r.store_mut().checkpoint(),
            PaimonTopNRanker::Retract(r) => r.store_mut().checkpoint(),
            PaimonTopNRanker::UpdateFast(r) => r.store_mut().checkpoint(),
        }
    }

    fn state_bytes(&self) -> usize {
        match self {
            PaimonTopNRanker::Append(r) => r.memory.state_bytes,
            PaimonTopNRanker::Retract(r) => r.memory.state_bytes,
            PaimonTopNRanker::UpdateFast(r) => r.memory.state_bytes,
        }
    }

    fn staging_bytes(&self) -> usize {
        match self {
            PaimonTopNRanker::Append(r) => r.staging_bytes(),
            PaimonTopNRanker::Retract(r) => r.staging_bytes(),
            PaimonTopNRanker::UpdateFast(_) => 0, // no net-diff staging
        }
    }

    fn staged_partitions(&self) -> usize {
        match self {
            PaimonTopNRanker::Append(r) => r.staged_partitions(),
            PaimonTopNRanker::Retract(r) => r.staged_partitions(),
            PaimonTopNRanker::UpdateFast(_) => 0, // no net-diff staging
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonTopNRanker<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    row_schema_address: jlong,
    offset: jlong,
    limit: jlong,
    output_rank_number: jboolean,
    retracting: jboolean,
    net_diff: jboolean,
    state_ttl_millis: jlong,
    now_millis: jlong,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
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
        let row_types: Vec<DataType> = import_schema(row_schema_address)
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let source_snapshots: Vec<i64> = read_strings(&mut env, &source_snapshot_tokens)
            .into_iter()
            .flatten()
            .map(|token| token.parse::<i64>().expect("single-table paimon snapshot token"))
            .collect();

        let codec = TopNStateCodec::new(row_types, sort.clone());
        // Hydrated rows must come from the SAME converter instances the operator emits with
        // (arrow-row rejects rows decoded by a different converter), so the ranker's converters are
        // built from — and share — the codec's.
        let converters = TopNConverters::from_codec(&codec, &partitions);
        let config = PaimonStoreConfig {
            table_dir,
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format,
            file_compression: compression,
            deletion_vectors: true,
            ttl_ms: state_ttl_millis.max(0),
        };
        let store = if source_dirs.is_empty() {
            PaimonTopNStore::create(config, codec)
        } else {
            let sources: Vec<(String, i64)> =
                source_dirs.into_iter().zip(source_snapshots).collect();
            PaimonTopNStore::open_merged(config, codec, &sources, key_group_start..=key_group_end, aligned != 0, now_millis)
        };
        let ranker = store.and_then(|store| {
            if retracting != 0 {
                RetractableTopNRanker::new(partitions, sort, offset, limit, output_rank_number != 0)
                    .with_key_timestamp_precisions(timestamp_precisions)
                    .with_net_diff(net_diff != 0)
                    .with_state_ttl(state_ttl_millis)
                    .with_converters(converters)
                    .with_backend(store)
                    .with_read_through_budget(memory_budget_bytes)
                    .map(PaimonTopNRanker::Retract)
            } else {
                // The append-only ranker is the no-OFFSET path (offset always 0).
                TopNRanker::new(partitions, sort, limit, output_rank_number != 0, net_diff != 0)
                    .with_key_timestamp_precisions(timestamp_precisions)
                    .with_state_ttl(state_ttl_millis)
                    .with_converters(converters)
                    .with_backend(store)
                    .with_read_through_budget(memory_budget_bytes)
                    .map(PaimonTopNRanker::Append)
            }
        });
        boxed_or_throw(&mut env, ranker)
    })
}

/// `createUpdateFastTopNRanker` on the Paimon state backend: the row-keyed map shape (see
/// `PaimonUpdatableTopNStore`), served by the same push/flush/checkpoint/close entry points as
/// the other Paimon-backed rankers.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonUpdateFastTopNRanker<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    row_key_columns: JIntArray<'local>,
    row_key_timestamp_precisions: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    row_schema_address: jlong,
    limit: jlong,
    output_rank_number: jboolean,
    state_ttl_millis: jlong,
    now_millis: jlong,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
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
        let row_types: Vec<DataType> = import_schema(row_schema_address)
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let source_snapshots: Vec<i64> = read_strings(&mut env, &source_snapshot_tokens)
            .into_iter()
            .flatten()
            .map(|token| token.parse::<i64>().expect("single-table paimon snapshot token"))
            .collect();

        let codec = TopNStateCodec::new(row_types, sort.clone());
        // See createPaimonTopNRanker: the ranker's converters are built from — and share — the
        // codec's, so hydrated rows and operator-built rows are interchangeable.
        let converters = TopNConverters::from_codec(&codec, &partitions);
        let config = PaimonStoreConfig {
            table_dir,
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format,
            file_compression: compression,
            deletion_vectors: true,
            ttl_ms: state_ttl_millis.max(0),
        };
        let store = if source_dirs.is_empty() {
            PaimonUpdatableTopNStore::create(config, codec)
        } else {
            let sources: Vec<(String, i64)> =
                source_dirs.into_iter().zip(source_snapshots).collect();
            PaimonUpdatableTopNStore::open_merged(
                config,
                codec,
                &sources,
                key_group_start..=key_group_end,
                aligned != 0,
                now_millis,
            )
        };
        let ranker = store.and_then(|store| {
            UpdatableTopNRanker::new(
                partitions,
                partition_precisions,
                row_keys,
                row_key_precisions,
                sort,
                limit,
                output_rank_number != 0,
            )
            .with_state_ttl(state_ttl_millis)
            .with_converters(converters)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)
            .map(PaimonTopNRanker::UpdateFast)
        });
        boxed_or_throw(&mut env, ranker)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushPaimonTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut PaimonTopNRanker) };
        ranker.set_clock(now_millis);
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            ranker.push(&batch, now_millis)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushPaimonTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        let ranker = unsafe { &mut *(handle as *mut PaimonTopNRanker) };
        let out = ranker.flush();
        export_record_batch(out, out_array_address, out_schema_address);
    })
}

/// Checkpoint sync phase (task thread, at the barrier); see `checkpointPaimonGroupAggregator`.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointPaimonTopNRanker<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut PaimonTopNRanker) };
        match ranker.checkpoint() {
            Ok(manifest) => manifest_array(&mut env, &manifest),
            Err(e) => {
                throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonTopNRankerStateBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let ranker = unsafe { &*(handle as *const PaimonTopNRanker) };
        ranker.state_bytes() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonTopNRankerStagingBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let ranker = unsafe { &*(handle as *const PaimonTopNRanker) };
        ranker.staging_bytes() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonTopNRankerStagedKeys<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let ranker = unsafe { &*(handle as *const PaimonTopNRanker) };
        ranker.staged_partitions() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closePaimonTopNRanker<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<PaimonTopNRanker>(handle));
        }
    })
}

type PaimonUpdatingJoiner = UpdatingJoiner<PaimonJoinStore>;

/// Parses one restored two-table token — `"<left id>:<right id>"`, either id `-1` when that side
/// had never committed.
fn parse_join_token(token: &str) -> (i64, i64) {
    let (left, right) = token.split_once(':').expect("two-table paimon snapshot token");
    (
        left.parse::<i64>().expect("left paimon snapshot id"),
        right.parse::<i64>().expect("right paimon snapshot id"),
    )
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonUpdatingJoiner<
    'local,
>(
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
    mini_batch: jboolean,
    left_state_ttl_millis: jlong,
    right_state_ttl_millis: jlong,
    now_millis: jlong,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let left = read_columns(&env, &left_keys);
        let right = read_columns(&env, &right_keys);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
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
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let source_tokens: Vec<String> = read_strings(&mut env, &source_snapshot_tokens)
            .into_iter()
            .flatten()
            .collect();

        // One table per side under the operator's state directory; each side restores independently
        // from whichever sources ever committed it. Each side's table carries its OWN retention
        // (the STATE_TTL hint sets the sides independently), so per-entry expiry at hydration
        // runs under that side's clock rule.
        let side_config = |side: &str, ttl_ms: jlong| PaimonStoreConfig {
            table_dir: format!("{table_dir}/{side}"),
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format.clone(),
            file_compression: compression.clone(),
            deletion_vectors: true,
            ttl_ms: ttl_ms.max(0),
        };
        let side_store = |side: &str, schema: &SchemaRef, ttl_ms: jlong, pick: fn(&str) -> i64| {
            let codec = JoinStateCodec::new(schema);
            let sources: Vec<(String, i64)> = source_dirs
                .iter()
                .zip(source_tokens.iter())
                .filter_map(|(dir, token)| {
                    let id = pick(token);
                    (id >= 0).then(|| (format!("{dir}/{side}"), id))
                })
                .collect();
            if sources.is_empty() {
                PaimonJoinStore::create(side_config(side, ttl_ms), codec)
            } else {
                PaimonJoinStore::open_merged(
                    side_config(side, ttl_ms),
                    codec,
                    &sources,
                    key_group_start..=key_group_end,
                    aligned != 0,
                    now_millis,
                )
            }
        };
        let left_store =
            side_store("left", &left_schema, left_state_ttl_millis, |t| parse_join_token(t).0);
        let right_store =
            side_store("right", &right_schema, right_state_ttl_millis, |t| parse_join_token(t).1);
        let joiner = left_store.and_then(|left_store| {
            let right_store = right_store?;
            UpdatingJoiner::new(
                left,
                right,
                JoinKind::from_code(join_type),
                left_schema,
                right_schema,
                predicate,
            )
            .with_mini_batch(mini_batch != 0)
            .with_key_timestamp_precisions(timestamp_precisions)
            .with_state_ttl(left_state_ttl_millis, right_state_ttl_millis)
            .with_backend(left_store, right_store)
            .with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, joiner)
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushLeftPaimonUpdatingJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut PaimonUpdatingJoiner) };
        // A push hydrates BOTH sides (the input folds into its store, the probe reads the other),
        // so both stores take this call's clock.
        let (left_store, right_store) = joiner.stores_mut();
        left_store.set_clock(now_millis);
        right_store.set_clock(now_millis);
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            joiner.push(&batch, true, now_millis)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRightPaimonUpdatingJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut PaimonUpdatingJoiner) };
        // See pushLeftPaimonUpdatingJoiner: both stores take this call's clock.
        let (left_store, right_store) = joiner.stores_mut();
        left_store.set_clock(now_millis);
        right_store.set_clock(now_millis);
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            joiner.push(&batch, false, now_millis)
        };
        match result {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushPaimonUpdatingJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut PaimonUpdatingJoiner) };
        match joiner.flush_mini_batch() {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Checkpoint sync phase (task thread, at the barrier): commits BOTH side tables and hands back
/// one merged manifest — token `"<left id>:<right id>"` (empty when neither side ever committed),
/// file paths prefixed `left/` / `right/` relative to the operator's state directory.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointPaimonUpdatingJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut PaimonUpdatingJoiner) };
        let (left_store, right_store) = joiner.stores_mut();
        let manifests = left_store
            .checkpoint()
            .and_then(|left| Ok((left, right_store.checkpoint()?)));
        match manifests {
            Ok((left, right)) => {
                let token = if left.snapshot_id < 0 && right.snapshot_id < 0 {
                    String::new()
                } else {
                    format!("{}:{}", left.snapshot_id, right.snapshot_id)
                };
                let mut lines = Vec::with_capacity(
                    1 + left.data_files.len()
                        + left.meta_files.len()
                        + right.data_files.len()
                        + right.meta_files.len(),
                );
                lines.push(token);
                lines.extend(left.data_files.iter().map(|f| format!("d:left/{f}")));
                lines.extend(right.data_files.iter().map(|f| format!("d:right/{f}")));
                lines.extend(left.meta_files.iter().map(|f| format!("m:left/{f}")));
                lines.extend(right.meta_files.iter().map(|f| format!("m:right/{f}")));
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
            Err(e) => {
                throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonUpdatingJoinerStateBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let joiner = unsafe { &*(handle as *const PaimonUpdatingJoiner) };
        joiner.memory.state_bytes as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonUpdatingJoinerStagingBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let joiner = unsafe { &*(handle as *const PaimonUpdatingJoiner) };
        joiner.staging_bytes() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonUpdatingJoinerStagedKeys<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let joiner = unsafe { &*(handle as *const PaimonUpdatingJoiner) };
        joiner.staged_keys() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closePaimonUpdatingJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<PaimonUpdatingJoiner>(handle));
        }
    })
}

// ---------------------------------------------------------------------------------------------
// Window Top-N / window dedup on the window-rank time store (dirty region + range overlay). The
// snapshot token carries the watermark alongside the snapshot id ("<snapshot>:<watermark>") —
// the memory path persists the watermark in its raw snapshot, and without it a restored subtask
// would re-buffer replayed rows of already-fired windows and emit them twice.
// ---------------------------------------------------------------------------------------------

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonWindowRanker<
    'local,
>(
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
    row_schema_address: jlong,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let partitions = read_columns(&env, &partition_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let sort =
            crate::topn::read_sort_columns(&env, &sort_indices, &sort_ascending, &sort_nulls_first);
        let row_types: Vec<DataType> = import_schema(row_schema_address)
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let mut watermark = i64::MIN;
        let source_snapshots: Vec<i64> = read_strings(&mut env, &source_snapshot_tokens)
            .into_iter()
            .flatten()
            .map(|token| {
                let (snapshot, wm) =
                    token.split_once(':').expect("window-rank paimon snapshot token");
                watermark = watermark
                    .max(wm.parse::<i64>().expect("window-rank paimon watermark"));
                snapshot.parse::<i64>().expect("window-rank paimon snapshot id")
            })
            .collect();

        let config = PaimonStoreConfig {
            table_dir,
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format,
            file_compression: compression,
            deletion_vectors: true,
            ttl_ms: 0,
        };
        let store = if source_dirs.is_empty() {
            PaimonWindowRankStore::create(config, row_types)
        } else {
            let sources: Vec<(String, i64)> =
                source_dirs.into_iter().zip(source_snapshots).collect();
            PaimonWindowRankStore::open_merged(
                config,
                row_types,
                &sources,
                key_group_start..=key_group_end,
                aligned != 0,
            )
        };
        let ranker = store.and_then(|store| {
            let mut ranker = crate::topn::WindowRanker::new(
                window_start_col as usize,
                window_end_col as usize,
                partitions,
                sort,
                limit,
                output_rank_number != 0,
            )
            .with_key_timestamp_precisions(timestamp_precisions)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)?;
            ranker.current_watermark = watermark;
            Ok(ranker)
        });
        boxed_or_throw(&mut env, ranker)
    })
}

/// Buffers an input batch (no output); emission is watermark-driven (`flush`).
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushPaimonWindowRanker<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut crate::topn::WindowRanker) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            ranker.push(&batch)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Exports every closed window's top-N rows — the overlay range read over the write buffer and
/// the committed table.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushPaimonWindowRanker<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut crate::topn::WindowRanker) };
        match ranker.flush(watermark_millis) {
            Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Checkpoint sync phase (task thread, at the barrier); the token line packs the watermark.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointPaimonWindowRanker<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let ranker = unsafe { &mut *(handle as *mut crate::topn::WindowRanker) };
        let watermark = ranker.current_watermark;
        match ranker.store_mut().checkpoint() {
            Ok(manifest) => {
                let token = if manifest.snapshot_id < 0 {
                    String::new()
                } else {
                    format!("{}:{}", manifest.snapshot_id, watermark)
                };
                let mut lines = Vec::with_capacity(
                    1 + manifest.data_files.len() + manifest.meta_files.len(),
                );
                lines.push(token);
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
            Err(e) => {
                throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonWindowRankerStateBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let ranker = unsafe { &*(handle as *const crate::topn::WindowRanker) };
        ranker.memory.state_bytes as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closePaimonWindowRanker<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<crate::topn::WindowRanker>(handle));
        }
    })
}

// ---------------------------------------------------------------------------------------------
// Event-time OVER on the over store: pending rows (dirty region + range overlay), per-key fold
// state (point-access dirty slots), and — with retention on — the per-key cleanup deadlines. The
// snapshot token packs the snapshot ids and the arrival sequence
// ("<pending>:<folds>:<seq>:<deadlines>") — the sequence orders pending rows across a restore;
// without it a restored subtask's new rows would emit ahead of older buffered rows.
// ---------------------------------------------------------------------------------------------

/// Parses one restored over-store token — any id `-1` when that table had never committed. The
/// deadlines field is optional: a pre-retention token has three fields, and its absence IS the
/// enable-flip signal (no deadlines table to restore).
fn parse_over_token(token: &str) -> (i64, i64, i64, i64) {
    let fields: Vec<i64> = token
        .split(':')
        .map(|field| field.parse::<i64>().expect("over paimon token field"))
        .collect();
    assert!(fields.len() >= 3, "over paimon snapshot token");
    (fields[0], fields[1], fields[2], fields.get(3).copied().unwrap_or(-1))
}

/// True when this OVER instance's whole state shape is persistable: a watermark-driven fold
/// (rowtime, unbounded RANGE frame or pure window functions) whose payload row and fold-state
/// columns all sit in the backend's type map.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonOverStateSupported<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    row_schema_address: jlong,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    frame_kind: jint,
    proctime: jboolean,
) -> jboolean {
    crate::bridge::jni_guard(env, move |env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_type_codes = read_int_array(&env, &value_types);
        let payload_types: Vec<DataType> = import_schema(row_schema_address)
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let Some(state_types) = crate::over_agg::paimon_over_state_types(
            &value_type_codes,
            &kinds,
            frame_kind as i64,
            proctime != 0,
        ) else {
            return 0;
        };
        (paimon_row_supported(&payload_types) && paimon_row_supported(&state_types)) as jboolean
    })
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonOverAggregator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    rt_column: jint,
    value_columns: JIntArray<'local>,
    key_columns: JIntArray<'local>,
    frame_kind: jint,
    frame_offset: jlong,
    key_timestamp_precisions: JIntArray<'local>,
    row_schema_address: jlong,
    state_ttl_millis: jlong,
    now_millis: jlong,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_type_codes = read_int_array(&env, &value_types);
        let values = read_columns(&env, &value_columns);
        let keys = read_columns(&env, &key_columns);
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let payload_types: Vec<DataType> = import_schema(row_schema_address)
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let state_types = crate::over_agg::paimon_over_state_types(
            &value_type_codes,
            &kinds,
            frame_kind as i64,
            false,
        );
        let Some(state_types) = state_types else {
            throw_runtime(&mut env, "over shape not persistable on the paimon backend");
            return 0;
        };
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        // Flink's exact cleaning enablement (`minRetentionTime > 1`): the deadlines table exists
        // only while cleaning is on, so a retention-off restore sheds any restored deadlines.
        let retention = state_ttl_millis > 1;
        let mut next_seq = 0i64;
        let mut pending_sources: Vec<(String, i64)> = Vec::new();
        let mut fold_sources: Vec<(String, i64)> = Vec::new();
        let mut deadline_sources: Vec<(String, i64)> = Vec::new();
        for (dir, token) in source_dirs.iter().zip(
            read_strings(&mut env, &source_snapshot_tokens)
                .into_iter()
                .flatten(),
        ) {
            let (pending_id, folds_id, seq, deadlines_id) = parse_over_token(&token);
            next_seq = next_seq.max(seq);
            if pending_id >= 0 {
                pending_sources.push((format!("{dir}/pending"), pending_id));
            }
            if folds_id >= 0 {
                fold_sources.push((format!("{dir}/folds"), folds_id));
            }
            if retention && deadlines_id >= 0 {
                deadline_sources.push((format!("{dir}/deadlines"), deadlines_id));
            }
        }

        let config = PaimonStoreConfig {
            table_dir,
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format,
            file_compression: compression,
            deletion_vectors: true,
            ttl_ms: 0,
        };
        let store = if source_dirs.is_empty() {
            PaimonOverStore::create(config, payload_types, state_types, retention)
        } else {
            PaimonOverStore::open_merged(
                config,
                payload_types,
                state_types,
                &pending_sources,
                &fold_sources,
                &deadline_sources,
                retention,
                key_group_start..=key_group_end,
                aligned != 0,
            )
        };
        let aggregator = store.and_then(|mut store| {
            store.set_next_seq(next_seq);
            crate::over_agg::OverWindowAggregator::new(
                value_type_codes,
                kinds,
                rt_column as usize,
                values,
                keys,
                frame_kind as i64,
                frame_offset,
                false,
            )
            .with_key_timestamp_precisions(timestamp_precisions)
            .with_state_retention(state_ttl_millis)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)
        });
        let aggregator = aggregator.and_then(|mut aggregator| {
            if !source_dirs.is_empty() {
                aggregator.hydrate_backend_retention(now_millis)?;
            }
            Ok(aggregator)
        });
        boxed_or_throw(&mut env, aggregator)
    })
}

/// Buffers an input batch into pending state (no output); emission is watermark-driven.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushPaimonOverAggregator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    now_millis: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut crate::over_agg::OverWindowAggregator) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            aggregator.push(batch, now_millis)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Exports the rows the watermark completed (input columns + running aggregates) — the overlay
/// range read over the pending write buffer and the committed table.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushPaimonOverAggregator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    now_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut crate::over_agg::OverWindowAggregator) };
        match aggregator.flush(watermark_millis, now_millis) {
            Ok(result) => export_record_batch(result, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Checkpoint sync phase (task thread, at the barrier): commits every table; the token line
/// packs the snapshot ids and the arrival sequence.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointPaimonOverAggregator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut crate::over_agg::OverWindowAggregator) };
        let store = aggregator.store_mut();
        let next_seq = store.next_seq();
        match store.checkpoint() {
            Ok((pending, folds, deadlines)) => {
                let token = if pending.snapshot_id < 0
                    && folds.snapshot_id < 0
                    && deadlines.snapshot_id < 0
                {
                    String::new()
                } else {
                    format!(
                        "{}:{}:{}:{}",
                        pending.snapshot_id, folds.snapshot_id, next_seq, deadlines.snapshot_id
                    )
                };
                let mut lines = Vec::with_capacity(
                    1 + pending.data_files.len()
                        + pending.meta_files.len()
                        + folds.data_files.len()
                        + folds.meta_files.len()
                        + deadlines.data_files.len()
                        + deadlines.meta_files.len(),
                );
                lines.push(token);
                lines.extend(pending.data_files.iter().map(|f| format!("d:pending/{f}")));
                lines.extend(folds.data_files.iter().map(|f| format!("d:folds/{f}")));
                lines.extend(deadlines.data_files.iter().map(|f| format!("d:deadlines/{f}")));
                lines.extend(pending.meta_files.iter().map(|f| format!("m:pending/{f}")));
                lines.extend(folds.meta_files.iter().map(|f| format!("m:folds/{f}")));
                lines.extend(deadlines.meta_files.iter().map(|f| format!("m:deadlines/{f}")));
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
            Err(e) => {
                throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonOverAggregatorStateBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let aggregator = unsafe { &*(handle as *const crate::over_agg::OverWindowAggregator) };
        aggregator.memory.state_bytes as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closePaimonOverAggregator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<crate::over_agg::OverWindowAggregator>(handle));
        }
    })
}

// ---------------------------------------------------------------------------------------------
// Event-time window join on two row-buffer tables (left/, right/). The snapshot token packs
// both snapshot ids and both arrival sequences ("<left>:<right>:<lseq>:<rseq>") — the sequences
// keep each side's emission order across a restore.
// ---------------------------------------------------------------------------------------------

/// Parses one restored window-join token — either id `-1` when that side had never committed.
fn parse_window_join_token(token: &str) -> (i64, i64, i64, i64) {
    let mut parts = token.splitn(4, ':');
    let mut next = || {
        parts
            .next()
            .expect("window-join paimon snapshot token")
            .parse::<i64>()
            .expect("window-join paimon token field")
    };
    (next(), next(), next(), next())
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonWindowJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
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
    key_timestamp_precisions: JIntArray<'local>,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
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
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let mut left_seq = 0i64;
        let mut right_seq = 0i64;
        let mut left_sources: Vec<(String, i64)> = Vec::new();
        let mut right_sources: Vec<(String, i64)> = Vec::new();
        for (dir, token) in source_dirs.iter().zip(
            read_strings(&mut env, &source_snapshot_tokens)
                .into_iter()
                .flatten(),
        ) {
            let (left_id, right_id, lseq, rseq) = parse_window_join_token(&token);
            left_seq = left_seq.max(lseq);
            right_seq = right_seq.max(rseq);
            if left_id >= 0 {
                left_sources.push((format!("{dir}/left"), left_id));
            }
            if right_id >= 0 {
                right_sources.push((format!("{dir}/right"), right_id));
            }
        }

        let left_types: Vec<DataType> =
            left_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let right_types: Vec<DataType> =
            right_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let config = PaimonStoreConfig {
            table_dir,
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format,
            file_compression: compression,
            deletion_vectors: true,
            ttl_ms: 0,
        };
        let store = if source_dirs.is_empty() {
            PaimonWindowJoinStore::create(config, left_types, right_types)
        } else {
            PaimonWindowJoinStore::open_merged(
                config,
                left_types,
                right_types,
                &left_sources,
                &right_sources,
                key_group_start..=key_group_end,
                aligned != 0,
            )
        };
        let joiner = store.and_then(|mut store| {
            store.left.set_next_seq(left_seq);
            store.right.set_next_seq(right_seq);
            crate::window_join::WindowJoiner::new(
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
            .with_key_timestamp_precisions(timestamp_precisions)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, joiner)
    })
}

/// Buffers a left batch into pending state (no output); emission is watermark-driven.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushLeftPaimonWindowJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut crate::window_join::WindowJoiner) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            joiner.push_left(batch)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Buffers a right batch into pending state (no output).
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_pushRightPaimonWindowJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut crate::window_join::WindowJoiner) };
        // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
        let result = {
            let batch = import_record_batch(in_array_address, in_schema_address);
            joiner.push_right(batch)
        };
        if let Err(e) = result {
            throw_memory_limit(&mut env, &e.to_string());
        }
    })
}

/// Exports the matches of every window the watermark closed — each side's overlay range read
/// feeding the join, evicting the fired rows.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_flushPaimonWindowJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    watermark_millis: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut crate::window_join::WindowJoiner) };
        match joiner.flush(watermark_millis) {
            Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
            Err(e) => throw_memory_limit(&mut env, &e.to_string()),
        }
    })
}

/// Checkpoint sync phase (task thread, at the barrier): commits both side tables; the token line
/// packs both snapshot ids and both arrival sequences.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointPaimonWindowJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut crate::window_join::WindowJoiner) };
        let store = joiner.store_mut();
        let (left_seq, right_seq) = (store.left.next_seq(), store.right.next_seq());
        match store.checkpoint() {
            Ok((left, right)) => {
                let token = if left.snapshot_id < 0 && right.snapshot_id < 0 {
                    String::new()
                } else {
                    format!("{}:{}:{}:{}", left.snapshot_id, right.snapshot_id, left_seq, right_seq)
                };
                let mut lines = Vec::with_capacity(
                    1 + left.data_files.len()
                        + left.meta_files.len()
                        + right.data_files.len()
                        + right.meta_files.len(),
                );
                lines.push(token);
                lines.extend(left.data_files.iter().map(|f| format!("d:left/{f}")));
                lines.extend(right.data_files.iter().map(|f| format!("d:right/{f}")));
                lines.extend(left.meta_files.iter().map(|f| format!("m:left/{f}")));
                lines.extend(right.meta_files.iter().map(|f| format!("m:right/{f}")));
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
            Err(e) => {
                throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonWindowJoinerStateBytes<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    crate::bridge::jni_guard(env, move |_env| {
        let joiner = unsafe { &*(handle as *const crate::window_join::WindowJoiner) };
        joiner.memory.state_bytes as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closePaimonWindowJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| {
        unsafe {
            drop(from_handle::<crate::window_join::WindowJoiner>(handle));
        }
    })
}

// ---------------------------------------------------------------------------------------------
// Aligned window aggregates (tumbling / hopping / cumulative) on the window-agg store. The
// snapshot token carries the watermark alongside the snapshot id ("<snapshot>:<watermark>") —
// the memory path persists it in its raw snapshot metadata, and without it a restored subtask
// would stop dropping late rows.
// ---------------------------------------------------------------------------------------------

/// The Arrow type a window-aggregate key column arrives in, from the host's key-type code: the
/// bridge widens int keys to int64 and carries timestamp keys as int64 nanoseconds.
fn window_key_data_type(code: i64) -> DataType {
    match code {
        3 => DataType::Utf8,
        7 => DataType::Boolean,
        8 => DataType::Date32,
        c if c >= 2000 => {
            DataType::Decimal128(((c - 2000) / 100) as u8, ((c - 2000) % 100) as i8)
        }
        _ => DataType::Int64,
    }
}

/// True when this window aggregate's whole persisted shape — the key columns and every
/// accumulator's state fields — sits in the backend's type map.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_paimonWindowAggStateSupported<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    value_types: JIntArray<'local>,
    aggregate_kinds: JIntArray<'local>,
    key_types: JIntArray<'local>,
) -> jboolean {
    crate::bridge::jni_guard(env, move |env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_type_codes = read_int_array(&env, &value_types);
        let key_data_types: Vec<DataType> = read_int_array(&env, &key_types)
            .into_iter()
            .map(window_key_data_type)
            .collect();
        let state_types: Vec<DataType> = build_aggregates(&kinds, &value_type_codes)
            .iter()
            .flat_map(|a| a.state_fields().into_iter().map(|f| f.data_type().clone()))
            .collect();
        (paimon_row_supported(&key_data_types) && paimon_row_supported(&state_types)) as jboolean
    })
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonTumblingAggregator<
    'local,
>(
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
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_type_codes = read_int_array(&env, &value_types);
        let key_data_types: Vec<DataType> = read_int_array(&env, &key_types)
            .into_iter()
            .map(window_key_data_type)
            .collect();
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let state_types: Vec<DataType> = build_aggregates(&kinds, &value_type_codes)
            .iter()
            .flat_map(|a| a.state_fields().into_iter().map(|f| f.data_type().clone()))
            .collect();
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let mut watermark = i64::MIN;
        let source_snapshots: Vec<i64> = read_strings(&mut env, &source_snapshot_tokens)
            .into_iter()
            .flatten()
            .map(|token| {
                let (snapshot, wm) =
                    token.split_once(':').expect("window-agg paimon snapshot token");
                watermark = watermark
                    .max(wm.parse::<i64>().expect("window-agg paimon watermark"));
                snapshot.parse::<i64>().expect("window-agg paimon snapshot id")
            })
            .collect();

        let config = PaimonStoreConfig {
            table_dir,
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format,
            file_compression: compression,
            deletion_vectors: true,
            ttl_ms: 0,
        };
        let store = if source_dirs.is_empty() {
            PaimonWindowAggStore::create(config, key_data_types, state_types)
        } else {
            let sources: Vec<(String, i64)> =
                source_dirs.into_iter().zip(source_snapshots).collect();
            PaimonWindowAggStore::open_merged(
                config,
                key_data_types,
                state_types,
                &sources,
                key_group_start..=key_group_end,
                aligned != 0,
            )
        };
        let aggregator = store.and_then(|store| {
            let mut aggregator = crate::window_agg::TumblingAggregator::new(
                window_millis,
                slide_millis,
                cumulative != 0,
                value_type_codes,
                kinds,
            )
            .with_key_timestamp_precisions(timestamp_precisions)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)?;
            aggregator.set_current_watermark(watermark);
            Ok(aggregator)
        });
        boxed_or_throw(&mut env, aggregator)
    })
}

/// Checkpoint sync phase (task thread, at the barrier): stages the open windows, commits the
/// table; the token line packs the watermark.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointPaimonTumblingAggregator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut crate::window_agg::TumblingAggregator) };
        match aggregator.checkpoint_backend() {
            Ok((manifest, watermark)) => {
                let token = if manifest.snapshot_id < 0 {
                    String::new()
                } else {
                    format!("{}:{}", manifest.snapshot_id, watermark)
                };
                let mut lines = Vec::with_capacity(
                    1 + manifest.data_files.len() + manifest.meta_files.len(),
                );
                lines.push(token);
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
            Err(e) => {
                throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

// ---------------------------------------------------------------------------------------------
// Session-window aggregates on the session store. Single table, no persisted watermark (the
// memory path keeps none), so the token is the plain snapshot id.
// ---------------------------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonSessionAggregator<
    'local,
>(
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
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
        let kinds = read_int_array(&env, &aggregate_kinds);
        let value_type_codes = read_int_array(&env, &value_types);
        let key_data_types: Vec<DataType> = read_int_array(&env, &key_types)
            .into_iter()
            .map(window_key_data_type)
            .collect();
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let state_types: Vec<DataType> = build_aggregates(&kinds, &value_type_codes)
            .iter()
            .flat_map(|a| a.state_fields().into_iter().map(|f| f.data_type().clone()))
            .collect();
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let source_snapshots: Vec<i64> = read_strings(&mut env, &source_snapshot_tokens)
            .into_iter()
            .flatten()
            .map(|token| token.parse::<i64>().expect("single-table paimon snapshot token"))
            .collect();

        let config = PaimonStoreConfig {
            table_dir,
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format,
            file_compression: compression,
            deletion_vectors: true,
            ttl_ms: 0,
        };
        let store = if source_dirs.is_empty() {
            PaimonSessionAggStore::create(config, key_data_types, state_types)
        } else {
            let sources: Vec<(String, i64)> =
                source_dirs.into_iter().zip(source_snapshots).collect();
            PaimonSessionAggStore::open_merged(
                config,
                key_data_types,
                state_types,
                &sources,
                key_group_start..=key_group_end,
                aligned != 0,
            )
        };
        let aggregator = store.and_then(|store| {
            crate::session_agg::SessionAggregator::new(gap_millis, value_type_codes, kinds)
                .with_key_timestamp_precisions(timestamp_precisions)
                .with_backend(store)
                .with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, aggregator)
    })
}

/// Checkpoint sync phase (task thread, at the barrier): stages the open sessions, commits the
/// table, and hands back the manifest.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointPaimonSessionAggregator<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let aggregator = unsafe { &mut *(handle as *mut crate::session_agg::SessionAggregator) };
        match aggregator.checkpoint_backend() {
            Ok(manifest) => manifest_array(&mut env, &manifest),
            Err(e) => {
                throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

// ---------------------------------------------------------------------------------------------
// Interval join on two keyed row-buffer tables (left/, right/): reads happen on push (the
// incoming batch probes the opposite side by equi key), eviction is the watermark range read.
// The snapshot token packs both snapshot ids and both arrival sequences
// ("<left>:<right>:<lseq>:<rseq>").
// ---------------------------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonIntervalJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
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
    key_timestamp_precisions: JIntArray<'local>,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
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
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        let mut left_seq = 0i64;
        let mut right_seq = 0i64;
        let mut left_sources: Vec<(String, i64)> = Vec::new();
        let mut right_sources: Vec<(String, i64)> = Vec::new();
        for (dir, token) in source_dirs.iter().zip(
            read_strings(&mut env, &source_snapshot_tokens)
                .into_iter()
                .flatten(),
        ) {
            let (left_id, right_id, lseq, rseq) = parse_window_join_token(&token);
            left_seq = left_seq.max(lseq);
            right_seq = right_seq.max(rseq);
            if left_id >= 0 {
                left_sources.push((format!("{dir}/left"), left_id));
            }
            if right_id >= 0 {
                right_sources.push((format!("{dir}/right"), right_id));
            }
        }

        let left_types: Vec<DataType> =
            left_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let right_types: Vec<DataType> =
            right_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let config = PaimonStoreConfig {
            table_dir,
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format,
            file_compression: compression,
            deletion_vectors: true,
            ttl_ms: 0,
        };
        let store = if source_dirs.is_empty() {
            PaimonIntervalJoinStore::create(config, left_types, right_types)
        } else {
            PaimonIntervalJoinStore::open_merged(
                config,
                left_types,
                right_types,
                &left_sources,
                &right_sources,
                key_group_start..=key_group_end,
                aligned != 0,
            )
        };
        let joiner = store.and_then(|mut store| {
            store.left.set_next_seq(left_seq);
            store.right.set_next_seq(right_seq);
            crate::interval_join::IntervalJoiner::new(
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
            .with_key_timestamp_precisions(timestamp_precisions)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)
        });
        boxed_or_throw(&mut env, joiner)
    })
}

/// Checkpoint sync phase (task thread, at the barrier): commits both side tables; the token line
/// packs both snapshot ids and both arrival sequences.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointPaimonIntervalJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut crate::interval_join::IntervalJoiner) };
        let store = joiner.store_mut();
        let (left_seq, right_seq) = (store.left.next_seq(), store.right.next_seq());
        match store.checkpoint() {
            Ok((left, right)) => {
                let token = if left.snapshot_id < 0 && right.snapshot_id < 0 {
                    String::new()
                } else {
                    format!("{}:{}:{}:{}", left.snapshot_id, right.snapshot_id, left_seq, right_seq)
                };
                let mut lines = Vec::with_capacity(
                    1 + left.data_files.len()
                        + left.meta_files.len()
                        + right.data_files.len()
                        + right.meta_files.len(),
                );
                lines.push(token);
                lines.extend(left.data_files.iter().map(|f| format!("d:left/{f}")));
                lines.extend(right.data_files.iter().map(|f| format!("d:right/{f}")));
                lines.extend(left.meta_files.iter().map(|f| format!("m:left/{f}")));
                lines.extend(right.meta_files.iter().map(|f| format!("m:right/{f}")));
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
            Err(e) => {
                throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}

// ---------------------------------------------------------------------------------------------
// Temporal join: probe side on a keyed row buffer, versioned build side on plain upserts (the
// deduplicate merge engine IS last-write-wins per version timestamp), plus — with retention on —
// the per-key cleanup deadlines. The snapshot token packs the snapshot ids and the probe side's
// arrival sequence ("<left>:<right>:<lseq>:<deadlines>").
// ---------------------------------------------------------------------------------------------

/// Parses one restored temporal-join token — any id `-1` when that table had never committed.
/// The deadlines field is optional: a pre-retention token has three fields, and its absence IS
/// the enable-flip signal (no deadlines table to restore).
fn parse_temporal_token(token: &str) -> (i64, i64, i64, i64) {
    let fields: Vec<i64> = token
        .split(':')
        .map(|field| field.parse::<i64>().expect("temporal-join paimon token field"))
        .collect();
    assert!(fields.len() >= 3, "temporal-join paimon snapshot token");
    (fields[0], fields[1], fields[2], fields.get(3).copied().unwrap_or(-1))
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_createPaimonTemporalJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    left_keys: JIntArray<'local>,
    right_keys: JIntArray<'local>,
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
    key_timestamp_precisions: JIntArray<'local>,
    state_ttl_millis: jlong,
    now_millis: jlong,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    buckets: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
    aligned: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |mut env| {
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
        let timestamp_precisions = read_i32_array(&env, &key_timestamp_precisions);
        let table_dir = read_string(&mut env, &table_directory);
        let format = read_string(&mut env, &file_format);
        let compression = read_string(&mut env, &file_compression);
        let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
            .into_iter()
            .flatten()
            .collect();
        // Flink's exact cleaning enablement (`minRetentionTime > 1`): the deadlines table exists
        // only while cleaning is on, so a retention-off restore sheds any restored deadlines.
        let retention = state_ttl_millis > 1;
        let mut left_seq = 0i64;
        let mut left_sources: Vec<(String, i64)> = Vec::new();
        let mut right_sources: Vec<(String, i64)> = Vec::new();
        let mut deadline_sources: Vec<(String, i64)> = Vec::new();
        for (dir, token) in source_dirs.iter().zip(
            read_strings(&mut env, &source_snapshot_tokens)
                .into_iter()
                .flatten(),
        ) {
            let (left_id, right_id, lseq, deadlines_id) = parse_temporal_token(&token);
            left_seq = left_seq.max(lseq);
            if left_id >= 0 {
                left_sources.push((format!("{dir}/left"), left_id));
            }
            if right_id >= 0 {
                right_sources.push((format!("{dir}/right"), right_id));
            }
            if retention && deadlines_id >= 0 {
                deadline_sources.push((format!("{dir}/deadlines"), deadlines_id));
            }
        }

        // The probe side's payload carries the changelog kind as a trailing Int8 column.
        let mut left_types: Vec<DataType> =
            left_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        left_types.push(DataType::Int8);
        let right_types: Vec<DataType> =
            right_schema.fields().iter().map(|f| f.data_type().clone()).collect();
        let config = PaimonStoreConfig {
            table_dir,
            max_parallelism: max_parallelism as usize,
            buckets: buckets as usize,
            file_format: format,
            file_compression: compression,
            deletion_vectors: true,
            ttl_ms: 0,
        };
        let store = if source_dirs.is_empty() {
            PaimonTemporalJoinStore::create(config, left_types, right_types, retention)
        } else {
            PaimonTemporalJoinStore::open_merged(
                config,
                left_types,
                right_types,
                &left_sources,
                &right_sources,
                &deadline_sources,
                retention,
                key_group_start..=key_group_end,
                aligned != 0,
            )
        };
        let joiner = store.and_then(|mut store| {
            store.left.set_next_seq(left_seq);
            crate::temporal_join::TemporalJoiner::new(
                left,
                right,
                left_time as usize,
                right_time as usize,
                JoinKind::from_code(join_type),
                left_schema,
                right_schema,
                predicate,
            )
            .with_key_timestamp_precisions(timestamp_precisions)
            .with_state_retention(state_ttl_millis)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)
        });
        let joiner = joiner.and_then(|mut joiner| {
            if !source_dirs.is_empty() {
                joiner.hydrate_backend_retention(now_millis)?;
            }
            Ok(joiner)
        });
        boxed_or_throw(&mut env, joiner)
    })
}

/// Checkpoint sync phase (task thread, at the barrier): commits every table; the token line
/// packs the snapshot ids and the probe side's arrival sequence.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_checkpointPaimonTemporalJoiner<
    'local,
>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobjectArray {
    crate::bridge::jni_guard(env, move |mut env| {
        let joiner = unsafe { &mut *(handle as *mut crate::temporal_join::TemporalJoiner) };
        let store = joiner.store_mut();
        let left_seq = store.left.next_seq();
        match store.checkpoint() {
            Ok((left, right, deadlines)) => {
                let token = if left.snapshot_id < 0
                    && right.snapshot_id < 0
                    && deadlines.snapshot_id < 0
                {
                    String::new()
                } else {
                    format!(
                        "{}:{}:{}:{}",
                        left.snapshot_id, right.snapshot_id, left_seq, deadlines.snapshot_id
                    )
                };
                let mut lines = Vec::with_capacity(
                    1 + left.data_files.len()
                        + left.meta_files.len()
                        + right.data_files.len()
                        + right.meta_files.len()
                        + deadlines.data_files.len()
                        + deadlines.meta_files.len(),
                );
                lines.push(token);
                lines.extend(left.data_files.iter().map(|f| format!("d:left/{f}")));
                lines.extend(right.data_files.iter().map(|f| format!("d:right/{f}")));
                lines.extend(deadlines.data_files.iter().map(|f| format!("d:deadlines/{f}")));
                lines.extend(left.meta_files.iter().map(|f| format!("m:left/{f}")));
                lines.extend(right.meta_files.iter().map(|f| format!("m:right/{f}")));
                lines.extend(deadlines.meta_files.iter().map(|f| format!("m:deadlines/{f}")));
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
            Err(e) => {
                throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
                std::ptr::null_mut()
            }
        }
    })
}
