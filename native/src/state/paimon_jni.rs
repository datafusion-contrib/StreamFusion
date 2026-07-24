//! JNI surface of the Paimon-backed group aggregate. Mirrors the memory-backed entry points; the
//! handle is a distinct Rust type (`GroupAggregator<PaimonGroupStore>`), so the two families never
//! share a symbol. Checkpointing replaces the raw keyed-state snapshot family: the barrier calls
//! `checkpointPaimonGroupAggregator`, which commits the table's snapshot and hands back the file
//! manifest the host uploads.

use crate::*;
use jni::objects::{JClass, JIntArray, JLongArray, JObject, JObjectArray, JString};
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
/// `["<snapshot id>", "d:<data file>"…, "m:<meta file>"…]`, paths relative to the table root.
fn manifest_array<'local>(
    env: &mut JNIEnv<'local>,
    manifest: &PaimonCheckpointManifest,
) -> jobjectArray {
    let mut lines =
        Vec::with_capacity(1 + manifest.data_files.len() + manifest.meta_files.len());
    lines.push(manifest.snapshot_id.to_string());
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
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createPaimonGroupAggregator<
    'local,
>(
    mut env: JNIEnv<'local>,
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
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_ids: JLongArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
) -> jlong {
    let kinds = read_int_array(&env, &aggregate_kinds);
    let value_type_codes = read_int_array(&env, &value_types);
    let value_columns = read_int_array(&env, &value_columns);
    let filter_columns = read_int_array(&env, &filter_columns);
    let count_columns = read_int_array(&env, &count_columns);
    let distinct_view_columns = read_int_array(&env, &distinct_view_columns);
    let key_columns = read_columns(&env, &key_columns);
    let key_timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let table_dir = read_string(&mut env, &table_directory);
    let format = read_string(&mut env, &file_format);
    let compression = read_string(&mut env, &file_compression);
    let source_dirs: Vec<String> = read_strings(&mut env, &source_directories)
        .into_iter()
        .flatten()
        .collect();
    let source_snapshots = read_longs(&env, &source_snapshot_ids);

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
        file_format: format,
        file_compression: compression,
    };
    let store = if source_dirs.is_empty() {
        PaimonGroupStore::create(config, codec)
    } else {
        let sources: Vec<(String, i64)> =
            source_dirs.into_iter().zip(source_snapshots).collect();
        PaimonGroupStore::open_merged(config, codec, &sources, key_group_start..=key_group_end)
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
        .with_distinct_view_columns(distinct_view_columns);
        if mini_batch != 0 {
            base = base.with_mini_batch();
        }
        base.with_backend(store).with_read_through_budget(memory_budget_bytes)
    });
    boxed_or_throw(&mut env, aggregator)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_updatePaimonGroupAggregator<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut PaimonGroupAggregator) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        aggregator.update(&batch)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushPaimonGroupAggregator<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let aggregator = unsafe { &mut *(handle as *mut PaimonGroupAggregator) };
    match aggregator.flush_mini_batch() {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Checkpoint sync phase (task thread, at the barrier): commit and hand back the file manifest —
/// `["<snapshot id>", "d:<data file>"…, "m:<meta file>"…]`, paths relative to the table root.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_checkpointPaimonGroupAggregator<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    link_directory: JString<'local>,
) -> jobjectArray {
    let aggregator = unsafe { &mut *(handle as *mut PaimonGroupAggregator) };
    let link_dir = read_string(&mut env, &link_directory);
    match aggregator.store_mut().checkpoint(&link_dir) {
        Ok(manifest) => manifest_array(&mut env, &manifest),
        Err(e) => {
            throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonGroupAggregatorStateBytes<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let aggregator = unsafe { &*(handle as *const PaimonGroupAggregator) };
    aggregator.memory.state_bytes as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonGroupAggregatorStagingBytes<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let aggregator = unsafe { &*(handle as *const PaimonGroupAggregator) };
    aggregator.staging_bytes() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonGroupAggregatorStagedKeys<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let aggregator = unsafe { &*(handle as *const PaimonGroupAggregator) };
    aggregator.staged_keys() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closePaimonGroupAggregator<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<PaimonGroupAggregator>(handle));
    }
}

type PaimonKeepLastDeduplicator = KeepLastDeduplicator<PaimonDedupStore>;

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createPaimonKeepLastDeduplicator<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    rt_column: jint,
    row_schema_address: jlong,
    generate_update_before: jboolean,
    rowtime_ordered: jboolean,
    keep_first: jboolean,
    mini_batch: jboolean,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_ids: JLongArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
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
    let source_snapshots = read_longs(&env, &source_snapshot_ids);

    let codec = DedupStateCodec::new(row_types, rt_column as usize, rowtime_ordered != 0);
    let config = PaimonStoreConfig {
        table_dir,
        max_parallelism: max_parallelism as usize,
        file_format: format,
        file_compression: compression,
    };
    let store = if source_dirs.is_empty() {
        PaimonDedupStore::create(config, codec)
    } else {
        let sources: Vec<(String, i64)> =
            source_dirs.into_iter().zip(source_snapshots).collect();
        PaimonDedupStore::open_merged(config, codec, &sources, key_group_start..=key_group_end)
    };
    let dedup = store.and_then(|store| {
        KeepLastDeduplicator::new(
            partitions,
            rt_column as usize,
            generate_update_before != 0,
            rowtime_ordered != 0,
            keep_first != 0,
        )
        .with_mini_batch(mini_batch != 0)
        .with_key_timestamp_precisions(timestamp_precisions)
        .with_backend(store)
        .with_read_through_budget(memory_budget_bytes)
    });
    boxed_or_throw(&mut env, dedup)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushPaimonKeepLastDeduplicator<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let dedup = unsafe { &mut *(handle as *mut PaimonKeepLastDeduplicator) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        dedup.push(&batch)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushPaimonKeepLastDeduplicator<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let dedup = unsafe { &mut *(handle as *mut PaimonKeepLastDeduplicator) };
    match dedup.flush_mini_batch() {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Checkpoint sync phase (task thread, at the barrier); see `checkpointPaimonGroupAggregator`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_checkpointPaimonKeepLastDeduplicator<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    link_directory: JString<'local>,
) -> jobjectArray {
    let dedup = unsafe { &mut *(handle as *mut PaimonKeepLastDeduplicator) };
    let link_dir = read_string(&mut env, &link_directory);
    match dedup.store_mut().checkpoint(&link_dir) {
        Ok(manifest) => manifest_array(&mut env, &manifest),
        Err(e) => {
            throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonKeepLastDeduplicatorStateBytes<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let dedup = unsafe { &*(handle as *const PaimonKeepLastDeduplicator) };
    dedup.memory.state_bytes as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonKeepLastDeduplicatorStagingBytes<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let dedup = unsafe { &*(handle as *const PaimonKeepLastDeduplicator) };
    dedup.staging_bytes() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonKeepLastDeduplicatorStagedKeys<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let dedup = unsafe { &*(handle as *const PaimonKeepLastDeduplicator) };
    dedup.staged_keys() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closePaimonKeepLastDeduplicator<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<PaimonKeepLastDeduplicator>(handle));
    }
}

type PaimonChangelogNormalizer = ChangelogNormalizer<PaimonNormalizerStore>;

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createPaimonChangelogNormalizer<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    key_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    row_schema_address: jlong,
    generate_update_before: jboolean,
    mini_batch: jboolean,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_ids: JLongArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
) -> jlong {
    let keys = read_columns(&env, &key_columns);
    let timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
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
    let source_snapshots = read_longs(&env, &source_snapshot_ids);

    let codec = NormalizerStateCodec::new(row_types);
    let config = PaimonStoreConfig {
        table_dir,
        max_parallelism: max_parallelism as usize,
        file_format: format,
        file_compression: compression,
    };
    let store = if source_dirs.is_empty() {
        PaimonNormalizerStore::create(config, codec)
    } else {
        let sources: Vec<(String, i64)> =
            source_dirs.into_iter().zip(source_snapshots).collect();
        PaimonNormalizerStore::open_merged(config, codec, &sources, key_group_start..=key_group_end)
    };
    let normalizer = store.and_then(|store| {
        ChangelogNormalizer::new(keys, generate_update_before != 0)
            .with_mini_batch(mini_batch != 0)
            .with_key_timestamp_precisions(timestamp_precisions)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)
    });
    boxed_or_throw(&mut env, normalizer)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushPaimonChangelogNormalizer<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let normalizer = unsafe { &mut *(handle as *mut PaimonChangelogNormalizer) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        normalizer.push(&batch)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushPaimonChangelogNormalizer<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let normalizer = unsafe { &mut *(handle as *mut PaimonChangelogNormalizer) };
    match normalizer.flush_mini_batch() {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Checkpoint sync phase (task thread, at the barrier); see `checkpointPaimonGroupAggregator`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_checkpointPaimonChangelogNormalizer<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    link_directory: JString<'local>,
) -> jobjectArray {
    let normalizer = unsafe { &mut *(handle as *mut PaimonChangelogNormalizer) };
    let link_dir = read_string(&mut env, &link_directory);
    match normalizer.store_mut().checkpoint(&link_dir) {
        Ok(manifest) => manifest_array(&mut env, &manifest),
        Err(e) => {
            throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonChangelogNormalizerStateBytes<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let normalizer = unsafe { &*(handle as *const PaimonChangelogNormalizer) };
    normalizer.memory.state_bytes as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonChangelogNormalizerStagingBytes<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let normalizer = unsafe { &*(handle as *const PaimonChangelogNormalizer) };
    normalizer.staging_bytes() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonChangelogNormalizerStagedKeys<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let normalizer = unsafe { &*(handle as *const PaimonChangelogNormalizer) };
    normalizer.staged_keys() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closePaimonChangelogNormalizer<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<PaimonChangelogNormalizer>(handle));
    }
}
