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
    source_snapshot_tokens: JObjectArray<'local>,
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
    source_snapshot_tokens: JObjectArray<'local>,
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
    let source_snapshots: Vec<i64> = read_strings(&mut env, &source_snapshot_tokens)
        .into_iter()
        .flatten()
        .map(|token| token.parse::<i64>().expect("single-table paimon snapshot token"))
        .collect();

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
    source_snapshot_tokens: JObjectArray<'local>,
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
    let source_snapshots: Vec<i64> = read_strings(&mut env, &source_snapshot_tokens)
        .into_iter()
        .flatten()
        .map(|token| token.parse::<i64>().expect("single-table paimon snapshot token"))
        .collect();

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

type PaimonTopNRanker = TopNRanker<PaimonTopNStore>;

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createPaimonTopNRanker<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    partition_columns: JIntArray<'local>,
    key_timestamp_precisions: JIntArray<'local>,
    sort_indices: JIntArray<'local>,
    sort_ascending: JIntArray<'local>,
    sort_nulls_first: JIntArray<'local>,
    row_schema_address: jlong,
    limit: jlong,
    output_rank_number: jboolean,
    net_diff: jboolean,
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
) -> jlong {
    let partitions = read_columns(&env, &partition_columns);
    let timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
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
        file_format: format,
        file_compression: compression,
    };
    let store = if source_dirs.is_empty() {
        PaimonTopNStore::create(config, codec)
    } else {
        let sources: Vec<(String, i64)> =
            source_dirs.into_iter().zip(source_snapshots).collect();
        PaimonTopNStore::open_merged(config, codec, &sources, key_group_start..=key_group_end)
    };
    let ranker = store.and_then(|store| {
        TopNRanker::new(partitions, sort, limit, output_rank_number != 0, net_diff != 0)
            .with_key_timestamp_precisions(timestamp_precisions)
            .with_converters(converters)
            .with_backend(store)
            .with_read_through_budget(memory_budget_bytes)
    });
    boxed_or_throw(&mut env, ranker)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushPaimonTopNRanker<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let ranker = unsafe { &mut *(handle as *mut PaimonTopNRanker) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        ranker.push(&batch)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushPaimonTopNRanker<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let ranker = unsafe { &mut *(handle as *mut PaimonTopNRanker) };
    let out = ranker.flush_net_diff();
    export_record_batch(out, out_array_address, out_schema_address);
}

/// Checkpoint sync phase (task thread, at the barrier); see `checkpointPaimonGroupAggregator`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_checkpointPaimonTopNRanker<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    link_directory: JString<'local>,
) -> jobjectArray {
    let ranker = unsafe { &mut *(handle as *mut PaimonTopNRanker) };
    let link_dir = read_string(&mut env, &link_directory);
    match ranker.store_mut().checkpoint(&link_dir) {
        Ok(manifest) => manifest_array(&mut env, &manifest),
        Err(e) => {
            throw_runtime(&mut env, &format!("paimon state checkpoint failed: {e}"));
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonTopNRankerStateBytes<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let ranker = unsafe { &*(handle as *const PaimonTopNRanker) };
    ranker.memory.state_bytes as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonTopNRankerStagingBytes<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let ranker = unsafe { &*(handle as *const PaimonTopNRanker) };
    ranker.staging_bytes() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonTopNRankerStagedKeys<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let ranker = unsafe { &*(handle as *const PaimonTopNRanker) };
    ranker.staged_partitions() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closePaimonTopNRanker<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<PaimonTopNRanker>(handle));
    }
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
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_createPaimonUpdatingJoiner<
    'local,
>(
    mut env: JNIEnv<'local>,
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
    memory_budget_bytes: jlong,
    table_directory: JString<'local>,
    max_parallelism: jint,
    file_format: JString<'local>,
    file_compression: JString<'local>,
    source_directories: JObjectArray<'local>,
    source_snapshot_tokens: JObjectArray<'local>,
    key_group_start: jint,
    key_group_end: jint,
) -> jlong {
    let left = read_columns(&env, &left_keys);
    let right = read_columns(&env, &right_keys);
    let timestamp_precisions: Vec<i32> = read_int_array(&env, &key_timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
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
    // from whichever sources ever committed it.
    let side_config = |side: &str| PaimonStoreConfig {
        table_dir: format!("{table_dir}/{side}"),
        max_parallelism: max_parallelism as usize,
        file_format: format.clone(),
        file_compression: compression.clone(),
    };
    let side_store = |side: &str, schema: &SchemaRef, pick: fn(&str) -> i64| {
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
            PaimonJoinStore::create(side_config(side), codec)
        } else {
            PaimonJoinStore::open_merged(
                side_config(side),
                codec,
                &sources,
                key_group_start..=key_group_end,
            )
        }
    };
    let left_store = side_store("left", &left_schema, |t| parse_join_token(t).0);
    let right_store = side_store("right", &right_schema, |t| parse_join_token(t).1);
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
        .with_backend(left_store, right_store)
        .with_read_through_budget(memory_budget_bytes)
    });
    boxed_or_throw(&mut env, joiner)
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushLeftPaimonUpdatingJoiner<
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
    let joiner = unsafe { &mut *(handle as *mut PaimonUpdatingJoiner) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        joiner.push(&batch, true)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_pushRightPaimonUpdatingJoiner<
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
    let joiner = unsafe { &mut *(handle as *mut PaimonUpdatingJoiner) };
    // See updateTumblingAggregator: the batch's JVM release upcall must precede any throw.
    let result = {
        let batch = import_record_batch(in_array_address, in_schema_address);
        joiner.push(&batch, false)
    };
    match result {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_flushPaimonUpdatingJoiner<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    let joiner = unsafe { &mut *(handle as *mut PaimonUpdatingJoiner) };
    match joiner.flush_mini_batch() {
        Ok(out) => export_record_batch(out, out_array_address, out_schema_address),
        Err(e) => throw_memory_limit(&mut env, &e.to_string()),
    }
}

/// Checkpoint sync phase (task thread, at the barrier): commits BOTH side tables and hands back
/// one merged manifest — token `"<left id>:<right id>"` (empty when neither side ever committed),
/// file paths prefixed `left/` / `right/` relative to the operator's state directory.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_checkpointPaimonUpdatingJoiner<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    link_directory: JString<'local>,
) -> jobjectArray {
    let joiner = unsafe { &mut *(handle as *mut PaimonUpdatingJoiner) };
    let link_dir = read_string(&mut env, &link_directory);
    let (left_store, right_store) = joiner.stores_mut();
    let manifests = left_store
        .checkpoint(&format!("{link_dir}/left"))
        .and_then(|left| Ok((left, right_store.checkpoint(&format!("{link_dir}/right"))?)));
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
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonUpdatingJoinerStateBytes<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let joiner = unsafe { &*(handle as *const PaimonUpdatingJoiner) };
    joiner.memory.state_bytes as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonUpdatingJoinerStagingBytes<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let joiner = unsafe { &*(handle as *const PaimonUpdatingJoiner) };
    joiner.staging_bytes() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_paimonUpdatingJoinerStagedKeys<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let joiner = unsafe { &*(handle as *const PaimonUpdatingJoiner) };
    joiner.staged_keys() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closePaimonUpdatingJoiner<
    'local,
>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<PaimonUpdatingJoiner>(handle));
    }
}
