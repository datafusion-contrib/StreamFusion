use crate::*;

/// Splits a batch into one sub-batch per non-empty Flink key group using
/// `BinaryRowData.hashCode()` → `MathUtils.murmurHash`. Each sub-batch keeps the full input schema
/// and carries a topology-independent key-group id. Flink can therefore rerun the partitioner on
/// an in-flight sub-batch restored from an unaligned checkpoint after the downstream parallelism
/// changes; a destination-channel batch would not be independently reroutable.
pub(crate) fn partition_batch(
    batch: &RecordBatch,
    key_columns: &[usize],
    timestamp_precisions: &[i32],
    max_parallelism: usize,
) -> Vec<(usize, RecordBatch)> {
    // The precision sidecar is a pre-order type tree, so a nested key contributes more than one
    // descriptor; the encoder validates that it is consumed exactly.
    let mut rows_by_key_group: Vec<Vec<u32>> = vec![Vec::new(); max_parallelism];
    let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, timestamp_precisions);
    for row in 0..batch.num_rows() {
        let key_group = flink_key_group(encoder.hash(row), max_parallelism);
        rows_by_key_group[key_group].push(row as u32);
    }
    let mut out = Vec::new();
    for (key_group, rows) in rows_by_key_group.into_iter().enumerate() {
        if rows.is_empty() {
            continue;
        }
        let indices = UInt32Array::from(rows);
        let columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None).expect("take"))
            .collect();
        out.push((
            key_group,
            RecordBatch::try_new(batch.schema(), columns).expect("sub batch"),
        ));
    }
    out
}

/// Holds the per-key-group sub-batches of one split, pulled out one at a time by the JVM.
pub(crate) struct SplitState {
    key_groups: Vec<(usize, RecordBatch)>,
    cursor: usize,
}

/// Splits a batch from the JVM by key into per-key-group sub-batches and returns a handle to pull
/// them with `nextSplit`; released with `closeSplit`.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_splitByKey<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    key_columns: JIntArray<'local>,
    timestamp_precisions: JIntArray<'local>,
    max_parallelism: jint,
) -> jlong {
    crate::bridge::jni_guard(env, move |env| {
        let batch = import_record_batch(in_array_address, in_schema_address);
        let keys: Vec<usize> = read_int_array(&env, &key_columns)
            .into_iter()
            .map(|k| k as usize)
            .collect();
        let precisions = read_i32_array(&env, &timestamp_precisions);
        let key_groups = partition_batch(&batch, &keys, &precisions, max_parallelism as usize);
        into_handle(SplitState {
            key_groups,
            cursor: 0,
        })
    })
}

/// Exports the next sub-batch into the consumer-allocated C structs and returns its key group, or
/// -1 once the split is exhausted.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_nextSplit<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jint {
    crate::bridge::jni_guard(env, move |_env| {
        let state = unsafe { &mut *(handle as *mut SplitState) };
        if state.cursor >= state.key_groups.len() {
            return -1;
        }
        let (key_group, batch) = state.key_groups[state.cursor].clone();
        state.cursor += 1;
        export_record_batch(batch, out_array_address, out_schema_address);
        key_group as jint
    })
}

/// Releases a split handle.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeSplit<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<SplitState>(handle));
    })
}

/// Concatenates batches the JVM exported — row subsets of one exchange edge, so they share a
/// schema — into a single batch exported back into the consumer-allocated C structs. The merge
/// step of the post-exchange coalescer, undoing the fragmentation `splitByKey` introduced.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_concatBatches<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_addresses: JLongArray<'local>,
    in_schema_addresses: JLongArray<'local>,
    out_array_address: jlong,
    out_schema_address: jlong,
) {
    crate::bridge::jni_guard(env, move |env| {
        let arrays = read_longs(&env, &in_array_addresses);
        let schemas = read_longs(&env, &in_schema_addresses);
        let batches: Vec<RecordBatch> = arrays
            .into_iter()
            .zip(schemas)
            .map(|(array, schema)| import_record_batch(array, schema))
            .collect();
        let merged = concat_batches(&batches[0].schema(), &batches).expect("concat batches");
        export_record_batch(merged, out_array_address, out_schema_address);
    })
}
