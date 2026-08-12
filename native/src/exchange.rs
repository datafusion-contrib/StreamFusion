use crate::*;

/// Splits a batch into one order-preserving sub-batch per destination channel using
/// `BinaryRowData.hashCode()` → `MathUtils.murmurHash` → Flink's key-group range assignment.
/// Each sub-batch carries one representative key group owned by that channel, which lets Flink's
/// ordinary partitioner route the whole Arrow batch. The exchange requires aligned checkpoints:
/// after rescaling a retained multi-key-group batch could span several new channels, while Flink's
/// channel-state recovery can only keep or discard a whole record.
pub(crate) fn partition_batch(
    batch: &RecordBatch,
    key_columns: &[usize],
    timestamp_precisions: &[i32],
    max_parallelism: usize,
    parallelism: usize,
) -> Vec<(usize, RecordBatch)> {
    // The precision sidecar is a pre-order type tree, so a nested key contributes more than one
    // descriptor; the encoder validates that it is consumed exactly.
    let mut channels: Vec<Option<(usize, Vec<u32>)>> = vec![None; parallelism];
    let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, timestamp_precisions);
    for row in 0..batch.num_rows() {
        let key_group = flink_key_group(encoder.hash(row), max_parallelism);
        let channel = key_group * parallelism / max_parallelism;
        channels[channel]
            .get_or_insert_with(|| (key_group, Vec::new()))
            .1
            .push(row as u32);
    }
    let mut out = Vec::with_capacity(parallelism.min(batch.num_rows()));
    for (key_group, rows) in channels.into_iter().flatten() {
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

/// Splits a batch into one order-preserving sub-batch per non-empty key group. Unlike destination
/// batching, every output record remains independently routable when Flink restores unaligned
/// channel state at a different parallelism. Row ordinals let the receiver reconstruct the exact
/// destination-local order of the parent batch after the shuffle.
pub(crate) fn partition_batch_by_key_group(
    batch: &RecordBatch,
    key_columns: &[usize],
    timestamp_precisions: &[i32],
    max_parallelism: usize,
) -> Vec<(usize, Vec<i32>, RecordBatch)> {
    let mut rows_by_key_group: Vec<Vec<u32>> = vec![Vec::new(); max_parallelism];
    let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, timestamp_precisions);
    for row in 0..batch.num_rows() {
        let key_group = flink_key_group(encoder.hash(row), max_parallelism);
        rows_by_key_group[key_group].push(row as u32);
    }
    rows_by_key_group
        .into_iter()
        .enumerate()
        .filter_map(|(key_group, rows)| {
            if rows.is_empty() {
                return None;
            }
            let ordinals = rows.iter().map(|row| *row as i32).collect();
            let indices = UInt32Array::from(rows);
            let columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|column| take(column, &indices, None).expect("take"))
                .collect();
            Some((
                key_group,
                ordinals,
                RecordBatch::try_new(batch.schema(), columns).expect("key-group sub-batch"),
            ))
        })
        .collect()
}

struct SplitPart {
    key_group: usize,
    ordinals: Vec<i32>,
    batch: RecordBatch,
}

/// Holds the sub-batches of one split, pulled out one at a time by the JVM.
pub(crate) struct SplitState {
    parts: Vec<SplitPart>,
    key_groups: Vec<i32>,
    cursor: usize,
}

/// Splits a batch from the JVM by key and returns a handle to pull the resulting destination or
/// key-group sub-batches with `nextSplit`; released with `closeSplit`.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_splitByKey<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    key_columns: JIntArray<'local>,
    timestamp_precisions: JIntArray<'local>,
    max_parallelism: jint,
    parallelism: jint,
    recoverable: jboolean,
) -> jlong {
    crate::bridge::jni_guard(env, move |env| {
        let batch = import_record_batch(in_array_address, in_schema_address);
        let keys: Vec<usize> = read_int_array(&env, &key_columns)
            .into_iter()
            .map(|k| k as usize)
            .collect();
        let precisions = read_i32_array(&env, &timestamp_precisions);
        let parts: Vec<SplitPart> = if recoverable != 0 {
            partition_batch_by_key_group(&batch, &keys, &precisions, max_parallelism as usize)
                .into_iter()
                .map(|(key_group, ordinals, batch)| SplitPart {
                    key_group,
                    ordinals,
                    batch,
                })
                .collect()
        } else {
            partition_batch(
                &batch,
                &keys,
                &precisions,
                max_parallelism as usize,
                parallelism as usize,
            )
            .into_iter()
            .map(|(key_group, batch)| SplitPart {
                key_group,
                ordinals: Vec::new(),
                batch,
            })
            .collect()
        };
        let key_groups = if recoverable != 0 {
            parts.iter().map(|part| part.key_group as i32).collect()
        } else {
            Vec::new()
        };
        into_handle(SplitState {
            parts,
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
        if state.cursor >= state.parts.len() {
            return -1;
        }
        let part = &state.parts[state.cursor];
        state.cursor += 1;
        export_record_batch(part.batch.clone(), out_array_address, out_schema_address);
        part.key_group as jint
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_currentSplitKeyGroups<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jintArray {
    crate::bridge::jni_guard(env, move |env| {
        let state = unsafe { &*(handle as *const SplitState) };
        let output = env
            .new_int_array(state.key_groups.len() as i32)
            .expect("allocate parent key groups");
        env.set_int_array_region(&output, 0, &state.key_groups)
            .expect("write parent key groups");
        output.into_raw()
    })
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_currentSplitOrdinals<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jintArray {
    crate::bridge::jni_guard(env, move |env| {
        let state = unsafe { &*(handle as *const SplitState) };
        let ordinals = &state.parts[state.cursor - 1].ordinals;
        let output = env
            .new_int_array(ordinals.len() as i32)
            .expect("allocate row ordinals");
        env.set_int_array_region(&output, 0, ordinals)
            .expect("write row ordinals");
        output.into_raw()
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
