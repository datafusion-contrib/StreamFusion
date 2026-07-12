use crate::*;

/// Splits a batch into up to `num_partitions` sub-batches using Flink's key-group assignment:
/// `BinaryRowData.hashCode()` → `MathUtils.murmurHash` → key group → operator index. Each
/// sub-batch keeps the full input schema (a row subset); empty partitions are omitted. This makes
/// the columnar exchange and raw keyed state agree during rescaling.
pub(crate) fn partition_batch(
    batch: &RecordBatch,
    key_columns: &[usize],
    timestamp_precisions: &[i32],
    max_parallelism: usize,
    num_partitions: usize,
) -> Vec<(usize, RecordBatch)> {
    // The precision sidecar is a pre-order type tree, so a nested key contributes more than one
    // descriptor; the encoder validates that it is consumed exactly.
    assert!(max_parallelism >= num_partitions);
    let mut rows_by_partition: Vec<Vec<u32>> = vec![Vec::new(); num_partitions];
    let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, timestamp_precisions);
    for row in 0..batch.num_rows() {
        let key_group = flink_key_group(encoder.hash(row), max_parallelism);
        let partition = key_group * num_partitions / max_parallelism;
        rows_by_partition[partition].push(row as u32);
    }
    let mut out = Vec::new();
    for (partition, rows) in rows_by_partition.into_iter().enumerate() {
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
            partition,
            RecordBatch::try_new(batch.schema(), columns).expect("sub batch"),
        ));
    }
    out
}

/// Holds the per-partition sub-batches of one split, pulled out one at a time by the JVM.
pub(crate) struct SplitState {
    partitions: Vec<(usize, RecordBatch)>,
    cursor: usize,
}

/// Splits a batch from the JVM by key into per-partition sub-batches and returns a handle to pull
/// them with `nextSplit`; released with `closeSplit`.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_splitByKey<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    key_columns: JIntArray<'local>,
    timestamp_precisions: JIntArray<'local>,
    max_parallelism: jint,
    num_partitions: jint,
) -> jlong {
    let batch = import_record_batch(in_array_address, in_schema_address);
    let keys: Vec<usize> = read_int_array(&env, &key_columns)
        .into_iter()
        .map(|k| k as usize)
        .collect();
    let precisions: Vec<i32> = read_int_array(&env, &timestamp_precisions)
        .into_iter()
        .map(|precision| precision as i32)
        .collect();
    let partitions = partition_batch(
        &batch,
        &keys,
        &precisions,
        max_parallelism as usize,
        num_partitions as usize,
    );
    into_handle(SplitState {
        partitions,
        cursor: 0,
    })
}

/// Exports the next sub-batch into the consumer-allocated C structs and returns its partition, or
/// -1 once the split is exhausted.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_nextSplit<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jint {
    let state = unsafe { &mut *(handle as *mut SplitState) };
    if state.cursor >= state.partitions.len() {
        return -1;
    }
    let (partition, batch) = state.partitions[state.cursor].clone();
    state.cursor += 1;
    export_record_batch(batch, out_array_address, out_schema_address);
    partition as jint
}

/// Releases a split handle.
#[no_mangle]
pub extern "system" fn Java_io_github_jordepic_streamfusion_Native_closeSplit<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    unsafe {
        drop(from_handle::<SplitState>(handle));
    }
}
