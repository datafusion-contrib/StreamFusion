use crate::*;

mod coalesce;
pub(crate) use coalesce::coalesce_batches;

pub(crate) fn copy_compact_timestamps(batch: &RecordBatch, columns: &[usize]) -> RecordBatch {
    let mut values = batch.columns().to_vec();
    for &index in columns {
        let source = &values[index];
        if streamfusion_bridge::timestamp::is_component_timestamp(source.data_type()) {
            let source = source
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .unwrap();
            let nanos = source
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            if nanos.values().iter().all(|value| *value == 0) {
                continue;
            }
            values[index] = Arc::new(arrow::array::StructArray::new(
                source.fields().clone(),
                vec![
                    source.column(0).clone(),
                    Arc::new(Int32Array::from_value(0, source.len())),
                ],
                source.nulls().cloned(),
            ));
        } else {
            let view = streamfusion_bridge::timestamp::TimestampColumn::try_new(source.as_ref())
                .expect("compact timestamp exchange column");
            let compact: ArrayRef = Arc::new(streamfusion_bridge::timestamp::timestamp_array(
                (0..source.len()).map(|row| {
                    (!view.is_null(row)).then(|| {
                        streamfusion_bridge::timestamp::TimestampValue::from_millis(
                            view.value(row).unwrap().millis(),
                        )
                    })
                }),
            ));
            values[index] =
                streamfusion_bridge::timestamp::cast_timestamp(&compact, source.data_type())
                    .expect("compact timestamp exchange type");
        }
    }
    RecordBatch::try_new_with_options(
        batch.schema(),
        values,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .expect("compact timestamp exchange batch")
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_copyCompactTimestampFields<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
    columns: JIntArray<'local>,
) {
    crate::bridge::jni_guard(env, move |env| {
        let batch = import_record_batch(in_array_address, in_schema_address);
        let columns: Vec<usize> = read_int_array(&env, &columns)
            .into_iter()
            .map(|v| v as usize)
            .collect();
        export_record_batch(
            copy_compact_timestamps(&batch, &columns),
            out_array_address,
            out_schema_address,
        );
    })
}

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
    partition_batch_with_layout(
        batch,
        key_columns,
        timestamp_precisions,
        max_parallelism,
        parallelism,
        false,
    )
}

fn partition_batch_with_layout(
    batch: &RecordBatch,
    key_columns: &[usize],
    timestamp_precisions: &[i32],
    max_parallelism: usize,
    parallelism: usize,
    projected_keys: bool,
) -> Vec<(usize, RecordBatch)> {
    let serialized;
    let batch = if has_string_caches(batch) {
        serialized = cold_string_caches(batch);
        &serialized
    } else {
        batch
    };
    // The precision sidecar is a pre-order type tree, so a nested key contributes more than one
    // descriptor; the encoder validates that it is consumed exactly.
    let mut channels: Vec<Option<(usize, Vec<u32>)>> = vec![None; parallelism];
    let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, timestamp_precisions);
    if projected_keys {
        encoder = encoder.projected();
    }
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
            // A key-only projection carries no columns, and Arrow cannot infer a row count from none.
            RecordBatch::try_new_with_options(
                batch.schema(),
                columns,
                &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(indices.len())),
            )
            .expect("sub batch"),
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
    partition_batch_by_key_group_with_layout(
        batch,
        key_columns,
        timestamp_precisions,
        max_parallelism,
        false,
    )
}

fn partition_batch_by_key_group_with_layout(
    batch: &RecordBatch,
    key_columns: &[usize],
    timestamp_precisions: &[i32],
    max_parallelism: usize,
    projected_keys: bool,
) -> Vec<(usize, Vec<i32>, RecordBatch)> {
    let serialized;
    let batch = if has_string_caches(batch) {
        serialized = cold_string_caches(batch);
        &serialized
    } else {
        batch
    };
    let mut rows_by_key_group: Vec<Vec<u32>> = vec![Vec::new(); max_parallelism];
    let mut encoder = BinaryRowBatchEncoder::new(batch, key_columns, timestamp_precisions);
    if projected_keys {
        encoder = encoder.projected();
    }
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
                RecordBatch::try_new_with_options(
                    batch.schema(),
                    columns,
                    &arrow::record_batch::RecordBatchOptions::new()
                        .with_row_count(Some(indices.len())),
                )
                .expect("key-group sub-batch"),
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
    split_by_key(
        env,
        in_array_address,
        in_schema_address,
        key_columns,
        timestamp_precisions,
        max_parallelism,
        parallelism,
        recoverable,
        false,
    )
}

#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_splitByProjectedKey<'local>(
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
    split_by_key(
        env,
        in_array_address,
        in_schema_address,
        key_columns,
        timestamp_precisions,
        max_parallelism,
        parallelism,
        recoverable,
        true,
    )
}

fn split_by_key<'local>(
    env: JNIEnv<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    key_columns: JIntArray<'local>,
    timestamp_precisions: JIntArray<'local>,
    max_parallelism: jint,
    parallelism: jint,
    recoverable: jboolean,
    projected_keys: bool,
) -> jlong {
    crate::bridge::jni_guard(env, move |env| {
        let batch = import_record_batch(in_array_address, in_schema_address);
        let keys: Vec<usize> = read_int_array(&env, &key_columns)
            .into_iter()
            .map(|k| k as usize)
            .collect();
        let precisions = read_i32_array(&env, &timestamp_precisions);
        let parts: Vec<SplitPart> = if recoverable != 0 {
            partition_batch_by_key_group_with_layout(
                &batch,
                &keys,
                &precisions,
                max_parallelism as usize,
                projected_keys,
            )
            .into_iter()
            .map(|(key_group, ordinals, batch)| SplitPart {
                key_group,
                ordinals,
                batch,
            })
            .collect()
        } else {
            partition_batch_with_layout(
                &batch,
                &keys,
                &precisions,
                max_parallelism as usize,
                parallelism as usize,
                projected_keys,
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

/// Coalesces JVM-exported batches, including UNION inputs with optional physical sidecars,
/// into one batch exported back into the consumer-allocated C structs.
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
        assert_eq!(arrays.len(), schemas.len(), "one schema per input array");
        let batches: Vec<RecordBatch> = arrays
            .into_iter()
            .zip(schemas)
            .map(|(array, schema)| import_record_batch(array, schema))
            .collect();
        let merged = coalesce_batches(&batches);
        // Java-backed buffers run release callbacks on drop; finish those before raising an
        // exception so a callback cannot consume the pending validation exception.
        drop(batches);
        match merged {
            Ok(merged) => export_record_batch(merged, out_array_address, out_schema_address),
            Err(error) => {
                env.throw_new("tech/streamfusion/NativeException", error.to_string())
                    .expect("throw concat validation failure");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_timestamp_copy_keeps_slices_nulls_metadata_and_owned_buffers() {
        use streamfusion_bridge::timestamp::{
            timestamp_array, with_timezone, TimestampColumn, TimestampValue,
        };
        let values: ArrayRef = Arc::new(timestamp_array([
            Some(TimestampValue::new(99, 1).unwrap()),
            Some(TimestampValue::new(i64::MIN, 123).unwrap()),
            None,
            Some(TimestampValue::new(-1, 999999).unwrap()),
            Some(TimestampValue::new(i64::MAX, 999999).unwrap()),
        ]));
        let values = with_timezone(&values, "UTC").unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("compact", values.data_type().clone(), true),
                Field::new("precise", values.data_type().clone(), true),
                Field::new("kind", DataType::Int8, false),
            ])),
            vec![
                values.clone(),
                values,
                Arc::new(Int8Array::from(vec![0, 1, 2, 3, 0])),
            ],
        )
        .unwrap()
        .slice(1, 3);
        let copied = copy_compact_timestamps(&batch, &[0]);
        assert_eq!(copied.schema(), batch.schema());
        assert!(Arc::ptr_eq(copied.column(1), batch.column(1)));
        assert!(Arc::ptr_eq(copied.column(2), batch.column(2)));
        let before = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        let after = copied
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();
        assert!(Arc::ptr_eq(before.column(0), after.column(0)));
        assert_eq!(
            TimestampColumn::try_new(batch.column(0).as_ref())
                .unwrap()
                .value(0)
                .unwrap()
                .nano_of_milli(),
            123
        );
        let view = TimestampColumn::try_new(copied.column(0).as_ref()).unwrap();
        assert_eq!(
            view.value(0).unwrap(),
            TimestampValue::from_millis(i64::MIN)
        );
        assert!(view.is_null(1));
        assert_eq!(view.value(2).unwrap(), TimestampValue::from_millis(-1));
        let repeated = copy_compact_timestamps(&copied, &[0]);
        assert!(Arc::ptr_eq(repeated.column(0), copied.column(0)));
        drop(batch);
        assert_eq!(
            TimestampColumn::try_new(copied.column(1).as_ref())
                .unwrap()
                .value(2)
                .unwrap()
                .nano_of_milli(),
            999999
        );
    }
    use arrow::datatypes::Schema;
    use std::sync::Arc;

    fn zero_column_batch(rows: usize) -> RecordBatch {
        RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .expect("zero-column batch")
    }

    #[test]
    fn both_shuffle_modes_clear_caches_without_mutating_the_parent_batch() {
        let cache = string_cache_array([Some(&[0xd800][..]), Some(&[0xd801][..]), None]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("$string_java_1$", DataType::Utf8, false),
                string_cache_field(1, cache.data_type().clone()),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["?", "?", "binary"])),
                cache,
            ],
        )
        .unwrap();
        assert_eq!(data_arity(&batch), 2);
        let mut rows = 0;
        for (_, part) in partition_batch(&batch, &[0], &[-1], 128, 2) {
            rows += part.num_rows();
            let cache = string_cache_column(&part, 1).unwrap();
            assert_eq!(cache.null_count(), part.num_rows());
        }
        assert_eq!(rows, 3);
        let mut rows = 0;
        for (_, _, part) in partition_batch_by_key_group(&batch, &[0], &[-1], 128) {
            rows += part.num_rows();
            let cache = string_cache_column(&part, 1).unwrap();
            assert_eq!(cache.null_count(), part.num_rows());
        }
        assert_eq!(rows, 3);
        assert_eq!(
            JavaStringCacheColumn::new(string_cache_column(&batch, 1).unwrap()).value(0),
            Some(&[0xd800][..])
        );
    }

    /// A projection can leave no columns behind, and Arrow refuses to infer a row count from none.
    #[test]
    fn partitions_a_zero_column_batch() {
        let batch = zero_column_batch(4);
        let parts = partition_batch(&batch, &[], &[], 128, 2);
        assert_eq!(parts.iter().map(|(_, b)| b.num_rows()).sum::<usize>(), 4);
    }

    #[test]
    fn partitions_a_zero_column_batch_by_key_group() {
        let batch = zero_column_batch(3);
        let parts = partition_batch_by_key_group(&batch, &[], &[], 128);
        assert_eq!(parts.iter().map(|(_, _, b)| b.num_rows()).sum::<usize>(), 3);
    }
}
