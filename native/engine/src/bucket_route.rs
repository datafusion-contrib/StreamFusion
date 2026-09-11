use crate::*;
use std::collections::HashMap;

/// One routed sub-batch: the rows of a batch that share a Paimon write destination.
pub(crate) struct RoutedBatch {
    /// The destination partition as a `BinaryRow` over the partition columns. Paimon's `BinaryRow`
    /// is Flink's `BinaryRowData` layout, so the Flink key encoder produces it byte for byte and the
    /// JVM side can hand the bytes to Paimon's writer as its partition key.
    pub(crate) partition: Vec<u8>,
    pub(crate) bucket: i32,
    pub(crate) batch: RecordBatch,
}

/// Splits a batch into one order-preserving sub-batch per distinct (partition, bucket) pair,
/// Paimon's unit of file writing. The bucket is Paimon's default bucket function,
/// `abs(BinaryRow.hashCode() % numBuckets)` over the bucket-key columns; a non-positive
/// `num_buckets` is a bucket-unaware table, where every row is bucket 0 and only the partition
/// splits the batch. The precision sidecars follow the key encoder's pre-order type-tree contract.
pub(crate) fn route_batch(
    batch: &RecordBatch,
    partition_columns: &[usize],
    partition_precisions: &[i32],
    bucket_columns: &[usize],
    bucket_precisions: &[i32],
    num_buckets: i32,
) -> Vec<RoutedBatch> {
    let mut groups: Vec<(Vec<u8>, i32, Vec<u32>)> = Vec::new();
    let mut by_partition: HashMap<Vec<u8>, HashMap<i32, usize>> = HashMap::new();
    let mut partitions = BinaryRowBatchEncoder::new(batch, partition_columns, partition_precisions);
    let mut bucket_keys = BinaryRowBatchEncoder::new(batch, bucket_columns, bucket_precisions);
    for row in 0..batch.num_rows() {
        let bucket = if num_buckets > 0 {
            (bucket_keys.hash(row) % num_buckets).abs()
        } else {
            0
        };
        let partition = partitions.encode(row);
        let buckets = match by_partition.get_mut(partition) {
            Some(buckets) => buckets,
            None => by_partition.entry(partition.to_vec()).or_default(),
        };
        let group = *buckets.entry(bucket).or_insert_with(|| {
            groups.push((partition.to_vec(), bucket, Vec::new()));
            groups.len() - 1
        });
        groups[group].2.push(row as u32);
    }
    groups
        .into_iter()
        .map(|(partition, bucket, rows)| {
            let indices = UInt32Array::from(rows);
            let columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|column| take(column, &indices, None).expect("take"))
                .collect();
            RoutedBatch {
                partition,
                bucket,
                batch: RecordBatch::try_new_with_options(
                    batch.schema(),
                    columns,
                    &arrow::record_batch::RecordBatchOptions::new()
                        .with_row_count(Some(indices.len())),
                )
                .expect("routed sub-batch"),
            }
        })
        .collect()
}

/// Holds the routed sub-batches of one batch, pulled out one at a time by the JVM.
pub(crate) struct RouteState {
    routed: Vec<RoutedBatch>,
    cursor: usize,
}

/// Routes a batch from the JVM to its Paimon write destinations and returns a handle to pull the
/// sub-batches with `nextBucketRoute`; released with `closeBucketRoute`.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_routeByBucket<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    in_array_address: jlong,
    in_schema_address: jlong,
    partition_columns: JIntArray<'local>,
    partition_precisions: JIntArray<'local>,
    bucket_columns: JIntArray<'local>,
    bucket_precisions: JIntArray<'local>,
    num_buckets: jint,
) -> jlong {
    crate::bridge::jni_guard(env, move |env| {
        let batch = import_record_batch(in_array_address, in_schema_address);
        let column_indexes = |array: &JIntArray<'local>| -> Vec<usize> {
            read_i32_array(env, array)
                .into_iter()
                .map(|column| column as usize)
                .collect()
        };
        let routed = route_batch(
            &batch,
            &column_indexes(&partition_columns),
            &read_i32_array(env, &partition_precisions),
            &column_indexes(&bucket_columns),
            &read_i32_array(env, &bucket_precisions),
            num_buckets,
        );
        into_handle(RouteState { routed, cursor: 0 })
    })
}

/// Exports the next routed sub-batch into the consumer-allocated C structs and returns its bucket,
/// or -1 once the route is exhausted.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_nextBucketRoute<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    out_array_address: jlong,
    out_schema_address: jlong,
) -> jint {
    crate::bridge::jni_guard(env, move |_env| {
        let state = unsafe { &mut *(handle as *mut RouteState) };
        if state.cursor >= state.routed.len() {
            return -1;
        }
        let routed = &state.routed[state.cursor];
        state.cursor += 1;
        export_record_batch(routed.batch.clone(), out_array_address, out_schema_address);
        routed.bucket as jint
    })
}

/// The partition `BinaryRow` bytes of the sub-batch returned by the latest `nextBucketRoute`.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_currentBucketRoutePartition<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    crate::bridge::jni_guard(env, move |env| {
        let state = unsafe { &*(handle as *const RouteState) };
        env.byte_array_from_slice(&state.routed[state.cursor - 1].partition)
            .expect("allocate partition bytes")
            .into_raw()
    })
}

/// Releases a route handle.
#[no_mangle]
pub extern "system" fn Java_tech_streamfusion_Native_closeBucketRoute<'local>(
    env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    crate::bridge::jni_guard(env, move |_env| unsafe {
        drop(from_handle::<RouteState>(handle));
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("dt", DataType::Utf8, true),
                Field::new("k", DataType::Int64, true),
                Field::new("v", DataType::Int32, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("a"),
                    None,
                    Some("a"),
                    Some("b"),
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(2),
                    Some(1),
                    Some(1),
                    None,
                ])),
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5])),
            ],
        )
        .expect("batch")
    }

    fn values(routed: &RoutedBatch) -> Vec<i32> {
        routed
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("v")
            .values()
            .to_vec()
    }

    #[test]
    fn groups_rows_by_partition_and_default_bucket_in_arrival_order() {
        let batch = batch();
        let num_buckets = 4;
        let routed = route_batch(&batch, &[0], &[-1], &[1], &[-1], num_buckets);
        let mut seen = 0;
        for group in &routed {
            let first = values(group)[0] as usize;
            let expected_partition = binary_row_hash(&batch, &[0], first, &[-1]);
            let expected_bucket = (binary_row_hash(&batch, &[1], first, &[-1]) % num_buckets).abs();
            assert_eq!(hash_bytes_by_words(&group.partition), expected_partition);
            assert_eq!(group.bucket, expected_bucket);
            let rows = values(group);
            assert!(rows.windows(2).all(|pair| pair[0] < pair[1]), "order kept");
            for &row in &rows {
                assert_eq!(
                    hash_bytes_by_words(&group.partition),
                    binary_row_hash(&batch, &[0], row as usize, &[-1])
                );
                assert_eq!(
                    group.bucket,
                    (binary_row_hash(&batch, &[1], row as usize, &[-1]) % num_buckets).abs()
                );
            }
            seen += rows.len();
        }
        assert_eq!(seen, batch.num_rows());
        assert!(routed
            .iter()
            .all(|g| g.bucket >= 0 && g.bucket < num_buckets));
        let first_of_each: Vec<i32> = routed.iter().map(|g| values(g)[0]).collect();
        assert!(
            first_of_each.windows(2).all(|pair| pair[0] < pair[1]),
            "groups in first-seen order"
        );
        let rows_of_a_1: Vec<Vec<i32>> = routed
            .iter()
            .filter(|g| values(g).contains(&0))
            .map(values)
            .collect();
        assert_eq!(rows_of_a_1, vec![vec![0, 4]]);
    }

    #[test]
    fn bucket_unaware_tables_split_by_partition_only() {
        let batch = batch();
        let routed = route_batch(&batch, &[0], &[-1], &[1], &[-1], -1);
        assert_eq!(routed.len(), 3);
        assert!(routed.iter().all(|g| g.bucket == 0));
        assert_eq!(values(&routed[0]), vec![0, 2, 4]);
        assert_eq!(values(&routed[1]), vec![1, 5]);
        assert_eq!(values(&routed[2]), vec![3]);
    }

    #[test]
    fn unpartitioned_tables_carry_the_empty_binary_row() {
        let batch = batch();
        let routed = route_batch(&batch, &[], &[], &[1, 0], &[-1, -1], 1);
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].partition, vec![0u8; 8], "BinaryRow.EMPTY_ROW");
        assert_eq!(routed[0].bucket, 0);
        assert_eq!(routed[0].batch.num_rows(), batch.num_rows());
    }

    #[test]
    fn routes_a_zero_column_batch() {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .expect("zero-column batch");
        let routed = route_batch(&batch, &[], &[], &[], &[], 2);
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].batch.num_rows(), 3);
    }
}
