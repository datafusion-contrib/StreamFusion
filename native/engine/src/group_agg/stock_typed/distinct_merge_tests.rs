use super::*;
use arrow::array::{Float32Array, Float64Array};

trait Capture {
    fn capture(&mut self, slot: Option<usize>) -> Result<Vec<RecordBatch>, DataFusionError>;
}

impl Capture for GroupAggregator {
    fn capture(&mut self, slot: Option<usize>) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut frames = Vec::new();
        self.visit_typed(slot, |batch| {
            frames.push(batch);
            Ok(())
        })?;
        Ok(frames)
    }
}

#[cfg(feature = "rocksdb-state")]
impl Capture for GroupAggregator<RocksGroupStore> {
    fn capture(&mut self, slot: Option<usize>) -> Result<Vec<RecordBatch>, DataFusionError> {
        let mut frames = Vec::new();
        self.visit_typed(slot, |batch| {
            frames.push(batch);
            Ok(())
        })?;
        Ok(frames)
    }
}

fn partial_view(values: &[Option<i32>]) -> ArrayRef {
    let entries = arrow::array::StructArray::new(
        distinct_entry_fields(&DataType::Int32),
        vec![
            Arc::new(Int32Array::from(values.to_vec())),
            Arc::new(Int64Array::from(vec![1; values.len()])),
        ],
        None,
    );
    Arc::new(arrow::array::ListArray::new(
        Arc::new(Field::new("item", entries.data_type().clone(), false)),
        arrow::buffer::OffsetBuffer::new(vec![0, values.len() as i32].into()),
        Arc::new(entries),
        None,
    ))
}

fn partial(filtered: &[Option<i32>], all: &[Option<i32>], sum: i64, count: i64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("filtered_count", Arc::new(Int64Array::from(vec![99]))),
        ("filtered_sum", Arc::new(Int32Array::from(vec![777]))),
        ("all_count", Arc::new(Int64Array::from(vec![123]))),
        ("all_sum", Arc::new(Int32Array::from(vec![999]))),
        ("average_sum", Arc::new(Int64Array::from(vec![sum]))),
        ("average_count", Arc::new(Int64Array::from(vec![count]))),
        ("filtered_members", partial_view(filtered)),
        ("all_members", partial_view(all)),
    ])
    .unwrap()
}

fn owner() -> GroupAggregator {
    GroupAggregator::new(
        vec![7, 9, 7, 9, 4],
        vec![2; 5],
        vec![1, 2, 3, 4, 5],
        vec![0],
        true,
    )
    .with_count_columns(vec![-1, -1, -1, -1, 6])
    .with_distinct_view_columns(vec![7, 7, 8, 8, -1])
    .with_key_timestamp_precisions(vec![i32::MIN, -1])
    .with_mini_batch()
    .with_memory_budget(16 << 20)
    .unwrap()
}

fn assert_scalars<S: KeyedStateStore<GroupKeyState>>(
    owner: &mut GroupAggregator<S>,
    expected: (i64, i32, i64, i32, i64, i64),
) where
    GroupAggregator<S>: Capture,
{
    let headers = owner.capture(None).unwrap();
    assert_eq!(headers.len(), 1);
    let header = &headers[0];
    assert_eq!(header.num_rows(), 1);
    for (column, expected) in [
        (3, ScalarValue::Int64(Some(expected.0))),
        (5, ScalarValue::Int32(Some(expected.1))),
        (7, ScalarValue::Int64(Some(expected.2))),
        (9, ScalarValue::Int32(Some(expected.3))),
        (11, ScalarValue::Int64(Some(expected.4))),
        (12, ScalarValue::Int64(Some(expected.5))),
    ] {
        assert_eq!(
            ScalarValue::try_from_array(header.column(column), 0).unwrap(),
            expected
        );
    }
}

fn exercise<S: KeyedStateStore<GroupKeyState>>(mut owner: GroupAggregator<S>)
where
    GroupAggregator<S>: Capture,
{
    owner.configure_stock_global_merge().unwrap();
    owner.configure_typed(&[7, 9, 7, 9, 4], true).unwrap();
    owner
        .update(
            &partial(&[Some(3), None], &[Some(2), Some(3), None], 12, 3),
            0,
        )
        .unwrap();
    assert!(owner.capture(None).is_err());
    let output = owner.flush_mini_batch().unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_scalars(&mut owner, (1, 3, 2, 5, 12, 3));

    owner
        .update(
            &partial(&[Some(2), None], &[Some(2), Some(4), None], 6, 1),
            1,
        )
        .unwrap();
    // A duplicate local partial must union membership bits without changing distinct scalars.
    owner
        .update(
            &partial(&[Some(2), None], &[Some(2), Some(4), None], 0, 0),
            1,
        )
        .unwrap();
    owner.flush_mini_batch().unwrap();
    assert_scalars(&mut owner, (2, 5, 3, 9, 18, 4));
    for slot in 0..4 {
        let mut members = Vec::new();
        for batch in owner.capture(Some(slot)).unwrap() {
            for row in 0..batch.num_rows() {
                members.push((
                    ScalarValue::try_from_array(batch.column(1), row).unwrap(),
                    batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row),
                ));
            }
        }
        assert_eq!(members.len(), if slot < 2 { 3 } else { 4 });
        assert!(members
            .iter()
            .any(|(value, count)| value.is_null() && *count == 1));
        assert!(members.iter().all(|(_, count)| *count == 1));
    }
    assert_eq!(owner.staging_bytes(), 0);
}

#[test]
fn stock_global_distinct_unions_views_without_refolding_local_scalars() {
    exercise(owner());
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn rocks_stock_global_distinct_unions_views_without_refolding_local_scalars() {
    use crate::state::RocksStoreConfig;
    let directory = tempfile::tempdir().unwrap();
    let store = RocksGroupStore::create(
        RocksStoreConfig {
            table_dir: directory
                .path()
                .join("state")
                .to_string_lossy()
                .into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        },
        GroupStateCodec::new(
            vec![7, 9, 7, 9, 4],
            vec![DataType::Int32; 5],
            vec![1, 2, 3, 4, 5],
            vec![7, 7, 8, 8, -1],
        ),
    )
    .unwrap();
    exercise(owner().with_backend(store));
}

fn floating_owner(code: i64) -> GroupAggregator {
    GroupAggregator::new(vec![7, 9], vec![code; 2], vec![1, 2], vec![0], true)
        .with_distinct_view_columns(vec![3, 3])
        .with_key_timestamp_precisions(vec![i32::MIN, -1])
        .with_mini_batch()
        .with_memory_budget(16 << 20)
        .unwrap()
}

fn floating_partial(values: ArrayRef) -> RecordBatch {
    let count = values.len();
    let entries = arrow::array::StructArray::new(
        distinct_entry_fields(values.data_type()),
        vec![values.clone(), Arc::new(Int64Array::from(vec![1; count]))],
        None,
    );
    let members = arrow::array::ListArray::new(
        Arc::new(Field::new("item", entries.data_type().clone(), false)),
        arrow::buffer::OffsetBuffer::new(vec![0, count as i32].into()),
        Arc::new(entries),
        None,
    );
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("count", Arc::new(Int64Array::from(vec![999]))),
        ("sum", values.slice(0, 1)),
        ("members", Arc::new(members)),
    ])
    .unwrap()
}

fn floating_exercise<S: KeyedStateStore<GroupKeyState>>(mut owner: GroupAggregator<S>, code: i64)
where
    GroupAggregator<S>: Capture,
{
    owner.configure_stock_global_merge().unwrap();
    owner.configure_typed(&[7, 9], true).unwrap();
    let (initial, next, expected): (ArrayRef, ArrayRef, ScalarValue) = if code == 6 {
        (
            Arc::new(Float32Array::from(vec![7.0])),
            Arc::new(Float32Array::from(vec![
                Some(1e8),
                Some(-1e8),
                Some(1.0),
                None,
            ])),
            ScalarValue::Float32(Some(8.0)),
        )
    } else {
        (
            Arc::new(Float64Array::from(vec![2.0])),
            Arc::new(Float64Array::from(vec![
                Some(1e16),
                Some(-1e16),
                Some(1.0),
                None,
            ])),
            ScalarValue::Float64(Some(4.0)),
        )
    };
    owner.update(&floating_partial(initial), 0).unwrap();
    owner.flush_mini_batch().unwrap();
    owner.update(&floating_partial(next.clone()), 1).unwrap();
    owner.update(&floating_partial(next), 1).unwrap();
    owner.flush_mini_batch().unwrap();
    let headers = owner.capture(None).unwrap();
    assert_eq!(headers.len(), 1);
    assert_eq!(
        ScalarValue::try_from_array(headers[0].column(3), 0).unwrap(),
        ScalarValue::Int64(Some(4))
    );
    // JDK HashMap iteration visits 1, +large, -large. Fold these into the retained raw sum;
    // folding the input order or merging the local scalar gives a different answer.
    assert_eq!(
        ScalarValue::try_from_array(headers[0].column(5), 0).unwrap(),
        expected
    );
    for slot in [0, 1] {
        let frames = owner.capture(Some(slot)).unwrap();
        assert_eq!(frames.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
        assert!(frames
            .iter()
            .any(|frame| (0..frame.num_rows()).any(|row| frame.column(1).is_null(row))));
        for frame in frames {
            let counts = frame
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert!((0..counts.len()).all(|row| counts.value(row) == 1));
        }
    }
    assert_eq!(owner.staging_bytes(), 0);
}

#[test]
fn stock_global_floating_distinct_uses_map_order_and_retained_raw_sum() {
    for code in [1, 6] {
        floating_exercise(floating_owner(code), code);
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn rocks_stock_global_floating_distinct_uses_map_order_and_retained_raw_sum() {
    use crate::state::RocksStoreConfig;
    for code in [1, 6] {
        let directory = tempfile::tempdir().unwrap();
        let store = RocksGroupStore::create(
            RocksStoreConfig {
                table_dir: directory
                    .path()
                    .join("state")
                    .to_string_lossy()
                    .into_owned(),
                max_parallelism: 128,
                options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 0,
                shared_resources: 0,
            },
            GroupStateCodec::new(
                vec![7, 9],
                vec![value_data_type(code); 2],
                vec![1, 2],
                vec![3, 3],
            ),
        )
        .unwrap();
        floating_exercise(floating_owner(code).with_backend(store), code);
    }
}

#[test]
fn stock_local_distinct_transports_null_bits_and_filters_once_into_global() {
    for code in [2, 1, 6] {
        let mut local = LocalGroupAggregator::new(
            vec![7, 9],
            vec![code; 2],
            vec![1, 1],
            vec![2, -1],
            vec![0],
            vec![0, 1],
        );
        local.configure_stock_append_distinct().unwrap();
        let values: ArrayRef = match code {
            2 => Arc::new(Int32Array::from(vec![None, None, Some(2), Some(2)])),
            1 => Arc::new(Float64Array::from(vec![None, None, Some(2.0), Some(2.0)])),
            6 => Arc::new(Float32Array::from(vec![None, None, Some(2.0), Some(2.0)])),
            _ => unreachable!(),
        };
        local
            .update(
                &RecordBatch::try_from_iter(vec![
                    ("key", Arc::new(Int64Array::from(vec![1; 4])) as ArrayRef),
                    ("value", values),
                    (
                        "filter",
                        Arc::new(BooleanArray::from(vec![true, false, true, false])),
                    ),
                ])
                .unwrap(),
            )
            .unwrap();
        let partial = local.try_flush().unwrap();
        assert_eq!(partial.num_rows(), 1);
        for column in [3, 4] {
            let list = group_partial_map(partial.column(column));
            assert_eq!(list.values().len(), 2);
            let entries = list
                .values()
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .unwrap();
            assert_eq!(entries.column(0).null_count(), 1);
            let counts = entries
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert!((0..counts.len()).all(|row| counts.value(row) == 1));
        }
        let mut global = GroupAggregator::new(vec![7, 9], vec![code; 2], vec![1, 2], vec![0], true)
            .with_distinct_view_columns(vec![3, 4])
            .with_key_timestamp_precisions(vec![i32::MIN, -1])
            .with_mini_batch();
        global.configure_stock_global_merge().unwrap();
        global.configure_typed(&[7, 9], true).unwrap();
        global.update(&partial, 0).unwrap();
        let result = global.flush_mini_batch().unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            ScalarValue::try_from_array(result.column(1), 0).unwrap(),
            ScalarValue::Int64(Some(1))
        );
        let expected = match code {
            2 => ScalarValue::Int32(Some(2)),
            1 => ScalarValue::Float64(Some(2.0)),
            6 => ScalarValue::Float32(Some(2.0)),
            _ => unreachable!(),
        };
        assert_eq!(
            ScalarValue::try_from_array(result.column(2), 0).unwrap(),
            expected
        );
    }
}
