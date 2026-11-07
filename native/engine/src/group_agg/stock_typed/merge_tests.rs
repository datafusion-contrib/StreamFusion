use super::*;
use arrow::array::Float64Array;

fn strings(values: &[(&str, Option<Vec<u16>>)]) -> RecordBatch {
    let cache = streamfusion_bridge::string_cache::string_cache_array(
        values.iter().map(|(_, cache)| cache.as_deref()),
    );
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("value", DataType::Utf8, true),
            streamfusion_bridge::string_cache::string_cache_field(1, cache.data_type().clone()),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1; values.len()])),
            Arc::new(StringArray::from(
                values
                    .iter()
                    .map(|(value, _)| Some(*value))
                    .collect::<Vec<_>>(),
            )),
            cache,
        ],
    )
    .unwrap()
}

fn cache(batch: &RecordBatch, field: usize, row: usize) -> Option<Vec<u16>> {
    streamfusion_bridge::string_cache::string_cache_column(batch, field)
        .and_then(|array| {
            streamfusion_bridge::string_cache::JavaStringCacheColumn::new(array).value(row)
        })
        .map(<[u16]>::to_vec)
}

fn owner(
    kinds: Vec<i64>,
    types: Vec<i64>,
    values: Vec<i64>,
    counts: Vec<i64>,
    global: bool,
) -> GroupAggregator {
    let mut owner = GroupAggregator::new(kinds.clone(), types, values, vec![0], true)
        .with_count_columns(counts)
        .with_mini_batch()
        .with_memory_budget(16 << 20)
        .unwrap();
    if global {
        owner.configure_stock_global_merge().unwrap();
    }
    owner.configure_typed(&kinds, true).unwrap();
    owner
}

fn frames(owner: &GroupAggregator) -> Vec<RecordBatch> {
    let mut frames = vec![];
    owner
        .visit_typed(None, |batch| {
            frames.push(batch);
            Ok(())
        })
        .unwrap();
    frames
}

#[test]
fn local_running_string_partials_keep_java_order_and_companions() {
    let mut local = LocalGroupAggregator::new(
        vec![10, 11, 3],
        vec![3, 3, 0],
        vec![1, 1, -1],
        vec![],
        vec![0],
        vec![],
    )
    .with_memory_budget(16 << 20)
    .unwrap();
    local
        .update(&strings(&[("\u{e000}", Some(vec![0xe000]))]))
        .unwrap();
    local
        .update(&strings(&[("\u{10000}", Some(vec![0xd800, 0xdc00]))]))
        .unwrap();
    let partial = local.try_flush().unwrap();
    assert_eq!(partial.num_rows(), 1);
    assert_eq!(
        ScalarValue::try_from_array(partial.column(1), 0).unwrap(),
        ScalarValue::Utf8(Some("\u{10000}".into()))
    );
    assert_eq!(cache(&partial, 1, 0), Some(vec![0xd800, 0xdc00]));
    assert_eq!(cache(&partial, 2, 0), Some(vec![0xe000]));
    assert_eq!(
        ScalarValue::try_from_array(partial.column(3), 0).unwrap(),
        ScalarValue::Int64(Some(2))
    );
}

#[test]
fn mini_batch_string_preimages_keep_cache_and_utf16_only_changes() {
    let mut native = owner(vec![10, 11], vec![3, 3], vec![1, 1], vec![], false);
    native
        .update(&strings(&[("?", Some(vec![0xd800]))]), 0)
        .unwrap();
    let first = native.flush_mini_batch().unwrap();
    assert_eq!(first.num_rows(), 1);
    native
        .update(&strings(&[("?", Some(vec![0xd801]))]), 1)
        .unwrap();
    let output = native.flush_mini_batch().unwrap();
    assert_eq!(output.num_rows(), 2);
    assert_eq!(cache(&output, 2, 0), Some(vec![0xd800]));
    assert_eq!(cache(&output, 2, 1), Some(vec![0xd801]));
    let measured: usize = native
        .store
        .iter()
        .map(|(key, state)| byte_key_bytes(&key.0) + group_key_state_bytes(state))
        .sum();
    assert_eq!(native.memory.state_bytes, measured);
    assert_eq!(native.staging_bytes(), 0);
}

#[test]
fn stock_global_string_merge_combines_its_bundle_before_reading_retained_value() {
    let mut native = owner(vec![10], vec![3], vec![1], vec![], true);
    native.update(&strings(&[("\u{e000}b", None)]), 0).unwrap();
    native.flush_mini_batch().unwrap();
    native
        .update(&strings(&[("\u{10000}", Some(vec![0xd800, 0xdc00]))]), 1)
        .unwrap();
    native
        .update(&strings(&[("\u{e000}a", Some(vec![0xe000, 0x61]))]), 1)
        .unwrap();
    assert!(
        native.visit_typed(None, |_| Ok(())).is_err(),
        "pending partials cannot enter canonical state"
    );
    let output = native.flush_mini_batch().unwrap();
    assert_eq!(output.num_rows(), 0);
    let exported = frames(&native);
    assert_eq!(
        ScalarValue::try_from_array(exported[0].column(3), 0).unwrap(),
        ScalarValue::Utf8(Some("\u{e000}b".into()))
    );
}

fn numeric_batch(sum: Vec<f64>, counts: Vec<i64>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from(vec![1; sum.len()])) as ArrayRef,
        ),
        ("sum", Arc::new(Float64Array::from(sum.clone()))),
        ("avg", Arc::new(Float64Array::from(sum))),
        ("count", Arc::new(Int64Array::from(counts))),
    ])
    .unwrap()
}

#[test]
fn global_float_merge_keeps_both_rounding_stages_and_raw_average_count() {
    let mut native = owner(vec![0, 4], vec![1, 1], vec![1, 2], vec![-1, 3], true);
    native
        .update(&numeric_batch(vec![1e16], vec![1]), 0)
        .unwrap();
    native.flush_mini_batch().unwrap();
    native
        .update(&numeric_batch(vec![-1e16, 1.0], vec![1, 1]), 1)
        .unwrap();
    let output = native.flush_mini_batch().unwrap();
    assert_eq!(output.num_rows(), 2);
    assert_eq!(
        ScalarValue::try_from_array(output.column(1), 1).unwrap(),
        ScalarValue::Float64(Some(0.0))
    );
    assert_eq!(
        ScalarValue::try_from_array(output.column(2), 1).unwrap(),
        ScalarValue::Float64(Some(0.0))
    );
    let exported = frames(&native);
    assert_eq!(
        ScalarValue::try_from_array(exported[0].column(6), 0).unwrap(),
        ScalarValue::Int64(Some(3))
    );
}

#[test]
fn global_average_counts_wrap_in_buffer_and_state_instead_of_panicking() {
    let mut native = owner(vec![4], vec![1], vec![2], vec![3], true);
    native
        .update(&numeric_batch(vec![1.0, 2.0], vec![i64::MAX, 1]), 0)
        .unwrap();
    native.flush_mini_batch().unwrap();
    let exported = frames(&native);
    assert_eq!(
        ScalarValue::try_from_array(exported[0].column(4), 0).unwrap(),
        ScalarValue::Int64(Some(i64::MIN))
    );
    native
        .update(&numeric_batch(vec![7.0], vec![-1]), 1)
        .unwrap();
    native.flush_mini_batch().unwrap();
    let exported = frames(&native);
    assert_eq!(
        ScalarValue::try_from_array(exported[0].column(4), 0).unwrap(),
        ScalarValue::Int64(Some(i64::MAX))
    );
}

#[test]
fn drained_global_canonical_and_native_checkpoint_restore_keep_future_merges() {
    let kinds = vec![0, 4];
    let types = vec![1, 1];
    let values = vec![1, 2];
    let counts = vec![-1, 3];
    let mut source = owner(
        kinds.clone(),
        types.clone(),
        values.clone(),
        counts.clone(),
        true,
    );
    source
        .update(&numeric_batch(vec![3.0, 5.0], vec![1, 2]), 0)
        .unwrap();
    source.flush_mini_batch().unwrap();
    let exported = frames(&source);
    let mut canonical = owner(
        kinds.clone(),
        types.clone(),
        values.clone(),
        counts.clone(),
        true,
    );
    for frame in &exported {
        canonical.transfer_typed_headers(frame, false).unwrap();
    }
    let mut checkpoint = GroupAggregator::restore(
        kinds.clone(),
        types,
        values,
        vec![0],
        true,
        &source.snapshot(),
        0,
    )
    .with_count_columns(counts)
    .with_mini_batch()
    .with_memory_budget(16 << 20)
    .unwrap();
    checkpoint.configure_stock_global_merge().unwrap();
    checkpoint.configure_typed(&kinds, true).unwrap();
    for batch in [
        numeric_batch(vec![11.0], vec![2]),
        numeric_batch(vec![-5.0, 1.0], vec![0, 1]),
    ] {
        source.update(&batch, 1).unwrap();
        canonical.update(&batch, 1).unwrap();
        checkpoint.update(&batch, 1).unwrap();
        let expected = source.flush_mini_batch().unwrap();
        assert_eq!(expected, canonical.flush_mini_batch().unwrap());
        assert_eq!(expected, checkpoint.flush_mini_batch().unwrap());
        assert_eq!(frames(&source), frames(&canonical));
        assert_eq!(frames(&source), frames(&checkpoint));
    }
}

#[test]
fn local_string_empty_barrier_before_first_input_is_safe() {
    let mut local = LocalGroupAggregator::new(
        vec![10, 11],
        vec![3, 3],
        vec![1, 1],
        vec![],
        vec![0],
        vec![],
    )
    .with_memory_budget(16 << 20)
    .unwrap();
    assert_eq!(local.try_flush().unwrap().num_rows(), 0);
    local.update(&strings(&[("x", Some(vec![0x78]))])).unwrap();
    let first = local.try_flush().unwrap();
    assert_eq!(first.num_rows(), 1);
    assert_eq!(cache(&first, 1, 0), Some(vec![0x78]));
    assert_eq!(local.try_flush().unwrap().num_rows(), 0);
    assert_eq!(local.memory.state_bytes, 0);
}
