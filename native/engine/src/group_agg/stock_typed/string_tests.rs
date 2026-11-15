use super::*;

fn owner() -> GroupAggregator {
    let mut native = GroupAggregator::new(
        vec![3, 10, 11],
        vec![0, 3, 3],
        vec![-1, 1, 1],
        vec![0],
        true,
    );
    native.configure_typed(&[3, 10, 11], true).unwrap();
    native
}

fn input(values: Vec<Option<&str>>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from(vec![1; values.len()])) as ArrayRef,
        ),
        ("value", Arc::new(StringArray::from(values))),
    ])
    .unwrap()
}

fn frames(owner: &GroupAggregator) -> Vec<RecordBatch> {
    let mut frames = vec![];
    owner
        .visit_typed(None, |frame| {
            frames.push(frame);
            Ok(())
        })
        .unwrap();
    frames
}

#[test]
fn string_running_selection_uses_binary_utf8_order_and_ignores_nulls() {
    let mut native = owner();
    let result = native
        .update(
            &input(vec![None, Some("\u{10000}"), Some("\u{e000}"), None]),
            0,
        )
        .unwrap();
    assert_eq!(
        ScalarValue::try_from_array(result.column(2), result.num_rows() - 1).unwrap(),
        ScalarValue::Utf8(Some("\u{e000}".into()))
    );
    assert_eq!(
        ScalarValue::try_from_array(result.column(3), result.num_rows() - 1).unwrap(),
        ScalarValue::Utf8(Some("\u{10000}".into()))
    );
    let result = native.update(&input(vec![Some(""), None]), 1).unwrap();
    assert_eq!(
        ScalarValue::try_from_array(result.column(2), result.num_rows() - 1).unwrap(),
        ScalarValue::Utf8(Some("".into()))
    );
}

#[test]
fn nonnull_string_headers_and_native_checkpoints_restore_then_continue() {
    let mut source = owner();
    source
        .update(
            &input(vec![
                None,
                Some("\u{10000}"),
                Some("\u{e000}"),
                Some("same"),
            ]),
            1000,
        )
        .unwrap();
    let expected = frames(&source);
    let mut target = owner().with_memory_budget(8 << 20).unwrap();
    for frame in &expected {
        target.transfer_typed_headers(frame, true).unwrap();
    }
    assert_eq!(target.store.iter().count(), 0);
    for frame in &expected {
        target.transfer_typed_headers(frame, false).unwrap();
    }
    assert_eq!(expected, frames(&target));
    let saved = source.snapshot();
    let mut recovered = GroupAggregator::restore(
        vec![3, 10, 11],
        vec![0, 3, 3],
        vec![-1, 1, 1],
        vec![0],
        true,
        &saved,
        1000,
    );
    recovered.configure_typed(&[3, 10, 11], true).unwrap();
    assert_eq!(expected, frames(&recovered));
    for values in [
        vec![None],
        vec![Some("")],
        vec![Some("z")],
        vec![Some("\u{10000}long"), Some("same")],
    ] {
        let batch = input(values);
        let expected = source.update(&batch, 1100).unwrap();
        assert_eq!(expected, target.update(&batch, 1100).unwrap());
        assert_eq!(expected, recovered.update(&batch, 1100).unwrap());
        assert_eq!(frames(&source), frames(&target));
        assert_eq!(frames(&source), frames(&recovered));
    }
}

#[test]
fn failed_string_header_receiver_leaves_live_state_exportable() {
    let mut source = owner();
    let value = "long-value-".repeat(1024);
    source.update(&input(vec![Some(&value)]), 0).unwrap();
    let before = frames(&source);
    let error = source
        .visit_typed(None, |_| Err(invalid("string receiver sentinel")))
        .unwrap_err();
    assert!(error.to_string().contains("string receiver sentinel"));
    assert_eq!(before, frames(&source));
}

#[test]
fn owned_string_payloads_remain_in_the_state_budget_after_replacement_and_restore() {
    let value = "z".repeat(100000);
    let mut native = owner().with_memory_budget(8 << 20).unwrap();
    for batch in [
        input(vec![Some(&value)]),
        input(vec![Some("a")]),
        input(vec![Some("z")]),
        input(vec![None]),
    ] {
        native.update(&batch, 0).unwrap();
        let measured: usize = native
            .store
            .iter()
            .map(|(key, state)| byte_key_bytes(&key.0) + group_key_state_bytes(state))
            .sum();
        assert_eq!(native.memory.state_bytes, measured);
        assert!(native.memory.state_bytes >= value.len());
    }
    let saved = native.snapshot();
    let mut recovered = GroupAggregator::restore(
        vec![3, 10, 11],
        vec![0, 3, 3],
        vec![-1, 1, 1],
        vec![0],
        true,
        &saved,
        0,
    )
    .with_memory_budget(8 << 20)
    .unwrap();
    recovered.configure_typed(&[3, 10, 11], true).unwrap();
    let measured: usize = recovered
        .store
        .iter()
        .map(|(key, state)| byte_key_bytes(&key.0) + group_key_state_bytes(state))
        .sum();
    assert_eq!(recovered.memory.state_bytes, measured);
    let mut denied = owner().with_memory_budget(1024).unwrap();
    assert!(denied.update(&input(vec![Some(&value)]), 0).is_err());
}

fn cached_input(values: &[(&str, Option<Vec<u16>>)]) -> RecordBatch {
    let batch = input(values.iter().map(|(value, _)| Some(*value)).collect());
    let cache = streamfusion_bridge::string_cache::string_cache_array(
        values.iter().map(|(_, cache)| cache.as_deref()),
    );
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(
        streamfusion_bridge::string_cache::string_cache_field(1, cache.data_type().clone()),
    ));
    let mut columns = batch.columns().to_vec();
    columns.push(cache);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn cache_at(batch: &RecordBatch, field: usize, row: usize) -> Option<Vec<u16>> {
    streamfusion_bridge::string_cache::string_cache_column(batch, field)
        .and_then(|column| {
            streamfusion_bridge::string_cache::JavaStringCacheColumn::new(column).value(row)
        })
        .map(<[u16]>::to_vec)
}

#[test]
fn java_cached_extrema_preserve_utf16_order_and_both_changelog_images() {
    let low = "\u{10000}";
    let high = "\u{e000}";
    let low_cache = low.encode_utf16().collect::<Vec<_>>();
    let high_cache = high.encode_utf16().collect::<Vec<_>>();
    let mut native = owner().with_memory_budget(8 << 20).unwrap();
    let result = native
        .update(
            &cached_input(&[
                (low, Some(low_cache.clone())),
                (high, Some(high_cache.clone())),
            ]),
            0,
        )
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(
        ScalarValue::try_from_array(result.column(2), 2).unwrap(),
        ScalarValue::Utf8(Some(low.into()))
    );
    assert_eq!(
        ScalarValue::try_from_array(result.column(3), 2).unwrap(),
        ScalarValue::Utf8(Some(high.into()))
    );
    for (row, min, max) in [
        (0, &low_cache, &low_cache),
        (1, &low_cache, &low_cache),
        (2, &low_cache, &high_cache),
    ] {
        assert_eq!(cache_at(&result, 2, row).as_ref(), Some(min));
        assert_eq!(cache_at(&result, 3, row).as_ref(), Some(max));
    }
    let measured: usize = native
        .store
        .iter()
        .map(|(key, state)| byte_key_bytes(&key.0) + group_key_state_bytes(state))
        .sum();
    assert_eq!(native.memory.state_bytes, measured);
    let mixed = native.update(&cached_input(&[(high, None)]), 1).unwrap();
    assert_eq!(
        ScalarValue::try_from_array(mixed.column(2), 1).unwrap(),
        ScalarValue::Utf8(Some(high.into()))
    );
    assert_eq!(cache_at(&mixed, 2, 0), Some(low_cache));
    assert_eq!(cache_at(&mixed, 2, 1), None);
    assert_eq!(cache_at(&mixed, 3, 1), Some(high_cache));
}

#[test]
fn canonical_and_native_restore_clear_caches_without_mutating_live_source() {
    let low = "\u{10000}";
    let high = "\u{e000}";
    let mut source = owner();
    source
        .update(
            &cached_input(&[(low, Some(low.encode_utf16().collect()))]),
            0,
        )
        .unwrap();
    let exported = frames(&source);
    let mut canonical = owner();
    for frame in &exported {
        canonical.transfer_typed_headers(frame, false).unwrap();
    }
    let mut recovered = GroupAggregator::restore(
        vec![3, 10, 11],
        vec![0, 3, 3],
        vec![-1, 1, 1],
        vec![0],
        true,
        &source.snapshot(),
        0,
    );
    recovered.configure_typed(&[3, 10, 11], true).unwrap();
    let next = cached_input(&[(high, Some(high.encode_utf16().collect()))]);
    let live = source.update(&next, 1).unwrap();
    let restored = canonical.update(&next, 1).unwrap();
    assert_eq!(restored, recovered.update(&next, 1).unwrap());
    assert_eq!(
        ScalarValue::try_from_array(live.column(2), 1).unwrap(),
        ScalarValue::Utf8(Some(low.into()))
    );
    assert_eq!(
        ScalarValue::try_from_array(restored.column(2), 1).unwrap(),
        ScalarValue::Utf8(Some(high.into()))
    );
    assert_eq!(cache_at(&live, 2, 1), Some(low.encode_utf16().collect()));
    assert_eq!(cache_at(&restored, 2, 0), None);
    assert_eq!(
        cache_at(&restored, 2, 1),
        Some(high.encode_utf16().collect())
    );
}

#[test]
fn utf16_only_change_emits_without_a_visible_count() {
    let mut native = GroupAggregator::new(vec![10, 11], vec![3, 3], vec![1, 1], vec![0], true);
    native.configure_typed(&[10, 11], true).unwrap();
    let result = native
        .update(
            &cached_input(&[("?", Some(vec![0xd800])), ("?", Some(vec![0xd801]))]),
            0,
        )
        .unwrap();
    assert_eq!(
        result.num_rows(),
        3,
        "different Java strings with identical UTF8 must not suppress updates"
    );
    assert_eq!(cache_at(&result, 1, 0), Some(vec![0xd800]));
    assert_eq!(cache_at(&result, 2, 1), Some(vec![0xd800]));
    assert_eq!(cache_at(&result, 2, 2), Some(vec![0xd801]));
    let result = native.update(&cached_input(&[("?", None)]), 1).unwrap();
    assert_eq!(
        result.num_rows(),
        0,
        "a binary equal value does not change the winner"
    );
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn persistent_string_extrema_drop_java_cache_between_rows_of_one_arrow_batch() {
    let directory = tempfile::tempdir().unwrap();
    let store = RocksGroupStore::create(
        crate::state::RocksStoreConfig {
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
            vec![3, 10, 11],
            vec![DataType::Int64, DataType::Utf8, DataType::Utf8],
            vec![-1, 1, 1],
            vec![],
        ),
    )
    .unwrap();
    let mut native = owner()
        .with_backend(store)
        .with_read_through_budget(8 << 20)
        .unwrap();
    let first = "\u{10000}";
    let second = "\u{e000}";
    let result = native
        .update(
            &cached_input(&[
                (first, Some(first.encode_utf16().collect())),
                (second, Some(second.encode_utf16().collect())),
            ]),
            0,
        )
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(
        ScalarValue::try_from_array(result.column(2), 2).unwrap(),
        ScalarValue::Utf8(Some(second.into())),
        "the persisted stock accumulator is binary on each row read"
    );
    assert_eq!(
        ScalarValue::try_from_array(result.column(3), 2).unwrap(),
        ScalarValue::Utf8(Some(first.into()))
    );
    assert_eq!(
        cache_at(&result, 2, 1),
        None,
        "before image was read from serialized state"
    );
    assert_eq!(
        cache_at(&result, 2, 2),
        Some(second.encode_utf16().collect())
    );
    assert_eq!(
        cache_at(&result, 3, 2),
        None,
        "unchanged binary selection stays binary"
    );
}
