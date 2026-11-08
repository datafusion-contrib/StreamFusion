use super::*;

fn owner(kinds: Vec<i64>, ttl: i64) -> GroupAggregator {
    let width = kinds.len();
    GroupAggregator::new(kinds, vec![0; width], vec![1; width], vec![0], true).with_state_ttl(ttl)
}

fn key(value: i64) -> ByteKey {
    let mut bytes = vec![0; 16];
    bytes[8..].copy_from_slice(&value.to_le_bytes());
    ByteKey::from(bytes.as_slice())
}

fn seed(owner: &mut GroupAggregator, id: i64, records: i64, stamp: i64) {
    let state = owner.create(key(id));
    state.records = records;
    state.last_write_ms = stamp;
}

fn frame(owner: &GroupAggregator) -> RecordBatch {
    let mut entries = owner.store.iter().collect::<Vec<_>>();
    entries.sort_by(|(a, _), (b, _)| a.0.cmp(&b.0));
    transfer_batch(
        &owner.kinds,
        owner.ttl_ms > 0,
        owner.stock_append_only == Some(true),
        &entries,
    )
    .unwrap()
}

fn replace(batch: &RecordBatch, index: usize, array: ArrayRef) -> RecordBatch {
    let mut columns = batch.columns().to_vec();
    let mut fields = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    fields[index] = Field::new(fields[index].name(), array.data_type().clone(), true);
    columns[index] = array;
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn raw_nullable_sum_average_counter_and_independent_record_count_round_trip() {
    let kinds = vec![0, 4, 3, 22];
    let mut source = owner(kinds.clone(), 1000);
    seed(&mut source, 1, 2, 1500);
    let state = source.store.get_mut(&key(1).0).unwrap();
    for (i, (scalar, count)) in [
        (None, 0),
        (Some(-15), 2),
        (Some(i64::MAX), i64::MAX),
        (Some(42), i64::MIN),
    ]
    .into_iter()
    .enumerate()
    {
        let GroupAggState::Running { agg, non_null } = &mut state.aggs[i] else {
            unreachable!()
        };
        agg.restore_value(&ScalarValue::Int64(scalar));
        *non_null = count;
    }
    let batch = frame(&source);
    let mut target = owner(kinds, 1000);
    target.transfer_arithmetic(&batch, true).unwrap();
    assert_eq!(target.store.iter().count(), 0);
    target.transfer_arithmetic(&batch, false).unwrap();
    assert_eq!(batch, frame(&target));
    let state = target.store.get(&key(1).0).unwrap();
    assert!(state.last_output.is_none());
    let GroupAggState::Running { agg, .. } = &state.aggs[1] else {
        unreachable!()
    };
    assert_eq!(agg.emit(), ScalarValue::Int64(Some(-15)));
    assert_eq!(state.records, 2);
}

#[test]
fn ttl_off_projection_suppresses_migration_age_without_mutating_native_state() {
    let mut source = owner(vec![0], 0);
    seed(&mut source, 1, 0, 5000);
    let batch = frame(&source);
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        0
    );
    assert_eq!(source.store.get(&key(1).0).unwrap().last_write_ms, 5000);
    let mut target = owner(vec![0], 0);
    target.transfer_arithmetic(&batch, false).unwrap();
    assert_eq!(target.store.get(&key(1).0).unwrap().records, 0);
    let bad = replace(&batch, 2, Arc::new(Int64Array::from(vec![1])));
    assert!(owner(vec![0], 0).transfer_arithmetic(&bad, false).is_err());
}

#[test]
fn malformed_late_rows_and_required_fields_do_not_partially_insert() {
    let mut source = owner(vec![3], 1000);
    seed(&mut source, 1, 1, 1000);
    seed(&mut source, 2, -3, 1500);
    let batch = frame(&source);
    let mut bad_key = key(2).0.to_vec();
    bad_key[0] = 3;
    let bad = replace(
        &batch,
        0,
        Arc::new(BinaryArray::from_iter_values([
            key(1).0.as_ref(),
            bad_key.as_slice(),
        ])),
    );
    let mut target = owner(vec![3], 1000);
    assert!(target.transfer_arithmetic(&bad, false).is_err());
    assert_eq!(target.store.iter().count(), 0);
    for index in [1, 2, 3, 4] {
        let bad = replace(
            &batch,
            index,
            Arc::new(Int64Array::from(vec![Some(1), None])),
        );
        assert!(
            target.transfer_arithmetic(&bad, false).is_err(),
            "column {index}"
        );
        assert_eq!(target.store.iter().count(), 0);
    }
    let bad = replace(&batch, 1, Arc::new(Int32Array::from(vec![1, 1])));
    assert!(target.transfer_arithmetic(&bad, true).is_err());
    target.transfer_arithmetic(&batch, false).unwrap();
    assert_eq!(batch, frame(&target));
}

#[test]
fn bounded_chunks_preserve_zero_negative_and_maximum_counts_and_reject_prior_chunks() {
    let mut source = owner(vec![0], 1000);
    for id in 0..(CHUNK_ROWS + 1) {
        seed(&mut source, id as i64, [0, -3, i64::MAX][id % 3], 1000);
    }
    let mut batches = Vec::new();
    visit(
        &source.kinds,
        true,
        source.stock_append_only == Some(true),
        source.store.iter(),
        |batch| {
            batches.push(batch);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        [CHUNK_ROWS, 1]
    );
    let mut target = owner(vec![0], 1000);
    for batch in &batches {
        target.transfer_arithmetic(batch, true).unwrap();
    }
    for batch in &batches {
        target.transfer_arithmetic(batch, false).unwrap();
    }
    assert!(target.transfer_arithmetic(&batches[0], false).is_err());
    assert_eq!(frame(&source), frame(&target));
    let oversized = frame(&source);
    assert!(owner(vec![0], 1000)
        .transfer_arithmetic(&oversized, false)
        .is_err());
}

#[test]
fn undrained_input_other_aggregate_contracts_and_resource_denial_are_explicit() {
    assert!(owner(vec![1], 0).arithmetic_contract().is_err());
    assert!(
        GroupAggregator::new(vec![0], vec![2], vec![1], vec![0], true)
            .arithmetic_contract()
            .is_err()
    );
    let mut staged = owner(vec![0], 0).with_mini_batch();
    staged.staged_order.push(key(1));
    assert!(staged.arithmetic_contract().is_err());
    assert!(owner(vec![0], 0)
        .with_record_count_column(2)
        .arithmetic_contract()
        .is_err());
    let mut source = owner(vec![0], 0);
    seed(&mut source, 1, 1, 0);
    let batch = frame(&source);
    for kind in [0, 3] {
        let mut source = owner(vec![kind], 0);
        for id in 0..1025 {
            seed(&mut source, id, 1, 0);
        }
        let all = frame(&source);
        let mut denied = owner(vec![kind], 0).with_memory_budget(64).unwrap();
        for offset in [0, 1024] {
            let count = (1025 - offset).min(1024);
            let part = all.slice(offset, count);
            denied.transfer_arithmetic(&part, true).unwrap();
        }
        assert!(
            matches!(
                denied.transfer_arithmetic(&all.slice(0, 1024), false),
                Err(DataFusionError::ResourcesExhausted(_))
            ),
            "kind={kind} bytes={}",
            denied.memory.state_bytes
        );
    }
    let mut denied = owner(vec![0], 0).with_memory_budget(0).unwrap();
    assert!(matches!(
        denied.transfer_arithmetic(&batch, false),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    drop(denied);
    let mut retry = owner(vec![0], 0).with_memory_budget(1_000_000).unwrap();
    retry.transfer_arithmetic(&batch, false).unwrap();
    assert_eq!(frame(&retry), batch);
}

fn input(value: Option<i64>, kind: i8) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("k", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("v", Arc::new(Int64Array::from(vec![value]))),
        (ROW_KIND_COLUMN, Arc::new(Int8Array::from(vec![kind]))),
    ])
    .unwrap()
}

#[test]
fn null_only_and_cancelled_sums_remain_distinct_through_repeated_switches() {
    let kinds = vec![0, 4, 3];
    for ttl in [0, 1000] {
        let mut baseline = owner(kinds.clone(), ttl);
        let mut switched = owner(kinds.clone(), ttl);
        for (step, (value, kind)) in [
            (None, 0),
            (Some(7), 0),
            (Some(3), 0),
            (Some(7), 3),
            (Some(3), 3),
            (None, 3),
        ]
        .into_iter()
        .enumerate()
        {
            let batch = input(value, kind);
            let now = 1000 + step as i64 * 100;
            assert_eq!(
                baseline.update(&batch, now).unwrap(),
                switched.update(&batch, now).unwrap()
            );
            let transferred = frame(&switched);
            let mut next = owner(kinds.clone(), ttl);
            next.transfer_arithmetic(&transferred, false).unwrap();
            assert_eq!(frame(&baseline), frame(&next));
            if step == 0 || step == 4 {
                let value = transferred
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(value.is_null(0), step == 0);
                if step == 4 {
                    assert_eq!(value.value(0), 0);
                }
            }
            switched = next;
        }
        assert_eq!(switched.store.iter().count(), 0);
    }
}

#[test]
fn restored_long_counters_wrap_on_continuation_like_flink() {
    for (initial, kind, expected) in [(i64::MAX, 0, i64::MIN), (i64::MIN, 3, i64::MAX)] {
        let mut source = owner(vec![3], 0);
        seed(&mut source, 1, initial, 0);
        let GroupAggState::Running { agg, non_null } =
            &mut source.store.get_mut(&key(1).0).unwrap().aggs[0]
        else {
            unreachable!()
        };
        agg.restore_value(&ScalarValue::Int64(Some(initial)));
        *non_null = initial;
        let mut target = owner(vec![3], 0);
        target.transfer_arithmetic(&frame(&source), false).unwrap();
        target.update(&input(Some(7), kind), 0).unwrap();
        let state = target.store.get(&key(1).0).unwrap();
        assert_eq!(state.records, expected);
        let GroupAggState::Running { agg, non_null } = &state.aggs[0] else {
            unreachable!()
        };
        assert_eq!(agg.emit(), ScalarValue::Int64(Some(expected)));
        assert_eq!(*non_null, expected);
    }
}

#[test]
fn restored_integer_average_wraps_the_signed_division_boundary() {
    let mut source = owner(vec![4], 0);
    seed(&mut source, 1, 1, 0);
    let GroupAggState::Running { agg, non_null } =
        &mut source.store.get_mut(&key(1).0).unwrap().aggs[0]
    else {
        unreachable!()
    };
    agg.restore_value(&ScalarValue::Int64(Some(i64::MIN)));
    *non_null = -1;
    let mut restored = owner(vec![4], 0);
    restored
        .transfer_arithmetic(&frame(&source), false)
        .unwrap();
    restored.update(&input(None, 0), 0).unwrap();
    let state = restored.store.get(&key(1).0).unwrap();
    assert_eq!(
        state.aggs[0].emit(&DataType::Int64),
        ScalarValue::Int64(Some(i64::MIN))
    );
    assert_eq!(
        frame(&restored)
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MIN
    );
}

#[test]
fn append_presence_is_independent_of_zero_and_wrapped_aggregate_counts() {
    for kind in [3, 4] {
        for initial in [-1, i64::MAX] {
            let mut source = owner(vec![kind], 0);
            source.configure_arithmetic(&[kind], true).unwrap();
            seed(&mut source, 1, 1, 0);
            let GroupAggState::Running { agg, non_null } =
                &mut source.store.get_mut(&key(1).0).unwrap().aggs[0]
            else {
                unreachable!()
            };
            agg.restore_value(&ScalarValue::Int64(Some(if kind == 3 {
                initial
            } else {
                1
            })));
            *non_null = initial;
            let mut restored = owner(vec![kind], 0);
            restored.configure_arithmetic(&[kind], true).unwrap();
            restored
                .transfer_arithmetic(&frame(&source), false)
                .unwrap();
            restored.update(&input(Some(0), 0), 0).unwrap();
            let state = restored.store.get(&key(1).0).unwrap();
            assert_eq!(state.records, 1);
            let expected = if kind == 3 {
                Some(initial.wrapping_add(1))
            } else if initial == -1 {
                None
            } else {
                Some(0)
            };
            assert_eq!(
                state.aggs[0].emit(&DataType::Int64),
                ScalarValue::Int64(expected)
            );
            let mut returned = owner(vec![kind], 0);
            returned.configure_arithmetic(&[kind], true).unwrap();
            returned
                .transfer_arithmetic(&frame(&restored), false)
                .unwrap();
            assert_eq!(frame(&returned), frame(&restored));
        }
    }
}

#[test]
fn append_nullable_sum_retains_initialization_without_a_physical_non_null_count() {
    let mut state = owner(vec![0], 0);
    state.configure_arithmetic(&[0], true).unwrap();
    for (value, expected) in [
        (None, None),
        (Some(7), Some(7)),
        (Some(4), Some(11)),
        (Some(-11), Some(0)),
        (None, Some(0)),
        (Some(0), Some(0)),
        (Some(i64::MIN), Some(i64::MIN)),
        (Some(-1), Some(i64::MAX)),
    ] {
        state.update(&input(value, 0), 0).unwrap();
        let record = state.store.get(&key(1).0).unwrap();
        assert_eq!(record.records, 1);
        assert_eq!(
            record.aggs[0].emit(&DataType::Int64),
            ScalarValue::Int64(expected)
        );
        let mut next = owner(vec![0], 0);
        next.configure_arithmetic(&[0], true).unwrap();
        next.transfer_arithmetic(&frame(&state), false).unwrap();
        assert_eq!(frame(&next), frame(&state));
        state = next;
    }
}

#[test]
fn append_contract_rejects_bad_presence_and_whole_retracting_batches_before_mutation() {
    let mut source = owner(vec![0], 0);
    source.configure_arithmetic(&[0], true).unwrap();
    source.update(&input(Some(7), 0), 0).unwrap();
    let good = frame(&source);
    for (index, value) in [(1, 0), (1, -1), (4, 0)] {
        let bad = replace(&good, index, Arc::new(Int64Array::from(vec![value])));
        let mut target = owner(vec![0], 0);
        target.configure_arithmetic(&[0], true).unwrap();
        assert!(target.transfer_arithmetic(&bad, false).is_err());
        assert!(target.store.iter().next().is_none());
    }
    for kind in [Some(1), Some(3), Some(9), None] {
        let batch = RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef),
            ("v", Arc::new(Int64Array::from(vec![Some(4), Some(7)]))),
            (
                ROW_KIND_COLUMN,
                Arc::new(Int8Array::from(vec![Some(0), kind])),
            ),
        ])
        .unwrap();
        assert!(source.update(&batch, 0).is_err());
        assert_eq!(frame(&source), good);
    }
}

#[test]
fn append_configuration_preserves_kind_checks_and_cannot_reinterpret_a_bound_mode() {
    let mut state = owner(vec![0], 0);
    assert!(state.configure_arithmetic(&[3], true).is_err());
    assert_eq!(state.stock_append_only, None);
    state.configure_arithmetic(&[0], true).unwrap();
    state.configure_arithmetic(&[0], true).unwrap();
    assert!(state.configure_arithmetic(&[0], false).is_err());
    assert_eq!(state.stock_append_only, Some(true));
    let mut mini = owner(vec![0], 0).with_mini_batch();
    assert!(mini.configure_arithmetic(&[0], true).is_err());
    assert_eq!(mini.stock_append_only, None);
}

#[test]
fn native_checkpoint_restore_rebinds_append_liveness_without_changing_the_state_format() {
    let kinds = vec![0, 3, 4];
    let mut source = owner(kinds.clone(), 0);
    source.configure_arithmetic(&kinds, true).unwrap();
    for value in [None, Some(7), Some(4), Some(-11)] {
        source.update(&input(value, 0), 0).unwrap();
    }
    let before = frame(&source);
    let saved = source.snapshot();
    let mut restored = GroupAggregator::restore(
        kinds.clone(),
        vec![0; 3],
        vec![1; 3],
        vec![0],
        true,
        &saved,
        0,
    );
    assert_eq!(restored.stock_append_only, None);
    restored.configure_arithmetic(&kinds, true).unwrap();
    assert_eq!(frame(&restored), before);
    for value in [None, Some(0), Some(5)] {
        assert_eq!(
            source.update(&input(value, 0), 0).unwrap(),
            restored.update(&input(value, 0), 0).unwrap()
        );
        assert_eq!(frame(&source), frame(&restored));
    }
}

#[test]
fn append_min_max_preserve_null_initialization_and_signed_extrema_across_switches() {
    let kinds = vec![10, 11];
    for ttl in [0, 1000] {
        let mut state = owner(kinds.clone(), ttl);
        state.configure_arithmetic(&kinds, true).unwrap();
        for (index, (value, min, max)) in [
            (None, None, None),
            (Some(7), Some(7), Some(7)),
            (Some(-11), Some(-11), Some(7)),
            (None, Some(-11), Some(7)),
            (Some(i64::MIN), Some(i64::MIN), Some(7)),
            (Some(i64::MAX), Some(i64::MIN), Some(i64::MAX)),
        ]
        .into_iter()
        .enumerate()
        {
            state.update(&input(value, 0), 1000 + index as i64).unwrap();
            let row = state.store.get(&key(1).0).unwrap();
            assert_eq!(row.records, 1);
            for (aggregate, expected) in [(0, min), (1, max)] {
                assert_eq!(
                    row.aggs[aggregate].emit(&DataType::Int64),
                    ScalarValue::Int64(expected)
                );
            }
            let mut next = owner(kinds.clone(), ttl);
            next.configure_arithmetic(&kinds, true).unwrap();
            next.transfer_arithmetic(&frame(&state), true).unwrap();
            next.transfer_arithmetic(&frame(&state), false).unwrap();
            assert_eq!(frame(&state), frame(&next));
            state = next;
        }
    }
}

#[test]
fn min_max_scalar_contract_rejects_retracting_or_inconsistent_initialization() {
    for kind in [10, 11] {
        let mut state = owner(vec![kind], 0);
        assert!(state.configure_arithmetic(&[kind], false).is_err());
        assert_eq!(state.stock_append_only, None);
        assert!(state.arithmetic_contract().is_err());
        state.configure_arithmetic(&[kind], true).unwrap();
        state.update(&input(Some(7), 0), 0).unwrap();
        let good = frame(&state);
        for (column, value) in [(1, 0), (4, 0), (4, -1)] {
            let bad = replace(&good, column, Arc::new(Int64Array::from(vec![value])));
            let mut target = owner(vec![kind], 0);
            target.configure_arithmetic(&[kind], true).unwrap();
            assert!(target.transfer_arithmetic(&bad, false).is_err());
            assert!(target.store.iter().next().is_none());
        }
        for row_kind in [1, 3] {
            assert!(state.update(&input(Some(7), row_kind), 0).is_err());
            assert_eq!(good, frame(&state));
        }
    }
}

#[test]
fn min_max_native_checkpoint_rebinds_initialization_before_cached_before_images() {
    let kinds = vec![10, 11];
    let mut source = owner(kinds.clone(), 0);
    source.update(&input(Some(7), 0), 0).unwrap();
    source.update(&input(Some(-11), 0), 0).unwrap();
    for state in &mut source.store.get_mut(&key(1).0).unwrap().aggs {
        let GroupAggState::Running { non_null, .. } = state else {
            unreachable!()
        };
        *non_null = 0;
    }
    let saved = source.snapshot();
    let mut restored = GroupAggregator::restore(
        kinds.clone(),
        vec![0; 2],
        vec![1; 2],
        vec![0],
        true,
        &saved,
        0,
    );
    restored.configure_arithmetic(&kinds, true).unwrap();
    let output = restored.update(&input(None, 0), 0).unwrap();
    assert_eq!(
        output.num_rows(),
        0,
        "normalizing private counters must not emit a false update"
    );
    let state = restored.store.get(&key(1).0).unwrap();
    assert_eq!(
        state.aggs[0].emit(&DataType::Int64),
        ScalarValue::Int64(Some(-11))
    );
    assert_eq!(
        state.aggs[1].emit(&DataType::Int64),
        ScalarValue::Int64(Some(7))
    );
}

#[test]
fn independent_memory_captures_retain_raw_counters_ages_and_deleted_keys() {
    use crate::state::canonical_memory::CanonicalMemoryState;
    let mut live = owner(vec![0, 3], 1000);
    seed(&mut live, 1, 2, 100);
    let expected_first = frame(&live);
    let mut first = owner(vec![0, 3], 1000);
    live.copy_state(&mut first).unwrap();
    seed(&mut live, 1, 3, 200);
    seed(&mut live, 2, 1, 200);
    let expected_second = frame(&live);
    let mut second = owner(vec![0, 3], 1000);
    live.copy_state(&mut second).unwrap();
    live.store.remove(&key(1).0);
    seed(&mut live, 2, 4, 300);
    drop(live);
    assert_eq!(frame(&second), expected_second);
    assert_eq!(frame(&first), expected_first);
}

#[test]
fn memory_capture_rejects_an_insufficient_budget_before_copying_resident_groups() {
    use crate::state::canonical_memory::CanonicalMemoryState;
    let mut live = owner(vec![0], 0);
    seed(&mut live, 1, 2, 100);
    live.memory.set(1024);
    let mut target = owner(vec![0], 0).with_memory_budget(1).unwrap();
    assert!(live.copy_state(&mut target).is_err());
    assert_eq!(target.store.iter().count(), 0);
    assert_eq!(live.store.iter().count(), 1);
}
