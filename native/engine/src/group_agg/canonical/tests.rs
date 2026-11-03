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
    transfer_batch(&owner.kinds, owner.ttl_ms > 0, &entries).unwrap()
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
    visit(&source.kinds, true, source.store.iter(), |batch| {
        batches.push(batch);
        Ok(())
    })
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
