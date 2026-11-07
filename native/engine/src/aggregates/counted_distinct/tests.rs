use super::*;
use arrow::array::Float64Array;

fn columns(value: &mut CountedDistinctAccumulator) -> Vec<ArrayRef> {
    value
        .state()
        .unwrap()
        .iter()
        .map(|value| value.to_array().unwrap())
        .collect()
}

fn entries(value: &CountedDistinctAccumulator) -> Vec<String> {
    let mut entries = value
        .entries()
        .iter()
        .map(|(value, count)| format!("{value:?}:{count}"))
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

#[test]
fn duplicate_updates_and_retractions_retain_exact_frequencies() {
    let mut value = CountedDistinctAccumulator::new(&DataType::Int64);
    value
        .update_batch(&[Arc::new(Int64Array::from(vec![
            None,
            Some(10),
            Some(10),
            Some(20),
            Some(10),
        ]))])
        .unwrap();
    assert_eq!(entries(&value), vec!["Int64(10):3", "Int64(20):1"]);
    assert_eq!(value.evaluate().unwrap(), ScalarValue::Int64(Some(2)));
    value
        .retract_batch(&[Arc::new(Int64Array::from(vec![10, 10]))])
        .unwrap();
    assert_eq!(value.evaluate().unwrap(), ScalarValue::Int64(Some(2)));
    value
        .retract_batch(&[Arc::new(Int64Array::from(vec![10]))])
        .unwrap();
    assert_eq!(value.evaluate().unwrap(), ScalarValue::Int64(Some(1)));
    value
        .retract_batch(&[Arc::new(Int64Array::from(vec![10]))])
        .unwrap();
    assert_eq!(entries(&value), vec!["Int64(10):-1", "Int64(20):1"]);
    value
        .update_batch(&[Arc::new(Int64Array::from(vec![10]))])
        .unwrap();
    assert_eq!(value.evaluate().unwrap(), ScalarValue::Int64(Some(1)));
}

#[test]
fn columnar_row_kinds_apply_insert_update_and_delete_deltas() {
    let mut value = CountedDistinctAccumulator::new(&DataType::Int64);
    value
        .update_batch(&[
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(10),
                Some(10),
                Some(20),
                Some(30),
                Some(30),
                None,
            ])),
            Arc::new(Int8Array::from(vec![0, 0, 1, 2, 3, 0, 3])),
        ])
        .unwrap();
    assert_eq!(entries(&value), vec!["Int64(10):1", "Int64(20):1"]);
    assert_eq!(value.evaluate().unwrap(), ScalarValue::Int64(Some(2)));
}

#[test]
fn malformed_row_kind_sidecars_are_rejected_before_changing_state() {
    let mut value = CountedDistinctAccumulator::new(&DataType::Int64);
    value
        .update_batch(&[Arc::new(Int64Array::from(vec![10]))])
        .unwrap();
    let before = entries(&value);
    for sidecar in [
        Arc::new(Int8Array::from(vec![0, 1])) as ArrayRef,
        Arc::new(Int8Array::from(vec![4])),
        Arc::new(Int8Array::from(vec![None])),
        Arc::new(Int64Array::from(vec![0])),
    ] {
        assert!(value
            .update_batch(&[Arc::new(Int64Array::from(vec![20])), sidecar])
            .is_err());
        assert_eq!(entries(&value), before);
    }
}

#[test]
fn pane_merge_combines_counts_and_counts_each_positive_value_once() {
    let mut left = CountedDistinctAccumulator::new(&DataType::Int64);
    let mut right = CountedDistinctAccumulator::new(&DataType::Int64);
    left.update_batch(&[Arc::new(Int64Array::from(vec![10, 10, 20]))])
        .unwrap();
    right
        .update_batch(&[Arc::new(Int64Array::from(vec![10, 30, 30]))])
        .unwrap();
    left.merge_batch(&columns(&mut right)).unwrap();
    assert_eq!(left.evaluate().unwrap(), ScalarValue::Int64(Some(3)));
    assert_eq!(
        entries(&left),
        vec!["Int64(10):3", "Int64(20):1", "Int64(30):2"]
    );
    assert_eq!(entries(&right), vec!["Int64(10):1", "Int64(30):2"]);
}

#[test]
fn snapshot_restore_preserves_declared_scalar_while_merge_uses_counter_transitions() {
    let mut value = CountedDistinctAccumulator::new(&DataType::Int64);
    value
        .import_entry(ScalarValue::Int64(Some(10)), i64::MAX)
        .unwrap();
    value.set_cardinality(1);
    value
        .update_batch(&[Arc::new(Int64Array::from(vec![10]))])
        .unwrap();
    assert_eq!(entries(&value), vec![format!("Int64(10):{}", i64::MIN)]);
    assert_eq!(value.evaluate().unwrap(), ScalarValue::Int64(Some(1)));
    let state = columns(&mut value);
    let mut restored = CountedDistinctAccumulator::new(&DataType::Int64);
    restored.restore_exact(&state).unwrap();
    assert_eq!(restored.evaluate().unwrap(), ScalarValue::Int64(Some(1)));
    assert_eq!(entries(&value), entries(&restored));
    let mut merged = CountedDistinctAccumulator::new(&DataType::Int64);
    merged.merge_batch(&state).unwrap();
    assert_eq!(merged.evaluate().unwrap(), ScalarValue::Int64(Some(0)));
    assert_eq!(entries(&value), entries(&merged));
}

#[test]
fn live_snapshot_reads_restore_the_exact_scalar_and_counters() {
    let aggregate = WindowAggregate::new(COUNT_DISTINCT_WITH_COUNTS, &DataType::Utf8);
    let mut value = aggregate.create_accumulator();
    value
        .update_batch(&[Arc::new(StringArray::from(vec![
            Some("a"),
            None,
            Some("a"),
            Some("b"),
        ]))])
        .unwrap();
    for _ in 0..3 {
        let state = snapshot_accumulator_state(&mut value, || aggregate.create_accumulator());
        assert_eq!(state[2], ScalarValue::Int64(Some(2)));
        assert_eq!(
            (value.as_ref() as &dyn std::any::Any)
                .downcast_ref::<CountedDistinctAccumulator>()
                .unwrap()
                .entries()
                .iter()
                .map(|(_, count)| *count)
                .sum::<i64>(),
            3
        );
    }
}

#[test]
fn snapshot_reads_preserve_the_live_capacity_charged_to_managed_memory() {
    let aggregate = WindowAggregate::new(COUNT_DISTINCT_WITH_COUNTS, &DataType::Int64);
    let mut value = aggregate.create_accumulator();
    value
        .update_batch(&[Arc::new(Int64Array::from_iter_values(0..4096))])
        .unwrap();
    value
        .retract_batch(&[Arc::new(Int64Array::from_iter_values(0..4095))])
        .unwrap();
    let footprint = value.size();
    snapshot_accumulator_state(&mut value, || aggregate.create_accumulator());
    assert_eq!(
        value.size(),
        footprint,
        "snapshot reads must not replace or compact the live counted set"
    );
    assert_eq!(value.evaluate().unwrap(), ScalarValue::Int64(Some(1)));
}

#[test]
fn shared_equality_primitives_preserve_nan_signs_zero_signs_and_large_decimals() {
    let mut value = CountedDistinctAccumulator::new(&DataType::Float64);
    value
        .update_batch(&[Arc::new(Float64Array::from(vec![
            None,
            Some(0.0),
            Some(-0.0),
            Some(f64::from_bits(0x7ff8000000000001)),
            Some(f64::from_bits(0xfff8000000000002)),
        ]))])
        .unwrap();
    assert_eq!(value.evaluate().unwrap(), ScalarValue::Int64(Some(3)));
    let mut restored = CountedDistinctAccumulator::new(&DataType::Float64);
    restored.restore_exact(&columns(&mut value)).unwrap();
    assert_eq!(entries(&value), entries(&restored));
    let mut decimal = CountedDistinctAccumulator::new(&DataType::Decimal128(38, 2));
    decimal
        .update_batch(&[Arc::new(
            Decimal128Array::from(vec![
                Some(10_i128.pow(30)),
                Some(10_i128.pow(30)),
                Some(-10_i128.pow(30)),
            ])
            .with_precision_and_scale(38, 2)
            .unwrap(),
        )])
        .unwrap();
    assert_eq!(decimal.evaluate().unwrap(), ScalarValue::Int64(Some(2)));
    let mut restored = CountedDistinctAccumulator::new(&DataType::Decimal128(38, 2));
    restored.restore_exact(&columns(&mut decimal)).unwrap();
    assert_eq!(entries(&decimal), entries(&restored));
}

#[test]
fn invalid_snapshots_and_duplicate_imports_leave_live_state_intact() {
    let mut value = CountedDistinctAccumulator::new(&DataType::Int64);
    value
        .update_batch(&[Arc::new(Int64Array::from(vec![10, 10, 20]))])
        .unwrap();
    let before = entries(&value);
    assert!(value.import_entry(ScalarValue::Int64(Some(10)), 2).is_err());
    assert!(value.import_entry(ScalarValue::Int64(Some(30)), 0).is_err());
    assert!(value
        .import_entry(ScalarValue::Utf8(Some("wrong".into())), 1)
        .is_err());
    let mut state = columns(&mut value);
    state.pop();
    assert!(value.restore_exact(&state).is_err());
    assert_eq!(entries(&value), before);
    assert_eq!(value.evaluate().unwrap(), ScalarValue::Int64(Some(2)));
}
