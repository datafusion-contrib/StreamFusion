use super::*;
use arrow::datatypes::TimeUnit;

fn batch(keys: Vec<i64>, times: Vec<i64>, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("k", Arc::new(Int64Array::from(keys)) as ArrayRef),
        ("rt", Arc::new(Int64Array::from(times)) as ArrayRef),
        ("v", Arc::new(Int64Array::from(values)) as ArrayRef),
    ])
    .unwrap()
}
fn values(batch: &RecordBatch, column: usize) -> Vec<i64> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

#[test]
fn replacements_preserve_winning_arrival_order_and_incumbent_ties() {
    let mut operator = KeepFirstDeduplicator::new(vec![0], 1);
    operator
        .push(&batch(vec![1, 2, 3], vec![30, 40, 50], vec![10, 20, 30]), 0)
        .unwrap();
    operator
        .push(
            &batch(
                vec![2, 1, 4, 2, 3],
                vec![20, 30, 60, 20, 40],
                vec![200, 100, 40, 201, 300],
            ),
            0,
        )
        .unwrap();
    let snapshot = operator.snapshot();
    let mut restored = KeepFirstDeduplicator::restore(vec![0], 1, &snapshot, 0);
    for state in [&mut operator, &mut restored] {
        let early = state.flush(35, 0).unwrap();
        assert_eq!(values(&early, 0), vec![1, 2]);
        assert_eq!(values(&early, 2), vec![10, 200]);
        state
            .push(&batch(vec![4, 3], vec![60, 40], vec![400, 301]), 0)
            .unwrap();
        let rest = state.flush(i64::MAX, 0).unwrap();
        assert_eq!(values(&rest, 0), vec![4, 3]);
        assert_eq!(values(&rest, 2), vec![40, 300]);
        assert_eq!(state.flush(i64::MAX, 0).unwrap().num_rows(), 0);
    }
}

#[test]
fn repeated_replacements_bound_retained_chunks_and_budget() {
    let mut operator = KeepFirstDeduplicator::new(vec![0], 1)
        .with_memory_budget(1_000_000)
        .unwrap();
    operator
        .push(
            &batch((0..2048).collect(), vec![1000; 2048], vec![0; 2048]),
            0,
        )
        .unwrap();
    let initial = operator.pending_bytes();
    for turn in 1..100 {
        let start = (turn % 4) * 512;
        operator
            .push(
                &batch(
                    (start..start + 512).collect(),
                    vec![1000 - turn; 512],
                    vec![turn; 512],
                ),
                0,
            )
            .unwrap();
        assert!(
            operator.pending_bytes() < initial * 4,
            "unbounded stale Arrow chunks"
        );
    }
    let output = operator.flush(i64::MAX, 0).unwrap();
    assert_eq!(output.num_rows(), 2048);
    assert_eq!(operator.pending_bytes(), 0);
    let mut tiny = KeepFirstDeduplicator::new(vec![0], 1)
        .with_memory_budget(1)
        .unwrap();
    assert!(tiny.push(&batch(vec![1], vec![100], vec![10]), 0).is_err());
}

#[test]
fn keyed_snapshot_roundtrip_with_fragmented_nullable_timestamp_keys() {
    let make = |keys: Vec<Option<i64>>, times: Vec<i64>, payload: Vec<Option<&str>>| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
                Field::new("rt", DataType::Int64, false),
                Field::new("v", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(TimestampNanosecondArray::from(keys)) as ArrayRef,
                Arc::new(Int64Array::from(times)) as ArrayRef,
                Arc::new(StringArray::from(payload)) as ArrayRef,
            ],
        )
        .unwrap()
    };
    let mut operator =
        KeepFirstDeduplicator::new(vec![0], 1).with_key_timestamp_precisions(vec![9]);
    operator
        .push(
            &make(
                (1..=2048).map(Some).collect(),
                vec![1000; 2048],
                vec![Some("filler"); 2048],
            ),
            0,
        )
        .unwrap();

    operator
        .push(
            &make(
                vec![Some(1_000_123_456), None],
                vec![30, 40],
                vec![Some("old"), None],
            ),
            0,
        )
        .unwrap();
    operator
        .push(
            &make(vec![Some(1_000_123_456)], vec![20], vec![Some("new")]),
            0,
        )
        .unwrap();
    let partitions = operator
        .snapshot_partitions(128, &[9])
        .into_values()
        .collect::<Vec<_>>();
    let mut restored = KeepFirstDeduplicator::restore_partitions(vec![0], 1, &partitions, 0)
        .with_key_timestamp_precisions(vec![9]);
    // Trigger lazy index reconstruction before emission; equal-time arrival must keep "new".
    restored
        .push(
            &make(vec![Some(1_000_123_456)], vec![20], vec![Some("tie")]),
            0,
        )
        .unwrap();
    let expected = operator.flush(i64::MAX, 0).unwrap();
    let actual = restored.flush(i64::MAX, 0).unwrap();
    // Key-group restore may reorder different groups, but each key's payload remains aligned.
    for row in 0..actual.num_rows() {
        let keys = actual
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let values = actual
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        if keys.is_null(row) {
            assert!(values.is_null(row));
        } else if keys.value(row) == 1_000_123_456 {
            assert_eq!(values.value(row), "new");
        } else {
            assert_eq!(values.value(row), "filler");
        }
    }
    assert_eq!(actual.num_rows(), expected.num_rows());
}

#[test]
fn fragmented_candidates_match_an_arrival_order_reference_across_watermarks_and_restore() {
    let mut operator = KeepFirstDeduplicator::new(vec![0], 1);
    let mut reference: HashMap<i64, (i64, i64)> = HashMap::default();
    let mut emitted = HashSet::default();
    let mut frontier = i64::MIN;
    let mut seed = 17u64;
    let mut ordinal = 2048i64;
    operator
        .push(
            &batch((0..2048).collect(), vec![1000; 2048], (0..2048).collect()),
            0,
        )
        .unwrap();
    for key in 0..2048 {
        reference.insert(key, (1000, key));
    }
    for turn in 0..100 {
        let mut keys = Vec::new();
        let mut times = Vec::new();
        let mut payload = Vec::new();
        for _ in 0..8 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let key = (seed >> 32) as i64 % 2048;
            let time = ((seed >> 16) % 500) as i64;
            keys.push(key);
            times.push(time);
            payload.push(ordinal);
            if time >= frontier
                && !emitted.contains(&key)
                && reference.get(&key).is_none_or(|old| time < old.0)
            {
                reference.insert(key, (time, ordinal));
            }
            ordinal += 1;
        }
        operator.push(&batch(keys, times, payload), 0).unwrap();
        if turn % 13 == 0 {
            operator = KeepFirstDeduplicator::restore(vec![0], 1, &operator.snapshot(), 0);
        }
        if turn % 9 == 0 || turn == 99 {
            frontier = if turn == 99 { i64::MAX } else { turn * 4 };
            let mut ready = reference
                .iter()
                .filter(|(_, (time, _))| *time <= frontier)
                .map(|(&key, &(time, value))| (key, time, value))
                .collect::<Vec<_>>();
            ready.sort_unstable_by_key(|row| row.2);
            for &(key, _, _) in &ready {
                reference.remove(&key);
                emitted.insert(key);
            }
            let output = operator.flush(frontier, 0).unwrap();
            assert_eq!(
                values(&output, 0),
                ready.iter().map(|row| row.0).collect::<Vec<_>>()
            );
            assert_eq!(
                values(&output, 1),
                ready.iter().map(|row| row.1).collect::<Vec<_>>()
            );
            assert_eq!(
                values(&output, 2),
                ready.iter().map(|row| row.2).collect::<Vec<_>>()
            );
        }
    }
    assert!(reference.is_empty());
}

#[test]
fn budget_pressure_compacts_skewed_dead_payload_before_rejecting_live_state() {
    let make = |keys: Vec<i64>, times: Vec<i64>, values: Vec<String>| {
        RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(keys)) as ArrayRef),
            ("rt", Arc::new(Int64Array::from(times)) as ArrayRef),
            ("v", Arc::new(StringArray::from(values)) as ArrayRef),
        ])
        .unwrap()
    };
    let initial = make(
        (0..2048).collect(),
        vec![1000; 2048],
        (0..2048)
            .map(|key| {
                if key == 0 {
                    "x".repeat(64 * 1024 * 1024)
                } else {
                    key.to_string()
                }
            })
            .collect(),
    );
    let near = make(vec![2048], vec![1000], vec!["y".repeat(4096)]);
    let mut sizing = KeepFirstDeduplicator::new(vec![0], 1);
    sizing.push(&initial, 0).unwrap();
    sizing.push(&near, 0).unwrap();
    let budget = sizing.pending_bytes() as i64 + 100;
    drop(sizing);
    let mut operator = KeepFirstDeduplicator::new(vec![0], 1)
        .with_memory_budget(budget)
        .unwrap();
    operator.push(&initial, 0).unwrap();
    operator.push(&near, 0).unwrap();
    let replacement = make(vec![0], vec![999], vec!["tiny".to_owned()]);
    operator.push(&replacement, 0).unwrap();
    assert!(
        operator.pending_bytes() < 1_000_000,
        "pressure must release the dead 64MiB value"
    );
    let output = operator.flush(i64::MAX, 0).unwrap();
    assert_eq!(output.num_rows(), 2049);
    assert_eq!(values(&output, 0).last(), Some(&0));
    let payload = output
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(payload.value(output.num_rows() - 1), "tiny");
    assert_eq!(operator.pending_bytes(), 0);
}

#[test]
fn crossing_the_small_state_threshold_promotes_before_the_next_arrival() {
    let mut operator = KeepFirstDeduplicator::new(vec![0], 1);
    operator
        .push(
            &batch((0..1024).collect(), vec![1000; 1024], (0..1024).collect()),
            0,
        )
        .unwrap();
    assert!(operator.pending_rows.is_empty());
    operator
        .push(&batch(vec![1024, 0], vec![1000, 900], vec![1024, 5000]), 0)
        .unwrap();
    assert!(!operator.pending_rows.is_empty());
    operator
        .push(&batch(vec![1, 0], vec![800, 900], vec![5001, 9999]), 0)
        .unwrap();
    let output = operator.flush(i64::MAX, 0).unwrap();
    let mut expected = (2..1024).collect::<Vec<_>>();
    expected.extend([1024, 0, 1]);
    assert_eq!(values(&output, 0), expected);
    assert_eq!(values(&output, 2)[1023..], [5000, 5001]);
}
