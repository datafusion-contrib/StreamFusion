use super::*;

fn represented_row_batch(count: usize, binary: bool, time: i64) -> RecordBatch {
    use arrow::array::{BinaryArray, Int8Array, ListArray, StructArray};
    use arrow::datatypes::UInt16Type;
    let java = ListArray::from_iter_primitive::<UInt16Type, _, _>(
        (0..count).map(|_| (!binary).then_some(vec![Some(0x03bbu16)])),
    );
    let string_fields = vec![
        Arc::new(Field::new("utf8", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([("streamfusion.nested-string".into(), "1".into())]),
        )),
        Arc::new(Field::new("java", java.data_type().clone(), true)),
        Arc::new(Field::new("array_copy_class", DataType::Int8, true)),
    ];
    let strings = Arc::new(StructArray::new(
        string_fields.into(),
        vec![
            Arc::new(StringArray::from(vec!["λ"; count])),
            Arc::new(java),
            Arc::new(Int8Array::from(vec![(!binary).then_some(0); count])),
        ],
        None,
    ));
    let bytes = (0..count)
        .map(|key| {
            let mut row = vec![0u8; 24];
            row[0] = 2;
            row[8..16].copy_from_slice(&(key as i64).to_ne_bytes());
            if cfg!(target_endian = "little") {
                row[16..18].copy_from_slice("λ".as_bytes());
                row[23] = 0x82;
            } else {
                row[16] = 0x82;
                row[17..19].copy_from_slice("λ".as_bytes());
            }
            binary.then_some(row)
        })
        .collect::<Vec<_>>();
    let fields = vec![
        Arc::new(Field::new("id", DataType::Int64, true)),
        Arc::new(Field::new("s", strings.data_type().clone(), true)),
        Arc::new(Field::new("owned", DataType::Binary, true).with_metadata(
            std::collections::HashMap::from([(
                "streamfusion.nested-row-binary".into(),
                "1".into(),
            )]),
        )),
        Arc::new(Field::new("kind", DataType::Int8, true).with_metadata(
            std::collections::HashMap::from([("streamfusion.nested-row-kind".into(), "1".into())]),
        )),
    ];
    let keys = Arc::new(StructArray::new(
        fields.into(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..count as i64)),
            strings,
            Arc::new(BinaryArray::from_iter(bytes)),
            Arc::new(Int8Array::from(vec![2; count])),
        ],
        None,
    ));
    RecordBatch::try_from_iter(vec![
        ("k", keys as ArrayRef),
        ("rt", Arc::new(Int64Array::from(vec![time; count]))),
        ("v", Arc::new(Int64Array::from_iter_values(0..count as i64))),
    ])
    .unwrap()
}

fn represented_snapshots(operator: &mut KeepFirstDeduplicator, descriptor: &[i32]) -> Vec<Vec<u8>> {
    #[cfg(feature = "rocksdb-state")]
    if operator.store.is_some() {
        return operator
            .canonical_partitions()
            .unwrap()
            .into_values()
            .collect();
    }
    operator
        .snapshot_partitions(128, descriptor)
        .into_values()
        .collect()
}

#[test]
fn represented_row_keys_share_candidates_and_markers_across_raw_recovery() {
    let descriptor = vec![i32::MIN, -1, -1, -1];
    for count in [2, 2048] {
        for persistent in [false, true] {
            #[cfg(not(feature = "rocksdb-state"))]
            if persistent {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let binary = represented_row_batch(count, true, 1000);
            let generic = represented_row_batch(count, false, 900);
            let mut source = KeepFirstDeduplicator::new(vec![0], 1)
                .with_key_timestamp_precisions(descriptor.clone())
                .with_state_ttl(1000);
            #[cfg(feature = "rocksdb-state")]
            if persistent {
                let store = crate::state::RocksKeepFirstDedupStore::create(
                    crate::state::RocksStoreConfig {
                        table_dir: directory.path().join("source").to_str().unwrap().into(),
                        max_parallelism: 128,
                        options_json: include_str!("../../benches/fixtures/rocks-options.json")
                            .into(),
                        ttl_ms: 1000,
                        shared_resources: 0,
                    },
                    binary.schema(),
                    &[0],
                )
                .unwrap();
                source = source.with_store(store);
            }
            source.push(&binary, 100).unwrap();
            source.push(&generic, 100).unwrap();
            if !persistent {
                assert_eq!(source.pending_batch().unwrap().num_rows(), count);
            }
            let snapshots = represented_snapshots(&mut source, &descriptor);
            let mut restored =
                KeepFirstDeduplicator::restore_partitions(vec![0], 1, &snapshots, 100)
                    .with_key_timestamp_precisions(descriptor.clone())
                    .with_state_ttl(1000);
            for operator in [&mut source, &mut restored] {
                let output = operator.flush(1000, 100).unwrap();
                assert_eq!(output.num_rows(), count);
                assert!(column_i64(&output, "rt")
                    .values()
                    .iter()
                    .all(|time| *time == 900));
            }
            let snapshots = represented_snapshots(&mut source, &descriptor);
            let mut marker_restore =
                KeepFirstDeduplicator::restore_partitions(vec![0], 1, &snapshots, 100)
                    .with_key_timestamp_precisions(descriptor.clone())
                    .with_state_ttl(1000);
            for operator in [&mut source, &mut marker_restore] {
                operator
                    .push(&represented_row_batch(count, false, 2000), 500)
                    .unwrap();
                operator
                    .push(&represented_row_batch(count, true, 2000), 500)
                    .unwrap();
                assert_eq!(operator.flush(2000, 500).unwrap().num_rows(), 0);
                operator
                    .push(&represented_row_batch(count, false, 3000), 1200)
                    .unwrap();
                operator
                    .push(&represented_row_batch(count, true, 2999), 1200)
                    .unwrap();
                let output = operator.flush(3000, 1200).unwrap();
                assert_eq!(output.num_rows(), count);
                assert!(column_i64(&output, "rt")
                    .values()
                    .iter()
                    .all(|time| *time == 2999));
            }
        }
    }
}
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

#[test]
fn keep_last_preserves_nested_field_names_after_restore() {
    use arrow::array::{ListArray, StringArray, StructArray, UInt16Array};
    use arrow::buffer::OffsetBuffer;
    let units = Arc::new(Field::new("$data$", DataType::UInt16, false));
    let fields: arrow::datatypes::Fields = vec![
        Field::new("utf8", DataType::Utf8, true)
            .with_metadata([("streamfusion.nested-string".into(), "1".into())].into()),
        Field::new("java", DataType::List(units.clone()), true),
        Field::new("array_copy_class", DataType::Int8, true),
    ]
    .into();
    let payload: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(StringArray::from(vec![Some("?"), None, Some("next")])),
            Arc::new(ListArray::new(
                units,
                OffsetBuffer::new(vec![0, 1, 1, 1].into()),
                Arc::new(UInt16Array::from(vec![0xd800])),
                None,
            )),
            Arc::new(Int8Array::from(vec![None, None, None])),
        ],
        None,
    ));
    let input = RecordBatch::try_from_iter(vec![
        ("k", Arc::new(Int64Array::from(vec![1, 2, 1])) as ArrayRef),
        ("rt", Arc::new(Int64Array::from(vec![0, 0, 0])) as ArrayRef),
        ("v", payload),
    ])
    .unwrap();
    let mut operator = KeepLastDeduplicator::new(vec![0], 1, true, false, false);
    let first = operator.push(&input.slice(0, 2), 0).unwrap();
    assert_eq!(first.column(2).data_type(), input.column(2).data_type());
    assert_eq!(
        first.column(2).as_ref(),
        input.column(2).slice(0, 2).as_ref()
    );
    let snapshot = operator.snapshot();
    let mut restored =
        KeepLastDeduplicator::restore(vec![0], vec![-1], 1, true, false, false, &snapshot, 0);
    let expected = arrow::compute::concat(&[
        input.column(2).slice(0, 1).as_ref(),
        input.column(2).slice(2, 1).as_ref(),
    ])
    .unwrap();
    for state in [&mut operator, &mut restored] {
        let output = state.push(&input.slice(2, 1), 1).unwrap();
        assert_eq!(output.column(2).data_type(), input.column(2).data_type());
        assert_eq!(output.column(2).as_ref(), expected.as_ref());
        assert_eq!(
            output
                .column(3)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values()
                .to_vec(),
            vec![1, 2]
        );
    }
}
