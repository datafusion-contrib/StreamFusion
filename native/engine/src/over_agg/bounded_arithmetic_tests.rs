use super::*;
use arrow::array::{Float32Array, Float64Array};

const KINDS: [i64; 8] = [0, 3, 4, 22, 100, 103, 104, 122];
fn input(code: i64, time: i64, value: Option<f64>) -> RecordBatch {
    let value: ArrayRef = if code == 6 {
        Arc::new(Float32Array::from(vec![value.map(|v| v as f32)]))
    } else {
        Arc::new(Float64Array::from(vec![value]))
    };
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![Some(7)])) as ArrayRef),
        ("value", value),
        ("time", Arc::new(Int64Array::from(vec![time])) as ArrayRef),
    ])
    .unwrap()
}
fn memory(code: i64, frame: i64, proctime: bool, keyless: bool) -> OverWindowAggregator {
    OverWindowAggregator::new(
        vec![code; 8],
        KINDS.to_vec(),
        2,
        vec![1; 8],
        if keyless { vec![] } else { vec![0] },
        frame,
        2,
        proctime,
    )
}
#[cfg(feature = "rocksdb-state")]
fn operator(
    code: i64,
    frame: i64,
    proctime: bool,
    keyless: bool,
    persistent: bool,
    path: &std::path::Path,
) -> OverWindowAggregator {
    let mut operator = memory(code, frame, proctime, keyless);
    if persistent {
        let store = crate::state::RocksOverAggStore::create(
            crate::state::RocksStoreConfig {
                table_dir: path.to_str().unwrap().into(),
                max_parallelism: 8,
                options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 0,
                shared_resources: 0,
            },
            &[],
            &rocks_over_frame_value_types(&[code; 8], &KINDS, frame),
            &[],
            input(code, 0, None).schema(),
            0..=7,
        )
        .unwrap();
        operator = operator
            .with_key_timestamp_precisions(if keyless { vec![] } else { vec![-1] })
            .with_store(
                store,
                if keyless {
                    vec![]
                } else {
                    vec![DataType::Int64]
                },
            );
    } else {
        operator = operator.with_memory_budget(64 << 20).unwrap();
    }
    operator
}
fn execute(
    operator: &mut OverWindowAggregator,
    code: i64,
    step: i64,
    value: Option<f64>,
) -> RecordBatch {
    let batch = input(code, 1000 + step, value);
    if operator.proctime {
        operator.push_proctime(batch, 100 + step).unwrap()
    } else {
        operator.push(batch, 100 + step).unwrap();
        operator.flush(1000 + step, 100 + step).unwrap()
    }
}
fn scalar(batch: &RecordBatch, aggregate: usize) -> ScalarValue {
    ScalarValue::try_from_array(batch.column(3 + aggregate), 0).unwrap()
}
fn number(code: i64, value: f64) -> ScalarValue {
    if code == 6 {
        ScalarValue::Float32(Some(value as f32))
    } else {
        ScalarValue::Float64(Some(value))
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn primitive_raw_sums_counts_and_distinct_history_continue_between_both_stores() {
    for code in [1, 6] {
        for (frame, proctime) in [(1, true), (1, false), (2, false)] {
            for keyless in [false, true] {
                for from_store in [false, true] {
                    for to_store in [false, true] {
                        let directory = tempfile::tempdir().unwrap();
                        let mut source = operator(
                            code,
                            frame,
                            proctime,
                            keyless,
                            from_store,
                            &directory.path().join("source"),
                        );
                        source.bind_canonical_frame_inputs().unwrap();
                        let high = if code == 6 {
                            16_777_216.0
                        } else {
                            9_007_199_254_740_992.0
                        };
                        for (step, value) in [high, 1.0, -high].into_iter().enumerate() {
                            execute(&mut source, code, step as i64, Some(value));
                        }
                        let snapshots = if from_store {
                            source
                                .canonical_partitions(8, if keyless { &[] } else { &[-1] })
                                .unwrap()
                        } else {
                            source.snapshot_partitions(8, if keyless { &[] } else { &[-1] })
                        }
                        .into_values()
                        .collect::<Vec<_>>();
                        assert_eq!(snapshots.len(), 1);
                        let len =
                            u32::from_le_bytes(snapshots[0][8..12].try_into().unwrap()) as usize;
                        let states = read_ipc(&snapshots[0][12..12 + len]);
                        let state = &states[0];
                        let last = state.num_rows() - 1;
                        assert_eq!(
                            ScalarValue::try_from_array(
                                state.column_by_name("fold0").unwrap(),
                                last
                            )
                            .unwrap(),
                            number(code, 0.0)
                        );
                        assert_eq!(
                            ScalarValue::try_from_array(
                                state.column_by_name("fold1").unwrap(),
                                last
                            )
                            .unwrap(),
                            ScalarValue::Int64(Some(3))
                        );
                        assert_eq!(
                            ScalarValue::try_from_array(
                                state.column_by_name("fold3").unwrap(),
                                last
                            )
                            .unwrap(),
                            ScalarValue::Float64(Some(if code == 6 { 1.0 } else { 0.0 }))
                        );
                        let mut target = if to_store {
                            let mut op = operator(
                                code,
                                frame,
                                proctime,
                                keyless,
                                true,
                                &directory.path().join("target"),
                            );
                            op.import_partitions(&snapshots).unwrap();
                            op.adopt_store_retention(0).unwrap();
                            op
                        } else {
                            OverWindowAggregator::restore_partitions(
                                vec![code; 8],
                                KINDS.to_vec(),
                                2,
                                vec![1; 8],
                                if keyless { vec![] } else { vec![0] },
                                frame,
                                2,
                                proctime,
                                &snapshots,
                                0,
                                0,
                            )
                            .with_memory_budget(64 << 20)
                            .unwrap()
                        };
                        for step in 3..=5 {
                            let actual = execute(&mut target, code, step, None);
                            assert_eq!(execute(&mut source, code, step, None), actual);
                            if step == 3 {
                                assert_eq!(scalar(&actual, 0), number(code, -high));
                                assert_eq!(scalar(&actual, 1), ScalarValue::Int64(Some(2)));
                            }
                            if step == 5 {
                                for aggregate in [0, 2, 4, 6] {
                                    assert!(scalar(&actual, aggregate).is_null());
                                }
                                for aggregate in [1, 5] {
                                    assert_eq!(
                                        scalar(&actual, aggregate),
                                        ScalarValue::Int64(Some(0))
                                    );
                                }
                            }
                        }
                        let actual = execute(&mut target, code, 6, Some(1.0));
                        assert_eq!(execute(&mut source, code, 6, Some(1.0)), actual);
                        assert_eq!(scalar(&actual, 0), number(code, 1.0));
                    }
                }
            }
        }
    }
}

#[test]
fn zero_live_count_keeps_raw_nan_history_without_exposing_a_nonnull_sum() {
    let mut source = memory(1, 1, true, true);
    for (step, value) in [
        Some(f64::INFINITY),
        Some(1.0),
        Some(f64::NEG_INFINITY),
        None,
        None,
        None,
    ]
    .into_iter()
    .enumerate()
    {
        let out = execute(&mut source, 1, step as i64, value);
        if step == 5 {
            assert!(scalar(&out, 0).is_null());
            assert!(scalar(&out, 2).is_null());
            assert_eq!(scalar(&out, 1), ScalarValue::Int64(Some(0)));
        }
    }
    let snapshots = source
        .snapshot_partitions(8, &[])
        .into_values()
        .collect::<Vec<_>>();
    let mut restored = OverWindowAggregator::restore_partitions(
        vec![1; 8],
        KINDS.to_vec(),
        2,
        vec![1; 8],
        vec![],
        1,
        2,
        true,
        &snapshots,
        0,
        0,
    );
    let out = execute(&mut restored, 1, 6, Some(1.0));
    for aggregate in [0, 2, 3, 4, 6, 7] {
        let ScalarValue::Float64(Some(value)) = scalar(&out, aggregate) else {
            panic!("missing raw NaN history")
        };
        assert!(value.is_nan());
    }
    assert_eq!(scalar(&out, 1), ScalarValue::Int64(Some(1)));
}

fn mixed_input(time: i64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![7])) as ArrayRef),
        ("time", Arc::new(Int64Array::from(vec![time])) as ArrayRef),
        (
            "value",
            Arc::new(Float64Array::from(vec![None])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn frozen_v1_memory_layout_keeps_existing_raw_fields_while_seeding_only_missing_sums() {
    let mut target = OverWindowAggregator::restore_partitions(
        vec![1; 4],
        vec![0, 22, 3, 4],
        1,
        vec![2; 4],
        vec![0],
        1,
        2,
        false,
        &[legacy_frame_fixture("memory-v1-mixed-float")],
        0,
        0,
    );
    let next = RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("time", Arc::new(Int64Array::from(vec![1003])) as ArrayRef),
        (
            "value",
            Arc::new(Float64Array::from(vec![None])) as ArrayRef,
        ),
    ])
    .unwrap();
    target.push(next, 0).unwrap();
    let out = target.flush(1003, 0).unwrap();
    assert_eq!(scalar(&out, 0), number(1, -9_007_199_254_740_992.0));
    assert_eq!(scalar(&out, 1), number(1, -9_007_199_254_740_992.0));
    assert_eq!(scalar(&out, 2), ScalarValue::Int64(Some(2)));
    assert_eq!(scalar(&out, 3), number(1, -4_503_599_627_370_496.0));
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn frozen_v1_rocks_rows_coexist_with_new_v2_rows_and_survive_memory_restore() {
    let directory = tempfile::tempdir().unwrap();
    let input = mixed_input(1003);
    let store = crate::state::RocksOverAggStore::create(
        crate::state::RocksStoreConfig {
            table_dir: directory.path().to_str().unwrap().into(),
            max_parallelism: 8,
            options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        },
        &[],
        &rocks_over_frame_value_types(&[1; 4], &[0, 22, 3, 4], 1),
        &[],
        input.schema(),
        0..=7,
    )
    .unwrap();
    let mut target = OverWindowAggregator::new(
        vec![1; 4],
        vec![0, 22, 3, 4],
        1,
        vec![2; 4],
        vec![0],
        1,
        2,
        false,
    )
    .with_store(store, vec![DataType::Int64]);
    let mut converter = None;
    let keys = encode_keys(&mut converter, &[input.column(0)], 1);
    let store = target.store.as_mut().unwrap();
    let group = store.key_group(binary_row_hash(&input, &[0], 0, &[-1]));
    let prefix = store.frame_prefix(group, keys.row(0).data());
    for row in 0..3 {
        store.insert_frame_fixture(
            &prefix,
            1000 + row,
            row as u64,
            &legacy_frame_fixture(&format!("rocks-v1-mixed-float-{row}")),
        );
    }
    store.adopt_next_seq(3);
    let frames = store.scan_frames().unwrap();
    let raw = &frames[0].1[2].values;
    assert_eq!(raw.len(), 10);
    assert!(raw[4].is_null());
    assert!(raw[5].is_null());
    assert_eq!(raw[6], number(1, 0.0));
    assert_eq!(raw[7], ScalarValue::Int64(Some(3)));
    assert_eq!(raw[8], number(1, 0.0));
    assert_eq!(raw[9], ScalarValue::Int64(Some(3)));
    target.push(input, 0).unwrap();
    let out = target.flush(1003, 0).unwrap();
    assert_eq!(scalar(&out, 0), number(1, -9_007_199_254_740_992.0));
    assert_eq!(scalar(&out, 1), number(1, -9_007_199_254_740_992.0));
    assert_eq!(scalar(&out, 2), ScalarValue::Int64(Some(2)));
    let snapshots = target
        .canonical_partitions(8, &[-1])
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
    let mut memory = OverWindowAggregator::restore_partitions(
        vec![1; 4],
        vec![0, 22, 3, 4],
        1,
        vec![2; 4],
        vec![0],
        1,
        2,
        false,
        &snapshots,
        0,
        0,
    );
    for time in 1004..=1007 {
        let batch = mixed_input(time);
        target.push(batch.clone(), 0).unwrap();
        memory.push(batch, 0).unwrap();
        assert_eq!(
            target.flush(time, 0).unwrap(),
            memory.flush(time, 0).unwrap()
        );
    }
    let store = target.store.as_ref().unwrap();
    let mut invalid = vec![83, 70, 79, 86, 9];
    invalid.extend(legacy_frame_fixture("rocks-unmarked-null-f32"));
    store.insert_frame_fixture(&prefix, 9999, 99, &invalid);
    assert!(store
        .scan_frames()
        .err()
        .unwrap()
        .to_string()
        .contains("unsupported OVER frame codec version 9"));
}
