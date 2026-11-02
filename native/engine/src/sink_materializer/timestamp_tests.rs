use super::tests::{batch, disk_materializer, materializer, rows};
use super::*;
use base64::Engine;

fn strategies() -> [Strategy; 6] {
    [
        Strategy::Legacy,
        Strategy::Value,
        Strategy::Map { serialized: false },
        Strategy::Map { serialized: true },
        Strategy::Adaptive {
            high: 3,
            low: 1,
            serialized: false,
        },
        Strategy::Adaptive {
            high: 3,
            low: 1,
            serialized: true,
        },
    ]
}

fn create(path: Option<&std::path::Path>, upsert: bool, strategy: Strategy) -> SinkMaterializer {
    match path {
        Some(path) => {
            let disk_strategy = match strategy {
                Strategy::Map { .. } => Strategy::Map { serialized: true },
                Strategy::Adaptive { high, low, .. } => Strategy::Adaptive {
                    high,
                    low,
                    serialized: true,
                },
                other => other,
            };
            disk_materializer(path, upsert, disk_strategy, 0, &[], 0..=127, false)
        }
        None => materializer(upsert, 0)
            .with_strategy(strategy, if upsert { vec![-1] } else { vec![-1; 3] }),
    }
}

fn view(m: &SinkMaterializer) -> Vec<(i64, i64, i64, Option<i64>)> {
    let Some(codec) = &m.codec else { return vec![] };
    let mut result = Vec::new();
    let mut visit = |values: &Candidates| {
        let (shape, payloads) = values.snapshot(codec);
        let shape: serde_json::Value = serde_json::from_str(&shape).unwrap();
        let count = shape["count"].as_u64().unwrap() as usize;
        assert_eq!(shape["timestamps"].as_array().unwrap().len(), count);
        let columns = codec.decode(&payloads[..count]);
        let batch = RecordBatch::try_new(codec.schema.clone(), columns).unwrap();
        for i in 0..count {
            result.push((
                column_i64(&batch, "sink_key").value(i),
                column_i64(&batch, "input_key").value(i),
                column_i64(&batch, "value").value(i),
                shape["timestamps"][i].as_i64(),
            ));
        }
    };
    if let Some(store) = &m.rocks {
        for key in store.keys().unwrap() {
            visit(&store.read(&key.0, codec).unwrap());
        }
    } else {
        for (_, values) in m.groups.iter() {
            visit(values);
        }
    }
    result.sort();
    result
}

fn portable(m: &SinkMaterializer) -> Vec<Vec<u8>> {
    let snapshots: Vec<_> = m.snapshot(128).into_values().collect();
    for bytes in &snapshots {
        for batch in read_ipc(bytes) {
            assert_eq!(
                batch.schema_ref().metadata()["streamfusion.sink-materializer.version"],
                "3"
            );
        }
    }
    snapshots
}

#[test]
fn sampled_times_follow_replacement_and_survive_all_native_store_transfers() {
    for strategy in strategies() {
        for upsert in [false, true] {
            for persistent in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let state = directory.path().join("source");
                let mut source = create(persistent.then_some(state.as_path()), upsert, strategy);
                source.push(&batch(&[(1, 10, 100, 0)]), i64::MIN).unwrap();
                source.push(&batch(&[(1, 20, 200, 0)]), -1).unwrap();
                source.push(&batch(&[(1, 30, 300, 0)]), 42).unwrap();
                source.push(&batch(&[(1, 30, 310, 2)]), i64::MAX).unwrap();
                let expected = if upsert {
                    vec![
                        (1, 10, 100, Some(i64::MIN)),
                        (1, 20, 200, Some(-1)),
                        (1, 30, 310, Some(i64::MAX)),
                    ]
                } else {
                    vec![
                        (1, 10, 100, Some(i64::MIN)),
                        (1, 20, 200, Some(-1)),
                        (1, 30, 300, Some(42)),
                        (1, 30, 310, Some(i64::MAX)),
                    ]
                };
                assert_eq!(expected, view(&source));
                let snapshots = portable(&source);
                for restored_store in [false, true] {
                    let target = directory.path().join(format!("target-{restored_store}"));
                    let mut restored =
                        create(restored_store.then_some(target.as_path()), upsert, strategy);
                    restored.restore(&snapshots, 999);
                    assert_eq!(expected, view(&restored));
                    let mut again = create(None, upsert, strategy);
                    again.restore(&portable(&restored), 1001);
                    assert_eq!(
                        expected,
                        view(&again),
                        "no-input re-export refreshed timestamps"
                    );
                    let before = view(&restored);
                    restored.push(&batch(&[(1, 999, 0, 3)]), 2000).unwrap();
                    assert_eq!(
                        before,
                        view(&restored),
                        "unmatched retract refreshed timestamps"
                    );
                    restored.push(&batch(&[(1, 30, 310, 3)]), 3000).unwrap();
                    let mut remaining = expected.clone();
                    if upsert {
                        remaining.retain(|v| v.1 != 30);
                    } else {
                        remaining.retain(|v| v.2 != 310);
                    }
                    assert_eq!(remaining, view(&restored));
                }
            }
        }
    }
}

#[test]
fn adaptive_migrations_keep_each_surviving_candidates_original_sample() {
    for serialized in [false, true] {
        for persistent in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut m = create(
                persistent.then_some(directory.path()),
                false,
                Strategy::Adaptive {
                    high: 3,
                    low: 1,
                    serialized,
                },
            );
            for i in 1..=3 {
                m.push(&batch(&[(1, i, i * 100, 0)]), i * 10).unwrap();
            }
            assert_eq!(
                vec![
                    (1, 1, 100, Some(10)),
                    (1, 2, 200, Some(20)),
                    (1, 3, 300, Some(30))
                ],
                view(&m)
            );
            m.push(&batch(&[(1, 1, 100, 3), (1, 3, 300, 3)]), 99)
                .unwrap();
            assert_eq!(vec![(1, 2, 200, Some(20))], view(&m));
            for i in 4..=5 {
                m.push(&batch(&[(1, i, i * 100, 0)]), i * 10).unwrap();
            }
            assert_eq!(
                vec![
                    (1, 2, 200, Some(20)),
                    (1, 4, 400, Some(40)),
                    (1, 5, 500, Some(50))
                ],
                view(&m)
            );
            let mut restored = create(None, false, Strategy::Value);
            restored.restore(&portable(&m), 1000);
            assert_eq!(
                view(&m),
                view(&restored),
                "strategy change lost sampled times"
            );
        }
    }
}

#[test]
fn batched_single_candidate_fast_path_retains_the_final_replacements_sample() {
    for strategy in strategies() {
        for persistent in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut m = create(persistent.then_some(directory.path()), true, strategy);
            m.upsert_columns = vec![0];
            for now in [-10, i64::MAX] {
                let input: Vec<_> = (0..128).map(|i| (1, i, i, 2)).collect();
                let output = m.push(&batch(&input), now).unwrap();
                assert_eq!(128, output.num_rows());
                assert_eq!(vec![(1, 127, 127, Some(now))], view(&m));
            }
            let mut restored = create(None, true, strategy);
            restored.upsert_columns = vec![0];
            restored.restore(&portable(&m), 0);
            assert_eq!(view(&m), view(&restored));
        }
    }
}

fn fixture_strategy(name: &str) -> Strategy {
    match name {
        "legacy" => Strategy::Legacy,
        "value" => Strategy::Value,
        "map" => Strategy::Map { serialized: true },
        "adaptive" => Strategy::Adaptive {
            high: 3,
            low: 1,
            serialized: true,
        },
        _ => panic!("unknown frozen fixture"),
    }
}

#[test]
fn actual_version_two_snapshots_and_disk_records_keep_missing_history_unknown() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/version2.json")).unwrap();
    assert_eq!(
        fixture["writer_commit"],
        "6b57bfc91a8d213db0ba54c7fc3cd6af00071af7"
    );
    let b64 = base64::engine::general_purpose::STANDARD;
    for case in fixture["cases"].as_array().unwrap() {
        let strategy = fixture_strategy(case["strategy"].as_str().unwrap());
        let upsert = case["upsert"].as_bool().unwrap();
        let snapshots: Vec<_> = case["snapshots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| b64.decode(v.as_str().unwrap()).unwrap())
            .collect();
        let mut reference = create(None, upsert, strategy);
        reference.restore(&snapshots, 9999);
        let expected = view(&reference);
        assert!(!expected.is_empty());
        assert!(expected.iter().all(|v| v.3.is_none()));
        let directory = tempfile::tempdir().unwrap();
        for from_disk in [false, true] {
            let path = directory.path().join(format!("source-{from_disk}"));
            let mut m = create(Some(&path), upsert, strategy);
            if from_disk {
                let records = case["rocks_records"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|pair| {
                        (
                            b64.decode(pair[0].as_str().unwrap()).unwrap(),
                            Some(b64.decode(pair[1].as_str().unwrap()).unwrap()),
                        )
                    });
                m.rocks.as_ref().unwrap().table.apply(records).unwrap();
                let store = m.rocks.take().unwrap();
                m = m.with_rocks(store, 9999).unwrap();
            } else {
                m.restore(&snapshots, 9999);
            }
            assert_eq!(expected, view(&m));
            let mut reexport = create(None, upsert, strategy);
            reexport.restore(&portable(&m), -999);
            assert_eq!(expected, view(&reexport));
            let input = batch(&[(1, 50, 500, 0)]);
            assert_eq!(
                rows(&reexport.push(&input, 123).unwrap()),
                rows(&m.push(&input, 123).unwrap())
            );
            let current = view(&m);
            assert!(current.contains(&(1, 50, 500, Some(123))));
            assert_eq!(expected.len() + 1, current.len());
            assert_eq!(
                expected,
                current
                    .into_iter()
                    .filter(|v| v.3.is_none())
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn native_database_files_clip_timestamp_records_with_their_keys_on_rescale() {
    for strategy in [
        Strategy::Value,
        Strategy::Map { serialized: true },
        Strategy::Adaptive {
            high: 3,
            low: 1,
            serialized: true,
        },
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut m = create(Some(&directory.path().join("source")), false, strategy);
        for key in 0..32 {
            m.push(&batch(&[(key, 10, key * 100, 0)]), key - 100)
                .unwrap();
            m.push(&batch(&[(key, 20, key * 100 + 1, 0)]), key + 1000)
                .unwrap();
        }
        let expected = view(&m);
        let checkpoint = directory.path().join("checkpoint");
        let manifest = m
            .rocks
            .as_mut()
            .unwrap()
            .table
            .checkpoint(checkpoint.to_str().unwrap())
            .unwrap();
        drop(m);
        let sources = [(
            checkpoint.to_string_lossy().into_owned(),
            manifest.snapshot_id,
        )];
        let mut combined = Vec::new();
        for (index, range) in [(0, 0..=63), (1, 64..=127)] {
            let restored = disk_materializer(
                &directory.path().join(format!("restored-{index}")),
                false,
                strategy,
                0,
                &sources,
                range.clone(),
                false,
            );
            let actual = view(&restored);
            for row in &actual {
                let key_batch = batch(&[(row.0, 0, 0, 0)]);
                let key = BinaryRowBatchEncoder::new(&key_batch, &[0], &[-1])
                    .encode(0)
                    .to_vec();
                assert!(range.contains(&(flink_key_group(hash_bytes_by_words(&key), 128) as i32)));
            }
            combined.extend(actual);
            let mut again = create(None, false, strategy);
            again.restore(&portable(&restored), 0);
            assert_eq!(view(&restored), view(&again));
        }
        combined.sort();
        assert_eq!(expected, combined);
    }
}

#[test]
fn malformed_candidate_timestamp_shapes_are_rejected_before_installation() {
    let mut m = create(None, false, Strategy::Value);
    m.push(&batch(&[(1, 10, 100, 0)]), 123).unwrap();
    let snapshots = portable(&m);
    for timestamps in [serde_json::json!([]), serde_json::json!([123, 456])] {
        let old = read_ipc(&snapshots[0]).remove(0);
        let shapes = old
            .column_by_name("__sink_shape__")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut shape: serde_json::Value = serde_json::from_str(shapes.value(0)).unwrap();
        shape["timestamps"] = timestamps;
        let mut changed = StringBuilder::new();
        for row in 0..old.num_rows() {
            if row == 0 {
                changed.append_value(shape.to_string());
            } else {
                changed.append_null();
            }
        }
        let mut columns = old.columns().to_vec();
        let index = old.schema().index_of("__sink_shape__").unwrap();
        columns[index] = Arc::new(changed.finish());
        let bytes = write_ipc(&RecordBatch::try_new(old.schema(), columns).unwrap());
        let mut target = create(None, false, Strategy::Value);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || target.restore(&[bytes], 0)
        ))
        .is_err());
        assert!(target.groups.iter().next().is_none());
    }
}

#[test]
fn malformed_disk_timestamps_fail_reads_and_deletions_remove_their_side_records() {
    for strategy in [Strategy::Value, Strategy::Map { serialized: true }] {
        let directory = tempfile::tempdir().unwrap();
        let mut m = create(Some(directory.path()), false, strategy);
        m.push(&batch(&[(1, 10, 100, 0), (1, 20, 200, 0)]), 123)
            .unwrap();
        m.push(&batch(&[(1, 10, 100, 3)]), 456).unwrap();
        let store = m.rocks.as_ref().unwrap();
        let time_records: Vec<_> = store
            .table
            .db
            .iterator(rocksdb::IteratorMode::Start)
            .map(|entry| entry.unwrap())
            .filter(|(key, _)| key.len() >= 9 && matches!(key[key.len() - 9], 4 | 5))
            .collect();
        assert_eq!(1, time_records.len());
        let group = store.keys().unwrap().remove(0);
        let time_key = time_records[0].0.to_vec();
        store
            .table
            .apply([(time_key.clone(), Some(vec![0; 7]))])
            .unwrap();
        let error = store
            .read(&group.0, m.codec.as_ref().unwrap())
            .err()
            .unwrap();
        assert!(error
            .to_string()
            .contains("invalid materializer candidate timestamp"));
        store
            .table
            .apply([(time_key, Some(123i64.to_le_bytes().to_vec()))])
            .unwrap();
        assert_eq!(vec![(1, 20, 200, Some(123))], view(&m));
        m.push(&batch(&[(1, 20, 200, 3)]), 999).unwrap();
        assert!(view(&m).is_empty());
        assert!(m
            .rocks
            .as_ref()
            .unwrap()
            .table
            .db
            .iterator(rocksdb::IteratorMode::Start)
            .all(|entry| {
                let (key, _) = entry.unwrap();
                key.len() < 9 || !matches!(key[key.len() - 9], 4 | 5)
            }));
    }
}
