use super::*;
#[cfg(feature = "rocksdb-state")]
use crate::sink_materializer::tests::disk_materializer;
use crate::sink_materializer::tests::{batch, materializer, rows};

fn operator(path: Option<&std::path::Path>, upsert: bool, strategy: Strategy) -> SinkMaterializer {
    #[cfg(feature = "rocksdb-state")]
    if let Some(path) = path {
        let strategy = match strategy {
            Strategy::Map { .. } => Strategy::Map { serialized: true },
            Strategy::Adaptive { high, low, .. } => Strategy::Adaptive {
                high,
                low,
                serialized: true,
            },
            other => other,
        };
        return disk_materializer(path, upsert, strategy, 0, &[], 0..=127, false)
            .with_budget(1 << 26)
            .unwrap();
    }
    #[cfg(not(feature = "rocksdb-state"))]
    assert!(path.is_none());
    materializer(upsert, 0)
        .with_strategy(strategy, if upsert { vec![-1] } else { vec![-1; 3] })
        .with_budget(1 << 26)
        .unwrap()
}
fn capture(m: &SinkMaterializer, section: i32) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    m.visit_canonical(section, |batch| {
        assert!(batch.num_rows() <= CHUNK_ROWS);
        batches.push(batch);
        Ok(())
    })
    .unwrap();
    batches
}
fn digest(m: &SinkMaterializer) -> Vec<String> {
    let mut result = Vec::new();
    for section in 0..=2 {
        for batch in capture(m, section) {
            for row in 0..batch.num_rows() {
                let mut fields = Vec::new();
                for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                    let value = if !column.is_null(row) && column.data_type() == &DataType::Float64
                    {
                        format!(
                            "bits:{:016x}",
                            column
                                .as_any()
                                .downcast_ref::<arrow::array::Float64Array>()
                                .unwrap()
                                .value(row)
                                .to_bits()
                        )
                    } else if !column.is_null(row) && column.data_type() == &DataType::Float32 {
                        format!(
                            "bits:{:08x}",
                            column
                                .as_any()
                                .downcast_ref::<arrow::array::Float32Array>()
                                .unwrap()
                                .value(row)
                                .to_bits()
                        )
                    } else {
                        format!("{:?}", ScalarValue::try_from_array(column, row).unwrap())
                    };
                    fields.push(format!("{}:{:?}={value}", field.name(), field.data_type()));
                }
                result.push(format!("{section}|{}", fields.join("|")));
            }
        }
    }
    result.sort();
    result
}
fn transfer(source: &SinkMaterializer, target: &mut SinkMaterializer) {
    let codec = source.codec.as_ref().unwrap();
    target
        .bind_canonical(&RecordBatch::new_empty(codec.schema.clone()))
        .unwrap();
    let candidates = capture(source, 1);
    let indexes = capture(source, 2);
    for header in capture(source, 0) {
        let key = binary(&header, 0, 0);
        target.import_canonical_section(0, &header).unwrap();
        for batch in &candidates {
            if binary(batch, codec.schema.fields().len(), 0) == key {
                target.import_canonical_section(1, batch).unwrap();
            }
        }
        for batch in &indexes {
            if binary(batch, codec.match_fields.len(), 0) == key {
                target.import_canonical_section(2, batch).unwrap();
            }
        }
        target.finish_canonical_group().unwrap();
    }
}
fn strategies() -> [Strategy; 5] {
    [
        Strategy::Legacy,
        Strategy::Value,
        Strategy::Map { serialized: false },
        Strategy::Adaptive {
            high: 3,
            low: 1,
            serialized: false,
        },
        Strategy::Adaptive {
            high: 100,
            low: 1,
            serialized: false,
        },
    ]
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn direct_live_sections_preserve_rows_indexes_cursors_and_continuation_between_both_stores() {
    for strategy in strategies() {
        for upsert in [false, true] {
            for persistent in [false, true] {
                for target_store in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let mut source = operator(
                        persistent.then_some(dir.path().join("source").as_path()),
                        upsert,
                        strategy,
                    );
                    for (clock, input) in [
                        (i64::MIN, batch(&[(1, 10, 100, 0), (2, 10, 900, 0)])),
                        (-1, batch(&[(1, 20, 200, 0)])),
                        (42, batch(&[(1, 30, 300, 0)])),
                        (
                            i64::MAX,
                            batch(&[(1, 30, 310, 2), (1, 40, 400, 0), (1, 40, 400, 3)]),
                        ),
                    ] {
                        source.push(&input, clock).unwrap();
                    }
                    let before = digest(&source);
                    let mut target = operator(
                        target_store.then_some(dir.path().join("target").as_path()),
                        upsert,
                        strategy,
                    );
                    transfer(&source, &mut target);
                    assert_eq!(before, digest(&target));
                    assert_eq!(before, digest(&source), "export mutated source");
                    let snapshots: Vec<_> = target.snapshot(128).into_values().collect();
                    let mut checkpoint = operator(None, upsert, strategy);
                    checkpoint.restore(&snapshots, 12345);
                    assert_eq!(before, digest(&checkpoint));
                    let mut again = operator(None, upsert, strategy);
                    transfer(&target, &mut again);
                    assert_eq!(before, digest(&again));
                    for changes in [
                        batch(&[(1, 999, 0, 3)]),
                        batch(&[(1, 30, 310, 3)]),
                        batch(&[(1, 20, 200, 3)]),
                        batch(&[(2, 10, 900, 3)]),
                    ] {
                        let expected = rows(&source.push(&changes, 1000).unwrap());
                        assert_eq!(expected, rows(&target.push(&changes, 1000).unwrap()));
                        assert_eq!(expected, rows(&again.push(&changes, 1000).unwrap()));
                        assert_eq!(expected, rows(&checkpoint.push(&changes, 1000).unwrap()));
                    }
                    assert_eq!(digest(&source), digest(&target));
                    assert_eq!(digest(&source), digest(&again));
                }
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn large_persistent_groups_export_in_chunks_without_hydrating_native_memory_state() {
    for strategy in [Strategy::Value, Strategy::Map { serialized: true }] {
        let dir = tempfile::tempdir().unwrap();
        let mut source = operator(Some(&dir.path().join("source")), false, strategy);
        let input: Vec<_> = (0..2507).map(|i| (1, i, i * 100, 0)).collect();
        source.push(&batch(&input), -19).unwrap();
        assert_eq!(0, source.groups.iter().count());
        let exported = capture(&source, 1);
        assert_eq!(
            vec![1024, 1024, 459],
            exported
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>()
        );
        assert_eq!(0, source.groups.iter().count());
        let mut target = operator(Some(&dir.path().join("target")), false, strategy);
        transfer(&source, &mut target);
        assert_eq!(0, target.groups.iter().count());
        assert_eq!(digest(&source), digest(&target));
        assert_eq!(
            rows(&source.push(&batch(&[(1, 2506, 250600, 3)]), 999).unwrap()),
            rows(&target.push(&batch(&[(1, 2506, 250600, 3)]), 999).unwrap())
        );
    }
}

#[test]
fn failed_receivers_leave_live_state_and_ordinary_checkpoint_recovery_intact() {
    let mut source = operator(None, true, Strategy::Map { serialized: false });
    source
        .push(
            &batch(&[(1, 10, 100, 0), (1, 20, 200, 0), (1, 30, 300, 0)]),
            123,
        )
        .unwrap();
    let before = digest(&source);
    let allocated = source.memory.state_bytes;
    for section in 0..=2 {
        assert!(source
            .visit_canonical(section, |_| Err(invalid("receiver failed")))
            .is_err());
        assert_eq!(before, digest(&source));
        assert_eq!(allocated, source.memory.state_bytes);
    }
    assert_eq!(
        vec![(1, 20, 200, 2)],
        rows(&source.push(&batch(&[(1, 30, 300, 3)]), 500).unwrap())
    );
}

#[test]
fn imports_require_bound_schema_complete_groups_and_actual_matching_keys() {
    let mut source = operator(None, true, Strategy::Value);
    source
        .push(&batch(&[(1, 10, 100, 0), (1, 20, 200, 0)]), 123)
        .unwrap();
    let header = capture(&source, 0).remove(0);
    let rows = capture(&source, 1).remove(0);
    let mut target = operator(None, true, Strategy::Value);
    assert!(target.import_canonical_section(0, &header).is_err());
    target
        .bind_canonical(&RecordBatch::new_empty(
            source.codec.as_ref().unwrap().schema.clone(),
        ))
        .unwrap();
    assert!(target.import_canonical_section(1, &rows).is_err());
    target.import_canonical_section(0, &header).unwrap();
    assert!(target.finish_canonical_group().is_err());
    assert!(target.import_canonical_section(0, &header).is_err());
    assert!(target.push(&batch(&[(1, 30, 300, 0)]), 0).is_err());
    assert!(target.visit_canonical(0, |_| Ok(())).is_err());
    let mut columns = rows.columns().to_vec();
    columns[0] = Arc::new(Int64Array::from(vec![2, 2]));
    assert!(target
        .import_canonical_section(1, &RecordBatch::try_new(rows.schema(), columns).unwrap())
        .is_err());
    target.import_canonical_section(1, &rows).unwrap();
    assert!(target.import_canonical_section(1, &rows).is_err());
    target.finish_canonical_group().unwrap();
    assert_eq!(digest(&source), digest(&target));
    assert!(target
        .bind_canonical(&RecordBatch::new_empty(
            source.codec.as_ref().unwrap().schema.clone()
        ))
        .is_err());
    assert!(target.import_canonical_section(0, &header).is_err());
}

#[test]
fn row_and_index_payload_names_cannot_collide_with_user_columns() {
    let mut schema = batch(&[]).schema().fields().to_vec();
    schema[1] = Arc::new(schema[1].as_ref().clone().with_name("binary_key"));
    schema[2] = Arc::new(schema[2].as_ref().clone().with_name("sampled_at"));
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(schema)),
        batch(&[(1, 10, 100, 0), (1, 20, 200, 0)])
            .columns()
            .to_vec(),
    )
    .unwrap();
    let mut source = operator(None, true, Strategy::Map { serialized: false });
    source.push(&input, 123).unwrap();
    let mut target = operator(None, true, Strategy::Map { serialized: false });
    transfer(&source, &mut target);
    assert_eq!(digest(&source), digest(&target));
    assert_eq!(
        source.push(&input, 456).unwrap(),
        target.push(&input, 456).unwrap()
    );
}

#[test]
fn unknown_historical_samples_are_rejected_only_when_stock_requires_them() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/version2.json")).unwrap();
    use base64::Engine;
    for case in fixture["cases"].as_array().unwrap() {
        let legacy = case["strategy"] == "legacy";
        if !legacy && case["strategy"] != "value" {
            continue;
        }
        let mut source = operator(
            None,
            case["upsert"].as_bool().unwrap(),
            if legacy {
                Strategy::Legacy
            } else {
                Strategy::Value
            },
        );
        let snapshots: Vec<_> = case["snapshots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| {
                base64::engine::general_purpose::STANDARD
                    .decode(v.as_str().unwrap())
                    .unwrap()
            })
            .collect();
        source.restore(&snapshots, 12345);
        let before: Vec<_> = source.snapshot(128).into_values().collect();
        let result = source.visit_canonical(1, |_| Ok(()));
        assert_eq!(legacy, result.is_ok());
        if !legacy {
            assert!(result
                .err()
                .unwrap()
                .to_string()
                .contains("historical candidate timestamp"));
        }
        let after: Vec<_> = source.snapshot(128).into_values().collect();
        assert_eq!(before, after);
    }
}
