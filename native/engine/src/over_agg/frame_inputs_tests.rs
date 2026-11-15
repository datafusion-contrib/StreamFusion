use super::*;
use arrow::array::{Float64Array, Int8Array};

fn input(start: i64, count: usize) -> RecordBatch {
    let ids = (start..start + count as i64).collect::<Vec<_>>();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "partition",
            Arc::new(Int64Array::from(
                ids.iter()
                    .map(|id| (id % 2 == 1).then_some(7))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Float64Array::from(
                ids.iter()
                    .map(|id| (id % 3 != 0).then_some(*id as f64))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "time",
            Arc::new(Int64Array::from(
                ids.iter().map(|id| (id + 1) / 2).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "unused",
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| (id % 4 != 0).then(|| format!("retained-{id}-😀")))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        ("id", Arc::new(Int64Array::from(ids.clone())) as ArrayRef),
        (
            "__over_frame_time",
            Arc::new(Int64Array::from(
                ids.iter().map(|id| 1000 + id).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            ROW_KIND_COLUMN,
            Arc::new(Int8Array::from(vec![0; count])) as ArrayRef,
        ),
    ])
    .unwrap();
    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone().with_nullable(true))
                .collect::<Vec<_>>(),
            [(String::from("original"), String::from("schema"))].into(),
        )),
        batch.columns().to_vec(),
    )
    .unwrap()
}

fn memory(frame: i64, proctime: bool, keyless: bool) -> OverWindowAggregator {
    OverWindowAggregator::new(
        vec![1; 4],
        vec![22, 3, 1, 2],
        2,
        vec![1; 4],
        if keyless { vec![] } else { vec![0] },
        frame,
        2,
        proctime,
    )
    .with_key_timestamp_precisions(if keyless { vec![] } else { vec![-1] })
}

#[cfg(feature = "rocksdb-state")]
fn config(path: &std::path::Path) -> crate::state::RocksStoreConfig {
    crate::state::RocksStoreConfig {
        table_dir: path.to_str().unwrap().into(),
        max_parallelism: 8,
        options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
        ttl_ms: 0,
        shared_resources: 0,
    }
}

#[cfg(feature = "rocksdb-state")]
fn operator(
    frame: i64,
    proctime: bool,
    keyless: bool,
    persistent: bool,
    path: &std::path::Path,
) -> OverWindowAggregator {
    let mut operator = memory(frame, proctime, keyless);
    if persistent {
        let store = crate::state::RocksOverAggStore::create(
            config(path),
            &[],
            &rocks_over_frame_value_types(&[1; 4], &[22, 3, 1, 2], frame),
            &[],
            input(1, 0).schema(),
            0..=7,
        )
        .unwrap();
        operator = operator.with_store(
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

fn execute(operator: &mut OverWindowAggregator, batch: RecordBatch, now: i64) -> RecordBatch {
    if operator.proctime {
        operator.push_proctime(batch, now).unwrap()
    } else {
        let watermark = column_i64(&batch, "time").values().last().copied().unwrap();
        operator.push(batch, now).unwrap();
        operator.flush(watermark, now).unwrap()
    }
}

fn exported(operator: &OverWindowAggregator) -> Vec<RecordBatch> {
    let mut rows = Vec::new();
    operator
        .visit_canonical_frame_inputs(|batch| {
            rows.push(batch);
            Ok(())
        })
        .unwrap();
    rows.sort_by_key(|row| column_i64(row, "id").value(0));
    rows
}

fn check_originals(rows: &[RecordBatch], proctime: bool) {
    for row in rows {
        let id = column_i64(row, "id").value(0);
        let original = input(id, 1);
        let arity = original.num_columns();
        assert_eq!(
            row.project(&(0..arity).collect::<Vec<_>>()).unwrap(),
            original
        );
        assert_eq!(row.num_columns(), arity + 3);
        assert_eq!(row.schema().field(arity).name(), "__over_frame_time_");
        let metadata = |index| {
            row.column(arity + index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(metadata(0), if proctime { id - 1 } else { (id + 1) / 2 });
        assert_eq!(
            metadata(1),
            if proctime {
                if id <= 6 {
                    100
                } else {
                    200
                }
            } else {
                (id + 1) / 2
            }
        );
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn originals_continue_across_memory_and_rocks_snapshots_without_replaying_aggregates() {
    for (frame, proctime) in [(1, true), (1, false), (2, false)] {
        for keyless in [false, true] {
            for from_store in [false, true] {
                for to_store in [false, true] {
                    let directory = tempfile::tempdir().unwrap();
                    let mut source = operator(
                        frame,
                        proctime,
                        keyless,
                        from_store,
                        &directory.path().join("source"),
                    );
                    let mut control = operator(
                        frame,
                        proctime,
                        keyless,
                        false,
                        &directory.path().join("control"),
                    );
                    source.bind_canonical_frame_inputs().unwrap();
                    assert_eq!(
                        execute(&mut source, input(1, 6), 100),
                        execute(&mut control, input(1, 6), 100)
                    );
                    let before = exported(&source);
                    check_originals(&before, proctime);
                    assert_eq!(
                        before.len(),
                        if frame == 1 {
                            if keyless {
                                3
                            } else {
                                6
                            }
                        } else {
                            6
                        }
                    );
                    let snapshots = if from_store {
                        source
                            .canonical_partitions(8, if keyless { &[] } else { &[-1] })
                            .unwrap()
                    } else {
                        source.snapshot_partitions(8, if keyless { &[] } else { &[-1] })
                    }
                    .into_values()
                    .collect::<Vec<_>>();
                    let mut target = if to_store {
                        let mut target = operator(
                            frame,
                            proctime,
                            keyless,
                            true,
                            &directory.path().join("target"),
                        );
                        target.import_partitions(&snapshots).unwrap();
                        target.adopt_store_retention(0).unwrap();
                        target
                    } else {
                        OverWindowAggregator::restore_partitions(
                            vec![1; 4],
                            vec![22, 3, 1, 2],
                            2,
                            vec![1; 4],
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
                    target.bind_canonical_frame_inputs().unwrap();
                    assert_eq!(exported(&target), before);
                    assert_eq!(
                        execute(&mut source, input(7, 4), 200),
                        execute(&mut target, input(7, 4), 200)
                    );
                    assert_eq!(exported(&source), exported(&target));
                    check_originals(&exported(&target), proctime);
                    let mut calls = 0;
                    assert!(target
                        .visit_canonical_frame_inputs(|_| {
                            calls += 1;
                            Err(DataFusionError::Execution("receiver stopped".into()))
                        })
                        .is_err());
                    assert_eq!(calls, 1);
                    assert_eq!(exported(&source), exported(&target));
                    if let OverInner::Bounded(inner) = &mut target.inner {
                        if !to_store {
                            let accounted = inner.bytes;
                            inner.recompute_bytes();
                            assert_eq!(accounted, inner.bytes);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn aligned_and_merged_checkpoints_keep_the_input_binding_and_frame_order() {
    for (frame, proctime) in [(1, true), (1, false), (2, false)] {
        for keyless in [false, true] {
            for aligned in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let mut source = operator(
                    frame,
                    proctime,
                    keyless,
                    true,
                    &directory.path().join("source"),
                );
                source.bind_canonical_frame_inputs().unwrap();
                execute(&mut source, input(1, 6), 100);
                let before = exported(&source);
                let path = directory.path().join("checkpoint");
                let manifest = source.checkpoint_store(path.to_str().unwrap()).unwrap();
                let store = crate::state::RocksOverAggStore::open_merged(
                    config(&directory.path().join("target")),
                    &[],
                    &rocks_over_frame_value_types(&[1; 4], &[22, 3, 1, 2], frame),
                    &[],
                    input(1, 0).schema(),
                    0..=7,
                    &[(path.to_str().unwrap().into(), manifest.snapshot_id)],
                    aligned,
                )
                .unwrap();
                let mut target = memory(frame, proctime, keyless).with_store(
                    store,
                    if keyless {
                        vec![]
                    } else {
                        vec![DataType::Int64]
                    },
                );
                target.adopt_store_retention(0).unwrap();
                assert_eq!(exported(&target), before);
                assert_eq!(
                    execute(&mut source, input(7, 4), 200),
                    execute(&mut target, input(7, 4), 200)
                );
                assert_eq!(exported(&source), exported(&target));
                check_originals(&exported(&target), proctime);
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn binding_rejects_legacy_frames_and_does_not_change_default_output() {
    for persistent in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut source = operator(1, true, false, persistent, directory.path());
        assert!(source.visit_canonical_frame_inputs(|_| Ok(())).is_err());
        execute(&mut source, input(1, 6), 100);
        let error = source.bind_canonical_frame_inputs().unwrap_err();
        assert!(error.to_string().contains("legacy OVER frames"));
        let mut control = memory(1, true, false);
        execute(&mut control, input(1, 6), 100);
        assert_eq!(
            execute(&mut source, input(7, 4), 200),
            execute(&mut control, input(7, 4), 200)
        );
    }
    for (frame, proctime) in [(0, false), (3, true), (2, true)] {
        let mut source = memory(frame, proctime, false);
        assert!(source.bind_canonical_frame_inputs().is_err());
    }
}

#[test]
fn original_payload_owns_nested_and_dictionary_buffers_and_survives_ipc_roundtrip() {
    use arrow::array::{DictionaryArray, Int32Array};
    let dictionary = Arc::new(
        DictionaryArray::<arrow::datatypes::Int8Type>::try_new(
            Int8Array::from(vec![Some(1), None, Some(0)]),
            Arc::new(StringArray::from(vec![Some("left"), None])),
        )
        .unwrap(),
    ) as ArrayRef;
    let nested = Arc::new(arrow::array::ListArray::from_iter_primitive::<
        arrow::datatypes::Int32Type,
        _,
        _,
    >(vec![
        Some(vec![Some(1), None]),
        None,
        Some(vec![Some(3)]),
    ])) as ArrayRef;
    let source = RecordBatch::try_from_iter(vec![
        ("dictionary", dictionary.clone()),
        ("nested", nested),
        (
            "unused",
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    let mut operator = memory(1, true, true);
    operator.bind_canonical_frame_inputs().unwrap();
    operator.frame_clock = 123;
    operator.project_frame_input(
        &source,
        &mut fields,
        &mut columns,
        &(Arc::new(Int64Array::from(vec![0, 1, 2])) as ArrayRef),
    );
    let projected = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let saved = FrameInput::from_projected(&projected, 2).unwrap();
    assert_eq!(saved.clock, 123);
    assert_eq!(saved.row, source.slice(2, 1));
    let before = dictionary.to_data();
    let after = saved.row.column(0).to_data();
    assert_ne!(
        before.child_data()[0].buffers()[1].as_ptr(),
        after.child_data()[0].buffers()[1].as_ptr()
    );
    assert_ne!(
        before.child_data()[0].nulls().unwrap().buffer().as_ptr(),
        after.child_data()[0].nulls().unwrap().buffer().as_ptr()
    );
    assert_eq!(FrameInput::decode(&saved.encode()).unwrap().row, saved.row);
    assert!(FrameInput::decode(&[0; 8]).is_err());
    let invalid = FrameInput {
        row: source,
        clock: 123,
    };
    assert!(FrameInput::decode(&invalid.encode()).is_err());
}

#[test]
fn retained_rows_are_charged_to_the_resident_budget() {
    let mut source = memory(1, true, false).with_memory_budget(1000).unwrap();
    source.bind_canonical_frame_inputs().unwrap();
    let error = source.push_proctime(input(1, 6), 100).unwrap_err();
    assert!(error.to_string().contains("off-heap memory"));
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn count_only_rows_keep_the_complete_current_frame_when_original_inputs_are_bound() {
    for persistent in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut source =
            OverWindowAggregator::new(vec![1], vec![3], 2, vec![1], vec![], 1, 2, true);
        if persistent {
            let store = crate::state::RocksOverAggStore::create(
                config(directory.path()),
                &[],
                &rocks_over_frame_value_types(&[1], &[3], 1),
                &[],
                input(1, 0).schema(),
                0..=7,
            )
            .unwrap();
            source = source.with_store(store, vec![]);
        }
        source.bind_canonical_frame_inputs().unwrap();
        execute(&mut source, input(1, 6), 100);
        let rows = exported(&source);
        assert_eq!(rows.len(), 3);
        check_originals(&rows, true);
        assert_eq!(
            rows.iter()
                .map(|row| column_i64(row, "id").value(0))
                .collect::<Vec<_>>(),
            [4, 5, 6]
        );
        let output = execute(&mut source, input(7, 4), 200);
        let count = output
            .column(output.num_columns() - 2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count.values().as_ref(), &[2, 2, 2, 2]);
        assert_eq!(exported(&source).len(), 3);
    }
}
