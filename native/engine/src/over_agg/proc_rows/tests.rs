use super::*;

fn input(key: Option<i64>, value: Option<i64>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![key])) as ArrayRef),
        ("value", Arc::new(Int64Array::from(vec![value])) as ArrayRef),
    ])
    .unwrap()
}

fn memory(keyless: bool, offset: i64) -> OverWindowAggregator {
    OverWindowAggregator::new(
        vec![0; 4],
        vec![0, 3, 4, 22],
        0,
        vec![1; 4],
        if keyless { vec![] } else { vec![0] },
        1,
        offset,
        true,
    )
    .with_key_timestamp_precisions(if keyless { vec![] } else { vec![-1] })
}

#[cfg(feature = "rocksdb-state")]
fn config(path: &std::path::Path) -> crate::state::RocksStoreConfig {
    crate::state::RocksStoreConfig {
        table_dir: path.to_str().unwrap().into(),
        max_parallelism: 8,
        options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
        ttl_ms: 0,
        shared_resources: 0,
    }
}

#[cfg(feature = "rocksdb-state")]
fn operator(
    keyless: bool,
    offset: i64,
    persistent: bool,
    path: &std::path::Path,
) -> OverWindowAggregator {
    let op = memory(keyless, offset);
    if persistent {
        let store = crate::state::RocksOverAggStore::create(
            config(path),
            &[],
            &rocks_over_frame_value_types(&[0; 4], &[0, 3, 4, 22], 1),
            &[],
            input(None, None).schema(),
            0..=7,
        )
        .unwrap();
        op.with_store(
            store,
            if keyless {
                vec![]
            } else {
                vec![DataType::Int64]
            },
        )
    } else {
        op.with_memory_budget(64 << 20).unwrap()
    }
}

fn metadata(op: &OverWindowAggregator) -> Vec<(Vec<u8>, i64, i64)> {
    let mut rows = op
        .proc_rows
        .iter()
        .map(|(key, state)| (key.0.to_vec(), state.count, state.smallest))
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn bucket_minimum_and_counts_continue_across_memory_and_rocks_native_snapshots() {
    for keyless in [false, true] {
        for offset in [0, 2] {
            for clocks in [
                [100; 6],
                [100, 200, 300, 400, 500, 600],
                [300, 200, 100, 400, 200, 500],
                [i64::MAX; 6],
            ] {
                for from_store in [false, true] {
                    for to_store in [false, true] {
                        let dir = tempfile::tempdir().unwrap();
                        let mut source =
                            operator(keyless, offset, from_store, &dir.path().join("source"));
                        for (index, clock) in clocks[..4].iter().enumerate() {
                            source
                                .push_proctime(input(None, Some(index as i64 + 1)), *clock)
                                .unwrap();
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
                        let mut target = if to_store {
                            let mut op =
                                operator(keyless, offset, true, &dir.path().join("target"));
                            op.import_partitions(&snapshots).unwrap();
                            op.adopt_store_retention(0).unwrap();
                            op
                        } else {
                            OverWindowAggregator::restore_partitions(
                                vec![0; 4],
                                vec![0, 3, 4, 22],
                                0,
                                vec![1; 4],
                                if keyless { vec![] } else { vec![0] },
                                1,
                                offset,
                                true,
                                &snapshots,
                                0,
                                0,
                            )
                            .with_memory_budget(64 << 20)
                            .unwrap()
                        };
                        assert_eq!(metadata(&source), metadata(&target));
                        for (value, clock) in [None, Some(2)].into_iter().zip(&clocks[4..]) {
                            assert_eq!(
                                source.push_proctime(input(None, value), *clock).unwrap(),
                                target.push_proctime(input(None, value), *clock).unwrap()
                            );
                            assert_eq!(metadata(&source), metadata(&target));
                        }
                        let key = target.proc_rows.keys().next().unwrap().0.to_vec();
                        target.clear_key(&key);
                        assert_eq!(target.retention_bytes, 0);
                    }
                }
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn file_checkpoints_clip_processing_clocks_and_counters_with_their_frames() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = operator(false, 2, true, &dir.path().join("source"));
    for clock in [300, 200, 100, 400] {
        for key in 0..32 {
            source
                .push_proctime(input(Some(key), Some(clock)), clock)
                .unwrap();
        }
    }
    let path = dir.path().join("checkpoint");
    let manifest = source.checkpoint_store(path.to_str().unwrap()).unwrap();
    for (index, (aligned, groups)) in [(true, 0..=7), (false, 0..=3), (false, 4..=7)]
        .into_iter()
        .enumerate()
    {
        let store = crate::state::RocksOverAggStore::open_merged(
            config(&dir.path().join(format!("target{index}"))),
            &[],
            &rocks_over_frame_value_types(&[0; 4], &[0, 3, 4, 22], 1),
            &[],
            input(None, None).schema(),
            groups.clone(),
            &[(path.to_str().unwrap().into(), manifest.snapshot_id)],
            aligned,
        )
        .unwrap();
        let mut target = memory(false, 2).with_store(store, vec![DataType::Int64]);
        target.adopt_store_retention(0).unwrap();
        let expected = metadata(&source)
            .into_iter()
            .filter(|(key, _, _)| groups.contains(&source.store_fold_key_group(key)))
            .collect::<Vec<_>>();
        assert!(!expected.is_empty());
        assert_eq!(expected, metadata(&target));
        for key in 0..32 {
            let batch = input(Some(key), Some(500));
            let group = flink_key_group(binary_row_hash(&batch, &[0], 0, &[-1]), 8) as i32;
            if groups.contains(&group) {
                // The fourth row selected bucket 100 before appending bucket 400. The next row
                // retracts that middle frame row, rather than the first arrival (bucket 200).
                let result = target.push_proctime(batch, 500).unwrap();
                assert_eq!(
                    ScalarValue::try_from_array(result.column(2), 0).unwrap(),
                    ScalarValue::Int64(Some(1100))
                );
            }
        }
        let keys = target
            .proc_rows
            .keys()
            .map(|key| key.0.to_vec())
            .collect::<Vec<_>>();
        for key in keys {
            target.clear_key(&key);
        }
        let path = dir.path().join(format!("empty{index}"));
        target.checkpoint_store(path.to_str().unwrap()).unwrap();
        assert!(target
            .store
            .as_ref()
            .unwrap()
            .scan_proc_rows()
            .unwrap()
            .is_empty());
        assert!(target
            .store
            .as_ref()
            .unwrap()
            .scan_frames()
            .unwrap()
            .is_empty());
        assert_eq!(target.retention_bytes, 0);
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn direct_frame_and_processing_protocol_transfer_preserves_sentinel_and_middle_evictions() {
    for keyless in [false, true] {
        for offset in [0, 2] {
            for clocks in [[100; 6], [300, 200, 100, 400, 200, 500], [i64::MAX; 6]] {
                for from_store in [false, true] {
                    for to_store in [false, true] {
                        let dir = tempfile::tempdir().unwrap();
                        let mut source =
                            operator(keyless, offset, from_store, &dir.path().join("source"));
                        source.bind_canonical_frame_inputs().unwrap();
                        for (index, clock) in clocks[..4].iter().enumerate() {
                            source
                                .push_proctime(input(None, Some(index as i64 + 1)), *clock)
                                .unwrap();
                        }
                        let mut frames = Vec::new();
                        source
                            .visit_canonical_frames(|batch| {
                                frames.push(batch);
                                Ok(())
                            })
                            .unwrap();
                        let mut protocol = Vec::new();
                        source
                            .visit_canonical_processing_rows(|batch| {
                                protocol.push(batch);
                                Ok(())
                            })
                            .unwrap();
                        let mut target =
                            operator(keyless, offset, to_store, &dir.path().join("target"));
                        target.bind_canonical_frame_inputs().unwrap();
                        for batch in &frames {
                            target.import_canonical_frame(batch).unwrap();
                        }
                        for batch in &protocol {
                            target.import_canonical_processing_rows(batch).unwrap();
                        }
                        assert_eq!(metadata(&source), metadata(&target));
                        let mut exported = Vec::new();
                        target
                            .visit_canonical_processing_rows(|batch| {
                                exported.push(batch);
                                Ok(())
                            })
                            .unwrap();
                        assert_eq!(
                            protocol, exported,
                            "immediate re-export cannot derive the sentinel from membership"
                        );
                        assert!(target
                            .import_canonical_processing_rows(&protocol[0])
                            .is_err());
                        for (value, clock) in [None, Some(2)].into_iter().zip(&clocks[4..]) {
                            assert_eq!(
                                source.push_proctime(input(None, value), *clock).unwrap(),
                                target.push_proctime(input(None, value), *clock).unwrap()
                            );
                            assert_eq!(metadata(&source), metadata(&target));
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn processing_protocol_rejects_missing_frames_and_legacy_counters_without_mutation() {
    let mut source = memory(false, 0).with_memory_budget(64 << 20).unwrap();
    source.bind_canonical_frame_inputs().unwrap();
    source.push_proctime(input(None, Some(1)), 100).unwrap();
    let mut protocol = Vec::new();
    source
        .visit_canonical_processing_rows(|batch| {
            protocol.push(batch);
            Ok(())
        })
        .unwrap();
    let mut empty = memory(false, 0).with_memory_budget(64 << 20).unwrap();
    empty.inner.seed_key_codec(&[DataType::Int64]);
    assert!(empty
        .import_canonical_processing_rows(&protocol[0])
        .is_err());
    assert!(empty.proc_rows.is_empty());
    assert_eq!(empty.retention_bytes, 0);
    source.proc_rows.clear();
    assert!(source
        .visit_canonical_processing_rows(|_| panic!("legacy metadata must not be emitted"))
        .is_err());
}

#[test]
fn malformed_processing_metadata_is_rejected_before_any_counter_is_installed() {
    let mut source = memory(false, 0).with_memory_budget(64 << 20).unwrap();
    source.bind_canonical_frame_inputs().unwrap();
    source.push_proctime(input(None, Some(1)), 100).unwrap();
    let mut frames = Vec::new();
    source
        .visit_canonical_frames(|batch| {
            frames.push(batch);
            Ok(())
        })
        .unwrap();
    let mut protocol = Vec::new();
    source
        .visit_canonical_processing_rows(|batch| {
            protocol.push(batch);
            Ok(())
        })
        .unwrap();
    let good = &protocol[0];
    let mut target = memory(false, 0).with_memory_budget(64 << 20).unwrap();
    target.bind_canonical_frame_inputs().unwrap();
    target.import_canonical_frame(&frames[0]).unwrap();
    let mut bad = Vec::new();
    for (column, value) in [
        (1, None),
        (1, Some(0)),
        (1, Some(2)),
        (1, Some(-1)),
        (2, None),
        (3, Some(150)),
    ] {
        let mut columns = good.columns().to_vec();
        columns[column] = Arc::new(Int64Array::from(vec![value]));
        // Arrow allows nulls only when the declared field is nullable, so corrupt that declaration
        // too and let the protocol validate the actual values instead of relying on field flags.
        let mut fields = good
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        fields[column] = fields[column].clone().with_nullable(true);
        bad.push(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap());
    }
    let mut fields = good
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields[1] = fields[1].clone().with_name("unknown_count");
    bad.push(RecordBatch::try_new(Arc::new(Schema::new(fields)), good.columns().to_vec()).unwrap());
    bad.push(arrow::compute::concat_batches(&good.schema(), [good, good]).unwrap());
    let mut fields = good
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let mut columns = good.columns().to_vec();
    fields[1] = Field::new(fields[1].name(), DataType::Float64, false);
    columns[1] = Arc::new(arrow::array::Float64Array::from(vec![1.0]));
    bad.push(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap());
    let retained_bytes = target.retention_bytes;
    for malformed in bad {
        assert!(target.import_canonical_processing_rows(&malformed).is_err());
        assert!(target.proc_rows.is_empty());
        assert_eq!(target.retention_bytes, retained_bytes);
    }
    target.import_canonical_processing_rows(good).unwrap();
    assert_eq!(metadata(&source), metadata(&target));
}

#[test]
fn external_cleanup_fires_without_input_and_preserves_a_pending_minimum_long_timer() {
    let mut op = memory(false, 2)
        .with_state_retention(1000)
        .with_memory_budget(64 << 20)
        .unwrap();
    op.processing_rows_external_timers = true;
    let now = i64::MAX - 1499;
    op.push_proctime(input(None, Some(1)), now).unwrap();
    assert_eq!(
        op.cleanup_state.values().copied().collect::<Vec<_>>(),
        vec![i64::MIN]
    );
    // No host timer fired between these two batches at the same processing-time tick.
    let result = op.push_proctime(input(None, Some(2)), now).unwrap();
    assert_eq!(
        ScalarValue::try_from_array(result.column(2), 0).unwrap(),
        ScalarValue::Int64(Some(3))
    );
    op.fire_processing_rows_cleanup(i64::MIN).unwrap();
    assert!(op.proc_rows.is_empty());
    assert!(op.cleanup_state.is_empty());
    assert_eq!(op.retention_bytes, 0);
    let result = op.push_proctime(input(None, Some(7)), 100).unwrap();
    assert_eq!(
        ScalarValue::try_from_array(result.column(2), 0).unwrap(),
        ScalarValue::Int64(Some(7))
    );
}
