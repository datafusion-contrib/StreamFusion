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
