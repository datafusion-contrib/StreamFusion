use super::*;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("rt", DataType::Int64, false),
    ]))
}

fn batch(keys: Vec<i64>) -> RecordBatch {
    let length = keys.len();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(vec![10; length])),
            Arc::new(Int64Array::from(vec![100; length])),
        ],
    )
    .unwrap()
}

#[cfg(feature = "rocksdb-state")]
fn disk(path: &std::path::Path) -> crate::state::RocksTemporalJoinStore {
    let options = crate::state::rocks_config::FlinkRocksOptions {
        max_background_threads: 2,
        max_open_files: -1,
        log_max_file_size: 0,
        log_file_num: 1,
        log_directory: None,
        log_level: "INFO_LEVEL".into(),
        compaction_style: "LEVEL".into(),
        compression_per_level: vec!["NO_COMPRESSION".into()],
        use_dynamic_level_size: true,
        target_file_size_base: 4 << 20,
        max_size_level_base: 16 << 20,
        write_buffer_size: 4 << 20,
        max_write_buffer_number: 2,
        min_write_buffer_number_to_merge: 1,
        write_batch_size: 2 << 20,
        compaction_filter_query_time_after_num_entries: 1000,
        periodic_compaction_seconds: 0,
        block_size: 4096,
        metadata_block_size: 4096,
        block_cache_size: 8 << 20,
        use_bloom_filter: false,
        bloom_filter_bits_per_key: 10.0,
        bloom_filter_block_based_mode: false,
    };
    crate::state::RocksTemporalJoinStore::create(
        RocksStoreConfig {
            table_dir: path.to_string_lossy().into_owned(),
            max_parallelism: 128,
            options_json: serde_json::to_string(&options).unwrap(),
            ttl_ms: 0,
            shared_resources: 0,
        },
        schema(),
        schema(),
        &[0],
    )
    .unwrap()
}

fn operator(path: Option<&std::path::Path>) -> TemporalJoiner {
    let joiner = TemporalJoiner::new(
        vec![0],
        vec![0],
        2,
        2,
        JoinKind::LeftOuter,
        schema(),
        schema(),
        None,
    )
    .with_state_retention(100)
    .with_key_timestamp_precisions(vec![-1]);
    #[cfg(feature = "rocksdb-state")]
    if let Some(path) = path {
        return joiner.with_store(disk(path));
    }
    joiner
}

fn check_cleanup_groups(source_disk: bool) {
    for target_disk in [false, true] {
        #[cfg(not(feature = "rocksdb-state"))]
        if target_disk {
            continue;
        }
        for live_rows in [false, true] {
            let source_dir = tempfile::tempdir().unwrap();
            let mut source = operator(source_disk.then_some(source_dir.path()));
            if live_rows {
                source.push_left(&batch(vec![999]), 500).unwrap();
                source.push_right(&batch(vec![999]), 500).unwrap();
            }
            let data = batch((-64..0).collect());
            let mut expected_groups = std::collections::BTreeSet::new();
            for row in 0..data.num_rows() {
                expected_groups
                    .insert(flink_key_group(binary_row_hash(&data, &[0], row, &[-1]), 128) as i32);
                let deadline = 10_000 + row as i64;
                #[cfg(feature = "rocksdb-state")]
                if let Some(store) = &mut source.store {
                    let keys = store.entry_keys(&data, &[0], &[-1]);
                    store.set_deadline(&keys[row], deadline).unwrap();
                    continue;
                }
                source
                    .cleanup_state
                    .insert(read_key(&[data.column(0)], row), deadline);
            }
            let partitions = if source_disk {
                #[cfg(feature = "rocksdb-state")]
                {
                    source.canonical_partitions().unwrap()
                }
                #[cfg(not(feature = "rocksdb-state"))]
                {
                    unreachable!()
                }
            } else {
                source.snapshot_partitions(128, &[-1])
            };
            assert!(
                expected_groups
                    .iter()
                    .all(|group| partitions.contains_key(group)),
                "timer-only key groups must be exported independently of either row side"
            );
            let mut recovered = Vec::new();
            // Each key group goes to exactly one of three simulated rescaled subtasks.
            for subtask in 0..3 {
                let chunks: Vec<Vec<u8>> = partitions
                    .iter()
                    .filter(|(group, _)| **group % 3 == subtask)
                    .map(|(_, bytes)| bytes.clone())
                    .collect();
                let target_dir = tempfile::tempdir().unwrap();
                let mut target = if target_disk {
                    #[cfg(feature = "rocksdb-state")]
                    {
                        let mut target = operator(Some(target_dir.path()));
                        target.import_partitions(&chunks).unwrap();
                        target
                    }
                    #[cfg(not(feature = "rocksdb-state"))]
                    {
                        unreachable!()
                    }
                } else {
                    TemporalJoiner::restore_partitions(
                        vec![0],
                        vec![0],
                        2,
                        2,
                        JoinKind::LeftOuter,
                        schema(),
                        schema(),
                        None,
                        &chunks,
                        100,
                        0,
                    )
                };
                let restored = if target_disk {
                    #[cfg(feature = "rocksdb-state")]
                    {
                        target
                            .canonical_partitions()
                            .unwrap()
                            .into_values()
                            .collect::<Vec<_>>()
                    }
                    #[cfg(not(feature = "rocksdb-state"))]
                    {
                        unreachable!()
                    }
                } else {
                    target
                        .snapshot_partitions(128, &[-1])
                        .into_values()
                        .collect()
                };
                for part in restored {
                    let sections = read_framed_sections(&part);
                    for data in read_ipc_if_present(&sections[2]) {
                        let keys = data
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        let deadlines = column_i64(&data, CLEANUP_AT_COLUMN);
                        for row in 0..data.num_rows() {
                            if keys.value(row) < 0 {
                                recovered.push((keys.value(row), deadlines.value(row)));
                            }
                        }
                    }
                }
                // Preserved timer-only keys expire silently, with no new input required.
                assert_eq!(target.advance(0, 20_000).unwrap().num_rows(), 0);
                let remaining = if target_disk {
                    #[cfg(feature = "rocksdb-state")]
                    {
                        target.store.as_ref().unwrap().all_deadlines().len()
                    }
                    #[cfg(not(feature = "rocksdb-state"))]
                    {
                        unreachable!()
                    }
                } else {
                    target.cleanup_state.len()
                };
                assert_eq!(remaining, 0);
            }
            recovered.sort_unstable();
            assert_eq!(
                recovered,
                (-64..0)
                    .enumerate()
                    .map(|(row, key)| (key, 10_000 + row as i64))
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn memory_timer_only_groups_survive_partitioned_native_recovery() {
    check_cleanup_groups(false);
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn persistent_timer_only_groups_survive_partitioned_native_recovery() {
    check_cleanup_groups(true);
}
