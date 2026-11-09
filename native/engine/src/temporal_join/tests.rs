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
fn disk_config(path: &std::path::Path) -> RocksStoreConfig {
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
    RocksStoreConfig {
        table_dir: path.to_string_lossy().into_owned(),
        max_parallelism: 128,
        options_json: serde_json::to_string(&options).unwrap(),
        ttl_ms: 0,
        shared_resources: 0,
    }
}

#[cfg(feature = "rocksdb-state")]
fn disk(path: &std::path::Path) -> crate::state::RocksTemporalJoinStore {
    crate::state::RocksTemporalJoinStore::create(disk_config(path), schema(), schema(), &[0])
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

fn check_deferred_versions(source_disk: bool) {
    let dir = tempfile::tempdir().unwrap();
    let mut joiner = operator(source_disk.then_some(dir.path())).with_state_retention(0);
    let data = |value, time| {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![value])),
                Arc::new(Int64Array::from(vec![time])),
            ],
        )
        .unwrap()
    };
    joiner.push_right(&data(100, 100), 0).unwrap();
    joiner.push_right(&data(300, 300), 0).unwrap();
    assert_eq!(joiner.advance(100, 0).unwrap().num_rows(), 0);
    // The first callback has fired. Stock has no further event callback merely because a newer
    // build version remains; this watermark must leave version100 available to a late probe.
    assert_eq!(joiner.advance(500, 0).unwrap().num_rows(), 0);
    joiner.push_left(&data(1, 150), 0).unwrap();
    let result = joiner.advance(600, 0).unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(
        ScalarValue::try_from_array(result.column(4), 0).unwrap(),
        ScalarValue::Int64(Some(100)),
        "match released future_build metadata/late-probe trace"
    );
}

#[test]
fn memory_callback_ownership_retains_versions_for_late_probes() {
    check_deferred_versions(false);
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn persistent_callback_ownership_retains_versions_for_late_probes() {
    check_deferred_versions(true);
}

fn check_stock_trace(source_disk: bool, target_disk: bool, sst: bool, aligned: bool, direct: bool) {
    let mut directories = Vec::new();
    fn create(
        directories: &mut Vec<tempfile::TempDir>,
        disk: bool,
        retention: i64,
    ) -> TemporalJoiner {
        directories.push(tempfile::tempdir().unwrap());
        operator(disk.then_some(directories.last().unwrap().path())).with_state_retention(retention)
    }
    let trace = include_str!("../../../../src/test/resources/canonical-temporal-metadata.tsv");
    let mut scenario = "";
    let mut joiner = create(&mut directories, source_disk, 0);
    let mut now = 0;
    let mut ordinal = 0;
    let data = |value, time| {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![value])),
                Arc::new(Int64Array::from(vec![time])),
            ],
        )
        .unwrap()
    };
    let key = read_key(&[data(0, 0).column(0)], 0);
    for line in trace
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
    {
        let row = line.split('\t').collect::<Vec<_>>();
        let retention: i64 = row[1].parse().unwrap();
        if scenario != row[0] {
            scenario = row[0];
            now = 0;
            ordinal = 0;
            joiner = create(&mut directories, source_disk, retention);
        }
        let argument: i64 = row[3].parse().unwrap();
        match row[2] {
            "L" => {
                ordinal += 1;
                joiner.push_left(&data(ordinal, argument), now).unwrap();
            }
            "R" => {
                joiner.push_right(&data(argument, argument), now).unwrap();
            }
            "S" => {
                #[cfg(feature = "rocksdb-state")]
                if let Some(store) = &mut joiner.store {
                    let batch = data(0, 0);
                    let keys = store.entry_keys(&batch, &[0], &[-1]);
                    let mut metadata = store
                        .all_metadata()
                        .get(&keys[0])
                        .cloned()
                        .unwrap_or_default();
                    metadata.next_index = Some(argument);
                    let bytes = metadata.encode();
                    store
                        .import_metadata(
                            &RecordBatch::try_new(
                                Arc::new(Schema::new(vec![
                                    Field::new("k", DataType::Int64, false),
                                    Field::new("meta", DataType::Binary, false),
                                ])),
                                vec![
                                    batch.column(0).clone(),
                                    Arc::new(BinaryArray::from_iter_values([bytes.as_slice()])),
                                ],
                            )
                            .unwrap(),
                            &[-1],
                        )
                        .unwrap();
                } else {
                    joiner.metadata.entry(key.clone()).or_default().next_index = Some(argument);
                }
                #[cfg(not(feature = "rocksdb-state"))]
                {
                    joiner.metadata.entry(key.clone()).or_default().next_index = Some(argument);
                }
            }
            "C" | "T" => {
                now = argument;
                joiner.expire_processing_time(now).unwrap();
            }
            "W" => {
                let output = joiner.advance(argument, now).unwrap();
                if scenario == "overflow" {
                    let values = output
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert_eq!(values.values(), &[2, 1], "{line}");
                }
                if scenario == "future_build" && argument == 600 {
                    assert_eq!(output.num_rows(), 1);
                    assert_eq!(
                        ScalarValue::try_from_array(output.column(4), 0).unwrap(),
                        ScalarValue::Int64(Some(100))
                    );
                }
            }
            _ => unreachable!(),
        }
        let number = |text: &str| (text != "-").then(|| text.parse::<i64>().unwrap());
        let inspect = |joiner: &TemporalJoiner| {
            #[cfg(feature = "rocksdb-state")]
            if let Some(store) = &joiner.store {
                let keys = store.entry_keys(&data(0, 0), &[0], &[-1]);
                return (
                    store
                        .all_metadata()
                        .get(&keys[0])
                        .cloned()
                        .unwrap_or_default(),
                    store.deadline(&keys[0]),
                );
            }
            (
                joiner.metadata.get(&key).cloned().unwrap_or_default(),
                joiner.cleanup_state.get(&key).copied(),
            )
        };
        let (state, deadline) = inspect(&joiner);
        assert_eq!(state.next_index, number(row[6]), "{line}");
        assert_eq!(state.registered, number(row[7]), "{line}");
        let callbacks = if row[8] == "-" {
            std::collections::BTreeSet::new()
        } else {
            row[8]
                .split(',')
                .map(|value| value.parse::<i64>().unwrap())
                .collect()
        };
        assert_eq!(state.callbacks, callbacks, "{line}");
        assert_eq!(deadline, number(row[9]), "{line}");
        if direct {
            let directory = tempfile::tempdir().unwrap();
            let mut recovered =
                operator(target_disk.then_some(directory.path())).with_state_retention(retention);
            for left in [true, false] {
                joiner
                    .visit_canonical_rows(left, |batch| {
                        recovered.import_canonical_rows(left, &batch)
                    })
                    .unwrap();
            }
            joiner
                .visit_canonical_metadata(|batch| recovered.import_canonical_metadata(&batch))
                .unwrap();
            assert_eq!(inspect(&recovered), (state, deadline), "{line}");
            joiner = recovered;
            directories.push(directory);
            continue;
        }

        #[cfg(feature = "rocksdb-state")]
        if sst {
            let snapshot = tempfile::tempdir().unwrap();
            let snapshot_path = snapshot
                .path()
                .join("snapshot")
                .to_string_lossy()
                .into_owned();
            let manifest = joiner.store_mut().checkpoint(&snapshot_path).unwrap();
            let target = tempfile::tempdir().unwrap();
            let store = crate::state::RocksTemporalJoinStore::open_merged(
                disk_config(target.path()),
                schema(),
                schema(),
                &[0],
                &[(snapshot_path, manifest.snapshot_id)],
                0..=127,
                aligned,
            )
            .unwrap();
            let recovered = operator(None)
                .with_state_retention(retention)
                .with_store(store);
            assert_eq!(
                recovered.store.as_ref().unwrap().source_cleaning(),
                Some(retention > 1),
                "{line}"
            );
            assert_eq!(inspect(&recovered), (state, deadline), "{line}");
            joiner = recovered;
            directories.push(target);
            continue;
        }
        #[cfg(feature = "rocksdb-state")]
        let parts = if joiner.store.is_some() {
            joiner
                .canonical_partitions()
                .unwrap()
                .into_values()
                .collect::<Vec<_>>()
        } else {
            joiner
                .snapshot_partitions(128, &[-1])
                .into_values()
                .collect::<Vec<_>>()
        };
        #[cfg(not(feature = "rocksdb-state"))]
        let parts = joiner
            .snapshot_partitions(128, &[-1])
            .into_values()
            .collect::<Vec<_>>();
        let mut recovered = TemporalJoiner::restore_partitions(
            vec![0],
            vec![0],
            2,
            2,
            JoinKind::LeftOuter,
            schema(),
            schema(),
            None,
            &parts,
            retention,
            now,
        );
        #[cfg(feature = "rocksdb-state")]
        if target_disk {
            let directory = tempfile::tempdir().unwrap();
            recovered = operator(Some(directory.path())).with_state_retention(retention);
            recovered.import_partitions(&parts).unwrap();
            recovered.adopt_store_retention(now).unwrap();
            directories.push(directory);
        }
        assert_eq!(inspect(&recovered), (state, deadline), "{line}");
        joiner = recovered;
    }
}

#[test]
fn memory_stock_metadata_and_rows_follow_released_trace_through_each_native_checkpoint() {
    check_stock_trace(false, false, false, false, false);
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn persistent_stock_trace_survives_each_raw_checkpoint() {
    for (source, target) in [(false, true), (true, false), (true, true)] {
        check_stock_trace(source, target, false, false, false);
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn persistent_stock_trace_survives_each_sst_checkpoint() {
    for aligned in [false, true] {
        check_stock_trace(true, true, true, aligned, false);
    }
}

#[test]
fn frozen_legacy_raw_snapshots_restore_without_rewriting_the_fixture() {
    for (retention, bytes) in [
        (
            0,
            include_bytes!("../../../../src/test/resources/canonical-temporal-legacy/raw-0.bin")
                .as_slice(),
        ),
        (
            100,
            include_bytes!("../../../../src/test/resources/canonical-temporal-legacy/raw-100.bin")
                .as_slice(),
        ),
    ] {
        for target_disk in [false, true] {
            #[cfg(not(feature = "rocksdb-state"))]
            if target_disk {
                continue;
            }
            let dir = tempfile::tempdir().unwrap();
            let mut joiner = TemporalJoiner::restore(
                vec![0],
                vec![0],
                2,
                2,
                JoinKind::LeftOuter,
                schema(),
                schema(),
                None,
                bytes,
                100,
                1050,
            );
            assert_eq!(
                read_framed_sections(bytes).len(),
                if retention == 0 { 2 } else { 3 }
            );
            #[cfg(feature = "rocksdb-state")]
            if target_disk {
                joiner = operator(Some(dir.path()));
                joiner.import_partitions(&[bytes.to_vec()]).unwrap();
                joiner.adopt_store_retention(1050).unwrap();
            }
            let deadline = if retention == 0 { 1200 } else { 1150 };
            #[cfg(feature = "rocksdb-state")]
            if let Some(store) = &joiner.store {
                let key = &store.entry_keys(&batch(vec![1]), &[0], &[-1])[0];
                assert_eq!(store.deadline(key), Some(deadline));
            } else {
                assert_eq!(
                    joiner.cleanup_state.values().copied().collect::<Vec<_>>(),
                    vec![deadline]
                );
            }
            #[cfg(not(feature = "rocksdb-state"))]
            assert_eq!(
                joiner.cleanup_state.values().copied().collect::<Vec<_>>(),
                vec![deadline]
            );
            let output = joiner.advance(100, 1050).unwrap();
            assert_eq!(output.num_rows(), 2);
            assert_eq!(
                output
                    .column(4)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values(),
                &[10, 10]
            );
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn frozen_legacy_sst_checkpoints_upgrade_global_sequences_on_both_restore_paths() {
    for retention in [0, 100] {
        for aligned in [false, true] {
            let source = tempfile::tempdir().unwrap();
            let files: [(&str, &[u8]); 3] = if retention == 0 {
                [("000008.sst", include_bytes!("../../../../src/test/resources/canonical-temporal-legacy/sst-0/000008.sst")),
                 ("CURRENT", include_bytes!("../../../../src/test/resources/canonical-temporal-legacy/sst-0/CURRENT")),
                 ("MANIFEST-000005", include_bytes!("../../../../src/test/resources/canonical-temporal-legacy/sst-0/MANIFEST-000005"))]
            } else {
                [("000008.sst", include_bytes!("../../../../src/test/resources/canonical-temporal-legacy/sst-100/000008.sst")),
                 ("CURRENT", include_bytes!("../../../../src/test/resources/canonical-temporal-legacy/sst-100/CURRENT")),
                 ("MANIFEST-000005", include_bytes!("../../../../src/test/resources/canonical-temporal-legacy/sst-100/MANIFEST-000005"))]
            };
            for (name, bytes) in files {
                std::fs::write(source.path().join(name), bytes).unwrap();
            }
            let target = tempfile::tempdir().unwrap();
            let store = crate::state::RocksTemporalJoinStore::open_merged(
                disk_config(target.path()),
                schema(),
                schema(),
                &[0],
                &[(source.path().to_string_lossy().into_owned(), 0)],
                0..=127,
                aligned,
            )
            .unwrap();
            let mut joiner = operator(None).with_store(store);
            joiner.adopt_store_retention(1050).unwrap();
            let key = &joiner.store_mut().entry_keys(&batch(vec![1]), &[0], &[-1])[0];
            assert_eq!(joiner.store_mut().all_metadata()[key].next_index, Some(2));
            assert_eq!(
                joiner.store_mut().deadline(key),
                Some(if retention == 0 { 1200 } else { 1150 })
            );
            let output = joiner.advance(100, 1050).unwrap();
            assert_eq!(output.num_rows(), 2);
            assert_eq!(
                output
                    .column(4)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values(),
                &[10, 10]
            );
        }
    }
}

#[test]
fn orphan_callbacks_are_accounted_until_their_actual_event_firing() {
    for persistent in [false, true] {
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let mut joiner = operator(persistent.then_some(directory.path()));
        #[cfg(feature = "rocksdb-state")]
        if persistent {
            joiner = joiner.with_read_through_budget(1_000_000).unwrap();
        }
        if !persistent {
            joiner = joiner.with_memory_budget(1_000_000).unwrap();
        }
        joiner.push_left(&batch(vec![1]), 0).unwrap();
        joiner.expire_processing_time(150).unwrap();
        assert!(
            joiner.memory.state_bytes > 0,
            "orphan event callbacks own resident memory"
        );
        let bytes = joiner.memory.state_bytes;
        joiner.advance(99, 150).unwrap();
        assert_eq!(joiner.memory.state_bytes, bytes);
        joiner.advance(100, 150).unwrap();
        assert_eq!(joiner.memory.state_bytes, 0);
    }
}

#[test]
fn snapshot_helper_names_cannot_shadow_user_columns() {
    let names = Arc::new(Schema::new(vec![
        Field::new("__time__", DataType::Int64, false),
        Field::new("__kind__", DataType::Int64, false),
        Field::new("__sequence__", DataType::Int64, false),
    ]));
    let data = RecordBatch::try_new(names.clone(), batch(vec![1]).columns().to_vec()).unwrap();
    let construct = || {
        TemporalJoiner::new(
            vec![0],
            vec![0],
            2,
            2,
            JoinKind::LeftOuter,
            names.clone(),
            names.clone(),
            None,
        )
        .with_key_timestamp_precisions(vec![-1])
    };
    let mut source = construct();
    source.push_left(&data, 0).unwrap();
    source.push_right(&data, 0).unwrap();
    let parts = source
        .snapshot_partitions(128, &[-1])
        .into_values()
        .collect::<Vec<_>>();
    for target_disk in [false, true] {
        #[cfg(not(feature = "rocksdb-state"))]
        if target_disk {
            continue;
        }
        let dir = tempfile::tempdir().unwrap();
        let mut target = TemporalJoiner::restore_partitions(
            vec![0],
            vec![0],
            2,
            2,
            JoinKind::LeftOuter,
            names.clone(),
            names.clone(),
            None,
            &parts,
            0,
            0,
        );
        #[cfg(feature = "rocksdb-state")]
        if target_disk {
            let store = crate::state::RocksTemporalJoinStore::create(
                disk_config(dir.path()),
                names.clone(),
                names.clone(),
                &[0],
            )
            .unwrap();
            target = construct().with_store(store);
            target.import_partitions(&parts).unwrap();
        }
        let out = target.advance(100, 0).unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(
            out.column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            10
        );
    }
}

#[test]
fn orphan_callback_groups_rescale_without_any_rows_or_cleanup_deadlines() {
    for source_disk in [false, true] {
        #[cfg(not(feature = "rocksdb-state"))]
        if source_disk {
            continue;
        }
        let dir = tempfile::tempdir().unwrap();
        let mut source = operator(source_disk.then_some(dir.path()));
        source.push_left(&batch((-64..0).collect()), 0).unwrap();
        source.expire_processing_time(150).unwrap();
        #[cfg(feature = "rocksdb-state")]
        let parts = if source_disk {
            source.canonical_partitions().unwrap()
        } else {
            source.snapshot_partitions(128, &[-1])
        };
        #[cfg(not(feature = "rocksdb-state"))]
        let parts = source.snapshot_partitions(128, &[-1]);
        assert!(!parts.is_empty());
        for bytes in parts.values() {
            let sections = read_framed_sections(bytes);
            assert!(sections[..3].iter().all(Vec::is_empty));
            assert!(sections[3].len() > 2);
        }
        for target_disk in [false, true] {
            #[cfg(not(feature = "rocksdb-state"))]
            if target_disk {
                continue;
            }
            let mut count = 0;
            for subtask in 0..3 {
                let chunks = parts
                    .iter()
                    .filter(|(group, _)| **group % 3 == subtask)
                    .map(|(_, bytes)| bytes.clone())
                    .collect::<Vec<_>>();
                let target_dir = tempfile::tempdir().unwrap();
                let mut target = TemporalJoiner::restore_partitions(
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
                    150,
                );
                #[cfg(feature = "rocksdb-state")]
                if target_disk {
                    target = operator(Some(target_dir.path()));
                    target.import_partitions(&chunks).unwrap();
                    target.adopt_store_retention(150).unwrap();
                }
                #[cfg(feature = "rocksdb-state")]
                let metadata_count = if target_disk {
                    target.store_mut().all_metadata().len()
                } else {
                    target.metadata.len()
                };
                #[cfg(not(feature = "rocksdb-state"))]
                let metadata_count = target.metadata.len();
                count += metadata_count;
                assert_eq!(target.advance(100, 150).unwrap().num_rows(), 0);
                #[cfg(feature = "rocksdb-state")]
                if target_disk {
                    assert!(target.store_mut().all_metadata().is_empty());
                }
                assert!(target.metadata.is_empty());
            }
            assert_eq!(count, 64);
        }
    }
}

#[test]
fn externally_scheduled_cleanup_does_not_expire_keys_before_the_callback() {
    for persistent in [false, true] {
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let mut joiner = operator(persistent.then_some(directory.path()));
        joiner.external_cleanup_timers = true;
        joiner.push_left(&batch(vec![1]), 0).unwrap();
        assert_eq!(joiner.next_cleanup(), Some(150));
        assert_eq!(joiner.advance(99, 150).unwrap().num_rows(), 0);
        assert_eq!(joiner.next_cleanup(), Some(150));
        // The mailbox can deliver another record before the scheduled cleanup. A touch replaces
        // the old deadline; calling the cancelled callback must not clear the refreshed key.
        joiner.push_left(&batch(vec![1]), 150).unwrap();
        assert_eq!(joiner.next_cleanup(), Some(300));
        joiner.expire_processing_time(150).unwrap();
        assert_eq!(joiner.advance(100, 150).unwrap().num_rows(), 2);
        assert_eq!(joiner.next_cleanup(), None);
    }
}

#[test]
fn cleanup_minimum_preserves_the_entire_signed_deadline_range() {
    for persistent in [false, true] {
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let mut joiner = operator(persistent.then_some(directory.path()));
        for (key, deadline) in [(1, i64::MAX), (2, i64::MIN)] {
            #[cfg(feature = "rocksdb-state")]
            if let Some(store) = &mut joiner.store {
                let keys = store.entry_keys(&batch(vec![key]), &[0], &[-1]);
                store.set_deadline(&keys[0], deadline).unwrap();
            } else {
                joiner
                    .cleanup_state
                    .insert(vec![ScalarValue::Int64(Some(key))], deadline);
            }
            #[cfg(not(feature = "rocksdb-state"))]
            {
                joiner
                    .cleanup_state
                    .insert(vec![ScalarValue::Int64(Some(key))], deadline);
            }
        }
        assert_eq!(joiner.next_cleanup(), Some(i64::MIN));
        joiner.expire_processing_time(i64::MIN).unwrap();
        assert_eq!(joiner.next_cleanup(), Some(i64::MAX));
        joiner.expire_processing_time(i64::MAX).unwrap();
        assert_eq!(joiner.next_cleanup(), None);
    }
}

#[test]
fn processing_cleanup_deadlines_use_released_java_long_arithmetic() {
    for persistent in [false, true] {
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        for retention in [100, i64::MAX / 2 + 1, i64::MAX] {
            let directory = tempfile::tempdir().unwrap();
            let mut joiner =
                operator(persistent.then_some(directory.path())).with_state_retention(retention);
            joiner.external_cleanup_timers = true;
            let now = i64::MAX - 50;
            let max = retention.wrapping_mul(3) / 2;
            assert_eq!(joiner.max_retention_ms, max);
            joiner.push_left(&batch(vec![1]), now).unwrap();
            let first = now.wrapping_add(max);
            assert_eq!(joiner.next_cleanup(), Some(first));
            let next_now = now.wrapping_add(25);
            joiner.push_right(&batch(vec![1]), next_now).unwrap();
            let expected = if next_now.wrapping_add(retention) > first {
                next_now.wrapping_add(max)
            } else {
                first
            };
            assert_eq!(joiner.next_cleanup(), Some(expected));
        }
    }
}

#[test]
fn released_trace_continues_after_each_direct_live_state_transfer() {
    for source in [false, true] {
        #[cfg(not(feature = "rocksdb-state"))]
        if source {
            continue;
        }
        for target in [false, true] {
            #[cfg(not(feature = "rocksdb-state"))]
            if target {
                continue;
            }
            check_stock_trace(source, target, false, false, true);
        }
    }
}

#[test]
fn direct_visitors_are_bounded_read_only_and_retryable_after_receiver_failures() {
    for persistent in [false, true] {
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let mut source = operator(persistent.then_some(directory.path())).with_state_retention(0);
        source.push_left(&batch((0..1500).collect()), 0).unwrap();
        let capture = |source: &TemporalJoiner, metadata: bool| {
            let mut result = Vec::new();
            if metadata {
                source
                    .visit_canonical_metadata(|batch| {
                        assert!(batch.num_rows() <= 1024);
                        result.push(batch);
                        Ok(())
                    })
                    .unwrap();
            } else {
                source
                    .visit_canonical_rows(true, |batch| {
                        assert!(batch.num_rows() <= 1024);
                        result.push(batch);
                        Ok(())
                    })
                    .unwrap();
            }
            assert_eq!(
                result.iter().map(RecordBatch::num_rows).sum::<usize>(),
                1500
            );
            assert_eq!(result.len(), 2);
            result
        };
        for metadata in [false, true] {
            let before = capture(&source, metadata);
            let failed = if metadata {
                source.visit_canonical_metadata(|_| {
                    Err(DataFusionError::Execution("receiver failed".into()))
                })
            } else {
                source.visit_canonical_rows(true, |_| {
                    Err(DataFusionError::Execution("receiver failed".into()))
                })
            };
            assert!(failed.unwrap_err().to_string().contains("receiver failed"));
            assert_eq!(capture(&source, metadata), before);
        }
        assert_eq!(source.advance(100, 0).unwrap().num_rows(), 1500);
    }
}

#[test]
fn direct_import_rejects_duplicates_bad_callbacks_and_import_after_execution() {
    for persistent in [false, true] {
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        let source_directory = tempfile::tempdir().unwrap();
        let mut source = operator(persistent.then_some(source_directory.path()));
        source.push_left(&batch(vec![1]), 0).unwrap();
        let mut rows = Vec::new();
        let mut metadata = Vec::new();
        source
            .visit_canonical_rows(true, |batch| {
                rows.push(batch);
                Ok(())
            })
            .unwrap();
        source
            .visit_canonical_metadata(|batch| {
                metadata.push(batch);
                Ok(())
            })
            .unwrap();
        let target_directory = tempfile::tempdir().unwrap();
        let mut target = operator(persistent.then_some(target_directory.path()));
        let bad = RecordBatch::try_new(metadata[0].schema(), {
            let mut columns = metadata[0].columns().to_vec();
            columns[4] = Arc::new(ListArray::from_iter_primitive::<
                arrow::array::types::Int64Type,
                _,
                _,
            >([Some([Some(100), Some(100)])]));
            columns
        })
        .unwrap();
        assert!(target
            .import_canonical_metadata(&bad)
            .unwrap_err()
            .to_string()
            .contains("duplicate or unordered"));
        target.import_canonical_rows(true, &rows[0]).unwrap();
        assert!(target
            .import_canonical_rows(true, &rows[0])
            .unwrap_err()
            .to_string()
            .contains("already imported"));
        target.import_canonical_metadata(&metadata[0]).unwrap();
        assert!(target
            .import_canonical_metadata(&metadata[0])
            .unwrap_err()
            .to_string()
            .contains("already imported"));
        assert_eq!(target.advance(100, 0).unwrap().num_rows(), 1);
        assert!(target
            .import_canonical_rows(true, &rows[0])
            .unwrap_err()
            .to_string()
            .contains("has not started execution"));
        assert!(target
            .import_canonical_metadata(&metadata[0])
            .unwrap_err()
            .to_string()
            .contains("has not started execution"));
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn captured_temporal_readers_stream_metadata_without_hydrating_live_caches() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    let mut source = operator(Some(&path));
    source.push_left(&batch((0..2051).collect()), 10).unwrap();
    let orphan = source
        .store_mut()
        .entry_keys(&batch(vec![9999]), &[0], &[-1])
        .pop()
        .unwrap();
    source.store_mut().set_deadline(&orphan, 100000).unwrap();
    source.store_mut().checkpoint("").unwrap();
    let collect = |value: &TemporalJoiner| {
        let mut rows = Vec::new();
        value
            .visit_canonical_metadata(|batch| {
                assert!(batch.num_rows() <= 1024);
                for row in 0..batch.num_rows() {
                    rows.push(
                        batch
                            .columns()
                            .iter()
                            .map(|column| {
                                format!("{:?}", ScalarValue::try_from_array(column, row).unwrap())
                            })
                            .collect::<Vec<_>>()
                            .join("|"),
                    );
                }
                Ok(())
            })
            .unwrap();
        rows.sort();
        rows
    };
    let expected = collect(&source);
    let expected_records = flat_batches(&source);
    assert_eq!(expected.len(), 2052);
    let snapshot = crate::state::rocks_store::FrozenDatabases::capture(&path).unwrap();
    source.push_left(&batch(vec![0]), 20).unwrap();
    source.store_mut().set_deadline(&orphan, 200000).unwrap();
    source.store_mut().checkpoint("").unwrap();
    snapshot.with(|| {
        let store = crate::state::RocksTemporalJoinStore::open_merged(
            disk_config(&path),
            schema(),
            schema(),
            &[0],
            &[(path.to_string_lossy().into_owned(), 0)],
            0..=127,
            true,
        )
        .unwrap();
        assert!(store.all_metadata().is_empty());
        assert!(store.all_deadlines().is_empty());
        assert_eq!(store.metadata_bytes(), 0);
        let reader = operator(None).with_store(store);
        assert!(reader
            .visit_canonical_metadata(|_| Err(DataFusionError::Execution("receiver failed".into())))
            .is_err());
        assert_eq!(collect(&reader), expected);
        assert_eq!(flat_batches(&reader), expected_records);
        assert!(reader.store.as_ref().unwrap().all_metadata().is_empty());
        assert!(reader.store.as_ref().unwrap().all_deadlines().is_empty());
        snapshot.cancel();
        assert!(reader.visit_canonical_metadata(|_| Ok(())).is_err());
        assert!(reader.visit_canonical_records(|_| Ok(())).is_err());
    });
}

fn flat_metadata(rows: &[(i64, i8, i64)]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "key0",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
        ),
        (
            "__temporal_metadata_kind",
            Arc::new(Int8Array::from_iter_values(rows.iter().map(|r| r.1))) as ArrayRef,
        ),
        (
            "__temporal_metadata_value",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn flat_batches(op: &TemporalJoiner) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    op.visit_canonical_records(|batch| {
        assert!(batch.num_rows() <= 1024);
        batches.push(batch);
        Ok(())
    })
    .unwrap();
    batches
}

#[test]
fn flat_temporal_metadata_restores_hot_callbacks_and_independent_scalars() {
    for persistent in [false, true] {
        #[cfg(not(feature = "rocksdb-state"))]
        if persistent {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let mut target = operator(persistent.then_some(directory.path()))
            .with_memory_budget(8 << 20)
            .unwrap();
        let mut rows = vec![(7, 0, 0), (7, 1, i64::MIN), (7, 2, 1024), (7, 3, 42)];
        rows.extend((0..2051).map(|time| (7, 4, time)));
        rows.extend([
            (8, 0, 0),
            (8, 3, 77),
            (9, 0, 0),
            (9, 4, i64::MIN),
            (9, 4, i64::MAX),
        ]);
        for validating in [true, false] {
            target
                .transfer_canonical_records(&flat_metadata(&[]), false, validating)
                .unwrap();
            for chunk in rows.chunks(1024) {
                target
                    .transfer_canonical_records(&flat_metadata(chunk), false, validating)
                    .unwrap();
                assert!(target.ensure_canonical_ready().is_err());
                assert!(target.visit_canonical_rows(true, |_| Ok(())).is_err());
                assert!(target.expire_processing_time(1000).is_err());
                assert!(target.push_left(&batch(vec![7]), 0).is_err());
            }
            target
                .transfer_canonical_records(&flat_metadata(&[]), true, validating)
                .unwrap();
            assert!(target.ensure_canonical_ready().is_ok());
            if validating {
                assert!(flat_batches(&target).is_empty());
            }
        }
        let exported = flat_batches(&target);
        assert!(exported.len() >= 3);
        let mut actual = exported
            .iter()
            .flat_map(|batch| {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let kinds = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap();
                let values = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(move |row| (keys.value(row), kinds.value(row), values.value(row)))
            })
            .collect::<Vec<_>>();
        actual.sort();
        rows.sort();
        assert_eq!(actual, rows);
    }
}

#[test]
fn flat_temporal_metadata_rejects_corruption_and_keeps_failed_restore_guarded() {
    for rows in [
        vec![(7, 0, 1)],
        vec![(7, 0, 0)],
        vec![(7, 4, 1)],
        vec![(7, 0, 0), (7, 1, 1), (7, 1, 2)],
        vec![(7, 0, 0), (7, 2, 5), (7, 4, 6)],
        vec![(7, 0, 0), (8, 4, 6)],
        vec![(7, 0, 0), (7, 4, 6), (7, 4, 6)],
        vec![(7, 0, 0), (7, 4, 6), (7, 4, 5)],
        vec![(7, 0, 0), (7, 4, 6), (7, 3, 5)],
    ] {
        for validating in [true, false] {
            let mut target = operator(None);
            assert!(
                target
                    .transfer_canonical_records(&flat_metadata(&rows), true, validating)
                    .is_err(),
                "{rows:?}"
            );
            assert!(target.ensure_canonical_ready().is_err(), "{rows:?}");
            assert!(target
                .transfer_canonical_records(&flat_metadata(&[]), true, validating)
                .is_err());
        }
    }
    let mut target = operator(None);
    target
        .transfer_canonical_records(&flat_metadata(&[]), false, true)
        .unwrap();
    assert!(target
        .transfer_canonical_records(&flat_metadata(&[]), true, false)
        .is_err());
    assert!(target.ensure_canonical_ready().is_err());
}

#[test]
fn flat_temporal_metadata_denial_releases_reservations_on_drop() {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
    let mut target = operator(None);
    target
        .memory
        .attach_pool("temporal metadata denial", &pool, 0)
        .unwrap();
    let result =
        target.transfer_canonical_records(&flat_metadata(&[(7, 0, 0), (7, 4, 6)]), false, false);
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    drop(target);
    assert_eq!(pool.reserved(), 0);
}
