use super::*;

fn storage(directory: &std::path::Path) -> Storage {
    Storage {
        config: RocksStoreConfig {
            table_dir: directory.to_string_lossy().into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 1000,
            shared_resources: 0,
        },
        restore: Restore {
            sources: Vec::new(),
            key_groups: 0..=127,
            aligned: true,
            partitions: Vec::new(),
        },
    }
}

fn group_options(now_millis: i64) -> GroupOptions {
    GroupOptions {
        aggregate_kinds: vec![0],
        value_types: vec![0],
        value_columns: vec![1],
        key_columns: vec![0],
        key_timestamp_precisions: vec![-1],
        filter_columns: Vec::new(),
        count_columns: Vec::new(),
        distinct_view_columns: Vec::new(),
        record_count_column: -1,
        generate_update_before: true,
        mini_batch: false,
        state_ttl_millis: 1000,
        now_millis,
        memory_budget_bytes: 16 << 20,
        emit_unchanged_with_ttl: true,
    }
}

fn input(value: i64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("k", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("v", Arc::new(Int64Array::from(vec![value])) as ArrayRef),
    ])
    .unwrap()
}

fn totals(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

#[test]
fn typed_group_lifecycle_restores_aligned_files_and_clips_non_aligned_key_groups() {
    let directory = tempfile::tempdir().unwrap();
    let mut original = create_group_aggregator_with_ttl_emission(
        storage(&directory.path().join("original")),
        group_options(5000),
    )
    .unwrap();
    original.store_mut().set_clock(5000);
    assert_eq!(totals(&original.update(&input(10), 5000).unwrap()), [10]);
    let checkpoint = directory.path().join("checkpoint");
    let manifest =
        checkpoint_group_aggregator(&mut original, checkpoint.to_str().unwrap()).unwrap();
    drop(original);

    let mut aligned = storage(&directory.path().join("aligned"));
    aligned.restore.sources.push((
        checkpoint.to_string_lossy().into_owned(),
        manifest.snapshot_id,
    ));
    let mut restored =
        create_group_aggregator_with_ttl_emission(aligned, group_options(5500)).unwrap();
    restored.store_mut().set_clock(5500);
    assert_eq!(totals(&restored.update(&input(5), 5500).unwrap()), [10, 15]);
    assert_eq!(restored.memory.state_bytes(), 0);
    drop(restored);

    let data = input(1);
    let mut encoder = BinaryRowBatchEncoder::new(&data, &[0], &[-1]);
    let group = flink_key_group(hash_bytes_by_words(encoder.encode(0)), 128) as i32;
    let other = (group + 1) % 128;
    let mut clipped = storage(&directory.path().join("clipped"));
    clipped.restore.sources.push((
        checkpoint.to_string_lossy().into_owned(),
        manifest.snapshot_id,
    ));
    clipped.restore.aligned = false;
    clipped.restore.key_groups = other..=other;
    let mut clipped =
        create_group_aggregator_with_ttl_emission(clipped, group_options(5500)).unwrap();
    clipped.store_mut().set_clock(5500);
    assert_eq!(totals(&clipped.update(&input(7), 5500).unwrap()), [7]);
}

#[test]
fn constructor_failure_releases_the_open_store_before_a_fresh_retry() {
    let directory = tempfile::tempdir().unwrap();
    let original_path = directory.path().join("source");
    let mut original =
        create_group_aggregator_with_ttl_emission(storage(&original_path), group_options(5000))
            .unwrap();
    original.store_mut().set_clock(5000);
    original.update(&input(10), 5000).unwrap();
    let partitions = original
        .canonical_partitions()
        .unwrap()
        .into_values()
        .collect::<Vec<_>>();
    let destination = directory.path().join("destination");
    let mut denied = storage(&destination);
    denied.restore.partitions = partitions;
    let mut options = group_options(5000);
    options.memory_budget_bytes = 1;
    assert!(matches!(
        create_group_aggregator_with_ttl_emission(denied, options),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    // A live failed constructor would retain RocksDB's directory lock.
    drop(
        create_group_aggregator_with_ttl_emission(storage(&destination), group_options(5000))
            .unwrap(),
    );
    assert_eq!(totals(&original.update(&input(5), 5500).unwrap()), [10, 15]);
}

#[test]
fn fallback_snapshot_lifecycle_retains_partitions_timer_and_manifest_wire_order() {
    let directory = tempfile::tempdir().unwrap();
    let mut initial = create_snapshot_store(storage(&directory.path().join("initial"))).unwrap();
    let checkpoint = directory.path().join("checkpoint");
    let partitions = vec![vec![0, 0, 0, 0, 1, 2, 3]];
    let manifest = checkpoint_snapshot_store(
        &mut initial,
        &partitions,
        1234,
        checkpoint.to_str().unwrap(),
    )
    .unwrap();
    let lines = manifest_lines(&manifest);
    assert_eq!(lines[0], manifest.snapshot_id.to_string());
    assert_eq!(
        &lines[1..1 + manifest.data_files.len()],
        manifest
            .data_files
            .iter()
            .map(|file| format!("d:{file}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        &lines[1 + manifest.data_files.len()..],
        manifest
            .meta_files
            .iter()
            .map(|file| format!("m:{file}"))
            .collect::<Vec<_>>()
    );
    drop(initial);
    let mut restored = storage(&directory.path().join("restored"));
    restored.restore.sources.push((
        checkpoint.to_string_lossy().into_owned(),
        manifest.snapshot_id,
    ));
    let restored = create_snapshot_store(restored).unwrap();
    assert_eq!(restored.timer_deadline(), 1234);
    assert_eq!(restored.partitions().unwrap(), [vec![1, 2, 3]]);
    assert_eq!(
        manifest_lines(&RocksCheckpointManifest::absent()),
        [String::new()]
    );
}

#[test]
fn checkpoint_io_failure_keeps_the_typed_operator_usable() {
    let directory = tempfile::tempdir().unwrap();
    let mut operator = create_group_aggregator_with_ttl_emission(
        storage(&directory.path().join("operator")),
        group_options(5000),
    )
    .unwrap();
    operator.store_mut().set_clock(5000);
    operator.update(&input(10), 5000).unwrap();
    let blocked = directory.path().join("file");
    std::fs::write(&blocked, b"not a checkpoint directory").unwrap();
    assert!(matches!(
        checkpoint_group_aggregator(&mut operator, blocked.to_str().unwrap()),
        Err(DataFusionError::External(_))
    ));
    assert_eq!(totals(&operator.update(&input(5), 5500).unwrap()), [10, 15]);
    assert_eq!(operator.memory.state_bytes(), 0);
}
