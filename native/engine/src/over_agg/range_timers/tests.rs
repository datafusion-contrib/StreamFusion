use super::*;

fn input(key: Option<i64>, time: i64, value: i64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![key])) as ArrayRef),
        ("value", Arc::new(Int64Array::from(vec![value])) as ArrayRef),
        ("time", Arc::new(Int64Array::from(vec![time])) as ArrayRef),
    ])
    .unwrap()
}

fn memory(keyless: bool) -> OverWindowAggregator {
    OverWindowAggregator::new(
        vec![0],
        vec![0],
        2,
        vec![1],
        if keyless { vec![] } else { vec![0] },
        2,
        32,
        false,
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
fn operator(keyless: bool, persistent: bool, path: &std::path::Path) -> OverWindowAggregator {
    if persistent {
        let store = crate::state::RocksOverAggStore::create(
            config(path),
            &[],
            &rocks_over_frame_value_types(&[0], &[0], 2),
            &[],
            input(None, 0, 0).schema(),
            0..=7,
        )
        .unwrap();
        memory(keyless).with_store(
            store,
            if keyless {
                vec![]
            } else {
                vec![DataType::Int64]
            },
        )
    } else {
        memory(keyless).with_memory_budget(64 << 20).unwrap()
    }
}

fn timers(op: &OverWindowAggregator) -> Vec<(Vec<u8>, Option<i64>, Vec<i64>)> {
    let mut out = op
        .range_timers
        .iter()
        .map(|(key, value)| (key.0.to_vec(), value.last, value.armed.clone()))
        .collect::<Vec<_>>();
    out.sort();
    out
}

fn prefix(op: &mut OverWindowAggregator, fire_stale: bool) {
    op.push(input(None, 1000, 1), 0).unwrap();
    assert_eq!(op.flush(1000, 0).unwrap().num_rows(), 1);
    op.push(input(None, 1064, 7), 0).unwrap();
    assert_eq!(timers(op)[0].2, vec![1049, 1064, 1113]);
    if fire_stale {
        assert_eq!(op.flush(1050, 0).unwrap().num_rows(), 0);
        assert_eq!(timers(op)[0].1, Some(1049));
        assert_eq!(timers(op)[0].2, vec![1064, 1113]);
    }
}

fn suffix(op: &mut OverWindowAggregator, fire_stale: bool) {
    if !fire_stale {
        op.flush(1050, 0).unwrap();
    }
    let dropped = op.late_drops;
    op.push(input(None, 1020, 99), 0).unwrap();
    assert_eq!(op.late_drops, dropped + 1);
    let output = op.flush(1064, 0).unwrap();
    assert_eq!(output.num_rows(), 1);
    assert_eq!(
        ScalarValue::try_from_array(output.column(3), 0).unwrap(),
        ScalarValue::Int64(Some(7))
    );
    assert_eq!(timers(op)[0].1, Some(1064));
    op.flush(1113, 0).unwrap();
    assert!(op.range_timers.is_empty());
    assert!(op.cleanup_state.is_empty());
    assert_eq!(op.retention_bytes, 0);
    op.push(input(None, 1000, 3), 0).unwrap();
    assert_eq!(
        op.late_drops,
        dropped + 1,
        "cleanup resets the per-key cutoff"
    );
    assert_eq!(op.flush(1114, 0).unwrap().num_rows(), 1);
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn superseded_timers_and_late_cutoffs_transfer_between_both_native_stores() {
    for keyless in [false, true] {
        for from_store in [false, true] {
            for to_store in [false, true] {
                for fire_stale in [false, true] {
                    let dir = tempfile::tempdir().unwrap();
                    let mut source = operator(keyless, from_store, &dir.path().join("source"));
                    prefix(&mut source, fire_stale);
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
                        let mut op = operator(keyless, true, &dir.path().join("target"));
                        op.import_partitions(&snapshots).unwrap();
                        op.adopt_store_retention(0).unwrap();
                        op
                    } else {
                        OverWindowAggregator::restore_partitions(
                            vec![0],
                            vec![0],
                            2,
                            vec![1],
                            if keyless { vec![] } else { vec![0] },
                            2,
                            32,
                            false,
                            &snapshots,
                            0,
                            0,
                        )
                        .with_memory_budget(64 << 20)
                        .unwrap()
                    };
                    assert_eq!(timers(&source), timers(&target));
                    suffix(&mut source, fire_stale);
                    suffix(&mut target, fire_stale);
                }
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn file_checkpoints_clip_timer_and_cutoff_rows_with_their_key_groups() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = operator(false, true, &dir.path().join("source"));
    for key in 0..32 {
        source.push(input(Some(key), 1000, 1), 0).unwrap();
    }
    source.flush(1000, 0).unwrap();
    for key in 0..32 {
        source.push(input(Some(key), 1064, 7), 0).unwrap();
    }
    source.flush(1050, 0).unwrap();
    let path = dir.path().join("checkpoint");
    let manifest = source.checkpoint_store(path.to_str().unwrap()).unwrap();
    let expected = timers(&source);
    for (index, (aligned, groups)) in [(true, 0..=7), (false, 0..=3), (false, 4..=7)]
        .into_iter()
        .enumerate()
    {
        let store = crate::state::RocksOverAggStore::open_merged(
            config(&dir.path().join(format!("target{index}"))),
            &[],
            &rocks_over_frame_value_types(&[0], &[0], 2),
            &[],
            input(None, 0, 0).schema(),
            groups.clone(),
            &[(path.to_str().unwrap().into(), manifest.snapshot_id)],
            aligned,
        )
        .unwrap();
        let mut target = memory(false).with_store(store, vec![DataType::Int64]);
        target.adopt_store_retention(0).unwrap();
        let subset = expected
            .iter()
            .filter(|(key, _, _)| groups.contains(&source.store_fold_key_group(key)))
            .cloned()
            .collect::<Vec<_>>();
        assert!(!subset.is_empty());
        assert_eq!(timers(&target), subset);
        for key in 0..32 {
            let batch = input(Some(key), 1020, 99);
            let group = flink_key_group(binary_row_hash(&batch, &[0], 0, &[-1]), 8) as i32;
            if groups.contains(&group) {
                let previous = target.late_drops;
                target.push(batch, 0).unwrap();
                assert_eq!(target.late_drops, previous + 1);
            }
        }
        assert_eq!(target.flush(1064, 0).unwrap().num_rows(), subset.len());
        target.flush(1113, 0).unwrap();
        assert!(target.range_timers.is_empty());
        let path = dir.path().join(format!("empty{index}"));
        target.checkpoint_store(path.to_str().unwrap()).unwrap();
        assert!(target
            .store
            .as_ref()
            .unwrap()
            .scan_range_timers()
            .unwrap()
            .is_empty());
    }
}

#[test]
fn coincident_data_and_cleanup_timers_fire_once_and_keep_key_local_admission() {
    let mut op = memory(false).with_memory_budget(1 << 20).unwrap();
    op.push(input(Some(1), 1000, 1), 0).unwrap();
    op.flush(1000, 0).unwrap();
    op.push(input(Some(1), 1049, 7), 0).unwrap();
    assert_eq!(timers(&op)[0].2, vec![1049, 1098]);
    assert_eq!(op.flush(1049, 0).unwrap().num_rows(), 1);
    assert_eq!(timers(&op)[0].2, vec![1098]);
    op.push(input(Some(1), 1020, 99), 0).unwrap();
    op.push(input(Some(2), 1020, 3), 0).unwrap();
    assert_eq!(op.late_drops, 1);
    assert_eq!(op.flush(1050, 0).unwrap().num_rows(), 1);
    op.flush(1098, 0).unwrap();
    assert_eq!(op.retention_bytes, 0);
    assert!(op.range_timers.is_empty());
}

#[test]
fn actual_legacy_frame_seeds_its_previous_cutoff_without_inventing_old_timers() {
    let mut op = OverWindowAggregator::restore_partitions(
        vec![0],
        vec![0],
        1,
        vec![2],
        vec![0],
        2,
        10000,
        false,
        &[legacy_frame_fixture("primitive-0-2-false-true")],
        0,
        0,
    );
    assert_eq!(timers(&op)[0].1, Some(100));
    assert_eq!(timers(&op)[0].2, vec![15101]);
    let batch = RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("time", Arc::new(Int64Array::from(vec![100])) as ArrayRef),
        ("value", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
    ])
    .unwrap();
    op.push(batch, 0).unwrap();
    assert_eq!(op.late_drops, 1);
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn timer_codec_rejects_truncation_versions_and_unsorted_or_duplicate_times() {
    let timers = RangeTimers {
        last: Some(1049),
        armed: vec![1064, 1113],
    };
    let bytes = timers.encode();
    let restored = RangeTimers::decode(&bytes).unwrap();
    assert_eq!(restored.last, timers.last);
    assert_eq!(restored.armed, timers.armed);
    for length in [0, 1, 9, 11, 17, 25] {
        assert!(RangeTimers::decode(&bytes[..length]).is_err());
    }
    for (index, value) in [(0, 2), (1, 2)] {
        let mut invalid = bytes.clone();
        invalid[index] = value;
        assert!(RangeTimers::decode(&invalid).is_err());
    }
    for armed in [vec![1064, 1064], vec![1113, 1064]] {
        assert!(RangeTimers::decode(&RangeTimers { last: None, armed }.encode()).is_err());
    }
}
