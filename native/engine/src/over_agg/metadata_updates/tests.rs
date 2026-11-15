use super::*;

fn input(keys: Vec<i64>, time: i64) -> RecordBatch {
    let count = keys.len();
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(keys)) as ArrayRef),
        (
            "value",
            Arc::new(Int64Array::from(vec![1; count])) as ArrayRef,
        ),
        (
            "time",
            Arc::new(Int64Array::from(vec![time; count])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn operator(
    path: &std::path::Path,
    frame: i64,
    processing: bool,
    polling: bool,
) -> OverWindowAggregator {
    let mut op = OverWindowAggregator::new(
        vec![0; 3],
        vec![0, 3, 4],
        2,
        vec![1; 3],
        vec![0],
        frame,
        32,
        processing,
    )
    .with_state_retention(1000);
    let store = crate::state::RocksOverAggStore::create(
        crate::state::RocksStoreConfig {
            table_dir: path.to_str().unwrap().into(),
            max_parallelism: 128,
            options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        },
        &op.inner.store_state_types(),
        &rocks_over_frame_value_types(&[0; 3], &[0, 3, 4], frame),
        &[],
        input(vec![], 0).schema(),
        0..=127,
    )
    .unwrap();
    op = op
        .with_store(store, vec![DataType::Int64])
        .with_read_through_budget(64 << 20)
        .unwrap();
    if !processing && matches!(frame, 0 | 3) {
        op.bind_event_cleanup_timers().unwrap();
    }
    if polling {
        op.bind_event_polling().unwrap();
    }
    op
}

#[test]
fn populated_checkpoint_capture_neither_scans_nor_rewrites_metadata_tables() {
    for (frame, processing, polling) in [
        (0, false, false),
        (3, false, false),
        (0, false, true),
        (3, false, true),
        (1, false, false),
        (2, false, false),
        (1, true, false),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state");
        let mut source = operator(&path, frame, processing, polling);
        if processing {
            source
                .push_proctime(input((0..2051).collect(), 10), 100)
                .unwrap();
        } else {
            source.push(input((0..2051).collect(), 10), 100).unwrap();
            source.flush(5, 200).unwrap();
        }
        assert!(source.metadata_updates.keys.is_empty());
        let expected_cleanup = source
            .store
            .as_ref()
            .unwrap()
            .scan_cleanup_stamps()
            .unwrap();
        let expected_timers = source.store.as_ref().unwrap().scan_range_timers().unwrap();
        let expected_counters = source.store.as_ref().unwrap().scan_proc_rows().unwrap();
        let before = source.store.as_ref().unwrap().metadata_io_counts();
        source.checkpoint_store("").unwrap();
        let snapshot = crate::state::rocks_store::FrozenDatabases::capture(&path).unwrap();
        let after = source.store.as_ref().unwrap().metadata_io_counts();
        assert_eq!(
            after.0, before.0,
            "frame={frame}, processing={processing}, polling={polling}: barrier scanned state"
        );
        assert_eq!(
            after.1 - before.1,
            1,
            "only the reserved watermark/sequence batch belongs at capture"
        );
        if processing {
            source
                .push_proctime(input((0..2051).collect(), 20), 2000)
                .unwrap();
        } else {
            source.push(input((0..2051).collect(), 20), 2000).unwrap();
            source.flush(30, 2000).unwrap();
        }
        snapshot.with(|| {
            let reader = operator(&path, frame, processing, polling);
            assert!(reader.cleanup_state.is_empty());
            assert!(reader.range_timers.is_empty());
            assert!(reader.proc_rows.is_empty());
            let store = reader.store.as_ref().unwrap();
            assert_eq!(store.scan_cleanup_stamps().unwrap(), expected_cleanup);
            assert_eq!(store.scan_range_timers().unwrap(), expected_timers);
            assert_eq!(store.scan_proc_rows().unwrap(), expected_counters);
        });
    }
}

#[test]
fn failed_metadata_flush_retains_updates_for_checkpoint_retry() {
    let directory = tempfile::tempdir().unwrap();
    let mut op = operator(directory.path(), 3, false, false);
    op.push(input(vec![1], 10), 100).unwrap();
    let key = op.cleanup_state.keys().next().unwrap().clone();
    let retained_bytes = op.memory.state_bytes;
    op.register_cleanup(&key.0, 5000);
    assert!(!op.metadata_updates.keys.is_empty());
    op.store.as_ref().unwrap().fail_next_metadata_write();
    assert!(op.checkpoint_store("").is_err());
    assert!(!op.metadata_updates.keys.is_empty());
    assert!(
        op.memory.state_bytes > retained_bytes,
        "failed writes retain a charged journal"
    );
    op.checkpoint_store("").unwrap();
    assert!(op.metadata_updates.keys.is_empty());
    assert_eq!(op.memory.state_bytes, retained_bytes);
    assert_eq!(
        op.store
            .as_ref()
            .unwrap()
            .cleanup_stamp(op.store_fold_key_group(&key.0), &key.0)
            .unwrap(),
        Some((6500, true))
    );
}

#[test]
fn denied_metadata_serialization_preserves_the_journal_and_releases_work_for_retry() {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
    for (frame, processing, polling) in [(2, false, false), (0, false, true), (1, true, false)] {
        let directory = tempfile::tempdir().unwrap();
        let mut op = operator(directory.path(), frame, processing, polling);
        if processing {
            op.push_proctime(input(vec![7], 10), 100).unwrap();
        } else {
            op.push(input(vec![7], 10), 100).unwrap();
        }
        let key = op.cleanup_state.keys().next().unwrap().clone();
        op.cleanup_state.insert(key.clone(), 2045);
        if !processing {
            let timers = RangeTimers {
                last: (frame == 2).then_some(-10),
                armed: (-5..2046).collect(),
            };
            if let Some(previous) = op.range_timers.remove(&key) {
                op.retention_bytes -= byte_key_bytes(&key.0) + previous.bytes();
            }
            op.retention_bytes += byte_key_bytes(&key.0) + timers.bytes();
            op.range_timers.insert(key.clone(), timers);
        }
        op.mark_store_metadata(&key.0);
        let retained = op.retained_state_bytes() + op.metadata_updates.bytes;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(retained + (1 << 20)));
        op.memory
            .attach_pool("metadata precharge", &pool, retained)
            .unwrap();
        let mut blocker = MemoryConsumer::new("metadata blocker").register(&pool);
        blocker.try_resize((1 << 20) - 1).unwrap();
        let before = op.store.as_ref().unwrap().metadata_io_counts();
        assert!(op.flush_metadata_updates().is_err());
        assert_eq!(before, op.store.as_ref().unwrap().metadata_io_counts());
        assert_eq!(pool.reserved(), retained + (1 << 20) - 1);
        assert_eq!(op.metadata_updates.keys.len(), 1);
        drop(blocker);
        assert_eq!(pool.reserved(), retained);
        op.flush_metadata_updates().unwrap();
        assert!(op.metadata_updates.keys.is_empty());
        assert_eq!(pool.reserved(), op.retained_state_bytes());
        let store = op.store.as_ref().unwrap();
        assert_eq!(
            store
                .cleanup_stamp(op.store_fold_key_group(&key.0), &key.0)
                .unwrap(),
            Some((2045, true))
        );
        if !processing {
            let timers = store.scan_range_timers().unwrap();
            assert_eq!(timers.len(), 1);
            assert_eq!(
                RangeTimers::decode(&timers[0].1).unwrap().armed,
                (-5..2046).collect::<Vec<_>>()
            );
        }
        drop(op);
        assert_eq!(pool.reserved(), 0);
    }
}

#[test]
fn metadata_reconciliation_streams_many_owners_with_a_bounded_work_allowance() {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    let directory = tempfile::tempdir().unwrap();
    let mut op = operator(directory.path(), 2, false, false);
    op.push(input((0..2051).collect(), 10), 100).unwrap();
    let owners = op.cleanup_state.keys().cloned().collect::<Vec<_>>();
    for key in &owners {
        op.cleanup_state.insert(key.clone(), 2045);
        op.mark_store_metadata(&key.0);
    }
    let retained = op.retained_state_bytes() + op.metadata_updates.bytes;
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(retained + (256 << 10)));
    op.memory
        .attach_pool("bounded metadata", &pool, retained)
        .unwrap();
    let before = op.store.as_ref().unwrap().metadata_io_counts();
    op.flush_metadata_updates().unwrap();
    assert!(op.store.as_ref().unwrap().metadata_io_counts().1 > before.1 + 1);
    assert_eq!(
        op.store
            .as_ref()
            .unwrap()
            .scan_cleanup_stamps()
            .unwrap()
            .len(),
        2051
    );
    assert_eq!(pool.reserved(), op.retained_state_bytes());
    drop(op);
    assert_eq!(pool.reserved(), 0);
}
