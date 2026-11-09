use super::*;

fn wide_operator(shape: usize, persistent: bool, path: &std::path::Path) -> OverWindowAggregator {
    let mut op = operator(shape, false, 100_000, persistent, path)
        .with_memory_budget(32 << 20)
        .unwrap();
    let OverInner::Bounded(inner) = &mut op.inner else {
        unreachable!()
    };
    inner.offset = 10_000;
    op.bind_canonical_frame_inputs().unwrap();
    op
}

fn wide_source(shape: usize, rows: usize, path: &std::path::Path) -> OverWindowAggregator {
    let mut source = wide_operator(shape, false, path);
    for index in 0..rows {
        let value = [1e16, 1.0, -1e16][index % 3];
        execute(
            &mut source,
            Some(1),
            1000 + index as i64,
            Some(value),
            100 + index as i64,
        );
    }
    source
}

#[test]
fn chunks_validate_without_installing_rows_then_continue_all_shapes_on_both_backends() {
    let directory = tempfile::tempdir().unwrap();
    for shape in 0..3 {
        for persistent in [false, true] {
            let path = directory.path().join(format!("{shape}-{persistent}"));
            let mut source = wide_source(shape, 2051, &path);
            let saved = frames(&source).pop().unwrap();
            assert_eq!(saved.num_rows(), 2051);
            let mut target = wide_operator(shape, persistent, &path);
            let base_bytes = target.retention_bytes;
            for position in (0..2051).step_by(1024) {
                let count = (2051 - position).min(1024);
                target
                    .import_canonical_frame_chunk(
                        &saved.slice(position, count),
                        position + count == 2051,
                        true,
                    )
                    .unwrap();
                let OverInner::Bounded(inner) = &target.inner else {
                    unreachable!()
                };
                assert!(inner.keys.is_empty());
                assert!(target.canonical_frame_import.is_none());
                assert_eq!(base_bytes, target.retention_bytes);
                if let Some(store) = &target.store {
                    assert!(store.scan_frames().unwrap().is_empty());
                }
            }
            for position in (0..2051).step_by(1024) {
                let count = (2051 - position).min(1024);
                let last = position + count == 2051;
                target
                    .import_canonical_frame_chunk(&saved.slice(position, count), last, false)
                    .unwrap();
                if !last {
                    assert!(target.ensure_canonical_ready().is_err());
                    assert!(target.push(input(Some(1), 4000, Some(1.0)), 4000).is_err());
                    if persistent {
                        assert!(target.checkpoint_store("").is_err());
                    }
                }
                if persistent {
                    let OverInner::Bounded(inner) = &target.inner else {
                        unreachable!()
                    };
                    assert!(
                        inner.keys.is_empty(),
                        "RocksDB import must not hydrate a partition"
                    );
                }
            }
            assert!(target.canonical_frame_import.is_none());
            assert_eq!(frames(&source), frames(&target));
            assert_eq!(
                execute(&mut source, Some(1), 4000, Some(2.0), 4000),
                execute(&mut target, Some(1), 4000, Some(2.0), 4000)
            );
            assert_eq!(frames(&source), frames(&target));
        }
    }
}

#[test]
fn invalid_continuation_poisoning_preserves_the_source_for_a_fresh_target() {
    let directory = tempfile::tempdir().unwrap();
    for persistent in [false, true] {
        let path = directory.path().join(format!("failed-{persistent}"));
        let source = wide_source(1, 4, &path);
        let saved = frames(&source).pop().unwrap();
        let mut target = wide_operator(1, persistent, &path);
        target
            .import_canonical_frame_chunk(&saved.slice(0, 2), false, false)
            .unwrap();
        assert!(target
            .import_canonical_frame_chunk(&saved.slice(1, 3), true, false)
            .is_err());
        assert!(target
            .import_canonical_frame_chunk(&saved.slice(2, 2), true, false)
            .is_err());
        assert!(target.ensure_canonical_ready().is_err());
        drop(target);
        let mut fresh = wide_operator(
            1,
            persistent,
            &directory.path().join(format!("fresh-{persistent}")),
        );
        fresh
            .import_canonical_frame_chunk(&saved.slice(0, 2), false, false)
            .unwrap();
        fresh
            .import_canonical_frame_chunk(&saved.slice(2, 2), true, false)
            .unwrap();
        assert_eq!(frames(&source), frames(&fresh));
        assert_eq!(saved, frames(&source).pop().unwrap());
    }
}

#[test]
fn final_range_metadata_must_cover_earlier_chunks_on_heap_and_disk() {
    let directory = tempfile::tempdir().unwrap();
    for persistent in [false, true] {
        let path = directory.path().join(format!("range-{persistent}"));
        let source = wide_source(2, 4, &path);
        let saved = frames(&source).pop().unwrap();
        let mut target = wide_operator(2, persistent, &path);
        target
            .import_canonical_frame_chunk(&saved.slice(0, 2), false, false)
            .unwrap();
        let final_chunk = saved.slice(2, 2);
        let mut columns = final_chunk.columns().to_vec();
        let order_column = final_chunk.schema().index_of(ORDER).unwrap();
        columns[order_column] = tail_list(Some(&[1002, 1003]), 2);
        let malformed = RecordBatch::try_new(final_chunk.schema(), columns).unwrap();
        assert!(target
            .import_canonical_frame_chunk(&malformed, true, false)
            .is_err());
        assert!(target.ensure_canonical_ready().is_err());
    }
}

#[test]
fn validation_denial_and_incomplete_import_close_release_their_reservations() {
    let directory = tempfile::tempdir().unwrap();
    let source = wide_source(1, 4, directory.path());
    let saved = frames(&source).pop().unwrap();
    for persistent in [false, true] {
        let mut target = wide_operator(
            1,
            persistent,
            &directory.path().join(format!("budget-{persistent}")),
        );
        let denied: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        target
            .memory
            .attach_pool(
                "denied frame import",
                &denied,
                target.retained_state_bytes(),
            )
            .unwrap();
        assert!(target
            .import_canonical_frame_chunk(&saved, true, true)
            .is_err());
        assert_eq!(denied.reserved(), 0);
        assert!(target.canonical_frame_import.is_none());
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(32 << 20));
        target
            .memory
            .attach_pool("frame import", &pool, target.retained_state_bytes())
            .unwrap();
        target
            .import_canonical_frame_chunk(&saved, true, true)
            .unwrap();
        assert_eq!(
            pool.reserved(),
            0,
            "validation owns no retained target state"
        );
        target
            .import_canonical_frame_chunk(&saved.slice(0, 2), false, false)
            .unwrap();
        let retained = pool.reserved();
        assert!(retained > 0);
        assert!(target
            .import_canonical_frame_chunk(&saved.slice(1, 3), true, false)
            .is_err());
        assert_eq!(
            pool.reserved(),
            retained,
            "failed imports keep their retained state charged"
        );
        drop(target);
        assert_eq!(pool.reserved(), 0);
    }
}

#[test]
fn chunk_budget_transfers_to_retained_state_without_double_charging_or_losing_failed_writes() {
    let directory = tempfile::tempdir().unwrap();
    let source = wide_source(1, 4, directory.path());
    let saved = frames(&source).pop().unwrap();
    let chunk = saved.slice(0, 2);
    let measuring = wide_operator(1, false, directory.path());
    let required = measuring
        .decode_frame_chunk(&chunk, false)
        .unwrap()
        .work
        .unwrap()
        .size();
    for persistent in [false, true] {
        let mut target = wide_operator(
            1,
            persistent,
            &directory.path().join(format!("exact-{persistent}")),
        );
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(required));
        target
            .memory
            .attach_pool("exact chunk budget", &pool, target.retained_state_bytes())
            .unwrap();
        target
            .import_canonical_frame_chunk(&chunk, false, false)
            .unwrap();
        assert!(pool.reserved() > 0);
        assert!(pool.reserved() <= required);
        drop(target);
        assert_eq!(pool.reserved(), 0);
    }
    let mut failed = wide_operator(1, true, &directory.path().join("write-failed"));
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(required));
    failed
        .memory
        .attach_pool("failed frame write", &pool, failed.retained_state_bytes())
        .unwrap();
    failed.store.as_ref().unwrap().fail_next_metadata_write();
    assert!(failed
        .import_canonical_frame_chunk(&chunk, false, false)
        .is_err());
    assert!(failed.canonical_frame_import.as_ref().unwrap().failed);
    assert_eq!(pool.reserved(), failed.retained_state_bytes());
    assert!(pool.reserved() > 0);
    drop(failed);
    assert_eq!(pool.reserved(), 0);
}
