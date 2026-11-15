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

fn payload(saved: &RecordBatch) -> RecordBatch {
    let mut columns = saved.columns().to_vec();
    for column in saved.num_columns() - 4..saved.num_columns() {
        columns[column] =
            arrow::array::new_null_array(saved.column(column).data_type(), saved.num_rows());
    }
    RecordBatch::try_new(saved.schema(), columns).unwrap()
}

fn range_records(saved: &RecordBatch, persistent: bool) -> Vec<(i8, i64)> {
    let base = saved.num_columns() - 4;
    let mut order = read_tail_list(saved, base + 3).unwrap().unwrap();
    if persistent {
        order.sort();
    }
    let mut records = vec![(4, order.len() as i64)];
    let row = saved.num_rows() - 1;
    for (kind, column) in [(0, base), (1, base + 1)] {
        if !saved.column(column).is_null(row) {
            records.push((kind, column_at_i64(saved, column).value(row)));
        }
    }
    records.extend(
        read_tail_list(saved, base + 2)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|time| (2, time)),
    );
    records.extend(order.into_iter().map(|time| (5, time)));
    records
}

fn range_batch(records: &[(i8, i64)]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "key0",
            Arc::new(Int64Array::from(vec![1; records.len()])) as ArrayRef,
        ),
        (
            "__over_callback_kind",
            Arc::new(arrow::array::Int8Array::from_iter_values(
                records.iter().map(|row| row.0),
            )) as ArrayRef,
        ),
        (
            "__over_callback_time",
            Arc::new(Int64Array::from_iter_values(
                records.iter().map(|row| row.1),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn import_payload(target: &mut OverWindowAggregator, batch: &RecordBatch, validating: bool) {
    for position in (0..batch.num_rows()).step_by(1024) {
        let count = (batch.num_rows() - position).min(1024);
        target
            .import_canonical_frame_payload_chunk(
                &batch.slice(position, count),
                position + count == batch.num_rows(),
                validating,
            )
            .unwrap();
    }
}

#[test]
fn range_rows_and_flat_metadata_complete_together_without_per_owner_transfer_lists() {
    let directory = tempfile::tempdir().unwrap();
    let mut source = wide_source(2, 2051, directory.path());
    let saved = frames(&source).pop().unwrap();
    let rows = payload(&saved);
    for persistent in [false, true] {
        let path = directory.path().join(format!("flat-{persistent}"));
        let mut target = wide_operator(2, persistent, &path);
        import_payload(&mut target, &rows, true);
        let records = range_records(&saved, persistent);
        let chunks = records.chunks(1024).collect::<Vec<_>>();
        for (index, records) in chunks.iter().enumerate() {
            target
                .transfer_canonical_callbacks(
                    &range_batch(records),
                    index + 1 == chunks.len(),
                    true,
                )
                .unwrap();
        }
        assert!(target.ensure_canonical_ready().is_ok());
        let OverInner::Bounded(inner) = &target.inner else {
            unreachable!()
        };
        assert!(inner.keys.is_empty());
        assert!(target.range_timers.is_empty());
        import_payload(&mut target, &rows, false);
        assert!(target.ensure_canonical_ready().is_err());
        assert!(target.visit_canonical_frames(|_| Ok(())).is_err());
        if persistent {
            assert!(target.checkpoint_store("").is_err());
        }
        for (index, records) in chunks.iter().enumerate() {
            target
                .transfer_canonical_callbacks(
                    &range_batch(records),
                    index + 1 == chunks.len(),
                    false,
                )
                .unwrap();
        }
        assert!(target.ensure_canonical_ready().is_ok());
        assert!(target.canonical_frame_import.is_none());
        assert_eq!(frames(&source), frames(&target));
        let mut returned = wide_operator(2, false, &directory.path().join("returned"));
        returned
            .import_canonical_frame(&frames(&target).pop().unwrap())
            .unwrap();
        assert_eq!(
            execute(&mut source, Some(1), 4000, Some(2.0), 4000),
            execute(&mut target, Some(1), 4000, Some(2.0), 4000)
        );
        assert_eq!(frames(&source), frames(&target));
        // Keep the shared source at the same initial boundary for the other target backend.
        source = wide_source(2, 2051, directory.path());
    }
}

#[test]
fn range_peer_rows_cross_frame_chunks_but_require_one_timestamp_membership() {
    let directory = tempfile::tempdir().unwrap();
    let mut source = wide_operator(2, false, directory.path());
    let row = input(Some(1), 1000, Some(1.0));
    source
        .push(
            concat_batches(&row.schema(), std::iter::repeat_n(&row, 2051)).unwrap(),
            100,
        )
        .unwrap();
    source.flush(1000, 100).unwrap();
    let saved = frames(&source).pop().unwrap();
    assert_eq!(saved.num_rows(), 2051);
    for persistent in [false, true] {
        let path = directory.path().join(format!("peers-{persistent}"));
        let mut target = wide_operator(2, persistent, &path);
        import_payload(&mut target, &payload(&saved), false);
        assert_eq!(
            target
                .canonical_frame_import
                .as_ref()
                .unwrap()
                .distinct_times,
            1
        );
        let records = range_records(&saved, persistent);
        assert_eq!(records.iter().filter(|row| row.0 == 5).count(), 1);
        target
            .transfer_canonical_callbacks(&range_batch(&records), true, false)
            .unwrap();
        assert!(target.ensure_canonical_ready().is_ok());
        assert_eq!(frames(&source), frames(&target));
    }
}

#[test]
fn incomplete_or_corrupt_range_metadata_cannot_publish_a_restored_frame() {
    let directory = tempfile::tempdir().unwrap();
    let source = wide_source(2, 4, directory.path());
    let saved = frames(&source).pop().unwrap();
    for persistent in [false, true] {
        for reason in [
            "missing-owner",
            "missing-map",
            "deadline",
            "count",
            "duplicate",
        ] {
            let path = directory.path().join(format!("{persistent}-{reason}"));
            let mut target = wide_operator(2, persistent, &path);
            import_payload(&mut target, &payload(&saved), false);
            let mut records = range_records(&saved, persistent);
            match reason {
                "missing-owner" => records.clear(),
                "missing-map" => {
                    records.iter_mut().find(|row| row.0 == 5).unwrap().1 = i64::MIN;
                    if persistent {
                        let start = records.iter().position(|row| row.0 == 5).unwrap();
                        records[start..].sort_by_key(|row| row.1);
                    }
                }
                "deadline" => records.iter_mut().find(|row| row.0 == 0).unwrap().1 = i64::MIN,
                "count" => records[0].1 += 1,
                "duplicate" => {
                    let last = *records.last().unwrap();
                    records.push(last);
                }
                _ => unreachable!(),
            }
            assert!(
                target
                    .transfer_canonical_callbacks(&range_batch(&records), true, false)
                    .is_err(),
                "{persistent}-{reason}"
            );
            assert!(target.ensure_canonical_ready().is_err());
            assert!(target
                .transfer_canonical_callbacks(
                    &range_batch(&range_records(&saved, persistent)),
                    true,
                    false
                )
                .is_err());
        }
        let path = directory.path().join(format!("fresh-{persistent}"));
        let mut retry = wide_operator(2, persistent, &path);
        import_payload(&mut retry, &payload(&saved), false);
        retry
            .transfer_canonical_callbacks(
                &range_batch(&range_records(&saved, persistent)),
                true,
                false,
            )
            .unwrap();
        assert_eq!(frames(&source), frames(&retry));
    }
}

#[test]
fn range_metadata_memory_denial_cancellation_and_failed_writes_keep_frames_guarded() {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
    let directory = tempfile::tempdir().unwrap();
    let source = wide_source(2, 2051, directory.path());
    let saved = frames(&source).pop().unwrap();
    for persistent in [false, true] {
        for reason in ["memory", "cancel", "write"] {
            if reason == "write" && !persistent {
                continue;
            }
            let path = directory
                .path()
                .join(format!("failure-{persistent}-{reason}"));
            let mut target = wide_operator(2, persistent, &path);
            import_payload(&mut target, &payload(&saved), false);
            let initial = target.retained_state_bytes();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(initial + (4 << 20)));
            target
                .memory
                .attach_pool("flat RANGE failure", &pool, initial)
                .unwrap();
            let records = range_records(&saved, persistent);
            let chunks = records.chunks(1024).collect::<Vec<_>>();
            target
                .transfer_canonical_callbacks(&range_batch(chunks[0]), false, false)
                .unwrap();
            let held = pool.reserved();
            match reason {
                "memory" => {
                    let mut blocker = MemoryConsumer::new("RANGE blocker").register(&pool);
                    blocker.try_resize(initial + (4 << 20) - held - 1).unwrap();
                    assert!(target
                        .transfer_canonical_callbacks(&range_batch(chunks[1]), false, false)
                        .is_err());
                    drop(blocker);
                    assert_eq!(pool.reserved(), held);
                }
                "cancel" => {
                    let cancelled =
                        crate::state::canonical_context::CanonicalContext::new(Default::default());
                    cancelled.cancel();
                    cancelled.with(|| {
                        assert!(target
                            .transfer_canonical_callbacks(&range_batch(chunks[1]), false, false)
                            .is_err())
                    });
                    assert_eq!(pool.reserved(), held);
                }
                "write" => {
                    for part in &chunks[1..chunks.len() - 1] {
                        target
                            .transfer_canonical_callbacks(&range_batch(part), false, false)
                            .unwrap();
                    }
                    target.store.as_ref().unwrap().fail_next_metadata_write();
                    assert!(target
                        .transfer_canonical_callbacks(
                            &range_batch(chunks[chunks.len() - 1]),
                            true,
                            false
                        )
                        .is_err());
                }
                _ => unreachable!(),
            }
            assert!(target.ensure_canonical_ready().is_err());
            assert!(target.visit_canonical_frames(|_| Ok(())).is_err());
            assert!(target.canonical_frame_import.is_some());
            drop(target);
            assert_eq!(pool.reserved(), 0);
        }
    }
    assert_eq!(frames(&source), vec![saved]);
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
        .decode_frame_chunk(&chunk, false, false)
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
