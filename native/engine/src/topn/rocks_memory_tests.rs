use super::*;
use crate::state::RocksStoreConfig;

#[derive(Clone, Copy, Debug)]
enum Variant {
    Append,
    Retract,
    UpdateFast,
}

fn input(start: i64, rows: usize, value: i64, width: usize, kind: i8) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from_iter_values(start..start + rows as i64)) as ArrayRef,
        ),
        (
            "v",
            Arc::new(Int64Array::from(vec![value; rows])) as ArrayRef,
        ),
        (
            "id",
            Arc::new(Int64Array::from_iter_values(start..start + rows as i64)) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(StringArray::from(vec!["x".repeat(width); rows])) as ArrayRef,
        ),
        (
            ROW_KIND_COLUMN,
            Arc::new(Int8Array::from(vec![kind; rows])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn config(directory: &std::path::Path, ttl_ms: i64) -> RocksStoreConfig {
    RocksStoreConfig {
        table_dir: directory.to_string_lossy().into_owned(),
        max_parallelism: 128,
        options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
        ttl_ms,
        shared_resources: 0,
    }
}

fn rankers(
    directory: &std::path::Path,
    variant: Variant,
    limit: i64,
    mini_batch: bool,
    ttl_ms: i64,
    pool: &Arc<dyn MemoryPool>,
) -> (RocksTopNHandle, TopNHandle) {
    rankers_from(directory, variant, limit, mini_batch, ttl_ms, pool, None)
}

fn open_store<C: crate::state::RocksStateCodec>(
    directory: &std::path::Path,
    ttl_ms: i64,
    codec: C,
    source: Option<&(String, i64)>,
) -> crate::state::RocksStore<C> {
    match source {
        Some(source) => crate::state::RocksStore::open_merged(
            config(directory, ttl_ms),
            codec,
            std::slice::from_ref(source),
            0..=127,
            true,
            0,
        ),
        None => crate::state::RocksStore::create(config(directory, ttl_ms), codec),
    }
    .unwrap()
}

fn rankers_from(
    directory: &std::path::Path,
    variant: Variant,
    limit: i64,
    mini_batch: bool,
    ttl_ms: i64,
    pool: &Arc<dyn MemoryPool>,
    source: Option<&(String, i64)>,
) -> (RocksTopNHandle, TopNHandle) {
    let sort = vec![SortColumn {
        index: 1,
        ascending: false,
        nulls_first: false,
    }];
    let schema = data_schema(&input(0, 0, 0, 0, 0));
    let converters = TopNConverters::from_declared(&schema, &[0], &sort);
    let make_append =
        || TopNRanker::new(vec![0], sort.clone(), limit, false, mini_batch).with_state_ttl(ttl_ms);
    let make_retract = || {
        RetractableTopNRanker::new(vec![0], sort.clone(), 0, limit, false)
            .with_net_diff(mini_batch)
            .with_state_ttl(ttl_ms)
    };
    let make_update = || {
        UpdatableTopNRanker::new(
            vec![0],
            vec![-1],
            vec![0, 2],
            vec![-1; 2],
            sort.clone(),
            limit,
            false,
            true,
        )
        .with_state_ttl(ttl_ms)
    };
    match variant {
        Variant::Append => {
            let store = open_store(directory, ttl_ms, TopNStateCodec::new(&converters), source);
            let mut rocks = make_append()
                .with_converters(converters)
                .with_payload_schema(schema)
                .with_backend(store);
            rocks
                .memory
                .attach_pool("rocks-topn-test", pool, 0)
                .unwrap();
            rocks.groups.attach_memory(rocks.memory.state_memory());
            (
                RocksTopNHandle::Append(rocks),
                TopNHandle::Append(make_append()),
            )
        }
        Variant::Retract => {
            let store = open_store(directory, ttl_ms, TopNStateCodec::new(&converters), source);
            let mut rocks = make_retract()
                .with_converters(converters)
                .with_payload_schema(schema)
                .with_backend(store);
            rocks
                .memory
                .attach_pool("rocks-topn-test", pool, 0)
                .unwrap();
            rocks.groups.attach_memory(rocks.memory.state_memory());
            (
                RocksTopNHandle::Retract(rocks),
                TopNHandle::Retract(make_retract()),
            )
        }
        Variant::UpdateFast => {
            let store = open_store(
                directory,
                ttl_ms,
                UpdatableTopNStateCodec::new(&converters),
                source,
            );
            let mut rocks = make_update()
                .with_converters(converters)
                .with_payload_schema(schema)
                .with_backend(store);
            rocks
                .memory
                .attach_pool("rocks-topn-test", pool, 0)
                .unwrap();
            rocks.groups.attach_memory(rocks.memory.state_memory());
            (
                RocksTopNHandle::UpdateFast(rocks),
                TopNHandle::UpdateFast(make_update()),
            )
        }
    }
}

#[test]
fn completed_rocks_topn_bundles_return_reservations() {
    for variant in [Variant::Append, Variant::Retract, Variant::UpdateFast] {
        for limit in [1, 3] {
            for mini_batch in [false, true] {
                if matches!(variant, Variant::UpdateFast) && mini_batch {
                    continue;
                }
                let directory = tempfile::tempdir().unwrap();
                let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 << 10));
                let (mut rocks, mut memory) = rankers(
                    &directory.path().join("db"),
                    variant,
                    limit,
                    mini_batch,
                    0,
                    &pool,
                );
                for batch in 0..64 {
                    let data = input(batch * 16, 16, 1, 32, 0);
                    assert_eq!(
                        rocks.push(&data, batch).unwrap(),
                        memory.push(&data, batch).unwrap()
                    );
                    if mini_batch {
                        assert!(pool.reserved() > 0, "active mini-batch must remain charged");
                        assert_eq!(rocks.flush(), memory.flush());
                    }
                    assert_eq!(
                        pool.reserved(),
                        0,
                        "completed {variant:?} limit={limit} mini_batch={mini_batch} batch={batch}"
                    );
                    assert_eq!(rocks.state_bytes(), 0);
                }
                let checkpoint = directory.path().join("checkpoint");
                rocks.checkpoint(checkpoint.to_str().unwrap()).unwrap();
                assert_eq!(pool.reserved(), 0);
                let again = input(0, 16, 2, 32, 0);
                assert_eq!(
                    rocks.push(&again, 100).unwrap(),
                    memory.push(&again, 100).unwrap()
                );
                if mini_batch {
                    assert_eq!(rocks.flush(), memory.flush());
                }
                assert_eq!(pool.reserved(), 0);
                drop(rocks);
                assert_eq!(pool.reserved(), 0);
            }
        }
    }
}

#[test]
fn oversized_active_rocks_topn_bundles_still_fail() {
    for variant in [Variant::Append, Variant::Retract, Variant::UpdateFast] {
        for mini_batch in [false, true] {
            if matches!(variant, Variant::UpdateFast) && mini_batch {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
            let (mut rocks, _) = rankers(
                &directory.path().join("db"),
                variant,
                3,
                mini_batch,
                0,
                &pool,
            );
            assert!(rocks.push(&input(0, 16, 1, 1024, 0), 0).is_err());
            drop(rocks);
            assert_eq!(pool.reserved(), 0);
        }
    }
}

#[test]
fn mini_batches_charge_hydrated_rows_and_replaced_preimage_payloads() {
    for variant in [Variant::Append, Variant::Retract] {
        for limit in [1, 3] {
            let directory = tempfile::tempdir().unwrap();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 << 10));
            let (mut rocks, mut memory) =
                rankers(&directory.path().join("db"), variant, limit, true, 0, &pool);
            let seed = input(0, 1, 10, 16 << 10, 0);
            assert_eq!(
                rocks.push(&seed, 0).unwrap(),
                memory.push(&seed, 0).unwrap()
            );
            assert_eq!(rocks.flush(), memory.flush());
            assert_eq!(pool.reserved(), 0);

            // A rejected challenger still hydrates the existing wide winner from RocksDB.
            let rejected = input(0, 1, 1, 0, 0);
            assert_eq!(
                rocks.push(&rejected, 1).unwrap(),
                memory.push(&rejected, 1).unwrap()
            );
            assert!(pool.reserved() >= 16 << 10);
            assert_eq!(rocks.flush(), memory.flush());
            assert_eq!(pool.reserved(), 0);

            let narrow = input(0, 1, 20, 0, 0);
            assert_eq!(
                rocks.push(&narrow, 2).unwrap(),
                memory.push(&narrow, 2).unwrap()
            );
            assert!(
                pool.reserved() >= 16 << 10,
                "replaced wide payload survives in the preimage"
            );
            assert_eq!(rocks.flush(), memory.flush());
            assert_eq!(pool.reserved(), 0);
        }
    }
}

#[test]
fn hydrated_preimages_cannot_bypass_a_shared_pool_denial() {
    for variant in [Variant::Append, Variant::Retract] {
        let directory = tempfile::tempdir().unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 << 10));
        let (mut rocks, _) = rankers(&directory.path().join("db"), variant, 1, true, 0, &pool);
        rocks.push(&input(0, 1, 10, 16 << 10, 0), 0).unwrap();
        rocks.flush();
        let other_task = MemoryConsumer::new("other task").register(&pool);
        other_task.try_grow(120 << 10).unwrap();
        assert!(rocks.push(&input(0, 1, 20, 0, 0), 1).is_err());
        drop(rocks);
        assert_eq!(pool.reserved(), 120 << 10);
        drop(other_task);
        assert_eq!(pool.reserved(), 0);
    }
}

#[test]
fn winner_cache_remains_separately_reserved_and_bounded() {
    let directory = tempfile::tempdir().unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 << 10));
    let (mut rocks, mut memory) = rankers(
        &directory.path().join("db"),
        Variant::Append,
        1,
        false,
        0,
        &pool,
    );
    if let RocksTopNHandle::Append(ranker) = &mut rocks {
        ranker
            .groups
            .enable_resident_cache(4 << 10, ranker.memory.temporary_reservation());
    }
    for batch in 0..32 {
        let data = input(batch * 8, 8, 1, 32, 0);
        for _ in 0..2 {
            assert_eq!(
                rocks.push(&data, 0).unwrap(),
                memory.push(&data, 0).unwrap()
            );
            let bytes = rocks.cache_stats()[3] as usize;
            assert_eq!(pool.reserved(), bytes);
            assert!(bytes <= 4 << 10);
            assert_eq!(rocks.state_bytes(), 0);
        }
        assert!(rocks.cache_stats()[3] > 0);
    }
    rocks
        .checkpoint(directory.path().join("checkpoint").to_str().unwrap())
        .unwrap();
    assert_eq!(pool.reserved(), 0);
    drop(rocks);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn optional_winner_cache_denial_does_not_fail_or_leak_state() {
    let directory = tempfile::tempdir().unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 << 10));
    let (mut rocks, mut memory) = rankers(
        &directory.path().join("db"),
        Variant::Append,
        1,
        false,
        0,
        &pool,
    );
    if let RocksTopNHandle::Append(ranker) = &mut rocks {
        ranker
            .groups
            .enable_resident_cache(4 << 10, ranker.memory.temporary_reservation());
    }
    let data = input(0, 1, 1, 1024, 0);
    assert_eq!(
        rocks.push(&data, 0).unwrap(),
        memory.push(&data, 0).unwrap()
    );
    let other_task = MemoryConsumer::new("other task").register(&pool);
    other_task.try_grow(6 << 10).unwrap();
    assert_eq!(
        rocks.push(&data, 1).unwrap(),
        memory.push(&data, 1).unwrap()
    );
    assert_eq!(rocks.cache_stats()[3], 0);
    assert_eq!(rocks.state_bytes(), 0);
    assert_eq!(pool.reserved(), 6 << 10);
    drop(rocks);
    drop(other_task);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn checkpoint_restore_and_ttl_keep_changelogs_and_accounting() {
    for variant in [Variant::Append, Variant::Retract, Variant::UpdateFast] {
        for mini_batch in [false, true] {
            if matches!(variant, Variant::UpdateFast) && mini_batch {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 << 10));
            let (mut rocks, mut memory) = rankers(
                &directory.path().join("db"),
                variant,
                3,
                mini_batch,
                100,
                &pool,
            );
            let seed = input(0, 16, 1, 32, 0);
            assert_eq!(
                rocks.push(&seed, 0).unwrap(),
                memory.push(&seed, 0).unwrap()
            );
            if mini_batch {
                assert_eq!(rocks.flush(), memory.flush());
            }
            let checkpoint = directory.path().join("checkpoint");
            let manifest = rocks.checkpoint(checkpoint.to_str().unwrap()).unwrap();
            drop(rocks);
            assert_eq!(pool.reserved(), 0);
            let source = (
                checkpoint.to_string_lossy().into_owned(),
                manifest.snapshot_id,
            );
            let (mut rocks, _) = rankers_from(
                &directory.path().join("restored"),
                variant,
                3,
                mini_batch,
                100,
                &pool,
                Some(&source),
            );
            for (now, kind) in [(50, 0), (60, 3), (200, 0)] {
                if matches!(variant, Variant::Append | Variant::UpdateFast) && kind == 3 {
                    continue;
                }
                let data = input(0, 16, 2, 32, kind);
                assert_eq!(
                    rocks.push(&data, now).unwrap(),
                    memory.push(&data, now).unwrap()
                );
                if mini_batch {
                    assert_eq!(rocks.flush(), memory.flush());
                }
                assert_eq!(pool.reserved(), 0);
            }
            drop(rocks);
            assert_eq!(pool.reserved(), 0);
        }
    }
}

#[test]
fn resident_memory_backend_still_enforces_its_budget() {
    for variant in [Variant::Append, Variant::Retract, Variant::UpdateFast] {
        let directory = tempfile::tempdir().unwrap();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(128 << 10));
        let (_, memory) = rankers(&directory.path().join("db"), variant, 3, false, 0, &pool);
        let mut memory = memory.with_memory_budget(1024).unwrap();
        assert!(memory.push(&input(0, 16, 1, 1024, 0), 0).is_err());
    }
}
