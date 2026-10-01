use super::*;
use crate::state::rocks_store::join_record_tests::config;

struct Directory(std::path::PathBuf);
impl Directory {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("sf-selective-join-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, false),
    ]))
}
fn batch(rows: &[(Option<i64>, i64, i8)]) -> RecordBatch {
    let mut fields = schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new(ROW_KIND_COLUMN, DataType::Int8, false));
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int8Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}
fn memory(unique: bool, ttls: (i64, i64)) -> UpdatingJoiner {
    UpdatingJoiner::new(vec![0], vec![0], JoinKind::Inner, schema(), schema(), None)
        .with_filter_nulls(vec![0])
        .with_unique_join_keys(unique, unique)
        .with_state_ttl(ttls.0, ttls.1)
}
fn persistent(
    path: &std::path::Path,
    unique: bool,
    ttls: (i64, i64),
) -> UpdatingJoiner<RocksJoinStore> {
    let (left, right) = RocksStore::create_pair(
        config(path, ttls.0),
        ttls.1,
        (JoinStateCodec, JoinStateCodec),
    )
    .unwrap();
    memory(unique, ttls)
        .with_backend(
            RocksJoinStore::new(left, true),
            RocksJoinStore::new(right, true),
        )
        .with_read_through_budget(64 << 20)
        .unwrap()
}
fn multiset(batch: &RecordBatch) -> BTreeMap<Vec<Option<i64>>, usize> {
    let mut out = BTreeMap::new();
    for row in 0..batch.num_rows() {
        let values = batch
            .columns()
            .iter()
            .map(|column| {
                if column.is_null(row) {
                    None
                } else if let Some(column) = column.as_any().downcast_ref::<Int64Array>() {
                    Some(column.value(row))
                } else {
                    Some(
                        column
                            .as_any()
                            .downcast_ref::<Int8Array>()
                            .unwrap()
                            .value(row) as i64,
                    )
                }
            })
            .collect();
        *out.entry(values).or_default() += 1;
    }
    out
}
fn step(
    disk: &mut UpdatingJoiner<RocksJoinStore>,
    resident: &mut UpdatingJoiner,
    rows: &[(Option<i64>, i64, i8)],
    left: bool,
    now: i64,
) {
    let input = batch(rows);
    let (left_store, right_store) = disk.stores_mut();
    left_store.set_clock(now);
    right_store.set_clock(now);
    assert_eq!(
        multiset(&disk.push(&input, left, now).unwrap()),
        multiset(&resident.push(&input, left, now).unwrap())
    );
}

#[test]
fn record_join_counts_retractions_unique_replacement_and_null_safe_keys() {
    for unique in [false, true] {
        let dir = Directory::new(&format!("counts-{unique}"));
        let mut disk = persistent(&dir.0.join("state"), unique, (0, 0));
        let mut resident = memory(unique, (0, 0));
        let left: Vec<_> = (0..128).map(|value| (Some(1), value, 0)).collect();
        step(&mut disk, &mut resident, &left, true, 0);
        step(
            &mut disk,
            &mut resident,
            &[(Some(1), 1000, 0), (None, 9, 0)],
            false,
            1,
        );
        // A partial view becoming empty must not delete the other 127 committed rows.
        step(&mut disk, &mut resident, &[(Some(1), 1, 3)], true, 2);
        step(&mut disk, &mut resident, &[(Some(1), 1001, 0)], false, 3);
        step(
            &mut disk,
            &mut resident,
            &[(Some(1), 5, 0), (Some(1), 5, 2), (None, 7, 0)],
            true,
            4,
        );
        step(
            &mut disk,
            &mut resident,
            &[(Some(1), 5, 1), (Some(1), 6, 2), (Some(1), 5, 3)],
            true,
            5,
        );
        step(
            &mut disk,
            &mut resident,
            &[(Some(1), 1001, 3), (None, 9, 0)],
            false,
            6,
        );
        step(
            &mut disk,
            &mut resident,
            &[(Some(1), 6, 3), (Some(1), 6, 3), (None, 7, 3)],
            true,
            7,
        );
        step(
            &mut disk,
            &mut resident,
            &[(Some(1), 1002, 0), (None, 10, 0)],
            false,
            8,
        );
    }
}

#[test]
fn record_join_per_side_ttl_refresh_expiry_and_resurrection() {
    let dir = Directory::new("ttl");
    let mut disk = persistent(&dir.0.join("state"), false, (100, 200));
    let mut resident = memory(false, (100, 200));
    step(&mut disk, &mut resident, &[(Some(1), 100, 0)], false, 0);
    step(
        &mut disk,
        &mut resident,
        &[(Some(1), 10, 0), (Some(1), 11, 0)],
        true,
        10,
    );
    step(
        &mut disk,
        &mut resident,
        &[(Some(1), 10, 0), (Some(1), 10, 3)],
        true,
        90,
    );
    step(&mut disk, &mut resident, &[(Some(1), 11, 3)], true, 110);
    step(&mut disk, &mut resident, &[(Some(1), 101, 0)], false, 189);
    step(&mut disk, &mut resident, &[(Some(1), 11, 0)], true, 190);
    step(&mut disk, &mut resident, &[(Some(1), 100, 0)], false, 210);
    step(
        &mut disk,
        &mut resident,
        &[(Some(1), 11, 3), (Some(1), 11, 0)],
        true,
        290,
    );
    step(
        &mut disk,
        &mut resident,
        &[(Some(1), 101, 3), (Some(1), 102, 0)],
        false,
        389,
    );
}

fn restored(
    path: &std::path::Path,
    sources: &[(String, i64)],
    groups: std::ops::RangeInclusive<i32>,
    aligned: bool,
    ttls: (i64, i64),
    now: i64,
) -> UpdatingJoiner<RocksJoinStore> {
    let (left, right) = RocksStore::open_merged_pair(
        config(path, ttls.0),
        ttls.1,
        (JoinStateCodec, JoinStateCodec),
        sources,
        groups,
        aligned,
        now,
    )
    .unwrap();
    memory(false, ttls)
        .with_backend(
            RocksJoinStore::new(left, true),
            RocksJoinStore::new(right, true),
        )
        .with_read_through_budget(64 << 20)
        .unwrap()
}
fn checkpoint(
    joiner: &mut UpdatingJoiner<RocksJoinStore>,
    path: &std::path::Path,
) -> (String, i64) {
    let (left, right) = joiner.stores_mut();
    let manifest = RocksJoinStore::checkpoint_pair(left, right, path.to_str().unwrap()).unwrap();
    (path.to_string_lossy().into_owned(), manifest.snapshot_id)
}
fn group(key: Option<i64>) -> i32 {
    let input = batch(&[(key, 0, 0)]);
    let mut encoder = BinaryRowBatchEncoder::new(&input, &[0], &[-1]);
    flink_key_group(hash_bytes_by_words(encoder.encode(0)), 128) as i32
}

#[test]
fn record_join_reads_old_bucket_checkpoints_and_rescales_both_directions() {
    let dir = Directory::new("migration-rescale");
    let ttls = (1000, 2000);
    let mut resident = memory(false, ttls);
    let left_rows: Vec<_> = (0..64)
        .flat_map(|key| [(Some(key), 10, 0), (Some(key), 11, 0)])
        .collect();
    let right_rows: Vec<_> = (0..64).map(|key| (Some(key), 100, 0)).collect();
    resident.push(&batch(&left_rows), true, 10).unwrap();
    resident.push(&batch(&right_rows), false, 10).unwrap();
    let (mut old_left, mut old_right) = RocksStore::create_pair(
        config(&dir.0.join("old"), ttls.0),
        ttls.1,
        (JoinStateCodec, JoinStateCodec),
    )
    .unwrap();
    let (left, right) = resident.stores_mut();
    for (key, bucket) in left.iter() {
        old_left.insert(key.clone(), bucket.clone());
    }
    for (key, bucket) in right.iter() {
        old_right.insert(key.clone(), bucket.clone());
    }
    let path = dir.0.join("old-snapshot");
    let manifest =
        RocksStore::checkpoint_pair(&mut old_left, &mut old_right, path.to_str().unwrap()).unwrap();
    drop(old_left);
    drop(old_right);
    let source = (path.to_string_lossy().into_owned(), manifest.snapshot_id);
    let mut disk = restored(&dir.0.join("migrated"), &[source], 0..=127, true, ttls, 20);
    step(
        &mut disk,
        &mut resident,
        &[(Some(1), 10, 3), (Some(2), 12, 0)],
        true,
        20,
    );
    step(
        &mut disk,
        &mut resident,
        &[(Some(1), 101, 0), (Some(2), 101, 0)],
        false,
        20,
    );
    let source = checkpoint(&mut disk, &dir.0.join("migrated-snapshot"));
    drop(disk);
    let probe: Vec<_> = (0..64).map(|key| (Some(key), 102, 0)).collect();
    let expected = multiset(&resident.push(&batch(&probe), false, 30).unwrap());
    let mut observed = BTreeMap::new();
    let mut snapshots = Vec::new();
    for (ordinal, range) in [(0, 0..=63), (1, 64..=127)] {
        let mut split = restored(
            &dir.0.join(format!("split-{ordinal}")),
            std::slice::from_ref(&source),
            range.clone(),
            false,
            ttls,
            30,
        );
        let routed: Vec<_> = probe
            .iter()
            .copied()
            .filter(|row| range.contains(&group(row.0)))
            .collect();
        for (row, count) in multiset(&split.push(&batch(&routed), false, 30).unwrap()) {
            *observed.entry(row).or_default() += count;
        }
        snapshots.push(checkpoint(
            &mut split,
            &dir.0.join(format!("split-snapshot-{ordinal}")),
        ));
    }
    assert_eq!(observed, expected);
    let mut merged = restored(&dir.0.join("merged"), &snapshots, 0..=127, false, ttls, 40);
    let canonical: Vec<_> = merged
        .canonical_partitions()
        .unwrap()
        .into_values()
        .collect();
    let mut canonical_memory = UpdatingJoiner::restore_partitions(
        vec![0],
        vec![0],
        vec![-1],
        JoinKind::Inner,
        schema(),
        schema(),
        None,
        &canonical,
        40,
    )
    .with_filter_nulls(vec![0])
    .with_state_ttl(ttls.0, ttls.1);
    let mut imported = persistent(&dir.0.join("imported"), false, ttls);
    imported.import_partitions(&canonical, 40).unwrap();
    let input = [(Some(1), 11, 3), (Some(2), 12, 3), (Some(63), 13, 0)];
    step(&mut merged, &mut resident, &input, true, 40);
    let actual = multiset(&imported.push(&batch(&input), true, 40).unwrap());
    assert_eq!(
        actual,
        multiset(&canonical_memory.push(&batch(&input), true, 40).unwrap())
    );
}

fn attach_pool(joiner: &mut UpdatingJoiner<RocksJoinStore>, pool: &Arc<dyn MemoryPool>) {
    joiner
        .memory
        .attach_pool("record-join-test", pool, 0)
        .unwrap();
    let left_memory = joiner.memory.temporary_reservation();
    let right_memory = joiner.memory.temporary_reservation();
    let (left, right) = joiner.stores_mut();
    left.attach_read_memory(left_memory);
    right.attach_read_memory(right_memory);
}

#[test]
fn record_join_probe_reuses_more_than_eight_mib_within_shared_budget() {
    let dir = Directory::new("pool-governed-probe");
    let mut disk = persistent(&dir.0.join("state"), false, (0, 0));
    let rows: Vec<_> = (0..50_000).map(|value| (Some(1), value, 0)).collect();
    disk.push(&batch(&rows), false, 0).unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 << 20));
    attach_pool(&mut disk, &pool);
    let input = batch(&[(Some(1), -1, 0)]);
    let (_, right) = disk.stores_mut();
    right.prepare_inner_probe(&input, &[0], &[-1]).unwrap();
    assert!(right.cache_bound > 8 << 20);
    assert_eq!(
        right
            .probe_cache
            .values()
            .map(JoinBucket::len)
            .sum::<usize>(),
        50_000
    );
    assert!(pool.reserved() <= 64 << 20);
    right.end_bundle().unwrap();
    assert_eq!(pool.reserved(), 0);
    let mut total = 0;
    disk.push_to(&input, true, 0, &mut |out| {
        total += out.num_rows();
        Ok(())
    })
    .unwrap();
    assert_eq!(total, 50_000);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn record_join_streams_uncached_fanout_with_bounded_memory_and_releases_on_error() {
    let dir = Directory::new("bounded-probe");
    let mut disk = persistent(&dir.0.join("state"), false, (0, 0));
    let rows: Vec<_> = (0..50_000).map(|value| (Some(1), value, 0)).collect();
    disk.push(&batch(&rows), false, 0).unwrap();
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256 << 10));
    attach_pool(&mut disk, &pool);
    let mut total = 0;
    let mut chunks = 0;
    disk.push_to(&batch(&[(Some(1), -1, 0)]), true, 0, &mut |out| {
        assert!(out.num_rows() <= 4096);
        total += out.num_rows();
        chunks += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(total, 50_000);
    assert!(chunks > 1);
    assert_eq!(pool.reserved(), 0);
    let error = disk
        .push_to(&batch(&[(Some(1), -2, 0)]), true, 0, &mut |_| {
            Err(DataFusionError::Execution("test receiver failed".into()))
        })
        .unwrap_err();
    assert!(error.to_string().contains("test receiver failed"));
    drop(disk);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn record_join_mixed_batches_match_memory_with_ttl_and_null_policies() {
    for unique in [false, true] {
        for null_safe in [false, true] {
            let dir = Directory::new(&format!("mixed-{unique}-{null_safe}"));
            let mut disk = persistent(&dir.0.join("state"), unique, (37, 61));
            let mut resident = memory(unique, (37, 61));
            if !null_safe {
                disk = disk.with_filter_nulls(vec![1]);
                resident = resident.with_filter_nulls(vec![1]);
            }
            let mut random = 0x91e1_0da5_c79e_7b1du64;
            for tick in 0..200 {
                let mut rows = Vec::new();
                for _ in 0..7 {
                    random ^= random << 13;
                    random ^= random >> 7;
                    random ^= random << 17;
                    let key = (random % 5 != 0).then_some((random % 4) as i64);
                    let value = ((random >> 8) % 11) as i64;
                    let kind = ((random >> 16) % 4) as i8;
                    rows.push((key, value, kind));
                }
                step(&mut disk, &mut resident, &rows, tick % 3 != 0, tick * 3);
            }
        }
    }
}

#[test]
fn record_join_probe_skips_expired_payload_and_retries_denied_record() {
    let dir = Directory::new("probe-retry");
    let (left, _) = RocksStore::create_pair(
        config(&dir.0.join("state"), 100),
        100,
        (JoinStateCodec, JoinStateCodec),
    )
    .unwrap();
    let mut store = RocksJoinStore::new(left, true);
    store.set_clock(100);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 << 10));
    store.attach_read_memory(Some(MemoryConsumer::new("probe-test").register(&pool)));
    let key = b"equikey!";
    store.ensure_key(key).unwrap();
    let prefix = store.prefix(key);
    let expired = vec![b'a'; 1 << 20];
    let live = vec![b'b'; 32 << 10];
    for (payload, timestamp) in [(&expired, 0), (&live, 90)] {
        let mut db_key = prefix.clone();
        db_key.extend_from_slice(payload);
        store
            .inner
            .db
            .put(
                db_key,
                encode_meta(RowMeta {
                    count: 1,
                    num_assoc: -1,
                    last_write_ms: timestamp,
                }),
            )
            .unwrap();
    }
    let busy = MemoryConsumer::new("pending-output-test").register(&pool);
    busy.try_resize(48 << 10).unwrap();
    let mut probe = store.inner_probe(key).unwrap();
    assert!(matches!(
        probe.next().unwrap(),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    drop(busy);
    let (row, meta) = probe.next().unwrap().unwrap();
    assert_eq!(row.bytes(), live);
    assert_eq!(meta.last_write_ms, 90);
    assert!(probe.next().is_none());
    drop(row);
    drop(probe);
    drop(store);
    assert_eq!(pool.reserved(), 0);
}

#[test]
#[ignore = "release-only record JOIN probe-cache A/B diagnostic"]
fn record_join_probe_cache_profile() {
    const INPUT_ROWS: usize = 32_768;
    const BATCH_ROWS: usize = 1024;
    for width in [0, 256] {
        for (name, keys, opposite_rows) in [
            ("unique", INPUT_ROWS, 1),
            ("repeated", 64, 32),
            ("skewed", 1024, 8),
            ("large_group", 1, 50_000),
        ] {
            let input_rows = if name == "large_group" {
                64
            } else {
                INPUT_ROWS
            };
            let mut fields = vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ];
            if width > 0 {
                fields.push(Field::new("payload", DataType::Utf8, true));
            }
            let schema = Arc::new(Schema::new(fields));
            let make_batch = |rows: &[(i64, i64)]| {
                let mut columns: Vec<ArrayRef> = vec![
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                    Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
                ];
                if width > 0 {
                    let text = "x".repeat(width);
                    columns.push(Arc::new(StringArray::from_iter(
                        rows.iter()
                            .map(|row| (row.1 % 11 != 0).then_some(text.as_str())),
                    )));
                }
                RecordBatch::try_new(schema.clone(), columns).unwrap()
            };
            let seed: Vec<_> = (0..keys)
                .flat_map(|key| (0..opposite_rows).map(move |value| (key as i64, value as i64)))
                .collect();
            let seed: Vec<_> = seed.chunks(BATCH_ROWS).map(make_batch).collect();
            let input: Vec<_> = (0..input_rows)
                .map(|i| {
                    let key = if name == "skewed" && i % 8 != 0 {
                        0
                    } else {
                        i % keys
                    };
                    (key as i64, (i + opposite_rows) as i64)
                })
                .collect();
            let input: Vec<_> = input.chunks(BATCH_ROWS).map(make_batch).collect();
            for trial in 0..6 {
                // Alternate order to avoid always giving one policy the warmer host.
                let mut policies = [
                    ("bucket", false, 0),
                    ("record_uncached", true, 0),
                    ("record_8m", true, 8 << 20),
                    ("record_64m", true, 64 << 20),
                ];
                policies.rotate_left(trial % 4);
                for (policy, records, cache_bytes) in policies {
                    let dir =
                        Directory::new(&format!("cache-profile-{name}-{width}-{policy}-{trial}"));
                    let (left, right) = RocksStore::create_pair(
                        config(&dir.0.join("state"), 0),
                        0,
                        (JoinStateCodec, JoinStateCodec),
                    )
                    .unwrap();
                    let mut left = RocksJoinStore::new(left, records);
                    let mut right = RocksJoinStore::new(right, records);
                    left.probe_cache_bytes = Some(cache_bytes);
                    right.probe_cache_bytes = Some(cache_bytes);
                    let mut joiner = UpdatingJoiner::new(
                        vec![0],
                        vec![0],
                        JoinKind::Inner,
                        schema.clone(),
                        schema.clone(),
                        None,
                    )
                    .with_backend(left, right)
                    .with_read_through_budget(64 << 20)
                    .unwrap();
                    for batch in &seed {
                        joiner.push_to(batch, false, 0, &mut |_| Ok(())).unwrap();
                    }
                    let start = std::time::Instant::now();
                    let mut output_rows = 0usize;
                    let mut checksum = 0i128;
                    for batch in &input {
                        joiner
                            .push_to(batch, true, 0, &mut |batch| {
                                output_rows += batch.num_rows();
                                for index in [1, schema.fields().len() + 1] {
                                    let values = batch
                                        .column(index)
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap();
                                    checksum +=
                                        values.values().iter().map(|&v| v as i128).sum::<i128>();
                                }
                                Ok(())
                            })
                            .unwrap();
                    }
                    assert_eq!(output_rows, input_rows * opposite_rows);
                    println!("CACHE_PROFILE,{name},{width},{policy},{trial},{:.6},{output_rows},{checksum}", start.elapsed().as_secs_f64());
                }
            }
        }
    }
}
