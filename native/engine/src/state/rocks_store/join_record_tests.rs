use super::*;
use crate::state::rocks_config::FlinkRocksOptions;
use crate::updating_join::{JoinBucket, JoinStateCodec};

fn config(path: &std::path::Path, ttl_ms: i64) -> RocksStoreConfig {
    RocksStoreConfig {
        table_dir: path.to_string_lossy().into_owned(),
        max_parallelism: 128,
        ttl_ms,
        shared_resources: 0,
        options_json: serde_json::to_string(&FlinkRocksOptions {
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
            write_buffer_size: 16 << 20,
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
        })
        .unwrap(),
    }
}

fn input(key: i32) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![key]))],
    )
    .unwrap()
}

fn bucket(rows: usize, width: usize) -> JoinBucket {
    let mut out = JoinBucket::default();
    for row in 0..rows {
        let mut bytes = vec![b'x'; width];
        bytes[..8].copy_from_slice(&(row as u64).to_le_bytes());
        out.insert(
            ByteKey::from(bytes.as_slice()),
            RowMeta {
                count: 1,
                num_assoc: 0,
                last_write_ms: 10,
            },
        );
    }
    out
}

// A deliberately optimistic record-layout probe: it already knows which row changed. Production
// would additionally need a change journal, migration and checkpoint-format validation.
fn record_prefix(store: &RocksStore<JoinStateCodec>, key: &[u8]) -> Vec<u8> {
    let mut prefix = store.db_key(key);
    prefix[4] = 2;
    prefix.splice(5..5, (key.len() as u32).to_be_bytes());
    prefix
}

fn encode_meta(meta: &RowMeta) -> [u8; 20] {
    let mut out = [0; 20];
    out[..8].copy_from_slice(&meta.last_write_ms.to_le_bytes());
    out[8..16].copy_from_slice(&meta.count.to_le_bytes());
    out[16..].copy_from_slice(&meta.num_assoc.to_le_bytes());
    out
}

fn decode_meta(bytes: &[u8]) -> RowMeta {
    RowMeta {
        last_write_ms: i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        count: i64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        num_assoc: i32::from_le_bytes(bytes[16..].try_into().unwrap()),
    }
}

fn measured_store(path: &std::path::Path) -> (RocksStore<JoinStateCodec>, Options) {
    let config = config(path, 0);
    let resolved = FlinkRocksOptions::from_json(&config.options_json).unwrap();
    let (mut options, cache) = resolved.build(None).unwrap();
    options.enable_statistics();
    let opened = OpenedDb {
        db: Arc::new(
            DB::open_cf_with_opts(&options, path, [("default", options.clone())]).unwrap(),
        ),
        cache,
        clock: Arc::new(AtomicI64::new(0)),
        write_batch_size: resolved.write_batch_size,
    };
    (
        RocksStore::attach(&opened, config, JoinStateCodec, Some(0)).unwrap(),
        options,
    )
}

#[test]
#[ignore = "release-only persistent JOIN state diagnostic"]
fn join_record_profile() {
    use rocksdb::perf::{set_perf_stats, PerfContext, PerfMetric, PerfStatsLevel};
    use rocksdb::statistics::Ticker;
    for (name, keys, rows, width) in [
        ("unique", 128, 1, 64),
        ("uniform", 128, 8, 64),
        ("hot", 1, 4096, 1024),
    ] {
        for records in [false, true] {
            let path = std::env::temp_dir().join(format!(
                "sf-join-record-{name}-{records}-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&path);
            let (mut store, statistics) = measured_store(&path);
            for key in 0..keys {
                let batch = input(key);
                let mut encoder = BinaryRowBatchEncoder::new(&batch, &[0], &[-1]);
                let key = encoder.encode(0);
                if records {
                    let prefix = record_prefix(&store, key);
                    let mut writes = FlinkWriteBatch::new(&store.db, store.write_batch_size);
                    for (row, meta) in bucket(rows, width) {
                        let mut db_key = prefix.clone();
                        db_key.extend_from_slice(&row.0);
                        writes.put(db_key, encode_meta(&meta)).unwrap();
                    }
                    writes.finish().unwrap();
                } else {
                    store.insert(ByteKey::from(key), bucket(rows, width));
                }
            }
            store.end_bundle().unwrap();
            store.db.flush().unwrap();
            let mut elapsed = Vec::new();
            let mut reads = 0;
            let mut writes = 0;
            let mut serialized_bytes = 0;
            let mut logical_reads = 0;
            let mut flush_bytes = 0;
            let mut compaction_writes = 0;
            let mut compaction_reads = 0;
            for trial in -2..5 {
                set_perf_stats(PerfStatsLevel::EnableTimeExceptForMutex);
                let mut perf = PerfContext::default();
                perf.reset();
                let before_flush = statistics.get_ticker_count(Ticker::FlushWriteBytes);
                let before_compaction_writes =
                    statistics.get_ticker_count(Ticker::CompactWriteBytes);
                let before_compaction_reads = statistics.get_ticker_count(Ticker::CompactReadBytes);
                let start = std::time::Instant::now();
                for step in 0..64 {
                    let batch = input(step % keys);
                    let mut encoder = BinaryRowBatchEncoder::new(&batch, &[0], &[-1]);
                    let key = encoder.encode(0);
                    let mut row = vec![b'x'; width];
                    row[..8].copy_from_slice(&0u64.to_le_bytes());
                    if records {
                        let prefix = record_prefix(&store, key);
                        let mut hydrated = JoinBucket::default();
                        for entry in store
                            .db
                            .iterator(IteratorMode::From(&prefix, Direction::Forward))
                        {
                            let (db_key, value) = entry.unwrap();
                            if !db_key.starts_with(&prefix) {
                                break;
                            }
                            hydrated.insert(
                                ByteKey::from(&db_key[prefix.len()..]),
                                decode_meta(&value),
                            );
                        }
                        assert_eq!(hydrated.len(), rows);
                        let meta = hydrated.get_mut(row.as_slice()).unwrap();
                        meta.count += 1;
                        assert_eq!(
                            meta.count,
                            2 + (trial + 2) as i64 * (64 / keys as i64).max(1)
                                + (step / keys) as i64
                        );
                        let bytes = encode_meta(meta);
                        if trial >= 0 {
                            serialized_bytes += prefix.len() + row.len() + bytes.len();
                            logical_reads += rows * (prefix.len() + width + 20);
                        }
                        let mut db_key = prefix;
                        db_key.extend_from_slice(&row);
                        store
                            .db
                            .put_opt(db_key, bytes, &flink_write_options())
                            .unwrap();
                    } else {
                        store.begin_batch(&batch, &[0], &[-1]).unwrap();
                        let value = store.get_mut(key).unwrap();
                        value.get_mut(row.as_slice()).unwrap().count += 1;
                        assert_eq!(
                            value.get(row.as_slice()).unwrap().count,
                            2 + (trial + 2) as i64 * (64 / keys as i64).max(1)
                                + (step / keys) as i64
                        );
                        if trial >= 0 {
                            serialized_bytes +=
                                store.db_key(key).len() + 4 + rows * (4 + width + 20);
                            logical_reads += store.db_key(key).len() + 4 + rows * (4 + width + 20);
                        }
                        store.end_bundle().unwrap();
                    }
                }
                let seconds = start.elapsed().as_secs_f64();
                store.db.flush().unwrap();
                if trial >= 0 {
                    elapsed.push(seconds);
                    reads += perf.metric(PerfMetric::BlockReadByte);
                    writes += perf.metric(PerfMetric::WriteMemtableTime);
                    flush_bytes +=
                        statistics.get_ticker_count(Ticker::FlushWriteBytes) - before_flush;
                    compaction_writes += statistics.get_ticker_count(Ticker::CompactWriteBytes)
                        - before_compaction_writes;
                    compaction_reads += statistics.get_ticker_count(Ticker::CompactReadBytes)
                        - before_compaction_reads;
                }
                set_perf_stats(PerfStatsLevel::Disable);
            }
            elapsed.sort_by(f64::total_cmp);
            let values = bucket(rows, width);
            let encoding = std::time::Instant::now();
            for _ in 0..64 {
                if records {
                    std::hint::black_box(encode_meta(values.values().next().unwrap()));
                } else {
                    let mut bytes = Vec::new();
                    JoinStateCodec.raw_write(&values, &mut bytes);
                    std::hint::black_box(bytes);
                }
            }
            println!("JOIN_RECORD {name} records={records} seconds={:.6} block_read_bytes={} memtable_ns={} encode_ns={} logical_write_bytes={} logical_read_bytes={} flush_write_bytes={} compaction_write_bytes={} compaction_read_bytes={} hydrated_payload_bytes={}",
                elapsed[2], reads / 5, writes / 5, encoding.elapsed().as_nanos(), serialized_bytes / 5,
                logical_reads / 5, flush_bytes / 5, compaction_writes / 5, compaction_reads / 5, rows * width);
            drop(store);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
