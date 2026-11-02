use super::*;
#[cfg(feature = "rocksdb-state")]
use crate::state::rocks_config::FlinkRocksOptions;

#[cfg(feature = "rocksdb-state")]
pub(in crate::interval_join) fn options_json() -> String {
    serde_json::to_string(&FlinkRocksOptions {
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
    })
    .unwrap()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("rt", DataType::Int64, false),
    ]))
}

fn batch(keys: &[i64], values: &[Option<i64>], times: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(keys.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
            Arc::new(Int64Array::from(times.to_vec())),
        ],
    )
    .unwrap()
}

fn operator(kind: JoinKind, disk: Option<&std::path::Path>) -> IntervalJoiner {
    let mut joiner = IntervalJoiner::new(
        vec![0],
        vec![0],
        2,
        2,
        -100,
        100,
        None,
        kind,
        schema(),
        schema(),
    );
    #[cfg(feature = "rocksdb-state")]
    if let Some(path) = disk {
        joiner.store = Some(
            crate::state::RocksIntervalBuffer::create(
                RocksStoreConfig {
                    table_dir: path.to_string_lossy().into_owned(),
                    max_parallelism: 128,
                    options_json: options_json(),
                    ttl_ms: 0,
                    shared_resources: 0,
                },
                schema(),
                schema(),
            )
            .unwrap(),
        );
    }
    joiner.with_memory_budget(1 << 27).unwrap()
}

fn rows(joiner: &IntervalJoiner, left: bool) -> Vec<(RecordBatch, Vec<bool>)> {
    let mut batches = Vec::new();
    joiner
        .visit_canonical_rows(left, |batch, flags| {
            assert!(batch.num_rows() <= CHUNK_ROWS);
            assert_eq!(batch.num_rows(), flags.len());
            batches.push((batch, flags));
            Ok(())
        })
        .unwrap();
    batches
}

fn timers(joiner: &mut IntervalJoiner, left: bool) -> BTreeMap<Vec<u8>, i64> {
    let mut entries = BTreeMap::new();
    joiner
        .visit_canonical_timers(left, |key, deadline| {
            assert!(entries.insert(key.to_vec(), deadline).is_none());
            Ok(())
        })
        .unwrap();
    entries
}

fn digest(joiner: &IntervalJoiner) -> BTreeMap<String, Vec<String>> {
    let mut result = BTreeMap::<String, Vec<String>>::new();
    for left in [true, false] {
        for (batch, flags) in rows(joiner, left) {
            for (row, flag) in flags.into_iter().enumerate() {
                let value = |col| {
                    format!(
                        "{:?}",
                        ScalarValue::try_from_array(batch.column(col), row).unwrap()
                    )
                };
                result
                    .entry(format!("{left}:{}:{}", value(0), value(2)))
                    .or_default()
                    .push(format!(
                        "{}:{}",
                        value(1),
                        flag && joiner.join_type != JoinKind::Inner
                    ));
            }
        }
    }
    result
}

fn output(batch: &RecordBatch) -> Vec<String> {
    let mut rows = (0..batch.num_rows())
        .map(|row| {
            (0..batch.num_columns())
                .map(|col| {
                    format!(
                        "{:?}",
                        ScalarValue::try_from_array(batch.column(col), row).unwrap()
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn direct_handoffs_preserve_lists_matches_and_independent_timers() {
    for kind in [
        JoinKind::Inner,
        JoinKind::LeftOuter,
        JoinKind::RightOuter,
        JoinKind::FullOuter,
    ] {
        for source_disk in [false, true] {
            for target_disk in [false, true] {
                #[cfg(not(feature = "rocksdb-state"))]
                if source_disk || target_disk {
                    continue;
                }
                let source = tempfile::tempdir().unwrap();
                let target = tempfile::tempdir().unwrap();
                let mut original = operator(kind, source_disk.then_some(source.path()));
                let key_batch = batch(&[99], &[None], &[1]);
                let key = BinaryRowBatchEncoder::new(&key_batch, &[0], &[-1])
                    .encode(0)
                    .to_vec();
                // A timer survives eager cleanup independently of retained rows.
                original.import_canonical_timer(true, &key, 144).unwrap();
                original
                    .push_left(
                        batch(
                            &[1, 1, 2, 4],
                            &[Some(10), Some(11), Some(20), None],
                            &[100; 4],
                        ),
                        None,
                    )
                    .unwrap();
                original
                    .push_right(batch(&[1], &[Some(30)], &[150]), None)
                    .unwrap();
                let mut restored = operator(kind, target_disk.then_some(target.path()));
                for left in [true, false] {
                    for (batch, flags) in rows(&original, left) {
                        restored
                            .import_canonical_rows(left, &batch, &flags)
                            .unwrap();
                    }
                    for (key, deadline) in timers(&mut original, left) {
                        restored
                            .import_canonical_timer(left, &key, deadline)
                            .unwrap();
                    }
                    assert_eq!(timers(&mut original, left), timers(&mut restored, left));
                }
                assert_eq!(digest(&original), digest(&restored));
                let more = batch(&[2], &[Some(40)], &[120]);
                assert_eq!(
                    output(&original.push_right(more.clone(), None).unwrap()),
                    output(&restored.push_right(more, None).unwrap())
                );
                assert_eq!(
                    output(&original.advance(300).unwrap()),
                    output(&restored.advance(300).unwrap())
                );
                assert_eq!(digest(&original), digest(&restored));
                for left in [true, false] {
                    assert_eq!(timers(&mut original, left), timers(&mut restored, left));
                }
            }
        }
    }
}

#[test]
fn imports_are_owned_chunked_and_recover_as_native_state() {
    let mut before = operator(JoinKind::FullOuter, None);
    let keys = vec![1; CHUNK_ROWS * 2 + 3];
    let values = (0..keys.len()).map(|v| Some(v as i64)).collect::<Vec<_>>();
    before
        .push_left(batch(&keys, &values, &vec![100; keys.len()]), None)
        .unwrap();
    let captured = rows(&before, true);
    assert_eq!(captured.len(), 3);
    let mut restored = operator(JoinKind::FullOuter, None);
    for (batch, flags) in captured {
        let address = batch.column(0).to_data().buffers()[0].as_ptr();
        restored
            .import_canonical_rows(true, &batch, &flags)
            .unwrap();
        assert_ne!(
            address,
            restored
                .left_buffered
                .last()
                .unwrap()
                .column(0)
                .to_data()
                .buffers()[0]
                .as_ptr()
        );
    }
    for (key, deadline) in timers(&mut before, true) {
        restored
            .import_canonical_timer(true, &key, deadline)
            .unwrap();
    }
    assert_eq!(digest(&before), digest(&restored));
    let state = restored.snapshot();
    let mut recovered = IntervalJoiner::restore(
        vec![0],
        vec![0],
        2,
        2,
        -100,
        100,
        None,
        JoinKind::FullOuter,
        schema(),
        schema(),
        &state,
    );
    assert_eq!(digest(&restored), digest(&recovered));
    assert_eq!(timers(&mut restored, true), timers(&mut recovered, true));
    assert_eq!(
        output(&before.advance(300).unwrap()),
        output(&recovered.advance(300).unwrap())
    );
}

#[test]
fn invalid_transfers_leave_native_state_unchanged() {
    let mut joiner = operator(JoinKind::LeftOuter, None);
    let row = batch(&[1], &[Some(2)], &[100]);
    assert!(joiner.import_canonical_rows(true, &row, &[]).is_err());
    assert!(joiner
        .import_canonical_rows(
            true,
            &RecordBatch::new_empty(Arc::new(Schema::empty())),
            &[]
        )
        .is_err());
    let large = batch(
        &vec![1; CHUNK_ROWS + 1],
        &vec![None; CHUNK_ROWS + 1],
        &vec![100; CHUNK_ROWS + 1],
    );
    assert!(joiner
        .import_canonical_rows(true, &large, &vec![false; CHUNK_ROWS + 1])
        .is_err());
    assert!(digest(&joiner).is_empty());
    joiner.advance(10).unwrap();
    assert!(joiner.import_canonical_rows(true, &row, &[false]).is_err());
    assert!(joiner.import_canonical_timer(true, &[0; 16], 1).is_err());
    assert!(digest(&joiner).is_empty());
}

#[test]
fn memory_denial_releases_rows_and_timer_keys() {
    let mut joiner = operator(JoinKind::FullOuter, None)
        .with_memory_budget(1)
        .unwrap();
    let row = batch(&[1], &[Some(2)], &[100]);
    assert!(joiner.import_canonical_rows(true, &row, &[true]).is_err());
    assert!(digest(&joiner).is_empty());
    assert!(joiner.import_canonical_timer(true, &[0; 16], 100).is_err());
    assert!(timers(&mut joiner, true).is_empty());
    assert_eq!(joiner.left_cleanup.capacity(), 0);
    assert_eq!(joiner.left_next_id, 0);
}

fn tagged(batch: &RecordBatch, outer: bool, kind: i8) -> RecordBatch {
    let arity = if outer {
        batch.num_columns() - 1
    } else {
        batch.num_columns()
    };
    let mut fields = batch.schema().fields()[..arity].to_vec();
    let mut columns = batch.columns()[..arity].to_vec();
    fields.push(Arc::new(Field::new(ROW_KIND_COLUMN, DataType::Int8, false)));
    columns.push(Arc::new(Int8Array::from_value(kind, batch.num_rows())));
    if outer {
        fields.push(batch.schema().field(arity).clone().into());
        columns.push(batch.column(arity).clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn transport_companions_do_not_enter_interval_state() {
    for kind in [
        JoinKind::Inner,
        JoinKind::LeftOuter,
        JoinKind::RightOuter,
        JoinKind::FullOuter,
    ] {
        for disk in [false, true] {
            #[cfg(not(feature = "rocksdb-state"))]
            if disk {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let mut joiner = operator(kind, disk.then_some(directory.path()));
            let left = batch(&[1, 2], &[Some(10), Some(20)], &[100, 100]);
            joiner.push_left(tagged(&left, false, 0), None).unwrap();
            let right = batch(&[1], &[Some(30)], &[150]);
            assert_eq!(
                joiner
                    .push_right(tagged(&right, false, 0), None)
                    .unwrap()
                    .num_columns(),
                6
            );
            assert_eq!(
                rows(&joiner, true)
                    .iter()
                    .map(|(b, _)| b.num_rows())
                    .sum::<usize>(),
                2
            );
            assert!(rows(&joiner, true)
                .iter()
                .all(|(b, _)| b.num_columns() == 3));
        }
    }
}

#[test]
fn invalid_companions_fail_before_changing_interval_frontiers_or_state() {
    let mut joiner = operator(JoinKind::FullOuter, None);
    let input = batch(&[1], &[Some(10)], &[100]);
    for kind in [1, 2, 3] {
        assert!(joiner
            .push_left(tagged(&input, false, kind), Some(999))
            .is_err());
        assert_eq!(joiner.operator_time, 0);
        assert!(digest(&joiner).is_empty());
    }
    let mut fields = input.schema().fields().to_vec();
    fields.push(Arc::new(Field::new("unknown", DataType::Int64, false)));
    let mut columns = input.columns().to_vec();
    columns.push(Arc::new(Int64Array::from_value(0, 1)));
    assert!(joiner
        .push_left(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
            None
        )
        .is_err());
    assert!(digest(&joiner).is_empty());
}

#[test]
fn previous_tagged_native_frames_normalize_on_memory_and_persistent_restore() {
    for kind in [
        JoinKind::Inner,
        JoinKind::LeftOuter,
        JoinKind::RightOuter,
        JoinKind::FullOuter,
    ] {
        let mut source = operator(kind, None);
        source
            .push_left(batch(&[1, 2], &[Some(10), Some(20)], &[100, 100]), None)
            .unwrap();
        source
            .push_right(batch(&[1], &[Some(30)], &[150]), None)
            .unwrap();
        let left = tagged(&source.left_buffered[0], kind != JoinKind::Inner, 0);
        let right = tagged(&source.right_buffered[0], kind != JoinKind::Inner, 0);
        let raw = source.snapshot();
        let sections = read_framed_sections(&raw);
        let old = IntervalJoiner::snapshot_parts([
            write_ipc(&left),
            write_ipc(&right),
            sections[2].to_vec(),
            sections[3].to_vec(),
            sections[4].to_vec(),
            sections[5].to_vec(),
        ]);
        let mut restored = IntervalJoiner::restore(
            vec![0],
            vec![0],
            2,
            2,
            -100,
            100,
            None,
            kind,
            schema(),
            schema(),
            &old,
        );
        assert_eq!(digest(&source), digest(&restored));
        for left in [true, false] {
            assert_eq!(timers(&mut source, left), timers(&mut restored, left));
        }
        #[cfg(feature = "rocksdb-state")]
        {
            let directory = tempfile::tempdir().unwrap();
            let mut persistent = operator(kind, Some(directory.path()));
            persistent.import_partitions(&[old], i64::MIN).unwrap();
            assert_eq!(digest(&source), digest(&persistent));
            assert_eq!(
                output(&source.advance(400).unwrap()),
                output(&persistent.advance(400).unwrap())
            );
        }
    }
}
