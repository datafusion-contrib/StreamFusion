use super::*;
#[cfg(feature = "rocksdb-state")]
use crate::state::rocks_config::FlinkRocksOptions;

#[cfg(feature = "rocksdb-state")]
fn options_json() -> String {
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
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
        Field::new("start", DataType::Int64, true),
        Field::new("end", DataType::Int64, true),
    ]))
}

fn rows(values: &[(Option<i64>, Option<i64>, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter(values.iter().map(|x| x.0))),
            Arc::new(Int64Array::from_iter(values.iter().map(|x| x.1))),
            Arc::new(Int64Array::from_iter_values(
                values.iter().map(|x| x.2 - 1000),
            )),
            Arc::new(Int64Array::from_iter_values(values.iter().map(|x| x.2))),
        ],
    )
    .unwrap()
}

fn operator(kind: JoinKind, disk: Option<&std::path::Path>) -> WindowJoiner {
    let mut joiner =
        WindowJoiner::new(vec![0], vec![0], 2, 3, 2, 3, None, kind, schema(), schema());
    #[cfg(feature = "rocksdb-state")]
    if let Some(path) = disk {
        joiner.store = Some(
            crate::state::RocksWindowBuffer::create(
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

fn capture(joiner: &WindowJoiner, left: bool) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    joiner
        .visit_canonical_rows(left, |batch| {
            assert!(batch.num_rows() <= CHUNK_ROWS);
            batches.push(batch);
            Ok(())
        })
        .unwrap();
    batches
}

fn digest(joiner: &WindowJoiner) -> BTreeMap<String, Vec<String>> {
    let mut result = BTreeMap::<String, Vec<String>>::new();
    for left in [true, false] {
        for batch in capture(joiner, left) {
            for row in 0..batch.num_rows() {
                let value = |col| {
                    format!(
                        "{:?}",
                        ScalarValue::try_from_array(batch.column(col), row).unwrap()
                    )
                };
                result
                    .entry(format!("{left}|{}|{}", value(0), value(3)))
                    .or_default()
                    .push(value(1));
            }
        }
    }
    result
}

fn output(batch: RecordBatch) -> Vec<String> {
    let mut rows = (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|col| format!("{:?}", ScalarValue::try_from_array(col, row).unwrap()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn full_lists_and_continued_joining_survive_live_transfer_between_both_stores() {
    for kind in [
        JoinKind::Inner,
        JoinKind::LeftOuter,
        JoinKind::RightOuter,
        JoinKind::FullOuter,
    ] {
        for source_disk in [false, cfg!(feature = "rocksdb-state")] {
            for target_disk in [false, cfg!(feature = "rocksdb-state")] {
                let a = tempfile::tempdir().unwrap();
                let b = tempfile::tempdir().unwrap();
                let mut source = operator(kind, source_disk.then_some(a.path()));
                let mut target = operator(kind, target_disk.then_some(b.path()));
                source
                    .push_left(rows(&[
                        (Some(1), Some(10), 1000),
                        (Some(1), None, 1000),
                        (Some(2), Some(20), 2000),
                        (None, Some(30), 1000),
                    ]))
                    .unwrap();
                source
                    .push_right(rows(&[
                        (Some(1), Some(100), 1000),
                        (Some(3), Some(300), 1000),
                        (Some(2), Some(200), 2000),
                    ]))
                    .unwrap();
                for left in [true, false] {
                    for batch in capture(&source, left) {
                        target.import_canonical_rows(left, &batch).unwrap();
                    }
                }
                assert_eq!(digest(&source), digest(&target));
                assert_eq!(
                    output(source.flush(1000).unwrap()),
                    output(target.flush(1000).unwrap())
                );
                for joiner in [&mut source, &mut target] {
                    joiner
                        .push_left(rows(&[(Some(2), Some(21), 2000)]))
                        .unwrap();
                }
                assert_eq!(
                    output(source.flush(2000).unwrap()),
                    output(target.flush(2000).unwrap())
                );
                assert_eq!(digest(&source), digest(&target));
            }
        }
    }
}

#[test]
#[cfg(feature = "rocksdb-state")]
fn export_streams_large_persistent_lists_without_hydration() {
    let path = tempfile::tempdir().unwrap();
    let mut source = operator(JoinKind::Inner, Some(path.path()));
    let values = (0..2507)
        .map(|v| (Some(7), Some(v), 1000))
        .collect::<Vec<_>>();
    source.push_left(rows(&values)).unwrap();
    let batches = capture(&source, true);
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![1024, 1024, 459]
    );
    assert!(source.left_buffered.is_empty());
    let restored = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(restored, (0..2507).collect::<Vec<_>>());
}

#[test]
fn imported_slices_own_their_buffers_and_survive_native_checkpoint() {
    let original = rows(&[
        (Some(1), Some(10), 1000),
        (Some(1), None, 1000),
        (Some(2), Some(20), 2000),
    ]);
    let slice = original.slice(1, 2);
    let mut target = operator(JoinKind::Inner, None);
    target.import_canonical_rows(true, &slice).unwrap();
    let owned = &target.left_buffered[0];
    for (input, output) in slice.columns().iter().zip(owned.columns()) {
        assert_ne!(
            input.to_data().buffers()[0].as_ptr(),
            output.to_data().buffers()[0].as_ptr()
        );
    }
    let expected = digest(&target);
    drop(slice);
    drop(original);
    let restored = WindowJoiner::restore(
        vec![0],
        vec![0],
        2,
        3,
        2,
        3,
        None,
        JoinKind::Inner,
        schema(),
        schema(),
        &target.snapshot(),
    );
    assert_eq!(expected, digest(&restored));
}

#[test]
fn invalid_imports_and_memory_denial_leave_state_unchanged() {
    let mut target = operator(JoinKind::Inner, None);
    let expected = digest(&target);
    let mut columns = rows(&[(Some(1), Some(10), 1000)]).columns().to_vec();
    columns[3] = Arc::new(Int64Array::from(vec![None]));
    assert!(target
        .import_canonical_rows(true, &RecordBatch::try_new(schema(), columns).unwrap())
        .is_err());
    let large = rows(&vec![(Some(1), Some(10), 1000); CHUNK_ROWS + 1]);
    assert!(target.import_canonical_rows(true, &large).is_err());
    assert_eq!(expected, digest(&target));
    let mut denied = operator(JoinKind::Inner, None)
        .with_memory_budget(1)
        .unwrap();
    assert!(denied
        .import_canonical_rows(true, &rows(&[(Some(1), Some(10), 1000)]))
        .is_err());
    assert_eq!(expected, digest(&denied));
    target.current_watermark = 0;
    assert!(target
        .import_canonical_rows(true, &rows(&[(Some(1), Some(10), 1000)]))
        .is_err());
}

#[test]
fn nested_imported_payloads_do_not_retain_sliced_source_buffers() {
    let base = rows(&[
        (Some(1), Some(10), 1000),
        (Some(1), None, 1000),
        (Some(2), Some(20), 2000),
    ]);
    let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("first"), None, Some("last")]));
    let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<
        arrow::datatypes::Int64Type,
        _,
        _,
    >(vec![
        Some(vec![Some(1), None]),
        None,
        Some(vec![Some(3), Some(4)]),
    ]));
    let mut fields = base.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        "text",
        strings.data_type().clone(),
        true,
    )));
    fields.push(Arc::new(Field::new(
        "list",
        lists.data_type().clone(),
        true,
    )));
    let schema = Arc::new(Schema::new(fields));
    let mut columns = base.columns().to_vec();
    columns.extend([strings, lists]);
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .unwrap()
        .slice(1, 2);
    let mut target = WindowJoiner::new(
        vec![0],
        vec![0],
        2,
        3,
        2,
        3,
        None,
        JoinKind::Inner,
        schema.clone(),
        schema,
    );
    target.import_canonical_rows(true, &batch).unwrap();
    fn pointers(data: &arrow::array::ArrayData, out: &mut HashSet<usize>) {
        for buffer in data.buffers() {
            if !buffer.is_empty() {
                out.insert(buffer.as_ptr() as usize);
            }
        }
        for child in data.child_data() {
            pointers(child, out);
        }
    }
    let mut borrowed = HashSet::default();
    let mut owned = HashSet::default();
    for (input, retained) in batch
        .columns()
        .iter()
        .zip(target.left_buffered[0].columns())
    {
        assert_eq!(input.as_ref(), retained.as_ref());
        pointers(&input.to_data(), &mut borrowed);
        pointers(&retained.to_data(), &mut owned);
    }
    assert!(borrowed.is_disjoint(&owned));
    drop(batch);
    assert_eq!(target.left_buffered[0].num_rows(), 2);
}

#[test]
#[cfg(feature = "rocksdb-state")]
fn persistent_capture_respects_transient_budget_without_losing_rows() {
    let path = tempfile::tempdir().unwrap();
    let mut source = operator(JoinKind::Inner, Some(path.path()));
    source
        .push_left(rows(&[(Some(1), Some(10), 1000)]))
        .unwrap();
    let expected = digest(&source);
    let source = source.with_memory_budget(1).unwrap();
    assert!(source.visit_canonical_rows(true, |_| Ok(())).is_err());
    let source = source.with_memory_budget(1 << 26).unwrap();
    assert_eq!(expected, digest(&source));
}

#[test]
fn failed_receiver_does_not_consume_live_rows() {
    for disk in [false, cfg!(feature = "rocksdb-state")] {
        let path = tempfile::tempdir().unwrap();
        let mut source = operator(JoinKind::Inner, disk.then_some(path.path()));
        source
            .push_left(rows(&[(Some(1), Some(10), 1000)]))
            .unwrap();
        let expected = digest(&source);
        assert!(source
            .visit_canonical_rows(true, |_| Err(invalid("receiver failed")))
            .is_err());
        assert_eq!(expected, digest(&source));
        source
            .push_right(rows(&[(Some(1), Some(100), 1000)]))
            .unwrap();
        assert_eq!(source.flush(1000).unwrap().num_rows(), 1);
    }
}

fn tagged(batch: RecordBatch, kind: i8, name: &str) -> RecordBatch {
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(name, DataType::Int8, false)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int8Array::from(vec![kind; batch.num_rows()])));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn tagged_live_input_and_plain_canonical_rows_keep_one_logical_schema() {
    for disk in [false, cfg!(feature = "rocksdb-state")] {
        let path = tempfile::tempdir().unwrap();
        let mut source = operator(JoinKind::FullOuter, disk.then_some(path.path()));
        let mut target = operator(JoinKind::FullOuter, None);
        source
            .push_left(tagged(
                rows(&[(Some(1), Some(10), 1000)]),
                0,
                ROW_KIND_COLUMN,
            ))
            .unwrap();
        for batch in capture(&source, true) {
            assert_eq!(batch.schema(), schema());
            target.import_canonical_rows(true, &batch).unwrap();
        }
        for op in [&mut source, &mut target] {
            op.push_right(tagged(
                rows(&[(Some(1), Some(100), 1000)]),
                0,
                ROW_KIND_COLUMN,
            ))
            .unwrap();
            op.push_left(rows(&[(Some(2), Some(20), 1000)])).unwrap();
        }
        assert_eq!(
            output(source.flush(1000).unwrap()),
            output(target.flush(1000).unwrap())
        );
    }
}

#[test]
fn malformed_input_companions_fail_before_changing_live_state() {
    let mut source = operator(JoinKind::Inner, None);
    source
        .push_left(rows(&[(Some(1), Some(10), 1000)]))
        .unwrap();
    let expected = digest(&source);
    for (kind, name) in [(1, ROW_KIND_COLUMN), (0, "unknown-companion")] {
        assert!(source
            .push_left(tagged(rows(&[(Some(1), Some(20), 1000)]), kind, name))
            .is_err());
        assert_eq!(expected, digest(&source));
    }
    let batch = rows(&[(Some(2), Some(20), 2000)]);
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields[1] = Arc::new(Field::new(ROW_KIND_COLUMN, DataType::Int64, true));
    source
        .push_left(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec()).unwrap(),
        )
        .unwrap();
    assert_eq!(
        capture(&source, true)
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
}

#[test]
fn older_tagged_native_snapshots_normalize_before_continued_input() {
    let left = tagged(rows(&[(Some(1), Some(10), 1000)]), 0, ROW_KIND_COLUMN);
    let saved = WindowJoiner::snapshot_parts(write_ipc(&left), Vec::new());
    let mut restored = WindowJoiner::restore(
        vec![0],
        vec![0],
        2,
        3,
        2,
        3,
        None,
        JoinKind::Inner,
        schema(),
        schema(),
        &saved,
    );
    assert_eq!(capture(&restored, true)[0].schema(), schema());
    restored
        .push_left(rows(&[(Some(1), Some(20), 1000)]))
        .unwrap();
    restored
        .push_right(rows(&[(Some(1), Some(100), 1000)]))
        .unwrap();
    assert_eq!(restored.flush(1000).unwrap().num_rows(), 2);
    #[cfg(feature = "rocksdb-state")]
    {
        let path = tempfile::tempdir().unwrap();
        let mut persistent = operator(JoinKind::Inner, Some(path.path()));
        persistent.import_partitions(&[saved], i64::MIN).unwrap();
        persistent
            .push_right(rows(&[(Some(1), Some(100), 1000)]))
            .unwrap();
        assert_eq!(persistent.flush(1000).unwrap().num_rows(), 1);
    }
}
