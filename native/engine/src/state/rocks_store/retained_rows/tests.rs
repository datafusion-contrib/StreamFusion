use super::*;
use crate::state::rocks_store::*;
use arrow::array::builder::{Int64Builder, MapBuilder, StringBuilder};

fn input() -> RecordBatch {
    let mut maps = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    for row in 0..4 {
        if row == 0 || row == 3 {
            maps.keys().append_value("duplicate");
            maps.values().append_value(row);
            maps.keys().append_value("duplicate");
            maps.values().append_null();
        }
        maps.append(row != 1).unwrap();
    }
    let maps = Arc::new(maps.finish()) as ArrayRef;
    let field = Arc::new(
        Field::new("original-map", maps.data_type().clone(), true).with_metadata(
            std::collections::HashMap::from([("owner".into(), "flink".into())]),
        ),
    );
    let payload = Arc::new(StructArray::new(vec![field].into(), vec![maps], None)) as ArrayRef;
    RecordBatch::try_from_iter(vec![
        ("k", Arc::new(Int64Array::from(vec![1; 4])) as ArrayRef),
        ("rt", Arc::new(Int64Array::from(vec![300, 100, 200, 100]))),
        ("window_start", Arc::new(Int64Array::from(vec![0; 4]))),
        ("window_end", Arc::new(Int64Array::from(vec![1000; 4]))),
        ("payload", payload),
    ])
    .unwrap()
}

fn types(batch: &RecordBatch) -> Vec<DataType> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect()
}

fn config(root: &std::path::Path, name: &str) -> RocksStoreConfig {
    RocksStoreConfig {
        table_dir: root.join(name).to_string_lossy().into_owned(),
        max_parallelism: 128,
        options_json: include_str!("../../../../benches/fixtures/rocks-options.json").into(),
        ttl_ms: 0,
        shared_resources: 0,
    }
}

#[test]
fn nested_maps_keep_order_duplicates_nulls_offsets_and_declared_fields() {
    let batch = input();
    let codec = RetainedRows::new(&types(&batch)).unwrap();
    for slice in [batch.clone(), batch.slice(1, 3)] {
        let rows = codec.encode(slice.columns()).unwrap();
        let columns = codec.decode(rows.iter().map(|row| row.data())).unwrap();
        assert_eq!(
            slice,
            RecordBatch::try_new(slice.schema(), columns).unwrap()
        );
    }
    let columns = &batch.columns()[..4];
    let old = RowConverter::new(
        types(&batch)[..4]
            .iter()
            .cloned()
            .map(SortField::new)
            .collect(),
    )
    .unwrap()
    .convert_columns(columns)
    .unwrap();
    let current = RetainedRows::new(&types(&batch)[..4])
        .unwrap()
        .encode(columns)
        .unwrap();
    for (old, current) in old.iter().zip(current.iter()) {
        assert_eq!(
            old.data(),
            current.data(),
            "scalar checkpoint bytes changed"
        );
    }
}

#[test]
fn nested_payload_buffers_fire_identically_on_memory_and_persistent_stores() {
    let temp = tempfile::tempdir().unwrap();
    let batch = input();
    let mut memory = crate::sorter::TemporalSorter::new(1);
    let mut disk = crate::sorter::TemporalSorter::new(1).with_store(
        RocksTemporalSortBuffer::create(config(temp.path(), "sort"), batch.schema()).unwrap(),
    );
    memory.push(batch.clone()).unwrap();
    disk.push(batch.clone()).unwrap();
    assert_eq!(memory.flush(200).unwrap(), disk.flush(200).unwrap());
    assert_eq!(
        memory.flush(i64::MAX).unwrap(),
        disk.flush(i64::MAX).unwrap()
    );

    let mut memory = crate::dedup::KeepFirstDeduplicator::new(vec![0], 1)
        .with_key_timestamp_precisions(vec![i32::MIN, -1]);
    let mut disk = crate::dedup::KeepFirstDeduplicator::new(vec![0], 1)
        .with_key_timestamp_precisions(vec![i32::MIN, -1])
        .with_store(
            RocksKeepFirstDedupStore::create(config(temp.path(), "dedup"), batch.schema(), &[0])
                .unwrap(),
        );
    memory.push(&batch, 0).unwrap();
    disk.push(&batch, 0).unwrap();
    assert_eq!(memory.flush(200, 0).unwrap(), disk.flush(200, 0).unwrap());

    let ranker = || {
        crate::topn::WindowRanker::new(
            2,
            3,
            vec![0],
            vec![SortColumn {
                index: 1,
                ascending: true,
                nulls_first: true,
            }],
            2,
            false,
        )
        .with_key_timestamp_precisions(vec![i32::MIN, -1])
    };
    let mut memory = ranker();
    let mut disk = ranker().with_store(
        RocksWindowRankStore::create(config(temp.path(), "rank"), &types(&batch), 0..=127).unwrap(),
        batch.schema(),
    );
    memory.push(&batch).unwrap();
    disk.push(&batch).unwrap();
    assert_eq!(memory.flush(1000).unwrap(), disk.flush(1000).unwrap());
}

#[test]
fn old_blob_checkpoints_upgrade_nested_buffers_through_aligned_and_clipped_restore() {
    use crate::{dedup::KeepFirstDeduplicator, sorter::TemporalSorter, topn::WindowRanker};
    let batch = input();
    for aligned in [false, true] {
        for family in ["sort", "dedup", "rank"] {
            let temp = tempfile::tempdir().unwrap();
            let mut old =
                RocksSnapshotStore::open_merged(config(temp.path(), "old"), &[], 0..=127, false)
                    .unwrap();
            let ranker = || {
                WindowRanker::new(
                    2,
                    3,
                    vec![0],
                    vec![SortColumn {
                        index: 1,
                        ascending: true,
                        nulls_first: true,
                    }],
                    2,
                    false,
                )
                .with_key_timestamp_precisions(vec![i32::MIN, -1])
            };
            let mut dedup = KeepFirstDeduplicator::new(vec![0], 1)
                .with_key_timestamp_precisions(vec![i32::MIN, -1]);
            dedup.push(&batch, 0).unwrap();
            let bytes = match family {
                "sort" => write_ipc(&batch),
                "dedup" => dedup.snapshot(),
                _ => [i64::MIN.to_le_bytes().to_vec(), write_ipc(&batch)].concat(),
            };
            let group = if family == "sort" {
                0
            } else {
                flink_key_group(binary_row_hash(&batch, &[0], 0, &[i32::MIN, -1]), 128) as i32
            };
            let saved = temp.path().join("saved");
            let manifest = old
                .checkpoint(
                    &[[group.to_be_bytes().to_vec(), bytes].concat()],
                    4567,
                    saved.to_str().unwrap(),
                )
                .unwrap();
            drop(old);
            let sources = vec![(saved.to_string_lossy().into_owned(), manifest.snapshot_id)];
            match family {
                "sort" => {
                    let mut expected = TemporalSorter::new(1);
                    expected.push(batch.clone()).unwrap();
                    let store = RocksTemporalSortBuffer::open_merged(
                        config(temp.path(), "typed"),
                        batch.schema(),
                        &sources,
                        0..=127,
                        aligned,
                    )
                    .unwrap();
                    let mut target = TemporalSorter::new(1).with_store(store);
                    target.migrate_legacy_snapshot_store().unwrap();
                    assert_eq!(expected.flush(200).unwrap(), target.flush(200).unwrap());
                    expected.push(batch.clone()).unwrap();
                    target.push(batch.clone()).unwrap();
                    assert_eq!(
                        expected.flush(i64::MAX).unwrap(),
                        target.flush(i64::MAX).unwrap()
                    );
                }
                "dedup" => {
                    let store = RocksKeepFirstDedupStore::open_merged(
                        config(temp.path(), "typed"),
                        batch.schema(),
                        &[0],
                        &sources,
                        group..=group,
                        aligned,
                    )
                    .unwrap();
                    let mut target = KeepFirstDeduplicator::new(vec![0], 1)
                        .with_key_timestamp_precisions(vec![i32::MIN, -1])
                        .with_store(store);
                    target.migrate_legacy_snapshot_store().unwrap();
                    dedup.push(&batch, 0).unwrap();
                    target.push(&batch, 0).unwrap();
                    assert_eq!(dedup.flush(500, 0).unwrap(), target.flush(500, 0).unwrap());
                }
                _ => {
                    let store = RocksWindowRankStore::open_merged(
                        config(temp.path(), "typed"),
                        &types(&batch),
                        group..=group,
                        &sources,
                        aligned,
                    )
                    .unwrap();
                    let mut target = ranker().with_store(store, batch.schema());
                    target.migrate_legacy_snapshot_store().unwrap();
                    assert_eq!(target.store_timer_deadline(), 4567);
                    let mut expected = ranker();
                    expected.push(&batch).unwrap();
                    expected.push(&batch).unwrap();
                    target.push(&batch).unwrap();
                    assert_eq!(expected.flush(1000).unwrap(), target.flush(1000).unwrap());
                }
            }
        }
    }
}

#[test]
fn pinned_nested_rows_remain_bounded_after_live_replacement_and_owner_close() {
    let temp = tempfile::tempdir().unwrap();
    let batch = input();
    let wide = concat_batches(&batch.schema(), std::iter::repeat_n(&batch, 1025)).unwrap();
    let options = config(temp.path(), "sort");
    let mut live = RocksTemporalSortBuffer::create(options.clone(), batch.schema()).unwrap();
    live.push(&wide, &rt_to_millis(wide.column(1))).unwrap();
    let pin = FrozenDatabases::capture(temp.path()).unwrap();
    live.take_complete(i64::MAX).unwrap();
    live.push(&batch, &rt_to_millis(batch.column(1))).unwrap();
    drop(live);
    pin.with(|| {
        let captured = RocksTemporalSortBuffer::create(options, batch.schema()).unwrap();
        let mut frames = Vec::new();
        captured
            .visit_buffered(1024, |frame| {
                assert!(frame.num_rows() <= 1024);
                frames.push(frame);
                Ok(())
            })
            .unwrap();
        assert_eq!(frames.len(), 5);
        assert_eq!(wide, concat_batches(&batch.schema(), &frames).unwrap());
    });
}
