use super::*;

fn config(path: &std::path::Path) -> RocksStoreConfig {
    RocksStoreConfig {
        table_dir: path.to_str().unwrap().into(),
        max_parallelism: 8,
        options_json: include_str!("../../../../benches/fixtures/rocks-options.json").into(),
        ttl_ms: 0,
        shared_resources: 0,
    }
}

fn payload(value: i64) -> FrameInput {
    FrameInput {
        row: RecordBatch::try_from_iter(vec![(
            "original",
            Arc::new(Int64Array::from(vec![value])) as ArrayRef,
        )])
        .unwrap(),
        clock: 123,
    }
}

fn store(path: &std::path::Path) -> RocksOverAggStore {
    RocksOverAggStore::create(
        config(path),
        &[],
        &[DataType::Int64],
        &[],
        payload(1).row.schema(),
        0..=7,
    )
    .unwrap()
}

fn append(store: &mut RocksOverAggStore, group: i32, value: i64) -> Vec<u8> {
    let prefix = store.frame_prefix(group, b"partition");
    store
        .write_frames(
            &[(prefix.clone(), 10, 1)],
            &[Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap();
    prefix
}

fn input_rows(store: &RocksOverAggStore) -> usize {
    store
        .db
        .iterator(IteratorMode::Start)
        .map(|row| row.unwrap())
        .filter(|(key, _)| {
            key.len() >= KEY_PREFIX_LEN + FRAME_SUFFIX_LEN && key[4] == FRAME_INPUTS_TABLE
        })
        .count()
}

#[test]
fn frame_inputs_reject_missing_corrupt_or_multirow_payloads_and_remain_retryable() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = store(directory.path());
    store.enable_frame_inputs().unwrap();
    let prefix = append(&mut store, 2, 99);
    let key = RocksOverAggStore::frame_input_key(&RocksOverAggStore::frame_db_key(&prefix, 10, 1));
    assert!(store
        .visit_frame_inputs(|_, _, _| Ok(()))
        .unwrap_err()
        .to_string()
        .contains("missing original"));
    for corrupt in [
        vec![0; 8],
        vec![0; 9],
        FrameInput {
            row: concat_batches(&payload(1).row.schema(), &[payload(1).row, payload(2).row])
                .unwrap(),
            clock: 123,
        }
        .encode(),
    ] {
        store.db.put(&key, corrupt).unwrap();
        assert!(store.visit_frame_inputs(|_, _, _| Ok(())).is_err());
        assert!(store.load_frames(std::slice::from_ref(&prefix)).is_err());
        assert!(store.scan_frames().is_err());
    }
    store
        .write_frame_inputs(&[(prefix, 10, 1)], &[payload(99)])
        .unwrap();
    assert!(store
        .visit_frame_inputs(|_, _, _| Err(DataFusionError::Execution("receiver".into())))
        .is_err());
    let mut count = 0;
    store
        .visit_frame_inputs(|input, time, position| {
            assert_eq!(input.row, payload(99).row);
            assert_eq!(input.clock, 123);
            assert_eq!((time, position), (10, 0));
            count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(input_rows(&store), 1);
    store.db.put(FRAME_INPUTS_KEY, [2]).unwrap();
    assert!(store.frame_inputs_enabled().is_err());
}

#[test]
fn frame_eviction_and_key_cleanup_delete_original_payloads_too() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = store(directory.path());
    store.enable_frame_inputs().unwrap();
    let prefix = append(&mut store, 2, 99);
    store
        .write_frame_inputs(&[(prefix.clone(), 10, 1)], &[payload(99)])
        .unwrap();
    assert_eq!(input_rows(&store), 1);
    store.delete_frames(&[(prefix.clone(), 10, 1)]).unwrap();
    assert_eq!(input_rows(&store), 0);
    append(&mut store, 2, 99);
    store
        .write_frame_inputs(&[(prefix.clone(), 10, 1)], &[payload(99)])
        .unwrap();
    store
        .delete_prefix_range(&[prefix, store.frame_input_prefix(2, b"partition")])
        .unwrap();
    assert_eq!(input_rows(&store), 0);
    assert!(store.scan_frames().unwrap().is_empty());
}

#[test]
fn merged_checkpoint_clips_originals_with_frames_and_retains_the_binding_marker() {
    let directory = tempfile::tempdir().unwrap();
    let mut sources = Vec::new();
    for group in [2, 6] {
        let mut store = store(&directory.path().join(format!("source-{group}")));
        store.enable_frame_inputs().unwrap();
        let prefix = append(&mut store, group, group as i64);
        store
            .write_frame_inputs(&[(prefix, 10, 1)], &[payload(group as i64)])
            .unwrap();
        let path = directory.path().join(format!("checkpoint-{group}"));
        let manifest = store.checkpoint(20, path.to_str().unwrap()).unwrap();
        sources.push((path.to_str().unwrap().to_owned(), manifest.snapshot_id));
    }
    let restored = RocksOverAggStore::open_merged(
        config(&directory.path().join("target")),
        &[],
        &[DataType::Int64],
        &[],
        payload(1).row.schema(),
        0..=3,
        &sources,
        false,
    )
    .unwrap();
    assert!(restored.frame_inputs_enabled().unwrap());
    assert_eq!(input_rows(&restored), 1);
    let mut count = 0;
    restored
        .visit_frame_inputs(|input, time, position| {
            assert_eq!(input.row, payload(2).row);
            assert_eq!((time, position), (10, 0));
            count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(count, 1);
}
