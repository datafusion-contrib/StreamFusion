use super::*;

fn fixture(directory: &Path) -> StateDb {
    open_shared_db(
        &RocksStoreConfig {
            table_dir: directory.to_string_lossy().into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        },
        &[(None, 0)],
    )
    .unwrap()
    .db
}

fn codec() -> WindowStateCodec {
    WindowStateCodec::new(&build_aggregates(&[7, 43, 3], &[0, 0, 0])).unwrap()
}

fn owner(group: i32, tail: &[u8]) -> Vec<u8> {
    let mut key = group.to_be_bytes().to_vec();
    key.extend_from_slice(tail);
    key
}

fn list(ty: &DataType, values: ArrayRef) -> ArrayRef {
    let DataType::List(field) = ty else {
        panic!("list type")
    };
    Arc::new(ListArray::new(
        field.clone(),
        arrow::buffer::OffsetBuffer::new(vec![0, values.len() as i32].into()),
        values,
        None,
    ))
}

fn state(codec: &WindowStateCodec, values: &[i64], cached: i64) -> Vec<ArrayRef> {
    let array = Arc::new(Int64Array::from_iter_values(values.iter().copied())) as ArrayRef;
    vec![
        list(&codec.types[0], array.clone()),
        list(&codec.types[1], array),
        list(
            &codec.types[2],
            Arc::new(Int64Array::from_iter_values(
                values
                    .iter()
                    .enumerate()
                    .map(|(i, _)| if i % 2 == 0 { -2 } else { 3 }),
            )),
        ),
        Arc::new(Int64Array::from(vec![cached])),
        Arc::new(Int64Array::from(vec![99])),
    ]
}

fn collect(
    codec: &WindowStateCodec,
    db: &StateDb,
    owner: &[u8],
    field: usize,
) -> Vec<(ScalarValue, i64)> {
    let mut all = Vec::new();
    codec
        .visit_memberships(db, owner, field, |rows| {
            assert!(rows.len() <= CHUNK_ROWS);
            all.extend_from_slice(rows);
            Ok(())
        })
        .unwrap();
    all
}

#[test]
fn hot_window_and_session_maps_stream_from_independent_pins_after_live_owner_close() {
    for tail in [
        b"window-end/group-key".as_slice(),
        b"group-key/session-start".as_slice(),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let db = fixture(&path);
        let codec = codec();
        let key = owner(9, tail);
        let values = (0..2051).collect::<Vec<_>>();
        codec
            .encode_rows(&db, &[(key.clone(), 2000)], &state(&codec, &values, -7))
            .unwrap();
        let header = db.get(&key).unwrap().unwrap();
        assert!(header.len() < 64);
        assert_eq!(
            codec.decode_headers(&[&header[8..]]).unwrap()[0],
            vec![
                ScalarValue::Int64(Some(2051)),
                ScalarValue::Int64(Some(-7)),
                ScalarValue::Int64(Some(99))
            ]
        );
        let before = collect(&codec, &db, &key, 1);
        let pin = FrozenDatabases::capture(temp.path()).unwrap();
        codec
            .encode_rows(&db, &[(key.clone(), 2000)], &state(&codec, &[5000], 4))
            .unwrap();
        assert_eq!(collect(&codec, &db, &key, 0).len(), 1);
        db.flush().unwrap();
        db.compact_range(None::<&[u8]>, None::<&[u8]>);
        drop(db);
        pin.with(|| {
            let frozen = fixture(&path);
            assert_eq!(before, collect(&codec, &frozen, &key, 1));
            assert_eq!(header, frozen.get(&key).unwrap().unwrap());
            assert!(codec
                .import_memberships(
                    &frozen,
                    0,
                    &[(key.clone(), ScalarValue::Int64(Some(9000)), 1)]
                )
                .is_err());
            let restored = codec.decode_state(&frozen, &key, &header[8..]).unwrap();
            assert_eq!(restored.len(), 5);
            assert_eq!(restored[3], ScalarValue::Int64(Some(-7)));
        });
    }
}

#[test]
fn imports_validate_whole_frames_and_keep_signed_frequencies_and_owner_boundaries() {
    let temp = tempfile::tempdir().unwrap();
    let db = fixture(temp.path());
    let codec = codec();
    let first = owner(2, b"key");
    let neighbor = owner(2, b"key-longer");
    for key in [&first, &neighbor] {
        codec
            .encode_rows(&db, &[(key.clone(), 10)], &state(&codec, &[], 17))
            .unwrap();
    }
    let value = ScalarValue::Int64(Some(5));
    assert!(codec
        .import_memberships(
            &db,
            1,
            &[
                (first.clone(), value.clone(), -3),
                (owner(3, b"absent"), value.clone(), 7)
            ]
        )
        .is_err());
    assert!(collect(&codec, &db, &first, 1).is_empty());
    assert!(codec
        .import_memberships(
            &db,
            1,
            &[
                (first.clone(), value.clone(), 1),
                (first.clone(), value.clone(), 2)
            ]
        )
        .is_err());
    codec
        .import_memberships(
            &db,
            1,
            &[
                (first.clone(), value.clone(), -3),
                (neighbor.clone(), value.clone(), 9),
            ],
        )
        .unwrap();
    assert!(codec
        .import_memberships(&db, 1, &[(first.clone(), value.clone(), 8)])
        .is_err());
    assert_eq!(collect(&codec, &db, &first, 1), vec![(value.clone(), -3)]);
    assert!(codec
        .import_memberships(&db, 0, &[(first.clone(), value.clone(), -1)])
        .is_err());
    codec.delete_owner(&db, &first).unwrap();
    assert!(collect(&codec, &db, &first, 1).is_empty());
    assert_eq!(collect(&codec, &db, &neighbor, 1), vec![(value, 9)]);
    for entry in db.iterator(IteratorMode::Start) {
        let (key, _) = entry.unwrap();
        assert_eq!(persisted_key_group(&key), Some(2));
    }
    assert_eq!(persisted_key_group(TIMER_DEADLINE_KEY), None);
}

#[test]
fn scalar_headers_keep_existing_bytes_and_empty_aggregate_rows_keep_owners() {
    let temp = tempfile::tempdir().unwrap();
    let db = fixture(temp.path());
    let codec = WindowStateCodec::new(&build_aggregates(&[3, 0], &[0, 0])).unwrap();
    assert!(!codec.has_memberships());
    let key = owner(4, b"scalar");
    let columns = codec
        .types
        .iter()
        .enumerate()
        .map(|(index, ty)| {
            assert_eq!(ty, &DataType::Int64);
            Arc::new(Int64Array::from(vec![5 + index as i64])) as ArrayRef
        })
        .collect::<Vec<_>>();
    codec
        .encode_rows(&db, &[(key.clone(), -30)], &columns)
        .unwrap();
    let old = RowConverter::new(
        codec
            .types
            .iter()
            .map(|ty| SortField::new(ty.clone()))
            .collect(),
    )
    .unwrap()
    .convert_columns(&columns)
    .unwrap();
    let mut expected = (-30i64).to_le_bytes().to_vec();
    expected.extend_from_slice(old.row(0).data());
    assert_eq!(db.get(&key).unwrap().unwrap(), expected);
    let empty = WindowStateCodec::new(&[]).unwrap();
    let key = owner(4, b"empty");
    empty.encode_rows(&db, &[(key.clone(), 44)], &[]).unwrap();
    assert_eq!(db.get(key).unwrap().unwrap(), 44i64.to_le_bytes());
}

#[test]
fn refused_receivers_and_mid_owner_cancellation_preserve_live_records() {
    let temp = tempfile::tempdir().unwrap();
    let db = fixture(temp.path());
    let codec = codec();
    let key = owner(1, b"hot");
    let values = (0..2051).collect::<Vec<_>>();
    codec
        .encode_rows(&db, &[(key.clone(), 0)], &state(&codec, &values, 1))
        .unwrap();
    assert!(codec
        .visit_memberships(&db, &key, 0, |_| Err(invalid("receiver refused")))
        .is_err());
    let pin = FrozenDatabases::capture(temp.path()).unwrap();
    let mut calls = 0;
    pin.with(|| {
        assert!(codec
            .visit_memberships(&db, &key, 0, |_| {
                calls += 1;
                pin.cancel();
                Ok(())
            })
            .is_err())
    });
    assert_eq!(calls, 1);
    assert_eq!(collect(&codec, &db, &key, 0).len(), 2051);
}

fn config(path: &Path) -> RocksStoreConfig {
    RocksStoreConfig {
        table_dir: path.to_string_lossy().into_owned(),
        max_parallelism: 128,
        options_json: include_str!("../../../../benches/fixtures/rocks-options.json").into(),
        ttl_ms: 0,
        shared_resources: 0,
    }
}

fn input(times: &[i64], values: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("ts", Arc::new(Int64Array::from(times.to_vec())) as ArrayRef),
        (
            "value0",
            Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
        ),
        (
            "value1",
            Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
        ),
        (
            "value2",
            Arc::new(Int64Array::from(vec![1; times.len()])) as ArrayRef,
        ),
        (
            "key0",
            Arc::new(Int64Array::from(vec![Some(7); times.len()])) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn rendered(batch: &RecordBatch) -> Vec<String> {
    let mut rows = (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .map(|column| format!("{:?}", ScalarValue::try_from_array(column, row).unwrap()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn persistent_fixed_windows_continue_and_fire_with_record_level_views() {
    for cumulative in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let store =
            RocksWindowAggStore::create_with_codec(config(temp.path()), codec(), 0..=127).unwrap();
        let mut memory =
            TumblingAggregator::new(3000, 1000, cumulative, vec![0; 3], vec![7, 43, 3]);
        let mut rocks = TumblingAggregator::new(3000, 1000, cumulative, vec![0; 3], vec![7, 43, 3])
            .with_key_timestamp_precisions(vec![-1])
            .with_store(store, vec![DataType::Int64]);
        for time in [100, 200, 1100, 1300, 2200, 3500] {
            let batch = input(&[time; 4], &[Some(10), Some(10), Some(20), None]);
            memory.update(&batch).unwrap();
            rocks.update(&batch).unwrap();
        }
        for watermark in [1000, 2000, 3000, 4000, 10000] {
            assert_eq!(
                rendered(&memory.flush(watermark).unwrap()),
                rendered(&rocks.flush(watermark).unwrap())
            );
        }
    }
}

#[test]
fn persistent_sessions_bridge_and_rescale_memberships_with_their_headers() {
    let temp = tempfile::tempdir().unwrap();
    let live = temp.path().join("live");
    let mut store =
        RocksSessionAggStore::create_with_codec(config(&live), codec(), 0..=127).unwrap();
    let first = store.db_key(2, b"group", 100);
    let other = store.db_key(9, b"group", 100);
    for key in [&first, &other] {
        store
            .write(&[], &[(key.clone(), 1000)], &state(&codec(), &[3, 5], -7))
            .unwrap();
    }
    let saved = temp.path().join("saved");
    let manifest = store
        .checkpoint(500, i64::MIN, saved.to_str().unwrap())
        .unwrap();
    drop(store);
    let mut restored = RocksSessionAggStore::open_merged_with_codec(
        config(&temp.path().join("restored")),
        codec(),
        2..=2,
        &[(saved.to_string_lossy().into_owned(), manifest.snapshot_id)],
        false,
    )
    .unwrap();
    let sessions = restored
        .sessions_for(&[
            restored.key_prefix(2, b"group"),
            restored.key_prefix(9, b"group"),
        ])
        .unwrap();
    assert_eq!(sessions[0].len(), 1);
    assert!(sessions[1].is_empty());
    assert_eq!(sessions[0][0].2[3], ScalarValue::Int64(Some(-7)));
    assert_eq!(restored.take_closed(2000).unwrap().len(), 1);
    assert!(restored
        .sessions_for(&[restored.key_prefix(2, b"group")])
        .unwrap()[0]
        .is_empty());

    let store = RocksSessionAggStore::create_with_codec(
        config(&temp.path().join("execution")),
        codec(),
        0..=127,
    )
    .unwrap();
    let mut memory = SessionAggregator::new(1000, vec![0; 3], vec![7, 43, 3]);
    let mut rocks = SessionAggregator::new(1000, vec![0; 3], vec![7, 43, 3])
        .with_key_timestamp_precisions(vec![-1])
        .with_store(store, vec![DataType::Int64]);
    for (time, value) in [
        (100, Some(1)),
        (2000, Some(5)),
        (1100, Some(1)),
        (1900, Some(5)),
        (4000, None),
    ] {
        let batch = input(&[time], &[value]);
        memory.update(&batch).unwrap();
        rocks.update(&batch).unwrap();
    }
    assert_eq!(
        rendered(&memory.flush(10000).unwrap()),
        rendered(&rocks.flush(10000).unwrap())
    );
}

#[test]
fn nan_identity_is_canonical_and_signed_zeros_remain_distinct() {
    let temp = tempfile::tempdir().unwrap();
    let db = fixture(temp.path());
    let codec = WindowStateCodec::new(&build_aggregates(&[7], &[1])).unwrap();
    let key = owner(1, b"floating");
    let empty = list(
        &codec.types[0],
        Arc::new(arrow::array::Float64Array::from(Vec::<f64>::new())),
    );
    codec
        .encode_rows(&db, &[(key.clone(), 0)], &[empty])
        .unwrap();
    let value = |bits| ScalarValue::Float64(Some(f64::from_bits(bits)));
    let headers = codec
        .converter
        .convert_columns(&[Arc::new(Int64Array::from(vec![3])) as ArrayRef])
        .unwrap();
    let mut encoded = 0i64.to_le_bytes().to_vec();
    encoded.extend_from_slice(headers.row(0).data());
    db.put(&key, encoded).unwrap();
    codec
        .import_memberships(&db, 0, &[(key.clone(), value(0x7ff8000000000001), 1)])
        .unwrap();
    assert!(codec
        .import_memberships(&db, 0, &[(key.clone(), value(0x7ff8000000000042), 1)])
        .is_err());
    codec
        .import_memberships(
            &db,
            0,
            &[
                (key.clone(), ScalarValue::Float64(Some(0.0)), 1),
                (key.clone(), ScalarValue::Float64(Some(-0.0)), 1),
            ],
        )
        .unwrap();
    assert_eq!(collect(&codec, &db, &key, 0).len(), 3);
}

#[test]
fn canonical_header_and_membership_frames_stay_on_disk_until_complete() {
    let temp = tempfile::tempdir().unwrap();
    let mut store =
        RocksWindowAggStore::create_with_codec(config(temp.path()), codec(), 0..=127).unwrap();
    let key = store.db_key(2, 1000, b"canonical-owner");
    let headers = vec![
        Arc::new(Int64Array::from(vec![2051])) as ArrayRef,
        Arc::new(Int64Array::from(vec![-7])) as ArrayRef,
        Arc::new(Int64Array::from(vec![99])) as ArrayRef,
    ];
    store.import_headers(&[(key.clone(), 0)], &headers).unwrap();
    assert!(store.import_headers(&[(key.clone(), 0)], &headers).is_err());
    let header = vec![
        ScalarValue::Int64(Some(2051)),
        ScalarValue::Int64(Some(-7)),
        ScalarValue::Int64(Some(99)),
    ];
    assert!(store.validate_membership_owner(&key, &header).is_err());
    for start in (0..2051).step_by(1024) {
        let rows = (start..(start + 1024).min(2051))
            .map(|value| (key.clone(), ScalarValue::Int64(Some(value)), 1))
            .collect::<Vec<_>>();
        store.import_membership_rows(0, &rows).unwrap();
        let mut rows = rows;
        for row in &mut rows {
            row.2 = -3;
        }
        store.import_membership_rows(1, &rows).unwrap();
        store
            .visit_groups(1, |rows| {
                assert_eq!(rows[0].state, header);
                Ok(())
            })
            .unwrap();
    }
    store.validate_membership_owner(&key, &header).unwrap();
    assert!(store
        .import_membership_rows(0, &[(key.clone(), ScalarValue::Int64(Some(9999)), 1)])
        .is_err());
    store.validate_membership_owner(&key, &header).unwrap();
    let state = store.get(&[key.clone()]).unwrap().pop().unwrap().unwrap().1;
    assert_eq!(state[3], ScalarValue::Int64(Some(-7)));
    assert_eq!(store.take_closed(1000).unwrap().len(), 1);
    assert!(store.get(&[key]).unwrap()[0].is_none());
}
