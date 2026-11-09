use super::*;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("rt", DataType::Int64, false)]))
}
fn config(path: &std::path::Path) -> RocksStoreConfig {
    RocksStoreConfig {
        table_dir: path.to_str().unwrap().into(),
        max_parallelism: 8,
        options_json: include_str!("../../../../benches/fixtures/rocks-options.json").into(),
        ttl_ms: 0,
        shared_resources: 0,
    }
}

#[test]
fn legacy_nan_membership_keys_migrate_on_aligned_and_merged_restore_without_changing_folds() {
    for aligned in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let types = [DataType::Float64, DataType::Float32];
        let mut store = RocksOverAggStore::create(
            config(&directory.path().join("source")),
            &[DataType::Int64],
            &[],
            &types,
            schema(),
            0..=7,
        )
        .unwrap();
        let key_converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let keys = key_converter
            .convert_columns(&[Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        let partition = keys.row(0).data().to_vec();
        let raw: Vec<ArrayRef> = vec![
            Arc::new(arrow::array::Float64Array::from(vec![
                f64::from_bits(0x7ff8000000000001),
                f64::from_bits(0x7ff8000000000002),
                -0.0,
                0.0,
                1.0,
            ])),
            Arc::new(arrow::array::Float32Array::from(vec![
                f32::from_bits(0x7fc00001),
                f32::from_bits(0x7fc00002),
                -0.0,
                0.0,
                1.0,
            ])),
        ];
        for (slot, values) in raw.iter().enumerate() {
            let encoded = store.distinct_converters[slot]
                .convert_columns(std::slice::from_ref(values))
                .unwrap();
            for row in encoded.iter() {
                let mut key = store.distinct_prefix(slot, 1, &partition);
                key.extend_from_slice(row.data());
                store.db.put(key, []).unwrap();
            }
        }
        let fold = store.fold_key(1, &partition);
        store
            .write_folds(
                &[(fold.clone(), 100)],
                &[Arc::new(Int64Array::from(vec![4]))],
            )
            .unwrap();
        let original = store.db.get(&fold).unwrap().unwrap();
        let checkpoint = directory.path().join("checkpoint");
        let saved = store.checkpoint(0, checkpoint.to_str().unwrap()).unwrap();
        drop(store);
        let mut restored = RocksOverAggStore::open_merged(
            config(&directory.path().join("target")),
            &[DataType::Int64],
            &[],
            &types,
            schema(),
            0..=7,
            &[(checkpoint.to_str().unwrap().into(), saved.snapshot_id)],
            aligned,
        )
        .unwrap();
        assert_eq!(restored.db.get(&fold).unwrap().unwrap(), original);
        for slot in 0..2 {
            let values = restored.scan_distinct(slot, 1, &partition).unwrap();
            assert_eq!(values.len(), 4);
            let later = if slot == 0 {
                ScalarValue::Float64(Some(f64::from_bits(0x7ff8000000000003)))
            } else {
                ScalarValue::Float32(Some(f32::from_bits(0x7fc00003)))
            };
            let encoded = restored
                .encode_distinct_elements(slot, &later.to_array().unwrap())
                .unwrap();
            let mut key = restored.distinct_prefix(slot, 1, &partition);
            key.extend_from_slice(&encoded[0]);
            assert_eq!(restored.probe_distinct(&[key]).unwrap(), [true]);
        }
        let before = restored
            .db
            .iterator(IteratorMode::Start)
            .map(|row| row.unwrap())
            .collect::<Vec<_>>();
        restored.canonicalize_floating_distinct().unwrap();
        assert_eq!(
            before,
            restored
                .db
                .iterator(IteratorMode::Start)
                .map(|row| row.unwrap())
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn membership_visitors_decode_composite_variable_keys_without_accumulator_rows() {
    for keyless in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let key_types = if keyless {
            vec![]
        } else {
            vec![DataType::Utf8, DataType::Decimal128(20, 2)]
        };
        let mut store = RocksOverAggStore::create(
            config(directory.path()),
            &[DataType::Int64],
            &[],
            &[DataType::Utf8],
            schema(),
            0..=7,
        )
        .unwrap();
        let columns: Vec<ArrayRef> = if keyless {
            vec![]
        } else {
            vec![
                Arc::new(StringArray::from(vec![Some("a\0longé"), None])),
                Arc::new(
                    Decimal128Array::from(vec![Some(123), None])
                        .with_precision_and_scale(20, 2)
                        .unwrap(),
                ),
            ]
        };
        let converter = key_row_converter_from_types(&key_types);
        let keys = encode_group_keys(&converter, &columns, if keyless { 1 } else { 2 });
        let values: ArrayRef = Arc::new(StringArray::from(vec!["hello", "", "long\0é世界"]));
        let encoded = store.encode_distinct_elements(0, &values).unwrap();
        let mut writes = Vec::new();
        let mut expected = Vec::new();
        for key in keys.iter() {
            for (i, value) in encoded.iter().enumerate() {
                let mut entry = store.distinct_prefix(0, 1, key.data());
                entry.extend(value);
                writes.push(entry);
                expected.push((
                    key.data().to_vec(),
                    ScalarValue::try_from_array(&values, i).unwrap().to_string(),
                ));
            }
        }
        store.write_distinct(&writes).unwrap();
        assert!(store.scan_folds().unwrap().is_empty());
        let mut actual = Vec::new();
        store
            .visit_all_distinct(0, &key_types, 2, |members| {
                assert!(members.len() <= 2);
                actual.extend(
                    members
                        .into_iter()
                        .map(|(key, value)| (key.to_vec(), value.to_string())),
                );
                Ok(())
            })
            .unwrap();
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    }
}

#[test]
fn wide_membership_visitors_bound_bytes_retry_and_cancel_within_one_partition() {
    let directory = tempfile::tempdir().unwrap();
    let mut store = RocksOverAggStore::create(
        config(directory.path()),
        &[],
        &[],
        &[DataType::Utf8],
        schema(),
        0..=7,
    )
    .unwrap();
    let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
    let keys = converter
        .convert_columns(&[Arc::new(Int64Array::from(vec![7]))])
        .unwrap();
    let key = keys.row(0).data();
    let prefix = store.distinct_prefix(0, 1, key);
    let values: ArrayRef = Arc::new(arrow::array::StringArray::from(
        (0..3)
            .map(|i| format!("{}{}", "x".repeat(2 * 1024 * 1024), i))
            .collect::<Vec<_>>(),
    ));
    let entries = store
        .encode_distinct_elements(0, &values)
        .unwrap()
        .into_iter()
        .map(|element| {
            let mut key = prefix.clone();
            key.extend(element);
            key
        })
        .collect::<Vec<_>>();
    store.write_distinct(&entries).unwrap();
    let mut sizes = Vec::new();
    store
        .visit_distinct(0, 1, key, 1024, |rows| {
            sizes.push(rows.len());
            Ok(())
        })
        .unwrap();
    assert_eq!(sizes, [2, 1]);
    assert!(store
        .visit_all_distinct(0, &[DataType::Int64], 1024, |_| Err(
            DataFusionError::Execution("receiver failed".into())
        ))
        .is_err());
    let mut sizes = Vec::new();
    store
        .visit_all_distinct(0, &[DataType::Int64], 1024, |rows| {
            sizes.push(rows.len());
            Ok(())
        })
        .unwrap();
    assert_eq!(sizes, [1, 1, 1]);
    let context = crate::state::canonical_context::CanonicalContext::new(Default::default());
    let mut calls = 0;
    let result = context.with(|| {
        store.visit_distinct(0, 1, key, 1, |_| {
            calls += 1;
            context.cancel();
            Ok(())
        })
    });
    assert!(result.is_err());
    assert_eq!(calls, 1);
}

#[test]
fn tagged_polling_ledgers_and_floating_memberships_survive_native_database_restore() {
    for aligned in [false, true] {
        for legacy in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let types = if legacy {
                vec![DataType::Float64]
            } else {
                vec![DataType::Float64, DataType::Float32]
            };
            let mut store = RocksOverAggStore::create(
                config(&directory.path().join("source")),
                &[DataType::Int64],
                &[],
                &types,
                schema(),
                0..=7,
            )
            .unwrap();
            let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
            let owners = converter
                .convert_columns(&[Arc::new(Int64Array::from(vec![7]))])
                .unwrap();
            let key = owners.row(0).data();
            let mut timer_key = store.range_timer_key(1, key);
            if legacy {
                timer_key.remove(KEY_PREFIX_LEN);
            }
            let mut timer_value = vec![1, 0];
            timer_value.extend_from_slice(&0_i64.to_le_bytes());
            timer_value.extend_from_slice(&11_i64.to_le_bytes());
            store.db.put(&timer_key, &timer_value).unwrap();
            for (slot, ty) in types.iter().enumerate() {
                let values: ArrayRef = match ty {
                    DataType::Float64 => {
                        Arc::new(arrow::array::Float64Array::from(vec![f64::NAN, 1.0]))
                    }
                    _ => Arc::new(arrow::array::Float32Array::from(vec![f32::NAN, 1.0])),
                };
                let prefix = store.distinct_prefix(slot, 1, key);
                let entries = store
                    .encode_distinct_elements(slot, &values)
                    .unwrap()
                    .into_iter()
                    .map(|row| {
                        let mut out = prefix.clone();
                        out.extend(row);
                        out
                    })
                    .collect::<Vec<_>>();
                store.write_distinct(&entries).unwrap();
            }
            let snapshot = directory.path().join("checkpoint");
            let manifest = store.checkpoint(10, snapshot.to_str().unwrap()).unwrap();
            let recovered = RocksOverAggStore::open_merged(
                config(&directory.path().join("recovered")),
                &[DataType::Int64],
                &[],
                &types,
                schema(),
                0..=7,
                &[(snapshot.to_str().unwrap().into(), manifest.snapshot_id)],
                aligned,
            )
            .unwrap();
            assert_eq!(
                recovered.scan_range_timers().unwrap(),
                vec![(key.into(), timer_value.clone())]
            );
            assert_eq!(
                recovered.range_timer_state(1, key).unwrap(),
                Some(timer_value)
            );
            for slot in 0..types.len() {
                assert_eq!(recovered.scan_distinct(slot, 1, key).unwrap().len(), 2);
                let mut count = 0;
                recovered
                    .visit_all_distinct(slot, &[DataType::Int64], 1, |rows| {
                        count += rows.len();
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(count, 2);
            }
        }
    }
}
