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
