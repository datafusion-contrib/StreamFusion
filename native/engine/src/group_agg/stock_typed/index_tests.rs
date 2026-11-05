use super::*;

#[cfg(feature = "rocksdb-state")]
#[test]
fn companion_table_admission_cannot_wrap_into_the_main_table() {
    use crate::state::RocksStateCodec;
    for kind in [1, 2, 7, 9] {
        for slot in [254, 255, 256, 511] {
            let mut kinds = vec![3; slot + 1];
            kinds[slot] = kind;
            let codec = GroupStateCodec::new(
                kinds,
                vec![DataType::Int64; slot + 1],
                vec![1; slot + 1],
                vec![],
            );
            assert_eq!(codec.supported(), slot < 255, "kind={kind},slot={slot}");
            if slot < 255 {
                assert_eq!(codec.multiset_tables(), vec![(255, DataType::Int64)]);
            }
        }
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn scalar_only_wide_accumulators_do_not_need_companion_ids() {
    use crate::state::RocksStateCodec;
    let codec = GroupStateCodec::new(
        vec![3; 512],
        vec![DataType::Int64; 512],
        vec![1; 512],
        vec![],
    );
    assert!(codec.supported());
    assert!(codec.multiset_tables().is_empty());
}

#[test]
fn direct_typed_binding_rejects_unencodable_distinct_slots() {
    for slot in [254, 255, 256] {
        let mut kinds = vec![3; slot + 1];
        kinds[slot] = 7;
        let mut owner = GroupAggregator::new(
            kinds.clone(),
            vec![0; slot + 1],
            vec![1; slot + 1],
            vec![0],
            true,
        );
        assert_eq!(
            owner.configure_typed(&kinds, true).is_ok(),
            slot < 255,
            "slot={slot}"
        );
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn last_companion_table_roundtrips_actual_rocks_values() {
    use crate::state::RocksStoreConfig;
    let directory = tempfile::tempdir().unwrap();
    let mut kinds = vec![3; 255];
    kinds[254] = 7;
    let store = RocksGroupStore::create(
        RocksStoreConfig {
            table_dir: directory
                .path()
                .join("state")
                .to_string_lossy()
                .into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        },
        GroupStateCodec::new(
            kinds.clone(),
            vec![DataType::Int64; 255],
            vec![1; 255],
            vec![],
        ),
    )
    .unwrap();
    let mut owner = GroupAggregator::new(kinds.clone(), vec![0; 255], vec![1; 255], vec![0], true)
        .with_key_timestamp_precisions(vec![i32::MIN, -1])
        .with_backend(store);
    owner.configure_typed(&kinds, true).unwrap();
    owner
        .update(
            &RecordBatch::try_from_iter(vec![
                ("key", Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef),
                (
                    "value",
                    Arc::new(Int64Array::from(vec![Some(42), Some(42), None])),
                ),
            ])
            .unwrap(),
            0,
        )
        .unwrap();
    let mut members = Vec::new();
    owner
        .visit_typed(Some(254), |frame| {
            members.push(frame);
            Ok(())
        })
        .unwrap();
    assert_eq!(members.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert!(members
        .iter()
        .any(|frame| frame.column(1).null_count() == 1));
    let mut header = Vec::new();
    owner
        .visit_typed(None, |frame| {
            header.push(frame);
            Ok(())
        })
        .unwrap();
    assert_eq!(header[0].num_columns(), 513);
    assert_eq!(
        ScalarValue::try_from_array(header[0].column(511), 0).unwrap(),
        ScalarValue::Int64(Some(1))
    );
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn invalid_companion_shape_is_rejected_before_creating_a_database() {
    use crate::state::RocksStoreConfig;
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("state");
    let mut kinds = vec![3; 256];
    kinds[255] = 7;
    let result = RocksGroupStore::create(
        RocksStoreConfig {
            table_dir: state.to_string_lossy().into_owned(),
            max_parallelism: 128,
            options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        },
        GroupStateCodec::new(kinds, vec![DataType::Int64; 256], vec![1; 256], vec![]),
    );
    assert!(result.is_err());
    assert!(!state.exists());
}
