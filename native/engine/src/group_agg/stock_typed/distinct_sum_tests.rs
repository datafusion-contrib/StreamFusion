use super::*;

#[cfg(feature = "rocksdb-state")]
use crate::state::RocksStoreConfig;

enum Owner {
    Memory(GroupAggregator),
    #[cfg(feature = "rocksdb-state")]
    Rocks(GroupAggregator<RocksGroupStore>, tempfile::TempDir),
}

impl Owner {
    fn new(rocks: bool, code: i64) -> Self {
        let kinds = vec![7, 9];
        let mut owner =
            GroupAggregator::new(kinds.clone(), vec![code, code], vec![1, 1], vec![0], true)
                .with_key_timestamp_precisions(vec![i32::MIN, -1])
                .with_memory_budget(16 << 20)
                .unwrap();
        owner.configure_typed(&kinds, true).unwrap();
        if rocks {
            #[cfg(feature = "rocksdb-state")]
            {
                let directory = tempfile::tempdir().unwrap();
                let store = RocksGroupStore::create(
                    RocksStoreConfig {
                        table_dir: directory
                            .path()
                            .join("state")
                            .to_string_lossy()
                            .into_owned(),
                        max_parallelism: 128,
                        options_json: include_str!("../../../benches/fixtures/rocks-options.json")
                            .into(),
                        ttl_ms: 0,
                        shared_resources: 0,
                    },
                    GroupStateCodec::new(kinds, vec![value_data_type(code); 2], vec![1, 1], vec![]),
                )
                .unwrap();
                return Self::Rocks(owner.with_backend(store), directory);
            }
        }
        Self::Memory(owner)
    }

    fn recovered(self, code: i64) -> Self {
        match self {
            Self::Memory(mut owner) => {
                let bytes = owner.snapshot();
                let mut restored = GroupAggregator::restore(
                    vec![7, 9],
                    vec![code, code],
                    vec![1, 1],
                    vec![0],
                    true,
                    &bytes,
                    0,
                )
                .with_key_timestamp_precisions(vec![i32::MIN, -1]);
                assert!(!restored.stock_distinct_counters);
                restored.configure_typed(&[7, 9], true).unwrap();
                Self::Memory(restored)
            }
            #[cfg(feature = "rocksdb-state")]
            Self::Rocks(mut owner, directory) => {
                let snapshot = directory
                    .path()
                    .join("checkpoint")
                    .to_string_lossy()
                    .into_owned();
                let manifest = owner.store_mut().checkpoint(&snapshot).unwrap();
                drop(owner);
                let store = RocksGroupStore::open_merged(
                    RocksStoreConfig {
                        table_dir: directory
                            .path()
                            .join("restored")
                            .to_string_lossy()
                            .into_owned(),
                        max_parallelism: 128,
                        options_json: include_str!("../../../benches/fixtures/rocks-options.json")
                            .into(),
                        ttl_ms: 0,
                        shared_resources: 0,
                    },
                    GroupStateCodec::new(
                        vec![7, 9],
                        vec![value_data_type(code); 2],
                        vec![1, 1],
                        vec![],
                    ),
                    &[(snapshot, manifest.snapshot_id)],
                    0..=127,
                    true,
                    0,
                )
                .unwrap();
                let mut restored =
                    GroupAggregator::new(vec![7, 9], vec![code, code], vec![1, 1], vec![0], true)
                        .with_key_timestamp_precisions(vec![i32::MIN, -1])
                        .with_backend(store);
                restored.configure_typed(&[7, 9], true).unwrap();
                Self::Rocks(restored, directory)
            }
        }
    }

    fn import(&mut self, slot: Option<usize>, batch: &RecordBatch) -> Result<(), DataFusionError> {
        match self {
            Self::Memory(owner) => match slot {
                None => owner.transfer_typed_headers(batch, false),
                Some(slot) => owner.transfer_typed_entries(slot, batch, false),
            },
            #[cfg(feature = "rocksdb-state")]
            Self::Rocks(owner, _) => match slot {
                None => owner.transfer_typed_headers(batch, false),
                Some(slot) => owner.transfer_typed_entries(slot, batch, false),
            },
        }
    }

    fn frames(&mut self, slot: Option<usize>) -> Vec<RecordBatch> {
        let mut frames = vec![];
        let mut receiver = |frame| {
            frames.push(frame);
            Ok(())
        };
        match self {
            Self::Memory(owner) => owner.visit_typed(slot, &mut receiver).unwrap(),
            #[cfg(feature = "rocksdb-state")]
            Self::Rocks(owner, _) => owner.visit_typed(slot, &mut receiver).unwrap(),
        }
        frames
    }

    fn update(&mut self, values: ArrayRef) {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "key",
                Arc::new(Int64Array::from(vec![1; values.len()])) as ArrayRef,
            ),
            ("value", values),
        ])
        .unwrap();
        match self {
            Self::Memory(owner) => {
                owner.update(&batch, 0).unwrap();
            }
            #[cfg(feature = "rocksdb-state")]
            Self::Rocks(owner, _) => {
                owner.update(&batch, 0).unwrap();
            }
        }
    }
}

fn stores() -> Vec<bool> {
    #[cfg(feature = "rocksdb-state")]
    {
        vec![false, true]
    }
    #[cfg(not(feature = "rocksdb-state"))]
    {
        vec![false]
    }
}

fn key() -> Vec<u8> {
    let mut key = vec![0; 16];
    key[8..].copy_from_slice(&1_i64.to_le_bytes());
    key
}

fn header(count: i64, sum: ScalarValue, initialized: i64) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(BinaryArray::from_iter_values([key()])) as ArrayRef,
        ),
        ("records", Arc::new(Int64Array::from(vec![1]))),
        ("written", Arc::new(Int64Array::from(vec![0]))),
        ("count", Arc::new(Int64Array::from(vec![count]))),
        ("unused", Arc::new(Int64Array::from(vec![0]))),
        ("sum", sum.to_array().unwrap()),
        ("initialized", Arc::new(Int64Array::from(vec![initialized]))),
    ])
    .unwrap()
}

fn members(values: ArrayRef) -> RecordBatch {
    let size = values.len();
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(BinaryArray::from_iter_values((0..size).map(|_| key()))) as ArrayRef,
        ),
        ("value", values),
        ("count", Arc::new(Int64Array::from(vec![1; size]))),
    ])
    .unwrap()
}

fn scalars(owner: &mut Owner) -> (i64, ScalarValue) {
    let frames = owner.frames(None);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].num_rows(), 1);
    let count = frames[0]
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    (
        count,
        ScalarValue::try_from_array(frames[0].column(5), 0).unwrap(),
    )
}

#[test]
fn sum_distinct_keeps_raw_scalar_independent_of_shared_membership_and_null_markers() {
    for rocks in stores() {
        for initial in [Some(123), None] {
            let mut source = Owner::new(rocks, 2);
            source
                .import(
                    None,
                    &header(
                        99,
                        ScalarValue::Int32(initial),
                        i64::from(initial.is_some()),
                    ),
                )
                .unwrap();
            let membership = members(Arc::new(Int32Array::from(vec![Some(2), Some(3), None])));
            for slot in [0, 1] {
                source.import(Some(slot), &membership).unwrap();
            }
            assert_eq!(scalars(&mut source), (99, ScalarValue::Int32(initial)));
            let mut target = Owner::new(rocks, 2);
            for slot in [None, Some(0), Some(1)] {
                for batch in source.frames(slot) {
                    target.import(slot, &batch).unwrap();
                }
            }
            target.update(Arc::new(Int32Array::from(vec![Some(2), None])));
            assert_eq!(scalars(&mut target), (99, ScalarValue::Int32(initial)));
            target.update(Arc::new(Int32Array::from(vec![Some(4), Some(4), None])));
            assert_eq!(
                scalars(&mut target),
                (100, ScalarValue::Int32(Some(initial.unwrap_or(0) + 4)))
            );
            assert_eq!(
                source.frames(Some(1))[0].num_rows(),
                3,
                "capture does not drain source"
            );
        }
    }
}

#[test]
fn sum_distinct_and_count_wrap_like_java_without_refolding_imported_members() {
    for rocks in stores() {
        let mut owner = Owner::new(rocks, 2);
        owner
            .import(
                None,
                &header(i64::MAX, ScalarValue::Int32(Some(i32::MAX)), 1),
            )
            .unwrap();
        let membership = members(Arc::new(Int32Array::from(vec![2])));
        for slot in [0, 1] {
            owner.import(Some(slot), &membership).unwrap();
        }
        owner.update(Arc::new(Int32Array::from(vec![1, 1])));
        assert_eq!(
            scalars(&mut owner),
            (i64::MIN, ScalarValue::Int32(Some(i32::MIN)))
        );
    }
}

#[test]
fn sum_distinct_canonical_preserves_nan_payload_and_signed_zero() {
    for rocks in stores() {
        for bits in [0x7ff8_0000_0000_0042, (-0.0f64).to_bits()] {
            let mut source = Owner::new(rocks, 1);
            source
                .import(
                    None,
                    &header(7, ScalarValue::Float64(Some(f64::from_bits(bits))), 1),
                )
                .unwrap();
            let membership = members(Arc::new(arrow::array::Float64Array::from(vec![2.0])));
            for slot in [0, 1] {
                source.import(Some(slot), &membership).unwrap();
            }
            let mut target = Owner::new(rocks, 1);
            for slot in [None, Some(0), Some(1)] {
                for batch in source.frames(slot) {
                    target.import(slot, &batch).unwrap();
                }
            }
            target.update(Arc::new(arrow::array::Float64Array::from(vec![2.0])));
            let (_, ScalarValue::Float64(Some(value))) = scalars(&mut target) else {
                panic!("double sum");
            };
            assert_eq!(value.to_bits(), bits);
        }
    }
}

#[test]
fn sum_distinct_native_checkpoint_keeps_independent_scalar_and_membership_policy() {
    for rocks in stores() {
        let mut source = Owner::new(rocks, 2);
        source
            .import(None, &header(99, ScalarValue::Int32(Some(123)), 1))
            .unwrap();
        let values = members(Arc::new(Int32Array::from(vec![Some(2), None])));
        for slot in [0, 1] {
            source.import(Some(slot), &values).unwrap();
        }
        let mut restored = source.recovered(2);
        restored.update(Arc::new(Int32Array::from(vec![
            Some(2),
            None,
            Some(4),
            Some(4),
        ])));
        assert_eq!(scalars(&mut restored), (100, ScalarValue::Int32(Some(127))));
        assert_eq!(restored.frames(Some(1))[0].num_rows(), 3);
    }
}

#[test]
fn sum_distinct_invalid_initialization_and_duplicate_membership_do_not_mutate_state() {
    for rocks in stores() {
        let mut owner = Owner::new(rocks, 2);
        for (sum, present) in [(None, 1), (Some(123), 0), (Some(123), 2), (Some(123), -1)] {
            assert!(owner
                .import(None, &header(99, ScalarValue::Int32(sum), present))
                .is_err());
            assert!(owner.frames(None).is_empty());
        }
        owner
            .import(None, &header(99, ScalarValue::Int32(Some(123)), 1))
            .unwrap();
        let values = members(Arc::new(Int32Array::from(vec![2])));
        owner.import(Some(1), &values).unwrap();
        assert!(owner.import(Some(1), &values).is_err());
        assert_eq!(scalars(&mut owner), (99, ScalarValue::Int32(Some(123))));
        assert_eq!(owner.frames(Some(1))[0].num_rows(), 1);
    }
}

#[test]
fn sum_distinct_retractions_and_independent_entry_ttl_require_their_own_contracts() {
    let make = || GroupAggregator::new(vec![9], vec![2], vec![1], vec![0], true);
    assert!(make().configure_typed(&[9], false).is_err());
    assert!(make()
        .with_state_ttl(10)
        .configure_typed(&[9], true)
        .is_err());
}
