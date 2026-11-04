use super::*;

#[cfg(feature = "rocksdb-state")]
use crate::state::RocksStoreConfig;

enum Owner {
    Memory(GroupAggregator),
    #[cfg(feature = "rocksdb-state")]
    Rocks(GroupAggregator<RocksGroupStore>, tempfile::TempDir),
}
impl Owner {
    fn new(rocks: bool, append: bool) -> Self {
        let kinds = vec![3, 0, 7];
        let mut native = GroupAggregator::new(
            kinds.clone(),
            vec![0, 2, 3],
            vec![-1, 3, 4],
            vec![0, 1, 2],
            true,
        )
        .with_key_timestamp_precisions(vec![i32::MIN, -1, 3, 3]);
        native.configure_typed(&kinds, append).unwrap();
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
                    GroupStateCodec::new(
                        kinds,
                        vec![DataType::Int64, DataType::Int32, DataType::Utf8],
                        vec![-1, 3, 4],
                        vec![],
                    ),
                )
                .unwrap();
                return Self::Rocks(native.with_backend(store), directory);
            }
        }
        Self::Memory(native)
    }
    fn recovered(self, append: bool) -> Self {
        match self {
            Self::Memory(mut owner) => {
                let snapshot = owner.snapshot();
                let mut restored = GroupAggregator::restore(
                    vec![3, 0, 7],
                    vec![0, 2, 3],
                    vec![-1, 3, 4],
                    vec![0, 1, 2],
                    true,
                    &snapshot,
                    0,
                )
                .with_key_timestamp_precisions(vec![i32::MIN, -1, 3, 3]);
                assert!(!restored.stock_distinct_counters);
                restored.configure_typed(&[3, 0, 7], append).unwrap();
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
                        vec![3, 0, 7],
                        vec![DataType::Int64, DataType::Int32, DataType::Utf8],
                        vec![-1, 3, 4],
                        vec![],
                    ),
                    &[(snapshot, manifest.snapshot_id)],
                    0..=127,
                    true,
                    0,
                )
                .unwrap();
                let mut restored = GroupAggregator::new(
                    vec![3, 0, 7],
                    vec![0, 2, 3],
                    vec![-1, 3, 4],
                    vec![0, 1, 2],
                    true,
                )
                .with_key_timestamp_precisions(vec![i32::MIN, -1, 3, 3])
                .with_backend(store);
                restored.configure_typed(&[3, 0, 7], append).unwrap();
                Self::Rocks(restored, directory)
            }
        }
    }

    fn update(&mut self, batch: &RecordBatch) -> RecordBatch {
        match self {
            Self::Memory(owner) => owner.update(batch, 0).unwrap(),
            #[cfg(feature = "rocksdb-state")]
            Self::Rocks(owner, _) => owner.update(batch, 0).unwrap(),
        }
    }
    fn frames(&mut self, slot: Option<usize>) -> Vec<RecordBatch> {
        let mut frames = vec![];
        match self {
            Self::Memory(owner) => owner
                .visit_typed(slot, |frame| {
                    frames.push(frame);
                    Ok(())
                })
                .unwrap(),
            #[cfg(feature = "rocksdb-state")]
            Self::Rocks(owner, _) => owner
                .visit_typed(slot, |frame| {
                    frames.push(frame);
                    Ok(())
                })
                .unwrap(),
        }
        frames
    }
    fn import(
        &mut self,
        slot: Option<usize>,
        batch: &RecordBatch,
        validating: bool,
    ) -> Result<(), DataFusionError> {
        match self {
            Self::Memory(owner) => match slot {
                None => owner.transfer_typed_headers(batch, validating),
                Some(slot) => owner.transfer_typed_entries(slot, batch, validating),
            },
            #[cfg(feature = "rocksdb-state")]
            Self::Rocks(owner, _) => match slot {
                None => owner.transfer_typed_headers(batch, validating),
                Some(slot) => owner.transfer_typed_entries(slot, batch, validating),
            },
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
fn input(name: Option<&str>, value: Option<i32>, member: Option<&str>, kind: i8) -> RecordBatch {
    use streamfusion_bridge::timestamp::{timestamp_array, TimestampValue};
    RecordBatch::try_from_iter(vec![
        (
            "name",
            Arc::new(arrow::array::StringArray::from(vec![name])) as ArrayRef,
        ),
        (
            "start",
            Arc::new(timestamp_array([Some(TimestampValue::from_millis(-1000))])),
        ),
        (
            "end",
            Arc::new(timestamp_array([Some(TimestampValue::from_millis(4000))])),
        ),
        ("value", Arc::new(Int32Array::from(vec![value]))),
        (
            "member",
            Arc::new(arrow::array::StringArray::from(vec![member])),
        ),
        (ROW_KIND_COLUMN, Arc::new(Int8Array::from(vec![kind]))),
    ])
    .unwrap()
}
fn seed(owner: &mut Owner) {
    for (name, value, member) in [
        (Some("a"), Some(7), Some("x")),
        (Some("a"), Some(9), Some("x")),
        (Some("a"), None, Some("y")),
        (None, Some(-3), None),
    ] {
        owner.update(&input(name, value, member, 0));
    }
}
fn copy(source: &mut Owner, target: &mut Owner) {
    let headers = source.frames(None);
    let entries = source.frames(Some(2));
    for batch in &headers {
        target.import(None, batch, true).unwrap();
    }
    for batch in &entries {
        target.import(Some(2), batch, true).unwrap();
    }
    assert!(
        target.frames(None).is_empty(),
        "validation must not install header state"
    );
    for batch in &headers {
        target.import(None, batch, false).unwrap();
    }
    for batch in &entries {
        target.import(Some(2), batch, false).unwrap();
    }
}
fn outputs(batch: &RecordBatch) -> Vec<String> {
    (0..batch.num_rows())
        .map(|row| {
            (0..batch.num_columns())
                .map(|column| {
                    ScalarValue::try_from_array(batch.column(column), row)
                        .unwrap()
                        .to_string()
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}
fn raw(mut frames: Vec<RecordBatch>) -> Vec<String> {
    let mut rows = frames
        .drain(..)
        .flat_map(|frame| outputs(&frame))
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn typed_int_and_string_distinct_continue_after_all_memory_rocks_handoffs() {
    for source_store in stores() {
        for target_store in stores() {
            for append in [false, true] {
                let mut source = Owner::new(source_store, append);
                seed(&mut source);
                let mut target = Owner::new(target_store, append);
                copy(&mut source, &mut target);
                assert_eq!(raw(source.frames(None)), raw(target.frames(None)));
                assert_eq!(raw(source.frames(Some(2))), raw(target.frames(Some(2))));
                let mut returned = Owner::new(source_store, append);
                copy(&mut target, &mut returned);
                let kind = if append { 0 } else { 3 };
                for batch in [
                    input(Some("a"), Some(7), Some("x"), kind),
                    input(Some("a"), Some(9), Some("x"), kind),
                    input(Some("a"), Some(5), Some("z"), 0),
                    input(None, Some(12), Some(""), 0),
                ] {
                    let expected = outputs(&source.update(&batch));
                    assert_eq!(
                        expected,
                        outputs(&target.update(&batch)),
                        "target store={target_store}, append={append}"
                    );
                    assert_eq!(
                        expected,
                        outputs(&returned.update(&batch)),
                        "no-input returned state"
                    );
                }
            }
        }
    }
}

#[test]
fn late_invalid_headers_and_memberships_fail_preflight_without_installing_groups() {
    for rocks in stores() {
        let mut source = Owner::new(rocks, false);
        seed(&mut source);
        let mut target = Owner::new(rocks, false);
        let header = source.frames(None).pop().unwrap();
        target.import(None, &header, true).unwrap();
        let mut columns = header.columns().to_vec();
        columns[5] = Arc::new(arrow::array::StringArray::from(vec![
            Some("wrong");
            header.num_rows()
        ]));
        let fields = header
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == 5 {
                    Arc::new(Field::new("wrong", DataType::Utf8, true))
                } else {
                    f.clone()
                }
            })
            .collect::<Vec<_>>();
        let malformed = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        assert!(target.import(None, &malformed, true).is_err());
        assert!(target.frames(None).is_empty());
        let entry = source.frames(Some(2)).pop().unwrap();
        assert!(target.import(Some(0), &entry, true).is_err());
        assert!(target.frames(None).is_empty());
        target.import(None, &header, false).unwrap();
        assert!(target.import(None, &header, false).is_err());
        target.import(Some(2), &entry, false).unwrap();
        assert!(target.import(Some(2), &entry, false).is_err());
    }
}

#[test]
fn invalid_view_transfer_cannot_install_an_orphan_or_duplicate_membership() {
    for rocks in stores() {
        let mut source = Owner::new(rocks, false);
        seed(&mut source);
        let mut target = Owner::new(rocks, false);
        let entry = source.frames(Some(2)).pop().unwrap();
        assert!(target.import(Some(2), &entry, false).is_err());
        let mut columns = entry.columns().to_vec();
        columns[1] = Arc::new(arrow::array::StringArray::from(vec![
            None::<&str>;
            entry.num_rows()
        ]));
        let fields = entry
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                if i == 1 {
                    Arc::new(field.as_ref().clone().with_nullable(true))
                } else {
                    field.clone()
                }
            })
            .collect::<Vec<_>>();
        let nulls = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        assert!(target.import(Some(2), &nulls, true).is_err());
        assert!(target.frames(None).is_empty());
    }
}

#[test]
fn refused_receiver_does_not_drain_memory_or_persistent_distinct_state() {
    for rocks in stores() {
        let mut source = Owner::new(rocks, false);
        seed(&mut source);
        let before = raw(source.frames(Some(2)));
        let result = match &mut source {
            Owner::Memory(owner) => {
                owner.visit_typed(Some(2), |_| Err(invalid("receiver sentinel")))
            }
            #[cfg(feature = "rocksdb-state")]
            Owner::Rocks(owner, _) => {
                owner.visit_typed(Some(2), |_| Err(invalid("receiver sentinel")))
            }
        };
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("receiver sentinel"));
        assert_eq!(before, raw(source.frames(Some(2))));
    }
}

#[test]
fn stock_distinct_preserves_independent_scalars_and_signed_counter_transitions() {
    for rocks in stores() {
        for initial in [i64::MIN, -2, -1, 0, 1, i64::MAX] {
            let mut source = Owner::new(false, false);
            seed(&mut source);
            let first = source.frames(None).pop().unwrap().slice(0, 1);
            let key = first.column(0).clone();
            let mut columns = first.columns().to_vec();
            for index in [1, 3, 4] {
                columns[index] = Arc::new(Int64Array::from(vec![100]));
            }
            columns[7] = Arc::new(Int64Array::from(vec![42]));
            let header = RecordBatch::try_new(first.schema(), columns).unwrap();
            let frame = RecordBatch::try_from_iter(vec![
                ("key", key),
                (
                    "value",
                    Arc::new(arrow::array::StringArray::from(vec!["edge"])) as ArrayRef,
                ),
                ("count", Arc::new(Int64Array::from(vec![initial]))),
            ])
            .unwrap();
            let mut target = Owner::new(rocks, false);
            target.import(None, &header, false).unwrap();
            target.import(Some(2), &frame, false).unwrap();
            assert_eq!(raw(target.frames(Some(2))), raw(vec![frame]));
            let mut counter = Some(initial);
            let mut scalar = 42i64;
            // Decode the imported binary key's name so all updates hit exactly that group.
            let bytes = header
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0);
            let source_name = if bytes[1] & 1 != 0 { None } else { Some("a") };
            for retract in [false, true, true, false] {
                let next = if retract {
                    counter.unwrap_or(0).wrapping_sub(1)
                } else {
                    counter.unwrap_or(0).wrapping_add(1)
                };
                if retract && next == 0 {
                    scalar = scalar.wrapping_sub(1);
                }
                if !retract && next == 1 {
                    scalar = scalar.wrapping_add(1);
                }
                counter = (next != 0).then_some(next);
                target.update(&input(
                    source_name,
                    None,
                    Some("edge"),
                    if retract { 3 } else { 0 },
                ));
                let actual = target.frames(None);
                let raw = actual
                    .iter()
                    .flat_map(|frame| {
                        (0..frame.num_rows()).map(move |row| {
                            frame
                                .column(7)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(row)
                        })
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    raw,
                    vec![scalar],
                    "initial={initial}, rocks={rocks}, retract={retract}"
                );
                let members = target.frames(Some(2));
                let actual = members
                    .iter()
                    .flat_map(|frame| {
                        (0..frame.num_rows()).map(move |row| {
                            frame
                                .column(2)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(row)
                        })
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual, counter.into_iter().collect::<Vec<_>>());
            }
        }
    }
}

#[test]
fn native_checkpoint_recovery_rebinds_stock_counter_policy_before_canonical_return() {
    for rocks in stores() {
        for append in [false, true] {
            let mut reference = Owner::new(false, append);
            seed(&mut reference);
            let mut owner = Owner::new(rocks, append);
            copy(&mut reference, &mut owner);
            owner = owner.recovered(append);
            let mut returned = Owner::new(false, append);
            copy(&mut owner, &mut returned);
            for batch in [
                input(Some("a"), Some(7), Some("x"), if append { 0 } else { 3 }),
                input(Some("a"), Some(5), Some("z"), 0),
            ] {
                let expected = outputs(&reference.update(&batch));
                assert_eq!(expected, outputs(&owner.update(&batch)));
                assert_eq!(expected, outputs(&returned.update(&batch)));
            }
        }
    }
}

#[test]
fn null_markers_preserve_counts_without_contributing_to_the_distinct_scalar() {
    for source_store in stores() {
        for target_store in stores() {
            for append in [false, true] {
                let mut source = Owner::new(source_store, append);
                source.update(&input(Some("a"), Some(1), Some("x"), 0));
                source.update(&input(Some("a"), Some(2), None, 0));
                source.update(&input(Some("a"), Some(3), None, 0));
                let frames = source.frames(Some(2));
                let mut nulls = 0;
                for frame in &frames {
                    let counts = frame
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for row in 0..frame.num_rows() {
                        if frame.column(1).is_null(row) {
                            nulls += 1;
                            assert_eq!(counts.value(row), if append { 1 } else { 2 });
                        }
                    }
                }
                assert_eq!(nulls, 1);
                let mut target = Owner::new(target_store, append);
                copy(&mut source, &mut target);
                let mut target = target.recovered(append);
                assert_eq!(raw(source.frames(None)), raw(target.frames(None)));
                assert_eq!(raw(source.frames(Some(2))), raw(target.frames(Some(2))));
                for batch in [
                    input(Some("a"), Some(2), None, if append { 0 } else { 3 }),
                    input(Some("a"), Some(3), None, if append { 0 } else { 3 }),
                ] {
                    assert_eq!(
                        outputs(&source.update(&batch)),
                        outputs(&target.update(&batch))
                    );
                    assert_eq!(raw(source.frames(None)), raw(target.frames(None)));
                    assert_eq!(raw(source.frames(Some(2))), raw(target.frames(Some(2))));
                }
            }
        }
    }
}
