use super::*;

fn logical_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("original_time", DataType::Int64, true),
    ]))
}

fn physical_schema() -> SchemaRef {
    let mut fields = logical_schema().fields().to_vec();
    fields.push(Arc::new(Field::new("cache_clock", DataType::Int64, false)));
    Arc::new(Schema::new(fields))
}

fn payload(keys: &[i64], values: &[i64], originals: &[Option<i64>]) -> RecordBatch {
    RecordBatch::try_new(
        logical_schema(),
        vec![
            Arc::new(Int64Array::from(keys.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
            Arc::new(Int64Array::from(originals.to_vec())),
        ],
    )
    .unwrap()
}

fn operator(kind: JoinKind, predicate: bool, disk: Option<&std::path::Path>) -> IntervalJoiner {
    let predicate = predicate.then(|| JoinPredicate {
        kinds: vec![6, 0, 0],
        payload: vec![12, 1, 4],
        child_counts: vec![2, 0, 0],
        longs: vec![],
        doubles: vec![],
        strings: vec![],
        compiled: None,
    });
    let mut joiner = IntervalJoiner::new(
        vec![0],
        vec![0],
        3,
        3,
        -100,
        100,
        predicate,
        kind,
        physical_schema(),
        physical_schema(),
    );
    #[cfg(feature = "rocksdb-state")]
    if let Some(path) = disk {
        joiner.store = Some(
            crate::state::RocksIntervalBuffer::create(
                RocksStoreConfig {
                    table_dir: path.to_string_lossy().into_owned(),
                    max_parallelism: 128,
                    options_json: crate::interval_join::canonical::tests::options_json(),
                    ttl_ms: 0,
                    shared_resources: 0,
                },
                physical_schema(),
                physical_schema(),
            )
            .unwrap(),
        );
    }
    joiner.bind_clock([3, 3]).unwrap();
    joiner.with_memory_budget(1 << 27).unwrap()
}

fn transfer(source: &mut IntervalJoiner, target: &mut IntervalJoiner) {
    for left in [true, false] {
        source
            .visit_canonical_rows(left, |batch, flags| {
                target.import_canonical_rows(left, &batch, &flags)
            })
            .unwrap();
        source
            .visit_canonical_timers(left, |key, deadline| {
                target.import_canonical_timer(left, key, deadline)
            })
            .unwrap();
    }
}

#[test]
fn payload_and_clock_survive_direct_handoffs_on_both_stores() {
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
                let source_dir = tempfile::tempdir().unwrap();
                let target_dir = tempfile::tempdir().unwrap();
                let mut source = operator(kind, false, source_disk.then_some(source_dir.path()));
                source
                    .push_left(payload(&[1, 2], &[10, 20], &[Some(123), None]), Some(500))
                    .unwrap();
                let pairs = source
                    .push_right(payload(&[1], &[30], &[Some(456)]), Some(550))
                    .unwrap();
                assert_eq!(pairs.num_columns(), 6);
                assert_eq!(pairs.num_rows(), 1);
                assert_eq!(
                    ScalarValue::try_from_array(pairs.column(2), 0).unwrap(),
                    ScalarValue::Int64(Some(123))
                );
                assert_eq!(
                    ScalarValue::try_from_array(pairs.column(5), 0).unwrap(),
                    ScalarValue::Int64(Some(456))
                );
                let mut target = operator(kind, false, target_disk.then_some(target_dir.path()));
                transfer(&mut source, &mut target);
                assert_eq!(target.next_cleanup().unwrap(), Some(601));
                target
                    .visit_canonical_rows(true, |batch, _| {
                        assert_eq!(
                            batch
                                .column(3)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .values(),
                            &vec![500; batch.num_rows()]
                        );
                        Ok(())
                    })
                    .unwrap();
                let left = source.advance(601).unwrap();
                let right = target.advance(601).unwrap();
                assert_eq!(left, right);
                if kind.left_is_outer() {
                    assert_eq!(right.num_rows(), 1);
                    assert!(right.column(2).is_null(0));
                }
                assert_eq!(target.next_cleanup().unwrap(), Some(651));
                assert_eq!(source.advance(651).unwrap(), target.advance(651).unwrap());
                assert_eq!(target.next_cleanup().unwrap(), None);
            }
        }
    }
}

#[test]
fn residuals_read_original_right_columns_and_rebind_once_after_native_restore() {
    for disk in [false, true] {
        #[cfg(not(feature = "rocksdb-state"))]
        if disk {
            continue;
        }
        let dir = tempfile::tempdir().unwrap();
        let mut source = operator(JoinKind::FullOuter, true, disk.then_some(dir.path()));
        source.bind_clock([3, 3]).unwrap();
        assert_eq!(source.predicate.as_ref().unwrap().payload[2], 5);
        source
            .push_left(payload(&[1], &[10], &[None]), Some(500))
            .unwrap();
        let pairs = source
            .push_right(payload(&[1], &[30], &[Some(123)]), Some(550))
            .unwrap();
        assert_eq!(pairs.num_rows(), 1);
        source.bind_clock([3, 3]).unwrap();
        assert_eq!(source.predicate.as_ref().unwrap().payload[2], 5);
    }
    let mut source = operator(JoinKind::FullOuter, false, None);
    source
        .push_left(payload(&[1], &[10], &[Some(123)]), Some(500))
        .unwrap();
    let raw = source.snapshot();
    let mut restored = IntervalJoiner::restore(
        vec![0],
        vec![0],
        3,
        3,
        -100,
        100,
        None,
        JoinKind::FullOuter,
        physical_schema(),
        physical_schema(),
        &raw,
    );
    restored.bind_clock([3, 3]).unwrap();
    assert_eq!(restored.next_cleanup().unwrap(), Some(601));
    assert_eq!(source.advance(601).unwrap(), restored.advance(601).unwrap());
}

#[test]
fn clock_binding_and_missing_clock_fail_without_changing_state() {
    let mut joiner = operator(JoinKind::FullOuter, false, None);
    assert!(joiner.bind_clock([2, 3]).is_err());
    assert!(joiner
        .push_left(payload(&[1], &[10], &[Some(123)]), None)
        .is_err());
    assert_eq!(joiner.operator_time, 0);
    assert_eq!(joiner.next_cleanup().unwrap(), None);
    // MIN is a real retained deadline, not the absence sentinel used by legacy host timers.
    joiner
        .import_canonical_timer(true, &[0; 16], i64::MIN)
        .unwrap();
    assert_eq!(joiner.next_cleanup().unwrap(), Some(i64::MIN));
}
