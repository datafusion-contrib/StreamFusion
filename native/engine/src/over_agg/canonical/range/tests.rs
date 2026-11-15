use super::*;

fn operator() -> OverWindowAggregator {
    let mut op = OverWindowAggregator::new(vec![0], vec![0], 2, vec![1], vec![0], 2, 32, false)
        .with_memory_budget(1 << 20)
        .unwrap();
    op.bind_canonical_frame_inputs().unwrap();
    op
}

fn protocol(
    deadline: Option<i64>,
    last: Option<i64>,
    timers: &[i64],
    order: &[i64],
) -> RecordBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![Some(7)])),
        Arc::new(Int64Array::from(vec![deadline])),
        Arc::new(Int64Array::from(vec![last])),
        list(timers),
        list(order),
    ];
    let fields = ["key0", DEADLINE, LAST, TIMERS, ORDER]
        .into_iter()
        .zip(&columns)
        .map(|(name, column)| Field::new(name, column.data_type().clone(), true))
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn pending_and_orphan_protocols_transfer_without_folding_rows() {
    for (deadline, last, timers, order) in [
        (Some(1049), None, vec![1000, 1049], vec![1000]),
        (Some(-10), Some(1000), vec![-10, 1049], vec![1000]),
        (None, None, vec![i64::MAX - 10], vec![]),
        (None, Some(i64::MAX - 10), vec![], vec![]),
    ] {
        let mut op = operator();
        op.import_canonical_range(&protocol(deadline, last, &timers, &order))
            .unwrap();
        let mut exported = Vec::new();
        op.visit_canonical_range(|batch| {
            exported.push(batch);
            Ok(())
        })
        .unwrap();
        assert_eq!(exported.len(), 1);
        let mut restored = operator();
        restored.import_canonical_range(&exported[0]).unwrap();
        let mut twice = Vec::new();
        restored
            .visit_canonical_range(|batch| {
                twice.push(batch);
                Ok(())
            })
            .unwrap();
        assert_eq!(exported, twice);
        let mut frames = 0;
        restored
            .visit_canonical_frames(|_| {
                frames += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(frames, 0);
    }
}

#[test]
fn invalid_protocols_leave_the_native_key_unmodified() {
    for batch in [
        protocol(Some(1049), None, &[1000], &[1000]),
        protocol(Some(1049), None, &[1049, 1000], &[1000]),
        protocol(Some(1049), None, &[1000, 1049, 1049], &[1000]),
        protocol(Some(1049), None, &[1000, 1049], &[1000, 1000]),
        protocol(None, None, &[1000], &[1000]),
    ] {
        let mut op = operator();
        assert!(op.import_canonical_range(&batch).is_err());
        assert!(op.range_timers.is_empty());
        assert!(op.cleanup_state.is_empty());
        assert_eq!(op.retention_bytes, 0);
    }
    let mut op = operator();
    let valid = protocol(Some(1049), None, &[1000, 1049], &[1000]);
    op.import_canonical_range(&valid).unwrap();
    let bytes = op.retention_bytes;
    assert!(op.import_canonical_range(&valid).is_err());
    assert_eq!(op.range_timers.len(), 1);
    assert_eq!(op.retention_bytes, bytes);
}

#[test]
fn receiver_failure_preserves_protocol_for_retry() {
    let mut op = operator();
    op.import_canonical_range(&protocol(None, Some(1000), &[1049], &[]))
        .unwrap();
    let bytes = op.retention_bytes;
    assert!(op
        .visit_canonical_range(|_| Err(invalid("receiver stopped")))
        .is_err());
    let mut count = 0;
    op.visit_canonical_range(|_| {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(op.retention_bytes, bytes);
}
