use super::*;

fn owner(code: i64, ttl: i64) -> GroupAggregator {
    let mut owner = GroupAggregator::new(vec![38, 39], vec![code; 2], vec![1; 2], vec![0], true)
        .with_state_ttl(ttl);
    owner.configure_stock_extrema(&[38, 39]).unwrap();
    owner
}

fn input(code: i64, value: Option<i64>, kind: i8) -> RecordBatch {
    let ty = owner(code, 0).value_types[0].clone();
    let value = value.map_or_else(
        || null_scalar(&ty),
        |value| decode_key(value, &ty).unwrap().scalar(&ty),
    );
    RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("value", value.to_array().unwrap()),
        (ROW_KIND_COLUMN, Arc::new(Int8Array::from(vec![kind]))),
    ])
    .unwrap()
}

fn frames(owner: &GroupAggregator) -> Vec<(FrameKind, RecordBatch)> {
    let mut result = vec![];
    owner
        .visit_stock_extrema(|kind, batch| {
            result.push((kind, batch));
            Ok(())
        })
        .unwrap();
    result
}

fn replace(batch: &RecordBatch, index: usize, value: i64) -> RecordBatch {
    let mut columns = batch.columns().to_vec();
    columns[index] = Arc::new(Int64Array::from(vec![value; batch.num_rows()]));
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

#[test]
fn typed_multisets_keep_complete_state_and_native_checkpoint_continuation() {
    for (code, low, high) in [
        (0, i64::MIN, i64::MAX),
        (2, i32::MIN as i64, i32::MAX as i64),
        (4, i16::MIN as i64, i16::MAX as i64),
        (5, i8::MIN as i64, i8::MAX as i64),
        (8, -719162, 2932896),
        (9, 1, 86399999),
        (10, 0, 1),
    ] {
        for ttl in [0, 1000] {
            let mut source = owner(code, ttl);
            for (i, (value, kind)) in [
                (None, 0),
                (Some(high), 0),
                (Some(low), 0),
                (Some(low), 0),
                (Some(high), 3),
                (Some(high), 3),
            ]
            .into_iter()
            .enumerate()
            {
                source
                    .update(&input(code, value, kind), 1000 + i as i64)
                    .unwrap();
            }
            let before = source.snapshot();
            let frames = frames(&source);
            let mut target = owner(code, ttl);
            for (kind, batch) in &frames {
                target.transfer_stock_extrema(*kind, batch, true).unwrap();
            }
            assert_eq!(target.store.iter().count(), 0);
            for (kind, batch) in &frames {
                target.transfer_stock_extrema(*kind, batch, false).unwrap();
            }
            assert_eq!(before, source.snapshot());
            assert_eq!(before, target.snapshot());
            let mut recovered = GroupAggregator::restore(
                vec![38, 39],
                vec![code; 2],
                vec![1; 2],
                vec![0],
                true,
                &before,
                1006,
            )
            .with_state_ttl(ttl);
            recovered.configure_stock_extrema(&[38, 39]).unwrap();
            assert_eq!(before, recovered.snapshot());
            for (now, value, kind) in [
                (1100, Some(low), 3),
                (1200, Some(high), 0),
                (1300, None, 0),
                (2401, Some(low), 3),
                (2402, None, 0),
                (2403, Some(high), 0),
            ] {
                let batch = input(code, value, kind);
                let expected = source.update(&batch, now).unwrap();
                assert_eq!(expected, target.update(&batch, now).unwrap());
                assert_eq!(expected, recovered.update(&batch, now).unwrap());
                assert_eq!(source.snapshot(), target.snapshot());
                assert_eq!(source.snapshot(), recovered.snapshot());
            }
        }
    }
}

#[test]
fn typed_header_and_entry_ranges_are_checked_before_any_mutation() {
    for (code, invalid) in [
        (2, i32::MAX as i64 + 1),
        (4, i16::MIN as i64 - 1),
        (5, i8::MAX as i64 + 1),
        (8, i32::MIN as i64 - 1),
        (9, i32::MAX as i64 + 1),
        (10, -1),
        (10, 2),
    ] {
        let mut source = owner(code, 0);
        source.update(&input(code, Some(1), 0), 1000).unwrap();
        let frames = frames(&source);
        for (kind, index) in [(FrameKind::Headers, 3), (FrameKind::Entries, 2)] {
            let batch = &frames.iter().find(|(k, _)| *k == kind).unwrap().1;
            let corrupt = replace(batch, index, invalid);
            for validating in [true, false] {
                let mut target = owner(code, 0);
                if kind == FrameKind::Entries {
                    target
                        .transfer_stock_extrema(FrameKind::Headers, &frames[0].1, false)
                        .unwrap();
                }
                let before = target.snapshot();
                assert!(target
                    .transfer_stock_extrema(kind, &corrupt, validating)
                    .is_err());
                assert_eq!(before, target.snapshot());
            }
        }
    }
}

#[test]
fn typed_entry_duplicates_from_earlier_chunks_are_not_reinserted() {
    for code in [0, 2, 4, 5, 8, 9, 10] {
        let mut source = owner(code, 1000);
        source.update(&input(code, Some(1), 0), 1000).unwrap();
        let frames = frames(&source);
        let mut target = owner(code, 1000);
        for (kind, batch) in &frames {
            target.transfer_stock_extrema(*kind, batch, false).unwrap();
        }
        let before = target.snapshot();
        for validating in [true, false] {
            assert!(target
                .transfer_stock_extrema(FrameKind::Entries, &frames[1].1, validating)
                .is_err());
            assert_eq!(before, target.snapshot());
        }
    }
}

#[test]
fn noncarrier_types_and_nontyped_arithmetic_slots_keep_their_admission_boundary() {
    for code in [1, 3, 6, 1003, 3802] {
        let mut target =
            GroupAggregator::new(vec![38, 39], vec![code; 2], vec![1; 2], vec![0], true);
        assert!(target.configure_stock_extrema(&[38, 39]).is_err());
    }
    let mut target = GroupAggregator::new(vec![38, 3], vec![8, 8], vec![1; 2], vec![0], true);
    assert!(target.configure_stock_extrema(&[38, 3]).is_err());
}
