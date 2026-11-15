use super::super::tests::{input, owner};
use super::*;

fn frames(owner: &GroupAggregator) -> Vec<(FrameKind, RecordBatch)> {
    let mut frames = vec![];
    owner
        .visit_stock_extrema(|kind, batch| {
            frames.push((kind, batch));
            Ok(())
        })
        .unwrap();
    frames
}

fn replace(batch: &RecordBatch, index: usize, column: ArrayRef) -> RecordBatch {
    let mut fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields[index] = Field::new(fields[index].name(), column.data_type().clone(), true);
    let mut columns = batch.columns().to_vec();
    columns[index] = column;
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[test]
fn frame_reservations_deny_before_build_and_release_after_receiver_failure() {
    let denied = owner().with_state_ttl(0).with_memory_budget(1024).unwrap();
    let mut work = denied.memory.temporary_reservation();
    assert!(matches!(
        emit_frame(
            FrameKind::Headers,
            1 << 20,
            1,
            8,
            &mut work,
            || panic!("denied frame reached allocation"),
            &mut |_, _, _| panic!("denied frame reached receiver")
        ),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(work.as_ref().unwrap().size(), 0);
    let source = owner().with_state_ttl(0).with_memory_budget(16384).unwrap();
    let mut work = source.memory.temporary_reservation();
    let build = || {
        RecordBatch::try_from_iter(vec![("x", Arc::new(Int64Array::from(vec![7])) as ArrayRef)])
            .map_err(DataFusionError::from)
    };
    let result = emit_frame(
        FrameKind::Headers,
        8,
        1,
        1,
        &mut work,
        build,
        &mut |_, batch, work| {
            assert_eq!(work.as_ref().unwrap().size(), batch.get_array_memory_size());
            Err(invalid("receiver stopped"))
        },
    );
    assert!(result.unwrap_err().to_string().contains("receiver stopped"));
    assert_eq!(work.as_ref().unwrap().size(), 0);
    emit_frame(
        FrameKind::Headers,
        8,
        1,
        1,
        &mut work,
        build,
        &mut |_, _, _| Ok(()),
    )
    .unwrap();
    assert_eq!(work.as_ref().unwrap().size(), 0);
}

#[test]
fn hot_variable_keys_cannot_multiply_one_entry_frame_beyond_its_byte_bound() {
    let key = "x".repeat(2 * 1024 * 1024);
    let mut source = owner()
        .with_state_ttl(0)
        .with_memory_budget(16 << 20)
        .unwrap();
    for value in 0..16 {
        let input = RecordBatch::try_from_iter(vec![
            (
                "key",
                Arc::new(StringArray::from(vec![key.as_str()])) as ArrayRef,
            ),
            ("value", Arc::new(Int64Array::from(vec![value])) as ArrayRef),
            (
                ROW_KIND_COLUMN,
                Arc::new(Int8Array::from(vec![0])) as ArrayRef,
            ),
        ])
        .unwrap();
        source.update(&input, 0).unwrap();
    }
    let before = source.snapshot();
    let mut headers = 0;
    let mut entries = 0;
    source
        .visit_stock_extrema(|kind, batch| {
            assert_eq!(batch.num_rows(), 1);
            assert!(batch.get_array_memory_size() < 5 * 1024 * 1024);
            match kind {
                FrameKind::Headers => headers += batch.num_rows(),
                FrameKind::Entries => entries += batch.num_rows(),
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(headers, 1);
    assert_eq!(entries, 32);
    assert_eq!(source.snapshot(), before);
}

#[test]
fn variable_binary_row_keys_retain_complete_extrema_state_and_reject_unaligned_frames() {
    let input = |key: Option<&str>, value: Option<i64>, kind: i8| {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(StringArray::from(vec![key])) as ArrayRef),
            ("value", Arc::new(Int64Array::from(vec![value])) as ArrayRef),
            (
                ROW_KIND_COLUMN,
                Arc::new(Int8Array::from(vec![kind])) as ArrayRef,
            ),
        ])
        .unwrap()
    };
    let normalized = |owner: &GroupAggregator| {
        let mut result = Vec::new();
        for (kind, batch) in frames(owner) {
            for row in 0..batch.num_rows() {
                let values = batch
                    .columns()
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, row).unwrap())
                    .collect::<Vec<_>>();
                result.push(format!("{kind:?}:{values:?}"));
            }
        }
        result.sort();
        result
    };
    let long = "λ雪😀".repeat(400);
    for ttl in [0, 1000] {
        let mut source = owner().with_state_ttl(ttl);
        for (key, value) in [
            (Some("short"), Some(7)),
            (Some(long.as_str()), Some(3)),
            (None, None),
        ] {
            source.update(&input(key, value, 0), 1000).unwrap();
        }
        let saved = frames(&source);
        let key_column = saved[0]
            .1
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!((0..key_column.len()).any(|row| key_column.value(row).len() > 16));
        let before = source.snapshot();
        let mut target = owner()
            .with_state_ttl(ttl)
            .with_memory_budget(1 << 20)
            .unwrap();
        for (kind, batch) in &saved {
            target.transfer_stock_extrema(*kind, batch, true).unwrap();
        }
        assert_eq!(target.store.iter().count(), 0);
        for (kind, batch) in &saved {
            target.transfer_stock_extrema(*kind, batch, false).unwrap();
        }
        assert_eq!(normalized(&source), normalized(&target));
        assert_eq!(source.snapshot(), before);
        for (now, key, value, kind) in [
            (1100, Some("short"), Some(7), 3),
            (1200, Some(long.as_str()), Some(9), 0),
            (1300, None, Some(2), 0),
            (2401, Some(long.as_str()), Some(3), 3),
            (2402, Some(long.as_str()), Some(11), 0),
        ] {
            let batch = input(key, value, kind);
            assert_eq!(
                source.update(&batch, now).unwrap(),
                target.update(&batch, now).unwrap()
            );
            assert_eq!(normalized(&source), normalized(&target));
        }
        let mut key = key_column.value(0).to_vec();
        key.push(0);
        let first = arrow::compute::take_record_batch(
            &saved[0].1,
            &arrow::array::UInt32Array::from(vec![0]),
        )
        .unwrap();
        let invalid = replace(
            &first,
            0,
            Arc::new(BinaryArray::from_iter_values([key.as_slice()])),
        );
        for validating in [true, false] {
            let mut empty = owner().with_state_ttl(ttl);
            assert!(empty
                .transfer_stock_extrema(FrameKind::Headers, &invalid, validating)
                .is_err());
            assert_eq!(empty.store.iter().count(), 0);
        }
    }
}

#[test]
fn complete_state_and_orphans_validate_then_resume_without_source_mutation() {
    for ttl in [0, 1000] {
        for orphan in [false, true]
            .into_iter()
            .filter(|orphan| !orphan || ttl > 0)
        {
            let mut source = owner().with_state_ttl(ttl);
            for (now, value, kind) in [
                (1000, None, 0),
                (1100, Some(7), 0),
                (1200, Some(4), 0),
                (1300, Some(7), 3),
                (1400, None, 0),
                (1500, Some(11), 0),
            ] {
                source.update(&input(1, value, kind), now).unwrap();
            }
            if orphan {
                source.update(&input(1, Some(7), 3), 2601).unwrap();
            }
            let before = source.snapshot();
            let frames = frames(&source);
            let mut target = owner()
                .with_state_ttl(ttl)
                .with_memory_budget(1 << 20)
                .unwrap();
            for (kind, batch) in &frames {
                target.transfer_stock_extrema(*kind, batch, true).unwrap();
            }
            assert_eq!(target.store.iter().count(), 0);
            for (kind, batch) in &frames {
                target.transfer_stock_extrema(*kind, batch, false).unwrap();
            }
            assert_eq!(source.snapshot(), before);
            assert_eq!(target.snapshot(), before);
            for (now, value, kind) in [(2700, None, 0), (2800, Some(9), 0), (2900, Some(9), 3)] {
                assert_eq!(
                    source.update(&input(1, value, kind), now).unwrap(),
                    target.update(&input(1, value, kind), now).unwrap()
                );
                assert_eq!(source.snapshot(), target.snapshot());
                let measured: usize = target
                    .store
                    .iter()
                    .map(|(key, state)| byte_key_bytes(&key.0) + group_key_state_bytes(state))
                    .sum();
                assert_eq!(target.memory.state_bytes, measured);
            }
        }
    }
}

#[test]
fn invalid_header_fields_and_duplicate_keys_fail_without_population() {
    let mut source = owner();
    source.update(&input(1, Some(7), 0), 1000).unwrap();
    let header = frames(&source).remove(0).1;
    let duplicate = arrow::compute::concat_batches(&header.schema(), [&header, &header]).unwrap();
    for invalid in [
        replace(&header, 4, Arc::new(Int64Array::from(vec![None]))),
        replace(
            &header,
            header.num_columns() - 1,
            Arc::new(BooleanArray::from(vec![false])),
        ),
        replace(&header, 1, Arc::new(StringArray::from(vec!["bad"]))),
        replace(
            &header,
            0,
            Arc::new(BinaryArray::from_iter_values([b"bad".as_slice()])),
        ),
        duplicate,
    ] {
        for validating in [true, false] {
            let mut target = owner();
            assert!(target
                .transfer_stock_extrema(FrameKind::Headers, &invalid, validating)
                .is_err());
            assert_eq!(target.store.iter().count(), 0);
        }
    }
}

#[test]
fn invalid_or_duplicate_entries_and_missing_headers_fail_before_mutation() {
    let mut source = owner();
    source.update(&input(1, Some(7), 0), 1000).unwrap();
    let frames = frames(&source);
    let entries = &frames[1].1;
    let mut target = owner();
    assert!(target
        .transfer_stock_extrema(FrameKind::Entries, entries, false)
        .is_err());
    assert_eq!(target.store.iter().count(), 0);
    target
        .transfer_stock_extrema(FrameKind::Headers, &frames[0].1, false)
        .unwrap();
    let before = target.snapshot();
    for invalid in [
        replace(entries, 1, Arc::new(Int64Array::from(vec![-1, 1]))),
        replace(entries, 1, Arc::new(Int64Array::from(vec![99, 1]))),
        replace(entries, 2, Arc::new(Int64Array::from(vec![None, Some(7)]))),
        arrow::compute::concat_batches(&entries.schema(), [entries, entries]).unwrap(),
    ] {
        for validating in [true, false] {
            assert!(target
                .transfer_stock_extrema(FrameKind::Entries, &invalid, validating)
                .is_err());
            assert_eq!(target.snapshot(), before);
        }
    }
    target
        .transfer_stock_extrema(FrameKind::Entries, entries, false)
        .unwrap();
    let complete = target.snapshot();
    assert!(target
        .transfer_stock_extrema(FrameKind::Entries, entries, false)
        .is_err());
    assert_eq!(target.snapshot(), complete);
}

#[test]
fn bounded_large_view_import_and_fresh_retry_keep_original_source() {
    let mut source = owner().with_state_ttl(0);
    for value in 0..1025 {
        source.update(&input(1, Some(value), 0), 0).unwrap();
    }
    let before = source.snapshot();
    let frames = frames(&source);
    let mut denied = owner().with_state_ttl(0).with_memory_budget(64).unwrap();
    for (kind, batch) in &frames {
        denied.transfer_stock_extrema(*kind, batch, true).unwrap();
    }
    assert_eq!(denied.store.iter().count(), 0);
    assert!(matches!(
        denied.transfer_stock_extrema(frames[0].0, &frames[0].1, false),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    drop(denied);
    assert_eq!(source.snapshot(), before);
    let mut retry = owner()
        .with_state_ttl(0)
        .with_memory_budget(16_000_000)
        .unwrap();
    for (kind, batch) in frames {
        assert!(batch.num_rows() <= CHUNK_ROWS);
        retry.transfer_stock_extrema(kind, &batch, false).unwrap();
    }
    assert_eq!(retry.snapshot(), before);
}
