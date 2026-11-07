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
