use super::*;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

fn operator(range: bool, polling: bool) -> OverWindowAggregator {
    let mut op = OverWindowAggregator::new(
        vec![1],
        vec![3],
        2,
        vec![0],
        vec![0],
        if range { 2 } else { 0 },
        32,
        false,
    )
    .with_state_retention(1000)
    .with_memory_budget(32 << 20)
    .unwrap();
    op.inner.seed_key_codec(&[DataType::Int64]);
    if !range {
        op.bind_event_cleanup_timers().unwrap();
    }
    if polling {
        op.bind_event_polling().unwrap();
    }
    op
}

fn key() -> ByteKey {
    let converter = key_row_converter_from_types(&[DataType::Int64]);
    ByteKey::from(
        converter
            .convert_columns(&[Arc::new(Int64Array::from(vec![7]))])
            .unwrap()
            .row(0)
            .data(),
    )
}

fn seed(op: &mut OverWindowAggregator, range: bool, polling: bool, armed: bool) {
    let key = key();
    op.cleanup_state.insert(key.clone(), 2045);
    op.retention_bytes += byte_key_bytes(&key.0);
    if !armed {
        op.event_cleanup_unarmed.insert(key.clone());
        op.retention_bytes += byte_key_bytes(&key.0);
    }
    if range || polling {
        let timers = RangeTimers {
            last: range.then_some(-10),
            armed: (-5..2046).collect(),
        };
        op.retention_bytes += byte_key_bytes(&key.0) + timers.bytes();
        op.range_timers.insert(key.clone(), timers);
    }
    op.mark_store_metadata(&key.0);
    op.account().unwrap();
}

fn records(op: &OverWindowAggregator) -> Vec<(i64, i8, i64)> {
    let mut records = Vec::new();
    op.visit_canonical_callbacks(|batch| {
        assert!(batch.num_rows() <= CHUNK_ROWS);
        assert_eq!(batch.num_columns(), 3);
        let kinds = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int8Array>()
            .unwrap();
        let times = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            records.push((
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
                kinds.value(row),
                times.value(row),
            ));
        }
        Ok(())
    })
    .unwrap();
    records.sort();
    records
}

#[test]
fn empty_and_resident_metadata_preserve_each_scalar_and_physical_callback() {
    for (range, polling) in [(true, false), (false, true), (false, false)] {
        for armed in [false, true] {
            let mut op = operator(range, polling);
            assert!(records(&op).is_empty());
            seed(&mut op, range, polling, armed);
            let mut expected = vec![(7, CLEANUP, 2045)];
            if range {
                expected.push((7, LAST_TRIGGER, -10));
            }
            if armed && !range {
                expected.push((7, PROCESSING, 2045));
            }
            if range || polling {
                expected.extend((-5..2046).map(|time| (7, EVENT, time)));
            }
            expected.sort();
            assert_eq!(expected, records(&op));
        }
    }
}

#[test]
fn callback_writer_obeys_keyless_row_and_wide_key_byte_bounds() {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(32 << 20));
    let mut memory = OperatorMemory::unaccounted();
    memory.attach_pool("callback writer", &pool, 0).unwrap();
    let mut sizes = Vec::new();
    {
        let mut writer = CallbackWriter::new(
            vec![],
            memory.temporary_reservation(),
            |batch: RecordBatch| {
                sizes.push(batch.num_rows());
                Ok(())
            },
        );
        for time in 0..2051 {
            writer.push(&[], EVENT, time).unwrap();
        }
        writer.flush().unwrap();
    }
    assert_eq!(sizes, vec![1024, 1024, 3]);
    assert_eq!(pool.reserved(), 0);
    let types = vec![DataType::Utf8];
    let converter = key_row_converter_from_types(&types);
    let wide = "x".repeat(2 * 1024 * 1024);
    let keys = converter
        .convert_columns(&[Arc::new(arrow::array::StringArray::from(vec![
            wide.as_str()
        ]))])
        .unwrap();
    let mut sizes = Vec::new();
    {
        let mut writer = CallbackWriter::new(
            types,
            memory.temporary_reservation(),
            |batch: RecordBatch| {
                assert_eq!(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .unwrap()
                        .value(0),
                    wide
                );
                sizes.push(batch.num_rows());
                Ok(())
            },
        );
        for time in 0..3 {
            writer.push(keys.row(0).data(), EVENT, time).unwrap();
        }
        writer.flush().unwrap();
    }
    assert_eq!(sizes, vec![1, 1, 1]);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn failed_receivers_denied_work_and_mid_owner_cancellation_release_charges() {
    let mut op = operator(true, false);
    seed(&mut op, true, false, true);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 << 20));
    op.memory
        .attach_pool("callbacks", &pool, op.retained_state_bytes())
        .unwrap();
    let baseline = pool.reserved();
    let original = records(&op);
    let failure = op
        .visit_canonical_callbacks(|_| Err(invalid("callback receiver sentinel")))
        .unwrap_err();
    assert!(failure.to_string().contains("callback receiver sentinel"));
    assert_eq!(baseline, pool.reserved());
    assert_eq!(original, records(&op));
    let context = crate::state::canonical_context::CanonicalContext::new(Default::default());
    let empty = operator(true, false);
    let cancelled = crate::state::canonical_context::CanonicalContext::new(Default::default());
    cancelled.cancel();
    cancelled.with(|| assert!(empty.visit_canonical_callbacks(|_| Ok(())).is_err()));
    context.with(|| {
        assert!(op
            .visit_canonical_callbacks(|_| {
                context.cancel();
                Ok(())
            })
            .is_err());
    });
    assert!(check_cancelled().is_ok());
    assert_eq!(baseline, pool.reserved());
    assert_eq!(original, records(&op));
    let denied: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
    let mut memory = OperatorMemory::unaccounted();
    memory.attach_pool("denied callbacks", &denied, 0).unwrap();
    let mut writer = CallbackWriter::new(vec![], memory.temporary_reservation(), |_| Ok(()));
    assert!(writer.push(&[], EVENT, 1).is_err());
    drop(writer);
    assert_eq!(denied.reserved(), 0);
}

#[cfg(feature = "rocksdb-state")]
fn persistent(path: &std::path::Path, range: bool, polling: bool) -> OverWindowAggregator {
    let op = operator(range, polling);
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
        Field::new("time", DataType::Int64, false),
    ]));
    let frame = if range { 2 } else { 0 };
    let store = crate::state::RocksOverAggStore::create(
        crate::state::RocksStoreConfig {
            table_dir: path.to_str().unwrap().into(),
            max_parallelism: 128,
            options_json: include_str!("../../../../benches/fixtures/rocks-options.json").into(),
            ttl_ms: 0,
            shared_resources: 0,
        },
        &op.inner.store_state_types(),
        &rocks_over_frame_value_types(&[0], &[3], frame),
        &[],
        schema,
        0..=127,
    )
    .unwrap();
    op.with_store(store, vec![DataType::Int64])
        .with_read_through_budget(32 << 20)
        .unwrap()
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn frozen_callbacks_stream_a_hot_owner_without_hydrating_live_maps_and_keep_capture_versions() {
    for (range, polling) in [(true, false), (false, true), (false, false)] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state");
        let mut source = persistent(&path, range, polling);
        seed(&mut source, range, polling, false);
        let expected = records(&source);
        source.checkpoint_store("").unwrap();
        let snapshot = crate::state::rocks_store::FrozenDatabases::capture(&path).unwrap();
        source.cleanup_state.insert(key(), 9000);
        if range {
            source.register_range_timer(&key().0, 9000);
        }
        source.mark_store_metadata(&key().0);
        source.checkpoint_store("").unwrap();
        snapshot.with(|| {
            let mut reader = persistent(&path, range, polling);
            assert_eq!(expected, records(&reader));
            assert!(reader.range_timers.is_empty());
            assert!(reader.cleanup_state.is_empty());
            assert_eq!(reader.retention_bytes, 0);
            let denied: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
            reader
                .memory
                .attach_pool(
                    "denied raw callbacks",
                    &denied,
                    reader.retained_state_bytes(),
                )
                .unwrap();
            assert!(reader.visit_canonical_callbacks(|_| Ok(())).is_err());
            assert_eq!(denied.reserved(), 0);
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(32 << 20));
            reader
                .memory
                .attach_pool("retry raw callbacks", &pool, reader.retained_state_bytes())
                .unwrap();
            assert_eq!(expected, records(&reader));
            assert_eq!(pool.reserved(), 0);
            snapshot.cancel();
            assert!(reader.visit_canonical_callbacks(|_| Ok(())).is_err());
        });
    }
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn encoded_timer_views_reject_corrupt_headers_truncation_and_order_without_allocating_a_list() {
    use crate::over_agg::range_timers::RangeTimerView;
    let valid = RangeTimers {
        last: Some(i64::MIN),
        armed: vec![i64::MIN, -1, 0, i64::MAX],
    }
    .encode();
    let view = RangeTimerView::new(&valid).unwrap();
    assert_eq!(view.last, Some(i64::MIN));
    assert_eq!(
        view.armed().collect::<Result<Vec<_>, _>>().unwrap(),
        vec![i64::MIN, -1, 0, i64::MAX]
    );
    for bytes in [
        vec![],
        vec![1; 9],
        valid[..valid.len() - 1].to_vec(),
        RangeTimers {
            last: None,
            armed: vec![1, 1],
        }
        .encode(),
        RangeTimers {
            last: None,
            armed: vec![2, 1],
        }
        .encode(),
    ] {
        assert!(RangeTimers::decode(&bytes).is_err());
        if let Ok(view) = RangeTimerView::new(&bytes) {
            assert!(view.armed().collect::<Result<Vec<_>, _>>().is_err());
        }
    }
    for offset in [0, 1] {
        let mut bytes = valid.clone();
        bytes[offset] = 2;
        assert!(RangeTimers::decode(&bytes).is_err());
        if let Ok(view) = RangeTimerView::new(&bytes) {
            assert!(view.armed().collect::<Result<Vec<_>, _>>().is_err());
        }
    }
}
