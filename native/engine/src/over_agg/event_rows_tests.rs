use super::*;

fn batch(times: Vec<i64>, values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from(vec![None::<i64>; times.len()])) as ArrayRef,
        ),
        ("value", Arc::new(Int64Array::from(values)) as ArrayRef),
        ("time", Arc::new(Int64Array::from(times)) as ArrayRef),
    ])
    .unwrap()
}

fn memory(keyless: bool, offset: i64) -> OverWindowAggregator {
    OverWindowAggregator::new(
        vec![0; 2],
        vec![0, 3],
        2,
        vec![1; 2],
        if keyless { vec![] } else { vec![0] },
        1,
        offset,
        false,
    )
    .with_key_timestamp_precisions(if keyless { vec![] } else { vec![-1] })
}

#[cfg(feature = "rocksdb-state")]
fn operator(
    keyless: bool,
    offset: i64,
    persistent: bool,
    path: &std::path::Path,
) -> OverWindowAggregator {
    let op = memory(keyless, offset);
    if persistent {
        let store = crate::state::RocksOverAggStore::create(
            crate::state::RocksStoreConfig {
                table_dir: path.to_str().unwrap().into(),
                max_parallelism: 8,
                options_json: include_str!("../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 0,
                shared_resources: 0,
            },
            &[],
            &rocks_over_frame_value_types(&[0; 2], &[0, 3], 1),
            &[],
            batch(vec![], vec![]).schema(),
            0..=7,
        )
        .unwrap();
        op.with_store(
            store,
            if keyless {
                vec![]
            } else {
                vec![DataType::Int64]
            },
        )
    } else {
        op.with_memory_budget(64 << 20).unwrap()
    }
}

fn frames(op: &OverWindowAggregator) -> Vec<RecordBatch> {
    let mut frames = Vec::new();
    op.visit_canonical_frames(|batch| {
        frames.push(batch);
        Ok(())
    })
    .unwrap();
    frames
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn maximum_bucket_originals_and_raw_arithmetic_survive_native_backend_transfers() {
    for keyless in [false, true] {
        for offset in [0, 2] {
            for earlier in [false, true] {
                for from_store in [false, true] {
                    for to_store in [false, true] {
                        let dir = tempfile::tempdir().unwrap();
                        let mut source =
                            operator(keyless, offset, from_store, &dir.path().join("source"));
                        source.bind_canonical_frame_inputs().unwrap();
                        source
                            .push(
                                batch(
                                    vec![if earlier { i64::MAX - 1 } else { i64::MAX }; 2],
                                    vec![Some(1), Some(2)],
                                ),
                                0,
                            )
                            .unwrap();
                        source
                            .push(
                                batch(vec![i64::MAX; 4], vec![Some(3), Some(4), None, Some(2)]),
                                0,
                            )
                            .unwrap();
                        let output = source.flush(i64::MAX, 0).unwrap();
                        assert_eq!(output.num_rows(), 6);
                        assert_eq!(
                            ScalarValue::try_from_array(output.column(3), 5).unwrap(),
                            ScalarValue::Int64(Some(if earlier { 9 } else { 12 }))
                        );
                        assert_eq!(
                            ScalarValue::try_from_array(output.column(4), 5).unwrap(),
                            ScalarValue::Int64(Some(if earlier { 3 } else { 5 }))
                        );
                        let original = frames(&source);
                        assert_eq!(
                            original.iter().map(RecordBatch::num_rows).sum::<usize>(),
                            if earlier { 4 } else { 6 }
                        );
                        let snapshots = if from_store {
                            source
                                .canonical_partitions(8, if keyless { &[] } else { &[-1] })
                                .unwrap()
                        } else {
                            source.snapshot_partitions(8, if keyless { &[] } else { &[-1] })
                        }
                        .into_values()
                        .collect::<Vec<_>>();
                        let target = if to_store {
                            let mut op =
                                operator(keyless, offset, true, &dir.path().join("target"));
                            op.import_partitions(&snapshots).unwrap();
                            op.adopt_store_retention(0).unwrap();
                            op
                        } else {
                            OverWindowAggregator::restore_partitions(
                                vec![0; 2],
                                vec![0, 3],
                                2,
                                vec![1; 2],
                                if keyless { vec![] } else { vec![0] },
                                1,
                                offset,
                                false,
                                &snapshots,
                                0,
                                0,
                            )
                            .with_memory_budget(64 << 20)
                            .unwrap()
                        };
                        assert_eq!(original, frames(&target));
                    }
                }
            }
        }
    }
}
