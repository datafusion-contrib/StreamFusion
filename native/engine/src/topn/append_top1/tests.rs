use super::*;
use arrow::array::{Float64Array, StringArray};

fn input(keys: usize, turn: usize) -> RecordBatch {
    let candidates = [
        None,
        Some(0.0),
        Some(-0.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(2.0),
        Some(-2.0),
    ];
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from_iter_values(
                (0..128).map(|row| ((row * 17 + turn * 31) % keys) as i64),
            )) as ArrayRef,
        ),
        (
            "sort",
            Arc::new(Float64Array::from_iter(
                (0..128).map(|row| candidates[(row + turn * 3) % candidates.len()]),
            )) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(StringArray::from_iter(
                (0..128).map(|row| (row % 7 != 0).then(|| format!("row-{turn}-{row}"))),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn ignored_first_touch_keeps_bundle_order_when_that_partition_changes_later() {
    let batch = |keys, sorts| {
        RecordBatch::try_from_iter(vec![
            ("key", Arc::new(Int64Array::from(keys)) as ArrayRef),
            ("sort", Arc::new(Int64Array::from(sorts)) as ArrayRef),
        ])
        .unwrap()
    };
    for output_rank in [false, true] {
        let create = |fast| {
            TopNRanker::new(
                vec![0],
                vec![SortColumn {
                    index: 1,
                    ascending: true,
                    nulls_first: false,
                }],
                1,
                output_rank,
                true,
            )
            .with_top1_fast(fast)
        };
        let mut specialized = create(true);
        let mut generic = create(false);
        for input in [
            batch(vec![1, 2], vec![10, 10]),
            batch(vec![1, 2, 1], vec![20, 5, 5]),
        ] {
            assert_eq!(
                specialized.push(&input, 0).unwrap(),
                generic.push(&input, 0).unwrap()
            );
            assert_eq!(specialized.flush_net_diff(), generic.flush_net_diff());
        }
    }
}

#[test]
fn specialization_preserves_generic_changes_ties_ttl_and_checkpoint_continuation() {
    for keys in [4, 256] {
        for net_diff in [false, true] {
            for output_rank in [false, true] {
                for ttl in [0, 1000] {
                    for ascending in [false, true] {
                        for nulls_first in [false, true] {
                            let sort = vec![SortColumn {
                                index: 1,
                                ascending,
                                nulls_first,
                            }];
                            let create = |fast| {
                                let mut ranker = TopNRanker::new(
                                    vec![0],
                                    sort.clone(),
                                    1,
                                    output_rank,
                                    net_diff,
                                )
                                .with_state_ttl(ttl)
                                .with_memory_budget(32 << 20)
                                .unwrap();
                                ranker.top1_fast = fast;
                                ranker
                            };
                            let mut specialized = create(true);
                            let mut generic = create(false);
                            for turn in 0..12 {
                                let batch = input(keys, turn);
                                let now = 5000 + turn as i64 * 251;
                                assert_eq!(
                                    specialized.push(&batch, now).unwrap(),
                                    generic.push(&batch, now).unwrap(),
                                    "keys={keys} net={net_diff} rank={output_rank} ttl={ttl} asc={ascending} nulls={nulls_first} turn={turn}"
                                );
                                if turn % 3 == 2 {
                                    assert_eq!(
                                        specialized.flush_net_diff(),
                                        generic.flush_net_diff()
                                    );
                                    assert_eq!(
                                        specialized.memory.state_bytes,
                                        generic.memory.state_bytes
                                    );
                                    let restore = |ranker: &TopNRanker, fast| {
                                        let mut restored = TopNRanker::restore_partitions(
                                            vec![0],
                                            vec![-1],
                                            sort.clone(),
                                            1,
                                            output_rank,
                                            net_diff,
                                            &ranker
                                                .snapshot_partitions(16)
                                                .into_values()
                                                .collect::<Vec<_>>(),
                                            now,
                                        )
                                        .with_state_ttl(ttl)
                                        .with_memory_budget(32 << 20)
                                        .unwrap();
                                        restored.top1_fast = fast;
                                        restored
                                    };
                                    specialized = restore(&specialized, true);
                                    generic = restore(&generic, false);
                                }
                            }
                            assert_eq!(specialized.flush_net_diff(), generic.flush_net_diff());
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn constant_top1_raw_state_survives_scale_out_and_merge_back() {
    let sort = vec![SortColumn {
        index: 1,
        ascending: true,
        nulls_first: false,
    }];
    let mut original = TopNRanker::new(vec![0], sort.clone(), 1, true, true);
    original.push(&input(256, 0), 0).unwrap();
    original.flush_net_diff();
    let saved = original.snapshot_partitions(16);
    let restore = |parts: &[Vec<u8>], fast| {
        TopNRanker::restore_partitions(vec![0], vec![-1], sort.clone(), 1, true, true, parts, 0)
            .with_top1_fast(fast)
    };
    let mut generic = restore(&saved.values().cloned().collect::<Vec<_>>(), false);
    let mut workers: Vec<_> = (0..2)
        .map(|task| {
            restore(
                &saved
                    .iter()
                    .filter(|(group, _)| (**group as usize) / 8 == task)
                    .map(|(_, bytes)| bytes.clone())
                    .collect::<Vec<_>>(),
                true,
            )
        })
        .collect();
    for turn in 1..5 {
        let batch = input(256, turn);
        generic.push(&batch, 0).unwrap();
        generic.flush_net_diff();
        for (task, worker) in workers.iter_mut().enumerate() {
            let mut keys = BinaryRowBatchEncoder::new(&batch, &[0], &[-1]);
            let assigned = arrow::array::BooleanArray::from_iter(
                (0..batch.num_rows())
                    .map(|row| Some(flink_key_group(keys.hash(row), 16) / 8 == task)),
            );
            worker
                .push(
                    &arrow::compute::filter_record_batch(&batch, &assigned).unwrap(),
                    0,
                )
                .unwrap();
            worker.flush_net_diff();
        }
    }
    let merged: Vec<_> = workers
        .iter()
        .flat_map(|worker| worker.snapshot_partitions(16).into_values())
        .collect();
    let mut restored = restore(&merged, true);
    assert_eq!(
        restored.push(&input(256, 9), 0).unwrap(),
        generic.push(&input(256, 9), 0).unwrap()
    );
    assert_eq!(restored.flush_net_diff(), generic.flush_net_diff());
}

#[cfg(feature = "rocksdb-state")]
#[test]
fn rejected_top1_rows_do_not_encode_or_write_persistent_state() {
    use crate::state::{RocksStateCodec, RocksStore, RocksStoreConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct CountingCodec(TopNStateCodec, Arc<AtomicUsize>);
    impl RocksStateCodec for CountingCodec {
        type Value = TopNPartition;
        fn supported(&self) -> bool {
            self.0.supported()
        }
        fn value_fields(&self) -> Vec<(String, DataType)> {
            self.0.value_fields()
        }
        fn value_bytes(&self, value: &TopNPartition) -> usize {
            self.0.value_bytes(value)
        }
        fn write_ms(&self, value: &TopNPartition) -> i64 {
            self.0.write_ms(value)
        }
        fn raw(&self) -> bool {
            true
        }
        fn raw_write(&self, value: &TopNPartition, output: &mut Vec<u8>) {
            self.1.fetch_add(1, Ordering::Relaxed);
            self.0.raw_write(value, output);
        }
        fn from_raw(&self, bytes: &[u8]) -> TopNPartition {
            self.0.from_raw(bytes)
        }
    }
    for net_diff in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let writes = Arc::new(AtomicUsize::new(0));
        let batch = input(4, 0);
        let sort = vec![SortColumn {
            index: 1,
            ascending: true,
            nulls_first: true,
        }];
        let converters = TopNConverters::build(&batch, 3, &[0], &sort);
        let store = RocksStore::create(
            RocksStoreConfig {
                table_dir: directory
                    .path()
                    .join("state")
                    .to_string_lossy()
                    .into_owned(),
                max_parallelism: 16,
                options_json: include_str!("../../../benches/fixtures/rocks-options.json").into(),
                ttl_ms: 0,
                shared_resources: 0,
            },
            CountingCodec(TopNStateCodec::new(&converters), writes.clone()),
        )
        .unwrap();
        let mut ranker = TopNRanker::new(vec![0], sort, 1, false, net_diff)
            .with_converters(converters)
            .with_payload_schema(batch.schema())
            .with_backend(store);
        ranker.push(&batch, 0).unwrap();
        ranker.flush_net_diff();
        assert!(writes.load(Ordering::Relaxed) > 0);
        writes.store(0, Ordering::Relaxed);
        ranker.push(&batch, 0).unwrap();
        ranker.flush_net_diff();
        assert_eq!(writes.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn selected_before_image_policy_survives_top1_specialization() {
    let input = RecordBatch::try_from_iter(vec![
        ("key", Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef),
        (
            "sort",
            Arc::new(Int64Array::from(vec![Some(10), Some(5), None])) as ArrayRef,
        ),
    ])
    .unwrap();
    for before in [false, true] {
        let mut expected = vec![0];
        for _ in 0..2 {
            if before {
                expected.push(1);
            }
            expected.push(2);
        }
        for fast in [false, true] {
            for rank in [false, true] {
                let mut ranker = TopNRanker::new(
                    vec![0],
                    vec![SortColumn {
                        index: 1,
                        ascending: true,
                        nulls_first: true,
                    }],
                    1,
                    rank,
                    false,
                )
                .with_top1_fast(fast)
                .with_generate_update_before(before);
                let output = ranker.push(&input, 0).unwrap();
                let kinds = output
                    .column(output.num_columns() - 1)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap();
                assert_eq!(kinds.values().as_ref(), expected.as_slice());
            }
        }
    }
}
