//! Operator micro-benchmarks: each measures a native operator's steady-state hot loop over an
//! in-memory Arrow batch, isolated from the JVM bridge and Flink job scheduling. Run with
//! `cargo bench`; Criterion reports time per batch, from which rows/s follows.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int8Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use streamfusion::bench::{
    split_by_key, AppendTopN, Filter, IntervalJoin, KeepFirstDedup, LocalGroupBy, Over, RetractTopN,
    UniqueUpdatingJoin,
    KeepLastDedup, Normalize, Session, Tumbling, WindowJoin,
};

const ROWS: usize = 4096;

fn single_i64(name: &str, values: Vec<i64>) -> RecordBatch {
    let column: ArrayRef = Arc::new(Int64Array::from(values));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)])),
        vec![column],
    )
    .unwrap()
}

// Filter `v > 0` over a batch where half the rows pass: CALL gt ( INPUT_REF v , LIT_LONG 0 ).
fn bench_filter(c: &mut Criterion) {
    let values: Vec<i64> = (0..ROWS as i64).map(|i| i - ROWS as i64 / 2).collect();
    let batch = single_i64("v", values);

    let mut filter = Filter::new(vec![6, 0, 1], vec![10, 0, 0], vec![2, 0, 0], vec![0], vec![], vec![]);
    // Compile the predicate once (as the operator does at open) so the loop measures evaluation.
    filter.run(batch.clone());

    let mut group = c.benchmark_group("filter");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("gt_literal", |b| {
        b.iter(|| black_box(filter.run(black_box(batch.clone()))))
    });
    group.finish();
}

// Tumbling SUM over 16 windows: update one batch, then flush all closed windows.
fn bench_tumbling(c: &mut Criterion) {
    let window_millis = 1000;
    let ts: Vec<i64> = (0..ROWS as i64).map(|i| (i % 16) * window_millis + (i % window_millis)).collect();
    let value: Vec<i64> = (0..ROWS as i64).collect();
    let ts_col: ArrayRef = Arc::new(Int64Array::from(ts));
    let value_col: ArrayRef = Arc::new(Int64Array::from(value));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, true),
            Field::new("value0", DataType::Int64, true),
        ])),
        vec![ts_col, value_col],
    )
    .unwrap();

    let mut group = c.benchmark_group("tumbling");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("sum_update_flush", |b| {
        b.iter_batched(
            || Tumbling::new(window_millis, 0, vec![0]),
            |mut aggregator| {
                aggregator.update(black_box(&batch));
                black_box(aggregator.flush(i64::MAX));
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// Tumbling SUM grouped by a key column: this is the path that allocates a grouping key per row,
// so it measures the keyed hot loop the no-key bench above does not.
fn bench_tumbling_keyed(c: &mut Criterion) {
    let window_millis = 1000;
    let ts: Vec<i64> = (0..ROWS as i64).map(|i| (i % 16) * window_millis + (i % window_millis)).collect();
    let value: Vec<i64> = (0..ROWS as i64).collect();
    let key: Vec<i64> = (0..ROWS as i64).map(|i| i % 64).collect();
    let ts_col: ArrayRef = Arc::new(Int64Array::from(ts));
    let value_col: ArrayRef = Arc::new(Int64Array::from(value));
    let key_col: ArrayRef = Arc::new(Int64Array::from(key));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, true),
            Field::new("value0", DataType::Int64, true),
            Field::new("key0", DataType::Int64, true),
        ])),
        vec![ts_col, value_col, key_col],
    )
    .unwrap();

    let mut group = c.benchmark_group("tumbling");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("sum_keyed_update_flush", |b| {
        b.iter_batched(
            || Tumbling::new(window_millis, 0, vec![0]),
            |mut aggregator| {
                aggregator.update(black_box(&batch));
                black_box(aggregator.flush(i64::MAX));
            },
            BatchSize::SmallInput,
        )
    });
    // The same keyed path with managed-memory accounting on: the delta is the per-touched-group
    // footprint tracking the operator pays when the host hands it a memory budget.
    group.bench_function("sum_keyed_update_flush_accounted", |b| {
        b.iter_batched(
            || Tumbling::with_budget(window_millis, 0, vec![0], 1 << 30),
            |mut aggregator| {
                aggregator.update(black_box(&batch));
                black_box(aggregator.flush(i64::MAX));
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// Non-windowed GROUP BY SUM keyed by a string column: this is the changelog hot loop, and a string
// key is where the per-row grouping-key allocation hurts most (each key is a heap String).
fn bench_group_by_string_key(c: &mut Criterion) {
    use streamfusion::bench::GroupBy;
    let value: Vec<i64> = (0..ROWS as i64).collect();
    // 256 distinct keys, so after the first pass every row revises an existing group.
    let key: Vec<String> = (0..ROWS).map(|i| format!("key-{}", i % 256)).collect();
    let key_col: ArrayRef = Arc::new(StringArray::from(key));
    let value_col: ArrayRef = Arc::new(Int64Array::from(value));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Utf8, false),
            Field::new("value0", DataType::Int64, true),
        ])),
        vec![key_col, value_col],
    )
    .unwrap();

    let mut group = c.benchmark_group("group_by");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("sum_string_key", |b| {
        b.iter_batched(
            // SUM (kind 0) over value column 1, grouped by string key column 0.
            || GroupBy::new(vec![0], vec![0], vec![1], vec![0]),
            |mut aggregator| black_box(aggregator.update(black_box(&batch))),
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// Single-phase SUM with 64 hot keys. The logical variant materializes one final group transition;
// the immediate variant constructs the Flink-compatible changelog after every input row.
fn bench_group_by_logical_bundle(c: &mut Criterion) {
    use streamfusion::bench::GroupBy;
    let keys: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..ROWS as i64).map(|i| i % 64),
    ));
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..ROWS as i64));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ])),
        vec![keys, values],
    )
    .unwrap();
    let physical_size = 256;
    let mut group = c.benchmark_group("group_by_logical_bundle");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("immediate", |b| {
        b.iter_batched(
            || GroupBy::new(vec![0], vec![0], vec![1], vec![0]),
            |mut aggregator| {
                for offset in (0..ROWS).step_by(physical_size) {
                    black_box(aggregator.update(&batch.slice(offset, physical_size)));
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("physical_bundle", |b| {
        b.iter_batched(
            || GroupBy::mini_batch(vec![0], vec![0], vec![1], vec![0]),
            |mut aggregator| {
                for offset in (0..ROWS).step_by(physical_size) {
                    black_box(aggregator.update(&batch.slice(offset, physical_size)));
                    black_box(aggregator.flush());
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("logical_bundle", |b| {
        b.iter_batched(
            || GroupBy::mini_batch(vec![0], vec![0], vec![1], vec![0]),
            |mut aggregator| {
                for offset in (0..ROWS).step_by(physical_size) {
                    black_box(aggregator.update(&batch.slice(offset, physical_size)));
                }
                black_box(aggregator.flush());
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// Local two-phase GROUP BY with 64 hot keys. Each case processes the same ordered rows; only the
// logical flush frequency changes. `physical_batch` is the old Arrow-batch-sized behavior,
// `logical_*` is the exact Flink count boundary, and size 1 is the non-coalescing baseline.
fn bench_local_group_by_logical_bundle(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_group_by_logical_bundle");
    for logical_size in [1usize, 32, 256, 4096, 50_000] {
        let rows = logical_size.max(ROWS);
        let keys: ArrayRef = Arc::new(Int64Array::from_iter_values(
            (0..rows as i64).map(|i| i % 64),
        ));
        let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows as i64));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("key0", DataType::Int64, false),
                Field::new("value0", DataType::Int64, false),
            ])),
            vec![keys, values],
        )
        .unwrap();
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(
            BenchmarkId::new("logical", logical_size),
            &logical_size,
            |b, &logical_size| {
                b.iter_batched(
                    || LocalGroupBy::sum(1, vec![0]),
                    |mut aggregator| {
                        for offset in (0..rows).step_by(logical_size) {
                            let length = logical_size.min(rows - offset);
                            aggregator.update(black_box(&batch.slice(offset, length)));
                            black_box(aggregator.flush());
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );
        if logical_size == 4096 {
            group.bench_function("physical_batch", |b| {
                b.iter_batched(
                    || LocalGroupBy::sum(1, vec![0]),
                    |mut aggregator| {
                        aggregator.update(black_box(&batch));
                        black_box(aggregator.flush());
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }
    group.finish();
}

fn bench_local_group_by_extremes(c: &mut Criterion) {
    let keys: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..ROWS as i64).map(|i| i % 64),
    ));
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..ROWS as i64).map(|i| (i * 37) % 8192),
    ));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Int64, false),
            Field::new("value0", DataType::Int64, false),
        ])),
        vec![keys, values],
    )
    .unwrap();
    let mut group = c.benchmark_group("local_group_by_extremes");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("min_max_logical_4096_physical_1024", |b| {
        b.iter_batched(
            || LocalGroupBy::new(vec![1, 2], vec![0, 0], vec![1, 1], vec![0]),
            |mut aggregator| {
                for offset in (0..ROWS).step_by(1024) {
                    aggregator.update(black_box(&batch.slice(offset, 1024)));
                }
                black_box(aggregator.flush());
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// Decode a batch of raw JSON message bodies (one document per row) into a typed columnar batch —
// the source-edge work that replaces Flink's per-record document-tree -> RowData materialization.
#[cfg(feature = "json")]
fn bench_json_decode(c: &mut Criterion) {
    use streamfusion::bench::JsonDecode;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let docs: Vec<&[u8]> = (0..ROWS)
        .map(|i| {
            // One representative shape, varied by index so the decoder does real work each row.
            Box::leak(
                format!(r#"{{"id": {i}, "name": "row-{i}", "score": {}.5}}"#, i % 100).into_boxed_str(),
            )
            .as_bytes()
        })
        .collect();
    let body: ArrayRef = Arc::new(arrow::array::BinaryArray::from(docs));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("body", DataType::Binary, true)])),
        vec![body],
    )
    .unwrap();

    let decoder = JsonDecode::new(schema.clone());
    let mut group = c.benchmark_group("json_decode");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("three_field_object", |b| {
        b.iter(|| black_box(decoder.decode(black_box(&batch))))
    });

    // A Nexmark-bid-sized document (~210 bytes): the same three projected fields plus the fields a
    // real event carries that the pruned schema skips — the shape a routed query actually decodes.
    let docs: Vec<&[u8]> = (0..ROWS)
        .map(|i| {
            Box::leak(
                format!(
                    r#"{{"id": {i}, "name": "row-{i}", "score": {}.5, "channel": "channel-{}", "url": "https://example.com/item/{i}?tab=all", "dateTime": "2026-07-01 12:{:02}:{:02}.{:03}", "extra": "IdMkfLtiXpKuwqNnWEyPTgAbCdEfGhIjKlMnOpQrStUv"}}"#,
                    i % 100,
                    i % 10,
                    i % 60,
                    (i / 60) % 60,
                    i % 1000
                )
                .into_boxed_str(),
            )
            .as_bytes()
        })
        .collect();
    let body: ArrayRef = Arc::new(arrow::array::BinaryArray::from(docs));
    let wide = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("body", DataType::Binary, true)])),
        vec![body],
    )
    .unwrap();
    group.bench_function("nexmark_bid_shape", |b| {
        b.iter(|| black_box(decoder.decode(black_box(&wide))))
    });
    group.finish();
}

#[cfg(not(feature = "json"))]
fn bench_json_decode(_c: &mut Criterion) {}

// Session SUM grouped by a key: each row opens a gap-wide window and merges any it bridges.
fn bench_session_keyed(c: &mut Criterion) {
    let gap_millis = 500;
    let ts: Vec<i64> = (0..ROWS as i64).map(|i| i * 100).collect();
    let value: Vec<i64> = (0..ROWS as i64).collect();
    let key: Vec<i64> = (0..ROWS as i64).map(|i| i % 64).collect();
    let ts_col: ArrayRef = Arc::new(Int64Array::from(ts));
    let value_col: ArrayRef = Arc::new(Int64Array::from(value));
    let key_col: ArrayRef = Arc::new(Int64Array::from(key));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, true),
            Field::new("value0", DataType::Int64, true),
            Field::new("key0", DataType::Int64, true),
        ])),
        vec![ts_col, value_col, key_col],
    )
    .unwrap();

    let mut group = c.benchmark_group("session");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("sum_keyed_update_flush", |b| {
        b.iter_batched(
            || Session::new(gap_millis, 0, vec![0]),
            |mut aggregator| {
                aggregator.update(black_box(&batch));
                black_box(aggregator.flush(i64::MAX));
            },
            BatchSize::SmallInput,
        )
    });
    // Dense sessions: same 64 keys, but consecutive rows for a key fall within the gap, so each
    // key's rows chain into one long session — the multi-row-run shape where the per-run (rather
    // than per-row) value slice pays off.
    let dense_ts: Vec<i64> = (0..ROWS as i64).map(|i| i * 5).collect();
    let dense_batch = RecordBatch::try_new(
        batch.schema(),
        vec![
            Arc::new(Int64Array::from(dense_ts)) as ArrayRef,
            batch.column(1).clone(),
            batch.column(2).clone(),
        ],
    )
    .unwrap();
    group.bench_function("sum_keyed_dense_update_flush", |b| {
        b.iter_batched(
            || Session::new(gap_millis, 0, vec![0]),
            |mut aggregator| {
                aggregator.update(black_box(&dense_batch));
                black_box(aggregator.flush(i64::MAX));
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// Columnar OVER over `[k, value, rt]`, 64 keys: the running fold per partition plus the
// complete/pending split and passthrough, for a running SUM (DataFusion accumulator) and for
// ROW_NUMBER (per-key counter).
fn bench_over(c: &mut Criterion) {
    let k: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).map(|i| i % 64).collect::<Vec<_>>()));
    let value: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>()));
    let rt: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>()));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
            Field::new("rt", DataType::Int64, false),
        ])),
        vec![k, value, rt],
    )
    .unwrap();

    let mut group = c.benchmark_group("over");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("running_sum_keyed", |b| {
        b.iter_batched(
            || Over::new(0, vec![0], 2, Some(1), vec![0]), // bigint SUM; rt col 2, value col 1, key col 0
            |mut over| {
                over.push(black_box(batch.clone()));
                black_box(over.flush(i64::MAX));
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("row_number_keyed", |b| {
        b.iter_batched(
            || Over::new(0, vec![10], 2, None, vec![0]), // ROW_NUMBER; no value column
            |mut over| {
                over.push(black_box(batch.clone()));
                black_box(over.flush(i64::MAX));
            },
            BatchSize::SmallInput,
        )
    });
    // Bounded ROWS frame (SUM over the 10 preceding rows): the per-key buffer append plus the
    // per-row frame recompute — the third keyed OVER loop, not covered by the two above.
    group.bench_function("bounded_rows_sum_keyed", |b| {
        b.iter_batched(
            || Over::bounded(0, vec![0], 2, 1, vec![0], true, 10),
            |mut over| {
                over.push(black_box(batch.clone()));
                black_box(over.flush(i64::MAX));
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// Retracting Top-N (changelog input, full per-partition buffers): 64 partitions, top 10 by value
// descending. Steady state is a batch of inserts into already-populated buffers — every row pays
// the partition probe, the ordered insert, and the before/after top-N diff.
fn bench_retract_topn(c: &mut Criterion) {
    let k: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).map(|i| i % 64).collect::<Vec<_>>()));
    let v: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).map(|i| (i * 37) % 8192).collect::<Vec<_>>()));
    let s: ArrayRef = Arc::new(StringArray::from(
        (0..ROWS).map(|i| format!("payload-{i}")).collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ])),
        vec![k, v, s],
    )
    .unwrap();

    let mut group = c.benchmark_group("retract_topn");
    group.throughput(Throughput::Elements(ROWS as u64));
    let physical_size = 256;
    group.bench_function("immediate", |b| {
        b.iter_batched(
            || {
                let mut ranker = RetractTopN::new(vec![0], vec![(1, false)], 10);
                ranker.push(&batch); // pre-populate the buffers; the measured push is steady-state
                ranker
            },
            |mut ranker| {
                for offset in (0..ROWS).step_by(physical_size) {
                    black_box(ranker.push(&batch.slice(offset, physical_size)));
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("physical_256", |b| {
        b.iter_batched(
            || {
                let mut ranker = RetractTopN::new_mini_batch(vec![0], vec![(1, false)], 10);
                ranker.push(&batch);
                ranker.flush();
                ranker
            },
            |mut ranker| {
                for offset in (0..ROWS).step_by(physical_size) {
                    black_box(ranker.push(&batch.slice(offset, physical_size)));
                    black_box(ranker.flush());
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("logical_4096", |b| {
        b.iter_batched(
            || {
                let mut ranker = RetractTopN::new_mini_batch(vec![0], vec![(1, false)], 10);
                ranker.push(&batch);
                ranker.flush();
                ranker
            },
            |mut ranker| {
                for offset in (0..ROWS).step_by(physical_size) {
                    black_box(ranker.push(&batch.slice(offset, physical_size)));
                }
                black_box(ranker.flush());
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_unique_updating_join_logical_bundle(c: &mut Criterion) {
    let data_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let changelog_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("$row_kind$", DataType::Int8, false),
    ]));
    let right = RecordBatch::try_new(
        changelog_schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..64)),
            Arc::new(Int64Array::from_iter_values((0..64).map(|key| 10_000 + key))),
            Arc::new(Int8Array::from(vec![0; 64])),
        ],
    )
    .unwrap();
    let left = RecordBatch::try_new(
        changelog_schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..64)),
            Arc::new(Int64Array::from_iter_values(0..64)),
            Arc::new(Int8Array::from(vec![0; 64])),
        ],
    )
    .unwrap();
    let mut keys = Vec::with_capacity(ROWS);
    let mut values = Vec::with_capacity(ROWS);
    let mut kinds = Vec::with_capacity(ROWS);
    for pair in 0..(ROWS / 2) {
        let key = (pair % 64) as i64;
        let round = pair / 64;
        keys.extend([key, key]);
        values.push(if round == 0 { key } else { 1_000 + ((round - 1) * 64) as i64 + key });
        values.push(1_000 + (round * 64) as i64 + key);
        kinds.extend([3, 0]);
    }
    let changes = RecordBatch::try_new(
        changelog_schema,
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap();
    let physical_size = 256;
    let setup = |mini_batch| {
        let mut join = UniqueUpdatingJoin::new(data_schema.clone(), mini_batch);
        join.push(&right, false);
        join.push(&left, true);
        if mini_batch {
            join.flush();
        }
        join
    };
    let mut group = c.benchmark_group("unique_updating_join_logical_bundle");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("immediate", |b| {
        b.iter_batched(
            || setup(false),
            |mut join| {
                for offset in (0..ROWS).step_by(physical_size) {
                    black_box(join.push(&changes.slice(offset, physical_size), true));
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("physical_256", |b| {
        b.iter_batched(
            || setup(true),
            |mut join| {
                for offset in (0..ROWS).step_by(physical_size) {
                    black_box(join.push(&changes.slice(offset, physical_size), true));
                    black_box(join.flush());
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("logical_4096", |b| {
        b.iter_batched(
            || setup(true),
            |mut join| {
                for offset in (0..ROWS).step_by(physical_size) {
                    black_box(join.push(&changes.slice(offset, physical_size), true));
                }
                black_box(join.flush());
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// Append-only Top-10 with sustained boundary churn: descending values continually enter each
// partition's ascending top-N. Compare Flink-compatible cascades, the former physical-batch net
// diff cadence, and one net diff over the full logical bundle.
fn bench_append_topn_logical_bundle(c: &mut Criterion) {
    let keys: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (0..ROWS as i64).map(|i| i % 64),
    ));
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values((0..ROWS as i64).rev()));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("partition", DataType::Int64, false),
            Field::new("sort", DataType::Int64, false),
        ])),
        vec![keys, values],
    )
    .unwrap();
    let physical_size = 256;
    let mut group = c.benchmark_group("append_topn_logical_bundle");
    group.throughput(Throughput::Elements(ROWS as u64));

    for output_rank in [false, true] {
        let suffix = if output_rank { "rank" } else { "membership" };
        group.bench_function(format!("cascade_{suffix}"), |b| {
            b.iter_batched(
                || AppendTopN::new(vec![0], vec![(1, true)], 10, output_rank, false),
                |mut ranker| {
                    for offset in (0..ROWS).step_by(physical_size) {
                        black_box(ranker.push(&batch.slice(offset, physical_size)));
                    }
                },
                BatchSize::SmallInput,
            )
        });
        group.bench_function(format!("physical_diff_{suffix}"), |b| {
            b.iter_batched(
                || AppendTopN::new(vec![0], vec![(1, true)], 10, output_rank, true),
                |mut ranker| {
                    for offset in (0..ROWS).step_by(physical_size) {
                        black_box(ranker.push(&batch.slice(offset, physical_size)));
                        black_box(ranker.flush());
                    }
                },
                BatchSize::SmallInput,
            )
        });
        group.bench_function(format!("logical_diff_{suffix}"), |b| {
            b.iter_batched(
                || AppendTopN::new(vec![0], vec![(1, true)], 10, output_rank, true),
                |mut ranker| {
                    for offset in (0..ROWS).step_by(physical_size) {
                        black_box(ranker.push(&batch.slice(offset, physical_size)));
                    }
                    black_box(ranker.flush());
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

// Keep-first dedup: 256 keys over 4096 rows. Steady state is the post-emit phase — every key has
// already fired, so each row is one emitted-set probe and a drop; the push+flush pair measures
// both the per-batch reduction and the emitted-set growth path.
fn bench_dedup_keep_first(c: &mut Criterion) {
    let k: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).map(|i| i % 256).collect::<Vec<_>>()));
    let rt: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>()));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("rt", DataType::Int64, false),
        ])),
        vec![k, rt],
    )
    .unwrap();

    let mut group = c.benchmark_group("dedup");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("keep_first_emitted_probe", |b| {
        b.iter_batched(
            || {
                let mut dedup = KeepFirstDedup::new(vec![0], 1);
                dedup.push(&batch);
                dedup.flush(i64::MAX); // all 256 keys emitted; the measured push probes them
                dedup
            },
            |mut dedup| {
                dedup.push(black_box(&batch));
                black_box(dedup.flush(i64::MAX));
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// Changelog normalization under a replacement storm: 64 keys are inserted once and then updated
// round-robin. The strategies differ only in how often they materialize externally visible output.
fn bench_normalize_logical_bundle(c: &mut Criterion) {
    let keys: Vec<i64> = (0..ROWS as i64).map(|row| row % 64).collect();
    let values: Vec<i64> = (0..ROWS as i64).collect();
    let kinds: Vec<i8> = (0..ROWS).map(|row| if row < 64 { 0 } else { 2 }).collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("$row_kind$", DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap();

    let mut group = c.benchmark_group("normalize_logical_bundle");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("immediate", |b| {
        b.iter_batched(
            || Normalize::new(vec![0], true, false),
            |mut normalize| black_box(normalize.push(black_box(&batch))),
            BatchSize::SmallInput,
        )
    });
    group.bench_function("physical_256", |b| {
        b.iter_batched(
            || Normalize::new(vec![0], true, true),
            |mut normalize| {
                for offset in (0..ROWS).step_by(256) {
                    black_box(normalize.push(black_box(&batch.slice(offset, 256))));
                    black_box(normalize.flush());
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("logical_4096", |b| {
        b.iter_batched(
            || Normalize::new(vec![0], true, true),
            |mut normalize| {
                black_box(normalize.push(black_box(&batch)));
                black_box(normalize.flush());
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_keep_last_logical_bundle(c: &mut Criterion) {
    let keys: Vec<i64> = (0..ROWS as i64).map(|row| row % 64).collect();
    let values: Vec<i64> = (0..ROWS as i64).collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ])),
        vec![Arc::new(Int64Array::from(keys)), Arc::new(Int64Array::from(values))],
    )
    .unwrap();
    let mut group = c.benchmark_group("keep_last_logical_bundle");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("immediate", |b| {
        b.iter_batched(
            || KeepLastDedup::new(vec![0], false),
            |mut dedup| black_box(dedup.push(black_box(&batch))),
            BatchSize::SmallInput,
        )
    });
    group.bench_function("physical_256", |b| {
        b.iter_batched(
            || KeepLastDedup::new(vec![0], true),
            |mut dedup| {
                for offset in (0..ROWS).step_by(256) {
                    black_box(dedup.push(black_box(&batch.slice(offset, 256))));
                    black_box(dedup.flush());
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("logical_4096", |b| {
        b.iter_batched(
            || KeepLastDedup::new(vec![0], true),
            |mut dedup| {
                black_box(dedup.push(black_box(&batch)));
                black_box(dedup.flush());
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// The columnar exchange's by-key split: hash every row's key to a partition and gather the
// sub-batches — the whole per-batch cost of the native shuffle's split side.
fn bench_exchange_split(c: &mut Criterion) {
    let k: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).map(|i| i % 64).collect::<Vec<_>>()));
    let v: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>()));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![k, v],
    )
    .unwrap();

    let mut group = c.benchmark_group("exchange");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("split_by_key_8", |b| {
        b.iter(|| black_box(split_by_key(black_box(&batch), &[0], 8)))
    });
    group.finish();
}

// `[k, v, rt]` with a unique key per row, so the equi-join is 1:1 (no cross product) and the bench
// measures the join machinery rather than output volume.
fn join_batch() -> RecordBatch {
    let k: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>()));
    let v: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>()));
    let rt: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>()));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("rt", DataType::Int64, false),
        ])),
        vec![k, v, rt],
    )
    .unwrap()
}

// Interval join: with the left side already buffered, measure one right-batch push (which builds and
// runs a DataFusion hash join with the interval as a residual filter).
fn bench_interval_join(c: &mut Criterion) {
    let batch = join_batch();
    let mut group = c.benchmark_group("interval_join");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("equi_key_push", |b| {
        b.iter_batched(
            || {
                let mut join =
                    IntervalJoin::new(vec![0], vec![0], 2, 2, 0, 0, batch.schema(), batch.schema());
                join.push_left(batch.clone());
                join
            },
            |mut join| black_box(join.push_right(black_box(batch.clone()))),
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// Window join: with both sides buffered (one window per 64-row group), measure one flush (which
// builds and runs a DataFusion hash join keyed on the user key plus the window bounds).
fn bench_window_join(c: &mut Criterion) {
    let k: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>()));
    let v: ArrayRef = Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>()));
    let ws: Vec<i64> = (0..ROWS as i64).map(|i| (i / 64) * 1000).collect();
    let we: Vec<i64> = ws.iter().map(|s| s + 1000).collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, true),
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
        ])),
        vec![k, v, Arc::new(Int64Array::from(ws)), Arc::new(Int64Array::from(we))],
    )
    .unwrap();

    let mut group = c.benchmark_group("window_join");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("equi_key_flush", |b| {
        b.iter_batched(
            || {
                let mut join =
                    WindowJoin::new(vec![0], vec![0], 2, 3, 2, 3, batch.schema(), batch.schema());
                join.push_left(batch.clone());
                join.push_right(batch.clone());
                join
            },
            |mut join| black_box(join.flush(i64::MAX)),
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

/// The DATE_FORMAT hot loop, before and after the compile-once change: `per_row_parse` is the old
/// formulation (chrono re-parses the pattern inside every Display and renders into a fresh String
/// per row); `compiled` parses the pattern once and renders into a reused buffer. 4096 timestamps,
/// the Nexmark 'yyyy-MM-dd' pattern (%Y-%m-%d).
fn bench_date_format(c: &mut Criterion) {
    let mut group = c.benchmark_group("date_format");
    group.throughput(Throughput::Elements(4096));
    let times: Vec<i64> = (0..4096i64).map(|i| 1_700_000_000_000 + i * 977).collect();
    group.bench_function("per_row_parse", |b| {
        b.iter(|| {
            let mut out = Vec::with_capacity(times.len());
            for &t in &times {
                let wall = chrono::DateTime::from_timestamp_millis(t).unwrap().naive_utc();
                out.push(wall.format("%Y-%m-%d").to_string());
            }
            out
        })
    });
    group.bench_function("compiled", |b| {
        use std::fmt::Write as _;
        let items =
            chrono::format::StrftimeItems::new("%Y-%m-%d").parse_to_owned().expect("pattern");
        b.iter(|| {
            let mut builder = arrow::array::StringBuilder::new();
            let mut buf = String::new();
            for &t in &times {
                let wall = chrono::DateTime::from_timestamp_millis(t).unwrap().naive_utc();
                buf.clear();
                write!(buf, "{}", wall.format_with_items(items.iter())).unwrap();
                builder.append_value(&buf);
            }
            builder.finish()
        })
    });
    // The shipped path: the compiled items lowered once to a digit-writing plan (see
    // CompiledFormat::render_fast), skipping chrono's Display machinery per row.
    group.bench_function("digit_plan", |b| {
        use chrono::Datelike;
        let push2 = |buf: &mut String, v: u32| {
            buf.push((b'0' + (v / 10) as u8) as char);
            buf.push((b'0' + (v % 10) as u8) as char);
        };
        b.iter(|| {
            let mut builder = arrow::array::StringBuilder::new();
            let mut buf = String::new();
            for &t in &times {
                let wall = chrono::DateTime::from_timestamp_millis(t).unwrap().naive_utc();
                buf.clear();
                let year = wall.year() as u32;
                push2(&mut buf, year / 100);
                push2(&mut buf, year % 100);
                buf.push('-');
                push2(&mut buf, wall.month());
                buf.push('-');
                push2(&mut buf, wall.day());
                builder.append_value(&buf);
            }
            builder.finish()
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_filter,
    bench_tumbling,
    bench_tumbling_keyed,
    bench_group_by_string_key,
    bench_group_by_logical_bundle,
    bench_local_group_by_logical_bundle,
    bench_local_group_by_extremes,
    bench_json_decode,
    bench_session_keyed,
    bench_over,
    bench_retract_topn,
    bench_unique_updating_join_logical_bundle,
    bench_append_topn_logical_bundle,
    bench_dedup_keep_first,
    bench_normalize_logical_bundle,
    bench_keep_last_logical_bundle,
    bench_exchange_split,
    bench_interval_join,
    bench_window_join,
    bench_date_format
);
criterion_main!(benches);
