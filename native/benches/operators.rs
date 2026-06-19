//! Operator micro-benchmarks: each measures a native operator's steady-state hot loop over an
//! in-memory Arrow batch, isolated from the JVM bridge and Flink job scheduling. Run with
//! `cargo bench`; Criterion reports time per batch, from which rows/s follows.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use streamfusion::bench::{Filter, Tumbling};

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
            Field::new("value", DataType::Int64, true),
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
            Field::new("value", DataType::Int64, true),
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
    group.finish();
}

criterion_group!(benches, bench_filter, bench_tumbling, bench_tumbling_keyed);
criterion_main!(benches);
