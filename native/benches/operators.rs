//! Operator micro-benchmarks: each measures a native operator's steady-state hot loop over an
//! in-memory Arrow batch, isolated from the JVM bridge and Flink job scheduling. Run with
//! `cargo bench`; Criterion reports time per batch, from which rows/s follows.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use streamfusion::bench::{Filter, IntervalJoin, Over, Session, Tumbling, WindowJoin};

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

// Decode a batch of raw JSON message bodies (one document per row) into a typed columnar batch —
// the source-edge work that replaces Flink's per-record document-tree -> RowData materialization.
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

    let decoder = JsonDecode::new(schema);
    let mut group = c.benchmark_group("json_decode");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("three_field_object", |b| {
        b.iter(|| black_box(decoder.decode(black_box(&batch))))
    });
    group.finish();
}

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
                let mut join = IntervalJoin::new(vec![0], vec![0], 2, 2, 0, 0);
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
                let mut join = WindowJoin::new(vec![0], vec![0], 2, 3, 2, 3);
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

criterion_group!(
    benches,
    bench_filter,
    bench_tumbling,
    bench_tumbling_keyed,
    bench_group_by_string_key,
    bench_json_decode,
    bench_session_keyed,
    bench_over,
    bench_interval_join,
    bench_window_join
);
criterion_main!(benches);
