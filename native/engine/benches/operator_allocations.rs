//! Matching timing and allocation probes for the production operator hot paths.
use arrow::array::{ArrayRef, Int64Array, Int8Array, RecordBatch, StringArray};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::*;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
fn operation<T>(
    c: &mut Criterion,
    label: String,
    input: &RecordBatch,
    make: impl Fn() -> T,
    run: impl Fn(&mut T) -> RecordBatch,
) {
    let mut operator = make();
    let (output, allocations) = measure(|| run(&mut operator));
    assert!(
        output.num_rows() > 0,
        "{label} must exercise output materialization"
    );
    report(&label, input.columns(), output.columns(), allocations);
    let mut group = c.benchmark_group("engine/operator_allocations");
    group.throughput(Throughput::Elements(input.num_rows() as u64));
    group.bench_function(BenchmarkId::from_parameter(label), |b| {
        b.iter_batched_ref(
            &make,
            |operator| std::hint::black_box(run(operator)),
            BatchSize::LargeInput,
        )
    });
    group.finish();
}
fn operators(c: &mut Criterion) {
    header();
    for rows in [16, 1024, 16384] {
        for keys in [1, 64, rows] {
            let input = RecordBatch::try_from_iter(vec![
                (
                    "key",
                    Arc::new(Int64Array::from_iter_values(
                        (0..rows).map(|i| (i % keys) as i64),
                    )) as ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int64Array::from_iter(
                        (0..rows).map(|i| (i % 7 != 0).then_some(i as i64)),
                    )) as ArrayRef,
                ),
                (
                    "rt",
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                ),
                (
                    "payload",
                    Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                        "x".repeat(264),
                        rows,
                    ))) as ArrayRef,
                ),
                (
                    "$row_kind$",
                    Arc::new(Int8Array::from(vec![0i8; rows])) as ArrayRef,
                ),
            ])
            .unwrap();
            let label = |name: &str| format!("{name}/{rows}/keys={keys}");
            let right = input.slice(0, keys.min(rows));
            for first in [false, true] {
                operation(
                    c,
                    label(&format!("paimon_upsert_merge/first={first}")),
                    &input,
                    || UpsertMerge::new(4, first),
                    |op| op.run(&input),
                );
            }
            operation(
                c,
                label("filter"),
                &input,
                || {
                    Filter::new(
                        vec![6, 0, 1],
                        vec![10, 1, 0],
                        vec![2, 0, 0],
                        vec![0],
                        vec![],
                        vec![],
                    )
                },
                |op| op.run(input.clone()),
            );
            for mini in [false, true] {
                operation(
                    c,
                    label(&format!("append_topn/mini={mini}")),
                    &input,
                    || AppendTopN::new(vec![0], vec![(1, true)], 4, true, mini),
                    |op| {
                        let output = op.push(&input);
                        if mini {
                            op.flush()
                        } else {
                            output
                        }
                    },
                );
                operation(
                    c,
                    label(&format!("keep_last/mini={mini}")),
                    &input,
                    || KeepLastDedup::new(vec![0], mini),
                    |op| {
                        let output = op.push(&input);
                        if mini {
                            op.flush()
                        } else {
                            output
                        }
                    },
                );
                operation(
                    c,
                    label(&format!("normalize/mini={mini}")),
                    &input,
                    || Normalize::new(vec![0], true, mini),
                    |op| {
                        let output = op.push(&input);
                        if mini {
                            op.flush()
                        } else {
                            output
                        }
                    },
                );
                operation(
                    c,
                    label(&format!("updating_join/mini={mini}")),
                    &input,
                    || {
                        UniqueUpdatingJoin::new(
                            input.project(&[0, 1, 2, 3]).unwrap().schema(),
                            mini,
                        )
                    },
                    |op| {
                        op.push(&input, true);
                        let output = op.push(&right, false);
                        if mini {
                            op.flush()
                        } else {
                            output
                        }
                    },
                );
            }
            operation(
                c,
                label("retract_topn"),
                &input,
                || RetractTopN::new(vec![0], vec![(1, true)], 4),
                |op| op.push(&input),
            );
            operation(
                c,
                label("keep_first"),
                &input,
                || KeepFirstDedup::new(vec![0], 2),
                |op| {
                    op.push(&input);
                    op.flush(i64::MAX)
                },
            );
            operation(
                c,
                label("group_sum"),
                &input,
                || GroupBy::new(vec![0], vec![0], vec![1], vec![0]),
                |op| op.update(&input),
            );
            operation(
                c,
                label("local_group_sum"),
                &input,
                || LocalGroupBy::sum(1, vec![0]),
                |op| {
                    op.update(&input);
                    op.flush()
                },
            );
            operation(
                c,
                label("over_running"),
                &input,
                || Over::new(0, vec![0], 2, Some(1), vec![0]),
                |op| {
                    op.push(input.clone());
                    op.flush(i64::MAX)
                },
            );
            for rows_frame in [true, false] {
                operation(
                    c,
                    label(&format!("over_bounded/rows_frame={rows_frame}")),
                    &input,
                    || Over::bounded(0, vec![0], 2, 1, vec![0], rows_frame, 4),
                    |op| {
                        op.push(input.clone());
                        op.flush(i64::MAX)
                    },
                );
            }
            // Window aggregate's production input convention is [time, values..., keys...].
            let window_input = RecordBatch::try_from_iter(vec![
                ("ts", input.column(2).clone()),
                ("value0", input.column(1).clone()),
                ("key0", input.column(0).clone()),
            ])
            .unwrap();
            operation(
                c,
                label("tumbling"),
                &window_input,
                || Tumbling::new(1000, 0, vec![0]),
                |op| {
                    op.update(&window_input);
                    op.flush(i64::MAX)
                },
            );
            operation(
                c,
                label("session"),
                &window_input,
                || Session::new(10, 0, vec![0]),
                |op| {
                    op.update(&window_input);
                    op.flush(i64::MAX)
                },
            );
            operation(
                c,
                label("interval_join"),
                &input,
                || IntervalJoin::new(vec![0], vec![0], 2, 2, 0, 0, input.schema(), input.schema()),
                |op| {
                    op.push_left(input.clone());
                    op.push_right(input.clone())
                },
            );
            let start: ArrayRef = Arc::new(Int64Array::from(vec![0; rows]));
            let end: ArrayRef = Arc::new(Int64Array::from(vec![1000; rows]));
            // Unique join keys bound output to input size even in the low-cardinality state profiles.
            let window = RecordBatch::try_from_iter(vec![
                ("key", input.column(2).clone()),
                ("value", input.column(1).clone()),
                ("window_start", start),
                ("window_end", end),
            ])
            .unwrap();
            operation(
                c,
                label("window_join"),
                &window,
                || {
                    WindowJoin::new(
                        vec![0],
                        vec![0],
                        2,
                        3,
                        2,
                        3,
                        window.schema(),
                        window.schema(),
                    )
                },
                |op| {
                    op.push_left(window.clone());
                    op.push_right(window.clone());
                    op.flush(i64::MAX)
                },
            );
        }
    }
}
criterion_group!(benches, operators);
criterion_main!(benches);
