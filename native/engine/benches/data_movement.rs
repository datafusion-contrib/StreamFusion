//! Production transformations and stateful paths missing from the historical operator suite.
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, Int64Builder, ListBuilder, RecordBatch, StringArray};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use streamfusion::bench::{
    decode_ipc, encode_ipc, expand_grouping_sets, unnest, ArrivalFirstN, Projection, TemporalJoin,
    TemporalSort,
};
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn batch(rows: usize, width: usize, keys: usize) -> RecordBatch {
    let text = "x".repeat(width);
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| (i % keys) as i64),
            )) as ArrayRef,
        ),
        (
            "rowtime",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).rev().map(|i| i as i64),
            )) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(StringArray::from_iter(
                (0..rows).map(|i| (i % 7 != 0).then_some(text.as_str())),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn transformations(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("engine/data_movement");
    for rows in [16, 1024, 16384] {
        for width in [8, 264] {
            let input = batch(rows, width, 64);
            group.throughput(Throughput::Elements(rows as u64));
            let mut projection = Projection::columns(&[0, 2]);
            projection.run(input.clone());
            let label = format!("projection/{rows}/bytes={width}");
            let (output, allocations) = measure(|| projection.run(input.clone()));
            assert_eq!(output.num_rows(), rows);
            report(&label, input.columns(), output.columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(projection.run(std::hint::black_box(input.clone()))))
            });
            let label = format!("expand/{rows}/bytes={width}");
            let (output, allocations) = measure(|| expand_grouping_sets(&input));
            assert_eq!(output.num_rows(), rows * 2);
            report(&label, input.columns(), output.columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(expand_grouping_sets(std::hint::black_box(&input))))
            });
            let encoded = encode_ipc(&input);
            assert_eq!(decode_ipc(&encoded), vec![input.clone()]);
            let label = format!("ipc_encode/{rows}/bytes={width}");
            let (output, allocations) = measure(|| encode_ipc(&input));
            report(&label, input.columns(), &[], allocations);
            std::hint::black_box(output);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(encode_ipc(std::hint::black_box(&input))))
            });
            let label = format!("ipc_decode/{rows}/bytes={width}");
            let (output, allocations) = measure(|| decode_ipc(&encoded));
            report(&label, &[], output[0].columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(decode_ipc(std::hint::black_box(&encoded))))
            });
        }
        for width in [0, 4, 64] {
            let mut arrays = ListBuilder::new(Int64Builder::new());
            for row in 0..rows {
                for value in 0..width {
                    arrays
                        .values()
                        .append_option((value % 7 != 0).then_some(value as i64));
                }
                arrays.append(row % 7 != 0);
            }
            let input = RecordBatch::try_from_iter(vec![
                (
                    "key",
                    Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                ),
                ("items", Arc::new(arrays.finish()) as ArrayRef),
            ])
            .unwrap();
            for left in [false, true] {
                let label = format!("unnest/{rows}/width={width}/left={left}");
                let (output, allocations) = measure(|| unnest(&input, left, true));
                report(&label, input.columns(), output.columns(), allocations);
                group.bench_function(BenchmarkId::from_parameter(label), |b| {
                    b.iter(|| {
                        std::hint::black_box(unnest(std::hint::black_box(&input), left, true))
                    })
                });
            }
        }
    }
    group.finish();
}

fn stateful(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/stateful");
    for rows in [16, 1024, 16384] {
        for keys in [1, 64, rows] {
            let input = batch(rows, 264, keys);
            group.throughput(Throughput::Elements(rows as u64));
            let label = format!("first_n/{rows}/keys={keys}");
            let mut operator = ArrivalFirstN::new(vec![0], 4);
            let (output, allocations) = measure(|| operator.push(&input));
            assert!(output.num_rows() <= rows);
            report(&label, input.columns(), output.columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter_batched(
                    || ArrivalFirstN::new(vec![0], 4),
                    |mut operator| {
                        std::hint::black_box(operator.push(std::hint::black_box(&input)))
                    },
                    BatchSize::LargeInput,
                )
            });
            let label = format!("temporal_sort/{rows}/keys={keys}");
            let run = |mut operator: TemporalSort| {
                operator.push(input.clone());
                operator.flush(i64::MAX)
            };
            let operator = TemporalSort::new(1);
            let (output, allocations) = measure(|| run(operator));
            assert_eq!(output.num_rows(), rows);
            report(&label, input.columns(), output.columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter_batched(
                    || TemporalSort::new(1),
                    |operator| std::hint::black_box(run(operator)),
                    BatchSize::LargeInput,
                )
            });
            let label = format!("temporal_join/{rows}/keys={keys}");
            let run = |mut operator: TemporalJoin| {
                operator.push(&input, false);
                operator.push(&input, true);
                operator.flush(i64::MAX)
            };
            let operator = TemporalJoin::new(input.schema());
            let (output, allocations) = measure(|| run(operator));
            assert_eq!(output.num_rows(), rows);
            report(&label, input.columns(), output.columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter_batched(
                    || TemporalJoin::new(input.schema()),
                    |operator| std::hint::black_box(run(operator)),
                    BatchSize::LargeInput,
                )
            });
            let mut operator = TemporalJoin::new(input.schema());
            operator.push(&input, false);
            operator.push(&input, true);
            let label = format!("temporal_join_snapshot/{rows}/keys={keys}");
            let (output, allocations) = measure(|| operator.snapshot());
            report(&label, input.columns(), &[], allocations);
            assert!(!output.is_empty());
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter(|| std::hint::black_box(operator.snapshot()))
            });
        }
    }
    group.finish();
}

criterion_group!(benches, transformations, stateful);
criterion_main!(benches);
