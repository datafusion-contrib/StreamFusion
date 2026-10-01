use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::PersistentSort;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
const OPTIONS: &str = include_str!("fixtures/rocks-options.json");
fn persistent(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("engine/rocksdb");
    for rows in [16, 1024, 16384] {
        for width in [8, 264] {
            let text = "x".repeat(width);
            let input = RecordBatch::try_from_iter(vec![
                (
                    "value",
                    Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                        text.as_str(),
                        rows,
                    ))) as ArrayRef,
                ),
                (
                    "rt",
                    Arc::new(Int64Array::from_iter_values((0..rows as i64).rev())) as ArrayRef,
                ),
            ])
            .unwrap();
            let setup = || {
                let directory = tempfile::tempdir().unwrap();
                let operator = PersistentSort::new(
                    directory.path().join("db").to_str().unwrap(),
                    input.schema(),
                    OPTIONS,
                );
                (operator, directory)
            };
            group.throughput(Throughput::Elements(rows as u64));
            let label = format!("temporal_sort/{rows}/bytes={width}");
            let (mut operator, _directory) = setup();
            let (output, allocations) = measure(|| {
                operator.push(&input);
                operator.flush()
            });
            assert_eq!(output.num_rows(), rows);
            report(&label, input.columns(), output.columns(), allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter_batched_ref(
                    &setup,
                    |(operator, _)| {
                        operator.push(&input);
                        std::hint::black_box(operator.flush())
                    },
                    BatchSize::PerIteration,
                )
            });
            let snapshot_setup = || {
                let (mut operator, directory) = setup();
                operator.push(&input);
                (operator, directory)
            };
            let label = format!("temporal_sort_checkpoint/{rows}/bytes={width}");
            let (mut operator, directory) = snapshot_setup();
            let snapshot = directory.path().join("snapshot");
            let (_, allocations) = measure(|| operator.checkpoint(snapshot.to_str().unwrap()));
            assert!(snapshot.exists());
            report(&label, &[], &[], allocations);
            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                b.iter_batched_ref(
                    &snapshot_setup,
                    |(operator, directory)| {
                        operator.checkpoint(directory.path().join("snapshot").to_str().unwrap())
                    },
                    BatchSize::PerIteration,
                )
            });
        }
    }
    group.finish();
}
criterion_group!(benches, persistent);
criterion_main!(benches);
