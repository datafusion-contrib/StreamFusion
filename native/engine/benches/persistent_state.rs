use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt32Array};
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
            for nullable in [false, true] {
                let text = "x".repeat(width);
                let input = RecordBatch::try_from_iter(vec![
                    (
                        "value",
                        Arc::new(StringArray::from_iter((0..rows + 1).map(|i| {
                            if nullable && i % 7 == 0 {
                                None
                            } else {
                                Some(format!("{i}/{text}"))
                            }
                        }))) as ArrayRef,
                    ),
                    (
                        "rt",
                        Arc::new(Int64Array::from_iter_values((0..=rows as i64).rev())) as ArrayRef,
                    ),
                ])
                .unwrap()
                .slice(1, rows);
                let reversed = UInt32Array::from_iter_values((0..rows as u32).rev());
                let expected = RecordBatch::try_new(
                    input.schema(),
                    input
                        .columns()
                        .iter()
                        .map(|array| arrow::compute::take(array, &reversed, None).unwrap())
                        .collect(),
                )
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
                let label = format!("temporal_sort/{rows}/bytes={width}/nulls={nullable}");
                let (mut operator, _directory) = setup();
                let (output, allocations) = measure(|| {
                    operator.push(&input);
                    operator.flush()
                });
                assert_eq!(output, expected);
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
                let label =
                    format!("temporal_sort_checkpoint/{rows}/bytes={width}/nulls={nullable}");
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
                let (mut operator, source_directory) = snapshot_setup();
                let source_path = source_directory.path().join("restore-source");
                let source = source_path.to_str().unwrap();
                let generation = operator.checkpoint(source);
                drop(operator);
                for aligned in [false, true] {
                    let restore = |directory: &tempfile::TempDir| {
                        PersistentSort::restore(
                            directory.path().join("restored").to_str().unwrap(),
                            input.schema(),
                            OPTIONS,
                            source,
                            generation,
                            aligned,
                        )
                    };
                    let label = format!("temporal_sort_restore/{rows}/bytes={width}/nulls={nullable}/aligned={aligned}");
                    let directory = tempfile::tempdir().unwrap();
                    let (mut restored, allocations) = measure(|| restore(&directory));
                    let tail = input.slice(0, 1);
                    restored.push(&tail);
                    let continued =
                        arrow::compute::concat_batches(&input.schema(), [&expected, &tail])
                            .unwrap();
                    assert_eq!(restored.flush(), continued);
                    report(&label, &[], &[], allocations);
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter_batched_ref(
                            || tempfile::tempdir().unwrap(),
                            |directory| {
                                let restored = restore(directory);
                                std::hint::black_box(&restored);
                                drop(restored);
                            },
                            BatchSize::PerIteration,
                        )
                    });
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, persistent);
criterion_main!(benches);
