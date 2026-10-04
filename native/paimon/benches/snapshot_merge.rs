use arrow::array::{ArrayRef, Int64Array, Int8Array, RecordBatch, StringArray};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
use streamfusion_paimon::bench::merge;
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
fn snapshot(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("paimon/snapshot_merge");
    for rows in [16, 1024, 16384] {
        for count in [1, 4, 16] {
            for width in [8, 264] {
                let text = "x".repeat(width);
                let runs: Vec<_> = (0..count)
                    .map(|run| {
                        vec![RecordBatch::try_from_iter(vec![
                            (
                                "key",
                                Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
                            ),
                            (
                                "seq",
                                Arc::new(Int64Array::from(vec![run as i64; rows])) as ArrayRef,
                            ),
                            (
                                "kind",
                                Arc::new(Int8Array::from(vec![0i8; rows])) as ArrayRef,
                            ),
                            (
                                "value",
                                Arc::new(StringArray::from_iter(
                                    (0..rows).map(|i| (i % 7 != 0).then_some(text.as_str())),
                                )) as ArrayRef,
                            ),
                        ])
                        .unwrap()]
                    })
                    .collect();
                let inputs: Vec<_> = runs
                    .iter()
                    .flat_map(|run| run[0].columns().iter().cloned())
                    .collect();
                group.throughput(Throughput::Elements((rows * count) as u64));
                for (first, partial) in [(false, false), (true, false), (false, true)] {
                    let label = format!(
                        "{rows}/runs={count}/bytes={width}/first={first}/partial={partial}"
                    );
                    let (output, allocations) = measure(|| merge(&runs, 1024, first, partial));
                    assert_eq!(
                        output.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        rows
                    );
                    let columns: Vec<_> = output
                        .iter()
                        .flat_map(|batch| batch.columns().iter().cloned())
                        .collect();
                    report(&label, &inputs, &columns, allocations);
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter(|| {
                            std::hint::black_box(merge(
                                std::hint::black_box(&runs),
                                1024,
                                first,
                                partial,
                            ))
                        })
                    });
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, snapshot);
criterion_main!(benches);
