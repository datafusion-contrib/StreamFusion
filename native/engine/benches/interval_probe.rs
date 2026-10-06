//! Selective interval probes against retained state; setup is outside timing.
use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use std::sync::Arc;
use streamfusion::bench::IntervalJoin;

fn input(rows: usize, domain: usize, width: usize) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| (i % domain) as i64),
            )) as ArrayRef,
        ),
        (
            "v",
            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                (i % 7 != 0).then(|| format!("{i}/{}", "x".repeat(width)))
            }))) as ArrayRef,
        ),
        (
            "rt",
            Arc::new(Int64Array::from(vec![1000; rows])) as ArrayRef,
        ),
    ])
    .unwrap()
}
fn probes(c: &mut Criterion) {
    let mut group = c.benchmark_group("interval_selective_probe");
    for retained in [1024, 16384] {
        for arrivals in [16, 256] {
            for width in [8, 264] {
                let state = input(retained, retained, width);
                let incoming = input(arrivals, retained, width);
                let setup = || {
                    let mut join = IntervalJoin::new(
                        vec![0],
                        vec![0],
                        2,
                        2,
                        -100,
                        100,
                        state.schema(),
                        incoming.schema(),
                    );
                    assert_eq!(join.push_left(state.clone()).num_rows(), 0);
                    join
                };
                assert_eq!(setup().push_right(incoming.clone()).num_rows(), arrivals);
                group.bench_function(
                    BenchmarkId::new(
                        "matching_keys",
                        format!("retained={retained}/arrivals={arrivals}/bytes={width}"),
                    ),
                    |b| {
                        b.iter_batched_ref(
                            &setup,
                            |join| black_box(join.push_right(black_box(incoming.clone()))),
                            BatchSize::PerIteration,
                        );
                    },
                );
            }
        }
    }
    group.finish();
}
criterion_group!(benches, probes);
criterion_main!(benches);
