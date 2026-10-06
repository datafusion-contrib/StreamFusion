//! Pending rowtime dedup arrivals; initial state construction and final verification are untimed.
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use std::{collections::HashMap, sync::Arc};
use streamfusion::bench::KeepFirstDedup;

fn batch(keys: Vec<i64>, times: Vec<i64>, values: Vec<Option<String>>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("k", Arc::new(Int64Array::from(keys)) as ArrayRef),
        ("rt", Arc::new(Int64Array::from(times)) as ArrayRef),
        ("v", Arc::new(StringArray::from(values)) as ArrayRef),
    ])
    .unwrap()
}

fn pending(c: &mut Criterion) {
    let mut group = c.benchmark_group("dedup_pending");
    for cardinality in [64usize, 16384] {
        for arrivals in [16usize, 256] {
            for width in [8usize, 264] {
                let payload = "x".repeat(width);
                let initial = batch(
                    (0..cardinality as i64).collect(),
                    vec![1000; cardinality],
                    (0..cardinality)
                        .map(|key| (key % 7 != 0).then(|| format!("initial/{key}/{payload}")))
                        .collect(),
                );
                for mixed in [false, true] {
                    let mut keys = Vec::with_capacity(arrivals);
                    let mut times = Vec::with_capacity(arrivals);
                    for row in 0..arrivals {
                        let mode = if mixed { row % 4 } else { 0 };
                        keys.push(if mode == 0 {
                            (cardinality + row) as i64
                        } else {
                            (row / 4 % cardinality) as i64
                        });
                        times.push(match mode {
                            1 => 990,
                            3 => 1010,
                            _ => 1000,
                        });
                    }
                    let incoming = batch(
                        keys,
                        times,
                        (0..arrivals)
                            .map(|row| (row % 7 != 0).then(|| format!("arrival/{row}/{payload}")))
                            .collect(),
                    );
                    let setup = || {
                        let mut dedup = KeepFirstDedup::new(vec![0], 1);
                        dedup.push(&initial);
                        dedup
                    };
                    // Independent strict-minimum oracle also checks incumbent ties and null payloads.
                    let mut expected = HashMap::new();
                    for input in [&initial, &incoming] {
                        let keys = input
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        let times = input
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        let values = input
                            .column(2)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        for row in 0..input.num_rows() {
                            let candidate = (
                                times.value(row),
                                (!values.is_null(row)).then(|| values.value(row).to_owned()),
                            );
                            if expected
                                .get(&keys.value(row))
                                .is_none_or(|(time, _)| candidate.0 < *time)
                            {
                                expected.insert(keys.value(row), candidate);
                            }
                        }
                    }
                    let mut oracle = setup();
                    oracle.push(&incoming);
                    let output = oracle.flush(i64::MAX);
                    assert_eq!(output.num_rows(), expected.len());
                    let keys = output
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let times = output
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let values = output
                        .column(2)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    for row in 0..output.num_rows() {
                        assert_eq!(
                            expected.remove(&keys.value(row)).unwrap(),
                            (
                                times.value(row),
                                (!values.is_null(row)).then(|| values.value(row).to_owned())
                            )
                        );
                    }
                    assert!(expected.is_empty());
                    assert_eq!(oracle.flush(i64::MAX).num_rows(), 0);
                    group.throughput(Throughput::Elements(arrivals as u64));
                    group.bench_function(
                        BenchmarkId::new(
                            if mixed {
                                "mixed_minima_and_ties"
                            } else {
                                "fresh_keys"
                            },
                            format!("pending={cardinality}/arrivals={arrivals}/bytes={width}"),
                        ),
                        |b| {
                            b.iter_batched_ref(
                                &setup,
                                |dedup| black_box(dedup.push(black_box(&incoming))),
                                BatchSize::PerIteration,
                            )
                        },
                    );
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, pending);
criterion_main!(benches);
