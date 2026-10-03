use super::OPTIONS;
use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::IntervalState;
use streamfusion_benchmark_support::{measure, report};

fn input(rows: usize, domain: usize, width: usize, nullable: bool, right: bool) -> RecordBatch {
    let count = if right { domain / 2 } else { rows };
    let text = "中\0".to_owned() + &"x".repeat(width);
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from_iter_values((0..=count).map(|i| {
                if right {
                    (2 * i.saturating_sub(1)) as i64
                } else {
                    (i.saturating_sub(1) % domain) as i64
                }
            }))) as ArrayRef,
        ),
        (
            "value",
            Arc::new(StringArray::from_iter((0..=count).map(|i| {
                if nullable && i % 7 == 0 {
                    None
                } else {
                    Some(format!("{i}/{text}"))
                }
            }))) as ArrayRef,
        ),
        (
            "rt",
            Arc::new(Int64Array::from_iter_values((0..=count).map(|i| {
                if right {
                    150
                } else {
                    100 + (i.saturating_sub(1) % 13) as i64
                }
            }))) as ArrayRef,
        ),
    ])
    .unwrap()
    .slice(1, count)
}

fn ingest(operator: &mut IntervalState, left: &RecordBatch, right: &RecordBatch) -> RecordBatch {
    assert_eq!(operator.push_left(left).num_rows(), 0);
    operator.push_right(right)
}

pub(super) fn interval(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/rocksdb/interval");
    for rows in [16, 1024, 16384] {
        for domain in [8, rows] {
            for width in [8, 264] {
                for nullable in [false, true] {
                    let left = input(rows, domain, width, nullable, false);
                    let right = input(rows, domain, width, nullable, true);
                    for outer in [false, true] {
                        let mut oracle = IntervalState::memory(left.schema(), outer);
                        let expected_matches = ingest(&mut oracle, &left, &right);
                        assert_eq!(expected_matches.num_rows(), rows / 2);
                        let expected_expired = oracle.advance(500);
                        assert_eq!(
                            expected_expired.num_rows(),
                            if outer { rows / 2 } else { 0 }
                        );
                        let setup = || {
                            let directory = tempfile::tempdir().unwrap();
                            let operator = IntervalState::new(
                                directory.path().join("db").to_str().unwrap(),
                                left.schema(),
                                OPTIONS,
                                outer,
                            );
                            (operator, directory)
                        };
                        let sources = left
                            .columns()
                            .iter()
                            .chain(right.columns())
                            .cloned()
                            .collect::<Vec<_>>();
                        let shape = format!(
                            "{rows}/domain={domain}/bytes={width}/nulls={nullable}/outer={outer}"
                        );
                        group.throughput(Throughput::Elements(rows as u64));
                        let label = format!("append_left/{shape}");
                        let (mut operator, _directory) = setup();
                        let (output, allocations) = measure(|| operator.push_left(&left));
                        assert_eq!(output.num_rows(), 0);
                        report(&label, &sources, output.columns(), allocations);
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &setup,
                                |(operator, _)| std::hint::black_box(operator.push_left(&left)),
                                BatchSize::PerIteration,
                            )
                        });

                        let left_populated = || {
                            let (mut operator, directory) = setup();
                            assert_eq!(operator.push_left(&left).num_rows(), 0);
                            (operator, directory)
                        };
                        group.throughput(Throughput::Elements(right.num_rows() as u64));
                        let label = format!("probe_right/{shape}");
                        let (mut operator, _directory) = left_populated();
                        let (matches, allocations) = measure(|| operator.push_right(&right));
                        assert_eq!(matches, expected_matches);
                        report(&label, &sources, matches.columns(), allocations);
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &left_populated,
                                |(operator, _)| std::hint::black_box(operator.push_right(&right)),
                                BatchSize::PerIteration,
                            )
                        });

                        let populated = || {
                            let (mut operator, directory) = setup();
                            ingest(&mut operator, &left, &right);
                            (operator, directory)
                        };
                        group.throughput(Throughput::Elements((rows + right.num_rows()) as u64));
                        let label = format!("watermark_expire/{shape}");
                        let (mut operator, _directory) = populated();
                        let (expired, allocations) = measure(|| operator.advance(500));
                        assert_eq!(expired, expected_expired);
                        report(&label, &sources, expired.columns(), allocations);
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &populated,
                                |(operator, _)| std::hint::black_box(operator.advance(500)),
                                BatchSize::PerIteration,
                            )
                        });

                        let label = format!("checkpoint/{shape}");
                        let (mut operator, directory) = populated();
                        let snapshot = directory.path().join("snapshot");
                        let (generation, allocations) =
                            measure(|| operator.checkpoint(snapshot.to_str().unwrap()));
                        assert!(snapshot.exists());
                        report(&label, &[], &[], allocations);
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &populated,
                                |(operator, directory)| {
                                    operator.checkpoint(
                                        directory.path().join("snapshot").to_str().unwrap(),
                                    )
                                },
                                BatchSize::PerIteration,
                            )
                        });
                        drop(operator);

                        let even = left.slice(0, 1);
                        let odd = left.slice(1, 1);
                        let right_tail = right.slice(0, 1);
                        for aligned in [false, true] {
                            let restore = |destination: &tempfile::TempDir| {
                                IntervalState::restore(
                                    destination.path().join("db").to_str().unwrap(),
                                    left.schema(),
                                    OPTIONS,
                                    outer,
                                    snapshot.to_str().unwrap(),
                                    generation,
                                    aligned,
                                )
                            };
                            let label = format!("restore/{shape}/aligned={aligned}");
                            let destination = tempfile::tempdir().unwrap();
                            let (mut restored, allocations) = measure(|| restore(&destination));
                            assert_eq!(restored.timer_deadline(), 500);
                            let mut oracle = IntervalState::memory(left.schema(), outer);
                            ingest(&mut oracle, &left, &right);
                            // Repeated matching arrivals expose overwritten rows or reused sequence IDs.
                            assert_eq!(
                                restored.push_right(&right_tail),
                                oracle.push_right(&right_tail)
                            );
                            let matches = restored.push_left(&even);
                            assert_eq!(matches, oracle.push_left(&even));
                            assert_eq!(matches.num_rows(), 2);
                            assert_eq!(restored.push_left(&odd), oracle.push_left(&odd));
                            assert_eq!(restored.advance(500), oracle.advance(500));
                            assert_eq!(restored.advance(500).num_rows(), 0);
                            report(&label, &[], &[], allocations);
                            group.bench_function(BenchmarkId::from_parameter(label), |b| {
                                b.iter_batched_ref(
                                    || tempfile::tempdir().unwrap(),
                                    |destination| {
                                        let restored = restore(destination);
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
        }
    }
    group.finish();
}
