use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, UInt32Array};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::{PersistentFirstDedup, PersistentSort};
use streamfusion_benchmark_support::{header, measure, report, CountingAllocator};
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
const OPTIONS: &str = include_str!("fixtures/rocks-options.json");
#[path = "persistent_state/group.rs"]
mod group;
#[path = "persistent_state/interval.rs"]
mod interval;
#[path = "persistent_state/over.rs"]
mod over;
#[path = "persistent_state/temporal_join.rs"]
mod temporal_join;
#[path = "persistent_state/tumbling.rs"]
mod tumbling;
#[path = "persistent_state/window_rank.rs"]
mod window_rank;
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
fn dedup_input(rows: usize, domain: usize, width: usize, nullable: bool) -> RecordBatch {
    let text = "中\0".to_owned() + &"x".repeat(width);
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|i| (i % domain) as i64),
            )) as ArrayRef,
        ),
        (
            "value",
            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                if nullable && i % 7 == 0 {
                    None
                } else {
                    Some(text.as_str())
                }
            }))) as ArrayRef,
        ),
        (
            "rt",
            Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn dedup(c: &mut Criterion) {
    header();
    let mut group = c.benchmark_group("engine/rocksdb/keep_first");
    for rows in [16, 1024, 16384] {
        for domain in [8, rows] {
            for width in [8, 264] {
                for nullable in [false, true] {
                    let input = dedup_input(rows, domain, width, nullable);
                    let expected = input.slice(0, domain);
                    let setup = || {
                        let directory = tempfile::tempdir().unwrap();
                        let operator = PersistentFirstDedup::new(
                            directory.path().join("db").to_str().unwrap(),
                            input.schema(),
                            OPTIONS,
                        );
                        (operator, directory)
                    };
                    group.throughput(Throughput::Elements(rows as u64));
                    let label =
                        format!("pending/{rows}/keys={domain}/bytes={width}/nulls={nullable}");
                    let (mut operator, _directory) = setup();
                    let (output, allocations) = measure(|| {
                        operator.push(&input);
                        operator.flush(rows as i64 - 1)
                    });
                    assert_eq!(output, expected);
                    report(&label, input.columns(), output.columns(), allocations);
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter_batched_ref(
                            &setup,
                            |(operator, _)| {
                                operator.push(&input);
                                std::hint::black_box(operator.flush(rows as i64 - 1))
                            },
                            BatchSize::PerIteration,
                        )
                    });
                    let marked_setup = || {
                        let (mut operator, directory) = setup();
                        operator.push(&input);
                        assert_eq!(operator.flush(rows as i64 - 1), expected);
                        (operator, directory)
                    };
                    let later = RecordBatch::try_new(
                        input.schema(),
                        vec![
                            input.column(0).clone(),
                            input.column(1).clone(),
                            Arc::new(Int64Array::from_iter_values(
                                (0..rows).map(|i| rows as i64 + i as i64),
                            )),
                        ],
                    )
                    .unwrap();
                    let label = format!(
                        "emitted_markers/{rows}/keys={domain}/bytes={width}/nulls={nullable}"
                    );
                    let (mut operator, _directory) = marked_setup();
                    let (output, allocations) = measure(|| {
                        operator.push(&later);
                        operator.flush(2 * rows as i64)
                    });
                    assert_eq!(output.schema(), input.schema());
                    assert_eq!(output.num_rows(), 0);
                    report(&label, later.columns(), output.columns(), allocations);
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter_batched_ref(
                            &marked_setup,
                            |(operator, _)| {
                                operator.push(&later);
                                std::hint::black_box(operator.flush(2 * rows as i64))
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
fn dedup_ttl(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/rocksdb/keep_first_ttl");
    for rows in [16, 1024, 16384] {
        for domain in [8, rows] {
            for width in [8, 264] {
                for nullable in [false, true] {
                    let input = dedup_input(rows, domain, width, nullable);
                    let expected = input.slice(0, domain);
                    let later = RecordBatch::try_new(
                        input.schema(),
                        vec![
                            input.column(0).clone(),
                            input.column(1).clone(),
                            Arc::new(Int64Array::from_iter_values(
                                (0..rows).map(|i| (rows + i) as i64),
                            )),
                        ],
                    )
                    .unwrap();
                    let setup = || {
                        let directory = tempfile::tempdir().unwrap();
                        let mut operator = PersistentFirstDedup::with_ttl(
                            directory.path().join("db").to_str().unwrap(),
                            input.schema(),
                            OPTIONS,
                            1000,
                        );
                        operator.push_at(&input, 0);
                        (operator, directory)
                    };
                    let marked_setup = || {
                        let (mut operator, directory) = setup();
                        assert_eq!(operator.flush_at(rows as i64 - 1, 0), expected);
                        // A read at half the retention must not refresh emitted markers.
                        operator.push_at(&later, 500);
                        assert_eq!(operator.flush_at(rows as i64 - 1, 500).num_rows(), 0);
                        (operator, directory)
                    };
                    group.throughput(Throughput::Elements(rows as u64));
                    for (phase, now) in [("live_markers", 999), ("expired_markers", 1000)] {
                        let label =
                            format!("{phase}/{rows}/keys={domain}/bytes={width}/nulls={nullable}");
                        let execute = |operator: &mut PersistentFirstDedup| {
                            operator.push_at(&later, now);
                            operator.flush_at(2 * rows as i64, now)
                        };
                        let (mut operator, _directory) = marked_setup();
                        let (output, allocations) = measure(|| execute(&mut operator));
                        if now == 1000 {
                            assert_eq!(output, later.slice(0, domain));
                        } else {
                            assert_eq!(output.schema(), input.schema());
                            assert_eq!(output.num_rows(), 0);
                        }
                        report(&label, later.columns(), output.columns(), allocations);
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &marked_setup,
                                |(operator, _)| std::hint::black_box(execute(operator)),
                                BatchSize::PerIteration,
                            )
                        });
                    }
                    let label = format!(
                        "pending_not_expired/{rows}/keys={domain}/bytes={width}/nulls={nullable}"
                    );
                    let (mut operator, _directory) = setup();
                    let (output, allocations) =
                        measure(|| operator.flush_at(rows as i64 - 1, 1000));
                    assert_eq!(output, expected);
                    report(&label, input.columns(), output.columns(), allocations);
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter_batched_ref(
                            &setup,
                            |(operator, _)| {
                                std::hint::black_box(operator.flush_at(rows as i64 - 1, 1000))
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
fn dedup_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/rocksdb/keep_first_recovery");
    for rows in [16, 1024, 16384] {
        for domain in [8, rows] {
            for width in [8, 264] {
                for nullable in [false, true] {
                    let input = dedup_input(rows, domain, width, nullable);
                    let fired = domain / 2;
                    let setup = || {
                        let directory = tempfile::tempdir().unwrap();
                        let mut operator = PersistentFirstDedup::new(
                            directory.path().join("db").to_str().unwrap(),
                            input.schema(),
                            OPTIONS,
                        );
                        operator.push(&input);
                        assert_eq!(operator.flush(fired as i64 - 1), input.slice(0, fired));
                        (operator, directory)
                    };
                    let fresh = |rt| {
                        RecordBatch::try_new(
                            input.schema(),
                            vec![
                                Arc::new(Int64Array::from(vec![domain as i64])) as ArrayRef,
                                input.column(1).slice(0, 1),
                                Arc::new(Int64Array::from(vec![rt])) as ArrayRef,
                            ],
                        )
                        .unwrap()
                    };
                    let late = fresh(0);
                    let next = fresh(rows as i64);
                    let expected = arrow::compute::concat_batches(
                        &input.schema(),
                        [&input.slice(fired, domain - fired), &next],
                    )
                    .unwrap();
                    let verify = |operator: &mut PersistentFirstDedup| {
                        operator.push(&late);
                        operator.push(&input);
                        operator.push(&next);
                        assert_eq!(operator.flush(rows as i64), expected);
                        operator.push(&next);
                        assert_eq!(operator.flush(rows as i64 + 1).num_rows(), 0);
                    };
                    let label =
                        format!("checkpoint/{rows}/keys={domain}/bytes={width}/nulls={nullable}");
                    let (mut operator, directory) = setup();
                    let source_path = directory.path().join("snapshot");
                    let source = source_path.to_str().unwrap();
                    let (generation, allocations) = measure(|| operator.checkpoint(source));
                    verify(&mut operator);
                    report(&label, &[], &[], allocations);
                    group.bench_function(BenchmarkId::from_parameter(label), |b| {
                        b.iter_batched_ref(
                            &setup,
                            |(operator, directory)| {
                                std::hint::black_box(operator.checkpoint(
                                    directory.path().join("snapshot").to_str().unwrap(),
                                ))
                            },
                            BatchSize::PerIteration,
                        )
                    });
                    drop(operator);
                    for aligned in [false, true] {
                        let restore = |directory: &tempfile::TempDir| {
                            PersistentFirstDedup::restore(
                                directory.path().join("restored").to_str().unwrap(),
                                input.schema(),
                                OPTIONS,
                                source,
                                generation,
                                aligned,
                            )
                        };
                        let label = format!("restore/{rows}/keys={domain}/bytes={width}/nulls={nullable}/aligned={aligned}");
                        let target = tempfile::tempdir().unwrap();
                        let (mut restored, allocations) = measure(|| restore(&target));
                        verify(&mut restored);
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
    }
    group.finish();
}
criterion_group!(
    benches,
    persistent,
    dedup,
    dedup_ttl,
    dedup_recovery,
    interval::interval,
    window_rank::window_rank,
    window_rank::memory_window_rank,
    temporal_join::temporal_join,
    over::over,
    over::over_fold_recovery,
    group::group,
    tumbling::tumbling
);
criterion_main!(benches);
