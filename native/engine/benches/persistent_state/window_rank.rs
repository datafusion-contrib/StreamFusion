use super::OPTIONS;
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, UInt32Array};
use arrow::compute::{lexsort_to_indices, take, SortColumn};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use std::{collections::BTreeMap, sync::Arc};
use streamfusion::bench::WindowRankState;
use streamfusion_benchmark_support::{measure, report};

fn input(rows: usize, domain: usize, width: usize, nullable: bool) -> RecordBatch {
    let text = "中\0".to_owned() + &"x".repeat(width);
    let ordinal = |i: usize| i.saturating_sub(1);
    let window = |i: usize| ((ordinal(i) / domain + ordinal(i)) % 2) as i64;
    RecordBatch::try_from_iter(vec![
        (
            "window_start",
            Arc::new(Int64Array::from_iter_values(
                (0..=rows).map(|i| window(i) * 100),
            )) as ArrayRef,
        ),
        (
            "window_end",
            Arc::new(Int64Array::from_iter_values(
                (0..=rows).map(|i| (window(i) + 1) * 100),
            )) as ArrayRef,
        ),
        (
            "key",
            Arc::new(Int64Array::from_iter_values(
                (0..=rows).map(|i| (ordinal(i) % domain) as i64),
            )) as ArrayRef,
        ),
        (
            "sort",
            Arc::new(Int64Array::from_iter((0..=rows).map(|i| {
                if nullable && ordinal(i) % 7 == 0 {
                    None
                } else {
                    Some((ordinal(i) % 5) as i64)
                }
            }))) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(StringArray::from_iter((0..=rows).map(|i| {
                if nullable && ordinal(i) % 11 == 0 {
                    None
                } else {
                    Some(format!("{}/{text}", ordinal(i)))
                }
            }))) as ArrayRef,
        ),
    ])
    .unwrap()
    .slice(1, rows)
}

fn canonical(batch: &RecordBatch) -> RecordBatch {
    let columns = [1, 2, 5].map(|index| SortColumn {
        values: batch.column(index).clone(),
        options: None,
    });
    let indices = lexsort_to_indices(&columns, None).unwrap();
    RecordBatch::try_new(
        batch.schema(),
        batch
            .columns()
            .iter()
            .map(|a| take(a, &indices, None).unwrap())
            .collect(),
    )
    .unwrap()
}

fn expected(input: &RecordBatch, limit: usize, keep_last: bool, watermark: i64) -> RecordBatch {
    let ints = |index| {
        input
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
    };
    let end = ints(1);
    let key = ints(2);
    let sort = ints(3);
    let mut groups: BTreeMap<(i64, i64), Vec<usize>> = BTreeMap::new();
    for row in 0..input.num_rows() {
        if end.value(row) <= watermark {
            groups
                .entry((end.value(row), key.value(row)))
                .or_default()
                .push(row);
        }
    }
    let mut rows = Vec::new();
    let mut ranks = Vec::new();
    for indices in groups.values_mut() {
        indices.sort_by_key(|&row| {
            (
                sort.is_null(row),
                if sort.is_null(row) {
                    0
                } else {
                    sort.value(row)
                },
                if keep_last { usize::MAX - row } else { row },
            )
        });
        for (rank, &row) in indices.iter().take(limit).enumerate() {
            rows.push(row as u32);
            ranks.push((rank + 1) as i64);
        }
    }
    let indices = UInt32Array::from(rows);
    let mut columns = input
        .columns()
        .iter()
        .map(|a| take(a, &indices, None).unwrap())
        .collect::<Vec<_>>();
    columns.push(Arc::new(Int64Array::from(ranks)));
    let mut fields = input
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("w0$o0", DataType::Int64, false));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

pub(super) fn window_rank(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/rocksdb/window_rank");
    for rows in [16, 1024, 16384] {
        for domain in [8, rows] {
            for width in [8, 264] {
                for nullable in [false, true] {
                    let input = input(rows, domain, width, nullable);
                    for (limit, keep_last) in [(1, false), (1, true), (4, false)] {
                        let shape = format!("{rows}/domain={domain}/bytes={width}/nulls={nullable}/limit={limit}/last={keep_last}");
                        let wanted = expected(&input, limit, keep_last, 200);
                        let mut oracle = WindowRankState::memory(limit as i64, keep_last);
                        oracle.push(&input);
                        assert_eq!(canonical(&oracle.flush(200)), wanted);
                        let setup = || {
                            let directory = tempfile::tempdir().unwrap();
                            let operator = WindowRankState::new(
                                directory.path().join("db").to_str().unwrap(),
                                input.schema(),
                                OPTIONS,
                                limit as i64,
                                keep_last,
                            );
                            (operator, directory)
                        };
                        let populated = || {
                            let (mut operator, directory) = setup();
                            operator.push(&input);
                            (operator, directory)
                        };
                        group.throughput(Throughput::Elements(rows as u64));
                        let label = format!("append/{shape}");
                        let (mut operator, _directory) = setup();
                        let (_, allocations) = measure(|| operator.push(&input));
                        assert_eq!(canonical(&operator.flush(200)), wanted);
                        report(&label, input.columns(), &[], allocations);
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &setup,
                                |(operator, _)| operator.push(&input),
                                BatchSize::PerIteration,
                            )
                        });

                        let doubled =
                            arrow::compute::concat_batches(&input.schema(), [&input, &input])
                                .unwrap();
                        let wanted_updated = expected(&doubled, limit, keep_last, 200);
                        let label = format!("update/{shape}");
                        let (mut operator, _directory) = populated();
                        let (_, allocations) = measure(|| operator.push(&input));
                        assert_eq!(canonical(&operator.flush(200)), wanted_updated);
                        report(&label, input.columns(), &[], allocations);
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &populated,
                                |(operator, _)| operator.push(&input),
                                BatchSize::PerIteration,
                            )
                        });

                        let label = format!("fire/{shape}");
                        let (mut operator, _directory) = populated();
                        let (output, allocations) = measure(|| operator.flush(200));
                        assert_eq!(canonical(&output), wanted);
                        assert_eq!(operator.flush(200).num_rows(), 0);
                        report(&label, &[], output.columns(), allocations);
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &populated,
                                |(operator, _)| std::hint::black_box(operator.flush(200)),
                                BatchSize::PerIteration,
                            )
                        });

                        let partial = || {
                            let (mut operator, directory) = populated();
                            operator.flush(100);
                            (operator, directory)
                        };
                        let label = format!("checkpoint/{shape}");
                        let (mut operator, directory) = partial();
                        let path = directory.path().join("snapshot");
                        let (_, allocations) =
                            measure(|| operator.checkpoint(path.to_str().unwrap()));
                        assert!(path.exists());
                        report(&label, &[], &[], allocations);
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &partial,
                                |(operator, directory)| {
                                    operator.checkpoint(
                                        directory.path().join("snapshot").to_str().unwrap(),
                                    )
                                },
                                BatchSize::PerIteration,
                            )
                        });

                        let (mut operator, source_directory) = partial();
                        let source_path = source_directory.path().join("source");
                        let generation = operator.checkpoint(source_path.to_str().unwrap());
                        drop(operator);
                        for aligned in [false, true] {
                            let restore = |directory: &tempfile::TempDir| {
                                WindowRankState::restore(
                                    directory.path().join("restored").to_str().unwrap(),
                                    input.schema(),
                                    OPTIONS,
                                    limit as i64,
                                    keep_last,
                                    source_path.to_str().unwrap(),
                                    generation,
                                    aligned,
                                )
                            };
                            let label = format!("restore/{shape}/aligned={aligned}");
                            let directory = tempfile::tempdir().unwrap();
                            let (mut restored, allocations) = measure(|| restore(&directory));
                            assert_eq!(restored.timer_deadline(), 200);
                            restored.push(&input);
                            assert_eq!(restored.late_drops(), rows as u64 / 2);
                            assert_eq!(restored.flush(100).num_rows(), 0);
                            let output = restored.flush(200);
                            let all = expected(&doubled, limit, keep_last, 200);
                            let ends = all.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                            let remaining = arrow::compute::filter_record_batch(
                                &all,
                                &arrow::array::BooleanArray::from_iter(
                                    (0..all.num_rows()).map(|i| Some(ends.value(i) == 200)),
                                ),
                            )
                            .unwrap();
                            assert_eq!(canonical(&output), remaining);
                            assert_eq!(restored.flush(200).num_rows(), 0);
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
    }
    group.finish();
}

pub(super) fn memory_window_rank(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/memory/window_rank");
    for rows in [16, 1024, 16384] {
        for domain in [8, rows] {
            for width in [8, 264] {
                for nullable in [false, true] {
                    let input = input(rows, domain, width, nullable);
                    for (limit, keep_last) in [(1, false), (1, true), (4, false)] {
                        let shape = format!("{rows}/domain={domain}/bytes={width}/nulls={nullable}/limit={limit}/last={keep_last}");
                        let wanted = expected(&input, limit, keep_last, 200);
                        let setup = || WindowRankState::memory(limit as i64, keep_last);
                        let populated = || {
                            let mut operator = setup();
                            operator.push(&input);
                            operator
                        };
                        group.throughput(Throughput::Elements(rows as u64));

                        let label = format!("append/{shape}");
                        let mut operator = setup();
                        let (_, allocations) = measure(|| operator.push(&input));
                        assert_eq!(canonical(&operator.flush(200)), wanted);
                        report(
                            &format!("memory_rank/{label}"),
                            input.columns(),
                            &[],
                            allocations,
                        );
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &setup,
                                |operator| operator.push(&input),
                                BatchSize::PerIteration,
                            )
                        });

                        let doubled =
                            arrow::compute::concat_batches(&input.schema(), [&input, &input])
                                .unwrap();
                        let wanted_updated = expected(&doubled, limit, keep_last, 200);
                        let label = format!("update/{shape}");
                        let mut operator = populated();
                        let (_, allocations) = measure(|| operator.push(&input));
                        assert_eq!(canonical(&operator.flush(200)), wanted_updated);
                        report(
                            &format!("memory_rank/{label}"),
                            input.columns(),
                            &[],
                            allocations,
                        );
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &populated,
                                |operator| operator.push(&input),
                                BatchSize::PerIteration,
                            )
                        });

                        let label = format!("fire/{shape}");
                        let mut operator = populated();
                        let (output, allocations) = measure(|| operator.flush(200));
                        assert_eq!(canonical(&output), wanted);
                        assert_eq!(operator.flush(200).num_rows(), 0);
                        report(
                            &format!("memory_rank/{label}"),
                            &[],
                            output.columns(),
                            allocations,
                        );
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &populated,
                                |operator| std::hint::black_box(operator.flush(200)),
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
