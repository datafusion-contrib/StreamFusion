use super::OPTIONS;
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::PersistentOver;
use streamfusion_benchmark_support::{measure, report};

type OutputRow = (i64, Option<i64>, i64, Option<String>, Option<i64>);

fn payload(row: usize, width: usize, nullable: bool) -> Option<String> {
    if nullable && (row + 1) % 7 == 0 {
        None
    } else {
        Some(format!("{row}/中\0{}", "x".repeat(width)))
    }
}

fn input(rows: usize, domain: usize, width: usize, nullable: bool) -> RecordBatch {
    let indices = || (0..=rows).map(|i| i.saturating_sub(1));
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from_iter_values(
                indices().map(|i| (i % domain) as i64),
            )) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int64Array::from_iter(indices().map(|i| {
                if nullable && (i + 1) % 7 == 0 {
                    None
                } else {
                    Some(i as i64 + 1)
                }
            }))) as ArrayRef,
        ),
        (
            "rt",
            Arc::new(Int64Array::from_iter_values(indices().map(|i| i as i64))) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(StringArray::from_iter(
                indices().map(|i| payload(i, width, nullable)),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
    .slice(1, rows)
}

fn expected(rows: usize, domain: usize, width: usize, nullable: bool) -> Vec<OutputRow> {
    let mut sums = vec![None; domain];
    let mut result = (0..rows)
        .map(|i| {
            let key = i % domain;
            let value = if nullable && (i + 1) % 7 == 0 {
                None
            } else {
                Some(i as i64 + 1)
            };
            if let Some(value) = value {
                sums[key] = Some(sums[key].unwrap_or(0) + value);
            }
            (
                key as i64,
                value,
                i as i64,
                payload(i, width, nullable),
                sums[key],
            )
        })
        .collect::<Vec<_>>();
    result.sort_unstable();
    result
}

fn validate(batch: &RecordBatch, expected: &[OutputRow]) {
    assert_eq!(batch.num_columns(), 5);
    let integer = |column| {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
    };
    let text = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mut actual = (0..batch.num_rows())
        .map(|i| {
            (
                integer(0).value(i),
                (!integer(1).is_null(i)).then(|| integer(1).value(i)),
                integer(2).value(i),
                (!text.is_null(i)).then(|| text.value(i).to_owned()),
                (!integer(4).is_null(i)).then(|| integer(4).value(i)),
            )
        })
        .collect::<Vec<_>>();
    actual.sort_unstable();
    assert_eq!(actual, expected);
}

pub(super) fn over(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/rocksdb/over");
    for rows in [16, 1024, 16384] {
        for domain in [8, rows] {
            for width in [8, 264] {
                for nullable in [false, true] {
                    let input = input(rows, domain, width, nullable);
                    let expected = expected(rows, domain, width, nullable);
                    let create = |directory: &tempfile::TempDir| {
                        PersistentOver::new(
                            directory.path().join("db").to_str().unwrap(),
                            input.schema(),
                            OPTIONS,
                        )
                    };
                    let source_directory = tempfile::tempdir().unwrap();
                    let mut source_operator = create(&source_directory);
                    source_operator.push(&input);
                    let source_path = source_directory.path().join("checkpoint");
                    let source = source_path.to_str().unwrap();
                    let generation = source_operator.checkpoint(source);
                    validate(&source_operator.flush(), &expected);
                    assert_eq!(source_operator.flush().num_rows(), 0);
                    for phase in [
                        "push",
                        "fire",
                        "checkpoint",
                        "restore_aligned",
                        "restore_rebuilt",
                    ] {
                        let restoring = phase.starts_with("restore");
                        let setup = || {
                            let directory = tempfile::tempdir().unwrap();
                            let operator = if restoring {
                                None
                            } else {
                                let mut operator = create(&directory);
                                if phase != "push" {
                                    operator.push(&input);
                                }
                                Some(operator)
                            };
                            (operator, directory)
                        };
                        let run = |state: &mut (Option<PersistentOver>, tempfile::TempDir)| {
                            if restoring {
                                state.0 = Some(PersistentOver::restore(
                                    state.1.path().join("db").to_str().unwrap(),
                                    input.schema(),
                                    OPTIONS,
                                    source,
                                    generation,
                                    phase == "restore_aligned",
                                ));
                                None
                            } else {
                                let operator = state.0.as_mut().unwrap();
                                match phase {
                                    "push" => {
                                        operator.push(&input);
                                        None
                                    }
                                    "fire" => Some(operator.flush()),
                                    "checkpoint" => {
                                        operator.checkpoint(
                                            state.1.path().join("checkpoint").to_str().unwrap(),
                                        );
                                        None
                                    }
                                    _ => unreachable!(),
                                }
                            }
                        };
                        let label = format!(
                            "over/{phase}/{rows}/domain={domain}/bytes={width}/nulls={nullable}"
                        );
                        let mut state = setup();
                        let (output, allocations) = measure(|| run(&mut state));
                        let checked = output.unwrap_or_else(|| state.0.as_mut().unwrap().flush());
                        validate(&checked, &expected);
                        assert_eq!(state.0.as_mut().unwrap().flush().num_rows(), 0);
                        report(
                            &label,
                            if phase == "push" {
                                input.columns()
                            } else {
                                &[]
                            },
                            if phase == "fire" {
                                checked.columns()
                            } else {
                                &[]
                            },
                            allocations,
                        );
                        group.throughput(Throughput::Elements(rows as u64));
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &setup,
                                |state| std::hint::black_box(run(state)),
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
