use super::OPTIONS;
use arrow::array::{Array, ArrayRef, Int64Array, Int8Array, RecordBatch, StringArray};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::TemporalJoinState;
use streamfusion_benchmark_support::{measure, report};

type OutputRow = (
    i64,
    i64,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    i8,
);

fn payload(row: usize, width: usize, nullable: bool) -> Option<String> {
    if nullable && (row + 1) % 7 == 0 {
        None
    } else {
        Some(format!("{row}/中\0{}", "x".repeat(width)))
    }
}

fn input(rows: usize, domain: usize, width: usize, nullable: bool, left: bool) -> RecordBatch {
    let count = if left { rows } else { domain };
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(Int64Array::from_iter_values((0..=count).map(|i| {
                let row = i.saturating_sub(1);
                if left {
                    (row % domain) as i64
                } else {
                    (2 * (row / 2)) as i64
                }
            }))) as ArrayRef,
        ),
        (
            "rt",
            Arc::new(Int64Array::from_iter_values((0..=count).map(|i| {
                let row = i.saturating_sub(1);
                if left {
                    150 + 100 * ((row / domain) % 2) as i64
                } else {
                    100 + 100 * (row % 2) as i64
                }
            }))) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(StringArray::from_iter(
                (0..=count).map(|i| payload(i.saturating_sub(1), width, nullable)),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
    .slice(1, count)
}

fn expected(rows: usize, domain: usize, width: usize, nullable: bool) -> Vec<OutputRow> {
    let mut output = (0..rows)
        .map(|row| {
            let key = (row % domain) as i64;
            let version = (row / domain) % 2;
            let matched = key % 2 == 0;
            (
                key,
                150 + 100 * version as i64,
                payload(row, width, nullable),
                matched.then_some(key),
                matched.then_some(100 + 100 * version as i64),
                if matched {
                    payload(key as usize + version, width, nullable)
                } else {
                    None
                },
                0,
            )
        })
        .collect::<Vec<_>>();
    output.sort_unstable();
    output
}

fn validate(batch: &RecordBatch, expected: &[OutputRow]) {
    assert_eq!(batch.num_columns(), 7);
    let integer = |column| {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
    };
    let text = |column| {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    };
    let kinds = batch
        .column(6)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    let mut actual = (0..batch.num_rows())
        .map(|row| {
            (
                integer(0).value(row),
                integer(1).value(row),
                (!text(2).is_null(row)).then(|| text(2).value(row).to_owned()),
                (!integer(3).is_null(row)).then(|| integer(3).value(row)),
                (!integer(4).is_null(row)).then(|| integer(4).value(row)),
                (!text(5).is_null(row)).then(|| text(5).value(row).to_owned()),
                kinds.value(row),
            )
        })
        .collect::<Vec<_>>();
    actual.sort_unstable();
    assert_eq!(actual, expected);
}

pub(super) fn temporal_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/rocksdb/temporal_join");
    for rows in [16, 1024, 16384] {
        for domain in [8, rows] {
            for width in [8, 264] {
                for nullable in [false, true] {
                    let left = input(rows, domain, width, nullable, true);
                    let right = input(rows, domain, width, nullable, false);
                    let expected = expected(rows, domain, width, nullable);
                    let create = |directory: &tempfile::TempDir| {
                        TemporalJoinState::new(
                            directory.path().join("db").to_str().unwrap(),
                            left.schema(),
                            OPTIONS,
                        )
                    };
                    let source_directory = tempfile::tempdir().unwrap();
                    let mut source_operator = create(&source_directory);
                    source_operator.push(&right, false);
                    source_operator.push(&left, true);
                    let source_path = source_directory.path().join("checkpoint");
                    let source = source_path.to_str().unwrap();
                    let generation = source_operator.checkpoint(source);
                    validate(&source_operator.flush(500), &expected);
                    assert_eq!(source_operator.flush(500).num_rows(), 0);
                    for phase in [
                        "build",
                        "probe",
                        "fire",
                        "checkpoint",
                        "restore_aligned",
                        "restore_clipped",
                    ] {
                        let restoring = phase.starts_with("restore");
                        let setup = || {
                            let directory = tempfile::tempdir().unwrap();
                            let operator = if restoring {
                                None
                            } else {
                                let mut operator = create(&directory);
                                if phase != "build" {
                                    operator.push(&right, false);
                                }
                                if phase == "fire" || phase == "checkpoint" {
                                    operator.push(&left, true);
                                }
                                Some(operator)
                            };
                            (operator, directory)
                        };
                        let run = |state: &mut (Option<TemporalJoinState>, tempfile::TempDir)| {
                            if restoring {
                                state.0 = Some(TemporalJoinState::restore(
                                    state.1.path().join("db").to_str().unwrap(),
                                    left.schema(),
                                    OPTIONS,
                                    source,
                                    generation,
                                    phase == "restore_aligned",
                                ));
                                None
                            } else {
                                let operator = state.0.as_mut().unwrap();
                                match phase {
                                    "build" => {
                                        operator.push(&right, false);
                                        None
                                    }
                                    "probe" => {
                                        operator.push(&left, true);
                                        None
                                    }
                                    "fire" => Some(operator.flush(500)),
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
                        let label = format!("temporal_join/{phase}/{rows}/domain={domain}/bytes={width}/nulls={nullable}");
                        let mut state = setup();
                        let (output, allocations) = measure(|| run(&mut state));
                        if phase == "build" {
                            state.0.as_mut().unwrap().push(&left, true);
                        }
                        let checked =
                            output.unwrap_or_else(|| state.0.as_mut().unwrap().flush(500));
                        validate(&checked, &expected);
                        assert_eq!(state.0.as_mut().unwrap().flush(500).num_rows(), 0);
                        report(
                            &label,
                            if phase == "build" {
                                right.columns()
                            } else if phase == "probe" {
                                left.columns()
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
                        group.throughput(Throughput::Elements(if phase == "build" {
                            domain
                        } else {
                            rows
                        } as u64));
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
