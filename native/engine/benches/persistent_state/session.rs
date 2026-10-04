use super::OPTIONS;
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use std::{collections::BTreeMap, sync::Arc};
use streamfusion::bench::PersistentSession;
use streamfusion_benchmark_support::{measure, report};

type Output = (Option<String>, i64, i64, Option<i64>);
fn key(i: usize, domain: usize, width: usize, nullable: bool) -> Option<String> {
    (!(nullable && i % 11 == 0)).then(|| format!("{}/中\0{}", i % domain, "x".repeat(width)))
}
fn value(i: usize, domain: usize, nullable: bool) -> Option<i64> {
    (!(nullable && (i % 7 == 0 || i % domain == 1))).then_some(i as i64 % 101 - 50)
}
fn input(rows: usize, domain: usize, width: usize, nullable: bool, timestamp: i64) -> RecordBatch {
    let indices = || (0..=rows).map(|i| i.saturating_sub(1));
    RecordBatch::try_from_iter(vec![
        (
            "key0",
            Arc::new(StringArray::from_iter(
                indices().map(|i| key(i, domain, width, nullable)),
            )) as ArrayRef,
        ),
        (
            "value0",
            Arc::new(Int64Array::from_iter(
                indices().map(|i| value(i, domain, nullable)),
            )) as ArrayRef,
        ),
        (
            "ts",
            Arc::new(Int64Array::from_iter_values(indices().map(|_| timestamp))) as ArrayRef,
        ),
    ])
    .unwrap()
    .slice(1, rows)
}
fn expected(
    rows: usize,
    domain: usize,
    width: usize,
    nullable: bool,
    timestamps: &[i64],
) -> Vec<Output> {
    let mut events: BTreeMap<Option<String>, Vec<(i64, Option<i64>)>> = BTreeMap::new();
    for &ts in timestamps {
        for i in 0..rows {
            events
                .entry(key(i, domain, width, nullable))
                .or_default()
                .push((ts, value(i, domain, nullable)));
        }
    }
    let mut output = Vec::new();
    for (key, mut rows) in events {
        rows.sort_by_key(|row| row.0);
        let mut current: Option<(i64, i64, Option<i64>)> = None;
        for (ts, value) in rows {
            if current.as_ref().is_some_and(|session| ts > session.1) {
                let (start, end, sum) = current.take().unwrap();
                output.push((key.clone(), start, end, sum));
            }
            let session = current.get_or_insert((ts, ts + 1000, None));
            session.1 = session.1.max(ts + 1000);
            if let Some(value) = value {
                session.2 = Some(session.2.unwrap_or(0) + value);
            }
        }
        if let Some((start, end, sum)) = current {
            output.push((key, start, end, sum));
        }
    }
    output
}
fn validate(batch: &RecordBatch, expected: &[Output]) {
    assert_eq!(
        batch.schema().as_ref(),
        &Schema::new(vec![
            Field::new("key0", DataType::Utf8, true),
            Field::new("window_start", DataType::Int64, false),
            Field::new("window_end", DataType::Int64, false),
            Field::new("result0", DataType::Int64, true),
        ])
    );
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let starts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ends = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sums = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(starts.null_count(), 0);
    assert_eq!(ends.null_count(), 0);
    assert!(ends.values().windows(2).all(|pair| pair[0] <= pair[1]));
    let mut actual = (0..batch.num_rows())
        .map(|i| {
            (
                (!keys.is_null(i)).then(|| keys.value(i).to_owned()),
                starts.value(i),
                ends.value(i),
                (!sums.is_null(i)).then(|| sums.value(i)),
            )
        })
        .collect::<Vec<_>>();
    actual.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    assert_eq!(actual, expected);
}

pub(super) fn session(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/rocksdb/session_sum");
    for rows in [16, 1024, 16384] {
        for domain in [8, rows] {
            for width in [8, 264] {
                for nullable in [false, true] {
                    let initial = input(rows, domain, width, nullable, 100);
                    let separated = input(rows, domain, width, nullable, 2100);
                    let bridge = input(rows, domain, width, nullable, 1100);
                    let pending = input(rows, domain, width, nullable, 8100);
                    let continued = input(rows, domain, width, nullable, 10100);
                    let late = initial.clone();
                    let first = expected(rows, domain, width, nullable, &[100, 2100, 1100]);
                    let second = expected(rows, domain, width, nullable, &[8100]);
                    let third = expected(rows, domain, width, nullable, &[10100]);
                    let checkpoint_dir = tempfile::tempdir().unwrap();
                    let source = checkpoint_dir.path().join("checkpoint");
                    let source = source.to_str().unwrap();
                    let generation = {
                        let mut op = PersistentSession::new(
                            checkpoint_dir.path().join("db").to_str().unwrap(),
                            OPTIONS,
                        );
                        op.update(&initial);
                        op.update(&separated);
                        op.update(&bridge);
                        op.update(&pending);
                        validate(&op.flush(3100), &first);
                        op.checkpoint(source)
                    };
                    for phase in [
                        "merge",
                        "fire",
                        "checkpoint_after_fire",
                        "restore_aligned",
                        "restore_rebuilt",
                    ] {
                        let restoring = phase.starts_with("restore");
                        let setup = || {
                            let dir = tempfile::tempdir().unwrap();
                            let op = if restoring {
                                None
                            } else {
                                let mut op = PersistentSession::new(
                                    dir.path().join("db").to_str().unwrap(),
                                    OPTIONS,
                                );
                                op.update(&initial);
                                op.update(&separated);
                                op.update(&pending);
                                if phase != "merge" {
                                    op.update(&bridge);
                                    validate(&op.flush(3100), &first);
                                }
                                Some(op)
                            };
                            (op, dir)
                        };
                        let run = |state: &mut (Option<PersistentSession>, tempfile::TempDir)| {
                            if restoring {
                                state.0 = Some(PersistentSession::restore(
                                    state.1.path().join("db").to_str().unwrap(),
                                    OPTIONS,
                                    source,
                                    generation,
                                    phase == "restore_aligned",
                                ));
                                (None, None)
                            } else if phase == "checkpoint_after_fire" {
                                (
                                    None,
                                    Some(state.0.as_mut().unwrap().checkpoint(
                                        state.1.path().join("checkpoint").to_str().unwrap(),
                                    )),
                                )
                            } else if phase == "fire" {
                                (Some(state.0.as_mut().unwrap().flush(9100)), None)
                            } else {
                                state.0.as_mut().unwrap().update(&bridge);
                                (None, None)
                            }
                        };
                        let mut state = setup();
                        let ((output, snapshot), allocations) = measure(|| run(&mut state));
                        if let Some(ref out) = output {
                            validate(out, &second);
                        }
                        if let Some(snapshot) = snapshot {
                            state.0.take();
                            state.0 = Some(PersistentSession::restore(
                                state.1.path().join("verified-db").to_str().unwrap(),
                                OPTIONS,
                                state.1.path().join("checkpoint").to_str().unwrap(),
                                snapshot,
                                true,
                            ));
                        }
                        let op = state.0.as_mut().unwrap();
                        if restoring || snapshot.is_some() {
                            assert_eq!(op.timer_deadline(), 12345);
                            op.update(&late);
                        }
                        if phase == "merge" {
                            validate(&op.flush(3100), &first);
                        } else {
                            validate(&op.flush(3100), &[]);
                        }
                        if phase != "fire" {
                            validate(&op.flush(9100), &second);
                        }
                        validate(&op.flush(9100), &[]);
                        op.update(&initial); // Every original session is closed, including after restore.
                        op.update(&continued);
                        validate(&op.flush(11100), &third);
                        validate(&op.flush(11100), &[]);
                        let label = format!(
                            "session/{phase}/{rows}/domain={domain}/bytes={width}/nulls={nullable}"
                        );
                        report(
                            &label,
                            initial.columns(),
                            output.as_ref().map(|b| b.columns()).unwrap_or(&[]),
                            allocations,
                        );
                        group.throughput(Throughput::Elements(rows as u64));
                        group.bench_function(BenchmarkId::from_parameter(label), |b| {
                            b.iter_batched_ref(
                                &setup,
                                |s| std::hint::black_box(run(s)),
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
