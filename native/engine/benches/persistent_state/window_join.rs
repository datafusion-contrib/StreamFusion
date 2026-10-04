use super::OPTIONS;
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::PersistentWindowJoin;
use streamfusion_benchmark_support::{measure, report};

fn key(i: usize, width: usize) -> String {
    format!("{i}/中\0{}", "x".repeat(width))
}
fn input(rows: usize, width: usize, nullable: bool, start: i64, right: bool) -> RecordBatch {
    let indices = || (0..=rows).map(|i| i.saturating_sub(1));
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(StringArray::from_iter(
                indices().map(|i| (!(nullable && i % 11 == 0)).then(|| key(i, width))),
            )) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int64Array::from_iter(indices().map(|i| {
                (!(nullable && i % 7 == 0)).then_some(i as i64 + if right { 100000 } else { 0 })
            }))) as ArrayRef,
        ),
        (
            "start",
            Arc::new(Int64Array::from_iter_values(indices().map(|_| start))) as ArrayRef,
        ),
        (
            "end",
            Arc::new(Int64Array::from_iter_values(
                indices().map(|_| start + 1000),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
    .slice(1, rows)
}
fn validate(batch: &RecordBatch, rows: usize, width: usize, nullable: bool, start: i64) {
    if rows == 0 {
        assert_eq!(batch.num_rows(), 0);
        return;
    }
    assert_eq!(
        batch.schema().as_ref(),
        &Schema::new(
            (0..8)
                .map(|i| {
                    Field::new(
                        format!("c{i}"),
                        if i % 4 == 0 {
                            DataType::Utf8
                        } else {
                            DataType::Int64
                        },
                        true,
                    )
                })
                .collect::<Vec<_>>()
        )
    );
    let mut expected = (0..rows)
        .filter(|i| !(nullable && i % 11 == 0))
        .map(|i| {
            (
                key(i, width),
                (!(nullable && i % 7 == 0)).then_some(i as i64),
                (!(nullable && i % 7 == 0)).then_some(i as i64 + 100000),
            )
        })
        .collect::<Vec<_>>();
    let mut actual = Vec::new();
    for i in 0..batch.num_rows() {
        let string = |col| {
            let a = batch
                .column(col)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!(!a.is_null(i));
            a.value(i).to_owned()
        };
        let number = |col| {
            let a = batch
                .column(col)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (!a.is_null(i)).then(|| a.value(i))
        };
        assert_eq!(string(0), string(4));
        for col in [2, 6] {
            assert_eq!(number(col), Some(start));
        }
        for col in [3, 7] {
            assert_eq!(number(col), Some(start + 1000));
        }
        actual.push((string(0), number(1), number(5)));
    }
    expected.sort();
    actual.sort();
    assert_eq!(actual, expected);
}
pub(super) fn window_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/rocksdb/window_join");
    for rows in [16, 1024, 16384] {
        for width in [8, 264] {
            for nullable in [false, true] {
                let left = input(rows, width, nullable, 0, false);
                let right = input(rows, width, nullable, 0, true);
                let pending_left = input(rows, width, nullable, 1000, false);
                let pending_right = input(rows, width, nullable, 1000, true);
                let future_left = input(rows, width, nullable, 2000, false);
                let future_right = input(rows, width, nullable, 2000, true);
                let source_dir = tempfile::tempdir().unwrap();
                let source_path = source_dir.path().join("checkpoint");
                let source = source_path.to_str().unwrap();
                let generation = {
                    let mut op = PersistentWindowJoin::new(
                        source_dir.path().join("db").to_str().unwrap(),
                        left.schema(),
                        OPTIONS,
                    );
                    op.push(&left, true);
                    op.push(&right, false);
                    op.push(&pending_left, true);
                    op.push(&pending_right, false);
                    validate(&op.flush(1000), rows, width, nullable, 0);
                    op.checkpoint(source, 12345)
                };
                for phase in [
                    "push_right",
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
                            let mut op = PersistentWindowJoin::new(
                                dir.path().join("db").to_str().unwrap(),
                                left.schema(),
                                OPTIONS,
                            );
                            op.push(&left, true);
                            if phase != "push_right" {
                                op.push(&right, false);
                            }
                            op.push(&pending_left, true);
                            op.push(&pending_right, false);
                            if phase != "push_right" {
                                validate(&op.flush(1000), rows, width, nullable, 0);
                            }
                            Some(op)
                        };
                        (op, dir)
                    };
                    let run = |state: &mut (Option<PersistentWindowJoin>, tempfile::TempDir)| {
                        if restoring {
                            state.0 = Some(PersistentWindowJoin::restore(
                                state.1.path().join("db").to_str().unwrap(),
                                left.schema(),
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
                                    12345,
                                )),
                            )
                        } else if phase == "fire" {
                            (Some(state.0.as_mut().unwrap().flush(2000)), None)
                        } else {
                            state.0.as_mut().unwrap().push(&right, false);
                            (None, None)
                        }
                    };
                    let mut state = setup();
                    let ((output, snapshot), allocations) = measure(|| run(&mut state));
                    if let Some(ref out) = output {
                        validate(out, rows, width, nullable, 1000);
                    }
                    if let Some(id) = snapshot {
                        state.0.take();
                        state.0 = Some(PersistentWindowJoin::restore(
                            state.1.path().join("verified-db").to_str().unwrap(),
                            left.schema(),
                            OPTIONS,
                            state.1.path().join("checkpoint").to_str().unwrap(),
                            id,
                            true,
                        ));
                    }
                    let op = state.0.as_mut().unwrap();
                    if restoring || snapshot.is_some() {
                        assert_eq!(op.timer_deadline(), 12345);
                        op.push(&left, true);
                        op.push(&right, false);
                    }
                    validate(
                        &op.flush(1000),
                        if phase == "push_right" || restoring || snapshot.is_some() {
                            rows
                        } else {
                            0
                        },
                        width,
                        nullable,
                        0,
                    );
                    if phase != "fire" {
                        validate(&op.flush(2000), rows, width, nullable, 1000);
                    }
                    validate(&op.flush(2000), 0, width, nullable, 0);
                    op.push(&left, true);
                    op.push(&right, false);
                    op.push(&future_left, true);
                    op.push(&future_right, false);
                    validate(&op.flush(3000), rows, width, nullable, 2000);
                    validate(&op.flush(3000), 0, width, nullable, 0);
                    let label =
                        format!("window_join/{phase}/{rows}/bytes={width}/nulls={nullable}");
                    report(
                        &label,
                        left.columns(),
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
    group.finish();
}
