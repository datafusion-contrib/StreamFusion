use super::OPTIONS;
use arrow::array::{Array, ArrayRef, Int64Array, Int8Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use streamfusion::bench::PersistentUpdatingJoin;
use streamfusion_benchmark_support::{measure, report};

fn key(i: usize, width: usize) -> String {
    format!("{i}/中\0{}", "x".repeat(width))
}
fn input(
    rows: usize,
    domain: usize,
    width: usize,
    nullable: bool,
    left: bool,
    kind: i8,
) -> RecordBatch {
    let indices = || (0..=rows).map(|i| i.saturating_sub(1));
    let batch = RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(StringArray::from_iter(indices().map(|i| {
                let k = if left { i } else { i / 2 % domain };
                (!(nullable && k % 11 == 0)).then(|| key(k, width))
            }))) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int64Array::from_iter(indices().map(|i| {
                let v = if left { i + 500000 } else { i / 2 };
                (!(nullable && v % 7 == 0)).then_some(v as i64)
            }))) as ArrayRef,
        ),
        (
            "$row_kind$",
            Arc::new(Int8Array::from_iter_values(indices().map(|_| kind))) as ArrayRef,
        ),
    ])
    .unwrap();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
            Field::new("$row_kind$", DataType::Int8, false),
        ])),
        batch.columns().to_vec(),
    )
    .unwrap()
    .slice(1, rows)
}
fn apply(op: &mut PersistentUpdatingJoin, batch: &RecordBatch, left: bool) -> Vec<RecordBatch> {
    vec![op.push(batch, left), op.flush()]
}
fn validate_mode(
    outputs: &[RecordBatch],
    rows: usize,
    domain: usize,
    width: usize,
    nullable: bool,
    kind: i8,
    unique: bool,
) {
    let mut expected = (0..rows)
        .filter_map(|i| {
            let k = i / 2 % domain;
            if nullable && k % 11 == 0 {
                return None;
            }
            Some((
                key(k, width),
                (!(nullable && (k + 500000) % 7 == 0)).then_some((k + 500000) as i64),
                (!(nullable && (i / 2) % 7 == 0)).then_some((i / 2) as i64),
                kind,
            ))
        })
        .collect::<Vec<_>>();
    if unique {
        let mut seen = std::collections::HashSet::new();
        expected.reverse();
        expected.retain(|row| seen.insert(row.0.clone()));
    }
    let mut actual = Vec::new();
    for batch in outputs.iter().filter(|b| b.num_rows() > 0) {
        let schema = Schema::new(vec![
            Field::new("c0", DataType::Utf8, true),
            Field::new("c1", DataType::Int64, true),
            Field::new("c2", DataType::Utf8, true),
            Field::new("c3", DataType::Int64, true),
            Field::new("$row_kind$", DataType::Int8, false),
        ]);
        assert_eq!(batch.schema().as_ref(), &schema);
        let lk = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rk = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let lv = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let rv = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(!lk.is_null(i) && !rk.is_null(i));
            assert_eq!(lk.value(i), rk.value(i));
            actual.push((
                lk.value(i).to_owned(),
                (!lv.is_null(i)).then(|| lv.value(i)),
                (!rv.is_null(i)).then(|| rv.value(i)),
                kinds.value(i),
            ));
        }
    }
    expected.sort();
    actual.sort();
    assert_eq!(actual, expected);
}
pub(super) fn updating_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/rocksdb/updating_join");
    for rows in [16, 1024, 16384] {
        for domain in [1, 8, rows] {
            for width in [8, 264] {
                for nullable in [false, true] {
                    let left = input(domain, domain, width, nullable, true, 0);
                    let left_delete = input(domain, domain, width, nullable, true, 3);
                    let right_delete = input(rows, domain, width, nullable, false, 3);
                    let data_schema = Arc::new(Schema::new(left.schema().fields()[..2].to_vec()));
                    for mini_batch in [false, true] {
                        let right = input(
                            rows,
                            domain,
                            width,
                            nullable,
                            false,
                            if mini_batch { 2 } else { 0 },
                        );
                        let validate =
                            |outputs: &[RecordBatch], rows, domain, width, nullable, kind| {
                                validate_mode(
                                    outputs, rows, domain, width, nullable, kind, mini_batch,
                                );
                            };
                        let source_dir = tempfile::tempdir().unwrap();
                        let source_path = source_dir.path().join("checkpoint");
                        let source = source_path.to_str().unwrap();
                        let generation = {
                            let mut op = PersistentUpdatingJoin::new(
                                source_dir.path().join("db").to_str().unwrap(),
                                data_schema.clone(),
                                OPTIONS,
                                mini_batch,
                            );
                            validate(&apply(&mut op, &left, true), 0, domain, width, nullable, 0);
                            validate(
                                &apply(&mut op, &right, false),
                                rows,
                                domain,
                                width,
                                nullable,
                                0,
                            );
                            op.checkpoint(source)
                        };
                        for phase in [
                            "push_right",
                            "retract_right",
                            "checkpoint",
                            "restore_aligned",
                            "restore_rebuilt",
                        ] {
                            let restoring = phase.starts_with("restore");
                            let setup = || {
                                let dir = tempfile::tempdir().unwrap();
                                let op = if restoring {
                                    None
                                } else {
                                    let mut op = PersistentUpdatingJoin::new(
                                        dir.path().join("db").to_str().unwrap(),
                                        data_schema.clone(),
                                        OPTIONS,
                                        mini_batch,
                                    );
                                    validate(
                                        &apply(&mut op, &left, true),
                                        0,
                                        domain,
                                        width,
                                        nullable,
                                        0,
                                    );
                                    if phase != "push_right" {
                                        validate(
                                            &apply(&mut op, &right, false),
                                            rows,
                                            domain,
                                            width,
                                            nullable,
                                            0,
                                        );
                                    }
                                    Some(op)
                                };
                                (op, dir)
                            };
                            let run = |state: &mut (
                                Option<PersistentUpdatingJoin>,
                                tempfile::TempDir,
                            )| {
                                if restoring {
                                    state.0 = Some(PersistentUpdatingJoin::restore(
                                        state.1.path().join("db").to_str().unwrap(),
                                        data_schema.clone(),
                                        OPTIONS,
                                        mini_batch,
                                        source,
                                        generation,
                                        phase == "restore_aligned",
                                    ));
                                    (Vec::new(), None)
                                } else if phase == "checkpoint" {
                                    (
                                        Vec::new(),
                                        Some(state.0.as_mut().unwrap().checkpoint(
                                            state.1.path().join("checkpoint").to_str().unwrap(),
                                        )),
                                    )
                                } else {
                                    (
                                        apply(
                                            state.0.as_mut().unwrap(),
                                            if phase == "push_right" {
                                                &right
                                            } else {
                                                &right_delete
                                            },
                                            false,
                                        ),
                                        None,
                                    )
                                }
                            };
                            let mut state = setup();
                            let ((output, snapshot), allocations) = measure(|| run(&mut state));
                            if phase == "push_right" || phase == "retract_right" {
                                validate(
                                    &output,
                                    rows,
                                    domain,
                                    width,
                                    nullable,
                                    if phase == "push_right" { 0 } else { 3 },
                                );
                            }
                            if let Some(id) = snapshot {
                                state.0.take();
                                state.0 = Some(PersistentUpdatingJoin::restore(
                                    state.1.path().join("verified-db").to_str().unwrap(),
                                    data_schema.clone(),
                                    OPTIONS,
                                    mini_batch,
                                    state.1.path().join("checkpoint").to_str().unwrap(),
                                    id,
                                    true,
                                ));
                            }
                            let op = state.0.as_mut().unwrap();
                            validate(
                                &apply(op, &left_delete, true),
                                if phase == "retract_right" { 0 } else { rows },
                                domain,
                                width,
                                nullable,
                                3,
                            );
                            validate(
                                &apply(op, &right_delete, false),
                                0,
                                domain,
                                width,
                                nullable,
                                3,
                            );
                            validate(&apply(op, &left, true), 0, domain, width, nullable, 0);
                            validate(&apply(op, &right, false), rows, domain, width, nullable, 0);
                            validate(&[op.flush()], 0, domain, width, nullable, 0);
                            let label = format!("updating_join/{phase}/{rows}/domain={domain}/bytes={width}/nulls={nullable}/mini_batch={mini_batch}");
                            let out_columns = output
                                .iter()
                                .find(|b| b.num_rows() > 0)
                                .map(|b| b.columns())
                                .unwrap_or(&[]);
                            report(&label, right.columns(), out_columns, allocations);
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
    }
    group.finish();
}
