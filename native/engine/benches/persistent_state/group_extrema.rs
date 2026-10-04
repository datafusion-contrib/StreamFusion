use super::{input, key, value, OPTIONS};
use arrow::array::{Array, ArrayRef, Int64Array, Int8Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use streamfusion::bench::PersistentGroupBy;
use streamfusion_benchmark_support::{measure, report};

type Groups = BTreeMap<Option<String>, Vec<Option<i64>>>;
type Output = (Option<String>, i64, Option<i64>, Option<i64>, i8);

fn populate(
    groups: &mut Groups,
    start: usize,
    end: usize,
    domain: usize,
    width: usize,
    nullable: bool,
) {
    for i in start..end {
        groups
            .entry(key(i, domain, width, nullable))
            .or_default()
            .push(value(i, domain, nullable));
    }
}

fn row(k: &Option<String>, values: &[Option<i64>], kind: i8) -> Output {
    let present = || values.iter().flatten().copied();
    (
        k.clone(),
        values.len() as i64,
        present().min(),
        present().max(),
        kind,
    )
}

fn changes(before: &Groups, after: &Groups, touched: &BTreeSet<Option<String>>) -> Vec<Output> {
    let mut output = Vec::new();
    for k in touched {
        match (before.get(k), after.get(k)) {
            (Some(old), Some(new)) => {
                output.push(row(k, old, 1));
                output.push(row(k, new, 2));
            }
            (Some(old), None) => output.push(row(k, old, 3)),
            (None, Some(new)) => output.push(row(k, new, 0)),
            (None, None) => panic!("untouched group in independent oracle"),
        }
    }
    output.sort();
    output
}

fn touched(
    start: usize,
    end: usize,
    domain: usize,
    width: usize,
    nullable: bool,
) -> BTreeSet<Option<String>> {
    (start..end)
        .map(|i| key(i, domain, width, nullable))
        .collect()
}

fn validate(batch: &RecordBatch, expected: &[Output]) {
    assert_eq!(
        batch.schema().as_ref(),
        &Schema::new(vec![
            Field::new("key0", DataType::Utf8, true),
            Field::new("result0", DataType::Int64, true),
            Field::new("result1", DataType::Int64, true),
            Field::new("result2", DataType::Int64, true),
            Field::new("$row_kind$", DataType::Int8, false),
        ])
    );
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let minimum = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let maximum = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let kinds = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    let mut transitions: BTreeMap<Option<String>, Vec<i8>> = BTreeMap::new();
    let mut actual = (0..batch.num_rows())
        .map(|i| {
            assert!(!counts.is_null(i) && !kinds.is_null(i));
            let k = (!keys.is_null(i)).then(|| keys.value(i).to_owned());
            let kind = kinds.value(i);
            transitions.entry(k.clone()).or_default().push(kind);
            (
                k,
                counts.value(i),
                (!minimum.is_null(i)).then(|| minimum.value(i)),
                (!maximum.is_null(i)).then(|| maximum.value(i)),
                kind,
            )
        })
        .collect::<Vec<_>>();
    for kinds in transitions.values() {
        assert!(
            matches!(kinds.as_slice(), [0] | [1, 2] | [3]),
            "invalid extrema transition order: {kinds:?}"
        );
    }
    actual.sort();
    assert_eq!(actual, expected);
}

fn delete(batch: &RecordBatch) -> RecordBatch {
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new("$row_kind$", DataType::Int8, false)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int8Array::from(vec![3; batch.num_rows()])) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

pub(super) fn extrema(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/rocksdb/group_extrema");
    for rows in [16, 1024, 16384] {
        for domain in [8, rows] {
            for width in [8, 264] {
                for nullable in [false, true] {
                    let initial = input(0, rows, domain, width, nullable);
                    let continued = input(rows, rows, domain, width, nullable);
                    let half = delete(&initial.slice(0, rows / 2));
                    let all = delete(&initial);
                    let mut original = Groups::new();
                    populate(&mut original, 0, rows, domain, width, nullable);
                    let mut remaining = Groups::new();
                    populate(&mut remaining, rows / 2, rows, domain, width, nullable);
                    let initial_keys = touched(0, rows, domain, width, nullable);
                    let half_keys = touched(0, rows / 2, domain, width, nullable);
                    let continued_keys = touched(rows, rows * 2, domain, width, nullable);
                    let source_dir = tempfile::tempdir().unwrap();
                    let mut source_operator = PersistentGroupBy::new_extrema(
                        source_dir.path().join("db").to_str().unwrap(),
                        OPTIONS,
                    );
                    assert_eq!(source_operator.update(&initial).num_rows(), 0);
                    validate(
                        &source_operator.flush(),
                        &changes(&Groups::new(), &original, &initial_keys),
                    );
                    assert_eq!(source_operator.update(&half).num_rows(), 0);
                    validate(
                        &source_operator.flush(),
                        &changes(&original, &remaining, &half_keys),
                    );
                    let checkpoint_path = source_dir.path().join("checkpoint");
                    let checkpoint = checkpoint_path.to_str().unwrap();
                    let generation = source_operator.checkpoint(checkpoint);
                    for phase in [
                        "insert_flush",
                        "retract_half",
                        "retract_all",
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
                                let mut op = PersistentGroupBy::new_extrema(
                                    directory.path().join("db").to_str().unwrap(),
                                    OPTIONS,
                                );
                                if phase != "insert_flush" {
                                    op.update(&initial);
                                    op.flush();
                                }
                                if phase == "checkpoint" {
                                    op.update(&half);
                                    op.flush();
                                }
                                Some(op)
                            };
                            (operator, directory)
                        };
                        let run = |state: &mut (Option<PersistentGroupBy>, tempfile::TempDir)| {
                            if restoring {
                                state.0 = Some(PersistentGroupBy::restore_extrema(
                                    state.1.path().join("db").to_str().unwrap(),
                                    OPTIONS,
                                    checkpoint,
                                    generation,
                                    phase == "restore_aligned",
                                ));
                                (None, None)
                            } else if phase == "checkpoint" {
                                (
                                    None,
                                    Some(state.0.as_mut().unwrap().checkpoint(
                                        state.1.path().join("checkpoint").to_str().unwrap(),
                                    )),
                                )
                            } else {
                                let batch = match phase {
                                    "insert_flush" => &initial,
                                    "retract_half" => &half,
                                    "retract_all" => &all,
                                    _ => unreachable!(),
                                };
                                let op = state.0.as_mut().unwrap();
                                op.update(batch);
                                (Some(op.flush()), None)
                            }
                        };
                        let mut state = setup();
                        let ((output, saved), allocations) = measure(|| run(&mut state));
                        let mut current = match phase {
                            "insert_flush" => original.clone(),
                            "retract_all" => Groups::new(),
                            _ => remaining.clone(),
                        };
                        if let Some(ref batch) = output {
                            let (before, touched) = match phase {
                                "insert_flush" => (&Groups::new(), &initial_keys),
                                "retract_half" => (&original, &half_keys),
                                "retract_all" => (&original, &initial_keys),
                                _ => unreachable!(),
                            };
                            validate(batch, &changes(before, &current, touched));
                        }
                        if let Some(saved) = saved {
                            state.0.take();
                            state.0 = Some(PersistentGroupBy::restore_extrema(
                                state.1.path().join("verified-db").to_str().unwrap(),
                                OPTIONS,
                                state.1.path().join("checkpoint").to_str().unwrap(),
                                saved,
                                true,
                            ));
                        }
                        let op = state.0.as_mut().unwrap();
                        assert_eq!(op.flush().num_rows(), 0);
                        if phase.starts_with("retract_") {
                            let removed = if phase == "retract_half" {
                                rows / 2
                            } else {
                                rows
                            };
                            assert_eq!(op.update(&initial.slice(0, removed)).num_rows(), 0);
                            validate(
                                &op.flush(),
                                &changes(
                                    &current,
                                    &original,
                                    &touched(0, removed, domain, width, nullable),
                                ),
                            );
                            current = original.clone();
                        }
                        let mut after = current.clone();
                        populate(&mut after, rows, rows * 2, domain, width, nullable);
                        assert_eq!(op.update(&continued).num_rows(), 0);
                        validate(&op.flush(), &changes(&current, &after, &continued_keys));
                        assert_eq!(op.flush().num_rows(), 0);
                        let label = format!("group_extrema/{phase}/{rows}/domain={domain}/bytes={width}/nulls={nullable}");
                        report(
                            &label,
                            initial.columns(),
                            output.as_ref().map(|b| b.columns()).unwrap_or(&[]),
                            allocations,
                        );
                        group.throughput(Throughput::Elements(if phase == "retract_half" {
                            rows as u64 / 2
                        } else {
                            rows as u64
                        }));
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
