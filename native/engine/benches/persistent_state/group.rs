use super::OPTIONS;
use arrow::array::{Array, ArrayRef, Int64Array, Int8Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use std::{collections::BTreeMap, sync::Arc};
use streamfusion::bench::PersistentGroupBy;
use streamfusion_benchmark_support::{measure, report};

type Groups = BTreeMap<Option<String>, (i64, Option<i64>)>;
type Output = (Option<String>, i64, Option<i64>, i8);

fn key(i: usize, domain: usize, width: usize, nullable: bool) -> Option<String> {
    if nullable && i % 11 == 0 {
        None
    } else {
        Some(format!("{}/中\0{}", i % domain, "x".repeat(width)))
    }
}
fn value(i: usize, domain: usize, nullable: bool) -> Option<i64> {
    if nullable && (i % 7 == 0 || i % domain == 1) {
        None
    } else {
        Some(i as i64 % 101 - 50)
    }
}
fn input(start: usize, rows: usize, domain: usize, width: usize, nullable: bool) -> RecordBatch {
    let indices = || (0..=rows).map(|i| start + i.saturating_sub(1));
    RecordBatch::try_from_iter(vec![
        (
            "key",
            Arc::new(StringArray::from_iter(
                indices().map(|i| key(i, domain, width, nullable)),
            )) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int64Array::from_iter(
                indices().map(|i| value(i, domain, nullable)),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
    .slice(1, rows)
}
fn advance(
    groups: &mut Groups,
    start: usize,
    rows: usize,
    domain: usize,
    width: usize,
    nullable: bool,
) -> Vec<Output> {
    let before = groups.clone();
    let mut touched = BTreeMap::new();
    for i in start..start + rows {
        let k = key(i, domain, width, nullable);
        let state = groups.entry(k.clone()).or_insert((0, None));
        state.0 += 1;
        if let Some(v) = value(i, domain, nullable) {
            state.1 = Some(state.1.unwrap_or(0) + v);
        }
        touched.insert(k, ());
    }
    let mut output = Vec::new();
    for k in touched.keys() {
        if let Some(&(count, sum)) = before.get(k) {
            output.push((k.clone(), count, sum, 1));
        }
        let &(count, sum) = groups.get(k).unwrap();
        output.push((
            k.clone(),
            count,
            sum,
            if before.contains_key(k) { 2 } else { 0 },
        ));
    }
    output.sort();
    output
}
fn validate(batch: &RecordBatch, expected: &[Output]) {
    assert_eq!(
        batch.schema().as_ref(),
        &Schema::new(vec![
            Field::new("key0", DataType::Utf8, true),
            Field::new("result0", DataType::Int64, true),
            Field::new("result1", DataType::Int64, true),
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
    let sums = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let kinds = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    let mut actual = (0..batch.num_rows())
        .map(|i| {
            assert!(!counts.is_null(i));
            assert!(!kinds.is_null(i));
            (
                (!keys.is_null(i)).then(|| keys.value(i).to_owned()),
                counts.value(i),
                (!sums.is_null(i)).then(|| sums.value(i)),
                kinds.value(i),
            )
        })
        .collect::<Vec<_>>();
    let mut transitions: BTreeMap<Option<String>, Vec<i8>> = BTreeMap::new();
    for (key, _, _, kind) in &actual {
        transitions.entry(key.clone()).or_default().push(*kind);
    }
    for kinds in transitions.values() {
        assert!(
            kinds.as_slice() == [0] || kinds.as_slice() == [1, 2],
            "invalid per-key transition order: {kinds:?}"
        );
    }
    actual.sort();
    assert_eq!(actual, expected);
}
pub(super) fn group(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/rocksdb/group_aggregate");
    for rows in [16, 1024, 16384] {
        for domain in [8, rows] {
            for width in [8, 264] {
                for nullable in [false, true] {
                    let initial = input(0, rows, domain, width, nullable);
                    let continued = input(rows, rows, domain, width, nullable);
                    let mut oracle = Groups::new();
                    let first = advance(&mut oracle, 0, rows, domain, width, nullable);
                    let second = advance(&mut oracle, rows, rows, domain, width, nullable);
                    let source_dir = tempfile::tempdir().unwrap();
                    let mut source_operator = PersistentGroupBy::new(
                        source_dir.path().join("db").to_str().unwrap(),
                        OPTIONS,
                    );
                    assert_eq!(source_operator.update(&initial).num_rows(), 0);
                    validate(&source_operator.flush(), &first);
                    let source_path = source_dir.path().join("checkpoint");
                    let source = source_path.to_str().unwrap();
                    let generation = source_operator.checkpoint(source);
                    for phase in [
                        "update_flush",
                        "checkpoint",
                        "restore_aligned",
                        "restore_rebuilt",
                    ] {
                        let restoring = phase.starts_with("restore");
                        let setup = || {
                            let dir = tempfile::tempdir().unwrap();
                            let operator = if restoring {
                                None
                            } else {
                                let mut op = PersistentGroupBy::new(
                                    dir.path().join("db").to_str().unwrap(),
                                    OPTIONS,
                                );
                                if phase == "checkpoint" {
                                    op.update(&initial);
                                    op.flush();
                                }
                                Some(op)
                            };
                            (operator, dir)
                        };
                        let run = |state: &mut (Option<PersistentGroupBy>, tempfile::TempDir)| {
                            if restoring {
                                state.0 = Some(PersistentGroupBy::restore(
                                    state.1.path().join("db").to_str().unwrap(),
                                    OPTIONS,
                                    source,
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
                                let op = state.0.as_mut().unwrap();
                                op.update(&initial);
                                (Some(op.flush()), None)
                            }
                        };
                        let mut state = setup();
                        let ((output, snapshot), allocations) = measure(|| run(&mut state));
                        if let Some(ref output) = output {
                            validate(output, &first);
                        }
                        if let Some(snapshot) = snapshot {
                            state.0.take();
                            state.0 = Some(PersistentGroupBy::restore(
                                state.1.path().join("verified-db").to_str().unwrap(),
                                OPTIONS,
                                state.1.path().join("checkpoint").to_str().unwrap(),
                                snapshot,
                                true,
                            ));
                        }
                        let op = state.0.as_mut().unwrap();
                        assert_eq!(op.update(&continued).num_rows(), 0);
                        validate(&op.flush(), &second);
                        assert_eq!(op.flush().num_rows(), 0);
                        let label = format!(
                            "group/{phase}/{rows}/domain={domain}/bytes={width}/nulls={nullable}"
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
